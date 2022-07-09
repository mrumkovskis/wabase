package org.wabase
package client

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.coding.Coders.{ Gzip, Deflate, NoCoding }
import akka.http.scaladsl.marshalling.{Marshal, Marshaller}
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.model.ws.{Message, WebSocketRequest}
import akka.http.scaladsl.unmarshalling._
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}

import scala.collection.immutable.{Seq => iSeq}
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.language.postfixOps
import scala.util.{Failure, Success}


class ClientException(message: String, cause: Throwable, val status: StatusCode, val responseContent: String, val request: HttpRequest) extends Exception(message, cause)
object ClientException{
  def apply(status: StatusCode, message: String, responseContent: String, request: HttpRequest): ClientException = new ClientException(message, null, status, responseContent, request)
  def apply(status: StatusCode, message: String, request: HttpRequest): ClientException = new ClientException(message, null, status, null, request)
  def apply(message: String, cause: Throwable): ClientException = new ClientException(message, cause, null, null, null)
  def apply(cause: Throwable): ClientException = apply(cause.getMessage, cause)
  def apply(message: String): ClientException = apply(message, null)
}

trait RestClient extends Loggable{

  import RestClient.{WsClosed, WsFailed}
  def actorSystemName = "rest-client"
  def createActorSystem = ActorSystem(actorSystemName)
  implicit val system = createActorSystem
  implicit val executionContext = system.dispatcher

  lazy val port = 8080
  lazy val serverPath = s"http://localhost:$port/"
  lazy val serverWsPath = s"ws://localhost:$port/ws"

  val flow = Http().superPool[Unit]()

  val requestTimeout = RestClient.defaultRequestTimeout
  val awaitTimeout   = RestClient.defaultAwaitTimeout
  val defaultUsername: String = "admin"
  val defaultPassword: String = "admin"

  val urlEncoder = java.net.URLEncoder.encode(_: String, "UTF-8")
  val urlDecoder = java.net.URLDecoder.decode(_: String, "UTF-8")

  class CookieMap {
    val map =  scala.collection.mutable.Map.empty[String, HttpCookie]

    def getCookies = if(map.isEmpty) Nil else iSeq(Cookie(map.map(c=> c._2.pair).toList))
    def setCookiesFromHeaders(headers: iSeq[HttpHeader]): Unit = {
      map ++= headers.flatMap{
        case `Set-Cookie`(cookie) => List(cookie)
        case _ => Nil
      }.map(c => c.name -> c)
    }
    def setCookies(cookiesToSet: Map[String, Any], cookieStorage: CookieMap = cookiesThreadLocal.get()): Unit = {
      map ++= cookiesToSet.map(c => c._1 -> HttpCookie(c._1, c._2.toString))
    }
  }

  private val cookiesThreadLocal = new ThreadLocal[CookieMap](){override def initialValue = new CookieMap}
  def getCookieStorage = cookiesThreadLocal.get()
  def clearCookies = cookiesThreadLocal.remove

  def decodeResponse(response: HttpResponse): HttpResponse = {
    val decoder = response.encoding match {
      case HttpEncodings.gzip =>
        Gzip
      case HttpEncodings.deflate =>
        Deflate
      case _ /*HttpEncodings.identity*/ =>
        NoCoding
    }

    decoder.decodeMessage(response)
  }

  def httpGetAwait[R](path: String, params: Map[String, Any] = Map.empty, headers: iSeq[HttpHeader] = iSeq())
                (implicit unmarshaller: FromResponseUnmarshaller[R]): R =
    try Await.result(httpGet[R](path, params, headers), awaitTimeout) catch {
      case util.control.NonFatal(e) => requestFailed(s"Request failed (server: $serverPath, path: $path): ${e.getMessage}", e)
    }

  def httpPostAwait[T, R](method: HttpMethod, path: String, content: T, headers: iSeq[HttpHeader] = iSeq())
                    (implicit marshaller: Marshaller[T, RequestEntity], umarshaller: FromResponseUnmarshaller[R]): R =
    try Await.result(httpPost[T, R](method, path, content, headers), awaitTimeout) catch {
      case util.control.NonFatal(e) => requestFailed(s"Request failed (server: $serverPath, path: $path): ${e.getMessage}", e)
    }

  def httpGet[R](path: String, params: Map[String, Any] = Map.empty, headers: iSeq[HttpHeader] = iSeq(),
                 cookieStorage: CookieMap = getCookieStorage, timeout: FiniteDuration = requestTimeout)
                     (implicit unmarshaller: FromResponseUnmarshaller[R]): Future[R] = {
    val plainUri = Uri(requestPath(path))
    lazy val query = Query(params.toList.flatMap{
      case (k, null) => List(k -> "")
      case (k, list: Seq[_]) => list.map(li => k -> li.toString)
      case (k, v) => List(k -> v.toString)
    }:_*)
    val requestUri = if (params.nonEmpty) plainUri.withQuery(query) else plainUri
    for{
      response <- doRequest(HttpRequest(uri = requestUri, headers = headers), cookieStorage, timeout)
      responseEntity <- Unmarshal(decodeResponse(response)).to[R]
    } yield responseEntity
  }

  def httpPost[T, R](method: HttpMethod, path: String, content: T, headers: iSeq[HttpHeader] = iSeq(),
                     cookieStorage: CookieMap = getCookieStorage, timeout: FiniteDuration = requestTimeout)
                         (implicit marshaller: Marshaller[T, RequestEntity], unmarshaller: FromResponseUnmarshaller[R]): Future[R] = {
    val requestUri = requestPath(path)
    for{
      requestEntity <- Marshal(content).to[RequestEntity].map { requestEntity =>
        headers.find(_.isInstanceOf[`Content-Type`])
          .map(ct => requestEntity.withContentType(ct.asInstanceOf[`Content-Type`].contentType)).getOrElse(requestEntity)
      }
      response <- doRequest(HttpRequest(method = method, uri = requestUri, entity = requestEntity,
        headers = headers.filterNot(_.isInstanceOf[`Content-Type`])), cookieStorage, timeout)
      responseEntity <- Unmarshal(decodeResponse(response)).to[R]
    } yield  responseEntity

  }

  def requestPath(uri: String) =
    if (uri.startsWith("http://") || uri.startsWith("https://")) uri
    else if (uri.startsWith("/") && serverPath.endsWith("/")) serverPath + uri.drop(1)
    else if (!uri.startsWith("/") && !serverPath.endsWith("/")) serverPath + "/" + uri
    else serverPath + uri

  protected def doRequest(req: HttpRequest, cookieStorage: CookieMap, timeout: FiniteDuration, maxRedirects: Int = 20): Future[HttpResponse] = {
    val request = req.withHeaders(req.headers ++ cookieStorage.getCookies)
    logger.debug(s"HTTP ${request.method.value} ${request.uri}")
    Source.single((request, ())).via(flow).completionTimeout(timeout).runWith(Sink.head).recover {
      case util.control.NonFatal(ex) => (Failure(ex), ())
    }.flatMap {
      case (Failure(error), _) =>
        requestFailed(error.getMessage, error, null, null, request)
      case (Success(response), _) =>
        cookieStorage.setCookiesFromHeaders(response.headers)
        (response.status.intValue, response.header[Location]) match {
          case (200 | 201 | 204, _) => Future.successful(response)
          case (301 | 302 | 303, Some(Location(uri))) =>
            response.discardEntityBytes()
            if (maxRedirects > 0)
              doRequest(HttpRequest(uri = requestPath(uri.toString), headers = req.headers), cookieStorage, timeout, maxRedirects - 1).recover {
                case util.control.NonFatal(e) => requestFailed(e.getMessage, e, response.status, null, request)
              }
            else
              requestFailed("Too many http redirects", null, response.status, uri.toString, request)
          case _ =>
            Unmarshal(decodeResponse(response).entity).to[String].recover {
              case util.control.NonFatal(e) =>
                logger.error("Failed to unmarshal response for unexpected status ${response.status.intValue}", e)
                ""
            }.flatMap { content =>
              val exceptionMessage = response.status.value + "\n" + response.status.defaultMessage + "\n" + content
              requestFailed(exceptionMessage, null, response.status, content, request)
            }
        }
    }
  }

  protected def requestFailed(
      message: String, cause: Throwable,
      status: StatusCode = null, content: String = null, request: HttpRequest = null): Nothing = {
    val verboseMessage = if (request != null) s"Request to '${request.uri}' failed: $message" else message
    val causeStatus = cause match {
      case ce: ClientException => ce.status
      case _ => status
    }
    throw new ClientException(verboseMessage, cause, causeStatus, content, request)
  }

  def listenToWs(actor: ActorRef) = {
    val deferredFlow: Flow[Message, Message, Promise[Option[Message]]] =
      Flow.fromSinkAndSourceMat(
        Sink.actorRef(actor, WsClosed, e => WsFailed(e)), // FIXME do not use INTERNAL API
        Source.maybe[Message])(Keep.right)

    val (upgradeResponse, promise) = Http().singleWebSocketRequest(
      WebSocketRequest(serverWsPath, extraHeaders = getCookieStorage.getCookies), deferredFlow)
    clearCookies
  }
}

object RestClient extends Loggable{
  val defaultRequestTimeout = toFiniteDuration(config.getDuration("app.rest-client.request-timeout"))
  val defaultAwaitTimeout: FiniteDuration =
    Option("app.rest-client.await-timeout")
      .filter(config.hasPath).map(config.getDuration).map(toFiniteDuration)
      .getOrElse(defaultRequestTimeout + (2 seconds))
  object WsClosed
  case class WsFailed(cause: Throwable)
}
