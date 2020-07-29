package org.wabase
package client

import java.util.concurrent.TimeoutException

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.coding.{ Gzip, Deflate, NoCoding }
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


class ClientException(message: String, cause: Throwable, val status: StatusCode, val responseContent: String) extends Exception(message, cause)
object ClientException{
  def apply(status: StatusCode, message: String, responseContent: String): ClientException = new ClientException(message, null, status, responseContent)
  def apply(status: StatusCode, message: String): ClientException = new ClientException(message, null, status, null)
  def apply(message: String, cause: Throwable): ClientException = new ClientException(message, cause, null, null)
  def apply(cause: Throwable): ClientException = apply(cause.getMessage, cause)
  def apply(message: String): ClientException = apply(message, null)
}

trait RestClient extends Loggable{

  import RestClient._
  def actorSystemName = "rest-client"
  def createActorSystem = ActorSystem(actorSystemName)
  implicit val system = createActorSystem
  implicit val executionContext = system.dispatcher

  lazy val port = 8080
  lazy val serverPath = s"http://localhost:$port/"
  lazy val serverWsPath = s"ws://localhost:$port/ws"

  val flow = Http().superPool[Unit]()

  val requestTimeout = RestClient.requestTimeout
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
  def getCookiewStorage = cookiesThreadLocal.get()
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


  def httpGet[R](path: String, params: Map[String, Any] = Map.empty, headers: iSeq[HttpHeader] = iSeq())
                (implicit unmarshaller: FromResponseUnmarshaller[R]): R =
    handleFuture(httpGetAsync[R](path, params, headers))

  def httpPost[T, R](method: HttpMethod, path: String, content: T, headers: iSeq[HttpHeader] = iSeq())
                    (implicit marshaller: Marshaller[T, RequestEntity], umarshaller: FromResponseUnmarshaller[R]): R =
    handleFuture(httpPostAsync[T, R](method, path, content, headers))

  def httpGetAsync[R](path: String, params: Map[String, Any] = Map.empty, headers: iSeq[HttpHeader] = iSeq(), cookieStorage: CookieMap = getCookiewStorage)
                     (implicit unmarshaller: FromResponseUnmarshaller[R]): Future[R] = {
    val plainUri = Uri(requestPath(path))
    lazy val queryStringWithParams = plainUri.rawQueryString.map(_ + "&").getOrElse("") + Query(params.mapValues(Option(_).map(_.toString).getOrElse("")).toMap)
    val requestUri = if (params.nonEmpty) plainUri.withRawQueryString(queryStringWithParams) else plainUri
    logger.debug(s"HTTP GET requestUri: $requestUri")
    for{
      response <- doRequest(HttpRequest(uri = requestUri, headers = headers), cookieStorage)
      responseEntity <- Unmarshal(decodeResponse(response)).to[R]
    } yield responseEntity
  }

  def httpPostAsync[T, R](method: HttpMethod, path: String, content: T, headers: iSeq[HttpHeader] = iSeq(), cookieStorage: CookieMap = getCookiewStorage)
                         (implicit marshaller: Marshaller[T, RequestEntity], unmarshaller: FromResponseUnmarshaller[R]): Future[R] = {
    val requestUri = requestPath(path)
    logger.debug(s"HTTP ${method.value} requestUri: $requestUri")
    for{
      requestEntity <- Marshal(content).to[RequestEntity].map { requestEntity =>
        headers.find(_.isInstanceOf[`Content-Type`])
          .map(ct => requestEntity.withContentType(ct.asInstanceOf[`Content-Type`].contentType)).getOrElse(requestEntity)
      }
      response <- doRequest(HttpRequest(method = method, uri = requestUri, entity = requestEntity,
        headers = headers.filterNot(_.isInstanceOf[`Content-Type`])), cookieStorage)
      responseEntity <- Unmarshal(decodeResponse(response)).to[R]
    } yield  responseEntity

  }

  def requestPath(uri: String) =
    if (uri.startsWith("http://") || uri.startsWith("https://")) uri
    else if (uri.startsWith("/") && serverPath.endsWith("/")) serverPath + uri.drop(1)
    else if (!uri.startsWith("/") && !serverPath.endsWith("/")) serverPath + "/" + uri
    else serverPath + uri


  private def doRequest(req: HttpRequest, cookieStorage: CookieMap): Future[HttpResponse] = {
    val request = req.copy(headers = req.headers ++ cookieStorage.getCookies)
    Source.single(request -> ((): Unit)).via(flow).completionTimeout(requestTimeout).runWith(Sink.head).map {
      case (Failure(error), _) => handleError(error)
      case (Success(response), _) =>
        cookieStorage.setCookiesFromHeaders(response.headers)
        (response.status.intValue, response.header[Location]) match {
          case (200 | 201 | 204, _) => response
          case (301 | 302 | 303, Some(Location(uri))) =>
            response.discardEntityBytes()
            handleFuture(doRequest(HttpRequest(uri = requestPath(uri.toString), headers = req.headers),cookieStorage))
          case _ =>
            val content = await(Unmarshal(decodeResponse(response).entity).to[String]).flatMap(_.toOption).getOrElse("")
            val exceptionMessage = response.status.value + "\n" + response.status.defaultMessage + "\n" + content
            throw ClientException(response.status, exceptionMessage, content)
        }
    }
  }


  def listenToWs(actor: ActorRef) = {
    val deferredFlow: Flow[Message, Message, Promise[Option[Message]]] =
      Flow.fromSinkAndSourceMat(
        Sink.actorRef(actor, WsClosed, (e) => { logger.error("Knipis", e) }),
        Source.maybe[Message])(Keep.right)

    val (upgradeResponse, promise) = Http().singleWebSocketRequest(
      WebSocketRequest(serverWsPath, extraHeaders = getCookiewStorage.getCookies), deferredFlow)
    clearCookies
  }

}

object RestClient extends Loggable{
  val requestTimeout = durationConfig("app.rest-client.request-timeout", 50 seconds)
  val awaitTimeout = durationConfig("app.rest-client.await-timeout", requestTimeout + (10 seconds))
  def await[T](f: Future[T]) = Await.ready(f, awaitTimeout).value

  def handleError(e: Throwable): Nothing = e match{
    case error if error.getCause != null => handleError(error.getCause)
    case error: TimeoutException =>
      logger.error(error.getMessage, error)
      throw new TimeoutException(error.getMessage)
    case error: ClientException => throw new ClientException(error.getMessage, error, error.status, error.responseContent)
    case error => throw ClientException(error)
  }

  def handleFuture[T](future: Future[T]) = {
    await(future) match {
      case Some(Success(result)) => result
      case Some(Failure(error)) => handleError(error)
      case None => sys.error("Future reached timeout "+awaitTimeout)
    }
  }

  object WsClosed
}
