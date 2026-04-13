package org.wabase
package client

import com.typesafe.config.Config
import com.typesafe.sslconfig.ssl._
import com.typesafe.sslconfig.util._
import org.apache.pekko.actor.{ActorRef, ActorSystem}
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.HttpsConnectionContext
import org.apache.pekko.http.scaladsl.coding.Coders.{Deflate, Gzip, NoCoding}
import org.apache.pekko.http.scaladsl.marshalling.{Marshal, Marshaller}
import org.apache.pekko.http.scaladsl.model.Uri.Query
import org.apache.pekko.http.scaladsl.model._
import org.apache.pekko.http.scaladsl.model.headers._
import org.apache.pekko.http.scaladsl.model.ws.{Message, WebSocketRequest}
import org.apache.pekko.http.scaladsl.unmarshalling._
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.wabase.client.HttpClient.ProxyMode
import org.wabase.client.RestClient.fullErrorErrorMessage

import scala.collection.immutable.{Seq => iSeq}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future, Promise}
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

class RestClient(clientCfg: Config = HttpClientConfig.componentConfs.root) extends HttpClient with Loggable {

  import RestClient.{WsClosed, WsFailed}
  def actorSystemName   = clientCfg.getString("actor-system-name")
  def createActorSystem = ActorSystem(actorSystemName)
  implicit val system: ActorSystem = createActorSystem
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  lazy val serverPath   = clientCfg.getString("server-path")
  lazy val serverWsPath = clientCfg.getString("server-ws-path")

  protected def getHttpsConnectionContext: Option[HttpsConnectionContext] = {
    Option("ssl-config").filter(clientCfg.hasPath).map(clientCfg.getConfig).map { sslConfig =>
      val sslConfigSettings = SSLConfigFactory.parse(sslConfig)
      val sslContext =
        new ConfigSSLContextBuilder(
          NoopLogger.factory(), // PrintlnLogger.factory(),
          sslConfigSettings,
          new DefaultKeyManagerFactoryWrapper(javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm()),
          new DefaultTrustManagerFactoryWrapper(javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm())
        ).build()
      val httpsConnectionContext = org.apache.pekko.http.scaladsl.ConnectionContext.httpsClient(sslContext)
      httpsConnectionContext
    }
  }

  val flow = getHttpsConnectionContext match {
    case None             => Http().superPool[Unit]()
    case Some(sslContext) => Http().superPool[Unit](sslContext)
  }

  val requestTimeout: FiniteDuration = toFiniteDuration(clientCfg.getDuration("request-timeout"))
  val awaitTimeout:   FiniteDuration =
    Option("await-timeout").filter(clientCfg.hasPath).map(clientCfg.getDuration).map(toFiniteDuration)
      .getOrElse(requestTimeout + (2 seconds))

  val urlEncoder = java.net.URLEncoder.encode(_: String, "UTF-8")
  val urlDecoder = java.net.URLDecoder.decode(_: String, "UTF-8")

  class CookieMap {
    val map =  scala.collection.mutable.Map.empty[String, HttpCookie]

    def getCookies = if(map.isEmpty) Nil else iSeq(Cookie(map.map(c=> c._2.pair).toList))
    def setCookiesFromHeaders(headers: iSeq[HttpHeader]): Unit = {
      headers.foreach {
        case `Set-Cookie`(cookie) =>
          if ((cookie.maxAge.isEmpty  || cookie.maxAge.get > 0) &&
              (cookie.expires.isEmpty || cookie.expires.get.clicks > System.currentTimeMillis))
               map += (cookie.name -> cookie)
          else map -=  cookie.name
        case _ =>
      }
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
    val requestUri =
      if (params.nonEmpty) {
        plainUri.rawQueryString match {
          case Some(rawQ) =>
            val delim =
              if  (rawQ.startsWith("/") && rawQ.indexOf("?") < 0)
                   "?" // support for key in query string
              else "&" // add params to existing query
            plainUri.withRawQueryString(
              s"${rawQ}${delim}${Uri.Empty.withQuery(query).rawQueryString.get}")
          case None =>
            plainUri.withQuery(query)
        }
      } else plainUri
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

  override def doRequest(req: HttpRequest): Future[HttpResponse] =
    doRequest(req, new CookieMap, requestTimeout)

  private val defaultSuccessStatusCodes = Set(200, 201, 202, 204, 206)
  protected def isSuccess(response: HttpResponse) =
    defaultSuccessStatusCodes.contains(response.status.intValue)

  protected def doRequest(req: HttpRequest, cookieStorage: CookieMap, timeout: FiniteDuration, maxRedirects: Int = 20): Future[HttpResponse] = {
    val req_abs = if (req.uri.isAbsolute) req else req.withUri(Uri(requestPath(req.uri.toString)))
    val request = if (cookieStorage.map.isEmpty) req_abs else req_abs.withHeaders(req.headers ++ cookieStorage.getCookies)
    val isProxy = req.attribute(HttpClient.ModeKey) match {
      case Some(ProxyMode) => true
      case _ => false
    }
    logger.debug(s"HTTP ${request.method.value} ${request.uri}")
    Source.single((request, ())).via(flow).completionTimeout(timeout).runWith(Sink.head).recover {
      case util.control.NonFatal(ex) => (Failure(ex), ())
    }.flatMap {
      case (Failure(error), _) =>
        requestFailed(error.getMessage, error, null, null, request)
      case (Success(response), _) =>
        cookieStorage.setCookiesFromHeaders(response.headers)
        (response.status.intValue, response.header[Location]) match {
          case _  if isProxy || isSuccess(response)  => Future.successful(response)
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
                logger.error(s"Failed to unmarshal response for unexpected status ${response.status.intValue}", e)
                ""
            }.flatMap { content =>
              val exceptionMessage = fullErrorErrorMessage(response.status, content)
              requestFailed(exceptionMessage, null, response.status, content, request)
            }
        }
    }
  }

  protected def requestFailed(
    message: String,
    cause: Throwable,
    status: StatusCode = null,
    content: String = null,
    request: HttpRequest = null
  ): Nothing = {
    val verboseMessage =
      if (request != null)
        s"Request ${Option(request.method).map(_.value).orNull} ${request.uri} failed: $message"
      else message
    cause match {
      case ce: ClientException => requestFailed(
        Option(message).getOrElse(ce.getMessage),
        ce.getCause,
        Option(status).getOrElse(ce.status),
        Option(content).getOrElse(ce.responseContent),
        Option(request).getOrElse(ce.request),
      )
      case _ => throw new ClientException(verboseMessage, cause, status, content, request)
    }
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

object RestClient extends Loggable {
  object WsClosed
  case class WsFailed(cause: Throwable)
  /** For legacy purposes */
  private [wabase] def fullErrorErrorMessage(status: StatusCode, content: String) =
    status.value + "\n" + status.defaultMessage + "\n" + content
}
