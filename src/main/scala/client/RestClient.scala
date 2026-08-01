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

class RestClient(clientCfg: Config = HttpClientConfig.componentConfs.root)(implicit val system: ActorSystem) extends HttpClient with Loggable {

  import RestClient.{WsClosed, WsFailed}
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

  /** In-memory cookie jar. All public methods are synchronized (safe to share across threads /
    * async request callbacks). Update cookies via [[setCookies]] / [[setCookiesFromHeaders]].
    */
  class CookieMap {
    private val lock = new AnyRef
    private val store = scala.collection.mutable.Map.empty[String, HttpCookie]
    /** Host-only cookies (no `Domain` attribute): cookie name → host that set them. */
    private val hostOnlyHosts = scala.collection.mutable.Map.empty[String, String]

    /** Immutable snapshot of stored cookies */
    def map: scala.collection.immutable.Map[String, HttpCookie] =
      lock.synchronized(store.toMap)

    def getCookies: iSeq[Cookie] = lock.synchronized {
      cookieHeader(store.values.toList)
    }

    /** Cookies scoped for `uri` (host-only + Domain attribute; path when present). */
    def getCookies(uri: Uri): iSeq[Cookie] = lock.synchronized {
      val host = uri.authority.host.address
      val path = uri.path.toString
      cookieHeader(store.values.iterator.filter(c => cookieMatches(c, host, path)).toList)
    }

    private def cookieHeader(cookies: Iterable[HttpCookie]): iSeq[Cookie] = {
      val pairs = cookies.map(_.pair).toList
      if (pairs.isEmpty) Nil else iSeq(Cookie(pairs))
    }

    private def cookieMatches(cookie: HttpCookie, host: String, path: String): Boolean = {
      val domainOk = cookie.domain match {
        case Some(d) if !hostOnlyHosts.contains(cookie.name) =>
          RestClient.cookieDomainMatches(host, d)
        case _ =>
          hostOnlyHosts.get(cookie.name) match {
            case Some(h) => h.equalsIgnoreCase(host)
            // Programmatically set cookies (no origin recorded) — keep legacy send-anywhere behaviour
            case None => true
          }
      }
      val pathOk = cookie.path match {
        case Some(p) =>
          path == p ||
            path.startsWith(if (p.endsWith("/")) p else p + "/") ||
            (p != "/" && path.startsWith(p))
        case None => true
      }
      domainOk && pathOk
    }

    def setCookiesFromHeaders(headers: iSeq[HttpHeader], requestUri: Uri = null): Unit = {
      val reqHost =
        Option(requestUri).filter(_.authority.nonEmpty).map(_.authority.host.address)
      lock.synchronized { headers.foreach {
        case `Set-Cookie`(cookie) =>
          if ((cookie.maxAge.isEmpty  || cookie.maxAge.get > 0) &&
              (cookie.expires.isEmpty || cookie.expires.get.clicks > System.currentTimeMillis)) {
            store += (cookie.name -> cookie)
            (cookie.domain, reqHost) match {
              case (None, Some(h)) => hostOnlyHosts(cookie.name) = h
              case (Some(_), _)    => hostOnlyHosts -= cookie.name
              case (None, None)    => hostOnlyHosts -= cookie.name
            }
          } else {
            store -= cookie.name
            hostOnlyHosts -= cookie.name
          }
        case _ =>
      }}
    }
    def setCookies(cookies: Map[String, Any]): Unit = lock.synchronized {
      store ++= cookies.map { case (n, c) => n -> HttpCookie(n, c.toString) }
      cookies.keys.foreach(hostOnlyHosts -= _)
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

  def httpGetAwait[R](
    path: String,
    params: Map[String, Any] = Map.empty,
    headers: iSeq[HttpHeader] = iSeq(),
    throwHttpErrors: Boolean = true,
    followRedirects: Boolean = true,
  )(implicit unmarshaller: FromResponseUnmarshaller[R]): R =
    try Await.result(httpGet[R](path, params, headers, throwHttpErrors = throwHttpErrors, followRedirects = followRedirects), awaitTimeout) catch {
      case util.control.NonFatal(e) => requestFailed(s"Request failed (server: $serverPath, path: $path): ${e.getMessage}", e)
    }

  def httpPostAwait[T, R](
    method: HttpMethod,
    path: String,
    content: T,
    headers: iSeq[HttpHeader] = iSeq(),
    throwHttpErrors: Boolean = true,
    followRedirects: Boolean = true,
  )(implicit marshaller: Marshaller[T, RequestEntity], umarshaller: FromResponseUnmarshaller[R]): R =
    try Await.result(httpPost[T, R](method, path, content, headers, throwHttpErrors = throwHttpErrors, followRedirects = followRedirects), awaitTimeout) catch {
      case util.control.NonFatal(e) => requestFailed(s"Request failed (server: $serverPath, path: $path): ${e.getMessage}", e)
    }

  def httpGet[R](
    path: String,
    params: Map[String, Any] = Map.empty,
    headers: iSeq[HttpHeader] = iSeq(),
    cookieStorage: CookieMap = getCookieStorage,
    timeout: FiniteDuration = requestTimeout,
    throwHttpErrors: Boolean = true,
    followRedirects: Boolean = true,
  )(implicit unmarshaller: FromResponseUnmarshaller[R]): Future[R] = {
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
      response <- doRequest(HttpRequest(uri = requestUri, headers = headers), cookieStorage, timeout,
        throwHttpErrors = Some(throwHttpErrors), followRedirects = Some(followRedirects))
      responseEntity <- Unmarshal(decodeResponse(response)).to[R]
    } yield responseEntity
  }

  def httpPost[T, R](
    method: HttpMethod,
    path: String,
    content: T,
    headers: iSeq[HttpHeader] = iSeq(),
    cookieStorage: CookieMap = getCookieStorage,
    timeout: FiniteDuration = requestTimeout,
    throwHttpErrors: Boolean = true,
    followRedirects: Boolean = true,
  )(implicit marshaller: Marshaller[T, RequestEntity], unmarshaller: FromResponseUnmarshaller[R]): Future[R] = {
    val requestUri = requestPath(path)
    for{
      requestEntity <- Marshal(content).to[RequestEntity].map { requestEntity =>
        headers.find(_.isInstanceOf[`Content-Type`])
          .map(ct => requestEntity.withContentType(ct.asInstanceOf[`Content-Type`].contentType)).getOrElse(requestEntity)
      }
      response <- doRequest(HttpRequest(method = method, uri = requestUri, entity = requestEntity,
        headers = headers.filterNot(_.isInstanceOf[`Content-Type`])), cookieStorage, timeout,
        throwHttpErrors = Some(throwHttpErrors), followRedirects = Some(followRedirects))
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

  /** Performs an HTTP request with optional cookie handling, redirect following, and error throwing.
    *
    * Relative request URIs are resolved against [[serverPath]]. Cookies from `cookieStorage` are
    * sent with the request; `Set-Cookie` headers on the response update `cookieStorage`.
    *
    * @param req             HTTP request to send
    * @param cookieStorage   cookie jar used for outbound cookies and updated from the response
    * @param timeout         maximum time to wait for a response from the connection pool
    * @param maxRedirects    maximum number of 301/302/303 redirects to follow (default 20);
    *                        when exhausted, fails with "Too many http redirects"
    * @param throwHttpErrors controls handling of non-success response statuses (outside 200, 201, 202, 204, 206):
    *                        - `Some(true)` — fail with [[ClientException]] (body included in the message)
    *                        - `Some(false)` — return the response as-is
    *                        - `None` — use the request's `HttpClient.ModeKey` attribute:
    *                          `ProxyMode` means do not throw, otherwise throw
    * @param followRedirects controls handling of 301/302/303 responses that have a `Location` header:
    *                        - `Some(true)` — follow the redirect
    *                        - `Some(false)` — return the redirect response as-is
    *                        - `None` — use the request's `HttpClient.ModeKey` attribute:
    *                          `ProxyMode` means do not follow, otherwise follow
    *                        When following to a different origin (scheme/host/port), `Authorization`,
    *                        `Cookie`, and `Host` request headers are stripped; cookies from the jar
    *                        are re-scoped to the redirect URI (host-only + Domain).
    * @return future of the final HTTP response (after optional redirect following)
    */
  protected def doRequest(
    req: HttpRequest,
    cookieStorage: CookieMap,
    timeout: FiniteDuration,
    maxRedirects: Int = 20,
    throwHttpErrors: Option[Boolean] = None,
    followRedirects: Option[Boolean] = None,
  ): Future[HttpResponse] = {
    val req_abs = if (req.uri.isAbsolute) req else req.withUri(Uri(requestPath(req.uri.toString)))
    val cookies = cookieStorage.getCookies(req_abs.uri)
    val request = if (cookies.isEmpty) req_abs else req_abs.withHeaders(req.headers ++ cookies)
    val doThrow = throwHttpErrors.getOrElse(req.attribute(HttpClient.ModeKey) != Some(ProxyMode))
    val follow  = followRedirects.getOrElse(req.attribute(HttpClient.ModeKey) != Some(ProxyMode))
    logger.debug(s"HTTP ${request.method.value} ${request.uri}")
    Source.single((request, ())).via(flow).completionTimeout(timeout).runWith(Sink.head).recover {
      case util.control.NonFatal(ex) => (Failure(ex), ())
    }.flatMap {
      case (Failure(error), _) =>
        requestFailed(error.getMessage, error, null, null, request)
      case (Success(response), _) =>
        cookieStorage.setCookiesFromHeaders(response.headers, request.uri)
        (response.status.intValue, response.header[Location]) match {
          case _ if isSuccess(response) =>
            Future.successful(response)
          case (301 | 302 | 303, Some(Location(locationUri))) =>
           if (follow) {
            response.discardEntityBytes()
            if (maxRedirects > 0) {
              val redirectUri = RestClient.resolveRedirectUri(request.uri, locationUri)
              val redirectMethod =
                HttpMethods.GET
              val redirectHeaders =
                RestClient.redirectRequestHeaders(request.uri, redirectUri, req.headers)
              doRequest(
                HttpRequest(method = redirectMethod, uri = redirectUri, headers = redirectHeaders),
                cookieStorage, timeout, maxRedirects - 1, Some(doThrow), Some(follow)
              ).recover {
                case util.control.NonFatal(e) => requestFailed(e.getMessage, e, response.status, null, request)
              }
            } else
              requestFailed("Too many http redirects", null, response.status, locationUri.toString, request)
           } else
            // Not following — return redirect response as-is
            Future.successful(response)
          case _ if !doThrow =>
            Future.successful(response)
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

  /** Resolve a Location header URI reference against the request URI (RFC 3986 §5.2). */
  def resolveRedirectUri(baseUri: Uri, locationUri: Uri): Uri = {
    require(baseUri.isAbsolute, s"Base URI must be absolute for redirect resolution: $baseUri")
    if (locationUri.isAbsolute) locationUri
    else locationUri.resolvedAgainst(baseUri)
  }

  /** True when scheme, host, and effective port are the same (case-insensitive scheme/host). */
  def isSameOrigin(a: Uri, b: Uri): Boolean =
    a.scheme.equalsIgnoreCase(b.scheme) &&
      a.authority.host.equalsIgnoreCase(b.authority.host) &&
      a.effectivePort == b.effectivePort

  /** Headers to send when following a redirect.
    * On a different origin (scheme/host/port), strips `Authorization`, `Cookie`, and `Host`
    * so credentials are not leaked cross-origin; cookies are re-applied from the jar for the new URI.
    */
  def redirectRequestHeaders(fromUri: Uri, toUri: Uri, headers: iSeq[HttpHeader]): iSeq[HttpHeader] =
    if (isSameOrigin(fromUri, toUri)) headers
    else headers.filterNot(h => h.is("authorization") || h.is("cookie") || h.is("host"))

  /** RFC 6265 domain-match (simplified): cookie domain matches request host. */
  private[client] def cookieDomainMatches(host: String, cookieDomain: String): Boolean = {
    val dom = if (cookieDomain.startsWith(".")) cookieDomain.drop(1) else cookieDomain
    host.equalsIgnoreCase(dom) || host.toLowerCase.endsWith("." + dom.toLowerCase)
  }
}
