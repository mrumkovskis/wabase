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
import org.apache.pekko.http.scaladsl.model.ws.WebSocketUpgradeResponse
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

  /** Host used as jar key for host-only cookies when no Domain is set (from [[serverPath]]). */
  lazy val defaultCookieHost: String =
    RestClient.normalizeCookieDomain(Uri(serverPath).authority.host.address)

  /**
   * Optional default `Domain` attribute for programmatically set cookies.
   * `None` means host-only cookies keyed by [[defaultCookieHost]].
   */
  lazy val defaultCookieDomain: Option[String] = None

  /** Default cookie path when not specified (default-path of [[serverPath]], RFC 6265 §5.1.4). */
  lazy val defaultCookiePath: String =
    RestClient.defaultCookiePath(Uri(serverPath))

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
    private val store = scala.collection.mutable.Map.empty[RestClient.CookieKey, HttpCookie]

    /** Immutable snapshot of stored cookies */
    def map: scala.collection.immutable.Map[RestClient.CookieKey, HttpCookie] =
      lock.synchronized(store.toMap)

    /** All stored cookies as a `Cookie` header (no URI scoping; not for outbound requests). */
    def getCookies: iSeq[Cookie] = lock.synchronized {
      cookieHeader(store.values)
    }

    /**
     * Cookies in scope for `uri` (host-only / domain-match + path-match + Secure).
     * Cookies with the `Secure` attribute are omitted unless the URI scheme is `https` or `wss`.
     */
    def getCookies(uri: Uri): iSeq[Cookie] = lock.synchronized {
      val host = uri.authority.host.address
      val path = {
        val p = uri.path.toString
        if (p.isEmpty) "/" else p
      }
      cookieHeader(
        store.iterator.collect {
          case (key, cookie) if RestClient.cookieMatches(key, cookie, host, path, uri.scheme) => cookie
        }.toList
      )
    }

    private def cookieHeader(cookies: Iterable[HttpCookie]): iSeq[Cookie] = {
      val pairs = cookies.map(_.pair).toList
      if (pairs.isEmpty) Nil else iSeq(Cookie(pairs))
    }

    def setCookiesFromHeaders(headers: iSeq[HttpHeader], requestUri: Uri = null): Unit = {
      val reqUri = Option(requestUri).filter(_.isAbsolute)
      val reqHost = reqUri.filter(_.authority.nonEmpty).map(_.authority.host.address)
        .getOrElse(defaultCookieHost)
      val reqDefaultPath = reqUri.map(RestClient.defaultCookiePath).getOrElse(defaultCookiePath)
      lock.synchronized {
        headers.foreach {
          case `Set-Cookie`(cookie) =>
            // RFC 6265 §5.2.3 / §5.3: empty Domain - host-only; otherwise request-host must domain-match
            val domainAttr = cookie.domain.map(RestClient.normalizeCookieDomain).filter(_.nonEmpty)
            if (domainAttr.forall(d => RestClient.cookieDomainMatches(reqHost, d))) {
              // Persist normalized Domain (strip leading `.`, lowercase) when present
              val withDomain = domainAttr match {
                case Some(d) if !cookie.domain.contains(d) => cookie.withDomain(d)
                case _ => cookie
              }
              val pathAttr = RestClient.effectiveCookiePath(withDomain.path, reqDefaultPath)
              val toStore =
                if (withDomain.path.contains(pathAttr)) withDomain
                else withDomain.withPath(pathAttr)
              val key = RestClient.cookieKeyFor(toStore, reqHost, reqDefaultPath)
              val alive =
                (toStore.maxAge.isEmpty || toStore.maxAge.get > 0) &&
                  (toStore.expires.isEmpty || toStore.expires.get.clicks > System.currentTimeMillis)
              if (alive) store(key) = toStore
              else store -= key
            }
          case _ =>
        }
      }
    }

    /**
     * Programmatically set cookies.
     *
     * @param cookies name, value
     * @param domain  `Domain` attribute; when `None`, uses [[defaultCookieDomain]].
     *                If still `None`, cookie is host-only and the jar key uses [[defaultCookieHost]].
     * @param path    cookie path; when `None`, empty, or not starting with `/`,
     *                uses [[defaultCookiePath]] (RFC 6265 §5.2.4)
     */
    def setCookies(
      cookies: Map[String, Any],
      domain: Option[String] = None,
      path: Option[String] = None,
    ): Unit = {
      val cookieDomain = domain.orElse(defaultCookieDomain).map(RestClient.normalizeCookieDomain)
      val keyDomain = cookieDomain.getOrElse(defaultCookieHost)
      val p = RestClient.effectiveCookiePath(path, defaultCookiePath)
      lock.synchronized {
        cookies.foreach { case (n, v) =>
          val key = RestClient.CookieKey(n, keyDomain, p)
          // domain=None on HttpCookie ⇒ host-only; resolved host lives on CookieKey
          store(key) = HttpCookie(n, v.toString, domain = cookieDomain, path = Some(p))
        }
      }
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
    *                        When following, the method becomes GET and the request body is not resent.
    *                        Content-related headers (`Content-Type`, `Content-Length`,
    *                        `Content-Encoding`, `Content-Language`, `Content-Location`, `Digest`,
    *                        `Last-Modified`) are stripped (RFC 9110 §15.4). When following to a
    *                        different origin (scheme/host/port), `Authorization`, `Cookie`, and
    *                        `Host` are also stripped; cookies from the jar are re-scoped to the
    *                        redirect URI (host-only + Domain + Secure).
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
                RestClient.redirectRequestHeaders(request.uri, redirectUri, req.headers, dropContentHeaders = true)
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

  def listenToWs(actor: ActorRef): Future[WebSocketUpgradeResponse] = {
    val deferredFlow: Flow[Message, Message, Promise[Option[Message]]] =
      Flow.fromSinkAndSourceMat(
        Sink.actorRef(actor, WsClosed, e => WsFailed(e)), // FIXME do not use INTERNAL API
        Source.maybe[Message])(Keep.right)

    val (upgradeResponse, promise) = Http().singleWebSocketRequest(
      WebSocketRequest(serverWsPath, extraHeaders = getCookieStorage.getCookies(Uri(serverWsPath))), deferredFlow)
    clearCookies
    upgradeResponse
  }
}

object RestClient extends Loggable {
  object WsClosed
  case class WsFailed(cause: Throwable)

  /** RFC 6265 cookie store identity: name + domain + path. */
  case class CookieKey(name: String, domain: String, path: String)

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

  /**
   * Normalize Domain attribute (RFC 6265 §5.2.3): strip leading `.`, lowercase.
   * Empty result should be treated as absent Domain (host-only).
   */
  private[client] def normalizeCookieDomain(domain: String): String = {
    val d = if (domain.startsWith(".")) domain.drop(1) else domain
    d.toLowerCase
  }

  /**
   * True if `host` is an IPv4 or IPv6 literal.
   * Used by domain-match so IP addresses only match exactly (RFC 6265 §5.1.3).
   */
  private[client] def isIpHost(host: String): Boolean = {
    val h =
      if (host.startsWith("[") && host.endsWith("]")) host.substring(1, host.length - 1)
      else host
    if (h.indexOf(':') >= 0) true // IPv6
    else {
      val parts = h.split('.')
      parts.length == 4 && parts.forall { p =>
        p.nonEmpty && p.length <= 3 && p.forall(_.isDigit) && {
          val n = p.toInt
          n >= 0 && n <= 255
        }
      }
    }
  }

  /**
   * RFC 6265 §5.1.3 domain-match: does `string` domain-match `domainString`?
   *
   * Both sides are compared case-insensitively. A leading `.` on `domainString` is ignored.
   * Suffix matches require a `.` boundary and apply only when `string` is a host name (not an IP).
   */
  private[client] def cookieDomainMatches(string: String, domainString: String): Boolean = {
    val s = string.toLowerCase
    val d = {
      val raw = domainString.toLowerCase
      if (raw.startsWith(".")) raw.drop(1) else raw
    }
    if (d.isEmpty) false
    else if (s == d) true
    else if (isIpHost(s)) false
    else s.endsWith("." + d)
  }

  /** RFC 6265 §5.1.4 default-path of a request URI. */
  private[client] def defaultCookiePath(uri: Uri): String = {
    val uriPath = uri.path.toString
    if (uriPath.isEmpty || !uriPath.startsWith("/")) "/"
    else {
      val idx = uriPath.lastIndexOf('/')
      if (idx <= 0) "/" else uriPath.substring(0, idx)
    }
  }

  /**
   * RFC 6265 §5.2.4: resolve a Path attribute to the cookie-path.
   * Absent, empty, or values that do not start with `/` become `requestDefaultPath`.
   */
  private[client] def effectiveCookiePath(pathAttr: Option[String], requestDefaultPath: String): String =
    pathAttr.filter(p => p.nonEmpty && p.charAt(0) == '/').getOrElse(requestDefaultPath)

  /** RFC 6265 §5.1.4 path-match. */
  private[client] def cookiePathMatches(requestPath: String, cookiePath: String): Boolean = {
    val rp = if (requestPath.isEmpty) "/" else requestPath
    // After storage, path should always be non-empty and start with `/`; treat empty as `/`.
    val cp = if (cookiePath.isEmpty) "/" else cookiePath
    val n = cp.length
    rp.startsWith(cp) && (rp.length == n || cp.charAt(n - 1) == '/' || rp.charAt(n) == '/')
  }

  /** Build store key for a Set-Cookie using request host / default-path when attrs are absent. */
  private[client] def cookieKeyFor(
    cookie: HttpCookie,
    requestHost: String,
    requestDefaultPath: String,
  ): CookieKey = {
    val domain = cookie.domain.map(normalizeCookieDomain).filter(_.nonEmpty).getOrElse(requestHost.toLowerCase)
    val path = effectiveCookiePath(cookie.path, requestDefaultPath)
    CookieKey(cookie.name, domain, path)
  }

  /**
   * RFC 6265 §5.4 host + path + Secure match for a stored cookie.
   * Host-only when `cookie.domain` is empty (exact match on key.domain);
   * otherwise domain-match using key.domain (§5.1.3).
   * Secure cookies are only included for secure request schemes (`https`, `wss`).
   */
  private[client] def cookieMatches(
    key: CookieKey,
    cookie: HttpCookie,
    requestHost: String,
    requestPath: String,
    requestScheme: String,
  ): Boolean = {
    val hostOnly = cookie.domain.forall(_.isEmpty)
    val domainOk =
      if (hostOnly) key.domain.equalsIgnoreCase(requestHost)
      else cookieDomainMatches(requestHost, key.domain)
    val secureOk = !cookie.secure || isSecureRequestScheme(requestScheme)
    domainOk && cookiePathMatches(requestPath, key.path) && secureOk
  }

  /** Schemes over which cookies with the `Secure` attribute may be sent (RFC 6265 §5.4). */
  private[client] def isSecureRequestScheme(scheme: String): Boolean =
    scheme.equalsIgnoreCase("https") || scheme.equalsIgnoreCase("wss")

  /**
   * Content-specific request headers that must be removed when a redirect changes the method
   * to GET or HEAD (RFC 9110 §15.4 step 5).
   */
  private[client] val contentHeaderNames: Set[String] = Set(
    "content-encoding",
    "content-language",
    "content-location",
    "content-type",
    "content-length",
    "digest",
    "last-modified",
  )

  private[client] def isContentHeader(h: HttpHeader): Boolean =
    contentHeaderNames.contains(h.lowercaseName)

  /** Headers to send when following a redirect.
    *
    * @param dropContentHeaders when true (method becomes GET/HEAD), strips content-related headers
    *                           per RFC 9110 §15.4: `Content-Encoding`, `Content-Language`,
    *                           `Content-Location`, `Content-Type`, `Content-Length`, `Digest`,
    *                           `Last-Modified`
    * On a different origin (scheme/host/port), also strips `Authorization`, `Cookie`, and `Host`
    * so credentials are not leaked cross-origin; cookies are re-applied from the jar for the new URI.
    */
  def redirectRequestHeaders(
    fromUri: Uri,
    toUri: Uri,
    headers: iSeq[HttpHeader],
    dropContentHeaders: Boolean = true,
  ): iSeq[HttpHeader] = {
    val originFiltered =
      if (isSameOrigin(fromUri, toUri)) headers
      else headers.filterNot(h => h.is("authorization") || h.is("cookie") || h.is("host"))
    if (dropContentHeaders) originFiltered.filterNot(isContentHeader)
    else originFiltered
  }
}
