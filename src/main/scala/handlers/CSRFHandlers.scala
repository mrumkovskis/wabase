package org.wabase.handlers

import org.apache.pekko.http.scaladsl.model.{HttpRequest, HttpResponse, Uri}
import org.apache.pekko.http.scaladsl.model.headers.{Host, HttpCookie, HttpOrigin, HttpOriginRange, Origin, Referer, SameSite}
import org.wabase.{AppConfig, Authentication, WabaseService, invokeFunction}

class CSRFException(message: String) extends Exception(message)

object CSRFHandlers extends AppConfig {

  val CSRFCookieName = "XSRF-TOKEN"
  val CSRFHeaderName = "X-XSRF-TOKEN"

  val targetOrigin: HttpOrigin =
    if (appConfig.hasPath("host")) Uri(appConfig.getString("host")) match {
      case u => HttpOrigin(u.scheme, Host(u.authority.host, u.authority.port))
    } else null

  def checkSameOriginForRequest(req: HttpRequest): HttpRequest = {
    val targetOrigins = Option(List(targetOrigin)).orElse {
        WabaseService.optionalHttpHeaderValueByName(req)("X-Forwarded-Host")
          .map(Host.parseFromValueString)
          .map(_.map(fullOriginList))
          .map(_.toOption.getOrElse(Nil))
      }
      .getOrElse(error(s"Either 'Host' or 'X-Forwarded-Host' http header must be set.", req.uri))
      .map(normalizePort)
    val sourceOrigins = WabaseService.optionalHttpHeaderValuePF(req)({
      case Origin(origins) =>
        origins.map(normalizePort)
      case Referer(uri) =>
        List(HttpOrigin(uri.scheme, Host(uri.authority.host, uri.authority.port)))
          .map(normalizePort)
    }).getOrElse(error("Either 'Origin' or 'Referer' http header must be set.", req.uri))
    if (sourceOrigins.exists(HttpOriginRange(targetOrigins: _*).matches)) req
    else {
      val msg =
        "Cross Site Request Forgery (CSRF) - " +
          s"""Source origins: ${sourceOrigins.mkString(", ")}, """ +
          s"""target origins: ${targetOrigins.mkString(", ")}, """ +
          s"""uri: ${req.uri}"""
      error(msg, req.uri)
    }
  }

  def checkCsrfToken(req: HttpRequest): HttpRequest = {
    val csrfCookie = WabaseService.optionalCookie(req)(CSRFCookieName)
      .getOrElse(error(s"$CSRFCookieName cookie not found.", req.uri))
    val csrfHeader = WabaseService.optionalHttpHeaderValueByName(req)(CSRFHeaderName)
      .getOrElse(error(s"$CSRFHeaderName header not found.", req.uri))
    if (csrfCookie == csrfHeader) req
    else error(s"$CSRFCookieName cookie value does not match $CSRFHeaderName header value - " +
      s"$csrfCookie != $csrfHeader", req.uri)
  }

  def setCsrfCookie(resp: HttpResponse): HttpResponse = {
    val cookie = csrfCookieTransformer(
      HttpCookie(
        CSRFCookieName,
        value = Authentication.Crypto.uniqueSessionId,
        path = Some("/"),
        secure = Authentication.Crypto.secureCookies
      ).withSameSite(SameSite.Lax))
    WabaseService.setCookie(resp)(cookie)
  }

  def deleteCsrfCookie(resp: HttpResponse): HttpResponse =
    WabaseService.deleteCookie(resp)(CSRFCookieName)

  private lazy val CsrfCookieTransfomer =
    if (appConfig.getIsNull("csrf.cookie-transformer")) null else appConfig.getString("csrf.cookie-transformer")
  def csrfCookieTransformer(cookie: HttpCookie): HttpCookie = {
    if (CsrfCookieTransfomer == null) cookie
    else invokeFunction(CsrfCookieTransfomer, Seq((classOf[HttpCookie], () => cookie)))(
      scala.concurrent.ExecutionContext.global).asInstanceOf[HttpCookie]
  }

  def normalizePort(origin: HttpOrigin): HttpOrigin = {
    if (origin.host.port == 0)
      origin.scheme match {
        case "http" => origin.copy(host = origin.host.copy(port = 80))
        case "https" => origin.copy(host = origin.host.copy(port = 443))
        case _ => origin
      }
    else origin
  }

  private val schemas = List("http", "https")
  def fullOriginList(h: Host): List[HttpOrigin] =
    schemas
      .filterNot {
        case "https" => h.port == 80
        case "http"  => h.port == 443
        case _ => false
      }.map { HttpOrigin(_, h) }

  def error(msg: String, uri: Uri): Nothing =
    throw new CSRFException(s"$msg (url - ${uri.withQuery(Uri.Query(Map[String, String]())).toString()})")
}
