package org.wabase

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive0, Directive1}
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.headers.{Host, HttpCookie, HttpOrigin, HttpOriginRange, Origin, Referer, SameSite}

class CSRFException(message: String) extends Exception(message)

trait CSRFDefence { this: AppConfig =>

  lazy val CSRFCookieName = "XSRF-TOKEN"
  lazy val CSRFHeaderName = "X-XSRF-TOKEN"

  def csrfCheck = checkSameOrigin & checkCSRFToken

  protected val targetOrigin =
    if (appConfig.hasPath("host")) Uri(appConfig.getString("host")) match {
      case u => HttpOrigin(u.scheme, Host(u.authority.host, u.authority.port))
    } else null

  private val schemas = List("http", "https")
  private def fullOriginList(h: Host) =
    schemas
    .filterNot {
      case "https" => h.port == 80
      case "http"  => h.port == 443
      case _ => false
    }.map { HttpOrigin(_, h) }

  protected def extractTargetOrigins: Directive1[List[HttpOrigin]] =
    if (targetOrigin != null) provide(List(targetOrigin))
    else
      (headerValuePF[List[HttpOrigin]]({ case h: Host => fullOriginList(h) }) |
        headerValueByName("X-Forwarded-Host")
          .map(Host.parseFromValueString)
          .map {
            case Right(h) => fullOriginList(h)
            case Left(_) => Nil
          }).recover(_ => error(s"Either 'Host' or 'X-Forwarded-Host' http header must be set."))

  protected def normalizePort(origin: HttpOrigin) = {
    if (origin.host.port == 0)
      origin.scheme match {
        case "http" => origin.copy(host = origin.host.copy(port = 80))
        case "https" => origin.copy(host = origin.host.copy(port = 443))
        case _ => origin
      }
    else origin
  }

  def checkSameOrigin: Directive0 =
    (extractRequest & extractTargetOrigins).tflatMap { case (request, targetOriginsRaw) =>
      val targetOrigins = targetOriginsRaw.map(normalizePort)
      optionalHeaderValuePF[Seq[HttpOrigin]]({
        case Origin(origins) =>
          origins.map(normalizePort)
        case Referer(uri) =>
          List(HttpOrigin(uri.scheme, Host(uri.authority.host, uri.authority.port)))
            .map(normalizePort)
      }).map(_.getOrElse(error("Either 'Origin' or 'Referer' http header must be set.")))
        .flatMap { sourceOrigins =>
          if (sourceOrigins.exists(HttpOriginRange(targetOrigins: _*).matches)) pass
          else {
            val msg =
              "Cross Site Request Forgery (CSRF) - " +
                s"""Source origins: ${sourceOrigins.mkString(", ")}, """ +
                s"""target origins: ${targetOrigins.mkString(", ")}, """ +
                s"""uri: ${request.uri}"""
            throw new CSRFException(msg)
          }
        }
    }

  def checkCSRFToken: Directive0 = (
    optionalCookie(CSRFCookieName).map(_.getOrElse(error(s"$CSRFCookieName cookie not found."))) &
      optionalHeaderValueByName(CSRFHeaderName).map(_.getOrElse(error(s"$CSRFHeaderName header not found.")))
    ).tflatMap {
      case (cookie, header) =>
        if (cookie.value == header) pass
        else error(s"$CSRFCookieName cookie value does not match $CSRFHeaderName header value - " +
          s"${cookie.value} != $header")
    }

  protected def csrfCookieTransformer(cookie: HttpCookie): HttpCookie = cookie

  def setCSRFCookie: Directive0 = setCookie(
    csrfCookieTransformer(
      HttpCookie(
        CSRFCookieName,
        value = Authentication.Crypto.uniqueSessionId,
        path = Some("/"),
        secure = Authentication.Crypto.secureCookies
      ).withSameSite(SameSite.Lax)))

  def deleteCSRFCookie: Directive0 = deleteCookie(CSRFCookieName)
  private def error(msg: String) = throw new CSRFException(msg)
}
