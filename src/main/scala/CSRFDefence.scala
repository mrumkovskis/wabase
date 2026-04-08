package org.wabase

import org.apache.pekko.http.scaladsl.model.Uri
import org.wabase.handlers.CSRFHandlers._
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.{Directive0, Directive1}
import org.apache.pekko.http.scaladsl.model.headers.{Host, HttpCookie, HttpOrigin, HttpOriginRange, Origin, Referer, SameSite}

trait CSRFDefence { this: AppConfig =>
  protected val targetOrigin: HttpOrigin =
    if (appConfig.hasPath("host")) Uri(appConfig.getString("host")) match {
      case u => HttpOrigin(u.scheme, Host(u.authority.host, u.authority.port))
    } else null

  def csrfCheck = checkSameOrigin & checkCSRFToken

  protected def extractTargetOrigins: Directive1[List[HttpOrigin]] =
    if (targetOrigin != null) provide(List(targetOrigin))
    else extractUri.flatMap { uri =>
      (headerValuePF[List[HttpOrigin]]({ case h: Host => fullOriginList(h) }) |
        headerValueByName("X-Forwarded-Host")
          .map(Host.parseFromValueString)
          .map {
            case Right(h) => fullOriginList(h)
            case Left(_) => Nil
          }).recover(_ => error(s"Either 'Host' or 'X-Forwarded-Host' http header must be set.", uri))
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
      }).map(_.getOrElse(error("Either 'Origin' or 'Referer' http header must be set.", request.uri)))
        .flatMap { sourceOrigins =>
          if (sourceOrigins.exists(HttpOriginRange(targetOrigins: _*).matches)) pass
          else {
            val msg =
              "Cross Site Request Forgery (CSRF) - " +
                s"""Source origins: ${sourceOrigins.mkString(", ")}, """ +
                s"""target origins: ${targetOrigins.mkString(", ")}, """ +
                s"""uri: ${request.uri}"""
            error(msg, request.uri)
          }
        }
    }

  def checkCSRFToken: Directive0 = extractUri.flatMap { uri =>
    (optionalCookie(CSRFCookieName).map(_.getOrElse(error(s"$CSRFCookieName cookie not found.", uri))) &
        optionalHeaderValueByName(CSRFHeaderName).map(_.getOrElse(error(s"$CSRFHeaderName header not found.", uri)))
      ).tflatMap {
      case (cookie, header) =>
        if (cookie.value == header) pass
        else error(s"$CSRFCookieName cookie value does not match $CSRFHeaderName header value - " +
          s"${cookie.value} != $header", uri)
    }
  }

  protected def csrfCookieTransformer(cookie: HttpCookie): HttpCookie =
    org.wabase.handlers.CSRFHandlers.csrfCookieTransformer(cookie)

  def setCSRFCookie: Directive0 = setCookie(
    csrfCookieTransformer(
      HttpCookie(
        CSRFCookieName,
        value = Authentication.Crypto.uniqueSessionId,
        path = Some("/"),
        secure = Authentication.Crypto.secureCookies
      ).withSameSite(SameSite.Lax)))

  def deleteCSRFCookie: Directive0 = deleteCookie(CSRFCookieName)
}
