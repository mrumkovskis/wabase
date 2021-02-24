package org.wabase

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive0, Directive1}
import akka.http.scaladsl.server.{AuthenticationFailedRejection, InvalidOriginRejection}
import akka.http.scaladsl.server.AuthenticationFailedRejection._
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.headers.{Host, HttpCookie, HttpOrigin, HttpOriginRange, Origin, Referer, SameSite}

trait CSRFDefence { this: AppConfig with Authentication[_] with Loggable =>

  lazy val CSRFCookieName = "XSRF-TOKEN"
  lazy val CSRFHeaderName = "X-XSRF-TOKEN"

  def csrfCheck = checkSameOrigin & checkCSFRToken

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
      headerValuePF[List[HttpOrigin]]({ case h: Host => fullOriginList(h) }) |
      headerValueByName("X-Forwarded-Host")
      .map(Host.parseFromValueString(_))
      .map {
        case Right(h) => fullOriginList(h)
        case Left(_) => Nil
      }

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
    extractTargetOrigins.flatMap { targetOriginsRaw =>
      val targetOrigins = targetOriginsRaw.map(normalizePort)
      headerValuePF[Seq[HttpOrigin]]({
        case Origin(origins) =>
          origins.map(normalizePort)
        case Referer(uri) =>
          List(HttpOrigin(uri.scheme, Host(uri.authority.host, uri.authority.port)))
            .map(normalizePort)
      }).flatMap { sourceOrigins =>
        if (sourceOrigins.exists(HttpOriginRange(targetOrigins: _*).matches)) pass
        else {
          logger.error(s"CSRF rejection, source origins: $sourceOrigins, target origins: $targetOrigins")
          reject(InvalidOriginRejection(targetOrigins))
        }
      }
    }

  def checkCSFRToken: Directive0 = (cookie(CSRFCookieName) & headerValueByName(CSRFHeaderName))
    .tflatMap {
      case (cookie, header) =>
        if (cookie.value == header) pass
        else reject(AuthenticationFailedRejection(
          CredentialsRejected,
          AppDefaultChallenge))
    }

  private def hash(string: String) = org.apache.commons.codec.digest.DigestUtils.sha256Hex(
    string + String.valueOf(Authentication.Crypto.randomBytes(8)))

  protected def csfrCookieTransformer(cookie: HttpCookie): HttpCookie = cookie

  def setCSFRCookie: Directive0 = setCookie(
    csfrCookieTransformer(
      HttpCookie(
        CSRFCookieName,
        value = uniqueSessionId,
        path = Some("/"),
        secure = secureCookies
      ).withSameSite(SameSite.Lax)))

  def deleteCSRFCookie: Directive0 = deleteCookie(CSRFCookieName)
}
