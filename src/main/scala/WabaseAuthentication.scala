package org.wabase

import io.bullet.borer.{Codec, Decoder, Json}
import io.bullet.borer.derivation.MapBasedCodecs._
import ResultEncoder._
import JsonEncoder._
import org.apache.pekko.http.scaladsl.server.directives.AuthenticationDirective
import io.bullet.borer.compat.pekko._
import org.apache.pekko.http.scaladsl.model.RemoteAddress.Unknown
import org.apache.pekko.http.scaladsl.model.{AttributeKey, AttributeKeys, HttpRequest, HttpResponse, RemoteAddress}
import org.apache.pekko.http.scaladsl.model.headers.{HttpCookie, SameSite, `Remote-Address`, `User-Agent`, `X-Forwarded-For`, `X-Real-Ip`}
import org.apache.pekko.util.ByteString

import scala.util.Try


class AuthenticationException(msg: String) extends Exception(msg)

object WabaseAuthentication extends Authentication[WabaseUser] with Execution {

  type Session = Authentication.Session[WabaseUser]

  implicit val userCodec: Codec[WabaseUser] = {
    implicit val userMapDecoder: Decoder[Map[String, Any]] =
      new CborOrJsonAnyValueDecoder().toMapDecoder(() => Map[String, Any]())
    Codec.bimap[Map[String, Any], WabaseUser](_.properties, WabaseUser(_))
  }
  implicit val sessionCodec: Codec[Session] = deriveCodec[Session]

  override def userInfo(implicit user: WabaseUser): String = user.toString
  override def encodeSession(session: Session): String = Json.encode(session).toUtf8String
  override def decodeSession(session: String): Session = Json.decode(ByteString(session)).to[Session].value

  def extractSession(req: HttpRequest): Option[Session] = {
    WabaseService.optionalCookie(req)(SessionCookieName).flatMap { sessionCookie =>
      Try(decodeSession(decryptSession(sessionCookie))).toOption
    }
  }

  // code taken from extractClientIP directive
  def extractClientIP(req: HttpRequest): RemoteAddress = {
    WabaseService.optionalHttpHeaderValuePF(req) {
      case `X-Forwarded-For`(Seq(address, _*)) => address
      case `X-Real-Ip`(address) => address
      case `Remote-Address`(address) => address
    }.orElse(req.attribute(AttributeKeys.remoteAddress))
      .getOrElse(Unknown)
  }
  def extractUserAgent(req: HttpRequest): Option[String] = {
    WabaseService.optionalHttpHeaderValuePF(req) { case ua: `User-Agent` => ua.value() }
  }

  def encryptedSession(req: HttpRequest, user: WabaseUser): String = {
    if (user == null) throw new AuthenticationException(s"User not found in session")
    val ip = extractClientIP(req) match {
      case ra: RemoteAddress.IP => remoteAddressToString(ra)
      case RemoteAddress.Unknown => null
    }
    if (ip == null)
      throw new BusinessException(s"Client IP http header not found, ensure pekko.http.server.remote-address-header = on")
    val userAgent = extractUserAgent(req)
    val expirationTime = currentTime + sessionTimeOut
    encryptSession(encodeSession(Authentication.Session(user, ip, expirationTime, userAgent)))
  }

  /* Request mapper */
  def authenticate(req: HttpRequest): WabaseUser = {
    val (session, ip, userAgent) = (extractSession(req), extractClientIP(req), extractUserAgent(req))
    session.filter(validateSession(_, ip, userAgent))
      .map(_.user)
      .getOrElse(throw new AuthenticationException("Unauthorized"))
  }

  /* Response transformer */
  def setSessionCookie(req: HttpRequest, user: WabaseUser, resp: HttpResponse): HttpResponse = {
    // remove null values, update rest
    val usr = resp.attribute(AttributeKey[WabaseUser](WabaseService.WabaseUserAttributeName)).map { u =>
      val (rp, cp) = u.properties.partition(_._2 == null)
      WabaseUser(user.properties -- rp.keys ++ cp)
    }.getOrElse(user)
    val enc_session = encryptedSession(req, usr)
    WabaseService.setCookie(resp)(HttpCookie(
      SessionCookieName,
      value = enc_session,
      path = Some("/"),
      httpOnly= httpOnlyCookies,
      secure = secureCookies
    ).withSameSite(SameSite.Lax))
  }

  /* Response transformer */
  def removeSessionCookie(resp: HttpResponse): HttpResponse =
    WabaseService.deleteCookie(resp)(SessionCookieName, path = "/")

  override def signInUser: AuthenticationDirective[WabaseUser] = ???
  override protected def execution: Execution = ???
}
