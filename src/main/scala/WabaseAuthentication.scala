package org.wabase

import io.bullet.borer.{Codec, Decoder, Json}
import io.bullet.borer.derivation.MapBasedCodecs._
import ResultEncoder._
import JsonEncoder._
import io.bullet.borer.compat.pekko._
import org.apache.pekko.http.scaladsl.model.RemoteAddress.Unknown
import org.apache.pekko.http.scaladsl.model.{AttributeKey, AttributeKeys, HttpRequest, HttpResponse, RemoteAddress}
import org.apache.pekko.http.scaladsl.server.directives.AuthenticationDirective
import org.apache.pekko.http.scaladsl.model.headers.{HttpCookie, HttpCredentials, SameSite, `Remote-Address`, `User-Agent`, `X-Forwarded-For`, `X-Real-Ip`}
import org.apache.pekko.util.ByteString

import scala.util.control.NonFatal


class AuthenticationException(msg: String, cause: Throwable = null) extends Exception(msg, cause)
class AuthorizationException(msg: String) extends Exception(msg)

object WabaseAuthentication extends Authentication[WabaseUser] {

  type Session = Authentication.Session[WabaseUser]
  val WabaseUserAttributeName = "wabase-user"

  implicit val userCodec: Codec[WabaseUser] = {
    implicit val userMapDecoder: Decoder[Map[String, Any]] =
      CborOrJsonAnyValueDecoder.toMapDecoder(() => Map[String, Any]())
    Codec.bimap[Map[String, Any], WabaseUser](
      _.properties - WabaseAppConfig.UserCredentialsParameterName,
      WabaseUser(_)
    )
  }
  implicit val sessionCodec: Codec[Session] = deriveCodec[Session]

  override def userInfo(implicit user: WabaseUser): String = Json.encode(user).toUtf8String
  override def encodeSession(session: Session): String = Json.encode(session).toUtf8String
  override def decodeSession(session: String): Session = Json.decode(ByteString(session)).to[Session].value

  def userPrincipal(user: WabaseUser) = userInfo(user)

  def extractSession(req: HttpRequest): Option[Session] = {
    WabaseService.optionalCookie(req)(SessionCookieName).map { sessionCookie =>
      try decodeSession(decryptSession(sessionCookie)) catch {
        case NonFatal(e) => throw new AuthenticationException("Unable to decode session", e)
      }
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
    val ip = remoteAddressToString(extractClientIP(req))
    val userAgent = extractUserAgent(req)
    val expirationTime = currentTime + sessionTimeOut
    encryptSession(encodeSession(Authentication.Session(user, ip, expirationTime, userAgent)))
  }

  def session(req: HttpRequest): Option[String] = WabaseService.optionalCookie(req)(SessionCookieName)

  def sessionId: String = java.util.UUID.randomUUID().toString

  def optUserFromRespAttributes(resp: HttpResponse): Option[WabaseUser] =
    resp.attribute(AttributeKey[WabaseUser](WabaseUserAttributeName))

  def mergeReqRespUserData(reqUser: WabaseUser, resp: HttpResponse) =
    optUserFromRespAttributes(resp).map { u =>
      val (rp, cp) = u.properties.partition(_._2 == null)
      // remove null values, update rest
      WabaseUser(Option(reqUser).map(_.properties).getOrElse(Map()) -- rp.keys ++ cp)
    }.getOrElse(reqUser)

  def sessionCookie(encryptedSession: String, domain: String = null, path: String = SessionCookiePath): HttpCookie =
    HttpCookie(
      SessionCookieName,
      value = encryptedSession,
      domain = Option(domain),
      path = Option(path),
      httpOnly= httpOnlyCookies,
      secure = secureCookies
    ).withSameSite(SameSite.Lax)

  def httpCredentials: HttpRequest => Option[HttpCredentials] = WabaseService.optionalHttpHeaderValuePF(_) {
    case org.apache.pekko.http.scaladsl.model.headers.Authorization(credentials) => credentials
  }

  lazy val jwtDecoder = new JwtDecoder(config.getConfig("jwt-decoder"))

  override def signInUser: AuthenticationDirective[WabaseUser] = ???
}
