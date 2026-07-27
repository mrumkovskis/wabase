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
  /** Set pekko.http.server.remote-address-attribute = on instead of pekko.http.server.remote-address-header = on
    *
    * The returned address is used both to mint and (in [[Authentication.validateSession]]) to validate the
    * session's IP pin, so it must be trustworthy. `X-Forwarded-For` (first entry) and `X-Real-Ip` are honored
    * unconditionally: this is only safe behind a trusted reverse proxy that overwrites those headers - if clients
    * can reach the app directly, these headers are spoofable (an attacker can only pin their own session, not
    * hijack another's, but the pin becomes meaningless). If no forwarded header is present and
    * remote-address-attribute is not enabled, this falls back to `Unknown`, so every session pins to the same
    * "Unknown" value and the IP check silently becomes a no-op. */
  @annotation.nowarn("msg=use remote-address-attribute instead")
  def extractClientIP(req: HttpRequest): RemoteAddress = {
    WabaseService.optionalHttpHeaderValuePF(req) {
      case `X-Forwarded-For`(Seq(address, _*)) => address
      case `X-Real-Ip`(address) => address
      // Remote-Address header have been deprecated since Akka HTTP 10.2.0.
      // Set pekko.http.server.remote-address-attribute = on instead of pekko.http.server.remote-address-header = on
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

  /** Merges user data a handler attached to the response (via [[WabaseUserAttributeName]]) back into the
    * session user before re-encrypting the session cookie. This is how a handler mutates session state.
    * Null-value semantics are load-bearing: a key with a `null` value in the response user *removes* that key
    * from the session, a non-null value *updates* it, and keys absent from the response user are left unchanged.
    * If the response carries no user attribute, the request user is returned unchanged. */
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
