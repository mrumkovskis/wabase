package org.wabase

import io.bullet.borer.{Codec, Decoder, Json}
import io.bullet.borer.derivation.MapBasedCodecs._
import ResultEncoder._
import JsonEncoder._
import io.bullet.borer.compat.pekko._
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.RemoteAddress.Unknown
import org.apache.pekko.http.scaladsl.model.{AttributeKey, AttributeKeys, HttpEntity, HttpRequest, HttpResponse, RemoteAddress}
import org.apache.pekko.http.scaladsl.server.directives.AuthenticationDirective
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshal
import org.apache.pekko.http.scaladsl.model.headers.{
  BasicHttpCredentials, HttpCookie, HttpCredentials, OAuth2BearerToken,
  SameSite, `Remote-Address`, `User-Agent`, `X-Forwarded-For`, `X-Real-Ip`
}
import org.apache.pekko.util.ByteString
import org.wabase.WabaseUnmarshallers.mapUnmarshaller

import scala.concurrent.{Future, ExecutionContext}
import scala.util.Try


class AuthenticationException(msg: String) extends Exception(msg)

object WabaseAuthentication extends Authentication[WabaseUser] {

  type Session = Authentication.Session[WabaseUser]

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
    val ip = remoteAddressToString(extractClientIP(req))
    val userAgent = extractUserAgent(req)
    val expirationTime = currentTime + sessionTimeOut
    encryptSession(encodeSession(Authentication.Session(user, ip, expirationTime, userAgent)))
  }

  /* Request mapper */
  def appAuthenticate(req: HttpRequest): WabaseUser = {
    val (session, ip, userAgent) = (extractSession(req), extractClientIP(req), extractUserAgent(req))
    session.filter(validateSession(_, ip, userAgent))
      .map(_.user)
      .getOrElse(throw new AuthenticationException("Unauthorized"))
  }

  /* Response transformer */
  // cannot name setSessionCookie because setSessionCookie from super trait appears from reflection to be member of this object
  def setAppSessionCookie(req: HttpRequest, user: WabaseUser, resp: HttpResponse): HttpResponse = {
    WabaseService.setCookie(resp)(sessionCookie(encryptedSession(req, mergeReqRespUserData(user, resp))))
  }

  def session(req: HttpRequest): Option[String] = WabaseService.optionalCookie(req)(SessionCookieName)

  def setAnonSessionCookie(resp: HttpResponse): HttpResponse = {
    WabaseService.setCookie(resp)(sessionCookie(sessionId))
  }

  def sessionId: String = java.util.UUID.randomUUID().toString

  def optUserFromRespAttributes(resp: HttpResponse): Option[WabaseUser] =
    resp.attribute(AttributeKey[WabaseUser](WabaseService.WabaseUserAttributeName))

  def mergeReqRespUserData(reqUser: WabaseUser, resp: HttpResponse) =
    optUserFromRespAttributes(resp).map { u =>
      val (rp, cp) = u.properties.partition(_._2 == null)
      // remove null values, update rest
      WabaseUser(reqUser.properties -- rp.keys ++ cp)
    }.getOrElse(reqUser)

  def sessionCookie(encryptedSession: String): HttpCookie =
    HttpCookie(
      SessionCookieName,
      value = encryptedSession,
      path = Some("/"),
      httpOnly= httpOnlyCookies,
      secure = secureCookies
    ).withSameSite(SameSite.Lax)

  /* Response transformer */
  def removeAppSessionCookie(resp: HttpResponse): HttpResponse =
    WabaseService.deleteCookie(resp)(SessionCookieName, path = "/")

  def httpCredentials: HttpRequest => Option[HttpCredentials] = WabaseService.optionalHttpHeaderValuePF(_) {
    case org.apache.pekko.http.scaladsl.model.headers.Authorization(credentials) => credentials
  }

  def extractBasicHttpCredentials(req: HttpRequest): WabaseUser = WabaseService.optionalHttpHeaderValuePF(req) {
    case org.apache.pekko.http.scaladsl.model.headers.Authorization(BasicHttpCredentials(usr, pwd)) =>
      WabaseUser(Map(WabaseAppConfig.UserCredentialsParameterName -> Map("username" -> usr, "password" -> pwd)))
  }.getOrElse(throw new AuthenticationException("Credentials required"))

  def extractFormDataCredentials(entity: HttpEntity)(implicit ec: ExecutionContext, as: ActorSystem): Future[WabaseUser] =
    Unmarshal(entity).to[Map[String, Any]].map { formData =>
      WabaseUser(Map(WabaseAppConfig.UserCredentialsParameterName -> formData))
    }

  lazy val jwtDecoder = new JwtDecoder(config.getConfig("jwt-decoder"))
  def extractJwtTokenCredentials(req: HttpRequest): WabaseUser = WabaseService.optionalHttpHeaderValuePF(req) {
    case org.apache.pekko.http.scaladsl.model.headers.Authorization(OAuth2BearerToken(jwtToken: String)) =>
      WabaseUser(Map(WabaseAppConfig.UserCredentialsParameterName -> jwtDecoder.decodeToMap(jwtToken)))
  }.getOrElse(throw new AuthenticationException("Credentials required"))

  override def signInUser: AuthenticationDirective[WabaseUser] = ???
}
