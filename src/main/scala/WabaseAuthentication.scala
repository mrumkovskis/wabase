package org.wabase

import io.bullet.borer.{Codec, Decoder, Json}
import io.bullet.borer.derivation.MapBasedCodecs._
import ResultEncoder._
import JsonEncoder._
import org.apache.pekko.http.scaladsl.server.directives.AuthenticationDirective
import io.bullet.borer.compat.pekko._
import org.apache.pekko.http.scaladsl.model.{HttpRequest, HttpResponse}
import org.apache.pekko.http.scaladsl.model.headers.`User-Agent`
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

  def extractUserAgent(req: HttpRequest): Option[String] = {
    WabaseService.optionalHttpHeaderValuePF(req) { case ua: `User-Agent` => ua.value() }
  }

  def authenticateUser(req: HttpRequest): WabaseUser = ???
  def setSessionCookie(req: HttpRequest, resp: HttpResponse): HttpResponse = ???

  override def signInUser: AuthenticationDirective[WabaseUser] = ???
  override protected def execution: Execution = ???
}
