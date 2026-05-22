package org.wabase.handlers

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import org.apache.pekko.http.scaladsl.model.headers.{BasicHttpCredentials, OAuth2BearerToken}
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshal
import org.wabase.WabaseAuthentication.{extractClientIP, extractSession, extractUserAgent, sessionCookie, sessionId, validateSession}
import org.wabase._
import org.wabase.WabaseService.RequestHandler
import org.wabase.WabaseUnmarshallers.mapUnmarshaller

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Try}
import scala.util.control.NonFatal

object AuthenticationHandlers {

  def checkRole(role: String)(ctx: WabaseRequestContext): Future[WabaseRequestContext] = {
    import ctx._
    implicit val ec: ExecutionContext = as.dispatcher
    if (user == null) Future.failed(HttpException(StatusCodes.Unauthorized))
    else wabase.hasRole(user, Set(role))(AuthContext(as, req, queryTimeout, logger)).map {
      case true  => ctx
      case false => throw HttpException(StatusCodes.Forbidden)
    }
  }

  def appAuthenticate(req: HttpRequest): WabaseUser = {
    val (session, ip, userAgent) = (extractSession(req), extractClientIP(req), extractUserAgent(req))
    session.filter(validateSession(_, ip, userAgent))
      .map(_.user)
      .getOrElse(throw new AuthenticationException("Unauthorized"))
  }

  def appAuthenticateOpt(ctx: WabaseRequestContext): WabaseRequestContext = {
    def extractSessionOpt(req: HttpRequest) =
      Try(extractSession(req)).recoverWith {
        case NonFatal(e) =>
          ctx.logger.debug("Error decoding session:", e)
          Failure(e)
      }.toOption.flatten
    import ctx.req
    val (session, ip, userAgent) = (extractSessionOpt(req), extractClientIP(req), extractUserAgent(req))
    session.filter(validateSession(_, ip, userAgent))
      .map(session => ctx.copy(user = session.user))
      .getOrElse(ctx)
  }

  def setAnonSessionCookie(resp: HttpResponse): HttpResponse = {
    WabaseService.setCookie(resp)(sessionCookie(sessionId))
  }

  def authenticatePlusSession(innerHandler: RequestHandler): RequestHandler =
    authenticateDomainAndPathPlusSession(null, WabaseAuthentication.SessionCookiePath)(innerHandler)

  def authenticatePlusSessionOpt(innerHandler: RequestHandler): RequestHandler =
    authenticateDomainAndPathPlusSessionOpt(null, WabaseAuthentication.SessionCookiePath)(innerHandler)

  def authenticateDomainAndPathPlusSession(domain: String, path: String)(
    innerHandler: RequestHandler): RequestHandler = ctx => {
    val user = appAuthenticate(ctx.req)
    innerHandler(ctx.copy(user = user))
      .map(setDomainAndPathSessionCookieOpt(domain, path)(ctx.req, user, _))(ctx.as.dispatcher)
  }

  def authenticateDomainAndPathPlusSessionOpt(domain: String, path: String)(
    innerHandler: RequestHandler): RequestHandler = ctx => {
    val ctxWithUser = appAuthenticateOpt(ctx)
    innerHandler(ctxWithUser)
      .map(setDomainAndPathSessionCookieOpt(domain, path)(ctx.req, ctxWithUser.user, _))(ctxWithUser.as.dispatcher)
  }

  // cannot name setSessionCookie because setSessionCookie from super trait appears from reflection to be member of this object
  def setAppSessionCookie(req: HttpRequest, user: WabaseUser, resp: HttpResponse): HttpResponse =
    setDomainAndPathSessionCookie(null, WabaseAuthentication.SessionCookiePath)(req, user, resp)

  def setAppSessionCookieOpt(req: HttpRequest, user: WabaseUser, resp: HttpResponse): HttpResponse =
    setDomainAndPathSessionCookieOpt(null, WabaseAuthentication.SessionCookiePath)(req, user, resp)

  def setDomainAndPathSessionCookie(domain: String, path: String)(
    req: HttpRequest, user: WabaseUser, resp: HttpResponse): HttpResponse = {
    if (resp.status.isSuccess)
      WabaseService.setCookie(resp)(
        WabaseAuthentication.sessionCookie(
          WabaseAuthentication.encryptedSession(req, WabaseAuthentication.mergeReqRespUserData(user, resp)),
          domain, path)
      )
    else resp
  }

  def setDomainAndPathSessionCookieOpt(domain: String, path: String)(
    req: HttpRequest, user: WabaseUser, resp: HttpResponse): HttpResponse = {
    if (resp.status.isSuccess && user != null)
      WabaseService.setCookie(resp)(
        WabaseAuthentication.sessionCookie(
          WabaseAuthentication.encryptedSession(req, WabaseAuthentication.mergeReqRespUserData(user, resp)),
          domain, path)
      )
    else resp
  }

  def removeAppSessionCookie(resp: HttpResponse): HttpResponse =
    WabaseService.deleteCookie(resp)(WabaseAuthentication.SessionCookieName, path = WabaseAuthentication.SessionCookiePath)

  def extractBasicHttpCredentials(req: HttpRequest): WabaseUser = WabaseService.optionalHttpHeaderValuePF(req) {
    case org.apache.pekko.http.scaladsl.model.headers.Authorization(BasicHttpCredentials(usr, pwd)) =>
      WabaseUser(Map(WabaseAppConfig.UserCredentialsParameterName -> Map("username" -> usr, "password" -> pwd)))
  }.getOrElse(throw new AuthenticationException("Credentials required"))

  def extractFormDataCredentials(req: HttpRequest)(implicit ec: ExecutionContext, as: ActorSystem): Future[WabaseUser] =
    Unmarshal(req.entity).to[Map[String, Any]].map { formData =>
      WabaseUser(Map(WabaseAppConfig.UserCredentialsParameterName -> formData))
    }

  def extractJwtTokenCredentials(req: HttpRequest): WabaseUser = WabaseService.optionalHttpHeaderValuePF(req) {
    case org.apache.pekko.http.scaladsl.model.headers.Authorization(OAuth2BearerToken(jwtToken: String)) =>
      WabaseUser(Map(WabaseAppConfig.UserCredentialsParameterName -> WabaseAuthentication.jwtDecoder.decodeToMap(jwtToken)))
  }.getOrElse(throw new AuthenticationException("Credentials required"))
}
