package org.wabase.handlers

import io.bullet.borer.Json
import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, StatusCodes}
import org.apache.pekko.util.ByteString
import org.wabase._
import org.wabase.WabaseService.RequestHandler

import scala.concurrent.Future

object DeferredHandlers {

  private def user_(ctx: WabaseRequestContext): String =
    Option(ctx.user).map(_.name).filter(_ != null).getOrElse("(anonymous)")

  /** Enable deferred processing for handler. */
  def maybeDeferred(innerHandler: RequestHandler): RequestHandler = ctx => {
    if (WabaseDeferredControl.isDeferredPath(ctx.req.uri) || WabaseDeferredControl.hasDeferredHeader(ctx.req)) {
      doDeferred(innerHandler)(ctx)
    } else innerHandler(ctx)
  }

  def doDeferred(handler: RequestHandler): RequestHandler = ctx => {
    require(ctx.deferred.deferredControl != null, "Cannot do deferred request. Deferred module not initialized.")
    val timeout = WabaseDeferredControl.extractTimeout(ctx, ctx.req)
    val dctx = ctx.copy(queryTimeout = timeout)
    val user = user_(dctx)
    val hash = DeferredControl.requestHash(user, dctx.req, WabaseAuthentication.removeSessionInfoFromRequest)
    val deferredCtx = DeferredControl.DeferredContext(user, hash, dctx, handler)
    ServerNotifications
      .publishMessages(EventMessage(DeferredControl.DeferredRequestArrived(dctx.deferred.deferredModule), deferredCtx))
    Future.successful(HttpResponse(
      status = StatusCodes.Accepted,
      entity = HttpEntity.Strict(ContentTypes.`application/json`,
        ByteString(Json.encode(Map("deferred" -> hash)).toUtf8String))
    ))
  }

  /** Get deferred request result */
  def deferredResult(deferred_id: String, ctx: WabaseRequestContext): Future[HttpResponse] = {
    require(ctx.deferred.deferredControl != null,
      s"Cannot retrieve deferred result $deferred_id, deferred module not initialized.")
    Future.successful(ctx.deferred.deferredControl.deferredResult(deferred_id, user_(ctx)))
  }
}
