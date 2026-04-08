package org.wabase.handlers

import org.apache.pekko.http.scaladsl.model.{HttpEntity, HttpMethods, HttpResponse, StatusCode, StatusCodes}
import org.wabase._
import org.wabase.WabaseService.okResponse

import scala.concurrent.{ExecutionContext, Future}

object ActionHandlers {

  def doActionWithKeyToPath(view_action: String, reqCtx: WabaseRequestContext): Future[HttpResponse] = {
    WabaseService.doAction(view_action, WabaseService.keyFromQueryToPath(reqCtx))
  }

  def doRequest(handlerName: String, ctx: WabaseRequestContext): Future[HttpResponse] = {
    val (cn, fn) = classNameFunctionName(handlerName)
    val key = WabaseService.key(ctx.req.uri.path, handlerName)
    WabaseService.buildRequestHandler(cn, fn, Nil, null)(ctx.copy(key = key))
  }

  def startJob(jobName: String, ctx: WabaseRequestContext): Future[HttpResponse] = {
    implicit val ec: ExecutionContext = ctx.as.dispatcher
    for {
      params <- if (ctx.req.method == HttpMethods.POST) {
        (if (ctx.req.entity.isKnownEmpty()) Future.successful(Map[String, Any]()) else WabaseService.toMapEntityDecoder(ctx))
          .map(_ ++ ctx.req.uri.query().toMap)
      } else Future.failed(HttpException(StatusCodes.MethodNotAllowed))
      result <- AppQuerease.startJob(jobName, params)(ctx.as, ctx.as.dispatcher, ctx.wabase.qio)
    } yield {
      val code: StatusCode = result
      code match {
        case StatusCodes.OK => okResponse
        case StatusCodes.Conflict =>
          HttpResponse(status = code, entity = HttpEntity(s"Job '$jobName' is already running."))
        case StatusCodes.NotFound =>
          HttpResponse(status = code, entity = HttpEntity(s"Job not found: '${ctx.wabase.sanitizedViewName(jobName)}'"))
        case x => HttpResponse(status = x)
      }
    }
  }
}
