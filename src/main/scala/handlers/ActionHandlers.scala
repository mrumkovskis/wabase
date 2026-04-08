package org.wabase.handlers

import org.apache.pekko.http.scaladsl.marshalling.Marshal
import org.apache.pekko.http.scaladsl.model.{HttpEntity, HttpMethods, HttpResponse, StatusCode, StatusCodes}
import org.wabase._
import org.wabase.WabaseService.{addResultFilter, error, toMapForViewEntityDecoder, withReqMaxContentSize, withReqTimeout}
import org.wabase.handlers.ResponseHandlers.okResponse
import org.wabase.handlers.RequestHandlers.viewActionKey

import scala.concurrent.{ExecutionContext, Future}

object ActionHandlers {

  def doActionWithKeyToPath(view_action: String, reqCtx: WabaseRequestContext): Future[HttpResponse] = {
    doAction(view_action, RequestHandlers.keyFromQueryToPath(reqCtx))
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

  def doAction(view_action: String, reqCtx: WabaseRequestContext): Future[HttpResponse] = {
    def extractParams(ctx: WabaseRequestContext) = {
      import ctx._
      AppServiceBase.filterParams(
        wabase.qe.metadataConventions, AppServiceBase.NamesForInts, AppServiceBase.escapeReflectedXss
      )(WabaseService.parameterMultiMap(req))
    }
    def dwa(ctx: WabaseRequestContext, params: Map[String, Any]) = {
      if (ctx.viewName == null || !ctx.wabase.qe.nameToViewDef.contains(ctx.viewName))
        if (ctx.viewName != null)
          error(StatusCodes.NotFound, s"View '${ctx.wabase.sanitizedViewName(ctx.viewName)}' not found!")
        else error(StatusCodes.NotFound, s"View not found!")
      else {
        val updatedCtx = withReqTimeout(withReqMaxContentSize(ctx))
        import updatedCtx._
        implicit val ec: ExecutionContext = as.dispatcher
        toMapForViewEntityDecoder(updatedCtx).flatMap { values =>
          updatedCtx.wabase.app.doAction(
            actionName = action,
            viewName = viewName,
            keyValues = updatedCtx.key,
            params = params,
            values = values,
            resultFilter = resultFilter,
          )(updatedCtx)
        }.flatMap { result =>
          Marshal(result).toResponseFor(updatedCtx.req)(wabase.toResponseWabaseResultMarshaller, ec)
        }
      }
    }
    val ctxWithView = if (reqCtx.viewName == null) viewActionKey(view_action, reqCtx) else reqCtx
    val ctxWithViewAndState =
      if (ctxWithView.applicationState == null)
        ctxWithView.copy(applicationState = handlers.RequestHandlers.extractState(ctxWithView))
      else ctxWithView
    val params = extractParams(ctxWithViewAndState)
    val ctxWithViewAndStateAndFilter =
      if (ctxWithViewAndState.resultFilter == null)
        addResultFilter(ctxWithViewAndState, params)
      else ctxWithViewAndState
    dwa(ctxWithViewAndStateAndFilter, params)
  }
}
