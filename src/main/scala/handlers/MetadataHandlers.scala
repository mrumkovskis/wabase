package org.wabase.handlers

import org.apache.pekko.http.scaladsl.model.headers.EntityTag
import org.apache.pekko.http.scaladsl.model.{ContentTypes, DateTime, HttpEntity, HttpResponse}
import org.wabase._
import org.wabase.handlers.CacheConditionHandlers._
import org.wabase.WabaseService.RequestHandler

import scala.concurrent.{ExecutionContext, Future}

object MetadataHandlers {

  def api(ctx: WabaseRequestContext): Future[HttpResponse] = {
    import ctx._
    implicit val ec: ExecutionContext = as.dispatcher
    ctx.wabase._api(ctx.user)(AuthContext(as, req, queryTimeout, logger)).map { json =>
      HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, ResultEncoder.encodeAnyToJsonByteString(json)))
    }
  }

  def metadata(viewName: String, ctx: WabaseRequestContext): RequestHandler = {
    conditional(EntityTag(ctx.wabase.app.metadataVersionString), DateTime(ctx.wabase.app.startupTimeMillis), _ => {
      implicit val user:  WabaseUser       = ctx.user
      implicit val state: ApplicationState = ctx.applicationState
      import ctx.wabase
      val json = if (viewName == "*") wabase._apiMetadata else wabase._metadata(viewName)
      Future.successful(
        HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, ResultEncoder.encodeAnyToJsonByteString(json)))
      )
    })
  }

  def generateSwaggerJson(ctx: WabaseRequestContext): RequestHandler = {
    conditional(EntityTag(ctx.wabase.app.metadataVersionString), DateTime(ctx.wabase.app.startupTimeMillis), _ => {
      Future.successful {
        val generator = WabaseService.createSwaggerGenerator(ctx)
        HttpResponse(entity = HttpEntity(WabaseService.MediaTypes.`application/json`, generator.generateSwaggerJson))
      }
    })
  }

  def generateSwaggerYaml(ctx: WabaseRequestContext): RequestHandler = {
    conditional(EntityTag(ctx.wabase.app.metadataVersionString), DateTime(ctx.wabase.app.startupTimeMillis), _ => {
      Future.successful {
        val generator = WabaseService.createSwaggerGenerator(ctx)
        HttpResponse(entity = HttpEntity(WabaseService.MediaTypes.`application/yaml`, generator.generateSwaggerYaml))
      }
    })
  }
}
