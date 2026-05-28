package org.wabase.handlers

import org.apache.pekko.http.scaladsl.model.{ContentType, HttpEntity, HttpResponse, StatusCodes}
import org.apache.pekko.http.scaladsl.server.directives.ContentTypeResolver
import org.apache.pekko.http.scaladsl.server.directives.FileAndResourceDirectives.ResourceFile
import org.apache.pekko.stream.scaladsl.StreamConverters
import org.apache.pekko.util.ByteString
import org.wabase._
import org.wabase.handlers.CacheConditionHandlers._
import org.wabase.WabaseService.RequestHandler

import scala.concurrent.Future

object ResponseHandlers {

  val okResponse: HttpResponse = HttpResponse(StatusCodes.OK)

  def statusResponse(statusCode: Int): HttpResponse = HttpResponse(statusCode)

  def statusAndTextResponse(statusCode: Int, text: String): HttpResponse = HttpResponse(statusCode, entity = text)

  def responseWithContentType(statusCode: Int, contentType: String, content: String): HttpResponse = {
    val ent = ContentType.parse(contentType)
      .toOption.getOrElse(sys.error(s"Invalid content type: $contentType")) match {
      case ct: ContentType.NonBinary => HttpEntity(contentType = ct, string = content)
      case ct => HttpEntity(contentType = ct, data = ByteString(content))
    }
    HttpResponse(statusCode, entity = ent)
  }

  def getFromResource(resourcesRootPath: String, resourcePathAndName: String): RequestHandler = {
    val resourceName = s"${resourcesRootPath}${resourcePathAndName}"
    val contentType = ContentTypeResolver.Default(resourceName)
    if (!resourceName.endsWith("/"))
        Option(getClass.getClassLoader.getResource(resourceName)).flatMap(ResourceFile.apply) match {
          case Some(ResourceFile(url, length, lastModified)) =>
            val (eTagOpt, lastModifiedOpt) = WabaseService.conditionsFor(length, lastModified)
            val inner: RequestHandler = _ => {
              if (length > 0)
                Future.successful(
                  HttpResponse(entity =
                    HttpEntity.Default(contentType, length,
                      StreamConverters.fromInputStream(() => url.openStream()))
                  )
                )
              else Future.successful(HttpResponse(entity = HttpEntity.Empty))
            }
            if (eTagOpt.nonEmpty || lastModifiedOpt.nonEmpty) conditional(eTagOpt, lastModifiedOpt, inner)
            else inner
          case _ => (_: WabaseRequestContext) => Future.successful(HttpResponse(StatusCodes.NotFound))
        }
    else (_: WabaseRequestContext) => Future.successful(HttpResponse(StatusCodes.NotFound))
  }
}
