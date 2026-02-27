package org.wabase.handlers

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.{ContentType, ContentTypes, HttpEntity, HttpResponse, StatusCodes}
import org.wabase.{WabaseRequestContext, WabaseService}

import scala.concurrent.{ExecutionContext, Future}

trait FileHandlers {
  def fileUpload(name: String)(ctx: WabaseRequestContext)(
    implicit as: ActorSystem, ec: ExecutionContext): Future[Map[String, Any]] =
    fileUploadUsing(name, null)(ctx)

  def fileUploadUsing(name: String, fsName: String)(ctx: WabaseRequestContext)(
    implicit as: ActorSystem, ec: ExecutionContext): Future[Map[String, Any]] = {
    val fn = Option(name).filter(_.nonEmpty).getOrElse("file")
    val fs = ctx.wabase.fileStreamers.fs(fsName)
    require(fs != null, s"File streamer '$fsName' not found")
    val ct = ctx.req.entity.contentType.toString()
    ctx.req.entity.dataBytes.runWith(fs.fileSink(fn, ct))
      .map(_.toMap)
  }

  def fileDownload(id: Long, hash: String)(ctx: WabaseRequestContext): HttpResponse =
    fileDownloadUsing(id, hash, null)(ctx)

  def fileDownloadUsing(id: Long, hash: String, fsName: String)(ctx: WabaseRequestContext): HttpResponse = {
    val fs = ctx.wabase.fileStreamers.fs(fsName)
    require(fs != null, s"File streamer '$fsName' not found")
    fs.getFileInfo(id, hash)
      .map { fi =>
        val ct = ContentType.parse(fi.content_type).toOption.getOrElse(ContentTypes.NoContentType)
        HttpResponse(
          status = StatusCodes.OK,
          entity = HttpEntity(ct, fi.source)
        )
      }
      .getOrElse(WabaseService.notFound)
  }
}

object FileHandlers extends FileHandlers
