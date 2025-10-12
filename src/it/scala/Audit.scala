package wabase.app


import io.bullet.borer.{Encoder, Json}
import io.bullet.borer.compat.pekko._
import io.bullet.borer.derivation.MapBasedCodecs._
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.{HttpEntity, HttpHeader, HttpResponse}
import org.apache.pekko.util.ByteString
import org.mojoz.querease.{QuereaseIo, SaveMethod}
import org.wabase.WabaseAppConfig.DefaultCp
import org.wabase.WabaseService.RequestHandler
import org.wabase.{BufferedAudit, CborOrJsonAnyValueDecoder, DbAccess, DefaultAppQuerease, DefaultAppQuereaseIo,
  Loggable, PoolName, TresqlResourcesConf, WabaseRequestContext, WabaseServer}

import java.nio.file.Files
import java.time.Instant
import scala.concurrent.Future
import scala.util.{Failure, Success}

object Audit extends Loggable {
  implicit val auditPoolName: PoolName = PoolName("wabase_it_audit_cp")
  case class RequestAudit(
    uri:      String,
    method:   String,
    headers:  Seq[String],
    ct_type:  String,
    content:  String,
    protocol: String,
  )
  case class ResponseAudit(
    code:     Int,
    headers:  Seq[String],
    ct_type:  String,
    content:  String,
    protocol: String,
  )
  case class UserAudit(
    name:     String,
  )
  case class AuditRecord(
    request_time: String,
    request:      RequestAudit,
    user:         UserAudit,
    response:     ResponseAudit,
  )

  implicit val requestAuditEncoder:  Encoder[RequestAudit]  = deriveEncoder[RequestAudit]
  implicit val responseAuditEncoder: Encoder[ResponseAudit] = deriveEncoder[ResponseAudit]
  implicit val userAuditEncoder:     Encoder[UserAudit]     = deriveEncoder[UserAudit]
  implicit val auditRecordEncoder:   Encoder[AuditRecord]   = deriveEncoder[AuditRecord]

  implicit val system: ActorSystem = WabaseServer.app.system
  implicit val ec: scala.concurrent.ExecutionContext = system.dispatcher
  private  val auditSaveView = DefaultAppQuerease.viewDef("audit")
  private  val resourcesTemplate =
    TresqlResourcesConf.tresqlResourcesTemplate(TresqlResourcesConf.confs, DefaultAppQuerease.tresqlMetadata)
  private implicit val qio: QuereaseIo[_] = DefaultAppQuereaseIo

  def saveAuditRecordsBatchToDatabase(records: Seq[ByteString]): Future[Unit] = {
    try {
      val decoded: Seq[Map[String, Any]] = records.map(CborOrJsonAnyValueDecoder.decodeToMap(_))
      DbAccess.newTransaction(auditPoolName, DefaultCp, resourcesTemplate) { implicit resources =>
        decoded.foreach { map =>
          val compatibleMap = DefaultAppQuerease.toCompatibleMap(map, auditSaveView)
          DefaultAppQuerease.save(auditSaveView, compatibleMap, null, SaveMethod.Insert, null, null)
        }
      }
      logger.info(s"Audit batch saved, ${records.size} record(s)")
      Future.successful(())
    } catch {
      case util.control.NonFatal(ex) =>
        logger.info("Failed to save audit batch", ex)
        Future.failed(ex)
    }
  }

  private val bufferedAudit = BufferedAudit(saveAuditRecordsBatchToDatabase)
  Files.createDirectories(bufferedAudit.writer.rootPath)

  private def renderHeader(header: HttpHeader): String =
    s"${header.name}: ${header.value}"

  def createAuditRecord(ts: Instant, ctx: WabaseRequestContext, response: HttpResponse): AuditRecord = {
    AuditRecord(
      request_time =
        Instant.now.toString,
      request   =
        RequestAudit(
          uri     = ctx.req.uri.toString,
          method  = ctx.req.method.name,
          headers = ctx.req.headers.filter(_.renderInRequests()).map(renderHeader),
          ct_type = ctx.req.entity.contentType.toString,
          content = ctx.req.entity match {
            case strict: HttpEntity.Strict => strict.data.utf8String
            case other => s"[STREAM? ${other.getClass.getName}]"
          },
          protocol= ctx.req.protocol.value,
        ),
      user =
        UserAudit(
          name    = Option(ctx.user).map(_.name).getOrElse(""),
        ),
      response =
        ResponseAudit(
          code    = response.status.intValue,
          headers = response.headers.filter(_.renderInResponses()).map(renderHeader),
          ct_type = response.entity.contentType.toString,
          content = response.entity match {
            case strict: HttpEntity.Strict => strict.data.utf8String
            case other => s"[STREAM? ${other.getClass.getName}]"
          },
          protocol= response.protocol.value,
        ),
    )
  }

  def bufferedAuditWriteRecord(record: AuditRecord) = {
    val serialized = Json.encode(record).to[ByteString].result
    bufferedAudit.writer.writeRecord(serialized)
  }

  def audit(innerHandler: RequestHandler): RequestHandler = ctx => {
    val ts = Instant.now
    val innerCtx = ctx
    innerHandler(innerCtx).andThen {
      case Success(response) =>
        val record = createAuditRecord(ts, ctx, response)
        bufferedAuditWriteRecord(record)
        response
      case Failure(ex) =>
        // Do nothing here. Use error handler to produce response and call audit from there.
    }(ctx.as.dispatcher)
  }
}
