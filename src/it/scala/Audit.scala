package wabase.app


import io.bullet.borer.compat.pekko._
import io.bullet.borer.derivation.MapBasedCodecs._
import io.bullet.borer.{Encoder, Json}
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.{AttributeKey, HttpEntity, HttpHeader, HttpResponse}
import org.apache.pekko.stream.scaladsl._
import org.apache.pekko.util.ByteString
import org.mojoz.querease.{QuereaseIo, SaveMethod}
import org.wabase.WabaseAppConfig.DefaultCp
import org.wabase.WabaseService.RequestHandler
import org.wabase.ds.PoolName
import org.wabase.{BufferedAudit, CborOrJsonAnyValueDecoder, DbAccess, DefaultAppQuerease, DefaultAppQuereaseIo, Loggable, TresqlResourcesConf, WabaseRequestContext, WabaseServer}

import java.nio.file.Files
import java.time.Instant
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

object Audit extends Loggable {

  /* Key to store request start time for auditing. If not set, audit method entry time will be used */
  val AuditTimestampKey = AttributeKey[Instant]("audit-timestamp")

  /* Key to store captured entity for auditing. */
  val AuditEntityKey = AttributeKey[HttpEntity.Strict]("audit-entity")

  implicit val auditPoolName: PoolName = PoolName("wabase_it_audit_cp")

  // Ensure AuditRecord (req + resp + etc) fits within bufferedAudit.reader.maxRecordSize!
  private val maxContentSizeToAudit: Long = 256 * 1024

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

  private def largeContentReplacementForAuditing(contentLength: Long): ByteString =
    ByteString(s"[large content: $contentLength bytes]")

  private def contentForAuditing(entity: HttpEntity): String = entity match {
    case strict: HttpEntity.Strict =>
      if  (strict.contentLength > maxContentSizeToAudit)
           largeContentReplacementForAuditing(strict.contentLength).utf8String
      else strict.data.utf8String
    case other => s"[STREAM? ${other.getClass.getName}]"
  }

  def createAuditRecord(ctx: WabaseRequestContext, response: HttpResponse): AuditRecord = {
    val ts = ctx.req.getAttribute(AuditTimestampKey).orElse(Instant.now)
    AuditRecord(
      request_time =
        Instant.now.toString,
      request   =
        RequestAudit(
          uri     = ctx.req.uri.toString,
          method  = ctx.req.method.name,
          headers = ctx.req.headers.filter(_.renderInRequests()).map(renderHeader),
          ct_type = ctx.req.entity.contentType.toString,
          content = contentForAuditing(
            Option(ctx.req.getAttribute(AuditEntityKey).orElse(null)).getOrElse(ctx.req.entity)),
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
          content = contentForAuditing(
            Option(response.getAttribute(AuditEntityKey).orElse(null)).getOrElse(response.entity)),
          protocol= response.protocol.value,
        ),
    )
  }

  def bufferedAuditWriteRecord(record: AuditRecord) = {
    val serialized = Json.encode(record).to[ByteString].result
    bufferedAudit.writer.writeRecord(serialized)
  }

  private def shouldCaptureAndPromise(contentLengthOption: Option[Long]): (Boolean, Promise[ByteString]) = {
    val promise = Promise[ByteString]()
    contentLengthOption match {
      case Some(len) if len > maxContentSizeToAudit =>
        promise.success(largeContentReplacementForAuditing(len))
        (false, promise)
      case Some(0) =>
        promise.success(ByteString.empty)
        (false, promise)
      case _ => (true, promise)
    }
  }

  private def createCaptureSink(promise: Promise[ByteString])(implicit ec: ExecutionContext): Sink[ByteString, NotUsed] = {
    Sink.fold[(Long, ByteString), ByteString]((0L, ByteString.empty)) { case ((size, content), bs) =>
      val added = bs.length.toLong
      val newSize = size + added
      if (size >= maxContentSizeToAudit) {
        (newSize, content)
      } else {
        val remain = maxContentSizeToAudit - size
        if (added <= remain) {
          (newSize, content ++ bs)
        } else {
          (newSize, content ++ bs.take(remain.toInt))
        }
      }
    }.mapMaterializedValue { fut =>
      fut.foreach { case (size, content) =>
        val bs = if (size > maxContentSizeToAudit) largeContentReplacementForAuditing(size) else content
        promise.success(bs)
      }
      NotUsed
    }
  }

  def audit(ctx: WabaseRequestContext, response: HttpResponse): Unit = {
    val record = createAuditRecord(ctx, response)
    bufferedAuditWriteRecord(record)
  }

  def audit(innerHandler: RequestHandler): RequestHandler = ctx => {
    if (!ctx.req.getAttribute(AuditTimestampKey).isPresent) {
      ctx.req.addAttribute(AuditTimestampKey, Instant.now)
    }
    val (attachReqCapture, reqPromise) = shouldCaptureAndPromise(ctx.req.entity.contentLengthOption)

    val reqDataBytes = if (attachReqCapture) {
      val sink = createCaptureSink(reqPromise)
      ctx.req.entity.dataBytes.alsoTo(sink)
    } else {
      ctx.req.entity.dataBytes
    }

    val modRequest = ctx.req.withEntity(
      ctx.req.entity.contentLengthOption match {
        case Some(0)    => ctx.req.entity
        case Some(len)  => HttpEntity.Default(ctx.req.entity.contentType, len, reqDataBytes)
        case None       => HttpEntity        (ctx.req.entity.contentType, reqDataBytes)
      }
    )

    val innerCtx = ctx.copy(req = modRequest)

    innerHandler(innerCtx).transformWith {
      case Success(response) =>
        val (attachRespCapture, respPromise) = shouldCaptureAndPromise(response.entity.contentLengthOption)

        val respDataBytes = if (attachRespCapture) {
          val sink = createCaptureSink(respPromise)
          response.entity.dataBytes.alsoTo(sink)
        } else {
          response.entity.dataBytes
        }

        val modResponse = response.withEntity(
          response.entity.contentLengthOption match {
            case Some(0)    => response.entity
            case Some(len)  => HttpEntity.Default(response.entity.contentType, len, respDataBytes)
            case None       => HttpEntity        (response.entity.contentType, respDataBytes)
          }
        )

        val auditF = for {
          reqC <- reqPromise.future
          respC <- respPromise.future
        } yield {
          ctx.req.addAttribute(AuditEntityKey, HttpEntity.Strict(ctx.req.entity.contentType, reqC))
          response.addAttribute(AuditEntityKey, HttpEntity.Strict(response.entity.contentType, respC))
          audit(ctx, response)
        }

        auditF.failed.foreach { ex =>
          logger.error("Failed to audit", ex)
        }

        Future.successful(modResponse)

      case Failure(ex) =>
        // Do not audit here. Use error handler to produce response and call audit from there.
        reqPromise.future.transform {
          case Success(reqC) =>
            ctx.req.addAttribute(AuditEntityKey, HttpEntity.Strict(ctx.req.entity.contentType, reqC))
            throw ex
          case Failure(ex2) =>
            logger.error("Failed to capture request entity for auditing", ex2)
            throw ex
        }
    }
  }

  def handleAuditing(innerHandler: RequestHandler): RequestHandler = audit(innerHandler)
}
