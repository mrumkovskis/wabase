package org.wabase

import org.apache.pekko.http.scaladsl.model.StatusCodes.{BadRequest, Forbidden, InternalServerError, NotFound, Unauthorized, UnprocessableContent}
import org.apache.pekko.http.scaladsl.model.{ContentTypes, EntityStreamSizeException, HttpEntity, HttpResponse, MediaTypes, StatusCodes, Uri}
import org.mojoz.metadata.ViewDef
import org.mojoz.querease.{ValidationException, ValidationResult, ValidationMessage}
import org.tresql.MissingBindVariableException
import org.wabase.AppServiceBase.AppExceptionHandler.PostgresTimeoutExceptionHandler
import org.wabase.AppServiceBase.AppExceptionHandler.PostgresTimeoutExceptionHandler.{TimeoutFriendlyMessage, TimeoutSignature}
import org.wabase.audit.HiddenValues
import org.wabase.handlers.CSRFException

import java.sql.SQLException
import java.util.Locale
import scala.concurrent.Future

object WabaseErrorHandler {
  private val dbConstraintMessageBuilder = DbConstraintMessage.PostgreSqlConstraintMessageBuilder
  /** log the raw (length-capped, NOT redacted) request body on error - dev troubleshooting only */
  private val logRawRequestBody      = config.getBoolean("app.error-handler.log-raw-request-body")
  /** return internal exception messages (e.g. missing bind variable) to the client */
  private val exposeInternalMessages = config.getBoolean("app.error-handler.expose-internal-messages")
  private val MaxRawPayloadChars     = 8192
  def errorHandler(ctx: WabaseRequestContext): WabaseService.ErrorHandler = {
    def debug(msg: String, e: Throwable) = ctx.logger.debug(s"[${ctxDebugInfo(ctx)}] $msg".trim, e)
    def applicationLocale = I18nService.applicationLocale(handlers.RequestHandlers.extractState(ctx))
    def friendlyConstraintErrorMessageResponse(exception: Throwable, sqlCause: SQLException, viewDefOpt: Option[ViewDef], tableName: String) = {
      import ctx.wabase.qe.tableMetadata
      dbConstraintMessageBuilder.friendlyMessageAndDetails(exception, sqlCause, viewDefOpt, tableName, tableMetadata.tableDefOption) match {
        case (friendlyMessage, details) =>
          val locale: Locale = applicationLocale
          val translated = ctx.wabase.translate(friendlyMessage, details)(locale)
          debug(badRequestMsg(exception.getMessage, ctx.req.entity), exception)
          HttpResponse(BadRequest, entity = translated)
      }
    }
    val eh: PartialFunction[Throwable, HttpResponse] = {
      case e: HttpException =>
        debug(e.getMessage, e)
        HttpResponse(status = e.status, entity = e.getMessage)
      case e: AuthenticationException =>
        debug(e.getMessage, e.getCause)
        HttpResponse(status = Unauthorized)
      case e: AuthorizationException =>
        debug(e.getMessage, e)
        HttpResponse(status = Forbidden)
      case e: EntityStreamSizeException => HttpResponse(status = StatusCodes.ContentTooLarge,
        entity = s"Content too large: actual size - ${e.actualSize.getOrElse("<unknown>")}, limit - ${e.limit}")
      case e: UnprocessableEntityException =>
        debug(badRequestMsg(e.getMessage, ctx.req.entity), e)
        HttpResponse(UnprocessableContent, entity = e.getMessage)
      case e: MissingBindVariableException =>
        debug(badRequestMsg(e.getMessage, ctx.req.entity), e)
        internalBadRequestResponse(e) // detail (internal query var names) logged server-side, not returned to client
      case e: QuereaseEnvException =>
        debug(badRequestMsg(e.getMessage, ctx.req.entity), e)
        internalBadRequestResponse(e) // detail (internal query/env structure) logged server-side, not returned to client
      case e: org.mojoz.querease.ViewNotFoundException =>
        HttpResponse(NotFound, entity = e.getMessage)
      case _: org.mojoz.querease.NotFoundException =>
        HttpResponse(NotFound)
      case e: BusinessException =>
        debug(badRequestMsg(e.getMessage, ctx.req.entity), e)
        HttpResponse(BadRequest, entity = e.getMessage)
      case e: ValidationException =>
        debug(badRequestMsg(e.getMessage, ctx.req.entity), e)
        if (e.details != null && e.details.nonEmpty) {
          import io.bullet.borer._, io.bullet.borer.derivation.MapBasedCodecs._, ResultEncoder._, JsonEncoder._
          implicit val vm_enc = deriveEncoder[ValidationMessage]
          implicit val vr_enc = deriveEncoder[ValidationResult]
          HttpResponse(BadRequest,
            entity = HttpEntity(ContentTypes.`application/json`, Json.encode(e.details).toUtf8String))
        } else HttpResponse(BadRequest, entity = e.getMessage)
      case e: CSRFException =>
        val msg = s"[${ctxDebugInfo(ctx)}] ${e.toString}".trim
        if  (ctx.logger.underlying.isDebugEnabled)
             ctx.logger.info(msg, e)
        else ctx.logger.info(msg)
        HttpResponse(StatusCodes.BadRequest)
      case e: org.postgresql.util.PSQLException if e.getMessage.startsWith(TimeoutSignature) =>
        val msg = s"[${ctxDebugInfo(ctx)}] JDBC timeout, statement cancelled"
        if  (ctx.logger.underlying.isDebugEnabled)
             ctx.logger.error(msg, e)
        else ctx.logger.error(msg)
        HttpResponse(InternalServerError,
          entity = ctx.wabase.translate(TimeoutFriendlyMessage)(applicationLocale))
      case e: SQLException if dbConstraintMessageBuilder.nameAndViolation(e)._1 != null =>
        val viewDefOpt = ctx.wabase.qe.viewDefOption(ctx.viewName)
        val tableName  = viewDefOpt.map(_.table).orNull
        friendlyConstraintErrorMessageResponse(e, e, viewDefOpt, tableName)
      case util.control.NonFatal(e) if dbConstraintMessageBuilder.nameAndViolation(dbConstraintMessageBuilder.getSqlCauseAndContext(e)._1)._1 != null =>
        dbConstraintMessageBuilder.getSqlCauseAndContext(e) match {
          case (sqe, ce) =>
            val viewDefOpt = ctx.wabase.qe.viewDefOption(ctx.viewName)
            val tableName =
              Option(ce).map(_.name).filter(_ != null)
                .orElse(viewDefOpt.map(_.table))
                .orNull
            friendlyConstraintErrorMessageResponse(e, sqe, viewDefOpt, tableName)
        }
    }
    eh.andThen(Future.successful(_)) orElse {
      case e: org.tresql.TresqlException if e.getCause.isInstanceOf[org.postgresql.util.PSQLException] &&
        e.getCause.getMessage.startsWith(PostgresTimeoutExceptionHandler.TimeoutSignature) =>
        WabaseService.errorHandler(ctx)(e.getCause)
      case e: QuereaseActionException =>
        (WabaseService.errorHandler(ctx) orElse { case _ =>
          ctx.logger.error(s"[${ctxDebugInfo(ctx)}] ${e.getMessage}".trim, e.getCause)
          Future.successful(HttpResponse(status = StatusCodes.InternalServerError))
        }:WabaseService.ErrorHandler)(e.getCause)
    }
  }

  private def redactValue(name: String, value: Any): String = HiddenValues.encode(name -> value)

  /** Best-effort redaction of a request body for logging. Structured bodies (json / form-urlencoded)
    * have top-level secret-looking values masked (per app.hidden-values.names); other or unparseable
    * bodies are logged as content-type + size, unless app.error-handler.log-raw-request-body is set.
    * Note: only top-level keys are matched - secrets nested deeper are not redacted. */
  private def redactedPayload(content: HttpEntity): String = content match {
    case s: HttpEntity.Strict =>
      def structured(pairs: => Seq[(String, Any)]): String =
        try pairs.map { case (k, v) => s"$k=${redactValue(k, v)}" }.mkString("{", ", ", "}")
        catch { case util.control.NonFatal(_) => rawOrSummary(s) }
      s.contentType.mediaType match {
        case MediaTypes.`application/json` =>
          structured(CborOrJsonAnyValueDecoder.decodeToMap(s.data).toSeq)
        case MediaTypes.`application/x-www-form-urlencoded` =>
          structured(Uri.Query(s.data.utf8String).toSeq)
        case _ => rawOrSummary(s)
      }
    case _ => "<not available>"
  }

  private def rawOrSummary(s: HttpEntity.Strict): String =
    if (logRawRequestBody) s.data.utf8String.take(MaxRawPayloadChars)
    else s"<${s.contentType}, ${s.data.length} bytes>"

  private def internalBadRequestResponse(e: Throwable): HttpResponse =
    if (exposeInternalMessages) HttpResponse(BadRequest, entity = e.getMessage)
    else HttpResponse(BadRequest)

  def badRequestMsg(msg: String, content: HttpEntity): String =
    s"$msg Payload: ${redactedPayload(content)}"

  /** Redacts values of secret-looking named query parameters (per app.hidden-values.names).
    * The wabase key-in-query form (?/key/parts) carries no named params and is left as-is. */
  private def redactedQueryString(uri: Uri): String = uri.rawQueryString match {
    case None                             => ""
    case Some(raw) if raw.startsWith("/") => "?" + raw
    case Some(_)                          =>
      try "?" + uri.query().map { case (k, v) => s"$k=${redactValue(k, v)}" }.mkString("&")
      catch { case util.control.NonFatal(_) => "?<unparseable query>" }
  }

  def ctxDebugInfo(ctx: WabaseRequestContext): String =
    Option(ctx.req).map(r => s"${r.method.value} ${r.uri.path}${redactedQueryString(r.uri)}").getOrElse("no req ctx")
}
