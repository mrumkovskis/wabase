package org.wabase

import org.apache.pekko.http.scaladsl.model.StatusCodes.{BadRequest, Forbidden, InternalServerError, NotFound, Unauthorized, UnprocessableContent}
import org.apache.pekko.http.scaladsl.model.{EntityStreamSizeException, HttpEntity, HttpResponse, StatusCodes}
import org.mojoz.metadata.ViewDef
import org.mojoz.querease.{ValidationException, ValidationResult}
import org.tresql.MissingBindVariableException
import org.wabase.AppServiceBase.AppExceptionHandler.PostgresTimeoutExceptionHandler
import org.wabase.AppServiceBase.AppExceptionHandler.PostgresTimeoutExceptionHandler.{TimeoutFriendlyMessage, TimeoutSignature}

import java.sql.SQLException
import java.util.Locale
import scala.concurrent.Future

object WabaseErrorHandler {
  private val dbConstraintMessageBuilder = DbConstraintMessage.PostgreSqlConstraintMessageBuilder
  def errorHandler(ctx: WabaseRequestContext): WabaseService.ErrorHandler = {
    def debug(msg: String, e: Throwable = null) = {
      val m = s"[${ctxDebugInfo(ctx)}] $msg"
      if (e == null) ctx.logger.debug(m) else ctx.logger.debug(m, e)
    }
    def friendlyConstraintErrorMessageResponse(exception: Throwable, sqlCause: SQLException, viewDefOpt: Option[ViewDef], tableName: String) = {
      import ctx.wabase.qe.tableMetadata
      dbConstraintMessageBuilder.friendlyMessageAndDetails(exception, sqlCause, viewDefOpt, tableName, tableMetadata.tableDefOption) match {
        case (friendlyMessage, details) =>
          val locale: Locale = I18nService.applicationLocale(ctx.applicationState)
          val translated = ctx.wabase.translate(friendlyMessage, details)(locale)
          debug(badRequestMsg(exception.getMessage, ctx.req.entity), exception)
          HttpResponse(BadRequest, entity = translated)
      }
    }
    val eh: PartialFunction[Throwable, HttpResponse] = {
      case e: HttpException =>
        debug(e.getMessage)
        HttpResponse(status = e.status, entity = e.getMessage)
      case e: AuthenticationException =>
        debug(e.getMessage)
        HttpResponse(status = Unauthorized)
      case e: AuthorizationException =>
        debug(e.getMessage)
        HttpResponse(status = Forbidden)
      case e: EntityStreamSizeException => HttpResponse(status = StatusCodes.ContentTooLarge,
        entity = s"Content too large: actual size - ${e.actualSize.getOrElse("<unknown>")}, limit - ${e.limit}")
      case e: UnprocessableEntityException =>
        debug(badRequestMsg(e.getMessage, ctx.req.entity), e)
        HttpResponse(UnprocessableContent, entity = e.getMessage)
      case e: MissingBindVariableException =>
        debug(badRequestMsg(e.getMessage, ctx.req.entity), e)
        HttpResponse(BadRequest, entity = e.getMessage)
      case e: QuereaseEnvException =>
        debug(badRequestMsg(e.getMessage, ctx.req.entity), e)
        HttpResponse(BadRequest, entity = e.getMessage)
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
          implicit val enc = deriveEncoder[ValidationResult]
          HttpResponse(BadRequest, entity = Json.encode(e.details).toUtf8String)
        } else HttpResponse(BadRequest, entity = e.getMessage)
      case e: CSRFException =>
        ctx.logger.info(e.toString)
        HttpResponse(StatusCodes.BadRequest)
      case e: org.postgresql.util.PSQLException if e.getMessage.startsWith(TimeoutSignature) =>
        val user = Option(ctx.user).map(_.toString).orNull
        val state = ctx.applicationState.state.map{ case (k,v) => s"$k = $v" }.mkString("{", ", ", "}")
        val msg = s"JDBC timeout, statement cancelled - ${ctx.req.method} ${ctx.req.uri}, state - $state, user - $user"
        ctx.logger.error(msg)
        HttpResponse(InternalServerError,
          entity = ctx.wabase.translate(TimeoutFriendlyMessage)(I18nService.applicationLocale(ctx.applicationState)))
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
        e.getCause.getMessage == PostgresTimeoutExceptionHandler.TimeoutSignature =>
        WabaseService.errorHandler(ctx)(e.getCause)
      case e: QuereaseActionException =>
        (WabaseService.errorHandler(ctx) orElse { case _ =>
          ctx.logger.error(s"[${WabaseErrorHandler.ctxDebugInfo(ctx)}] ${e.getMessage}", e.getCause)
          Future.successful(HttpResponse(status = StatusCodes.InternalServerError))
        }:WabaseService.ErrorHandler)(e.getCause)
    }
  }

  def badRequestMsg(msg: String, content: HttpEntity): String = {
    val payload = content match {
      case s: HttpEntity.Strict => s.data.utf8String
      case x => "<not available>"
    }
    s"""$msg Payload: "$payload""""
  }

  def ctxDebugInfo(ctx: WabaseRequestContext): String = s"${ctx.req.uri.toString()}"
}
