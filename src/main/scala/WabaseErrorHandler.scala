package org.wabase

import com.typesafe.scalalogging.Logger
import org.apache.pekko.http.scaladsl.model.StatusCodes.{BadRequest, InternalServerError, NotFound, UnprocessableContent}
import org.apache.pekko.http.scaladsl.model.{EntityStreamSizeException, HttpResponse, StatusCodes}
import org.mojoz.querease.{ValidationException, ValidationResult}
import org.slf4j.LoggerFactory
import org.tresql.MissingBindVariableException
import org.wabase.AppServiceBase.AppExceptionHandler.PostgresTimeoutExceptionHandler
import org.wabase.AppServiceBase.AppExceptionHandler.PostgresTimeoutExceptionHandler.{TimeoutFriendlyMessage, TimeoutSignature}

import scala.concurrent.Future

trait WabaseErrorHandler {
  def errorHandler(ctx: WabaseRequestContext): PartialFunction[Throwable, Future[HttpResponse]]
}

object WabaseErrorHandler extends WabaseErrorHandler {
  val logger = Logger(LoggerFactory.getLogger("org.wabase.error"))
  def errorHandlerPF(ctx: WabaseRequestContext): PartialFunction[Throwable, HttpResponse] = {
    case e: EntityStreamSizeException => HttpResponse(status = StatusCodes.ContentTooLarge,
      entity = s"Content too large: actual size - ${e.actualSize.getOrElse("<unknown>")}, limit - ${e.limit}")
    case e: UnprocessableEntityException =>
      logger.trace(e.getMessage, e)
      HttpResponse(UnprocessableContent, entity = e.getMessage)
    case e: MissingBindVariableException =>
      logger.debug(e.getMessage, e)
      HttpResponse(BadRequest, entity = e.getMessage)
    case e: QuereaseEnvException =>
      logger.debug(e.getMessage, e)
      HttpResponse(BadRequest, entity = e.getMessage)
    case e: org.mojoz.querease.ViewNotFoundException =>
      HttpResponse(NotFound, entity = e.getMessage)
    case _: org.mojoz.querease.NotFoundException =>
      HttpResponse(NotFound)
    case e: BusinessException =>
      logger.trace(e.getMessage, e)
      HttpResponse(BadRequest, entity = e.getMessage)
    case e: ValidationException =>
      logger.trace(e.getMessage, e)
      if (e.details != null && e.details.nonEmpty) {
        import io.bullet.borer._, io.bullet.borer.derivation.MapBasedCodecs._, ResultEncoder._, JsonEncoder._
        implicit val enc = deriveEncoder[ValidationResult]
        HttpResponse(BadRequest, entity = Json.encode(e.details).toUtf8String)
      } else HttpResponse(BadRequest, entity = e.getMessage)
    case e: CSRFException =>
      val logger = LoggerFactory.getLogger("org.wabase.csrf")
      logger.info(e.toString)
      HttpResponse(StatusCodes.BadRequest)
    case e: org.postgresql.util.PSQLException if e.getMessage.startsWith(TimeoutSignature) =>
      val timeoutLogger = LoggerFactory.getLogger("JdbcTimeoutLogger")
      val user = Option(ctx.user).map(_.toString).orNull
      val state = ctx.applicationState.state.map{ case (k,v) => s"$k = $v" }.mkString("{", ", ", "}")
      val msg = s"JDBC timeout, statement cancelled - ${ctx.req.method} ${ctx.req.uri}, state - $state, user - $user"
      timeoutLogger.error(msg)
      HttpResponse(InternalServerError,
        entity = ctx.wabase.translate(TimeoutFriendlyMessage)(I18nService.applicationLocale(ctx.applicationState)))
    case e: org.tresql.TresqlException if e.getCause.isInstanceOf[org.postgresql.util.PSQLException] &&
      e.getCause.getMessage == PostgresTimeoutExceptionHandler.TimeoutSignature =>
      errorHandlerPF(ctx)(e.getCause)
  }

  def errorHandler(ctx: WabaseRequestContext): PartialFunction[Throwable, Future[HttpResponse]] =
    errorHandlerPF(ctx).andThen(Future.successful(_))
}
