package org.wabase

import org.apache.pekko.http.scaladsl.model.{EntityStreamSizeException, HttpResponse}
import org.mojoz.querease.ValidationException
import org.tresql.MissingBindVariableException
import org.wabase.AppServiceBase.AppExceptionHandler.PostgresTimeoutExceptionHandler
import org.wabase.AppServiceBase.AppExceptionHandler.PostgresTimeoutExceptionHandler.TimeoutSignature

import scala.concurrent.Future

object WabaseExceptionHandler {
  def exceptionHandlerPF(ctx: WabaseRequestContext): PartialFunction[Throwable, HttpResponse] = {
    case e: EntityStreamSizeException => null
    case e: UnprocessableEntityException => null
    case e: MissingBindVariableException => null
    case e: QuereaseEnvException => null
    case e: org.mojoz.querease.ViewNotFoundException => null
    case e: org.mojoz.querease.NotFoundException => null
    case e: BusinessException => null
    case e: ValidationException => null
    case e: CSRFException => null
    case e: org.postgresql.util.PSQLException if e.getMessage.startsWith(TimeoutSignature) => null
    case e: org.tresql.TresqlException if e.getCause.isInstanceOf[org.postgresql.util.PSQLException] &&
        e.getCause.getMessage == PostgresTimeoutExceptionHandler.TimeoutSignature => null
  }

  def exceptionHandlerPFF(ctx: WabaseRequestContext): PartialFunction[Throwable, Future[HttpResponse]] =
    exceptionHandlerPF(ctx).andThen(Future.successful(_))

  /* Wabase default error handler */
  def errorHandler(ctx: WabaseRequestContext, error: Throwable): Future[HttpResponse] = {
    val pf: PartialFunction[Throwable, Future[HttpResponse]] =
      exceptionHandlerPFF(ctx).orElse { case error => Future.failed(error) }
    pf(error)
  }
}
