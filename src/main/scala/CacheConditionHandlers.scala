/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */
package org.wabase

import org.apache.pekko.http.scaladsl.model._
import org.apache.pekko.http.scaladsl.model.DateTime
import headers._
import HttpMethods._
import StatusCodes._
import EntityTag._
import WabaseService.RequestHandler

import scala.concurrent.Future

/**
 * Copied from apache pekko CacheConditionDirectives and modified for WabaseService types
 */
trait CacheConditionHandlers {

  /**
   * Wraps its inner route with support for Conditional Requests as defined
   * by http://tools.ietf.org/html/rfc7232
   *
   * In particular the algorithm defined by http://tools.ietf.org/html/rfc7232#section-6
   * is implemented by this directive.
   *
   * Note: if you want to combine this directive with `withRangeSupport(...)` you need to put
   * it on the *outside* of the `withRangeSupport(...)` directive, i.e. `withRangeSupport(...)`
   * must be on a deeper level in your route structure in order to function correctly.
   */
  def conditional(eTag: EntityTag, innerHandler: RequestHandler): RequestHandler =
    conditional(Some(eTag), None, innerHandler)

  /**
   * Wraps its inner route with support for Conditional Requests as defined
   * by http://tools.ietf.org/html/rfc7232
   *
   * In particular the algorithm defined by http://tools.ietf.org/html/rfc7232#section-6
   * is implemented by this directive.
   *
   * Note: if you want to combine this directive with `withRangeSupport(...)` you need to put
   * it on the *outside* of the `withRangeSupport(...)` directive, i.e. `withRangeSupport(...)`
   * must be on a deeper level in your route structure in order to function correctly.
   */
  def conditional(lastModified: DateTime, innerHandler: RequestHandler): RequestHandler =
    conditional(None, Some(lastModified), innerHandler)

  /**
   * Wraps its inner route with support for Conditional Requests as defined
   * by http://tools.ietf.org/html/rfc7232
   *
   * In particular the algorithm defined by http://tools.ietf.org/html/rfc7232#section-6
   * is implemented by this directive.
   *
   * Note: if you want to combine this directive with `withRangeSupport(...)` you need to put
   * it on the *outside* of the `withRangeSupport(...)` directive, i.e. `withRangeSupport(...)`
   * must be on a deeper level in your route structure in order to function correctly.
   */
  def conditional(eTag: EntityTag, lastModified: DateTime, innerHandler: RequestHandler): RequestHandler =
    conditional(Some(eTag), Some(lastModified), innerHandler)

  /**
   * Wraps its inner route with support for Conditional Requests as defined
   * by http://tools.ietf.org/html/rfc7232
   *
   * In particular the algorithm defined by http://tools.ietf.org/html/rfc7232#section-6
   * is implemented by this directive.
   *
   * Note: if you want to combine this directive with `withRangeSupport(...)` you need to put
   * it on the *outside* of the `withRangeSupport(...)` directive, i.e. `withRangeSupport(...)`
   * must be on a deeper level in your route structure in order to function correctly.
   */
  def conditional(eTag: Option[EntityTag], lastModified: Option[DateTime], innerHandler: RequestHandler): RequestHandler = ctx => {
    def addResponseHeaders(response: HttpResponse): HttpResponse =
      response.withDefaultHeaders(eTag.map(ETag(_)).toList ++ lastModified.map(`Last-Modified`(_)).toList)

    // TODO: also handle Cache-Control and Vary
    def complete304() = Future.successful(addResponseHeaders(HttpResponse(NotModified)))
    def complete412() = Future.successful(HttpResponse(PreconditionFailed))

    import ctx.req._
    def innerRouteWithRangeHeaderFilteredOut: Future[HttpResponse] = {
      val ctxWithoutRange = ctx.copy(req = ctx.req.mapHeaders(_.filterNot(_.isInstanceOf[Range])))
      innerHandler(ctxWithoutRange)
        .map(addResponseHeaders)(ctxWithoutRange.as.dispatcher)
    }

    def isGetOrHead = method == HEAD || method == GET
    def unmodified(ifModifiedSince: DateTime) =
      lastModified.get <= ifModifiedSince && ifModifiedSince.clicks < System.currentTimeMillis()

    def step1(): Future[HttpResponse] =
      header[`If-Match`] match {
        case Some(`If-Match`(im)) if eTag.isDefined =>
          if (matchesRange(eTag.get, im, weakComparison = false)) step3() else complete412()
        case _ => step2()
      }
    def step2(): Future[HttpResponse] =
      header[`If-Unmodified-Since`] match {
        case Some(`If-Unmodified-Since`(ius)) if lastModified.isDefined && !unmodified(ius) => complete412()
        case _                                                                              => step3()
      }
    def step3(): Future[HttpResponse] =
      header[`If-None-Match`] match {
        case Some(`If-None-Match`(inm)) if eTag.isDefined =>
          if (!matchesRange(eTag.get, inm, weakComparison = true)) step5()
          else if (isGetOrHead) complete304()
          else complete412()
        case _ => step4()
      }
    def step4(): Future[HttpResponse] =
      if (isGetOrHead) {
        header[`If-Modified-Since`] match {
          case Some(`If-Modified-Since`(ims)) if lastModified.isDefined && unmodified(ims) => complete304()
          case _                                                                           => step5()
        }
      } else step5()
    def step5(): Future[HttpResponse] =
      if (method == GET && header[Range].isDefined)
        header[`If-Range`] match {
          case Some(`If-Range`(Left(tag))) if eTag.isDefined && !matches(eTag.get, tag, weakComparison = false) =>
            innerRouteWithRangeHeaderFilteredOut
          case Some(`If-Range`(Right(ims))) if lastModified.isDefined && !unmodified(ims) =>
            innerRouteWithRangeHeaderFilteredOut
          case _ => step6()
        }
      else step6()
    def step6(): Future[HttpResponse] =
      innerHandler(ctx).map(addResponseHeaders)(ctx.as.dispatcher)

    step1()
  }
}

object CacheConditionHandlers extends CacheConditionHandlers
