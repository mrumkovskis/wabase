package org.wabase

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.{HttpHeader, HttpMessage, HttpRequest, HttpResponse}
import org.apache.pekko.http.scaladsl.testkit.{RouteTest, TestFrameworkInterface}
import org.apache.pekko.util.ByteString

import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future}

trait WabaseRouteTest extends RouteTest { this: TestFrameworkInterface =>
  //  implicit object InjectIntoWabaseRoute extends TildeArrow[HttpRequest, Future[HttpResponse]] {
  //    type Out = Future[HttpResponse]
  //    override def apply(request: HttpRequest, f: HttpRequest => Future[HttpResponse]): Out = f(request)
  //  }
  implicit def injectIntoWabaseRoute(implicit timeout: FiniteDuration):
    TildeArrow[HttpRequest, Future[HttpResponse]] { type Out = HttpResponse } =
    new TildeArrow[HttpRequest, Future[HttpResponse]] {
      type Out = HttpResponse
      override def apply(request: HttpRequest, f: HttpRequest => Future[HttpResponse]): Out =
        Await.result(f(request), timeout)
    }

  /** Inspect a response produced by `req ~> route`, analogous to pekko-http testkit `check`:
    * {{{
    * Get("/path") ~> route ~> checkResponse { resp =>
    *   resp.status shouldBe StatusCodes.OK
    *   jsonAssert(resp.entityString, ...)
    * }
    * }}}
    * Chained via `TransformerPipelineSupport.WithTransformation` (inherited from `RouteTest`).
    */
  def checkResponse[T](body: HttpResponse => T): HttpResponse => T = body

  implicit class WabaseResponseOps(resp: HttpResponse) {
    def entityString(implicit timeout: FiniteDuration): String = TestWabaseServer.entity(resp, timeout)
    /** Header lookup by (case insensitive) name, as in pekko-http testkit `header(name)`. */
    def headerNamed(name: String): Option[HttpHeader] = resp.headers.find(_.is(name.toLowerCase))
  }
}

object TestWabaseServer {
  implicit val responseTimeout: FiniteDuration = 5.seconds
  implicit val as: ActorSystem  = ActorSystem("wabase-server")
  implicit val ec: ExecutionContext = as.dispatcher

  def apply(
    wabase:  WabaseService.Wabase,
    invokeBeforeStart: String = null,
    enableServerNotifications: Boolean = false,
    enableDeferredRequests: Boolean = false
  ): WabaseServer =
    new WabaseServer(wabase, invokeBeforeStart, enableServerNotifications, enableDeferredRequests)

  def responseSync(server: WabaseServer, timeout: Duration = 5.seconds)(req: HttpRequest): HttpResponse =
    Await.result(server.handle(req), timeout)
  def response(server: WabaseServer)(req: HttpRequest): Future[HttpResponse] = server.handle(req)

  // helper function
  def entity(msg: HttpMessage, timeout: FiniteDuration = 1.second): String =
    Await.result(msg.entity.toStrict(timeout).map(_.data.utf8String), timeout)

  def encodeJs(value: Any) = ResultEncoder.encodeAnyToJsonString(value)

  def decodeJs(js: String) = CborOrJsonAnyValueDecoder.decode(ByteString(js))
}
