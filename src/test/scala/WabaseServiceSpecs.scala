package org.wabase

import org.apache.pekko.http.scaladsl.model.{HttpRequest, HttpResponse}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.wabase.WabaseService.Wabase

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

class WabaseServiceSpecs extends AnyFlatSpec with Matchers {

  class WA(exec: Execution) extends WabaseServer.App(exec) {
    override def initQuerease: AppQuerease = new TestQuerease("/service-specs-metadata.yaml")
  }
  protected val server = new WabaseServer {
    override val wabase: Wabase = new WA(this.executionImpl)
  }
  import server._
  protected val service = server.service

  protected def entityEquals(result: Future[HttpResponse], pattern: String) =
    Await.result(result.flatMap(_.entity.toStrict(1.second).map(_.data.utf8String)), 1.second) shouldBe pattern

  protected def callRoute(url: String) =
    service.handle(server.wabase, server.deferredControl)(HttpRequest(uri = url))

  it should "execute wabase service routes" in {
    entityEquals(callRoute("/simple"), "Simple handler response")
  }
}

object WabaseTestHandlers {
  def simpleHandler(ctx: WabaseRequestContext) = Future.successful {
    HttpResponse(entity = "Simple handler response")
  }
}
