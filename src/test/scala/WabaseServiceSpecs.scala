package org.wabase

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.{HttpEntity, HttpMessage, HttpMethod, HttpMethods, HttpRequest, HttpResponse, RequestEntity, Uri}
import org.apache.pekko.util.ByteString
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.wabase.WabaseService.Wabase

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}

class WabaseServiceSpecs extends AnyFlatSpec with Matchers {

  class WA(exec: Execution) extends WabaseServer.App(exec) {
    override def initQuerease: AppQuerease = new TestQuerease("/service-specs-metadata.yaml")
  }
  protected val server = new WabaseServer {
    override val wabase: Wabase = new WA(this.executionImpl)
  }
  import server._
  protected val service = server.service

  DbDrivers.loadDrivers

  protected def entityEquals(
    result: Future[HttpResponse],
    pattern: Any,
    decoder: String => Any = identity
  ) =
    Await.result(result.map(WabaseTestHandlers.entity).map(decoder), 5.seconds) shouldBe pattern

  protected def callRoute(
    url: String,
    data: RequestEntity = HttpEntity.Empty,
    method: HttpMethod = HttpMethods.GET,
  ) =
    service.handle(server.wabase, server.deferredControl)(HttpRequest(method = method, uri = url, entity = data))

  protected def encodeJs(value: Any) = ResultEncoder.encodeAnyToJsonString(value)

  protected def decodeJs(js: String) = CborOrJsonAnyValueDecoder.decode(ByteString(js))

  it should "execute wabase service routes" in {
    entityEquals(callRoute("/simple"), "Simple handler response")
    entityEquals(callRoute("/uri"), "/uri/added-segment")
    entityEquals(callRoute("/response-transformer"), "/response-transformer/added-segment transformed response")
    entityEquals(callRoute("/echo", "hi"), "hi")
    entityEquals(callRoute("/handler-transformer", "hi"), "Request transformed hi response transformed")
    entityEquals(callRoute("/user"), "Test user")
    entityEquals(callRoute("/long-handler-chain"), "Data from Test user: /long-handler-chain/added-segment transformed response")
  }

  it should "process errors for wabase service routes" in {
    entityEquals(callRoute("/greater/than-3/5"), "Key: 5")
    entityEquals(callRoute("/greater/than-3/2"), "Key must be greater then 3, got: 2")
    entityEquals(callRoute("/greater/than-3/fail"), """Key must be number instead got: For input string: "fail"""")
  }

  it should "execute wabase service routes for views" in {
    entityEquals(callRoute("/views/view1/10"), Map("id" -> 10, "value" -> "Value10"), decodeJs)
    entityEquals(callRoute("/views/view1/5", encodeJs(Map("value" -> "Value5-ins")), HttpMethods.POST),
      Map("id" -> 5, "value" -> "Value5-ins"), decodeJs)
  }

  val count = 1024
  it should s"execute $count wabase service routes" in {
    1 to count foreach { i =>
      entityEquals(callRoute("/long-handler-chain"), "Data from Test user: /long-handler-chain/added-segment transformed response")
    }
  }
}

object WabaseTestHandlers {
  def simpleHandler(ctx: WabaseRequestContext) =
    Future.successful { HttpResponse(entity = "Simple handler response") }

  def urlTransformer(uri: Uri) = uri.withPath(uri.path ?/ "added-segment")

  def uriValue(uri: Uri) = HttpResponse(entity = uri.toString())

  def responseTransformer(resp: HttpResponse)(implicit ec: ExecutionContext, as: ActorSystem) =
    resp.withEntity(entity(resp) + " transformed response")

  def echo(req: HttpRequest)(implicit ec: ExecutionContext, as: ActorSystem) = HttpResponse(entity = entity(req))

  def transformer(innerHandler: WabaseService.RequestHandler)(
    implicit as: ActorSystem, ec: ExecutionContext): WabaseService.RequestHandler = { ctx =>
    innerHandler(ctx.copy(req = ctx.req.withEntity("Request transformed " + entity(ctx.req))))
      .map { resp => resp.withEntity(entity(resp) + " response transformed") }
  }

  def testAuth(ctx: WabaseRequestContext) =
    ctx.copy(user = WabaseUser(Map("id" -> 111, "name" -> "Test user")))

  def respondWithUserName(ctx: WabaseRequestContext) = HttpResponse(entity = ctx.user.name)

  def addUserData(user: WabaseUser, resp: HttpResponse)(implicit ec: ExecutionContext, as: ActorSystem) =
    resp.withEntity(s"Data from ${user.name}: " + entity(resp))

  def keyExtractor(ctx: WabaseRequestContext, uri: Uri) = {
    val R = ctx.route.path
    val R(key) = uri.path.toString()
    ctx.copy(key = Seq(key))
  }

  def keyChecker(ctx: WabaseRequestContext) = ctx.key match {
    case Seq(key) =>
      val nr = String.valueOf(key).toInt
      if (nr > 3) HttpResponse(entity = s"Key: $nr") else throw BusinessException(s"Key must be greater then 3, got: $nr")
    case x => sys.error(s"Wrong key: $x")
  }

  def errorHandler(ctx: WabaseRequestContext): WabaseService.ErrorHandler = {
    val eh: WabaseService.ErrorHandler = {
      case e: NumberFormatException =>
        Future.successful(HttpResponse(entity = s"Key must be number instead got: ${e.getMessage}"))
    }
    eh orElse WabaseErrorHandler.errorHandler(ctx)
  }

  // helper function
  def entity(msg: HttpMessage)(implicit ec: ExecutionContext, as: ActorSystem): String =
    Await.result(msg.entity.toStrict(1.second).map(_.data.utf8String), 1.second)
}
