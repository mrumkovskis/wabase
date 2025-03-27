package org.wabase

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.{HttpEntity, HttpMessage, HttpMethod, HttpMethods, HttpRequest, HttpResponse, RequestEntity, Uri}
import org.apache.pekko.util.ByteString
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}

class WabaseServiceSpecs extends AnyFlatSpec with Matchers {

  class WA(exec: Execution) extends WabaseServer.App(exec) {
    override def initQuerease: AppQuerease = new TestQuerease("/service-specs-metadata.yaml")
  }
  implicit val serverSystem: ActorSystem  = ActorSystem("wabase-server")
  implicit val ec: ExecutionContext = serverSystem.dispatcher
  val executionImpl = new ExecutionImpl()(serverSystem)
  val server = new WabaseServer(new WA(executionImpl))

  DbDrivers.loadDrivers

  private def entity(result: Future[HttpResponse], decoder: String => Any = identity): Any =
    Await.result(result.map(WabaseTestHandlers.entity).map(decoder), 5.seconds)

  protected def callRoute(
    url: String,
    data: RequestEntity = HttpEntity.Empty,
    method: HttpMethod = HttpMethods.GET,
    decoder: String => Any = identity,
  ) = entity(server.handle(HttpRequest(method = method, uri = url, entity = data)), decoder)

  protected def encodeJs(value: Any) = ResultEncoder.encodeAnyToJsonString(value)

  protected def decodeJs(js: String) = CborOrJsonAnyValueDecoder.decode(ByteString(js))

  it should "do wabase service routes" in {
    callRoute("/simple") shouldBe "Simple handler response"
    callRoute("/no-param-handler") shouldBe "No param handler"
    callRoute("/uri") shouldBe "/uri/added-segment"
    WabaseService.toReadableString(
      Uri.Path(callRoute(Uri(path = Uri.Path("/non-ascii-uri/glāžšķūņu rūķīši")).toString).toString)
    ) shouldBe "/non-ascii-uri/glāžšķūņu rūķīši/added-segment"
    callRoute("/response-transformer") shouldBe "/response-transformer/added-segment transformed response"
    callRoute("/echo", "hi") shouldBe "hi"
    callRoute("/handler-transformer", "hi") shouldBe "Request transformed hi response transformed"
    callRoute("/user") shouldBe "Test user"
    callRoute("/long-handler-chain") shouldBe "Data from Test user: /long-handler-chain/added-segment transformed response"
  }

  it should "do http method dependant routes" in {
    callRoute("/method-dependent-path", method = HttpMethods.GET) shouldBe "Http method with path match: GET /method-dependent-path"
    callRoute("/method-dependent-path", method = HttpMethods.POST) shouldBe "Http method with path match: POST /method-dependent-path"
    callRoute("/method-dependent-path", method = HttpMethods.PUT) shouldBe "Http method with path match: PUT /method-dependent-path"
    callRoute("/method-dependent-path", method = HttpMethods.DELETE) shouldBe "DELETE /method-dependent-path"
    callRoute("/method-dependent-path", method = HttpMethods.HEAD) shouldBe "HEAD /method-dependent-path"
    callRoute("/method-dependent-path", method = HttpMethods.OPTIONS) shouldBe "OPTIONS /method-dependent-path"
    callRoute("/decoded-map-entity", data = encodeJs(Map("a" -> 1, "b" -> "x", "c" -> List(1,2,3))),
      method = HttpMethods.POST, decoder = decodeJs) shouldBe Map("a" -> 1, "b" -> "x", "c" -> List(1, 2, 3))
    callRoute("/decoded-seq-entity", data = encodeJs(List(Map("a" -> 1), 2, true, "x", List(1, "y"))),
      method = HttpMethods.PUT, decoder = decodeJs) shouldBe List(Map("a" -> 1), 2, true, "x", List(1, "y"))
    callRoute("/decoded-string-entity", data = HttpEntity("content"), method = HttpMethods.PUT) shouldBe "content"
    callRoute("/decoded-dto-entity", data = encodeJs(Map("id" -> 1, "name" -> "View1")),
      method = HttpMethods.POST) shouldBe "1:View1"
  }

  it should "process errors for wabase service routes" in {
    callRoute("/greater/than-3/5") shouldBe "Key: 5"
    callRoute("/greater/than-3/2") shouldBe "Key must be greater then 3, got: 2"
    callRoute("/greater/than-3/fail") shouldBe """Key must be number instead got: For input string: "fail""""
  }

  it should "do wabase service routes for views" in {
    callRoute("/views/view1/10", decoder = decodeJs) shouldBe Map("id" -> 10, "value" -> "Value10")
    callRoute("/views/view1/5", encodeJs(Map("value" -> "Value5-ins")), HttpMethods.POST, decodeJs) shouldBe
      Map("id" -> 5, "value" -> "Value5-ins")
    callRoute("/views/view1/5", encodeJs(Map("id" -> 5, "value" -> "Value5-ins")), HttpMethods.PUT,
      decodeJs) shouldBe Map("id" -> 5, "value" -> "upd-Value5-ins")
    callRoute("/views/view1/10", method = HttpMethods.DELETE) shouldBe "deleted 10"
    callRoute("/views/view1?list_filter_param=val", decoder = decodeJs) shouldBe "val"
    callRoute("/views/create:view1?p1=111&p2=aaa", decoder = decodeJs) shouldBe Seq(111, "aaa")
    callRoute("/views/count:view1", decoder = decodeJs) shouldBe 1
  }

  it should "do wabase service routes for handlers" in {
    callRoute("/do/test_handler/val/1/2/3") shouldBe "Key: [val, 1, 2, 3]"
    callRoute("/do/map_handler?par1=1.5&par2=abc&par3=true", decoder = decodeJs) shouldBe Map("par1" -> "1.5", "par2" -> "abc", "par3" -> "true")
    callRoute("/do/seq_handler/a/b/c", decoder = decodeJs) shouldBe Seq("a", "b", "c")
    callRoute("/do/dto_handler?id=123&name=ABC", decoder = decodeJs) shouldBe Map("id" -> 123, "name" -> "ABC")
    callRoute("/do/org.wabase.WabaseTestHandlers.dto_seq_handler?id=1&id=2&id=3&name=A&name=B&name=C",
      decoder = decodeJs) shouldBe List(Map("name" -> "A", "id" -> 1), Map("name" -> "B", "id" -> 2), Map("name" -> "C", "id" -> 3))
    callRoute("/do/test.QuereaseActionJavaManager.java_map_handler", data = encodeJs(Map("a" -> 1, "b" -> "x", "c" -> List(1,2,3))),
      method = HttpMethods.POST, decoder = decodeJs) shouldBe Map("a" -> 1, "b" -> "x", "c" -> List(1, 2, 3))
    callRoute("/do/test.QuereaseActionJavaManager.java_seq_handler", data = encodeJs(List(Map("a" -> 1), 2, true, "x", List(1, "y"))),
      method = HttpMethods.PUT, decoder = decodeJs) shouldBe List(Map("a" -> 1), 2, true, "x", List(1, "y"))
  }

  val count = 1024
  it should s"do $count wabase service routes" in {
    1 to count foreach { _ =>
      callRoute("/long-handler-chain") shouldBe "Data from Test user: /long-handler-chain/added-segment transformed response"
    }
  }
}

class View1 extends Dto {
  var id: Long = _
  var name: String = _
}

object WabaseTestHandlers {

  def simpleHandler(ctx: WabaseRequestContext) =
    Future.successful { HttpResponse(entity = "Simple handler response") }

  def noParamHandler = "No param handler"

  def urlTransformer(uri: Uri) = uri.withPath(uri.path ?/ "added-segment")

  def uriValue(uri: Uri) = HttpResponse(entity = uri.toString())

  def responseTransformer(resp: HttpResponse)(implicit ec: ExecutionContext, as: ActorSystem) =
    resp.withEntity(entity(resp) + " transformed response")

  def echo(req: HttpRequest)(implicit ec: ExecutionContext, as: ActorSystem) = HttpResponse(entity = entity(req))

  def methodAndPathMatch(req: HttpRequest) = s"Http method with path match: ${req.method.value} ${req.uri.path}"

  def methodAndPath(req: HttpRequest) = s"${req.method.value} ${req.uri.path}"

  def decodedMapEntity(map: Map[String, Any]) = ResultEncoder.encodeAnyToJsonString(map)

  def decodedSeqEntity(seq: Seq[Any]) = ResultEncoder.encodeAnyToJsonString(seq)

  def decodedStringEntity(str: String) = str

  def decodedDtoEntity(dto: View1) = s"${dto.id}:${dto.name}"

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

  def testHandler(ctx: WabaseRequestContext) = s"Key: [${ctx.key.mkString(", ")}]"
  def map_handler(uri: Uri) = uri.query().toMap
  def seq_handler(ctx: WabaseRequestContext) = ctx.key
  def dto_handler(ctx: WabaseRequestContext) = ctx.wabase.qio.fill[View1](ctx.req.uri.query().toMap)
  def dto_seq_handler(ctx: WabaseRequestContext) = {
    import scala.language.existentials
    val List(l1: List[(String, Any)], l2: List[(String, Any)]) =
      ctx.req.uri.query().toMultiMap.map { case (k, v) => v.map(k -> _) }
    l1.zip(l2)
      .map(_.productIterator.asInstanceOf[Iterator[(String, Any)]].toMap)
      .map(m => ctx.wabase.qio.fill[View1](m))
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
