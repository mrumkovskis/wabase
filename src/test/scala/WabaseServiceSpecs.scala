package org.wabase

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.client.RequestBuilding
import org.apache.pekko.http.scaladsl.client.RequestBuilding.{Get, Head, Options, Post, Put}
import org.apache.pekko.http.scaladsl.model.headers.{BasicHttpCredentials, Cookie, HttpCookiePair, `Set-Cookie`}
import org.apache.pekko.http.scaladsl.model.{HttpEntity, HttpMessage, HttpMethod, HttpMethods, HttpRequest, HttpResponse, RequestEntity, StatusCodes, Uri}
import org.apache.pekko.util.ByteString
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.wabase.WabaseService.MediaTypes

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}

class WabaseServiceSpecs extends AnyFlatSpec with Matchers {

  class WA(exec: Execution) extends WabaseServer.App(exec) {
    override def initQuerease: AppQuerease = new TestQuerease(List("/service-specs-metadata.yaml", "/roles-test.yaml"))
    override implicit lazy val httpClients: WabaseHttpClients =
      WabaseHttpClients(Map("default-wabase-http-client" -> (_ => server.handle)))
  }
  implicit val serverSystem: ActorSystem  = ActorSystem("wabase-server")
  implicit val ec: ExecutionContext = serverSystem.dispatcher
  val executionImpl = new ExecutionImpl()(serverSystem)
  val wabase = new WA(executionImpl)
  val server = new WabaseServer(wabase, enableServerNotifications = false, enableDeferredRequests = false)

  DbDrivers.loadDrivers

  private def response(req: HttpRequest) = Await.result(server.handle(req), 5.seconds)

  protected def callRoute(
    url: String,
    data: RequestEntity = HttpEntity.Empty,
    method: HttpMethod = HttpMethods.GET,
    decoder: String => Any = identity,
  ) = entityForRequest(HttpRequest(method = method, uri = url, entity = data), decoder)

  protected def entityForRequest(req: HttpRequest, decoder: String => Any = identity) =
    decoder(WabaseTestHandlers.entity(response(req)))

  protected def statusAndEntityForRequest(req: HttpRequest, decoder: String => Any = identity) = {
    val resp = response(req)
    (resp.status, decoder(WabaseTestHandlers.entity(resp)))
  }

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
    callRoute("/user-info", decoder = decodeJs) shouldBe Map("id" -> 111, "name" -> "Test user", "password" -> "password")
    callRoute("/user_data_merge", decoder = decodeJs) shouldBe Map("id" -> 111, "name" -> "Test user", "session_id" -> "abcdefgh", "roles" -> List("admin", "guest", "operator"))
    callRoute("/long-handler-chain") shouldBe "Data from Test user: /long-handler-chain/added-segment transformed response"
  }

  it should "do http method dependant routes" in {
    entityForRequest(Get("/method-dependent-path")) shouldBe "Http method with path match: GET /method-dependent-path"
    entityForRequest(Post("/method-dependent-path")) shouldBe "Http method with path match: POST /method-dependent-path"
    entityForRequest(Put("/method-dependent-path")) shouldBe "Http method with path match: PUT /method-dependent-path"
    entityForRequest(RequestBuilding.Delete("/method-dependent-path")) shouldBe "DELETE /method-dependent-path"
    entityForRequest(Head("/method-dependent-path")) shouldBe "HEAD /method-dependent-path"
    entityForRequest(Options("/method-dependent-path")) shouldBe "OPTIONS /method-dependent-path"
    entityForRequest(Post("/decoded-map-entity", encodeJs(Map("a" -> 1, "b" -> "x", "c" -> List(1,2,3)))),
      decodeJs) shouldBe Map("a" -> 1, "b" -> "x", "c" -> List(1, 2, 3))
    entityForRequest(Put("/decoded-seq-entity", encodeJs(List(Map("a" -> 1), 2, true, "x", List(1, "y")))),
      decodeJs) shouldBe List(Map("a" -> 1), 2, true, "x", List(1, "y"))
    entityForRequest(Put("/decoded-string-entity", HttpEntity("content"))) shouldBe "content"
    entityForRequest(Post("/decoded-dto-entity", encodeJs(Map("id" -> 1, "name" -> "View1")))) shouldBe "1:View1"
  }

  it should "process errors for wabase service routes" in {
    callRoute("/greater/than-3/5") shouldBe "Key: 5"
    callRoute("/greater/than-3/2") shouldBe "Key must be greater then 3, got: 2"
    callRoute("/greater/than-3/fail") shouldBe """[GET /greater/than-3/fail] Key must be number instead got: For input string: "fail""""
    callRoute("/key_in_action?code=1") shouldBe "1"
    callRoute("/key_in_action?code=x") shouldBe """[GET /key_in_action?code=x] Key must be number instead got: For input string: "x""""
  }

  it should "do wabase service routes for public views" in {
    callRoute("/public/view1/10", decoder = decodeJs) shouldBe Map("id" -> 10, "value" -> "Value10")
    entityForRequest(Post("/public/view1/5", encodeJs(Map("value" -> "Value5-ins"))), decodeJs) shouldBe
      Map("id" -> 5, "value" -> "Value5-ins")
    entityForRequest(Put("/public/view1/5", encodeJs(Map("id" -> 5, "value" -> "Value5-ins"))),
      decodeJs) shouldBe Map("id" -> 5, "value" -> "upd-Value5-ins")
    entityForRequest(RequestBuilding.Delete("/public/view1/10")) shouldBe "deleted 10"
    callRoute("/public/view1?list_filter_param=val", decoder = decodeJs) shouldBe "val"
    callRoute("/public/create:view1?p1=111&p2=aaa", decoder = decodeJs) shouldBe Seq(111, "aaa")
    callRoute("/public/count:view1", decoder = decodeJs) shouldBe 1
    response(Get("/public/querease_action_exception")).status shouldBe StatusCodes.InternalServerError
    response(Post("/public/querease_action_exception")).status shouldBe StatusCodes.Unauthorized
    response(Put("/public/querease_action_exception")).status shouldBe StatusCodes.BadRequest
  }

  it should "process request decoder errors" in {
    statusAndEntityForRequest(Post("/public/view1/5", encodeJs(Seq(Map("value" -> "Value5-ins"))))) match {
      case (code, resp) =>
        code shouldBe StatusCodes.BadRequest
        String.valueOf(resp) should startWith("Failed to read to map for view1")
    }
    statusAndEntityForRequest(Post(
      "/public/view1/5",
      encodeJs(Seq(Map("id" -> 5, "value" -> "Value5-ins")))
    ))  match { case (code, resp) =>
      code shouldBe StatusCodes.BadRequest
      String.valueOf(resp) should startWith("Failed to read to map for view1")
    }
  }

  it should "invoke default error handler" in {
    val (st, _) = statusAndEntityForRequest(Get("/error"))
    st shouldBe StatusCodes.InternalServerError
  }

  it should "do login and authenticated requests" in {

    def doBasicAuthReq(usr: String, pwd: String) =
      response(HttpRequest(
        uri = Uri("/login"),
        headers = List(
          org.apache.pekko.http.scaladsl.model.headers.Authorization(BasicHttpCredentials(usr, pwd)),
          // set this header to see if marshalling content negotiation is passed
          org.apache.pekko.http.scaladsl.model.headers.Accept(MediaTypes.`application/json`)
        )
      ))
    def encryptedSession(resp: HttpResponse) = WabaseService.optionalHttpHeaderValuePF(resp) {
      case `Set-Cookie`(c) if c.name == WabaseAuthentication.SessionCookieName => c.value
    }.get
    def authReq(session: String, req: HttpRequest) =
      req.mapHeaders(_ ++ List(Cookie(List(HttpCookiePair(WabaseAuthentication.SessionCookieName, session)))))
    def decSes(ses: String) = WabaseAuthentication.decodeSession(WabaseAuthentication.decryptSession(ses))

    var resp = doBasicAuthReq("Gunza", "good")
    resp.status shouldBe StatusCodes.OK

    val enc_session = encryptedSession(resp)
    decSes(enc_session).user shouldBe WabaseUser(Map("id" -> 10, "roles" ->  List("admin", "guest", "operator")))

    val x = (1 to 3).scanLeft(enc_session) { (enc_ses, _) =>
      Thread.sleep(10) // ensure that session expiration time changes
      resp = response(authReq(enc_ses, Get("/restricted/user_principal")))
      decodeJs(WabaseTestHandlers.entity(resp)) shouldBe Map("id" -> 10, "roles" ->  List("admin", "guest", "operator"))
      encryptedSession(resp)
    }.reduce {(s1, s2) => decSes(s1).expirationTime should be < decSes(s2).expirationTime; s2}

    entityForRequest(
      authReq(enc_session, Post("/restricted/user_principal"))
    ) shouldBe "10"

    response(
      authReq(enc_session, Get("/restricted/restricted_view"))
    ).status shouldBe StatusCodes.OK

    response(Get("/restricted/restricted_view")).status shouldBe StatusCodes.Unauthorized

    // invalid session, throws AuthenticationException with cause, which can be logged in debug mode with
    // logger in logback-test.xml:
    // <logger name="get.restricted.restricted_view" level="debug"/>
    response(
      authReq("abc", Get("/restricted/restricted_view"))
    ).status shouldBe StatusCodes.Unauthorized


    resp = doBasicAuthReq("Gunza", "bad")
    resp.status shouldBe StatusCodes.Unauthorized

    resp = response(Get("/restricted/user_principal"))
    resp.status shouldBe StatusCodes.Unauthorized
  }

  it should "do wabase service routes for handlers" in {
    callRoute("/do/test_handler/val/1/2/3") shouldBe "Key: [val, 1, 2, 3]"
    callRoute("/do/map_handler?par1=1.5&par2=abc&par3=true", decoder = decodeJs) shouldBe Map("par1" -> "1.5", "par2" -> "abc", "par3" -> "true")
    callRoute("/do/seq_handler/a/b/c", decoder = decodeJs) shouldBe Seq("a", "b", "c")
    callRoute("/do/dto_handler?id=123&name=ABC", decoder = decodeJs) shouldBe Map("id" -> 123, "name" -> "ABC")
    callRoute("/do/org.wabase.WabaseTestHandlers.dto_seq_handler?id=1&id=2&id=3&name=A&name=B&name=C",
      decoder = decodeJs) shouldBe List(Map("name" -> "A", "id" -> 1), Map("name" -> "B", "id" -> 2), Map("name" -> "C", "id" -> 3))
    entityForRequest(Post("/do/test.QuereaseActionJavaManager.java_map_handler", encodeJs(Map("a" -> 1, "b" -> "x", "c" -> List(1,2,3)))),
      decodeJs) shouldBe Map("a" -> 1, "b" -> "x", "c" -> List(1, 2, 3))
    entityForRequest(Put("/do/test.QuereaseActionJavaManager.java_seq_handler", encodeJs(List(Map("a" -> 1), 2, true, "x", List(1, "y")))),
      decodeJs) shouldBe List(Map("a" -> 1), 2, true, "x", List(1, "y"))
    callRoute("/do/org.wabase.WabaseTestHandlers.optionHandler?key=true") shouldBe "yes"
    response(Get("/do/org.wabase.WabaseTestHandlers.optionHandler?key=false")).status shouldBe StatusCodes.NotFound
  }

  it should "do routes with additional args" in {
    callRoute("/static_resources/file.txt") shouldBe "Resource: static_resources/file.txt from uri: /static_resources/file.txt"
    callRoute("/static_resources/") shouldBe "Resource: static_resources/null from uri: /static_resources/"
  }

  it should "extract application state" in {
    def res(cookies: HttpCookiePair *) = entityForRequest(
      HttpRequest(
        uri = "/public/state_extractor",
        headers = if (cookies.isEmpty) Nil else List(Cookie(cookies.head, cookies.drop(1): _*))
      ),
      decoder = decodeJs)
    res(HttpCookiePair("current_name", "Ann"), HttpCookiePair("current_dept", "Sales")) shouldBe
      Map("name" -> "Ann", "dept" -> "Sales")
    res(HttpCookiePair("current_name", "Ann")) shouldBe  Map("name" -> "Ann", "dept" -> null)
    res() shouldBe Map("name" -> null, "dept" -> null)
  }

  it should "handle deep nested structures" in {
    def deepNestedData(depth: Int) =
      (1 to depth).foldLeft(Map[String, Any]("code" -> depth, "children" -> Nil)) { (r, i) =>
        Map("code" -> (depth - i), "children" -> List(r))
      }
    // borer does not support more than 64 Array/Object nesting levels
    val depth = 30
    val data = deepNestedData(depth)
    entityForRequest(Post("/deep-nested-view/nested_view", encodeJs(data)), decodeJs) shouldBe data
    entityForRequest(Post("/deep-nested-data", encodeJs(data)), decodeJs) shouldBe data
  }

  it should "control request size limit" in {
    val uri = "/public/entity_size_limit"
    entityForRequest(Post(uri, encodeJs(Map("id" -> 1, "name" -> "John"))), decodeJs) shouldBe Map("id" -> 1, "name" -> "John")
    val (status, result) = statusAndEntityForRequest(Post(
      uri, encodeJs(Map("id" -> 1, "name" -> "John John John John John John John John John John John John John"))
    ))
    status shouldBe StatusCodes.ContentTooLarge
    result shouldBe "Content too large: actual size - 82, limit - 64"
  }

  it should "handle http head method" in {
    callRoute("/head") shouldBe "Mon, 29 Sep 2025 22:18:54 EET"
  }

  val count = 1024
  it should s"do ${count * 2} wabase service routes" in {
    1 to count foreach { _ =>
      callRoute("/long-handler-chain") shouldBe "Data from Test user: /long-handler-chain/added-segment transformed response"
      callRoute("/static_resources/file.txt") shouldBe "Resource: static_resources/file.txt from uri: /static_resources/file.txt"
    }
  }
}

class View1 extends Dto {
  var id: Long = _
  var name: String = _
}

object UserParameterProviderFactory extends AppQuerease.InjectionParametersProviderFactory {
  def createInjectionParametersProvider: AppQuerease.InjectionParametersProvider = ctx => {
    case par if par.getType.isAssignableFrom(classOf[WabaseUser]) =>
      WabaseUser(ctx.data("current_user").asInstanceOf[Map[String, Any]])
  }
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

  def optionHandler(ctx: WabaseRequestContext) = {
    ctx.req.uri.query().toMap.get("key").filter(_ == "true").map(_ => "yes")
  }

  def testAuth(ctx: WabaseRequestContext) =
    ctx.copy(user = WabaseUser(Map("id" -> 111, "name" -> "Test user", "password" -> "password")))

  def respondWithUserName(ctx: WabaseRequestContext) = HttpResponse(entity = ctx.user.name)

  def mergeUserData(user: WabaseUser, resp: HttpResponse) =
    WabaseAuthentication.userPrincipal(WabaseAuthentication.mergeReqRespUserData(user, resp))

  def addUserData(user: WabaseUser, resp: HttpResponse)(implicit ec: ExecutionContext, as: ActorSystem) =
    resp.withEntity(s"Data from ${user.name}: " + entity(resp))

  def currentUser(user: WabaseUser) = user.name

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

  def keyInAction(key: String) = key.toInt

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

  def staticResources(dir: String, file: String)(uri: Uri) = s"Resource: $dir/$file from uri: ${uri.path}"

  def errorHandler(ctx: WabaseRequestContext): WabaseService.ErrorHandler = {
    ({
      case e: NumberFormatException =>
        Future.successful(HttpResponse(
          status = StatusCodes.BadRequest,
          entity = s"[${WabaseErrorHandler.ctxDebugInfo(ctx)}] Key must be number instead got: ${e.getMessage}"
        ))
    }: WabaseService.ErrorHandler) orElse WabaseErrorHandler.errorHandler(ctx)
  }

  def error = throw new IllegalArgumentException("Error")
  def authError = throw new AuthenticationException("bad")
  def csrfException = throw new CSRFException("bad")

  // helper function
  def entity(msg: HttpMessage)(implicit ec: ExecutionContext, as: ActorSystem): String =
    Await.result(msg.entity.toStrict(1.second).map(_.data.utf8String), 1.second)
}
