package wabase.app

import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.client.RequestBuilding.Post
import org.apache.pekko.http.scaladsl.model._
import org.apache.pekko.http.scaladsl.model.headers.{EntityTag, HttpCookie}
import org.apache.pekko.http.scaladsl.model.ws.{Message, TextMessage, WebSocketRequest}
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshal
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.apache.pekko.util.ByteString
import org.mojoz.metadata.ViewDef
import org.mojoz.metadata.out.DdlGenerator
import org.wabase.AppMetadata.FilterParameter
import org.wabase.CacheConditionHandlers.conditional
import org.wabase.WabaseService.{MediaTypes, RequestHandler}
import org.wabase.WabaseUnmarshallers.mapUnmarshaller
import org.wabase._
import org.wabase.ds.ConnectionPools
import org.wabase.swagger.WabaseSwaggerGenerator

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.reflectiveCalls
import scala.util.Try

object BusinessScenariosSpecs extends Loggable {
  def executeStatements(statements: String*): Unit = {
    val conn = ConnectionPools(TresqlResourcesConf.DefaultCpName).getConnection()
    try {
      val statement = conn.createStatement
      try statements foreach { st =>
        logger.debug(st)
        try statement.execute(st) catch {
          case util.control.NonFatal(ex) =>
            logger.error(s"Failed to execute statement: $st")
            throw ex
        }
      } finally statement.close()
    } finally conn.close()
  }

  def requestInfo(req: HttpRequest)(implicit as: ActorSystem, ec: ExecutionContext): Future[Map[String, Any]] = {
    Unmarshal(req.entity).to[Map[String, Any]].map { map =>
      val contentType     = req.entity.contentType
      val contentTypeName = s"${contentType.mediaType.mainType}/${contentType.mediaType.subType}"
      def transformFileContentToString(map: Map[String, Any]) =
        MapUtils.transform("file/content", _.asInstanceOf[ByteString].utf8String, map)
      val contentTypeParameters =
        Try(contentType.toString.drop(contentTypeName.length + 1).trim).toOption.filter(_ != "").orNull
      Map(
        "content-type" -> contentTypeName,
        "content-type-parameters" -> contentTypeParameters,
        "data" -> transformFileContentToString(map),
      ).filter(_._2 != null).toMap
    }
  }

  def echo(req: HttpRequest)(implicit as: ActorSystem, ec: ExecutionContext): HttpResponse = {
    HttpResponse(StatusCodes.OK, entity = req.entity)
  }

  def sleep(millis: Long, response: HttpResponse)(implicit ec: ExecutionContext): Future[HttpResponse] = Future {
    Thread.sleep(millis)
    response
  }

  def toIntArray(coll: Seq[Int]): Array[java.lang.Integer] = {
    coll.map(Integer.valueOf).toArray[java.lang.Integer]
  }
  def toStringArray(coll: Seq[String]): Array[String] = coll.toArray[String]

  def identityCsrfCookieTransformer(cookie: HttpCookie): HttpCookie = cookie

  def queryParamsTransformer(): Map[String, Any] => Map[String, Any] = (params: Map[String, Any]) =>
    params.get("query").collect { case m: Map[String, Any]@unchecked => params ++ m }.getOrElse(params)

}

object ScriptValidations {
  def loadValidations(viewName: String, actionName: String, dbAccess: DbAccess)(implicit qe: AppQuerease) = {
    if (viewName == "save_person_email") {
      WabaseScriptValidation.loadValidations(viewName, actionName, dbAccess)
    } else Nil
  }
}

object Guidelines {
  def requestCalculation(result: Array[dto.response_calculation_view],
                         filterCond: dto.request_calculation_view) = {
    result.filter(d => d.code == "code2" && d.category == filterCond.category)
  }

  def qeCall(ctx: WabaseRequestContext) = {
    val result = ctx.wabase.withConn("guideline_calculation_helper", "list", ctx.queryTimeout) { implicit res =>
      implicit val qio: AppQuereaseIo[Dto] = ctx.wabase.qio
      ctx.wabase.qe.list[dto.guideline_calculation_helper](Map[String, Any]())
    }
    HttpResponse(
      status = StatusCodes.OK,
      entity = HttpEntity(ContentTypes.`application/json`,
        ResultEncoder.encodeAnyToJsonByteString(result.map(_.toMap(ctx.wabase.qe))))
    )
  }
}

object SwaggerTests {
  def generateSwaggerJsonForRedirects(ctx: WabaseRequestContext): RequestHandler = {
    conditional(EntityTag(ctx.wabase.app.metadataVersionString), DateTime(ctx.wabase.app.startupTimeMillis), _ => {
      Future.successful {
        val hasApi = ctx.wabase.app.hasApi(_, null, _, _, _ => true)
        val generatorConfig = ConfigFactory.parseString(s"""app.marshal_key_as_json = false""").withFallback(org.wabase.config)
        val generator = new WabaseSwaggerGenerator(Seq(ctx.wabase.qe), config.getString("app.host"), hasApi, config = generatorConfig) {
          override def getQueryParameters(method: String, viewDef: ViewDef, keySize: Int = 99): Seq[FilterParameter] = {
            super.getQueryParameters(method, viewDef, keySize)
              .filterNot(p => ctx.wabase.app.isInternalParameter(viewDef, p.name))
          }
        }
        HttpResponse(entity = HttpEntity(MediaTypes.`application/json`, generator.generateSwaggerJson))
      }
    })
  }
}

class BusinessScenariosSpecs extends BusinessScenariosBaseSpecs("http_tests") {
  import BusinessScenariosSpecs._
  lazy val server = new RunningServer
  override def initHttpClient = server
  override def beforeAll() = {
    server
  }
  override def afterAll() = {
    server.unbind() // unbind for cross-scala tests
  }

  override def scenariosAutoLogin  = false
  override def scenariosAutoLogout = false

  override def backdoorAction(requestInfo: RequestInfo, context: Map[String, Any], map: Map[String, Any]): Any = {
    import requestInfo.path
    if (path.startsWith("/backdoor/create-sequences/")) {
      val seqNames   = path.substring("/backdoor/create-sequences/".length).split(",").toSeq
      val statements = seqNames.map { seqName => s"create sequence $seqName start with 1;" }
      executeStatements(statements: _*)
    } else if (path.startsWith("/backdoor/create-tables/")) {
      val tableNames = path.substring("/backdoor/create-tables/".length).split(",").toSeq
      val tableDefs  = tableNames.map { tableName => qe.tableMetadata.tableDef(tableName, null) }.toVector
      val generator  = DdlGenerator.hsqldb()
      val statements = generator.schema(tableDefs).split(";[\r\n]+").toSeq
      executeStatements(statements: _*)
    } else if (path.startsWith("/backdoor/drop-sequences/")) {
      val seqNames   = path.substring("/backdoor/drop-sequences/".length).split(",").toSeq
      val statements = seqNames.map { seqName => s"drop sequence $seqName;" }
      executeStatements(statements: _*)
    } else if (path.startsWith("/backdoor/drop-tables/")) {
      val tableNames = path.substring("/backdoor/drop-tables/".length).split(",").toSeq
      val statements = tableNames.map { tableName => s"drop table $tableName;" }
      executeStatements(statements: _*)
    } else {
      super.backdoorAction(requestInfo, context, map)
    }
  }

  behavior of "server notifications"
  it should "read web socket messages" in {
    val port = config.getString("port")
    implicit val as: ActorSystem = ActorSystem("test-server-ws-messages-client")
    implicit val ec: ExecutionContext = as.dispatcher

    val topic = "test_topic"

    val sink =
      Sink.takeLast[Message](3).mapMaterializedValue(_.map(_.map {
        case message: TextMessage.Strict => message.text
        case _ => ""
      }))

    val flow = Flow.fromSinkAndSourceMat(sink,
      Source.maybe[Message]/*keep web socket alive until promise is completed*/)(Keep.both)

    val (upgradeResponse, (resF, close)) =
      Http().singleWebSocketRequest(WebSocketRequest(s"ws://localhost:$port/server_ws_events_subscription/$topic"), flow)
    val upgrade = Await.result(upgradeResponse, 2.seconds)
    upgrade.response.status shouldBe StatusCodes.SwitchingProtocols
    Future.traverse(List("ws_value1", "ws_value2", "ws_value3")) { value =>
      Http()
        .singleRequest(Post(s"http://localhost:$port/data/server_events/$topic?value=$value"))
    }.flatMap(_ => resF)
    // wait until all messages are arrived in the sink
    Thread.sleep(1000)
    // close the connection
    close.success(None)
    val res = Await.result(resF, 1.second)
    res.sorted shouldBe List("ws_value1", "ws_value2", "ws_value3")
  }
}
