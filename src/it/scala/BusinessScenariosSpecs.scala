package wabase.app

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.client.RequestBuilding.{Get, Post}
import org.apache.pekko.http.scaladsl.model.sse.ServerSentEvent
import org.apache.pekko.http.scaladsl.model.ws.{Message, TextMessage, WebSocketRequest}
import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpEntity, HttpRequest, HttpResponse, StatusCodes}
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshal
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.apache.pekko.util.ByteString
import org.mojoz.metadata.out.DdlGenerator
import org.tresql.{Result, RowLike}
import org.wabase.WabaseScriptValidation.Validation
import org.wabase._
import org.wabase.WabaseUnmarshallers.mapUnmarshaller

import java.io.File
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.reflectiveCalls
import scala.util.Try
import scala.util.control.NonFatal

object BusinessScenariosSpecs {
  def executeStatements(statements: String*): Unit = {
    val conn = ConnectionPools(TresqlResourcesConf.DefaultCpName).getConnection()
    try {
      val statement = conn.createStatement
      try statements foreach { statement.execute } finally statement.close()
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

  def sleep(millis: Long, response: HttpResponse)(implicit ec: ExecutionContext): Future[HttpResponse] = Future {
    Thread.sleep(millis)
    response
  }
}

object EventsFunctions {
  def subscribeToEvent(topic: String)(as: ActorSystem, req: HttpRequest) = {
    ServerNotifications.subscribeToEventsAndListen(b => a => b.subscribe(a, topic), _ => ())(as, req)
  }

  def publishEvent(topic: String, value: String) = {
    ServerNotifications.publish { _.publish(EventMessage(topic, value)) }
  }

  def subscribeToWsMessages(topic: String)(as: ActorSystem, req: HttpRequest) = {
    ServerNotifications.subscribeToWsMessagesAndListen(b => a => b.subscribe(a, topic), _ => ())(as, req)
  }
}

object ScriptValidations {
  def loadValidations(viewName: String, actionName: String, dbAccess: DbAccess)(implicit qe: AppQuerease) = {
    if (viewName == "save_person_email") {
      WabaseScriptValidation.loadValidations(viewName, actionName, dbAccess)
    } else Nil
  }
}

object JobUtils {
  def sleep(millis: Long) = Thread.sleep(millis)
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

class BusinessScenariosSpecs extends BusinessScenariosBaseSpecs("http_tests") {
  import BusinessScenariosSpecs._
  lazy val server = new RunningServer
  override def resourcePath = "resources/"
  override def initHttpClient = server
  override def beforeAll() = {
    server
  }
  override def afterAll() = {
    server.unbind() // unbind for cross-scala tests
  }

  override def scenariosAutoLogin  = false
  override def scenariosAutoLogout = false

  override def checkTestCase(
    scenario: File, testCase: File, context: Map[String, Any], map: Map[String, Any], retriesLeft: Int
  ): Map[String, Any] = {
    val path   = map.s("path")
    val method = map.sd("method", "GET")
    if (path.startsWith("/backdoor/create-sequence/")) {
      val seqName   = path.substring("/backdoor/create-sequence/".length)
      val statement = s"create sequence $seqName;"
      executeStatements(statement)
      context
    } else if (path.startsWith("/backdoor/create-table/")) {
      val tableName = path.substring("/backdoor/create-table/".length)
      val tableDef  = qe.tableMetadata.tableDef(tableName, null)
      val generator = DdlGenerator.hsqldb()
      val statement = generator.table(tableDef)
      executeStatements(statement)
      context
    } else if (path.startsWith("/backdoor/drop-sequence/")) {
      val seqName   = path.substring("/backdoor/drop-sequence/".length)
      val statement = s"drop sequence $seqName;"
      executeStatements(statement)
      context
    } else if (path.startsWith("/backdoor/drop-table/")) {
      val tableName = path.substring("/backdoor/drop-table/".length)
      executeStatements(s"drop table $tableName;")
      context
    } else {
      super.checkTestCase(scenario, testCase, context, map, retriesLeft)
    }
  }

  behavior of "server notifications"
  it should "read server events" in {
    val port = config.getString("port")
    import org.apache.pekko.http.scaladsl.unmarshalling.sse.EventStreamUnmarshalling._
    implicit val as: ActorSystem = ActorSystem("test-server-events-client")
    implicit val ec: ExecutionContext = as.dispatcher

    val topic = "test_topic"

    val resF = Http()
      .singleRequest(Get(s"http://localhost:$port/data/server_events/$topic"))
      .flatMap { Unmarshal(_).to[Source[ServerSentEvent, NotUsed]] }
      .flatMap { src =>
        Future.traverse(List("value1", "value2", "value3")) { value =>
          Http()
            .singleRequest(Post(s"http://localhost:$port/data/server_events?topic=$topic&value=$value"))
        }.flatMap(_ => Future.successful(src))
      }
      .flatMap(_.take(3).runFold(List[String]()){ (res, ev) => ev.data :: res })
    val res = Await.result(resF, 3.seconds)
    res.sorted shouldBe List("value1", "value2", "value3")
  }

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
      Http().singleWebSocketRequest(WebSocketRequest(s"ws://localhost:$port/data/server_events?topic=$topic"), flow)
    val upgrade = Await.result(upgradeResponse, 2.seconds)
    upgrade.response.status shouldBe StatusCodes.SwitchingProtocols
    Future.traverse(List("ws_value1", "ws_value2", "ws_value3")) { value =>
      Http()
        .singleRequest(Post(s"http://localhost:$port/data/server_events?topic=$topic&value=$value"))
    }.flatMap(_ => resF)
    // wait until all messages are arrived in the sink
    Thread.sleep(1000)
    // close the connection
    close.success(None)
    val res = Await.result(resF, 1.second)
    res.sorted shouldBe List("ws_value1", "ws_value2", "ws_value3")
  }
}
