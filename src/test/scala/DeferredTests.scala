package org.wabase

import java.io.File
import java.nio.file.Files
import java.util.UUID
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, StatusCodes}
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.model.ws.TextMessage
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.{ScalatestRouteTest, WSProbe}
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.language.implicitConversions
import scala.util.Try

class DeferredTests extends AnyFlatSpec with QuereaseBaseSpecs with ScalatestRouteTest {

  var streamerConfQe: QuereaseProvider with AppFileStreamerConfig = _

  var service: TestAppService = _

  var deferredResultFileRootPath: String = _

  implicit val queryTimeout = QueryTimeout(10)
  implicit def userToString(user: TestUsr) = user.id.toString

  override def beforeAll(): Unit = {
    querease = new QuereaseBase("/deferred-metadata.yaml")
    super.beforeAll()

    val db = new DbAccess with Loggable {
      override val tresqlResources = DeferredTests.this.tresqlResources

      override def dbUse[A](a: => A)(implicit timeout: QueryTimeout, pool: PoolName): A =
        try a finally tresqlResources.conn.rollback
      override protected def transactionInternal[A](forceNewConnection: Boolean, a: => A)(implicit timeout: QueryTimeout,
                                                                                          pool: PoolName): A =
        try a finally tresqlResources.conn.commit
    }

    val appl = new TestApp {
      override def dbAccessDelegate = db
      override protected def initQuerease: QE = querease
      override lazy val rootPath =
        new File(System.getProperty("java.io.tmpdir"),"deferred-tests/" + UUID.randomUUID().toString).getPath
    }

    streamerConfQe = appl

    deferredResultFileRootPath =
      new File(System.getProperty("java.io.tmpdir"),"wabase-deferred-results-tests/" + UUID.randomUUID().toString).getPath

    service = new TestAppService(system) {
      override def initApp: App = appl
      override def initFileStreamer = appl

      override def listOrGetAction(viewName: String)(
        implicit user: TestUsr, state: ApplicationState, timeout: QueryTimeout): Route =
        complete(s"$viewName:${timeout.timeoutSeconds}")
      override protected def initDeferredStorage = new DbDeferredStorage(appConfig, this, dbAccess, this) {
        override lazy val rootPath = deferredResultFileRootPath
      }
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    val p = new File(deferredResultFileRootPath).toPath
    if (p.toFile.exists)
      Files.walk(p).sorted(java.util.Comparator.reverseOrder()).map[File](_.toFile).forEach(_.delete)
  }

  "The isDeferredPath directive" should "be deferred paths" in {
    val route = service.isDeferredPath(service.extractTimeout {
      timeout => complete(s"defered-timeout:${timeout.timeoutSeconds}")})

    Get("/long-req") ~> route ~> check {
      responseAs[String] shouldEqual "defered-timeout:300"
    }
    Get("/long-req/") ~> route ~> check {
      handled shouldBe true
    }
    Get("/long-req/123") ~> route ~> check {
      handled shouldBe true
    }
  }

  "The isDeferredPath directive" should "not be deferred paths" in {
    val route = service.isDeferredPath(service.extractTimeout {
      timeout => complete(s"defered-timeout:${timeout.timeoutSeconds}")})
    Get("/data/aaa/123") ~> route ~> check {
      handled shouldBe false
    }
  }

  "The hasDeferredHeader directive" should "have deferred header" in {
    val route = service.hasDeferredHeader(complete("ok"))

    Get("/") ~> RawHeader("X-Deferred", "10s") ~> route ~> check {
      handled shouldBe true
    }

    Get("/") ~> RawHeader("X-Deferred", "true") ~> route ~> check {
      handled shouldBe true
    }
  }

  "The hasDeferredHeader directive" should "not have deferred header" in {
    val route = service.hasDeferredHeader(complete("ok"))
    Get("/") ~> RawHeader("X-Def", "100s") ~> route ~> check {
      handled shouldBe false
    }
    Get("/") ~> route ~> check {
      handled shouldBe false
    }
    Get("/") ~> RawHeader("X-Deferred", "false") ~> route ~> check {
      handled shouldBe false
    }
  }

  "The deferred directive" should "work" in {
    implicit val user = TestUsr(1)
    val route = service.wsNotificationsAction(user.id.toString) ~
      service.deferred(user) { path("deferred-req" / LongNumber) { id =>
        complete {
          //println(s"Enter - $id")
          Thread.sleep(100)
          //println(s"Exit - $id")
          //if (id % 2 == 0) sys.error("Fail on every even call")
          s"Req nr. $id"
        }
      }}
    val reqCount = 10
    val wsClient = WSProbe()
    @volatile var processedCount = 0

    WS("/ws", wsClient.flow) ~> route ~> check {
      Future {
        import spray.json._
        var receiveNotifications = true
        while(receiveNotifications) {
          val message = wsClient.expectMessage()
          message match {
            case TextMessage.Strict("DONE") =>
              receiveNotifications = false
            case TextMessage.Strict(msg) =>
              try {
                msg.parseJson match {
                  case JsObject(obj) if JsString("OK") ==
                    obj.values.headOption
                      .filter(_.isInstanceOf[JsObject])
                      .flatMap(_.asJsObject.fields.get("status")).orNull =>
                    processedCount += 1
                  case _ =>
                }
              } catch {
                case ex: Exception =>
                  ex.printStackTrace
              }
            case _ =>
          }
        }
      }
    }

    // generate requests
    1 to reqCount foreach { i =>
      Get(s"/deferred-req/$i") ~> route ~> check {
        handled shouldBe true
      }
    }

    // wait, complete, check results
    Thread.sleep(5000)
    service.publishUserEvent("1", "DONE")
    wsClient.sendCompletion()

    processedCount shouldEqual reqCount
  }

  import spray.json._
  import DefaultJsonProtocol._

  def parseDeferredRequestId(resp: String): String = {
    resp.parseJson.convertTo[Map[String, String]].apply("deferred")
  }

  "The enableDeferred directive" should "work" in {
    implicit val user = TestUsr(2)
    val route = handleExceptions(service.appExceptionHandler) {
      service.wsNotificationsAction(user.id.toString) ~ pathPrefix("data") {
        service.enableDeferred(user) {
          pathPrefix("action") {
            complete("OK")
          } ~ pathPrefix("fault") {
            throw new BusinessException("fault")
          } ~ service.crudAction
        }
      }
    }

    Get("/data/action") ~> route ~> check {
      handled shouldBe true
    }
    Get("/data/fault") ~> route ~> check {
      handled shouldBe true
      status shouldBe StatusCodes.InternalServerError
      responseAs[String] shouldEqual "fault"
    }

    @volatile var exeCount = 0
    @volatile var errCount = 0
    @volatile var okCount = 0
    val wsClient = WSProbe()

    WS("/ws", wsClient.flow) ~> route

    var results = Map[String, String]()

    Get("/data/action") ~> RawHeader("X-Deferred", "10s") ~> route ~> check {
      results += (parseDeferredRequestId(responseAs[String]) -> "OK")
      handled shouldBe true
    }
    Get("/data/fault") ~> RawHeader("X-Deferred", "10s") ~> route ~> check {
      results += (parseDeferredRequestId(responseAs[String]) -> "fault")
      handled shouldBe true
    }
    // no need of X-Deferred header as long-req is in deferredTimeouts
    Get("/data/long-req") ~> route ~> check {
      results += (parseDeferredRequestId(responseAs[String]) -> "long-req:300")
      handled shouldBe true
    }
    Get("/data/long-req1") ~> RawHeader("X-Deferred", "30s") ~> route ~> check {
      results += (parseDeferredRequestId(responseAs[String]) -> "long-req1:30")
      handled shouldBe true
    }
    Get("/data/long-req1") ~> RawHeader("X-Deferred", "100s") ~> route ~> check {
      results += (parseDeferredRequestId(responseAs[String]) -> "Max request timeout exceeded: 100 > 60")
      handled shouldBe true
    }
    Get("/data/long-req1") ~> RawHeader("X-Deferred", "true") ~> route ~> check {
      results += (parseDeferredRequestId(responseAs[String]) -> "long-req1:60")
      handled shouldBe true
    }

    def checkDeferredResult(hash: String) =
      Get("/") ~> service.deferredResultAction(hash, user) ~> check {
        responseAs[String] shouldEqual results(hash)
      }

    Try {
      while(true) {
        val TextMessage.Strict(msg) = wsClient.expectMessage()
        msg.parseJson.asJsObject.fields.toList match {
          case List(("version", JsString(version))) => version shouldBe service.appVersion
          case List((hash, JsObject(statusObj))) =>
            val JsString(status) = statusObj("status")
            status match {
              case "EXE" => exeCount += 1
              case "OK" =>
                checkDeferredResult(hash)
                okCount += 1
              case "ERR" =>
                checkDeferredResult(hash)
                errCount += 1
              case "QUEUE" => //do nothing
            }
          case x => throw new IllegalStateException("unexpected: " + x)
        }
      }
    }
    .failed.foreach {
      case _: AssertionError => //ws message read timeout occured, all messages consumed
    }

    exeCount shouldEqual 6
    okCount shouldEqual 4
    errCount shouldEqual 2
    wsClient.sendCompletion()
  }

  "The deferred directive" should "properly handle exceptions" in {
    implicit val user = TestUsr(3)

    def err_route(err: Throwable) = service.deferred(user) {
      complete(throw err)
    }

    def err_marshalling(err: Throwable, len: Int) = service.deferred(user) {
      complete(HttpResponse(status = StatusCodes.OK,
        entity = HttpEntity.Default(contentType = ContentTypes.`text/plain(UTF-8)`,
          contentLength = 1,
          data = Source.fromIterator[ByteString](() => (1 to len map (ByteString(_))).iterator) ++
            Source.failed[ByteString](err))))
    }

    val requests = ArrayBuffer[String]()

    Get("/exception") ~> RawHeader("X-Deferred", "true") ~> err_route(new Exception("EXCEPTION!")) ~> check {
      requests += parseDeferredRequestId(responseAs[String])
      handled shouldBe true
    }

    0 until 100 foreach { len =>
      Get("/marshalling_error") ~> err_marshalling(new Exception("Marshalling ere"), len) ~> check {
        requests += parseDeferredRequestId(responseAs[String])
        handled shouldBe true
      }
    }

    Thread.sleep(1000)

    requests.foreach { req_hash =>
      Get("/results") ~> service.deferredRequest(req_hash, user) ~> check {
        responseAs[String].parseJson.convertTo[Map[String, String]].apply("status") shouldBe "ERR"
      }
    }
  }
}
