package org.wabase

import java.io.File
import java.nio.file.Files
import java.sql.{Connection, DriverManager}
import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.model.ws.TextMessage
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.testkit.{ScalatestRouteTest, WSProbe}
import org.mojoz.metadata.in.YamlMd
import org.mojoz.metadata.out.SqlGenerator
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.tresql._

import scala.concurrent.Future
import scala.language.implicitConversions

class DeferredTests extends AnyFlatSpec with Matchers with ScalatestRouteTest {
  var conn: Connection = _

  var streamerConfQe: QuereaseProvider with AppFileStreamerConfig = _

  var service: TestAppService = _

  implicit val queryTimeout = QueryTimeout(10)
  implicit def userToString(user: TestUsr) = user.id.toString

  override def beforeAll(): Unit = {
    super.beforeAll()
    Class.forName("org.hsqldb.jdbc.JDBCDriver")
    this.conn = DriverManager.getConnection("jdbc:hsqldb:mem:deferred_test")
    val querease = new AppQuerease {
      override lazy val yamlMetadata = YamlMd.fromResource("/deferred-metadata.yaml")
    }

    val db = new DbAccess with Loggable {
      logger.debug("Creating database for file upload ...\n")
      SqlGenerator.hsqldb().schema(querease.tableMetadata.tableDefs)
        .split(";\\s+").map(_ + ";")
        .foreach { sql =>
          logger.debug(sql)
          val st = conn.createStatement
          st.execute(sql)
          st.close
        }
      val st = conn.createStatement
      st.execute("create sequence seq")
      st.close
      logger.debug("Database created successfully.")

      override val tresqlResources  = new TresqlResources {
        override val resourcesTemplate =
          super.resourcesTemplate.copy(
            conn = DeferredTests.this.conn,
            metadata = querease.tresqlMetadata,
            dialect = dialects.HSQLDialect,
            idExpr = s => "nextval('seq')"
          )
      }

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

    service = new TestAppService(system) {
      override def initApp: App = appl
      override def initFileStreamer = appl
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    val p = new File(streamerConfQe.rootPath).toPath
    if (p.toFile.exists)
      Files.walk(p).sorted(java.util.Comparator.reverseOrder()).map[File](_.toFile).forEach(_.delete)
  }

  "The isDeferredPath directive" should "be deferred paths" in {
    val route = service.isDeferredPath(service.extractTimeout {
      timeout => complete(s"defered-timeout:${timeout.timeoutSeconds}")})

      Get("long-req") ~> route ~> check {
        responseAs[String] shouldEqual "defered-timeout:10"
      }
      Get("long-req/") ~> route ~> check {
        handled shouldBe true
      }
      Get("long-req/123") ~> route ~> check {
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
  }

  "The hasDeferredHeader directive" should "not have deferred header" in {
    val route = service.hasDeferredHeader(complete("ok"))
    Get("/") ~> RawHeader("X-Def", "100s") ~> route ~> check {
      handled shouldBe false
    }
    Get("/") ~> route ~> check {
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

  "The enableDeferred directive" should "work" in {
    implicit val user = TestUsr(2)
    val route = handleExceptions(service.appExceptionHandler) {
      service.wsNotificationsAction(user.id.toString) ~ pathPrefix("data") {
        service.enableDeferred(user) {
          pathPrefix("action") {
            complete("OK")
          } ~ pathPrefix("fault") {
            throw new BusinessException("fault")
          }
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

    Get("/data/action") ~> RawHeader("X-Deferred", "10s") ~> route ~> check {
      handled shouldBe true
    }
    Get("/data/fault") ~> RawHeader("X-Deferred", "10s") ~> route ~> check {
      handled shouldBe true
    }

    def checkDeferredResult(hash: String, status: String) =
      Get("/") ~> service.deferredResultAction(hash, user) ~> check {responseAs[String] shouldEqual status}

    import spray.json._
    1 to 5 foreach { _ => // 1 version, 2 start execution statuses, 2 results
      val TextMessage.Strict(msg) = wsClient.expectMessage()
      msg.parseJson.asJsObject.fields.toList match{
        case List(("version", JsString(version))) => version shouldBe service.appVersion
        case List((hash, JsObject(statusObj))) =>
          val JsString(status) = statusObj("status")
          status match {
            case "EXE" => exeCount += 1
            case "OK" =>
              checkDeferredResult(hash, "OK")
              okCount += 1
            case "ERR" =>
              checkDeferredResult(hash, "fault")
              errCount += 1
          }
        case x => throw new IllegalStateException("unexpected: " + x)
      }
    }

    exeCount shouldEqual 2
    okCount shouldEqual 1
    errCount shouldEqual 1
    wsClient.sendCompletion()
  }
}
