package org.wabase

import java.io.File
import java.nio.file.Files
import java.util.UUID
import akka.http.scaladsl.model.HttpEntity.{Chunk, Chunked, Default}
import akka.http.scaladsl.model._
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.http.scaladsl.unmarshalling.FromResponseUnmarshaller
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.scalatest.flatspec.AnyFlatSpec
import spray.json.{JsObject, JsString}
import org.wabase.client.CoreClient

import scala.concurrent.duration
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.reflect.ClassTag

class FileUploadSpecs extends AnyFlatSpec with QuereaseBaseSpecs with ScalatestRouteTest {

  var streamerConfQe: QuereaseProvider with AppFileStreamerConfig = _

  var service: TestAppService = _

  override def beforeAll(): Unit = {
    querease = new QuereaseBase("/filestreamer-specs-table-metadata.yaml")
    super.beforeAll()

    val db = new DbAccess with Loggable {
      override val tresqlResources  = FileUploadSpecs.this.tresqlResources

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
        new File(System.getProperty("java.io.tmpdir"),"file-upload-specs/" + UUID.randomUUID().toString).getPath
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
    Files.walk(p).sorted(java.util.Comparator.reverseOrder()).map[java.io.File](_.toFile).forEach(_.delete)
  }

  def uploadPath = "/upload"
  def downloadPath(id: Number, sha: String) = s"download/$id/$sha"
  val usr = TestUsr(1)

  implicit val routeTimeout = RouteTestTimeout(FiniteDuration(5, duration.SECONDS))
  implicit val responseTimeout = Duration("5s")

  "File upload" should "work" in {
    val content = List.fill(10000)(ByteString("FILE CONTENT UTF-8 (зимние rūķīši) "))
    val source = Source(content)

    val route = service.uploadPath { _ => service.uploadAction(None)(usr, ApplicationState(Map()))}
    val entity = Default(ContentTypes.`text/plain(UTF-8)`, content.length, source)
    Post(uploadPath, entity) ~> route ~> check {
      implicit val m = service.jsObjectUnmarshaller(service.sprayJsValueUnmarshaller)
      val res = responseAs[JsObject](implicitly[FromResponseUnmarshaller[JsObject]], implicitly[ClassTag[JsObject]], implicitly[Duration])
      assertResult(res.fields.get("sha_256"))(Some(JsString("718004c597c5343242b7d4f8bfca6f08c57bf424014605fa0691f2cec05488d0")))
    }
  }

  it should "work (Multipart)" in {
    val contentSent = "FILE CONTENT UTF-8 (зимние rūķīši) " * 10000
    val multipartForm = CoreClient.fileUploadForm(HttpEntity(ContentTypes.`text/plain(UTF-8)`, ByteString(contentSent)), "Test.txt")

    val route = service.uploadPath { _ => service.uploadAction(None)(usr, ApplicationState(Map()))}
    Post(uploadPath, multipartForm) ~> route ~> check {
      implicit val m = service.jsObjectUnmarshaller(service.sprayJsValueUnmarshaller)
      val res = responseAs[JsObject]
      assertResult(res.fields.get("sha_256"))(Some(JsString("718004c597c5343242b7d4f8bfca6f08c57bf424014605fa0691f2cec05488d0")))
    }
  }

  it should "reject file that is too large (Body)" in {
    val content = "1" * (service.uploadSizeLimit.toInt + 1)
    val multipartForm = CoreClient.fileUploadForm(HttpEntity(ContentTypes.`text/plain(UTF-8)`, ByteString(content)), "Test.txt")

    val route = service.uploadPath { _ => service.uploadAction(None)(usr, ApplicationState(Map()))}
    Post(uploadPath, multipartForm) ~> route ~> check {
      assertResult(status)(StatusCodes.PayloadTooLarge)
    }
  }

  it should "reject file that is too large (Body Chunked)" in {
    val chunkSize = service.uploadSizeLimit.toInt / 1024
    val content = "1" * chunkSize
    val chunk = Chunk(content)

    val chunkCount = service.uploadSizeLimit.toInt / 1024 + 1
    val source = Source(List.fill(chunkCount)(chunk))

    val route = service.uploadPath { _ => service.uploadAction(None)(usr, ApplicationState(Map()))}
    val entity = Chunked(ContentTypes.`text/plain(UTF-8)`, source)
    Post(uploadPath, entity) ~> route ~> check {
      assertResult(status)(StatusCodes.PayloadTooLarge)
    }
  }
}
