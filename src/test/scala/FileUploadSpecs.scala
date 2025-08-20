package org.wabase


import com.typesafe.config.ConfigFactory
import java.io.File
import java.nio.file.Files
import java.util.UUID
import org.apache.pekko.http.scaladsl.model.{ContentTypes, EntityStreamSizeException, HttpEntity, StatusCodes}
import org.apache.pekko.http.scaladsl.model.HttpEntity.{Chunk, Chunked, Default}
import org.apache.pekko.http.scaladsl.server.Directives.{complete, handleExceptions}
import org.apache.pekko.http.scaladsl.server.ExceptionHandler
import org.apache.pekko.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import org.scalatest.flatspec.AnyFlatSpec
import org.wabase.client.WabaseHttpClient

import scala.concurrent.duration
import scala.concurrent.duration.{Duration, FiniteDuration}

class FileUploadSpecs extends AnyFlatSpec with TestQuereaseInitializer with ScalatestRouteTest {

  var streamerConfQe: QuereaseProvider with AppFileStreamerConfig = _

  var service: TestAppService = _

  private val uploadTestsDb = "file-upload-tests"
  override protected def dbNamePrefix: String = uploadTestsDb
  override def beforeAll(): Unit = {
    querease = new TestQuerease("/filestreamer-specs-table-metadata.yaml") {
      override lazy val defaultCpName = uploadTestsDb
    }
    super.beforeAll()

    val db = new DbAccess with QuereaseProvider with Loggable {
      override val DefaultCp: PoolName = PoolName(uploadTestsDb)
      override val tresqlResources  = FileUploadSpecs.this.tresqlThreadLocalResources
      override protected def tresqlMetadata = querease.tresqlMetadata
      override protected def initQuerease: AppQuerease = querease
      override protected def initQuereaseIo: AppQuereaseIo[Dto] = new AppQuereaseIo[Dto](querease)
    }

    val appl = new TestApp {
      override val DefaultCp: PoolName = PoolName(uploadTestsDb)
      override def dbAccessDelegate = db
      override protected def initQuerease = querease
      private val root_path =
        new File(System.getProperty("java.io.tmpdir"),"file-upload-specs/" + UUID.randomUUID().toString).getPath
      override lazy val fileStreamerConfig =
        ConfigFactory.parseString(s"files.path = $root_path")
          .withFallback(FileStreamerConfig.configs("main"))
    }

    streamerConfQe = appl

    def entityStreamSizeExceptionHandler(logger: com.typesafe.scalalogging.Logger) = ExceptionHandler {
      case e: EntityStreamSizeException =>
        logger.debug("File upload specs: Stream size exception", e)
        complete(StatusCodes.ContentTooLarge)
    }

    service = new TestAppService(system) {
      override def initApp: App = appl
      override def initFileStreamer = appl
      override lazy val appExceptionHandler =
        entityStreamSizeExceptionHandler(this.logger)
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

  implicit val routeTimeout: RouteTestTimeout = RouteTestTimeout(FiniteDuration(5, duration.SECONDS))
  implicit val responseTimeout: Duration = Duration("5s")

  "File upload" should "work" in {
    val content = List.fill(10000)(ByteString("FILE CONTENT UTF-8 (зимние rūķīši) "))
    val source = Source(content)

    val route = service.uploadPath { _ => service.uploadAction(None)(usr, ApplicationState(Map()))}
    val entity = Default(ContentTypes.`text/plain(UTF-8)`, content.length, source)
    Post(uploadPath, entity) ~> route ~> check {
      implicit val m = service.toMapUnmarshaller
      val res = responseAs[Map[String, Any]]
      assertResult(res.get("sha_256"))(Some("718004c597c5343242b7d4f8bfca6f08c57bf424014605fa0691f2cec05488d0"))
    }
  }

  it should "work (Multipart)" in {
    val contentSent = "FILE CONTENT UTF-8 (зимние rūķīši) " * 10000
    val multipartForm = WabaseHttpClient.fileUploadForm(HttpEntity(ContentTypes.`text/plain(UTF-8)`, ByteString(contentSent)), "Test.txt")

    val route = service.uploadPath { _ => service.uploadAction(None)(usr, ApplicationState(Map()))}
    Post(uploadPath, multipartForm) ~> route ~> check {
      implicit val m = service.toMapUnmarshaller
      val res = responseAs[Map[String, Any]]
      assertResult(res.get("sha_256"))(Some("718004c597c5343242b7d4f8bfca6f08c57bf424014605fa0691f2cec05488d0"))
    }
  }

  it should "reject file that is too large (Body)" in {
    val content = "1" * (service.uploadSizeLimit.toInt + 1)
    val multipartForm = WabaseHttpClient.fileUploadForm(HttpEntity(ContentTypes.`text/plain(UTF-8)`, ByteString(content)), "Test.txt")

    val route = handleExceptions(service.appExceptionHandler) {
      service.uploadPath { _ => service.uploadAction(None)(usr, ApplicationState(Map()))}
    }
    Post(uploadPath, multipartForm) ~> route ~> check {
      assertResult(status)(StatusCodes.ContentTooLarge)
    }
  }

  it should "reject file that is too large (Body Chunked)" in {
    val chunkSize = service.uploadSizeLimit.toInt / 1024
    val content = "1" * chunkSize
    val chunk = Chunk(content)

    val chunkCount = service.uploadSizeLimit.toInt / 1024 + 1
    val source = Source(List.fill(chunkCount)(chunk))

    val route = handleExceptions(service.appExceptionHandler) {
      service.uploadPath { _ => service.uploadAction(None)(usr, ApplicationState(Map()))}
    }
    val entity = Chunked(ContentTypes.`text/plain(UTF-8)`, source)
    Post(uploadPath, entity) ~> route ~> check {
      assertResult(status)(StatusCodes.ContentTooLarge)
    }
  }
}
