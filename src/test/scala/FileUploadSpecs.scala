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
import org.scalatest.matchers.should.Matchers
import org.wabase.client.WabaseHttpClient
import org.wabase.ds.PoolName

import scala.concurrent.{Await, Future, duration}
import scala.concurrent.duration.{Duration, FiniteDuration}

class FileUploadSpecs extends AnyFlatSpec with Matchers with TestQuereaseInitializer with ScalatestRouteTest {

  var streamerConfQe: QuereaseProvider with AppFileStreamerConfig = _
  var app: TestApp = _
  var testDb: DbAccess = _
  var s3App: TestApp = _
  private val s3Server = new S3TestServer
  private val s3Bucket = "file-upload-specs"
  private val s3TmpPath = Files.createTempDirectory("file-upload-specs-s3-tmp").toFile

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
        ConfigFactory.parseString(s"files.root = $root_path")
          .withFallback(FileStreamerConfig.configs("main"))
    }

    streamerConfQe = appl
    testDb = db
    app = appl

    s3Server.start().createBucket(s3Bucket)
    s3App = new TestApp {
      override val DefaultCp: PoolName = PoolName(uploadTestsDb)
      override def dbAccessDelegate = db
      override protected def initQuerease = querease
      override lazy val fileStreamerConfig =
        s3Server.filesConfig(s"$s3Bucket/uploads", s3TmpPath.getAbsolutePath)
          .withFallback(FileStreamerConfig.configs("main"))
    }

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
    s3Server.stop()
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

  private def saveFile(content: String): Future[AppFileStreamer.FileInfo] = {
    implicit val user: TestUsr = usr
    Source.single(ByteString(content)).runWith(app.fileSink("test.txt", "text/plain"))
  }
  private def storedPath(fi: AppFileStreamer.FileInfo): String =
    app.getFileInfo(fi.id, fi.sha_256)(usr).get.path
  private def storedFile(fi: AppFileStreamer.FileInfo): File =
    new File(app.rootPath + "/" + storedPath(fi))
  private def tmpFileCount = Option(new File(app.tmpPath).list).map(_.length).getOrElse(0)

  it should "store again stored file that went missing" in {
    val content = "File upload specs - missing stored file"
    val fi_1 = Await.result(saveFile(content), responseTimeout)
    storedFile(fi_1).delete() shouldBe true

    val fi_2 = Await.result(saveFile(content), responseTimeout)
    fi_2.sha_256 shouldBe fi_1.sha_256
    val f = storedFile(fi_2)
    f.exists shouldBe true
    new String(Files.readAllBytes(f.toPath), "UTF-8") shouldBe content
  }

  it should "store again stored file that is corrupt" in {
    val content = "File upload specs - corrupt stored file"
    val fi_1 = Await.result(saveFile(content), responseTimeout)
    Files.write(storedFile(fi_1).toPath, "corrupt".getBytes("UTF-8"))

    val tmpCountBefore = tmpFileCount
    val fi_2 = Await.result(saveFile(content), responseTimeout)
    fi_2.sha_256 shouldBe fi_1.sha_256
    readFile(fi_2) shouldBe content
    readFile(fi_1) shouldBe content // single file body info - old file info refers to new file body
    tmpFileCount shouldBe tmpCountBefore // temp file consumed
  }

  private def newTransaction[A](f: org.tresql.Resources => A): A =
    testDb.newTransaction(PoolName(uploadTestsDb))(f)
  private def ageUploadInfo(fi: AppFileStreamer.FileInfo) = newTransaction { implicit res =>
    org.tresql.Query(
      s"""${app.file_info_table}[id = ?]{upload_time} = [date_sub(upload_time, sql("interval 2 day"))]""", fi.id)
  }
  private def readFile(fi: AppFileStreamer.FileInfo): String =
    new String(Files.readAllBytes(storedFile(fi).toPath), "UTF-8")
  private val oldTime = System.currentTimeMillis - 2 * 24 * 3600 * 1000L

  it should "deduplicate file uploaded within dedup window" in {
    val content = "File upload specs - dedup within window"
    val fi_1 = Await.result(saveFile(content), responseTimeout)
    val stored = storedFile(fi_1)
    stored.setLastModified(oldTime) shouldBe true // marker - stored file is not written again

    val tmpCountBefore = tmpFileCount
    val fi_2 = Await.result(saveFile(content), responseTimeout)
    fi_2.id should not be fi_1.id
    storedFile(fi_2) shouldBe stored
    stored.lastModified shouldBe oldTime
    tmpFileCount shouldBe tmpCountBefore // temp file deleted
  }

  it should "store file again if stored body is out of dedup window" in {
    val content = "File upload specs - dedup out of window"
    val fi_1 = Await.result(saveFile(content), responseTimeout)
    ageUploadInfo(fi_1)
    // corrupt stored file - it is not verified out of dedup window, but stored again
    Files.write(storedFile(fi_1).toPath, "corrupt".getBytes("UTF-8"))

    val fi_2 = Await.result(saveFile(content), responseTimeout)
    readFile(fi_2) shouldBe content
    readFile(fi_1) shouldBe content // single file body info - old file info refers to new file body

    // fi_2 is recent - dedup window applies again
    val stored = storedFile(fi_2)
    stored.setLastModified(oldTime) shouldBe true
    val fi_3 = Await.result(saveFile(content), responseTimeout)
    storedFile(fi_3) shouldBe stored
    stored.lastModified shouldBe oldTime
  }

  it should "store file again if file body info is removed by file cleanup but file is still on disk" in {
    val content = "File upload specs - body info removed"
    val fi_1 = Await.result(saveFile(content), responseTimeout)
    val stored = storedFile(fi_1)
    stored.setLastModified(oldTime) shouldBe true
    newTransaction { implicit res =>
      org.tresql.Query(s"-${app.file_info_table}[id = ?]", fi_1.id)
      org.tresql.Query(s"-${app.file_body_info_table}[${app.shaColName} = ?]", fi_1.sha_256)
    }

    val fi_2 = Await.result(saveFile(content), responseTimeout)
    app.getFileInfo(fi_2.id, fi_2.sha_256)(usr).map(_.path) shouldBe Some(storedPath(fi_2))
    readFile(fi_2) shouldBe content
    storedFile(fi_2).lastModified should be > oldTime // stored again, not deduplicated
  }

  it should "upload, deduplicate and download file with S3 storage" in {
    implicit val user: TestUsr = usr
    val content = "File upload specs - S3 storage"
    def upload = Await.result(
      Source.single(ByteString(content)).runWith(s3App.fileSink("s3.txt", "text/plain")), responseTimeout)
    def download(fi: AppFileStreamer.FileInfo) = Await.result(
      s3App.getFileInfo(fi.id, fi.sha_256).get.source.runFold(ByteString.empty)(_ ++ _), responseTimeout).utf8String

    val fi_1 = upload
    download(fi_1) shouldBe content
    s3TmpPath.list.length shouldBe 0 // temp file uploaded and deleted

    val fi_2 = upload // deduplicated - stored file verified with S3 HEAD
    fi_2.id should not be fi_1.id
    s3App.getFileInfo(fi_2.id, fi_2.sha_256).get.path shouldBe s3App.getFileInfo(fi_1.id, fi_1.sha_256).get.path
    download(fi_2) shouldBe content
    s3TmpPath.list.length shouldBe 0
  }
}
