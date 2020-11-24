package org.wabase

import java.io.File
import java.nio.file.attribute.{BasicFileAttributeView, FileTime}
import java.nio.file.{Files, Paths}
import java.time.temporal.ChronoUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentType, ContentTypes}
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.mojoz.metadata.in.YamlMd
import org.mojoz.metadata.out.SqlGenerator
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers
import org.tresql._

import scala.concurrent._
import scala.concurrent.duration._

class FileCleanupSpecs extends FlatSpec with Matchers with BeforeAndAfterEach {
  behavior of "AppFileCleanup"

  import FileCleanupSpecsHelper._
  import FileCleanupSpecsHelper.db._

  val schemaSql: String = SqlGenerator.hsqldb().schema(FileCleanupSpecsQuerease.tableMetadata.tableDefs)
  transaction {
    executeStatements(schemaSql.split(";\\s+").map(_ + ";").toIndexedSeq: _*)
    executeStatements("create sequence seq;")
  }

  val maxWait = 1.minute
  val txtContentType = ContentTypes.`text/plain(UTF-8)`
  val execution = new ExecutionImpl()(ActorSystem("file-cleanup-spec-system"))
  def saveFile(fileStreamer: AppFileStreamer[String], fileName: String, contentType: ContentType) = {
    implicit val system = ActorSystem("file-cleanup-specs")
    implicit val executor = system.dispatcher
    implicit val user: String = "Guest"
    val fileInfoF = Source.single(ByteString(s"Content of $fileName"))
      .runWith(fileStreamer.fileSink(fileName,contentType.toString())).map(f => fileStreamer.getFileInfo(f.id, f.sha_256))
    Await.result(fileInfoF, maxWait).get
  }
  def saveFileAndRef(fileStreamer: TestFileStreamer, fileName: String, contentType: ContentType) = {
    val fileInfo = saveFile(fileStreamer, fileName, contentType)
    import fileStreamer._
    transaction(Query(s"+$file_ref_table{id, ${file_info_table}_id} [#$file_info_table, ${fileInfo.id}]"))
    fileInfo
  }
  def removeRefs(fileStreamer: TestFileStreamer, fileInfo: AppFileStreamer.FileInfoHelper) = {
    transaction(Query(s"-${fileStreamer.file_ref_table}[${fileStreamer.file_info_table}_id = ${fileInfo.id}]"))
  }

  def ageUploadInfo(fileStreamer: AppFileStreamer[_], fileInfo: AppFileStreamer.FileInfoHelper) =
    transaction(Query(s"""${fileStreamer.file_info_table}[${fileInfo.id}]{upload_time} = [date_sub(upload_time, sql("interval 1 day"))]"""))
  def ageFile(fileInfo: AppFileStreamer.FileInfoHelper) = {
    val path = Paths.get(fileInfo.path)
    val view = Files.getFileAttributeView(path, classOf[BasicFileAttributeView])
    val attributes = view.readAttributes()
    val creationTime = attributes.creationTime()
    val newTime = FileTime.from(creationTime.toInstant.minus(1, ChronoUnit.DAYS))
    view.setTimes(newTime, newTime, newTime)
  }

  def fileInfoExists(fileStreamer: AppFileStreamer[_], fileInfo: AppFileStreamer.FileInfoHelper) =
    dbUse(Query(s"${fileStreamer.file_info_table}[${fileInfo.id}]{count(1)}").unique[Long] == 1)
  def fileBodyInfoExists(fileStreamer: AppFileStreamer[_], fileInfo: AppFileStreamer.FileInfoHelper) =
    dbUse(Query(s"${fileStreamer.file_body_info_table}[${fileStreamer.shaColName} = '${fileInfo.sha_256}']{count(1)}").unique[Long] == 1)
  def fileExists(file: AppFileStreamer.FileInfoHelper) = new java.io.File(file.path).exists()

  val filestreamerConfigs: List[List[TestFileStreamer]] = List(
    List(
      new TestFileStreamer1("nested/level1"),
      new TestFileStreamer2("nested/level1/level2"),
    ),
    List(
      new TestFileStreamer1("shared"),
      new TestFileStreamer2("shared"),
    ),
    List(
      new TestFileStreamer1("siblings/fs1"),
      new TestFileStreamer2("siblings/fs2"),
    ),
  )

  private def deleteFilesRecursively(file: File): Unit = {
    if (file.isDirectory)
      Option(file.listFiles).map(_.toSeq).getOrElse(Nil).foreach(deleteFilesRecursively)
    file.delete
  }

  def clearDb = transaction {
    Query("file_ref_1 - []")
    Query("file_info_1 - []")
    Query("file_body_info_1 - []")
    Query("file_ref_2 - []")
    Query("file_info_2 - []")
    Query("file_body_info_2 - []")
  }

  def clearFiles = {
    deleteFilesRecursively(new File(new TestFileStreamer(".").attachmentsRootPath))
  }

  override protected def beforeEach() = {
    clearDb
    clearFiles
  }

  filestreamerConfigs foreach { fileStreamerList =>
    val fsConfigName = fileStreamerList.head.attachmentsRootPathTail.split("/")(0) + " paths"
    val fileCleaner = new TestFileCleanup(db, fileStreamerList: _*)
    def doCleanup = fileCleaner.doCleanup(execution.system.log)

    it should ("cleanup file body info with references removed, for " + fsConfigName) in {
      fileStreamerList.foreach { fs =>
        val fi = saveFileAndRef(fs, "textFile.txt", txtContentType)
        ageFile(fi)

        fileInfoExists(fs, fi) shouldBe true
        fileBodyInfoExists(fs, fi) shouldBe true
        fileExists(fi) shouldBe true

        doCleanup

        fileInfoExists(fs, fi) shouldBe true
        fileBodyInfoExists(fs, fi) shouldBe true
        fileExists(fi) shouldBe true

        ageUploadInfo(fs, fi)
        doCleanup

        fileInfoExists(fs, fi) shouldBe true
        fileBodyInfoExists(fs, fi) shouldBe true
        fileExists(fi) shouldBe true

        removeRefs(fs, fi)
        doCleanup

        fileInfoExists(fs, fi) shouldBe false
        fileBodyInfoExists(fs, fi) shouldBe false
        fileExists(fi) shouldBe false
      }
    }

    it should ("cleanup file body info only when all file info's are removed, for " + fsConfigName) in {
      fileStreamerList.foreach { fs =>
        val fi_1 = saveFile(fs, "textFile.txt", txtContentType)
        val fi_2 = saveFile(fs, "textFile.txt", txtContentType)
        ageFile(fi_1)

        fileInfoExists(fs, fi_1) shouldBe true
        fileBodyInfoExists(fs, fi_1) shouldBe true
        fileExists(fi_1) shouldBe true
        fileInfoExists(fs, fi_2) shouldBe true
        fileBodyInfoExists(fs, fi_2) shouldBe true
        fileExists(fi_2) shouldBe true

        doCleanup

        fileInfoExists(fs, fi_1) shouldBe true
        fileBodyInfoExists(fs, fi_1) shouldBe true
        fileExists(fi_1) shouldBe true
        fileInfoExists(fs, fi_2) shouldBe true
        fileBodyInfoExists(fs, fi_2) shouldBe true
        fileExists(fi_2) shouldBe true

        ageUploadInfo(fs, fi_1)
        doCleanup

        fileInfoExists(fs, fi_1) shouldBe false
        fileBodyInfoExists(fs, fi_1) shouldBe true
        fileExists(fi_1) shouldBe true
        fileInfoExists(fs, fi_2) shouldBe true
        fileBodyInfoExists(fs, fi_2) shouldBe true
        fileExists(fi_2) shouldBe true

        ageUploadInfo(fs, fi_2)
        doCleanup

        fileInfoExists(fs, fi_1) shouldBe false
        fileBodyInfoExists(fs, fi_1) shouldBe false
        fileExists(fi_1) shouldBe false
        fileInfoExists(fs, fi_2) shouldBe false
        fileBodyInfoExists(fs, fi_2) shouldBe false
        fileExists(fi_2) shouldBe false

        val fi_2_new_path = fi_2.path.replace(fs.rootPath + "/", fs.rootPath + "/trash/")
        fileExists(fi_2.copy(path = fi_2_new_path)) shouldBe true
      }
    }

    it should ("cleanup file body only when all file info's across all filestreamers are removed, for " + fsConfigName) in {
      def test(fs_1: TestFileStreamer, fs_2: TestFileStreamer): Unit = {
        val fi_1 = saveFile(fs_1, "textFile.txt", txtContentType)
        val fi_2 = saveFile(fs_2, "textFile.txt", txtContentType)

        ageFile(fi_1)
        ageFile(fi_2)

        fileInfoExists(fs_1, fi_1) shouldBe true
        fileBodyInfoExists(fs_1, fi_1) shouldBe true
        fileExists(fi_1) shouldBe true
        fileInfoExists(fs_2, fi_2) shouldBe true
        fileBodyInfoExists(fs_2, fi_2) shouldBe true
        fileExists(fi_2) shouldBe true

        doCleanup

        fileInfoExists(fs_1, fi_1) shouldBe true
        fileBodyInfoExists(fs_1, fi_1) shouldBe true
        fileExists(fi_1) shouldBe true
        fileInfoExists(fs_2, fi_2) shouldBe true
        fileBodyInfoExists(fs_2, fi_2) shouldBe true
        fileExists(fi_2) shouldBe true

        ageUploadInfo(fs_1, fi_1)
        doCleanup

        fileInfoExists(fs_1, fi_1) shouldBe false
        fileBodyInfoExists(fs_1, fi_1) shouldBe false
        fileExists(fi_1) shouldBe (fs_1.rootPath == fs_2.rootPath)
        fileInfoExists(fs_2, fi_2) shouldBe true
        fileBodyInfoExists(fs_2, fi_2) shouldBe true
        fileExists(fi_2) shouldBe true

        ageUploadInfo(fs_2, fi_2)
        doCleanup

        fileInfoExists(fs_1, fi_1) shouldBe false
        fileBodyInfoExists(fs_1, fi_1) shouldBe false
        fileExists(fi_1) shouldBe false
        fileInfoExists(fs_2, fi_2) shouldBe false
        fileBodyInfoExists(fs_2, fi_2) shouldBe false
        fileExists(fi_2) shouldBe false
      }
      // test nested paths both ways
      test(fileStreamerList(0), fileStreamerList(1))
      test(fileStreamerList(1), fileStreamerList(0))
    }

    it should ("cleanup file body info with upload time expired, for " + fsConfigName) in {
      fileStreamerList.foreach { fs =>
        val fi = saveFile(fs, "textFile.txt", txtContentType)
        ageFile(fi)

        fileInfoExists(fs, fi) shouldBe true
        fileBodyInfoExists(fs, fi) shouldBe true
        fileExists(fi) shouldBe true

        doCleanup
        fileInfoExists(fs, fi) shouldBe true
        fileBodyInfoExists(fs, fi) shouldBe true
        fileExists(fi) shouldBe true

        ageUploadInfo(fs, fi)
        doCleanup

        fileInfoExists(fs, fi) shouldBe false
        fileBodyInfoExists(fs, fi) shouldBe false
        fileExists(fi) shouldBe false
      }
    }

    it should ("cleanup file body info with file creation time expired, for " + fsConfigName) in {
      fileStreamerList.foreach { fs =>
        val fi = saveFile(fs, "textFile.txt", txtContentType)
        fileInfoExists(fs, fi) shouldBe true
        fileBodyInfoExists(fs, fi) shouldBe true
        fileExists(fi) shouldBe true

        ageUploadInfo(fs, fi)
        doCleanup

        fileInfoExists(fs, fi) shouldBe false
        fileBodyInfoExists(fs, fi) shouldBe false
        fileExists(fi) shouldBe true

        ageFile(fi)
        doCleanup

        fileInfoExists(fs, fi) shouldBe false
        fileBodyInfoExists(fs, fi) shouldBe false
        fileExists(fi) shouldBe false
      }
    }

    it should ("cleanup file body info if file on disk missing, for " + fsConfigName) in {
      fileStreamerList.foreach { fs =>
        val fi = saveFile(fs, "textFile.txt", txtContentType)
        fileInfoExists(fs, fi) shouldBe true
        fileBodyInfoExists(fs, fi) shouldBe true
        fileExists(fi) shouldBe true

        new java.io.File(fi.path).delete()

        fileInfoExists(fs, fi) shouldBe true
        fileBodyInfoExists(fs, fi) shouldBe true
        fileExists(fi) shouldBe false

        ageUploadInfo(fs, fi)
        doCleanup

        fileInfoExists(fs, fi) shouldBe false
        fileBodyInfoExists(fs, fi) shouldBe false
        fileExists(fi) shouldBe false
      }
    }
  }
}

object FileCleanupSpecsHelper {
  implicit val queryTimeout = QueryTimeout(10)

  val db: DbAccess = new DbAccess with Loggable {
    override implicit lazy val tresqlResources: ThreadLocalResources = new TresqlResources {
      override lazy val resourcesTemplate = super.resourcesTemplate
        .copy(dialect = dialects.HSQLDialect)
        .copy(idExpr = s => "nextval('seq')")
        .copy(metadata = FileCleanupSpecsQuerease.tresqlMetadata)
    }
  }
  import db._

  implicit val TestCp = PoolName("file-cleanup-test")
  class TestFileStreamer(val attachmentsRootPathTail: String) extends AppFileStreamer[String]
    with AppConfig with QuereaseProvider with DbAccessProvider {
    override type QE = FileCleanupSpecsQuerease.type
    override protected def initQuerease = FileCleanupSpecsQuerease
    override def dbAccess = db
    override protected def fileStreamerConnectionPool: PoolName = TestCp
    override lazy val appConfig = null
    lazy val attachmentsRootPath = System.getProperty("java.io.tmpdir") + "/" + "fs-test-uploads"
    override lazy val rootPath = attachmentsRootPath + "/" + attachmentsRootPathTail
    lazy val file_ref_table: String = null
  }
  class TestFileStreamer1(attachmentsRootPathTail: String) extends TestFileStreamer(attachmentsRootPathTail) {
    override lazy val file_ref_table = "file_ref_1"
    override lazy val file_info_table = "file_info_1"
    override lazy val file_body_info_table = "file_body_info_1"
  }
  class TestFileStreamer2(attachmentsRootPathTail: String) extends TestFileStreamer(attachmentsRootPathTail) {
    override lazy val file_ref_table = "file_ref_2"
    override lazy val file_info_table = "file_info_2"
    override lazy val file_body_info_table = "file_body_info_2"
  }
  object FileCleanupSpecsQuerease extends AppQuerease {
    override lazy val yamlMetadata = YamlMd.fromResource("/filestreamer-specs-table-metadata.yaml")
  }
  val qe = FileCleanupSpecsQuerease
  def executeStatements(statements: String*) = transaction {
    val conn = db.tresqlResources.conn
    val statement = conn.createStatement
    try statements foreach { statement.execute } finally statement.close()
  }
  class TestFileCleanup(db: DbAccess, fileStreamers: AppFileStreamerConfig*)
      extends AppFileCleanup(db, fileStreamers: _*) with QuereaseProvider {
    override type QE = FileCleanupSpecsQuerease.type
    override protected def initQuerease = FileCleanupSpecsQuerease
    override implicit lazy val connectionPool = TestCp
    override lazy val ageCheckSql: String = "now() - interval 1 day"
  }
}
