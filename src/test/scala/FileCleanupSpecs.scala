package org.wabase

import com.typesafe.config.{Config, ConfigFactory}

import java.io.File
import java.nio.file.attribute.{BasicFileAttributeView, FileTime}
import java.nio.file.{Files, Paths}
import java.time.temporal.ChronoUnit
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.{ContentType, ContentTypes}
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import org.mojoz.metadata.in.YamlMd
import org.mojoz.metadata.out.DdlGenerator
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers
import org.tresql._
import org.wabase.AppMetadata.DbAccessKey
import org.wabase.ds.{PoolName, QueryTimeout}

import scala.concurrent._
import scala.concurrent.duration._

class FileCleanupSpecs extends FlatSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {
  behavior of "AppFileCleanup"

  import FileCleanupSpecsHelper._

  implicit val system: ActorSystem = ActorSystem("file-cleanup-specs")
  implicit val executor: ExecutionContext = system.dispatcher

  private var s3ServerStarted = false
  private lazy val s3Server = { s3ServerStarted = true; new S3TestServer().start() }

  override def afterAll(): Unit = {
    if (s3ServerStarted) s3Server.stop()
    Await.result(org.apache.pekko.http.scaladsl.Http().shutdownAllConnectionPools(), maxWait)
    Await.result(system.terminate(), maxWait)
    super.afterAll()
  }

  val schemaSql: String = DdlGenerator.hsqldb().schema(FileCleanupSpecsQuerease.tableMetadata.tableDefs)
  executeStatements(schemaSql.split(";\\s+").map(_ + ";").toIndexedSeq: _*)
  executeStatements("create sequence seq;")

  val maxWait = 1.minute
  val txtContentType = ContentTypes.`text/plain(UTF-8)`
  def saveFile(fileStreamer: AppFileStreamer[String], fileName: String, contentType: ContentType) = {
    implicit val user: String = "Guest"
    val fileInfoF = Source.single(ByteString(s"Content of $fileName"))
      .runWith(fileStreamer.fileSink(fileName,contentType.toString())).map(f => fileStreamer.getFileInfo(f.id, f.sha_256))
    Await.result(fileInfoF, maxWait).get
  }
  def saveFileAndRef(fileStreamer: TestFileStreamer, fileName: String, contentType: ContentType) = {
    val fileInfo = saveFile(fileStreamer, fileName, contentType)
    import fileStreamer._
    newTransaction(implicit res => Query(s"+$file_ref_table{id, ${file_info_table}_id} [#$file_info_table, ${fileInfo.id}]"))
    fileInfo
  }
  def removeRefs(fileStreamer: TestFileStreamer, fileInfo: AppFileStreamer.FileInfoHelper) = {
    newTransaction(implicit res => Query(s"-${fileStreamer.file_ref_table}[${fileStreamer.file_info_table}_id = ${fileInfo.id}]"))
  }

  def ageUploadInfo(fileStreamer: AppFileStreamer[_], fileInfo: AppFileStreamer.FileInfoHelper) =
    newTransaction(implicit res => Query(s"""${fileStreamer.file_info_table}[id = ${fileInfo.id}]{upload_time} = [date_sub(upload_time, sql("interval 2 day"))]"""))
  def ageFile(root: String, fileInfo: AppFileStreamer.FileInfoHelper) = {
    val path = Paths.get(s"$root/${fileInfo.path}")
    val view = Files.getFileAttributeView(path, classOf[BasicFileAttributeView])
    val attributes = view.readAttributes()
    val creationTime = attributes.creationTime()
    val newTime = FileTime.from(creationTime.toInstant.minus(2, ChronoUnit.DAYS))
    view.setTimes(newTime, newTime, newTime)
  }

  @annotation.nowarn("msg=Manifest")
  def fileInfoExists(fileStreamer: AppFileStreamer[_], fileInfo: AppFileStreamer.FileInfoHelper) =
    newTransaction(implicit res => Query(s"${fileStreamer.file_info_table}[id = ${fileInfo.id}]{count(1)}").unique[Long] == 1)
  @annotation.nowarn("msg=Manifest")
  def fileBodyInfoExists(fileStreamer: AppFileStreamer[_], fileInfo: AppFileStreamer.FileInfoHelper) =
    newTransaction(implicit res => Query(s"${fileStreamer.file_body_info_table}[${fileStreamer.shaColName} = '${fileInfo.sha_256}']{count(1)}").unique[Long] == 1)
  def fileExists(root: String, file: AppFileStreamer.FileInfoHelper) = new java.io.File(s"$root/${file.path}").exists()

  val filestreamerConfigs: List[List[TestFileStreamer]] = List("days", "hours", "minutes").flatMap { pf =>
    List(
      List(
        new TestFileStreamer1("nested/level1", pf),
        new TestFileStreamer2("nested/level1/level2", pf),
      ),
      List(
        new TestFileStreamer1("shared", pf),
        new TestFileStreamer2("shared", pf),
      ),
      List(
        new TestFileStreamer1("siblings/fs1", pf),
        new TestFileStreamer2("siblings/fs2", pf),
      ),
    )
  } :+ List( // different path formats in shared root
    new TestFileStreamer1("shared", "days"),
    new TestFileStreamer2("shared", "minutes"),
  )

  private def deleteFilesRecursively(file: File): Unit = {
    if (file.isDirectory)
      Option(file.listFiles).map(_.toSeq).getOrElse(Nil).foreach(deleteFilesRecursively)
    file.delete
  }

  def clearDb = newTransaction { implicit res =>
    Query("file_ref_1 - []")
    Query("file_info_1 - []")
    Query("file_body_info_1 - []")
    Query("file_ref_2 - []")
    Query("file_info_2 - []")
    Query("file_body_info_2 - []")
  }

  def clearFiles = {
    deleteFilesRecursively(new File(FileCleanupSpecsHelper.attachmentsRootPath))
  }

  override protected def beforeEach() = {
    clearDb
    clearFiles
  }

  filestreamerConfigs foreach { fileStreamerList =>
    val fsConfigName = fileStreamerList.head.attachmentsRootPathTail.split("/")(0) + " paths, " +
      fileStreamerList.map(_.pathFormat).distinct.mkString("/") + " path format"

    val fileCleaner =
      new TestFileCleanup(FileCleanupSpecsQuerease, FileCleanupSpecsHelper.db.rt, fileStreamerList: _*)
    def comfortFileCleaner = {
      fileStreamerList.foreach { fs =>
        new File(fs.rootPath + "/" + "tmp").mkdirs
      }
    }
    def doCleanup = {
      comfortFileCleaner
      Await.result(fileCleaner.doCleanup(), maxWait)
    }

    it should ("cleanup file body info with references removed, for " + fsConfigName) in {
      fileStreamerList.foreach { fs =>
        val fi = saveFileAndRef(fs, "textFile.txt", txtContentType)
        ageFile(fs.rootPath, fi)

        fileInfoExists(fs, fi) shouldBe true
        fileBodyInfoExists(fs, fi) shouldBe true
        fileExists(fs.rootPath, fi) shouldBe true

        doCleanup

        fileInfoExists(fs, fi) shouldBe true
        fileBodyInfoExists(fs, fi) shouldBe true
        fileExists(fs.rootPath, fi) shouldBe true

        ageUploadInfo(fs, fi)
        doCleanup

        fileInfoExists(fs, fi) shouldBe true
        fileBodyInfoExists(fs, fi) shouldBe true
        fileExists(fs.rootPath, fi) shouldBe true

        removeRefs(fs, fi)
        doCleanup

        fileInfoExists(fs, fi) shouldBe false
        fileBodyInfoExists(fs, fi) shouldBe false
        fileExists(fs.rootPath, fi) shouldBe false
      }
    }

    it should ("cleanup file body info only when all file info's are removed, for " + fsConfigName) in {
      fileStreamerList.foreach { fs =>
        val fi_1 = saveFile(fs, "textFile.txt", txtContentType)
        val fi_2 = saveFile(fs, "textFile.txt", txtContentType)
        ageFile(fs.rootPath, fi_1)

        fileInfoExists(fs, fi_1) shouldBe true
        fileBodyInfoExists(fs, fi_1) shouldBe true
        fileExists(fs.rootPath, fi_1) shouldBe true
        fileInfoExists(fs, fi_2) shouldBe true
        fileBodyInfoExists(fs, fi_2) shouldBe true
        fileExists(fs.rootPath, fi_2) shouldBe true

        doCleanup

        fileInfoExists(fs, fi_1) shouldBe true
        fileBodyInfoExists(fs, fi_1) shouldBe true
        fileExists(fs.rootPath, fi_1) shouldBe true
        fileInfoExists(fs, fi_2) shouldBe true
        fileBodyInfoExists(fs, fi_2) shouldBe true
        fileExists(fs.rootPath, fi_2) shouldBe true

        ageUploadInfo(fs, fi_1)
        doCleanup

        fileInfoExists(fs, fi_1) shouldBe false
        fileBodyInfoExists(fs, fi_1) shouldBe true
        fileExists(fs.rootPath, fi_1) shouldBe true
        fileInfoExists(fs, fi_2) shouldBe true
        fileBodyInfoExists(fs, fi_2) shouldBe true
        fileExists(fs.rootPath, fi_2) shouldBe true

        ageUploadInfo(fs, fi_2)
        doCleanup

        fileInfoExists(fs, fi_1) shouldBe false
        fileBodyInfoExists(fs, fi_1) shouldBe false
        fileExists(fs.rootPath, fi_1) shouldBe false
        fileInfoExists(fs, fi_2) shouldBe false
        fileBodyInfoExists(fs, fi_2) shouldBe false
        fileExists(fs.rootPath, fi_2) shouldBe false

        fileExists(fs.rootPath + "/trash", fi_2) shouldBe true
      }
    }

    it should ("cleanup file body only when all file info's across all filestreamers are removed, for " + fsConfigName) in {
      def test(fs_1: TestFileStreamer, fs_2: TestFileStreamer): Unit = {
        val fi_1 = saveFile(fs_1, "textFile.txt", txtContentType)
        val fi_2 = saveFile(fs_2, "textFile.txt", txtContentType)

        ageFile(fs_1.rootPath, fi_1)
        ageFile(fs_2.rootPath, fi_2)

        fileInfoExists(fs_1, fi_1) shouldBe true
        fileBodyInfoExists(fs_1, fi_1) shouldBe true
        fileExists(fs_1.rootPath, fi_1) shouldBe true
        fileInfoExists(fs_2, fi_2) shouldBe true
        fileBodyInfoExists(fs_2, fi_2) shouldBe true
        fileExists(fs_2.rootPath, fi_2) shouldBe true

        doCleanup

        fileInfoExists(fs_1, fi_1) shouldBe true
        fileBodyInfoExists(fs_1, fi_1) shouldBe true
        fileExists(fs_1.rootPath, fi_1) shouldBe true
        fileInfoExists(fs_2, fi_2) shouldBe true
        fileBodyInfoExists(fs_2, fi_2) shouldBe true
        fileExists(fs_2.rootPath, fi_2) shouldBe true

        ageUploadInfo(fs_1, fi_1)
        doCleanup

        fileInfoExists(fs_1, fi_1) shouldBe false
        fileBodyInfoExists(fs_1, fi_1) shouldBe false
        fileExists(fs_1.rootPath, fi_1) shouldBe (fs_1.rootPath == fs_2.rootPath && fi_1.path == fi_2.path) // same physical file
        fileInfoExists(fs_2, fi_2) shouldBe true
        fileBodyInfoExists(fs_2, fi_2) shouldBe true
        fileExists(fs_2.rootPath, fi_2) shouldBe true

        ageUploadInfo(fs_2, fi_2)
        doCleanup

        fileInfoExists(fs_1, fi_1) shouldBe false
        fileBodyInfoExists(fs_1, fi_1) shouldBe false
        fileExists(fs_1.rootPath, fi_1) shouldBe false
        fileInfoExists(fs_2, fi_2) shouldBe false
        fileBodyInfoExists(fs_2, fi_2) shouldBe false
        fileExists(fs_2.rootPath, fi_2) shouldBe false
      }
      // test nested paths both ways
      test(fileStreamerList(0), fileStreamerList(1))
      test(fileStreamerList(1), fileStreamerList(0))
    }

    it should ("cleanup file body info with upload time expired, for " + fsConfigName) in {
      fileStreamerList.foreach { fs =>
        val fi = saveFile(fs, "textFile.txt", txtContentType)
        ageFile(fs.rootPath, fi)

        fileInfoExists(fs, fi) shouldBe true
        fileBodyInfoExists(fs, fi) shouldBe true
        fileExists(fs.rootPath, fi) shouldBe true

        doCleanup
        fileInfoExists(fs, fi) shouldBe true
        fileBodyInfoExists(fs, fi) shouldBe true
        fileExists(fs.rootPath, fi) shouldBe true

        ageUploadInfo(fs, fi)
        doCleanup

        fileInfoExists(fs, fi) shouldBe false
        fileBodyInfoExists(fs, fi) shouldBe false
        fileExists(fs.rootPath, fi) shouldBe false
      }
    }

    it should ("cleanup file body info with file creation time expired, for " + fsConfigName) in {
      fileStreamerList.foreach { fs =>
        val fi = saveFile(fs, "textFile.txt", txtContentType)
        fileInfoExists(fs, fi) shouldBe true
        fileBodyInfoExists(fs, fi) shouldBe true
        fileExists(fs.rootPath, fi) shouldBe true

        ageUploadInfo(fs, fi)
        doCleanup

        fileInfoExists(fs, fi) shouldBe false
        fileBodyInfoExists(fs, fi) shouldBe false
        fileExists(fs.rootPath, fi) shouldBe true

        ageFile(fs.rootPath, fi)
        doCleanup

        fileInfoExists(fs, fi) shouldBe false
        fileBodyInfoExists(fs, fi) shouldBe false
        fileExists(fs.rootPath, fi) shouldBe false
      }
    }

    it should ("not move unreferenced file to trash if it is stored again after listing, for " + fsConfigName) in {
      fileStreamerList.foreach { fs =>
        val fi = saveFile(fs, s"unreferenced-${fs.file_info_table}.txt", txtContentType)
        // file is unreferenced but fresh - as if stored again at the same path after files_on_disk was filled
        newTransaction { implicit res =>
          Query(s"-${fs.file_info_table}[id = ${fi.id}]")
          Query(s"-${fs.file_body_info_table}[${fs.shaColName} = ?]", fi.sha_256)
          Query("files_on_disk - []")
          Query("+files_on_disk {path} [?]", s"${fs.storage.location}/${fi.path}")
        }

        Await.result(fileCleaner.moveUnreferencedFilesToTrash(), maxWait)
        fileExists(fs.rootPath, fi) shouldBe true
        fileExists(fs.rootPath + "/trash", fi) shouldBe false

        ageFile(fs.rootPath, fi)
        Await.result(fileCleaner.moveUnreferencedFilesToTrash(), maxWait)
        fileExists(fs.rootPath, fi) shouldBe false
        fileExists(fs.rootPath + "/trash", fi) shouldBe true
      }
    }

    it should ("cleanup file body info if file on disk missing, for " + fsConfigName) in {
      fileStreamerList.foreach { fs =>
        val fi = saveFile(fs, "textFile.txt", txtContentType)
        fileInfoExists(fs, fi) shouldBe true
        fileBodyInfoExists(fs, fi) shouldBe true
        fileExists(fs.rootPath, fi) shouldBe true

        new java.io.File(s"${fs.rootPath}/${fi.path}").delete()

        fileInfoExists(fs, fi) shouldBe true
        fileBodyInfoExists(fs, fi) shouldBe true
        fileExists(fs.rootPath, fi) shouldBe false

        ageUploadInfo(fs, fi)
        doCleanup

        fileInfoExists(fs, fi) shouldBe false
        fileBodyInfoExists(fs, fi) shouldBe false
        fileExists(fs.rootPath, fi) shouldBe false
      }
    }
  }

  it should "check min-age against path format period" in {
    import java.time.Duration.{ofHours, ofMinutes, ofDays}
    val ex = intercept[com.typesafe.config.ConfigException.BadValue] {
      FileCleanup.checkMinAge(ofHours(23), ofDays(1), "file:/files")
    }
    ex.getMessage should include ("app.file-cleanup.min-age (PT23H) must not be shorter than files.path-format period (PT24H)")
    ex.getMessage should include ("file:/files")
    FileCleanup.checkMinAge(ofDays(1), ofDays(1), "file:/files").get should include ("by at least PT1H")
    FileCleanup.checkMinAge(ofMinutes(30), ofMinutes(1), "file:/files") shouldBe defined
    FileCleanup.checkMinAge(ofHours(25), ofDays(1), "file:/files") shouldBe None
    FileCleanup.checkMinAge(ofHours(2), ofHours(1), "file:/files") shouldBe None
    FileCleanup.checkMinAge(FileCleanup.minAge, ofDays(1), "file:/files") shouldBe None // default config
  }

  it should "compute dedup window from min-age" in {
    import java.time.Duration.{ofHours, ofMinutes, ZERO}
    FileCleanup.dedupWindow(ofHours(25)) shouldBe ofHours(24)
    FileCleanup.dedupWindow(ofHours(2)) shouldBe ofHours(1)
    FileCleanup.dedupWindow(ofMinutes(90)) shouldBe ofMinutes(45)
    FileCleanup.dedupWindow(ofMinutes(10)) shouldBe ofMinutes(5)
    FileCleanup.dedupWindow(ZERO) shouldBe ZERO
  }

  it should "cleanup files in S3 storage" in {
    val bucket = "file-cleanup-specs"
    s3Server.createBucket(bucket)
    val tmpPath = Files.createTempDirectory("file-cleanup-specs-s3-tmp").toFile.getAbsolutePath
    val fs = new TestS3FileStreamer(s3Server.filesConfig(s"$bucket/files", tmpPath))
    // S3 last modified time can not be changed - files and file_info records are aged by zero min-age
    val cleaner = new TestFileCleanup(FileCleanupSpecsQuerease, FileCleanupSpecsHelper.db.rt, fs) {
      override lazy val minAgeMillis: Long = 0L
    }
    def stored(name: String, size: Long, sha: String) =
      Await.result(fs.storage.verify(name, size, sha), maxWait).contains(true)
    def exists(fi: AppFileStreamer.FileInfoHelper) = stored(fi.path, fi.size, fi.sha_256)
    def trashed(fi: AppFileStreamer.FileInfoHelper) = stored(s"trash/${fi.path}", fi.size, fi.sha_256)

    val referenced   = saveFileAndRef(fs, "referenced.txt", txtContentType)
    val unreferenced = saveFile(fs, "unreferenced.txt", txtContentType)
    exists(referenced) shouldBe true
    exists(unreferenced) shouldBe true
    Thread.sleep(1100) // S3 last modified and upload time must be older than now

    Await.result(cleaner.doCleanup(), maxWait)
    fileInfoExists(fs, referenced) shouldBe true
    exists(referenced) shouldBe true
    fileInfoExists(fs, unreferenced) shouldBe false
    fileBodyInfoExists(fs, unreferenced) shouldBe false
    exists(unreferenced) shouldBe false
    trashed(unreferenced) shouldBe true

    Await.result(cleaner.doCleanup(), maxWait)
    trashed(unreferenced) shouldBe false
    exists(referenced) shouldBe true
  }
}



object FileCleanupSpecsHelper {
  implicit val queryTimeout: QueryTimeout = QueryTimeout(10)

  DbDrivers.loadDrivers
  class FileCleanupDbAccess extends DbAccess with QuereaseProvider with Loggable { self =>
    override protected def tresqlMetadata = FileCleanupSpecsQuerease.tresqlMetadata
    override protected def initQuereaseIo: AppQuereaseIo[Dto] = new AppQuereaseIo[Dto](FileCleanupSpecsQuerease)
    def rt: ResourcesTemplate = resourcesTemplate
  }
  val db: FileCleanupDbAccess = new FileCleanupDbAccess

  val attachmentsRootPath = {
    val slash = System.getProperty("file.separator")
    val tmpdir = System.getProperty("java.io.tmpdir")
    tmpdir.stripSuffix(slash) + slash + "fs-test-uploads"
  }

  val fsCfg1 = FileStreamerConfig.configs("TestFileStreamer1")
  val fsCfg2 = FileStreamerConfig.configs("TestFileStreamer2")

  val TestCp: PoolName = PoolName("file-cleanup-test")
  val extraDbs: Seq[DbAccessKey] = Nil
  class TestFileStreamer(config: Config, val attachmentsRootPathTail: String, val pathFormat: String)
      extends AppFileStreamer[String]
      with DbAccessProvider {
    override def dbAccess = db
    override lazy val fileStreamerConfig =
      ConfigFactory.parseString(s"files.root = $attachmentsRootPath/$attachmentsRootPathTail\nfiles.path-format = $pathFormat")
        .withFallback(config)
    val file_ref_table: String = config.getString("file-ref-table")
  }
  class TestS3FileStreamer(s3FilesConfig: Config)
      extends TestFileStreamer(fsCfg1, "s3", "days") {
    override lazy val fileStreamerConfig = s3FilesConfig.withFallback(fsCfg1)
  }
  class TestFileStreamer1(attachmentsRootPathTail: String, pathFormat: String)
      extends TestFileStreamer(fsCfg1, attachmentsRootPathTail, pathFormat) {
  }
  class TestFileStreamer2(attachmentsRootPathTail: String, pathFormat: String)
      extends TestFileStreamer(fsCfg2, attachmentsRootPathTail, pathFormat) {
  }
  object FileCleanupSpecsQuerease extends AppQuerease {
    override lazy val yamlMetadata = YamlMd.fromResource("/filestreamer-specs-table-metadata.yaml")
  }
  val qe = FileCleanupSpecsQuerease
  def newTransaction[A](f: Resources => A): A = db.newTransaction(TestCp, extraDb = extraDbs)(f)
  def executeStatements(statements: String*) = newTransaction{ res =>
    val statement = res.conn.createStatement
    try statements foreach { statement.execute } finally statement.close()
  }
  class TestFileCleanup(qe: AppQuerease, res: Resources, fileStreamers: AppFileStreamerConfig*)
      extends AppFileCleanup(qe, res, fileStreamers: _*) with QuereaseProvider {
    override implicit lazy val connectionPool: PoolName = TestCp
    override protected lazy val deleteLimitOpt: Option[Int] = Some(1)
    // every test exercises batching and paging of files_on_disk
    override protected lazy val batchSize: Int = 1
    def moveUnreferencedFilesToTrash()(implicit ec: ExecutionContext, as: ActorSystem): Future[Unit] =
      compareDataAndMoveFilesToTrash
  }
}
