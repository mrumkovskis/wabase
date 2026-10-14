package org.wabase

import java.io.File
import java.math.BigInteger
import java.nio.file.{Files, StandardCopyOption}
import java.security.MessageDigest
import org.apache.pekko.http.scaladsl.model.EntityStreamSizeException
import com.typesafe.config.Config

import scala.collection.immutable.TreeMap
import scala.concurrent.{ExecutionContext, Future}
import org.apache.pekko.stream._
import org.apache.pekko.stream.scaladsl._
import org.apache.pekko.util.ByteString
import org.tresql._
import org.wabase.ds.PoolName

import scala.util.Failure

object AppFileStreamer {
  /** User facing message template, registered in wabase_en and wabase_lv resource bundles */
  val CannotProcessFileMessage = "Cannot process file, please contact administrator: %1$s"

  private class FieldOrdering(val nameToIndex: Map[String, Int]) extends Ordering[String] {
    override def compare(x: String, y: String) =
      nameToIndex.getOrElse(x, 999) - nameToIndex.getOrElse(y, 999)
  }
  private val fieldNames = "id, filename, upload_time, content_type, size, sha_256, path"
    .split(", ").toList
  private val nameToIndex = fieldNames.zipWithIndex.toMap
  private val fieldOrdering = new FieldOrdering(nameToIndex)
  case class FileInfo(
    id: java.lang.Long,
    filename: String,
    upload_time: java.sql.Timestamp,
    content_type: String,
    size: java.lang.Long,
    sha_256: String,
  ) {
    def this(r: RowLike) = this(
      id = r.jLong("id"),
      filename = r.string("filename"),
      upload_time = r.timestamp("upload_time"),
      content_type = r.string("content_type"),
      sha_256 = r.string("sha_256"),
      size = r.jLong("size"),
    )
    def toMap: Map[String, Any] =
      TreeMap[String, Any](
        "id" -> id, "filename" -> filename, "upload_time" -> upload_time,
        "content_type" -> content_type, "sha_256" -> sha_256, "size" -> size
      )(fieldOrdering)
  }
  /**
   * @param path      stored file name relative to file streamer root (files.root)
   * @param sourceFor storage function to read file by name relative to file streamer root
   */
  case class FileInfoHelper(
    file_info: FileInfo,
    path: String,
    sourceFor: String => Source[ByteString, Any],
  ) {
    def this(r: RowLike, sourceFor: String => Source[ByteString, Any]) = this(
      file_info = new FileInfo(r),
      path = r.string("path"),
      sourceFor,
    )
    def id           = file_info.id
    def filename     = file_info.filename
    def upload_time  = file_info.upload_time
    def content_type = file_info.content_type
    def size         = file_info.size
    def sha_256      = file_info.sha_256
    def source: Source[ByteString, Any] = sourceFor(path)
    def toMap = file_info.toMap ++ Map("path" -> path)
  }

  private val pFieldNames = "name, value, file_info"
    .split(", ").toList
  private val pNameToIndex = pFieldNames.zipWithIndex.toMap
  private val pFieldOrdering = new FieldOrdering(pNameToIndex)
  // multipart/form-data part info
  case class PartInfo(
    name: String,
    value: String,
    file_info: FileInfo,
  ) {
    def toMap: Map[String, Any] =
      TreeMap[String, Any](
        "name" -> name,
        "value" -> value,
        "file_info" -> Option(file_info).map(_.toMap).orNull
      )(pFieldOrdering)
  }

  def sha256sink(implicit executor: ExecutionContext): Sink[ByteString, Future[String]] = {
    def updateDigest(digest: MessageDigest, chunk: ByteString): MessageDigest = {
      val a = chunk.toArray
      digest.update(a)
      digest
    }

    def hash(digest: MessageDigest): String = {
      val hash = digest.digest()
      val bigInt = new BigInteger(1, hash)
      val smallHash = bigInt.toString(16)
      "0" * (64 - smallHash.length) + smallHash
    }

    Sink.fold(MessageDigest.getInstance("SHA-256"))(updateDigest)
      .mapMaterializedValue(_.map(hash))
  }

  def sizeAndSha256sink(implicit executor: ExecutionContext): Sink[ByteString, Future[(Long, String)]] =
    Flow
      .fromFunction[ByteString, ByteString](identity)
      .alsoToMat(Sink.fold(0L){ (l, b) => l + b.length })(Keep.right)
      .toMat(sha256sink)(_ zip _)
}

trait AppFileStreamerConfig {
  def rootPath: String
  def tmpPath: String
  def file_info_table: String
  def file_body_info_table: String
  def shaColName: String
  def storage: org.wabase.fs.Storage
}

trait AppFileStreamer[User] extends AppFileStreamerConfig with Loggable { this: DbAccessProvider =>
  
  override def rootPath: String             = fileStreamer.rootPath
  override def file_info_table: String      = fileStreamer.file_info_table
  override def file_body_info_table: String = fileStreamer.file_body_info_table
  override def shaColName: String           = fileStreamer.shaColName
  override def tmpPath: String              = fileStreamer.tmpPath
  override def storage: org.wabase.fs.Storage = fileStreamer.storage

  lazy val fileStreamerConfig: Config = {
    import FileStreamerConfig.configs
    if (configs.size == 1)
      configs.head._2
    else configs.getOrElse("main",
      sys.error(
        "Single or 'main' file-streamer should be configured for this application. " +
        "See 'file-streamer' in reference.conf"
      )
    )
  }

  lazy val fileStreamer: FileStreamer =
    new FileStreamer(fileStreamerConfig, this, loggerName)

  import AppFileStreamer._

  def createTempFile =
    fileStreamer.createTempFile
  def fileSink(
    filename: String,
    contentType: String,
  )(implicit
    user: User,
    executor: ExecutionContext,
    materializer: Materializer,
  ): Sink[ByteString, Future[FileInfo]] = {
    fileStreamer.fileSink(filename, contentType)
  }
  def getFileInfo(id: Long, sha256: String)(implicit user: User): Option[FileInfoHelper] =
    fileStreamer.getFileInfo(id, sha256)
  /** Copies file within storage, source and dest are relative to file streamer root (files.root) */
  def copy(source: String, dest: String)(implicit executor: ExecutionContext, materializer: Materializer): Future[Unit] =
    fileStreamer.copy(source, dest)
}

class FileStreamer(
  fsCfg:            Config,
  dbAccessProvider: DbAccessProvider,
  name: String = null,
) extends AppFileStreamerConfig with Loggable {

  override def loggerName: String = if (fsCfg.hasPath("logger-name")) fsCfg.getString("logger-name") else
    Option(name).getOrElse(super.loggerName)

  override val rootPath: String             = org.wabase.fs.Storage.root(fsCfg)
  override val tmpPath: String              =
    Option("files.tmp-path").filter(fsCfg.hasPath).map(fsCfg.getString).getOrElse(rootPath + "/tmp").replaceAll("/+$", "")
  override val file_info_table: String      = fsCfg.getString("file-info-table")
  override val file_body_info_table: String = fsCfg.getString("file-body-info-table")
  override val shaColName: String           = fsCfg.getString("sha-col-name")
  val connectionPoolName: String            = Option("cp").filter(fsCfg.hasPath).map(fsCfg.getString).orNull

  @annotation.nowarn("msg=Manifest")
  override val storage: org.wabase.fs.Storage = getObjectOrNewInstance[org.wabase.fs.Storage](
    fsCfg, "files.storage-class", "file streamer file storage", Seq(fsCfg))

  private val fileInfoInsert =
    s"+$file_info_table {id, upload_time, content_type, $shaColName, filename} " +
      s"[#$file_info_table, :upload_time, :content_type, :sha_256, :filename]"
  private val fileInfoSelect =
    s"$file_info_table f; f/$file_body_info_table b?[id = :id][f.$shaColName = :sha_256] " +
      s"{id, filename, upload_time, content_type, f.$shaColName sha_256, size, path}@(1)"

  private lazy val db = dbAccessProvider.dbAccess
  private lazy val fsCp: PoolName = Option(connectionPoolName).map(PoolName.apply).getOrElse(db.DefaultCp)
  private def db_read[A]: (Resources => A) => A =
    db.withConn(
      poolName = fsCp,
      template =
        db.withDbAccessLogger(db.tresqlResources.resourcesTemplate, loggerName)
    )
  private def db_write[A]: (Resources => A) => A =
    db.newTransaction(
      poolName = fsCp,
      template =
        db.withDbAccessLogger(db.tresqlResources.resourcesTemplate, loggerName)
    )

  import AppFileStreamer._

  /**
   * Stored file body is reused (deduplicated) only if file_info with the same sha was uploaded within this window.
   * Such file_info is not deleted by file cleanup (it deletes file_info older than min-age only) and therefore
   * file body info and file body are not deleted either while upload is in progress.
   * Half of min-age is a safety margin for upload duration and clock differences.
   */
  val dedupWindowSeconds: Long = FileCleanup.minAge.getSeconds / 2

  private val recentFileInfoFilter =
    s"f.$shaColName = :sha_256 & f.upload_time > now() - seconds_to_interval($dedupWindowSeconds)"
  private val dedupPathSelect =
    s"$file_body_info_table b[b.$shaColName = :sha_256 & exists($file_info_table f[$recentFileInfoFilter])] {path}"
  private val recentFileInfoCount =
    s"$file_info_table f[$recentFileInfoFilter] {count(1)}"

  /** Inserts file_info and returns it with file body info */
  private def insertFileInfo(fi: FileInfo)(implicit res: Resources): Option[FileInfo] = {
    val id = Query(fileInfoInsert, fi.toMap) match { case r: InsertResult => r.id.get }
    Query(fileInfoSelect, Map("id" -> id, "sha_256" -> fi.sha_256))
      .map(new FileInfo(_)).find(_ => true)
  }

  def createTempFile = {
    val tempPath = new File(tmpPath)
    tempPath.mkdirs
    try File.createTempFile("tmp", null, tempPath) catch {
      case util.control.NonFatal(ex) =>
        throw new RuntimeException(s"Failed to create temporary file in $tempPath", ex)
    }
  }

  def fileSink(
    filename: String,
    contentType: String,
  )(implicit
    executor: ExecutionContext,
    materializer: Materializer,
  ): Sink[ByteString, Future[FileInfo]] = {
    val tempFile = createTempFile
    val toFile = FileIO.toPath(tempFile.toPath)

    def fileInfo(ioF: Future[IOResult], sizeAndShaF: Future[(Long, String)]): Future[FileInfo] = {
      ioF.flatMap { _ =>
        sizeAndShaF.map { sizeAndSha =>
          FileInfo(
            id = null,
            filename = filename,
            upload_time = new Timestamp((new jDate).getTime),
            content_type = contentType,
            size = sizeAndSha._1,
            sha_256 = sizeAndSha._2,
          )
        }
      }.transform(identity, { //try to unwrap error to force 413 Payload too large error code
        case e: EntityStreamSizeException => e
        case e =>
          e.getCause match {
            case tooLong: EntityStreamSizeException => tooLong
            case _ => e
          }
      }).andThen {
        case Failure(e) =>
          if (!tempFile.delete) logger.error(s"Unable to delete temp file $tempFile on error:", e)
      }
    }

    def moveAndCheckFile(fi: FileInfo): Future[FileInfo] = {
      val size = fi.size
      val sha = fi.sha_256
      val shaParams = Map("sha_256" -> sha)
      def badFileException = new RuntimeException(AppFileStreamer.CannotProcessFileMessage.format(sha))

      // stores temp file as new file body
      def storeNew: Future[Option[FileInfo]] =
        storage.storeFile(tempFile, sha).map { tailPath =>
          // if we are here, file body is accepted and stored, db has to be updated
          db_write { implicit res =>
            // update or insert - body info may be missing or deleted by file cleanup meanwhile
            Query(s"=$file_body_info_table[$shaColName = ?] {path = ?}", sha, tailPath) match {
              case r: UpdateResult if r.count.getOrElse(0) == 0 =>
                Query(s"+$file_body_info_table{$shaColName, size, path} [?, ?, ?]", sha, size, tailPath)
              case _ =>
            }
            insertFileInfo(fi)
          }
        }

      // reuses stored file body if it is still within dedup window
      @annotation.nowarn("msg=Manifest")
      def insertDeduplicated: Option[FileInfo] = db_write { implicit res =>
        // re-check, verify may have taken long
        if (Query(recentFileInfoCount, shaParams).unique[Long] > 0) insertFileInfo(fi) else None
      }

      @annotation.nowarn("msg=Manifest")
      val dedupPathOpt = db_read { implicit res =>
        Query(dedupPathSelect, shaParams).uniqueOption[String]
      }
      dedupPathOpt.map { path =>
        storage.verify(path + "/" + sha, size, sha).flatMap {
          case Some(true) => insertDeduplicated match {
            case someFi @ Some(_) =>
              Files.delete(tempFile.toPath)
              Future.successful(someFi)
            case None => storeNew      // dedup window passed during verify
          }
          case Some(false) => Future.failed(badFileException) // not accessible, bad sha or size
          case None => storeNew        // file body missing, will add
        }
      }.getOrElse(storeNew)            // new, or stored body is out of dedup window
       .map(_.get)
    }

    Flow
      .fromFunction[ByteString, ByteString](identity)
      .alsoToMat(toFile)(Keep.right)
      .toMat(sizeAndSha256sink)(fileInfo)
      .mapMaterializedValue(_.flatMap(moveAndCheckFile).andThen {
        case Failure(e) =>
          if (tempFile.exists && !tempFile.delete) logger.error(s"Unable to delete temp file $tempFile on error:", e)
      })
  }

  def getFileInfo(id: Long, sha256: String): Option[FileInfoHelper] = {
    db_read { implicit res =>
      Query(fileInfoSelect, Map("id" -> id, "sha_256" -> sha256))
        .map(new FileInfoHelper(_, storage.sourceFor)).find(_ => true)
    }
        .map { fi => fi.copy(
          path = fi.path + "/" + fi.sha_256
        )}
  }

  /** Copies file within storage, source and dest are relative to file streamer root (files.root) */
  def copy(source: String, dest: String)(implicit executor: ExecutionContext, materializer: Materializer): Future[Unit] = {
    storage.copy(source, dest)
  }
}

object FileStreamerConfig {
  val fsConfigTunablePaths = Set("files.root", "files.tmp-path", "jdbc.query-timeout")
  lazy val componentConfs = ComponentConf.getConfigs("file-streamer", fsConfigTunablePaths)
  lazy val configs: Map[String, Config] = componentConfs.confs.toMap - "files" - "jdbc"
  @annotation.nowarn("msg=Manifest")
  lazy val fileStreamerFactory: FileStreamerFactory =
    getObjectOrNewInstance[FileStreamerFactory](componentConfs.root, "factory-class", "file streamer factory")
}

trait FileStreamerFactory {
  def createFileStreamers(dbAccessProvider: DbAccessProvider): Map[String, FileStreamer]
}

object FileStreamerFactory extends FileStreamerFactory {
  def createFileStreamers(dbAccessProvider: DbAccessProvider): Map[String, FileStreamer] = {
    FileStreamerConfig.configs.map { case (n, fsCfg) =>
      n -> new FileStreamer(fsCfg, dbAccessProvider, "wabase.file-streamer." + n)
    }.toMap
  }
}
