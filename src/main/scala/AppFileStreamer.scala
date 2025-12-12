package org.wabase

import java.io.{File, FileInputStream, FileOutputStream}
import java.math.BigInteger
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.security.MessageDigest
import org.apache.pekko.http.scaladsl.model.EntityStreamSizeException
import com.typesafe.config.Config

import scala.collection.immutable.TreeMap
import scala.concurrent.{ExecutionContext, Future}
import org.apache.pekko.stream._
import org.apache.pekko.stream.scaladsl._
import org.apache.pekko.util.ByteString
import org.tresql._
import org.wabase.ds.{PoolName, QueryTimeout}

import scala.util.Failure

object AppFileStreamer {
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
  case class FileInfoHelper(
    file_info: FileInfo,
    path: String,
  ) {
    def this(r: RowLike) = this(
      file_info = new FileInfo(r),
      path = r.string("path"),
    )
    def id           = file_info.id
    def filename     = file_info.filename
    def upload_time  = file_info.upload_time
    def content_type = file_info.content_type
    def size         = file_info.size
    def sha_256      = file_info.sha_256
    def source: Source[ByteString, Future[IOResult]] =
      FileIO.fromPath(java.nio.file.Paths.get(path))
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
  def file_info_table: String
  def file_body_info_table: String
  def shaColName: String
}

trait AppFileStreamer[User] extends AppFileStreamerConfig { this: DbAccessProvider =>
  
  override def rootPath: String             = fileStreamer.rootPath
  override def file_info_table: String      = fileStreamer.file_info_table
  override def file_body_info_table: String = fileStreamer.file_body_info_table
  override def shaColName: String           = fileStreamer.shaColName

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
    new FileStreamer(fileStreamerConfig, this)


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
  def copy(source: String, dest: String, mkdirs: Boolean = false): Unit =
    fileStreamer.copy(source, dest, mkdirs)
}

class FileStreamer(
  fsCfg:            Config,
  dbAccessProvider: DbAccessProvider,
) extends AppFileStreamerConfig with Loggable {

  override val rootPath: String             = fsCfg.getString("files.path").replaceAll("/+$", "")
  override val file_info_table: String      = fsCfg.getString("file-info-table")
  override val file_body_info_table: String = fsCfg.getString("file-body-info-table")
  override val shaColName: String           = fsCfg.getString("sha-col-name")
  val connectionPoolName: String            = Option("cp").filter(fsCfg.hasPath).map(fsCfg.getString).orNull
  val queryTimeoutSeconds: Int              = fsCfg.getDuration("jdbc.query-timeout").toSeconds.toInt

  private val fileInfoInsert =
    s"+$file_info_table {id, upload_time, content_type, $shaColName, filename} " +
      s"[#$file_info_table, :upload_time, :content_type, :sha_256, :filename]"
  private val fileInfoSelect =
    s"$file_info_table f; f/$file_body_info_table b?[id = :id][f.$shaColName = :sha_256] " +
      s"{id, filename, upload_time, content_type, f.$shaColName sha_256, size, path}@(1)"

  private lazy val db = dbAccessProvider.dbAccess
  private lazy val fsCp: PoolName = Option(connectionPoolName).map(PoolName).getOrElse(db.DefaultCp)
  private implicit val queryTimeout: QueryTimeout  = QueryTimeout(queryTimeoutSeconds)

  import AppFileStreamer._

  def createTempFile = {
    val tempPath = new File(rootPath + "/" + "tmp")
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
      def badFileException =
        // TODO log! (BusinessExceptions are not logged)
        new BusinessException(
          "Cannot process file, please contact administrator: " + sha)

      def oldPathOpt = db.withConn(fsCp) { implicit res =>
        Query(s"$file_body_info_table[$shaColName=?]{path}", sha).uniqueOption[String]
      }
      val oldFileInfoF: Future[Option[FileInfo]] = oldPathOpt match {
        case Some(oldPath) =>
          val old = new File(rootPath + "/" + oldPath + "/" + sha).toPath
          if (Files.exists(old)) {
            if (Files.isRegularFile(old) && Files.isReadable(old)) {
              val sizeAndShaF = FileIO.fromPath(old).runWith(sizeAndSha256sink)
              for {
                (oldSize, oldSha) <- sizeAndShaF
              } yield {
                if (size == oldSize && sha == oldSha) {
                  // old file ok
                  val id = db.newTransaction(fsCp) { implicit res =>
                    Query(fileInfoInsert, fi.toMap) match { case r: InsertResult => r.id.get }
                  }
                  Files.delete(tempFile.toPath)
                  db.withConn(fsCp) { implicit res =>
                    Query(fileInfoSelect, Map("id" -> id, "sha_256" -> sha))
                      .map(new FileInfo(_)).find(_ => true)
                  }
                } else throw badFileException // bad sha or size
              }
            } else Future.failed(badFileException) // not readable or is directory
          } else Future.successful(None) // missing, will add
        case None => Future.successful(None) // new, will add
      }
      oldFileInfoF map {
        case None =>
          val tailPath = new java.text.SimpleDateFormat("yyyy/MM/dd").format(new jDate)
          val fullPath = new File(rootPath + "/" + tailPath)
          fullPath.mkdirs
          val targetFile = new File(fullPath.toString + "/" + sha)
          if (!tempFile.renameTo(targetFile))
            if (!targetFile.exists || !Files.isRegularFile(targetFile.toPath))
              throw badFileException
          // if we are here, file body is accepted and copied, db has to be updated
          db.newTransaction(fsCp) { implicit res =>
            if (oldPathOpt.isDefined)
                Query(s"=$file_body_info_table[$shaColName = ?] {path = ?}", sha, tailPath)
            else
                Query(s"+$file_body_info_table{$shaColName, size, path} [?, ?, ?]", sha, size, tailPath)
            val id = Query(fileInfoInsert, fi.toMap) match { case r: InsertResult => r.id.get }
            Query(fileInfoSelect, Map("id" -> id, "sha_256" -> sha))
              .map(new FileInfo(_)).find(_ => true)
          }
        case someFi =>
          someFi
      } map(_.get)
    }

    Flow
      .fromFunction[ByteString, ByteString](identity)
      .alsoToMat(toFile)(Keep.right)
      .toMat(sizeAndSha256sink)(fileInfo)
      .mapMaterializedValue(_.flatMap(moveAndCheckFile))
  }

  def getFileInfo(id: Long, sha256: String): Option[FileInfoHelper] = {
    db.withConn(fsCp) { implicit res =>
      Query(fileInfoSelect, Map("id" -> id, "sha_256" -> sha256))
        .map(new FileInfoHelper(_)).find(_ => true)
    }
        .map { fi => fi.copy(
          path = rootPath + "/" + fi.path + "/" + fi.sha_256
        )}
  }

  def copy(source: String, dest: String, mkdirs: Boolean = false): Unit = {
    if (source != dest) {
      val s = new File(source)
      val d = new File(dest)
      if (mkdirs)
        d.getParentFile.mkdirs
      val in = new FileInputStream(new File(source)).getChannel
      val out = new FileOutputStream(new File(dest)).getChannel
      try {
        val buf = in.map(FileChannel.MapMode.READ_ONLY, 0, in.size)
        out.write(buf)
      } finally {
        in.close()
        out.close()
      }
    }
  }
}

object FileStreamerConfig {
  val fsConfigTunablePaths = Set("files.path", "jdbc.query-timeout")
  lazy val componentConfs = ComponentConf.getConfigs("file-streamer", fsConfigTunablePaths)
  lazy val configs: Map[String, Config] = componentConfs.confs.toMap - "files" - "jdbc"
  lazy val fileStreamerFactory: FileStreamerFactory =
    getObjectOrNewInstance[FileStreamerFactory](componentConfs.root, "factory-class", "file streamer factory")
}

trait FileStreamerFactory {
  def createFileStreamers(dbAccessProvider: DbAccessProvider): Map[String, FileStreamer]
}

object FileStreamerFactory extends FileStreamerFactory {
  def createFileStreamers(dbAccessProvider: DbAccessProvider): Map[String, FileStreamer] = {
    FileStreamerConfig.configs.map { case (n, fsCfg) =>
      n -> new FileStreamer(fsCfg, dbAccessProvider)
    }.toMap
  }
}
