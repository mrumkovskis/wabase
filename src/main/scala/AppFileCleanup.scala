package org.wabase

import java.io.{File, IOException}
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitOption, FileVisitResult, Files, Path, SimpleFileVisitor, StandardCopyOption}
import java.util.EnumSet
import org.tresql._
import org.wabase.AppMetadata.DbAccessKey
import org.wabase.ds.ConnectionPools.DEFAULT_CP
import org.wabase.ds.PoolName

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.language.reflectiveCalls

class AppFileCleanup(qe: AppQuerease, resourcesTemplate: Resources,
                     fileStreamers: AppFileStreamerConfig*) extends Loggable {

  lazy val minAgeMillis: Long = config.getDuration("app.file-cleanup.min-age").toMillis
  protected lazy val refsToIgnore: Set[(String, String)] = Set.empty
  protected lazy val batchSizeOpt: Option[Int] = None

  val connectionPoolName: String  = Option("app.file-cleanup.cp").filter(config.hasPath).map(config.getString).orNull
  implicit lazy val connectionPool: PoolName = Option(connectionPoolName).map(PoolName.apply).getOrElse(WabaseAppConfig.DefaultCp)
  implicit lazy val extraDb: Seq[DbAccessKey] = Nil

  override def loggerName: String = "wabase.file-cleanup"

  private def db_read[A]: (Resources => A) => A = DbAccess.withRollbackConn(
    connectionPool, WabaseAppConfig.DefaultCp, DbAccess.withLogger(resourcesTemplate, loggerName)
  )

  private def db_write[A]: (Resources => A) => A = DbAccess.newTransaction(
    connectionPool, WabaseAppConfig.DefaultCp, DbAccess.withLogger(resourcesTemplate, loggerName)
  )


  /*
  1. delete all records from file_info, if id not referenced in linked tables (info about linked tables from metadata)
  2. delete all records from file_body_info where sha256 is not found in file_info
  3. list files from file system and insert into files_on_disk
  4. delete all files from file system when they are in files_on_disk but not in file_body_info
  */

  def doCleanup(): Unit = {
    logger.debug(s"File cleanup started, file streamers: ${fileStreamers.map(_.rootPath).mkString(", ")}")
    fileStreamers foreach { fs =>
      val wd = new File(fs.rootPath)
      val tmp = new File(fs.rootPath + "/tmp")
      if (!wd.exists)  logger.error("Filestreamer directory doesn't exist: " + wd.getAbsolutePath)
      if (!tmp.exists) logger.error("Filestreamer tmp directory doesn't exist: " + tmp.getAbsolutePath)
    }
    cleanTrash
    cleanupFileInfo
    cleanupFileBodyInfo
    cleanupFiles
    cleanupTmp
    logger.debug("File cleanup finished")
  }

  // One GETATTR/stat per path. File.listFiles + isFile/isDirectory/mtime is 2-3 NFS RPCs each.
  private def foreachFile(
    root: File,
    skipTopDirNames: Set[String] = Set.empty,
  )(visit: (File, BasicFileAttributes) => Unit): Unit = {
    val start = root.toPath
    if (!Files.isDirectory(start)) return
    Files.walkFileTree(
      start,
      EnumSet.of(FileVisitOption.FOLLOW_LINKS),
      Integer.MAX_VALUE,
      new SimpleFileVisitor[Path] {
        override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult = {
          val name = dir.getFileName
          if (name != null && (dir.getParent == start) && skipTopDirNames.contains(name.toString))
            FileVisitResult.SKIP_SUBTREE
          else FileVisitResult.CONTINUE
        }
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          visit(file.toFile, attrs)
          FileVisitResult.CONTINUE
        }
        override def visitFileFailed(file: Path, exc: IOException): FileVisitResult =
          FileVisitResult.CONTINUE
      }
    )
  }

  private def foreachAgedFile(
    root: File,
    skipTopDirNames: Set[String] = Set.empty,
  )(visit: File => Unit): Unit =
    foreachFile(root, skipTopDirNames) { (file, attrs) =>
      if (fileFilter(file, attrs)) visit(file)
    }

  private def deleteFilesRecursively(file: File): Int = {
    val start = file.toPath
    if (!Files.exists(start)) 0
    else {
      var filesDeleted = 0
      Files.walkFileTree(start, new SimpleFileVisitor[Path] {
        override def visitFile(f: Path, attrs: BasicFileAttributes): FileVisitResult = {
          Files.deleteIfExists(f)
          filesDeleted += 1
          FileVisitResult.CONTINUE
        }
        override def visitFileFailed(f: Path, exc: IOException): FileVisitResult =
          FileVisitResult.CONTINUE
        override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
          Files.deleteIfExists(dir)
          FileVisitResult.CONTINUE
        }
      })
      filesDeleted
    }
  }

  protected def fileFilter(file: File): Boolean =
    Try(Files.readAttributes(file.toPath, classOf[BasicFileAttributes])).toOption
      .exists(fileFilter(file, _))

  protected def fileFilter(file: File, attrs: BasicFileAttributes): Boolean =
    attrs.isRegularFile &&
      Try(System.currentTimeMillis > attrs.lastModifiedTime.toMillis + minAgeMillis).getOrElse(true)

  protected def cleanTrash = {
    logger.debug("Cleaning trash")
    //remove files which where moved to trash directory in previous cron job run
    val filesDeleted = fileStreamers.map { fs =>
      deleteFilesRecursively(new File(fs.rootPath + "/trash"))
    }.sum
    logger.debug("Trash files deleted: " + filesDeleted)
  }

  protected def cleanupFileInfo = {
    logger.debug("Cleaning file_info")
    fileStreamers foreach { fs => deleteAndLog(
      fileInfoCleanupStatement(fs),
      s"${fs.file_info_table} table cleanup - records deleted:",
    )}
  }

  protected def cleanupFileBodyInfo = {
    logger.debug("Cleaning file_body_info")
    fileStreamers  foreach { fs => deleteAndLog(
      fileBodyInfoCleanupStatement(fs),
      s"${fs.file_body_info_table} table cleanup - records deleted:",
    )}
  }

  protected def cleanupFiles = {
    logger.debug("Cleaning files on disk")
    prepCompareTable
    fillCompareTable()
    compareDataAndMoveFilesToTrash
  }

  protected def prepCompareTable: Unit = {
    logger.debug("Preparing files_on_disk table")
    deleteAndLog(
      "files_on_disk-[]",
      "files_on_disk table cleanup - records deleted:",
    )
  }

  protected def fillCompareTable(batchSize: Int = 500): Unit = {
    logger.debug("Filling files_on_disk table")
    // file system list all files applicable for deletion
    val YYYY_MM_DD_SHA = """.*(/\d\d\d\d/\d\d/\d\d/[0-9a-fA-F]{64})$""".r
    val allRootPaths = fileStreamers.map(_.rootPath).distinct
    allRootPaths.filterNot(rp => allRootPaths.exists(rrp => rp.startsWith(rrp + "/"))) foreach { rootPath =>
      logger.debug(s"Listing files on disk for $rootPath")
      val wd = new File(rootPath)
      val batch = new ArrayBuffer[String](batchSize)
      def flush(): Unit = if (batch.nonEmpty) {
        db_write { implicit res =>
          val stmt = res.conn.prepareStatement("INSERT INTO files_on_disk(path) VALUES (?)")
          batch.foreach { file =>
            stmt.setString(1, file)
            stmt.addBatch()
          }
          stmt.executeBatch()
        }
        batch.clear()
      }
      foreachAgedFile(wd, Set("tmp", "trash")) { file =>
        val path = file.getAbsolutePath
        // process files according to the parttern: [fileStreamer.rootPath]/year/mmonth/day/sha256
        path match {
          case YYYY_MM_DD_SHA(x) if allRootPaths.exists(_ + x == path) =>
            batch += path
            if (batch.size >= batchSize) flush()
          case _ =>
        }
      }
      flush()
      //filesUploaded as count query also for "warming up" DB (something like sql "analyze file_body_info"); independent of logger.debug scope
      @annotation.nowarn("msg=Manifest")
      val filesUploaded = db_write { implicit res =>
        Query("files_on_disk{count(1)}").unique[Long]
      }
      logger.debug(s"Number of records inserted into files_on_disk for $rootPath: $filesUploaded")
    }
  }

  protected def compareDataAndMoveFilesToTrash: Unit = {
    logger.debug("Moving unreferenced files to trash")
    // delete files from file system
    val query = fileStreamers.zipWithIndex.map {
      case (fs, idx) =>
        import fs.{file_body_info_table}
        s"!exists($file_body_info_table fbi[fd.path = :path_$idx || '/' || fbi.path || '/' || fbi.${fs.shaColName}])"
    }.mkString("files_on_disk fd [", " & ", "]{fd.path}")
    val pathsParams = fileStreamers.zipWithIndex.map {
      case (fs, idx) => s"path_$idx" -> fs.rootPath
    }.toMap
    db_read { implicit res: Resources =>
      @annotation.nowarn("msg=Manifest")
      val filesMoved = Query(query, pathsParams).list[String]
        .map(new File(_))
        .foldLeft(0){case (counter, fullPathFile) =>
          val wd = fullPathFile.getParentFile.getParentFile.getParentFile.getParentFile
          val file = fullPathFile.getAbsolutePath.substring(wd.getAbsolutePath.length + 1)
          val moveToFile = new File(wd.getAbsolutePath + "/trash/" + file)
          val moveToDir = moveToFile.getParentFile
          if (!moveToDir.exists) moveToDir.mkdirs
          Files.move(fullPathFile.toPath, moveToFile.toPath, StandardCopyOption.ATOMIC_MOVE)
          counter + 1
        }
      logger.debug("Files moved to trash: " + filesMoved)
    }
  }

  protected def cleanupTmp = {
    logger.debug("Cleaning tmp")
    val filesDeleted = fileStreamers.map { fs =>
      val wd = new File(fs.rootPath + "/tmp")
      if (wd.exists) {
        var n = 0
        foreachAgedFile(wd) { file =>
          file.delete()
          n += 1
        }
        n
      } else 0
    }.sum
    logger.debug("Temporary files deleted: " + filesDeleted)
  }

  private lazy val batchLimit: String =
    batchSizeOpt.filter(_ > 0).map(n => s"@($n)").getOrElse("")

  protected def fileBodyInfoCleanupSelectStatement(fs: AppFileStreamerConfig) =
    s"${fs.file_info_table} fi[fi.${fs.shaColName} = fbi.${fs.shaColName}]{1}"

  protected def fileBodyInfoCleanupStatement(fs: AppFileStreamerConfig) =
    s"${fs.file_body_info_table} fbi - [!exists(${fileBodyInfoCleanupSelectStatement(fs)}$batchLimit)]"

  protected def fileInfoCleanupSelectStatement(fs: AppFileStreamerConfig) = {
    // select records from file_info table where id is not referenced in linked tables
    val tableMetadataWithFileInfo = (for {
      tableDef <- qe.tableMetadata.tableDefs
      tableRef <- tableDef.refs
      if tableRef.refTable == fs.file_info_table
    } yield (tableDef.name, tableRef.cols.head)).toSet -- refsToIgnore

    val unionSubquery   = tableMetadataWithFileInfo.map { case (table, col) => s"$table[$col = fi.id]{1}" }.mkString(" + ")
    val andNotExists    = if (unionSubquery.nonEmpty) s"& !exists($unionSubquery)" else ""
    val selectStatement =
      s"${fs.file_info_table} fi[fi.upload_time < now() - seconds_to_interval(${minAgeMillis/1000}) $andNotExists]{fi.id}"
    selectStatement
  }

  protected def fileInfoCleanupStatement(fs: AppFileStreamerConfig) =
    // delete all records from file_info table where id is not referenced in linked tables
    s"${fs.file_info_table} - [id in (${fileInfoCleanupSelectStatement(fs)}$batchLimit)]"

  protected def deleteAndLog(statement: String, message: String) = {
    @tailrec
    def deleteWhileNonEmpty(deletedTotalCount: Int): Int = {
      val deletedCount =
        (db_write { implicit res: Resources =>
          Query(statement)
        }) match {
          case deleteResult: DeleteResult => deleteResult.count.getOrElse(0)
          case x => sys.error(s"Unexpected result class: ${x.getClass.getName}. Expecting DeleteResult.")
        }
      if (deletedCount == 0)
        deletedTotalCount
      else
        deleteWhileNonEmpty(deletedTotalCount + deletedCount)
    }
    val deletedTotalCount = deleteWhileNonEmpty(deletedTotalCount = 0)
    logger.debug(s"$message $deletedTotalCount")
  }
}

object FileCleanup {
  /**
   * File cleanup invocation.
   *
   *  NOTE: view definition using this invocation must have `explicit db: true` setting!
   * This is necessary so that db connection is used only on demand.
   * */
  def doCleanup(qe: AppQuerease, resourcesTemplate: Resources, fs: WabaseFileStreamers): Unit = {
    new AppFileCleanup(qe, resourcesTemplate, fs.fileStreamers.values.toSeq: _*).doCleanup()
  }
}
