package org.wabase

import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import org.tresql._
import org.wabase.AppMetadata.DbAccessKey

import scala.annotation.tailrec
import scala.util.Try
import scala.language.reflectiveCalls

class AppFileCleanup(dbAccess: DbAccess, fileStreamers: AppFileStreamerConfig*) extends Loggable { this: QuereaseProvider =>

  lazy val minAgeMillis: Long = 1000L * 60 * 60 * 24
  protected lazy val ageCheckSql: String = "now() - interval '1 days'"
  protected lazy val refsToIgnore: Set[(String, String)] = Set.empty
  protected lazy val batchSizeOpt: Option[Int] = None

  implicit lazy val connectionPool: PoolName = DEFAULT_CP
  implicit lazy val extraDb: Seq[DbAccessKey] = Nil
  implicit lazy val queryTimeout: QueryTimeout =
    QueryTimeout(config.getDuration("app.file-cleanup.jdbc.query-timeout").toSeconds.toInt)

  /*
  1. delete all records from file_info, if id not referenced in linked tables (info about linked tables from metadata)
  2. delete all records from file_body_info where sha256 is not found in file_info
  3. list files from file system and insert into files_on_disk
  4. delete all files from file system when they are in files_on_disk but not in file_body_info
  */

  def doCleanup(log: akka.event.LoggingAdapter) = {
    fileStreamers foreach { fs =>
      val wd = new File(fs.rootPath)
      val tmp = new File(fs.rootPath + "/tmp")
      if (!wd.exists)  log.error("Filestreamer directory doesn't exist: " + wd.getAbsolutePath)
      if (!tmp.exists) log.error("Filestreamer tmp directory doesn't exist: " + tmp.getAbsolutePath)
    }
    cleanTrash
    cleanupFileInfo
    cleanupFileBodyInfo
    cleanupFiles
    cleanupTmp
  }

  private def listFilesRecursively(file: File, filter: File => Boolean = _ => true): Seq[File] = {
    val these = Option(file.listFiles).map(_.toSeq).getOrElse(Nil)
    these.filter(filter) ++ these.filter(_.isDirectory).flatMap(listFilesRecursively(_, filter))
  }

  private def deleteFilesRecursively(file: File): Unit = {
    if (file.isDirectory)
      Option(file.listFiles).map(_.toSeq).getOrElse(Nil).foreach(deleteFilesRecursively)
    file.delete
  }

  protected def fileFilter(file: File): Boolean =
    file.isFile &&
      Try(System.currentTimeMillis > Files.getLastModifiedTime(file.toPath).toMillis + minAgeMillis).toOption.getOrElse(true)

  protected def cleanTrash = {
    //remove files which where moved to trash directory in previous cron job run
    fileStreamers foreach { fs =>
      deleteFilesRecursively(new File(fs.rootPath + "/trash"))
    }
  }

  protected def cleanupFileInfo =
    fileStreamers foreach { fs => deleteAndLog(
      fileInfoCleanupStatement(fs),
      s"${fs.file_info_table} table cleanup - records deleted:",
    )}

  protected def cleanupFileBodyInfo =
    fileStreamers  foreach { fs => deleteAndLog(
      fileBodyInfoCleanupStatement(fs),
      s"${fs.file_body_info_table} table cleanup - records deleted:",
    )}

  protected def cleanupFiles = {
    prepCompareTable
    fillCompareTable()
    compareDataAndMoveFilesToTrash
  }

  protected def prepCompareTable: Unit =
    deleteAndLog(
      "files_on_disk-[]",
      "files_on_disk table cleanup - records deleted:",
    )

  protected def fillCompareTable(batchSize: Int = 500): Unit = {
    // file system list all files applicable for deletion
    val YYYY_MM_DD_SHA = """.*(/\d\d\d\d/\d\d/\d\d/[0-9a-fA-F]{64})$""".r
    val allRootPaths = fileStreamers.map(_.rootPath).distinct
    allRootPaths.filterNot(rp => allRootPaths.exists(rrp => rp.startsWith(rrp + "/"))) foreach { rootPath =>
      val wd = new File(rootPath)
      val files =
        listFilesRecursively(wd, fileFilter)
          .map(_.getAbsolutePath)
          // process files according to the parttern: [fileStreamer.rootPath]/year/mmonth/day/sha256
          .filter(f => f match {
            case YYYY_MM_DD_SHA(x) =>
              allRootPaths.exists(_ + x == f)
            case _ => false
          })

      // insert files into files_on_disk
      dbAccess.transaction(dbAccess.tresqlResources.resourcesTemplate, connectionPool) { implicit res =>
        def prepareStatement = res.conn.prepareStatement("INSERT INTO files_on_disk(path) VALUES (?)")
        val lastBatch =
          files.foldLeft((prepareStatement, 0)) { case ((stmt, count), file) =>
            stmt.setString(1, file)
            stmt.addBatch()
            if(count >= batchSize) {
              stmt.executeBatch()
              res.conn.commit()
              (prepareStatement, 0)
            } else (stmt, count + 1)
          }
        if (lastBatch._2 > 0)
          lastBatch._1.executeBatch()
      }
      //filesUploaded as count query also for "warming up" DB (something like sql "analyze file_body_info"); independent of logger.debug scope
      val filesUploaded = dbAccess.withRollbackConn(dbAccess.tresqlResources.resourcesTemplate, connectionPool) { implicit res =>
        Query("files_on_disk{count(1)}").unique[Long]
      }
      logger.debug(s"Number of records inserted into files_on_disk for $rootPath: $filesUploaded")
    }
  }

  protected def compareDataAndMoveFilesToTrash: Unit = {
    // delete files from file system
    val query = fileStreamers.zipWithIndex.map {
      case (fs, idx) =>
        import fs.{file_body_info_table}
        s"!exists($file_body_info_table fbi[fd.path = :path_$idx || '/' || fbi.path || '/' || fbi.${fs.shaColName}])"
    }.mkString("files_on_disk fd [", " & ", "]{fd.path}")
    val pathsParams = fileStreamers.zipWithIndex.map {
      case (fs, idx) => s"path_$idx" -> fs.rootPath
    }.toMap
    dbAccess.withRollbackConn(dbAccess.tresqlResources.resourcesTemplate, connectionPool) { implicit res =>
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
    fileStreamers foreach { fs =>
      val wd = new File(fs.rootPath + "/tmp")
      if (wd.exists)
        listFilesRecursively(wd, fileFilter).foreach (_.delete)
    }
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

    val joinsAndTables = tableMetadataWithFileInfo.zipWithIndex.map {
      case ((table, col), idx) => (s"fi [t$idx.$col = fi.id] $table t$idx ?", s"t$idx.$col")
    }
    val selectStatement =
      s"${fs.file_info_table} fi" + joinsAndTables.map("; " + _._1).mkString +
        s"""[fi.upload_time < sql("$ageCheckSql") """ + joinsAndTables.map(" & " + _._2 + " = null").mkString + "]{fi.id}"
    selectStatement
  }

  protected def fileInfoCleanupStatement(fs: AppFileStreamerConfig) =
    // delete all records from file_info table where id is not referenced in linked tables
    s"${fs.file_info_table} - [id in (${fileInfoCleanupSelectStatement(fs)}$batchLimit)]"

  protected def deleteAndLog(statement: String, message: String) = {
    @tailrec
    def deleteWhileNonEmpty(deletedTotalCount: Int): Int = {
      val deletedCount =
        dbAccess.transaction(dbAccess.tresqlResources.resourcesTemplate, connectionPool) { implicit res =>
          Query(statement)
        } match {
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
