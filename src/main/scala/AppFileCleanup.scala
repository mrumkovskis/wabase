package org.wabase

import java.io.{File, IOException}
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitOption, FileVisitResult, Files, Path, SimpleFileVisitor}
import java.time.Duration
import java.util.EnumSet
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.tresql._
import org.wabase.AppMetadata.DbAccessKey
import org.wabase.ds.PoolName
import org.wabase.fs.Storage

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal

/**
 * Cleans unreferenced file_info, file_body_info records and files of file streamers.
 * Stored files are cleaned through file streamer [[org.wabase.fs.Storage]], file streamers with equal
 * storage location are cleaned as one storage. Temporary files are cleaned in local tmp-path.
 *
 * Storage failure (for example unreachable S3) is logged and other storages are still cleaned,
 * cleanup result fails after all steps are done.
 *
 * Cleanup relies on these invariants to not delete files which are being uploaded concurrently:
 *  - file_info records are deleted only by file cleanup, when they are older than min-age and not referenced
 *    (file streamer deduplicates only file bodies with recent file_info, see [[FileStreamer.dedupWindowSeconds]]);
 *  - file storage writes file anew, or at least refreshes its modification time, every time file is stored,
 *    so file stored again at the same path is not considered aged.
 */
class AppFileCleanup(qe: AppQuerease, resourcesTemplate: Resources,
                     fileStreamers: AppFileStreamerConfig*) extends Loggable {

  lazy val minAgeMillis: Long = FileCleanup.minAge.toMillis
  protected lazy val minAge: Duration = Duration.ofMillis(minAgeMillis)
  protected lazy val refsToIgnore: Set[(String, String)] = Set.empty
  /**
   * Limit of file_info records deleted by one statement (statement is repeated while records are deleted),
   * None - no limit. file_body_info is deleted by one statement - limit is applied to its exists subquery only.
   */
  protected lazy val deleteLimitOpt: Option[Int] = None
  /** Number of files_on_disk records inserted by one batch and read by one page */
  protected lazy val batchSize: Int = config.getInt("app.file-cleanup.batch-size")
  /** Number of files concurrently moved to trash in one storage */
  protected lazy val parallelism: Int = config.getInt("app.file-cleanup.parallelism")

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
  1. delete files moved to trash in previous run
  2. delete all records from file_info, if id not referenced in linked tables (info about linked tables from metadata)
  3. delete all records from file_body_info where sha256 is not found in file_info
  4. list aged files from storages and insert into files_on_disk
  5. move files to trash when they are in files_on_disk but not in file_body_info
  6. delete aged temporary files
  */

  /**
   * Runs file cleanup.
   * @param ec  execution context for cleanup steps, including blocking database and tmp directory calls
   * @param as  actor system for storage streams
   */
  def doCleanup()(implicit ec: ExecutionContext, as: ActorSystem): Future[Unit] = {
    logger.debug(s"File cleanup started, storages: ${storages.map(_.location).mkString(", ")}")
    fileStreamers foreach { fs =>
      val tmp = new File(fs.tmpPath)
      if (!tmp.exists) logger.error("Filestreamer tmp directory doesn't exist: " + tmp.getAbsolutePath)
    }
    for {
      trashError <- attempt(cleanTrash)
      _          <- Future {
                      cleanupFileInfo
                      cleanupFileBodyInfo
                    }
      filesError <- attempt(cleanupFiles)
      _          <- Future(cleanupTmp)
      _          <- failIfAny(trashError.toList ++ filesError)
    } yield logger.debug("File cleanup finished")
  }

  /** File streamer storages, one per storage location */
  protected lazy val storages: Seq[Storage] =
    fileStreamers.map(_.storage).foldLeft(Vector.empty[Storage]) { (storages, storage) =>
      if (storages.exists(_.location == storage.location)) storages else storages :+ storage
    }

  /** Runs operation for every storage sequentially. Failure is logged, other storages are processed,
    * returned future fails with first failure after all storages are processed */
  protected def forEachStorage(operation: String)(f: Storage => Future[Unit])(implicit
    ec: ExecutionContext,
  ): Future[Unit] =
    storages.foldLeft(Future.successful(List.empty[Throwable])) { (errorsF, storage) =>
      errorsF.flatMap { errors =>
        (try f(storage) catch { case NonFatal(e) => Future.failed(e) })
          .map(_ => errors)
          .recover { case NonFatal(e) =>
            val error = new RuntimeException(
              s"File cleanup $operation failed for ${storage.location}: ${e.getMessage}", e)
            logger.error(error.getMessage, e)
            errors :+ error
          }
      }
    }.flatMap(failIfAny)

  /** Step result, failure as value, so that following steps can be executed */
  private def attempt(step: => Future[Unit])(implicit ec: ExecutionContext): Future[Option[Throwable]] =
    (try step catch { case NonFatal(e) => Future.failed(e) })
      .map(_ => Option.empty[Throwable]).recover { case NonFatal(e) => Some(e) }

  private def failIfAny(errors: List[Throwable]): Future[Unit] = errors match {
    case Nil => Future.successful(())
    case error :: rest =>
      rest.foreach(error.addSuppressed)
      Future.failed(error)
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

  protected def fileFilter(file: File, attrs: BasicFileAttributes): Boolean =
    attrs.isRegularFile &&
      Try(System.currentTimeMillis > attrs.lastModifiedTime.toMillis + minAgeMillis).getOrElse(true)

  protected def cleanTrash(implicit ec: ExecutionContext, as: ActorSystem): Future[Unit] = {
    logger.debug("Cleaning trash")
    // remove files which were moved to trash in previous cleanup run
    forEachStorage("trash cleanup") { storage =>
      storage.cleanTrash().map { filesDeleted =>
        logger.debug(s"Trash files deleted from ${storage.location}: $filesDeleted")
      }
    }
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

  protected def cleanupFiles(implicit ec: ExecutionContext, as: ActorSystem): Future[Unit] = {
    logger.debug("Cleaning stored files")
    prepCompareTable
    for {
      // files of successfully listed storages are moved to trash even if listing of some storage failed
      listError  <- attempt(fillCompareTable)
      trashError <- attempt(compareDataAndMoveFilesToTrash)
      _          <- Future(clearCompareTable())
      _          <- failIfAny(listError.toList ++ trashError)
    } yield ()
  }

  protected def prepCompareTable: Unit = {
    logger.debug("Preparing files_on_disk table")
    clearCompareTable()
  }

  protected def clearCompareTable(): Unit =
    deleteAndLog(
      "files_on_disk-[]",
      "files_on_disk table cleanup - records deleted:",
    )

  /** Lists aged files of every storage and inserts `storage location/file name` into files_on_disk */
  protected def fillCompareTable(implicit ec: ExecutionContext, as: ActorSystem): Future[Unit] = {
    logger.debug("Filling files_on_disk table")
    def insert(batch: Seq[String]): Unit = db_write { implicit res =>
      val stmt = res.conn.prepareStatement("INSERT INTO files_on_disk(path) VALUES (?)")
      try {
        batch.foreach { path =>
          stmt.setString(1, path)
          stmt.addBatch()
        }
        stmt.executeBatch()
      } finally stmt.close()
    }
    forEachStorage("listing") { storage =>
      logger.debug(s"Listing files of ${storage.location}")
      storage.listAged(minAge)
        .map(name => storage.location + "/" + name)
        .grouped(batchSize)
        .mapAsync(1)(batch => Future(insert(batch)))
        .runWith(Sink.ignore)
        .map { _ =>
          //filesUploaded as count query also for "warming up" DB (something like sql "analyze file_body_info"); independent of logger.debug scope
          @annotation.nowarn("msg=Manifest")
          val filesUploaded = db_read { implicit res =>
            Query("files_on_disk{count(1)}").unique[Long]
          }
          logger.debug(s"Number of records in files_on_disk after listing ${storage.location}: $filesUploaded")
        }
    }
  }

  /**
   * Moves files which are in files_on_disk but not in file_body_info to trash. files_on_disk is read
   * in pages ordered by path (keyset paging on primary key), so memory use does not depend on file count.
   * Failure of a storage is logged, its files in following pages are skipped, other storages are processed.
   */
  protected def compareDataAndMoveFilesToTrash(implicit
    ec: ExecutionContext,
    as: ActorSystem,
  ): Future[Unit] = {
    logger.debug("Moving unreferenced files to trash")
    val query = fileStreamers.zipWithIndex.map {
      case (fs, idx) =>
        import fs.{file_body_info_table}
        s"!exists($file_body_info_table fbi[fd.path = :path_$idx || '/' || fbi.path || '/' || fbi.${fs.shaColName}])"
    }.mkString("files_on_disk fd [fd.path > :after & ", " & ", s"]{fd.path}#(fd.path)@($batchSize)")
    val pathsParams = fileStreamers.zipWithIndex.map {
      case (fs, idx) => s"path_$idx" -> fs.storage.location
    }.toMap
    // nested storage location is longer than location of enclosing storage
    val storagesByLocationLength = storages.sortBy(-_.location.length)

    def movePage(
      paths: List[String],
      moved: Map[Storage, Int],
      failures: Map[Storage, Throwable],
    ): Future[(Map[Storage, Int], Map[Storage, Throwable])] = {
      val pathsByStorage = paths.groupBy { path =>
        storagesByLocationLength.find(s => path.startsWith(s.location + "/")).orNull
      }
      pathsByStorage.get(null).foreach(_.foreach { path => // files_on_disk is filled from storages
        logger.error(s"Unexpected file path, not moved to trash: $path")
      })
      storages.filter(s => pathsByStorage.contains(s) && !failures.contains(s))
        .foldLeft(Future.successful((moved, failures))) { (resultF, storage) =>
          resultF.flatMap { case (moved, failures) =>
            val names = pathsByStorage(storage).map(_.substring(storage.location.length + 1))
            Source(names)
              .mapAsync(parallelism) { name =>
                // age is re-checked by storage: file could have been stored again at the same path
                // after files_on_disk was filled (possible if min-age is shorter than path-format period)
                storage.trashAged(name, minAge).map {
                  case Some(true)  => 1
                  case Some(false) => logger.debug(s"File too young, not moved to trash: ${storage.location}/$name"); 0
                  case None        => logger.debug(s"File missing, not moved to trash: ${storage.location}/$name"); 0
                }
              }
              .runFold(0)(_ + _)
              .map(n => (moved.updated(storage, moved.getOrElse(storage, 0) + n), failures))
              .recover { case NonFatal(e) =>
                val error = new RuntimeException(
                  s"File cleanup move to trash failed for ${storage.location}: ${e.getMessage}", e)
                logger.error(error.getMessage, e)
                (moved, failures.updated(storage, error))
              }
          }
        }
    }

    def moveFrom(
      after: String,
      moved: Map[Storage, Int],
      failures: Map[Storage, Throwable],
    ): Future[(Map[Storage, Int], Map[Storage, Throwable])] = {
      @annotation.nowarn("msg=Manifest")
      val paths = db_read { implicit res: Resources =>
        Query(query, pathsParams + ("after" -> after)).list[String]
      }
      if (paths.isEmpty) Future.successful((moved, failures))
      else movePage(paths, moved, failures).flatMap { case (m, f) => moveFrom(paths.last, m, f) }
    }

    Future(moveFrom("", Map.empty, Map.empty)).flatten.flatMap { case (moved, failures) =>
      storages.foreach { storage =>
        logger.debug(s"Files moved to trash in ${storage.location}: ${moved.getOrElse(storage, 0)}")
      }
      failIfAny(storages.flatMap(failures.get).toList)
    }
  }

  protected def cleanupTmp = {
    logger.debug("Cleaning tmp")
    val filesDeleted = fileStreamers.map { fs =>
      val wd = new File(fs.tmpPath)
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

  private lazy val deleteLimit: String =
    deleteLimitOpt.filter(_ > 0).map(n => s"@($n)").getOrElse("")

  protected def fileBodyInfoCleanupSelectStatement(fs: AppFileStreamerConfig) =
    s"${fs.file_info_table} fi[fi.${fs.shaColName} = fbi.${fs.shaColName}]{1}"

  protected def fileBodyInfoCleanupStatement(fs: AppFileStreamerConfig) =
    s"${fs.file_body_info_table} fbi - [!exists(${fileBodyInfoCleanupSelectStatement(fs)}$deleteLimit)]"

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
    s"${fs.file_info_table} - [id in (${fileInfoCleanupSelectStatement(fs)}$deleteLimit)]"

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
  /** Files and file_info records younger than this are not cleaned. File streamer dedup window depends on it. */
  lazy val minAge: java.time.Duration = config.getDuration("app.file-cleanup.min-age")

  /** Margin for clock differences of app servers, database and file storage */
  val ClockSkewMargin: Duration = Duration.ofHours(1)

  /**
   * File body is reused (deduplicated) only if file_info with the same sha was uploaded within this window.
   * Such file_info is not deleted by file cleanup for at least min-age minus dedup window - this margin covers
   * clock differences (file_info upload time is set by app server, compared to database time) and transaction
   * duration. It is [[ClockSkewMargin]], or half of min-age if min-age is shorter than twice the margin.
   */
  def dedupWindow(minAge: Duration): Duration =
    minAge.minus(if (minAge.compareTo(ClockSkewMargin.multipliedBy(2)) < 0) minAge.dividedBy(2) else ClockSkewMargin)

  /**
   * Checks that min-age is not shorter than path period of file storage, so that file stored now
   * (at path of current period) is never aged and therefore can not be moved to trash while being stored again.
   * @return warning message if min-age is longer than path period by less than [[ClockSkewMargin]]
   * @throws com.typesafe.config.ConfigException.BadValue if min-age is shorter than path period
   */
  def checkMinAge(minAge: Duration, pathPeriod: Duration, location: String): Option[String] = {
    val path = "app.file-cleanup.min-age"
    if (minAge.compareTo(pathPeriod) < 0)
      throw new com.typesafe.config.ConfigException.BadValue(path,
        s"$path ($minAge) must not be shorter than files.path-format period ($pathPeriod) " +
        s"of file streamer storage $location, otherwise aged file can be stored again at the same path " +
        s"while file cleanup moves it to trash")
    else if (minAge.compareTo(pathPeriod.plus(ClockSkewMargin)) < 0)
      Some(
        s"$path ($minAge) should be longer than files.path-format period ($pathPeriod) " +
        s"of file streamer storage $location by at least $ClockSkewMargin to cover clock differences")
    else None
  }

  /**
   * File cleanup invocation.
   *
   *  NOTE: view definition using this invocation must have `explicit db: true` setting!
   * This is necessary so that db connection is used only on demand.
   * */
  def doCleanup(qe: AppQuerease, resourcesTemplate: Resources, fs: WabaseFileStreamers)(implicit
    ec: ExecutionContext,
    as: ActorSystem,
  ): Future[Unit] = {
    new AppFileCleanup(qe, resourcesTemplate, fs.fileStreamers.values.toSeq: _*).doCleanup()
  }
}
