package org.wabase.fs

import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString

import com.typesafe.config.Config

import java.io.File
import java.time.Duration
import scala.concurrent.{ExecutionContext, Future}

/**
 * File storage. File names are relative to storage root (files.root).
 *
 * Stored files are named `yyyy/MM/dd[/HH[/mm]]/sha256` (see [[DatePathStorage]]).
 * File cleanup methods ([[listAged]], [[trashAged]]) operate only on names matching this layout,
 * other files in storage are never touched, except files in `trash` which are deleted by [[cleanTrash]].
 *
 * Methods must not block the calling thread: blocking work (file system calls) is to be run on
 * a dispatcher for blocking operations, e.g. `mat.system.dispatchers.lookup(ActorAttributes.IODispatcher.dispatcher)`.
 */
trait Storage {
  /**
   * Storage location, unique for storage root, without trailing slash,
   * for example `file:/data/files` or `s3://bucket/prefix`.
   * Used by file cleanup to identify storages sharing the same root.
   *
   * Location must identify storage conservatively: storages with equal location are treated by file cleanup
   * as one storage (listed once, unreferenced files are cleaned through one of them only), which is safe.
   * Different locations must never refer to the same files - file referenced by one storage
   * could then be considered unreferenced by another and moved to trash. Therefore location does not include
   * access details like S3 endpoint url, which may be written differently for the same storage.
   * */
  def location: String
  /**
   * Source for reading stored file
   * @param fullName  file name relative to storage root
   * */
  def sourceFor(fullName: String): Source[ByteString, Any]
  /**
   * Save file into storage, file is named by its sha-256. On success source file is consumed - moved into storage
   * or deleted after upload. Stored file must have fresh modification time even if file with the same name
   * is already stored - file cleanup relies on it.
   * @param src     source (temporary) file
   * @param sha256  sha-256 (hex) of the file content, used as stored file name
   * @return future of stored file directory path (or prefix) (excluding root and file name)
   * */
  def storeFile(src: File, sha256: String)(implicit
    ec: ExecutionContext,
    mat: Materializer,
  ): Future[String]
  /**
   * Check stored file against expected size and sha-256. How thoroughly content is checked depends on storage:
   * file system storage reads the file and calculates sha-256, S3 storage compares size and checksum
   * provided by S3 (size only if S3 does not provide checksum).
   * @param fullName  file name relative to storage root
   * @param size      expected file size
   * @param sha256    expected sha-256 (hex) of the file content
   * @return future of None if file is missing, Some(true) if file is present and valid,
   *         Some(false) if file is present but not accessible or corrupt
   * */
  def verify(fullName: String, size: Long, sha256: String)(implicit
    ec: ExecutionContext,
    mat: Materializer,
  ): Future[Option[Boolean]]
  /**
   * Copy stored file, existing destination is replaced. Missing parent directories are created (if applicable).
   * @param src     source file name relative to storage root
   * @param dest    destination file name relative to storage root
   * */
  def copy(src: String, dest: String)(implicit
    ec: ExecutionContext,
    mat: Materializer,
  ): Future[Unit]
  /**
   * Stored files older than min-age. Only names matching stored file layout `yyyy/MM/dd[/HH[/mm]]/sha256`
   * are returned (of any path format, not only configured one, so that files stored before path format change
   * are cleaned too). Files in `trash` and `tmp`, also files of other storages nested in this storage root
   * are not listed. Fails if storage root does not exist.
   * @param minAge  minimal age of last modification time
   * @return source of file names relative to storage root
   * */
  def listAged(minAge: Duration): Source[String, Any]
  /**
   * Move stored file to trash (`trash/` + fullName) if it is still older than min-age.
   * Age is re-checked just before move since file could have been stored again after it was listed.
   * This is possible only if min-age is shorter than the period of stored file path (day, hour or minute
   * according to path format) - otherwise file stored again gets a newer path. Check and move are not atomic,
   * so min-age must not be shorter than path period (UTC based, see [[DatePathStorage.pathPeriod]]),
   * then aged file is never stored again at the same path.
   * @param fullName  file name relative to storage root, must match stored file layout
   * @param minAge    minimal age of last modification time
   * @return future of None if file is missing, Some(true) if file is moved to trash,
   *         Some(false) if file is too young. Fails if name does not match stored file layout.
   * */
  def trashAged(fullName: String, minAge: Duration)(implicit
    ec: ExecutionContext,
    mat: Materializer,
  ): Future[Option[Boolean]]
  /**
   * Delete all files from trash (moved there by [[trashAged]] in previous file cleanup run)
   * @return future of deleted file count
   * */
  def cleanTrash()(implicit
    ec: ExecutionContext,
    mat: Materializer,
  ): Future[Int]
}

object Storage {
  /** Storage root from files.root without trailing slashes */
  def root(cfg: Config): String = cfg.getString("files.root").replaceAll("/+$", "")
}
