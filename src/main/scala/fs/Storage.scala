package org.wabase.fs

import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString

import com.typesafe.config.Config

import java.io.File
import scala.concurrent.{ExecutionContext, Future}

/** File storage. File names are relative to storage root (files.root) */
trait Storage {
  /**
   * Source for reading stored file
   * @param fullName  file name relative to storage root
   * */
  def sourceFor(fullName: String): Source[ByteString, Any]
  /**
   * Save file into storage under specified name. On success source file is consumed - moved into storage
   * or deleted after upload. Stored file must have fresh modification time even if file with the same name
   * is already stored - file cleanup relies on it.
   * @param src   source (temporary) file
   * @param name  destination file name
   * @return future of stored file directory path (or prefix) (excluding root and file name)
   * */
  def storeFile(src: File, name: String)(implicit
    ec: ExecutionContext,
    mat: Materializer,
  ): Future[String]
  /**
   * Check stored file against expected size and sha-256
   * @param fullName  file name relative to storage root
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
}

object Storage {
  /** Storage root from files.root without trailing slashes */
  def root(cfg: Config): String = cfg.getString("files.root").replaceAll("/+$", "")
}
