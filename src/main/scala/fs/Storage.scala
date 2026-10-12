package org.wabase.fs

import org.apache.pekko.stream.{IOResult, Materializer}
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
  def sourceFor(fullName: String): Source[ByteString, Future[IOResult]]
  /**
   * Save file into storage under specified name
   * @param src   source file
   * @param name  destination file name
   * @return future of stored file directory path (or prefix) (excluding root and file name)
   * */
  def storeFile(src: File, name: String): Future[String]
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
   * Copy stored file
   * @param src     source file name relative to storage root
   * @param dest    destination file name relative to storage root
   * @param mkdirs  create missing parent directories (if applicable to storage)
   * */
  def copy(src: String, dest: String, mkdirs: Boolean): Future[Unit]
}

object Storage {
  /** Storage root from files.root without trailing slashes */
  def root(cfg: Config): String = cfg.getString("files.root").replaceAll("/+$", "")
}
