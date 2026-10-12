package org.wabase.fs

import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import org.apache.pekko.http.scaladsl.model.ContentTypes
import org.apache.pekko.stream.{Attributes, Materializer}
import org.apache.pekko.stream.connectors.s3.{S3Attributes, S3Headers, S3Settings}
import org.apache.pekko.stream.connectors.s3.scaladsl.S3
import org.apache.pekko.stream.scaladsl.{FileIO, Sink, Source}
import org.apache.pekko.util.ByteString
import org.wabase.jDate

import java.io.File
import java.util.Base64
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

/**
 * Amazon S3 (or S3 compatible) storage, requires pekko-connectors-s3 dependency.
 *
 * Configuration:
 *  - `files.root` - bucket name optionally followed by key prefix: `bucket[/prefix[/...]]`
 *  - `files.tmp-path` - mandatory, local directory for temporary upload files
 *  - `files.s3` - optional pekko-connectors-s3 settings (endpoint, credentials, region, access style etc.),
 *    falls back to `pekko.connectors.s3`
 *  - `files.s3-checksum` - send `x-amz-checksum-sha256` with single part upload so that S3 rejects
 *    corrupted upload and returns checksum on verify (default true). Disable for S3 compatible storages
 *    which do not support checksum headers.
 *  - `files.s3-multipart-threshold` - files larger than this are uploaded using multipart upload,
 *    without checksum (default 16 MiB)
 */
class S3Storage(cfg: Config) extends DatePathStorage(cfg) {

  if (!cfg.hasPath("files.tmp-path"))
    throw new ConfigException.Missing(cfg.origin, "files.tmp-path")

  private val (bucket, prefix) = root.split("/", 2) match {
    case Array(b)    => (b, "")
    case Array(b, p) => (b, p.replaceAll("^/+", ""))
  }
  if (bucket.isEmpty)
    throw new ConfigException.BadValue(cfg.getValue("files.root").origin, "files.root",
      s"Bucket name is missing in '$root', expected bucket[/prefix]")

  private val checksum =
    !cfg.hasPath("files.s3-checksum") || cfg.getBoolean("files.s3-checksum")
  private val multipartThreshold: Long =
    if (cfg.hasPath("files.s3-multipart-threshold")) cfg.getBytes("files.s3-multipart-threshold") else 16L * 1024 * 1024
  private val s3Attrs: Attributes = S3Attributes.settings(S3Settings(
    (if (cfg.hasPath("files.s3")) cfg.getConfig("files.s3") else ConfigFactory.empty)
      .withFallback(org.wabase.config.getConfig("pekko.connectors.s3"))
  ))

  private def key(name: String) = if (prefix.isEmpty) name else s"$prefix/$name"
  private def location(name: String) = s"s3://$bucket/${key(name)}"

  /** Base64 of sha-256 bytes as required by x-amz-checksum-sha256 header */
  private def checksumOf(sha256: String): Option[String] =
    Option(sha256).filter(_.matches("[0-9a-fA-F]{64}")).map { hex =>
      Base64.getEncoder.encodeToString(hex.grouped(2).map(Integer.parseInt(_, 16).toByte).toArray)
    }

  private def withContext[T](operation: String, name: String)(f: => Future[T])(implicit
    ec: ExecutionContext,
  ): Future[T] =
    (try f catch { case NonFatal(e) => Future.failed(e) }).recoverWith {
      case NonFatal(e) =>
        Future.failed(new RuntimeException(s"S3 $operation failed for ${location(name)}: ${e.getMessage}", e))
    }

  override def sourceFor(fullName: String): Source[ByteString, Any] =
    S3.getObject(bucket, key(fullName)).withAttributes(s3Attrs)

  override def storeFile(src: File, sha256: String)(implicit
    ec: ExecutionContext,
    mat: Materializer,
  ): Future[String] = {
    val tailPath = new java.text.SimpleDateFormat(pathFormat).format(new jDate)
    val name = s"$tailPath/$sha256"
    withContext("upload", name) {
      val size = src.length
      val data = FileIO.fromPath(src.toPath)
      val uploaded =
        if (size <= multipartThreshold) {
          val headers = checksumOf(sha256).filter(_ => checksum)
            .map(c => S3Headers().withCustomHeaders(Map("x-amz-checksum-sha256" -> c)))
            .getOrElse(S3Headers())
          S3.putObject(bucket, key(name), data, size, ContentTypes.`application/octet-stream`, headers)
            .withAttributes(s3Attrs)
            .runWith(Sink.ignore)
        } else
          data.runWith(S3.multipartUpload(bucket, key(name)).withAttributes(s3Attrs))
      uploaded.map { _ =>
        src.delete()
        tailPath
      }
    }
  }

  override def verify(fullName: String, size: Long, sha256: String)(implicit
    ec: ExecutionContext,
    mat: Materializer,
  ): Future[Option[Boolean]] = withContext("verify", fullName) {
    val headers =
      if (checksum) S3Headers().withCustomHeaders(Map("x-amz-checksum-mode" -> "ENABLED")) else S3Headers()
    S3.getObjectMetadata(bucket, key(fullName), None, headers)
      .withAttributes(s3Attrs)
      .runWith(Sink.head)
      .map {
        case None => None
        case Some(meta) =>
          val checksumOk = meta.metadata
            .find(_.lowercaseName == "x-amz-checksum-sha256").map(_.value)
            .filterNot(_.contains("-")) // composite checksum of multipart upload
            .forall(c => checksumOf(sha256).contains(c))
          Some(meta.contentLength == size && checksumOk)
      }
  }

  override def copy(src: String, dest: String)(implicit
    ec: ExecutionContext,
    mat: Materializer,
  ): Future[Unit] =
    if (src == dest) Future.successful(())
    else withContext("copy", src) {
      S3.multipartCopy(bucket, key(src), bucket, key(dest))
        .withAttributes(s3Attrs)
        .run()
        .map(_ => ())
    }
}
