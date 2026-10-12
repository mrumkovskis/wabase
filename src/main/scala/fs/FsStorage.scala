package org.wabase.fs

import com.typesafe.config.Config
import org.apache.pekko.stream.{IOResult, Materializer}
import org.apache.pekko.stream.scaladsl.{FileIO, Source}
import org.apache.pekko.util.ByteString
import org.wabase.AppFileStreamer.sizeAndSha256sink
import org.wabase.jDate

import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

class FsStorage(cfg: Config) extends DatePathStorage(cfg) {

  override def sourceFor(fullName: String): Source[ByteString, Future[IOResult]] = {
    FileIO.fromPath(java.nio.file.Paths.get(root + "/" + fullName))
  }

  override def storeFile(src: File, name: String): Future[String] = try {
    val tailPath = new java.text.SimpleDateFormat(pathFormat).format(new jDate)
    val fullPath = new File(root + "/" + tailPath)
    fullPath.mkdirs
    val targetFile = new File(fullPath.toString + "/" + name)
    if (!src.renameTo(targetFile) && (!targetFile.exists || !Files.isRegularFile(targetFile.toPath)))
      Future.failed(new RuntimeException(s"Could not save file: $targetFile"))
    else
      Future.successful(tailPath)
  } catch {
    case NonFatal(e) => Future.failed(e)
  }

  override def verify(fullName: String, size: Long, sha256: String)(implicit
    ec: ExecutionContext,
    mat: Materializer,
  ): Future[Option[Boolean]] = try {
    val f = new File(root + "/" + fullName).toPath
    if (!Files.exists(f))
      Future.successful(None)
    else if (!Files.isRegularFile(f) || !Files.isReadable(f))
      Future.successful(Some(false))
    else
      FileIO.fromPath(f).runWith(sizeAndSha256sink).map { case (fileSize, fileSha) =>
        Some(fileSize == size && fileSha == sha256)
      }
  } catch {
    case NonFatal(e) => Future.failed(e)
  }

  override def copy(src: String, dest: String, mkdirs: Boolean): Future[Unit] = try {
    if (src != dest) {
      val destPath = new File(root + "/" + dest).toPath
      if (mkdirs)
        Option(destPath.getParent).foreach(Files.createDirectories(_))
      Files.copy(new File(root + "/" + src).toPath, destPath, StandardCopyOption.REPLACE_EXISTING)
    }
    Future.successful(())
  } catch {
    case NonFatal(e) => Future.failed(e)
  }
}
