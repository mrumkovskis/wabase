package org.wabase.fs

import com.typesafe.config.Config
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.{ActorAttributes, Attributes, Materializer}
import org.apache.pekko.stream.scaladsl.{FileIO, Source}
import org.apache.pekko.util.ByteString
import org.wabase.AppFileStreamer.sizeAndSha256sink
import org.wabase.jDate

import java.io.{File, IOException}
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{DirectoryStream, FileVisitResult, Files, NoSuchFileException, Path, SimpleFileVisitor, StandardCopyOption}
import java.time.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class FsStorage(cfg: Config) extends DatePathStorage(cfg) {

  override val location: String = "file:" + new File(root).getAbsoluteFile.toPath.normalize.toString

  /** Runs blocking file system operation on blocking IO dispatcher (the one used by FileIO and [[listAged]]) */
  private def blockingIO[T](body: => T)(implicit mat: Materializer): Future[T] =
    Future(body)(mat.system.dispatchers.lookup(ActorAttributes.IODispatcher.dispatcher))

  override def sourceFor(fullName: String): Source[ByteString, Any] = {
    FileIO.fromPath(java.nio.file.Paths.get(root + "/" + fullName))
  }

  override def storeFile(src: File, sha256: String)(implicit
    ec: ExecutionContext,
    mat: Materializer,
  ): Future[String] = blockingIO {
    val tailPath = new java.text.SimpleDateFormat(pathFormat).format(new jDate)
    val fullPath = new File(root + "/" + tailPath)
    fullPath.mkdirs
    val targetFile = new File(fullPath.toString + "/" + sha256)
    if (src.renameTo(targetFile))
      tailPath
    else if (targetFile.isFile) {
      // file with the same name (sha) is already stored - refresh its modification time
      // so that file cleanup does not consider it aged
      if (targetFile.setLastModified(System.currentTimeMillis)) {
        src.delete()
        tailPath
      } else throw new RuntimeException(s"Could not refresh modification time of stored file: $targetFile")
    } else
      throw new RuntimeException(s"Could not save file: $targetFile")
  }

  override def verify(fullName: String, size: Long, sha256: String)(implicit
    ec: ExecutionContext,
    mat: Materializer,
  ): Future[Option[Boolean]] = {
    val f = new File(root + "/" + fullName).toPath
    blockingIO {
      if (!Files.exists(f))
        Future.successful(None)
      else if (!Files.isRegularFile(f) || !Files.isReadable(f))
        Future.successful(Some(false))
      else
        FileIO.fromPath(f).runWith(sizeAndSha256sink).map { case (fileSize, fileSha) =>
          Some(fileSize == size && fileSha == sha256)
        }
    }.flatten
  }

  override def copy(src: String, dest: String)(implicit
    ec: ExecutionContext,
    mat: Materializer,
  ): Future[Unit] = blockingIO {
    if (src != dest) {
      val destPath = new File(root + "/" + dest).toPath
      Option(destPath.getParent).foreach(Files.createDirectories(_))
      Files.copy(new File(root + "/" + src).toPath, destPath, StandardCopyOption.REPLACE_EXISTING)
    }
  }

  /**
   * Directory entries with attributes, streamed from directory stream which is closed on completion.
   * Names are filtered before reading attributes. Unreadable directory or entries are skipped.
   */
  private def entries(dir: Path, nameFilter: String => Boolean): Source[(Path, BasicFileAttributes), NotUsed] =
    Source.unfoldResource[Path, Option[(DirectoryStream[Path], java.util.Iterator[Path])]](
      () => Try(Files.newDirectoryStream(dir, (p: Path) => nameFilter(p.getFileName.toString)))
        .toOption.map(ds => (ds, ds.iterator)),
      _.flatMap { case (_, it) =>
        // directory read failure ends listing of this directory, entries read so far are kept
        Try(if (it.hasNext) Some(it.next()) else None).toOption.flatten
      },
      _.foreach(_._1.close()),
    ).mapConcat { p =>
      Try(Files.readAttributes(p, classOf[BasicFileAttributes])).toOption.map(p -> _).toList
    }

  // Walks only directories of stored file layout yyyy/MM/dd[/HH[/mm]] - trash, tmp and other directories are skipped.
  // Directory depth: 0 - yyyy, 1 - MM, 2 - dd, 3 - HH or sha, 4 - mm or sha, 5 - sha
  private def walkAged(dir: Path, parent: String, depth: Int, minAge: Duration): Source[String, NotUsed] = {
    val nameFilter: String => Boolean =
      if (depth == 0) _.matches("\\d{4}")
      else name => name.matches("\\d\\d") || name.length == 64
    entries(dir, nameFilter).flatMapConcat { case (p, attrs) =>
      val name = if (parent.isEmpty) p.getFileName.toString else parent + "/" + p.getFileName
      if (attrs.isDirectory && depth < 5)
        walkAged(p, name, depth + 1, minAge)
      else if (attrs.isRegularFile && isStoredName(name) && isAged(attrs.lastModifiedTime.toMillis, minAge))
        Source.single(name)
      else Source.empty
    }
  }

  // Walk is blocking (readdir, stat), dispatcher must be set on the whole walk source: in fused stream
  // dispatcher of the whole island applies, so default IO dispatcher of unfoldResource is not effective,
  // flatMapConcat substreams run in the parent interpreter. Downstream stages fused with it may run
  // on IO dispatcher too.
  override def listAged(minAge: Duration): Source[String, Any] = {
    val rootPath = new File(root).toPath
    Source.single(rootPath).flatMapConcat { p =>
      // missing root is misconfiguration (or unmounted storage), not an empty storage
      if (Files.isDirectory(p)) walkAged(p, "", 0, minAge)
      else Source.failed(new NoSuchFileException(p.toString, null, "Storage root directory does not exist"))
    }.withAttributes(Attributes(ActorAttributes.IODispatcher))
  }

  override def trashAged(fullName: String, minAge: Duration)(implicit
    ec: ExecutionContext,
    mat: Materializer,
  ): Future[Option[Boolean]] = checkStoredName(fullName).flatMap { _ =>
    blockingIO {
      val file = new File(root + "/" + fullName).toPath
      Try(Files.readAttributes(file, classOf[BasicFileAttributes])).toOption match {
        case None => None
        case Some(attrs) if !attrs.isRegularFile => None
        case Some(attrs) if !isAged(attrs.lastModifiedTime.toMillis, minAge) => Some(false)
        case Some(_) =>
          val trashFile = new File(root + "/" + Trash + "/" + fullName).toPath
          Files.createDirectories(trashFile.getParent)
          try {
            Files.move(file, trashFile, StandardCopyOption.ATOMIC_MOVE)
            Some(true)
          } catch {
            case _: NoSuchFileException => None // removed meanwhile
          }
      }
    }
  }

  override def cleanTrash()(implicit
    ec: ExecutionContext,
    mat: Materializer,
  ): Future[Int] = blockingIO {
    val trash = new File(root + "/" + Trash).toPath
    var filesDeleted = 0
    if (Files.exists(trash))
      Files.walkFileTree(trash, new SimpleFileVisitor[Path] {
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
