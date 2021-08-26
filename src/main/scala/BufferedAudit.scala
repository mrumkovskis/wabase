package org.wabase

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{Framing, RestartSource, Sink, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.util.ByteString
import org.wabase.{Loggable, config}

import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardOpenOption}
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}
import scala.concurrent.Future
import scala.concurrent.duration._

class BufferedAudit(
  val writer: BufferedAuditWriter,
  val reader: BufferedAuditReader,
) {}
object BufferedAudit {
  sealed trait Notification
  case object Notification extends Notification
  def apply(saveAuditRecords: (Seq[ByteString]) => Future[Unit])(implicit system: ActorSystem): BufferedAudit = {
    val writer = new BufferedAuditWriter
    val reader = new BufferedAuditReader(writer, saveAuditRecords)
    new BufferedAudit(writer, reader)
  }
}
import BufferedAudit._

class BufferedAuditWriter(
  val rootPath: Path         = new File(config.getString("app.audit-queue.path")).toPath,
  val filenamePrefix: String = "audit-queue-",
  val maxFileSize: Int       = 16 * 1024 * 1024,
  val delimiter: ByteString  = ByteString("\r\n"),
  val exitOnFailure: Boolean = true,
)(implicit val system: ActorSystem) extends Loggable {
  val filenameDateTime = DateTimeFormatter
    .ofPattern("yyyy-MM-dd--HH-mm-ss-SSS")
    .withZone(ZoneOffset.UTC) // Reader expects ascending filenames. Using UTC to avoid problems with DST.
  private var filename: String = null
  private var channel: FileChannel = _
  implicit val executionContext = system.dispatcher
  val (fileCreationQueue, fileCreationNotificationsSource) =
    Source.queue[Notification](bufferSize = 1).preMaterialize() // for notifications only - best to drop superfluous elements
  fileCreationNotificationsSource.runWith(Sink.ignore)          // XXX keep it alive - https://github.com/akka/akka/issues/28926
  val (fileContentChangeQueue, fileContentChangeNotificationsSource) =
    Source.queue[Notification](bufferSize = 1).preMaterialize() // for notifications only - best to drop superfluous elements
  fileContentChangeNotificationsSource.runWith(Sink.ignore)     // XXX keep it alive - https://github.com/akka/akka/issues/28926
  private def getChannel = {
    if (channel == null) {
      val file = new File(rootPath.toFile, filenamePrefix + filenameDateTime.format(Instant.now()))
      if (!file.createNewFile)
        sys.error("Failed to create file " + file)
      else
        filename = file.getName
      fileCreationQueue.offer(Notification)
      channel = FileChannel.open(file.toPath, StandardOpenOption.WRITE)
    }
    channel
  }
  def currentFilename: String = this.synchronized { filename }
  def writeRecord(record: ByteString): Unit =
    try {
      this.synchronized {
        var ch = getChannel
        if (ch.position > 0 && ch.position + record.length > maxFileSize) {
          ch.close
          channel = null
          ch = getChannel
        }
        ch.write(record.toByteBuffer)
        ch.write(delimiter.toByteBuffer)
        ch.force(true)
      }
      fileContentChangeQueue.offer(Notification)
    } catch {
      case util.control.NonFatal(ex) =>
        if (exitOnFailure) {
          logger.error("Failed to audit, exiting", ex)
          System.exit(-1)
        } else {
          throw ex
        }
    }
}

class BufferedAuditReader(
  val writer: BufferedAuditWriter,
  val saveAuditRecords: (Seq[ByteString]) => Future[Unit],
  val maxRecordSize: Int                = 64 * 1024,
  val maxBatchSize: Int                 = 100,
  val maxBatchDelay: FiniteDuration     = 200.millis,
  val restartSettings: RestartSettings  = RestartSettings(
    minBackoff    = 5.seconds,
    maxBackoff    = 5.minutes,
    randomFactor  = 0.2 // adds 20% "noise" to vary the intervals slightly
  ),
)(implicit val system: ActorSystem) {
  import writer.{filenamePrefix, rootPath}
  implicit val executionContext = system.dispatcher
  private val buffer = ByteBuffer.allocate(256)
  private var file: File = _
  private var channel: FileChannel = _
  val controlFileName = filenamePrefix + "next-read"
  private def getChannel = {
    if (channel == null) {
      file = new File(rootPath.toFile, controlFileName)
      if (!Files.exists(file.toPath)) {
        if (!file.createNewFile)
          sys.error("Failed to create file " + file)
      } else if (Files.isDirectory(file.toPath)) {
          sys.error("Expected regular file, found directory: " + file)
      }
      channel = FileChannel.open(file.toPath, StandardOpenOption.READ, StandardOpenOption.WRITE)
    }
    channel
  }
  private def currentFilenameAndPos: (String, Int) = {
    this.synchronized {
      var ch = getChannel
      if (ch.size == 0)
        (null, 0)
      else {
        buffer.clear
        ch.position(0)
        while (ch.read(buffer) >= 0) {}
        buffer.flip
        val parts = StandardCharsets.UTF_8.decode(buffer).toString().split('\t')
        (parts(0), parts(1).toInt)
      }
    }
  }
  private def currentFilename: String = Option(currentFilenameAndPos).map(_._1).orNull
  def isAuditQueueFile(path: Path): Boolean = {
    val filename = path.getFileName.toString
    val result = {
      path.getParent == rootPath          &&
      filename.startsWith(filenamePrefix) &&
      filename != controlFileName         &&
      !path.toFile.isDirectory
    }
    result
  }
  def isProcessedAuditQueueFile(path: Path): Boolean =
    isAuditQueueFile(path) && Option(currentFilename).map(_  > path.getFileName.toString).getOrElse(false)
  def isUnprocessedAuditQueueFile(path: Path): Boolean =
    isAuditQueueFile(path) && Option(currentFilename).map(_ <= path.getFileName.toString).getOrElse(true)
  val (fileReadCompletedQueue, fileReadCompletedNotificationSource) =
    Source.queue[Notification](bufferSize = 1).preMaterialize() // for notifications only - best to drop superfluous elements
  fileReadCompletedNotificationSource.runWith(Sink.ignore)      // XXX keep it alive - https://github.com/akka/akka/issues/28926
  private val fileCleanup = Source
    .single(Notification)
    .concat(fileReadCompletedNotificationSource)
    .flatMapConcat { _ => Source
      .fromJavaStream(() => Files.list(rootPath).sorted)
      .filter(isProcessedAuditQueueFile)
      .mapAsync(parallelism = 1)(path => Future { Files.delete(path) }.map(_ => path))
      .recover {
        case e: Throwable => throw e // for logging
      }
      .named("audit-queue-cleaner")
      .addAttributes(Attributes(ActorAttributes.IODispatcher))
    }
    .run()
  private def saveNextReadPos(path: Path, pos: Int): Unit =
    try {
      val record = StandardCharsets.UTF_8.encode(s"${path.getFileName}\t$pos")
      this.synchronized {
        var ch = getChannel
        ch.position(0)
        ch.write(record)
        ch.truncate(ch.position())
        ch.force(true)
      }
    } catch {
      case util.control.NonFatal(ex) =>
        throw new RuntimeException("Failed to persist audit queue next read position", ex)
    }
  private val auditReader = RestartSource.withBackoff(restartSettings) { () => Source
    .single(Notification)
    .concat(writer.fileCreationNotificationsSource)
    .flatMapConcat { _ =>
      Source.fromJavaStream(() => Files.list(rootPath).sorted)
    }
    .filter(isUnprocessedAuditQueueFile)
    .statefulMapConcat { () => // deduplicate - accept only ascending
      var last: Path = null
      (path) =>
        if (last == null || last.toString < path.toString) {
          last = path
          path :: Nil
        } else Nil
     }
    .map { path =>
      val filename = path.getFileName.toString
      val (readerFilename, readerPos) = currentFilenameAndPos
      val writerFilename = writer.currentFilename
      val pos = if (filename == readerFilename) readerPos else 0
      val source = Source.single(Notification).concat {
        if  (filename == writerFilename)
             writer.fileContentChangeNotificationsSource
        else Source.empty
      }
      (source.via(new BufferedAuditFlow(path, pos, writer)), path)
    }
    .flatMapConcat { case (source, path) => source
      .via(Framing.delimiter(writer.delimiter, maximumFrameLength = maxRecordSize, allowTruncation = false))
      .scan((ByteString.empty, 0))((bytesAndPos, bytes) => bytesAndPos match {
        case (prevBytes, pos) => (bytes, pos + bytes.size + writer.delimiter.size)
      })
      .filter { case (bytes, nextPos) => nextPos > 0 }
      .map { case (bytes, nextPos) => (bytes, path, nextPos) }
    }
    .groupedWithin(maxBatchSize, maxBatchDelay)
    // do NOT run in parallel to properly keep track of current record on persistent storage!
    .mapAsync(parallelism = 1) { case bytesAndPathAndNextPos =>
      val records = bytesAndPathAndNextPos.map(_._1)
      saveAuditRecords(records).map { _ =>
        bytesAndPathAndNextPos.last match {
          case (_, path, nextPos) =>
            saveNextReadPos(path, nextPos)
            path
        }
      }
    }
    .statefulMapConcat { () =>
      var last: Path = null
      (path) =>
        if (last != path) {
          fileReadCompletedQueue.offer(Notification)
          last = path
          path :: Nil
        } else Nil
     }
    .recover {
      case e: Throwable => throw e // for logging
    }
    .named("audit-queue-reader")
    .addAttributes(Attributes(ActorAttributes.IODispatcher))
  }
  .runWith(Sink.ignore)
}

class BufferedAuditFlow(path: Path, pos: Int, writer: BufferedAuditWriter) extends GraphStage[FlowShape[Notification, ByteString]] {
  val in = Inlet[Notification]("BufferedAuditFlow.in")
  val out = Outlet[ByteString]("BufferedAuditFlow.out")
  val shape = FlowShape.of(in, out)
  val filename = path.getFileName.toString
  val bufferSize = 4096
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      val channel = FileChannel.open(path, StandardOpenOption.READ)
      val buf = ByteBuffer.allocate(bufferSize)
      channel.position(pos)
      def getChunk =
        if (channel.read(buf) > 0) {
          buf.flip()
          val bs = ByteString(buf)
          buf.clear()
          bs
        } else ByteString.empty
      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          grab(in)
          val isFinal = filename != writer.currentFilename
          val chunk = getChunk
          if (chunk.isEmpty) {
            if (isFinal)
              completeStage()
            else
              pull(in)
          } else emit(out, chunk)
        }
      })
      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          val isFinal = filename != writer.currentFilename
          val chunk = getChunk
          if (chunk.isEmpty) {
            if (isFinal)
              completeStage()
            else if (!hasBeenPulled(in))
              pull(in)
          } else emit(out, chunk)
        }
      })
      override def postStop(): Unit =
        channel.close()
    }
}
