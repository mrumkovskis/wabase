package org.wabase

import akka.actor.ActorSystem
import akka.util.ByteString
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.concurrent._
import scala.concurrent.duration._

class BufferedAuditSpecs extends FlatSpec with Matchers with Eventually {
  behavior of "BufferedAudit"

  implicit val system = ActorSystem("buffered-audit-specs")
  implicit val executor = system.dispatcher

  private val slash  = System.getProperty("file.separator")
  private val tmpdir = System.getProperty("java.io.tmpdir").stripSuffix(slash)

  private def fileCount(file: File): Int = {
    if (file.isDirectory)
      Option(file.listFiles).map(_.toSeq).getOrElse(Nil).size
    else sys.error(s"Not a directory: $file")
  }

  private def deleteFilesRecursively(file: File): Unit = {
    if (file.isDirectory)
      Option(file.listFiles).map(_.toSeq).getOrElse(Nil).foreach(deleteFilesRecursively)
    file.delete
  }

  private def resetPath(file: File) = {
    deleteFilesRecursively(file)
    file.mkdirs
  }

  class RecordConsumer(recordNumberToRejectionCount: Map[Int, Int]) {
    private var confirmedRecords: List[String] = Nil
    private var remainingRecordNumbersToRejectionCount: Map[Int, Int] = recordNumberToRejectionCount.toMap
    private def maybeReject(recordNumber: Int) = this.synchronized {
      remainingRecordNumbersToRejectionCount.getOrElse(recordNumber, 0) match {
        case 0 => // pass
        case n =>
          remainingRecordNumbersToRejectionCount =
            remainingRecordNumbersToRejectionCount + (recordNumber -> (n - 1))
          sys.error(s"For buffered audit test purposes - rejecting record number $recordNumber")
      }
    }
    private def confirmRecord(record: ByteString) = this.synchronized {
      confirmedRecords = record.utf8String :: confirmedRecords
    }
    def consumedRecords = this.synchronized {
      confirmedRecords
    }
    def confirmRecords(records: Seq[ByteString]): Future[Unit] = Future {
      (confirmedRecords.size to (confirmedRecords.size + records.size - 1)) foreach maybeReject
      records foreach confirmRecord
    }
  }

  private def createNewRecord: String =
    java.util.UUID.randomUUID.toString

  private val recordSize    = ByteString(createNewRecord).size
  private val delimiter     = ByteString("\r\n")

  it should ("buffer records if called concurrently, backoff on exceptions, deliver all records") in {
    val path = new File(tmpdir + slash + "buffered-audit-concurrency-test")
    resetPath(path)
    fileCount(path) shouldBe 0
    val consumer = new RecordConsumer(
      recordNumberToRejectionCount = Map(1 -> 1, 9 -> 1)
    )
    val writer = new BufferedAuditWriter(
      rootPath    = path.toPath,
      maxFileSize = 20 * (recordSize + delimiter.size) + 1, // small file size to test file management
      delimiter   = delimiter,
    )
    val reader = new BufferedAuditReader(
      writer,
      consumer.confirmRecords,
      maxBatchSize = 3, // small batch size to test current position management
    )
    val records = (1 to 100).map { i =>
      Future(i).map { i =>
        val record = createNewRecord
        writer.writeRecord(ByteString(record))
        record
      }
    }
    val expected = Await.result(Future.sequence(records), 1.minute)
    eventually(timeout(1.minute), interval(1.second)) {
      consumer.consumedRecords.size  shouldBe expected.size
      consumer.consumedRecords.toSet shouldBe expected.toSet
    }
  }
}
