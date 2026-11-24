package org.wabase.audit

import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.RestartSettings
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source}
import org.apache.pekko.util.ByteString
import org.apache.pekko.{Done, NotUsed}
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers
import org.wabase.msg._

import java.io.File
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.Success

class BufferedAuditSpecs extends FlatSpec with Matchers with Eventually {
  behavior of "BufferedAudit"

  implicit val system: ActorSystem = ActorSystem("buffered-audit-specs")
  implicit val executor: ExecutionContextExecutor = system.dispatcher

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

  it should ("buffer and deliver all records sequentially, switch and remove files") in {
    val path = new File(tmpdir + slash + "buffered-audit-seq-test")
    resetPath(path)
    fileCount(path) shouldBe 0
    var recordsWritten: List[String] = Nil
    def writeNewRecord(writer: BufferedAuditWriter): Unit = {
      val record = createNewRecord
      writer.writeRecord(ByteString(record))
      recordsWritten = record :: recordsWritten
    }
    val tinyFilesWriter = new BufferedAuditWriter(
      rootPath    = path.toPath,
      maxFileSize = 1, // small file size to test file management
      delimiter   = delimiter,
    )
    val smallFilesWriter = new BufferedAuditWriter(
      rootPath    = path.toPath,
      maxFileSize = 2 * (recordSize + delimiter.size), // small file size to test file management
      delimiter   = delimiter,
    )
    writeNewRecord(tinyFilesWriter)
    fileCount(path) shouldBe 1
    Thread.sleep(2)
    writeNewRecord(tinyFilesWriter)
    fileCount(path) shouldBe 2
    Thread.sleep(2)
    writeNewRecord(smallFilesWriter)
    fileCount(path) shouldBe 3
    Thread.sleep(2)
    writeNewRecord(smallFilesWriter)
    fileCount(path) shouldBe 3
    writeNewRecord(smallFilesWriter)
    fileCount(path) shouldBe 4
    val recordsWrittenBeforeRead = 5
    val consumer = new RecordConsumer(
      recordNumberToRejectionCount = Map.empty
    )
    val reader = new BufferedAuditReader(
      smallFilesWriter,
      consumer.confirmRecords,
    )
    eventually(timeout(1.minute), interval(1.second)) {
      recordsWritten.size shouldBe recordsWrittenBeforeRead
      consumer.consumedRecords shouldBe recordsWritten
      fileCount(path) shouldBe 2 // file cleanup done
      writeNewRecord(smallFilesWriter)
    }
    eventually(timeout(1.minute), interval(1.second)) {
      recordsWritten.size shouldBe (recordsWrittenBeforeRead + 1)
      consumer.consumedRecords shouldBe recordsWritten
    }
  }

  it should ("buffer records if called concurrently, backoff on exceptions, deliver all records") in {
    val path = new File(tmpdir + slash + "buffered-audit-concurrency-test")
    resetPath(path)
    fileCount(path) shouldBe 0
    val consumer = new RecordConsumer(
      recordNumberToRejectionCount = Map(1 -> 2, 9 -> 1, 20 -> 2, 99 -> 1)
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
      restartSettings = RestartSettings( // tuned to speed up tests - restart soon
        minBackoff    = 5.millis,
        maxBackoff    = 1.second,
        randomFactor  = 0.2 // adds 20% "noise" to vary the intervals slightly
      ),
    )
    val size = 100
    val records = Future.sequence((1 to size).map { i =>
      Future(i).map { i =>
        val record = createNewRecord
        writer.writeRecord(ByteString(record))
        record
      }
    })
    def expected = records.value.getOrElse(Success(Nil)).getOrElse(Nil)
    eventually(timeout(1.minute), interval(1.second)) {
      size                           shouldBe expected.size
      consumer.consumedRecords.size  shouldBe expected.size
      consumer.consumedRecords.toSet shouldBe expected.toSet
    }
  }

  it should "read message buffer settings" in {
    val defaults = MessageBufferedAudit.settings(ConfigFactory.load().getConfig("app.audit-queue"))
    defaults.provider shouldBe "main"
    defaults.topic shouldBe "audit"
    defaults.group shouldBe "audit"
    defaults.partition shouldBe None
    defaults.sendTimeout shouldBe 30.seconds
    BufferedAudit.implementation() shouldBe BufferedAudit.FileImplementation

    val custom = MessageBufferedAudit.settings(ConfigFactory.parseString(
      """provider = other
        |topic = t
        |group = g
        |partition = 2
        |send-timeout = 5s
        |""".stripMargin))
    custom shouldBe MessageBufferedAudit.Settings("other", "t", "g", Some(2), 5.seconds)
    BufferedAudit.implementation(ConfigFactory.parseString(
      "app.audit-queue.implementation = message")) shouldBe BufferedAudit.MessageImplementation
  }

  it should "pass reader settings from message buffer config" in {
    val defaults = MessageBufferedAudit.readerConfig(ConfigFactory.load().getConfig("app.audit-queue"))
    defaults.getDuration(MsgProvider.StopTimeoutPath) shouldBe java.time.Duration.ZERO
    defaults.getString("consumer.kafka-clients.auto.offset.reset") shouldBe "earliest"
    MessageBufferedAudit.bufferKeys.filter(defaults.hasPath) shouldBe Nil

    val custom = MessageBufferedAudit.readerConfig(ConfigFactory.parseString(
      """provider = other
        |topic = t
        |committer.max-batch = 5
        |stop-timeout = 3s
        |decode-error = skip
        |consumer.kafka-clients.auto.offset.reset = latest
        |""".stripMargin))
    custom.getDuration(MsgProvider.StopTimeoutPath) shouldBe java.time.Duration.ofSeconds(3)
    custom.getString("decode-error") shouldBe "skip"
    custom.getString("consumer.kafka-clients.auto.offset.reset") shouldBe "latest"
    MessageBufferedAudit.bufferKeys.filter(custom.hasPath) shouldBe Nil

    MessageBufferedAudit.readerConfig(null).isEmpty shouldBe true
    val committer = MessageBufferedAudit.committerConfig(ConfigFactory.load().getConfig("app.audit-queue"))
    committer.getInt("committer.max-batch") shouldBe 100
    committer.getDuration("committer.max-interval").toMillis shouldBe 10
    committer.getInt("committer.parallelism") shouldBe 1
    MessageBufferedAudit.committerConfig(null).isEmpty shouldBe true
    val partial = MessageBufferedAudit.committerConfig(ConfigFactory.parseString("committer.max-batch = 5"))
    partial.getInt("committer.max-batch") shouldBe 5
    partial.hasPath("committer.max-interval") shouldBe false
  }

  it should "deliver records published to a messaging provider, retrying failed batches" in {
    val provider = new InMemoryAuditMsgProvider
    val consumer = new RecordConsumer(
      recordNumberToRejectionCount = Map(1 -> 2, 9 -> 1, 20 -> 2, 99 -> 1)
    )
    val writer = new MessageBufferedAuditWriter(
      provider,
      topic         = "audit",
      sendTimeout   = 5.seconds,
      exitOnFailure = false,
    )
    val reader = new MessageBufferedAuditReader(
      provider,
      topic            = "audit",
      group            = "audit",
      saveAuditRecords = consumer.confirmRecords,
      maxBatchSize     = 3,
      maxBatchDelay    = 20.millis,
      restartSettings  = RestartSettings(
        minBackoff   = 5.millis,
        maxBackoff   = 1.second,
        randomFactor = 0.2,
      ),
    )
    val size = 100
    val records = Future.sequence((1 to size).map { _ =>
      val record = createNewRecord
      writer.writeRecord(ByteString(record)).map(_ => record)
    })
    def expected = records.value.getOrElse(Success(Nil)).getOrElse(Nil)
    eventually(timeout(1.minute), interval(200.millis)) {
      size                           shouldBe expected.size
      consumer.consumedRecords.size  shouldBe expected.size
      consumer.consumedRecords.toSet shouldBe expected.toSet
    }
    reader.topic shouldBe "audit"
    Await.result(reader.stop(), 5.seconds) shouldBe Done
  }

  it should "complete message reader when provider source completes, without restart" in {
    val provider = new InMemoryAuditMsgProvider
    val saved = new ConcurrentLinkedQueue[String]
    val writer = new MessageBufferedAuditWriter(provider, topic = "audit-completes", exitOnFailure = false)
    val reader = new MessageBufferedAuditReader(
      provider,
      topic            = "audit-completes",
      group            = "audit",
      saveAuditRecords = records => { records.foreach(r => saved.add(r.utf8String)); Future.unit },
      maxBatchDelay    = 20.millis,
      restartSettings  = RestartSettings(minBackoff = 5.millis, maxBackoff = 10.millis, randomFactor = 0),
    )
    Await.result(writer.writeRecord(ByteString("r1")), 5.seconds)
    eventually(timeout(5.seconds), interval(20.millis)) { saved.toArray.toList shouldBe List("r1") }
    provider.completeSources.set(true)
    Await.result(reader.done, 5.seconds) shouldBe Done
    provider.completeSources.set(false)
    Await.result(writer.writeRecord(ByteString("r2")), 5.seconds)
    Thread.sleep(200)
    saved.toArray.toList shouldBe List("r1")
  }

  it should "fail a message write when the provider rejects it and exitOnFailure is off" in {
    val provider = new InMemoryAuditMsgProvider
    provider.failSends.set(true)
    val writer = new MessageBufferedAuditWriter(
      provider,
      topic         = "audit",
      sendTimeout   = 5.seconds,
      exitOnFailure = false,
    )
    val ex = intercept[Exception] {
      Await.result(writer.writeRecord(ByteString("record")), 5.seconds)
    }
    def messages(t: Throwable, guard: Int = 0): String =
      if (t == null || guard > 8) ""
      else Option(t.getMessage).getOrElse("") + "\n" + messages(t.getCause, guard + 1)
    messages(ex) should include ("broker down")
  }
}

/** Messaging provider that keeps published bytes in memory. The reader polls and emits only
  * offsets that are not committed yet, so a restarted consumer retries a failed batch.
  */
class InMemoryAuditMsgProvider extends MsgProvider {
  private case class Stored(topic: String, offset: Long, value: Array[Byte])
  private val records = new ConcurrentLinkedQueue[Stored]
  private val lock = new Object
  private var nextOffset = 0L
  @volatile private var committedOffset = -1L
  val failSends = new AtomicBoolean(false)
  /** Reader sources complete, as provider sources do when provider is closed */
  val completeSources = new AtomicBoolean(false)

  private def append(topic: String, value: Array[Byte]): Long = lock.synchronized {
    val offset = nextOffset
    nextOffset += 1
    records.add(Stored(topic, offset, value))
    offset
  }

  override def createReader(config: com.typesafe.config.Config, keyDecoder: MsgDecoder, valueDecoder: MsgDecoder): MsgReader =
    new MsgReader {
      private val decoder = if (valueDecoder != null) valueDecoder else BytesMsgCodec
      private def pending(topic: String, from: Long): List[CommittableMessage[InMessage]] = {
        val buf = List.newBuilder[CommittableMessage[InMessage]]
        val it = records.iterator()
        while (it.hasNext) {
          val rec = it.next()
          if (rec.topic == topic && rec.offset >= from) {
            val decoded = decoder.decode(topic, Nil, rec.value)
            buf += CommittableMessage(
              InMessage(topic, null, decoded, 0, rec.offset, Nil, 0L),
              InMemoryAuditCommittable(rec.offset),
            )
          }
        }
        buf.result()
      }
      override def committableSource(subscription: Subscription): Source[CommittableMessage[InMessage], Control] = {
        val topic = subscription match {
          case Subscription.Topics(topics, _) if topics.nonEmpty => topics.head
          case other => sys.error(s"Unsupported subscription: $other")
        }
        val stopped = new AtomicBoolean(false)
        Source.tick(0.millis, 10.millis, ())
          .takeWhile(_ => !stopped.get && !completeSources.get)
          .statefulMapConcat { () =>
            var next = committedOffset + 1
            (_: Unit) => {
              val batch = pending(topic, next)
              if (batch.nonEmpty) next = batch.last.message.offset + 1
              batch
            }
          }
          .mapMaterializedValue { _ =>
            new Control {
              override def drainOn(streamCompletion: Future[Any]): Unit = ()
              override def stop(): Future[Done] = { stopped.set(true); Future.successful(Done) }
            }
          }
      }
      override def source(subscription: Subscription): Source[InMessage, Control] =
        sys.error("plain source is not used")
      override def atMostOnceSource(subscription: Subscription): Source[InMessage, Control] =
        sys.error("at most once source is not used")
      override def committableSourceWithMetadata(
        subscription: Subscription,
        metadata: InMessage => String,
      ): Source[CommittableMessage[InMessage], Control] =
        sys.error("metadata source is not used")
    }

  override def createSender(config: com.typesafe.config.Config): MsgSender = new MsgSender {
    override def optionalFlow[P]: Flow[(Option[OutMessage], P), (Option[SendResult], P), NotUsed] =
      Flow[(Option[OutMessage], P)].map { case (om, p) =>
        if (failSends.get) sys.error("broker down")
        (om.map { m =>
          val bytes = m.value match {
            case b: Array[Byte] => b
            case other =>
              Option(m.valueEncoder).getOrElse(BytesMsgCodec).encode(m.topic, m.headers, other)
          }
          val offset = append(m.topic, bytes)
          SendResult(m.topic, m.partition.getOrElse(0), offset, 0L)
        }, p)
      }
  }

  override def createCommitter(config: com.typesafe.config.Config): MsgCommitter = new MsgCommitter {
    override def flow: Flow[Committable, Done, NotUsed] = Flow[Committable].map {
      case InMemoryAuditCommittable(offset) =>
        lock.synchronized {
          if (offset > committedOffset) committedOffset = offset
        }
        Done
      case other => sys.error(s"Unexpected committable: $other")
    }
  }

  override def admin: MsgAdmin = sys.error("admin is not used")
  override val codecs: MsgCodecs = new MsgCodecs(com.typesafe.config.ConfigFactory.empty)
  override def close(): Future[Done] = Future.successful(Done)
}

case class InMemoryAuditCommittable(offset: Long) extends Committable
