package org.wabase
package audit

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.pekko.Done
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.apache.pekko.stream.{ActorAttributes, Attributes, Materializer, RestartSettings, SystemMaterializer}
import org.apache.pekko.util.ByteString
import org.wabase.msg._

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

import java.util.concurrent.TimeoutException

/** Audit buffer backed by a messaging provider ([[org.wabase.msg.MsgProvider]]).
  * [[MessageBufferedAuditWriter]] publishes each record. The write future completes when the provider accepts it.
  * [[MessageBufferedAuditReader]] reads the same topic, inserts a batch into the audit database,
  * then commits that batch before it saves the next one. Delivery is at least once: a record can
  * be inserted again when a batch was saved but not yet committed.
  */
class MessageBufferedAudit(
  val writer: MessageBufferedAuditWriter,
  val reader: MessageBufferedAuditReader,
) extends AuditBuffer {
  override def writeRecord(record: ByteString): Future[Unit] = writer.writeRecord(record)
}

object MessageBufferedAudit {
  /** `app.audit-queue` keys used by [[apply]]. */
  case class Settings(
    provider: String,
    topic: String,
    group: String,
    partition: Option[Int],
    sendTimeout: FiniteDuration,
  )

  /** `app.audit-queue` keys used by the buffer itself, not passed to the reader. */
  val bufferKeys: Seq[String] =
    Seq("implementation", "path", "provider", "topic", "group", "partition", "send-timeout", "committer")

  /** Reader config: `app.audit-queue` without [[bufferKeys]], for example `stop-timeout`,
    * `decode-error`, or kafka `consumer` settings. Restart stops the consumer after the attempt ends,
    * see [[MsgStreams.restartPipeline]].
    */
  def readerConfig(cfg: Config): Config =
    if (cfg == null) ConfigFactory.empty
    else bufferKeys.foldLeft(cfg)(_ withoutPath _)

  /** Committer for one saved database batch (`app.audit-queue.committer`). The reader commits a whole batch
    * before it saves the next one, and the committer flushes when that batch ends, so `max-batch` sets how many
    * commit requests a batch takes. Empty when `cfg` is null or has no `committer` object.
    */
  def committerConfig(cfg: Config): Config =
    if (cfg != null && cfg.hasPath("committer")) cfg.getConfig("committer").atPath("committer")
    else ConfigFactory.empty

  /** `provider`, `topic`, `group` and `send-timeout` are required. `partition` is optional. */
  def settings(cfg: Config): Settings = Settings(
    provider    = cfg.getString("provider"),
    topic       = cfg.getString("topic"),
    group       = cfg.getString("group"),
    partition   = if (cfg.hasPath("partition")) Some(cfg.getInt("partition")) else None,
    sendTimeout = cfg.getDuration("send-timeout"),
  )

  /** Reader and writer for `app.audit-queue`, using providers of the running server. */
  def apply(saveAuditRecords: Seq[ByteString] => Future[Unit])(implicit system: ActorSystem): MessageBufferedAudit =
    apply(saveAuditRecords, WabaseServer.app.msgProviders)

  /** Reader and writer for `app.audit-queue` on `providers`. */
  def apply(
    saveAuditRecords: Seq[ByteString] => Future[Unit],
    providers: WabaseMsgProviders,
  )(implicit system: ActorSystem): MessageBufferedAudit = {
    val cfg = config.getConfig("app.audit-queue")
    val s = settings(cfg)
    val provider =
      try providers.provider(s.provider)
      catch {
        case NonFatal(e) =>
          throw new RuntimeException(
            s"Audit buffer implementation is message but messaging provider '${s.provider}' is not available. " +
              s"Configure msg-provider.${s.provider} or app.audit-queue.provider. ${e.getMessage}", e)
      }
    val writer = new MessageBufferedAuditWriter(
      provider,
      topic       = s.topic,
      partition   = s.partition,
      sendTimeout = s.sendTimeout,
    )
    val reader = new MessageBufferedAuditReader(
      provider,
      topic            = s.topic,
      group            = s.group,
      saveAuditRecords = saveAuditRecords,
      readerConfig     = readerConfig(cfg),
      committerConfig  = committerConfig(cfg),
    )
    new MessageBufferedAudit(writer, reader)
  }
}

/** Publishes one audit record. The returned future completes when the provider accepts it,
  * or fails after [[sendTimeout]]. The record bytes are the message value, encoded with [[BytesMsgCodec]].
  * On failure the process exits when [[exitOnFailure]] is set, as [[BufferedAuditWriter]] does
  * when a file write fails.
  */
class MessageBufferedAuditWriter(
  val provider: MsgProvider,
  val topic: String,
  val partition: Option[Int] = None,
  val key: Any = null,
  val sendTimeout: FiniteDuration = config.getDuration("app.audit-queue.send-timeout"),
  val exitOnFailure: Boolean = true,
  val senderConfig: Config = ConfigFactory.empty,
)(implicit val system: ActorSystem) extends Loggable {
  private val sender = provider.createSender(senderConfig)
  private implicit val mat: Materializer = SystemMaterializer(system).materializer
  private implicit val ec: ExecutionContext = system.dispatcher

  def writeRecord(record: ByteString): Future[Unit] = {
    val message = OutMessage(
      topic        = topic,
      key          = key,
      value        = record.toArray,
      partition    = partition,
      valueEncoder = BytesMsgCodec,
    )
    val accepted =
      try accept(sender.send(message))
      catch { case NonFatal(ex) => Future.failed(ex) }
    if (exitOnFailure) accepted.andThen { case Failure(ex) =>
      logger.error("Failed to audit, exiting", ex)
      System.exit(-1)
    } else accepted
  }

  /** Completes when `send` does, or with a timeout failure. The timeout is cancelled when `send` finishes. */
  private def accept(send: Future[SendResult]): Future[Unit] = {
    val promise = Promise[Unit]()
    val timeout = system.scheduler.scheduleOnce(sendTimeout) {
      promise.tryFailure(new TimeoutException(
        s"Timed out after $sendTimeout waiting for audit message to be accepted on topic '$topic'"))
    }
    send.onComplete {
      case Success(_) =>
        timeout.cancel()
        promise.trySuccess(())
      case Failure(ex) =>
        timeout.cancel()
        promise.tryFailure(ex)
    }
    promise.future
  }
}

/** Reads audit records from `topic` as consumer `group`, saves batches with `saveAuditRecords`,
  * then commits that batch before the next one is saved. A failed batch is not committed.
  * Reading, saving and committing restart together with backoff, from the last committed offset.
  * A value that is not bytes fails the stream and is not retried until the process starts again.
  * Reading ends with [[stop]], with a permanent failure, or when the provider source completes (for example,
  * provider is closed) - it is logged and not restarted.
  */
class MessageBufferedAuditReader(
  val provider: MsgProvider,
  val topic: String,
  val group: String,
  val saveAuditRecords: Seq[ByteString] => Future[Unit],
  val maxBatchSize: Int = 100,
  val maxBatchDelay: FiniteDuration = 200.millis,
  val restartSettings: RestartSettings = RestartSettings(
    minBackoff   = 5.seconds,
    maxBackoff   = 5.minutes,
    randomFactor = 0.2,
  ),
  val readerConfig: Config = MessageBufferedAudit.readerConfig(config.getConfig("app.audit-queue")),
  val committerConfig: Config = MessageBufferedAudit.committerConfig(config.getConfig("app.audit-queue")),
)(implicit val system: ActorSystem) extends Loggable {
  implicit val ec: ExecutionContextExecutor = system.dispatcher
  private implicit val mat: Materializer = SystemMaterializer(system).materializer
  private val reader = provider.createReader(readerConfig, keyDecoder = null, valueDecoder = BytesMsgCodec)
  private val committer = provider.createCommitter(committerConfig)
  private val subscription = Subscription.Topics(Set(topic), Some(group))
  /** Shared across restarts so a save still running from the failed attempt finishes before the next one. */
  private val saves = new MsgStreams.Sequential

  private def recordBytes(message: InMessage): Option[ByteString] =
    if (message == null || message.value == null) None
    else message.value match {
      case b: Array[Byte] => Some(ByteString.fromArrayUnsafe(b))
      case other =>
        throw new MsgStreams.ProcessingFailure(new IllegalArgumentException(
          s"Audit message value at ${message.topic}-${message.partition}@${message.offset} " +
            s"must be bytes, got: ${other.getClass.getName}"))
    }

  logger.info(s"Reading audit records from messaging topic '$topic', group '$group'")
  private val auditReader = MsgStreams.restartPipeline(restartSettings) { () =>
    reader.committableSource(subscription)
      .map { case CommittableMessage(message, committable) =>
        val offset = if (message == null) -1L else message.offset
        (recordBytes(message), committable, offset)
      }
      .groupedWithin(maxBatchSize, maxBatchDelay)
      .mapAsync(parallelism = 1) { batch =>
        // Completes only after this batch is committed, so mapAsync(1) starts the next batch
        // then. A later failure restarts after this offset and does not insert this batch again.
        saves {
          val records = batch.flatMap(_._1)
          val persist =
            if (records.isEmpty) Future.successful(())
            else saveAuditRecords(records)
          persist.flatMap { _ =>
            Source(batch.iterator.map(_._2).toList).runWith(committer.sink).map(_ => ())
          }.recoverWith { case ex =>
            val offset = batch.iterator.map(_._3).find(_ >= 0).getOrElse(-1L)
            Future.failed(new RuntimeException(
              s"Failed to process audit queue - ${records.size} record(s) at offset $offset", ex))
          }
        }
      }
      .named("audit-message-reader")
      .addAttributes(Attributes(ActorAttributes.IODispatcher))
  }.toMat(Sink.ignore)(Keep.both).run()

  @volatile private var stopRequested = false

  /** Completes when the reader has stopped, fails with the cause of a permanent failure. */
  val done: Future[Done] = auditReader._2

  /** Stops reading. Batch in progress is not committed and is read again by the next reader of the group. */
  def stop(): Future[Done] = {
    stopRequested = true
    auditReader._1.stop().flatMap(_ => done)
  }

  done.onComplete {
    case Success(_) if stopRequested =>
      logger.info(s"Audit message reader of topic '$topic', group '$group' stopped")
    case Success(_) =>
      logger.warn(s"Audit message reader of topic '$topic', group '$group' completed, audit records are no longer read")
    case Failure(ex) =>
      logger.error(s"Audit message reader of topic '$topic', group '$group' stopped", ex)
  }
}
