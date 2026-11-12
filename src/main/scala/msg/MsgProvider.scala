package org.wabase.msg

import com.typesafe.config.Config
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.apache.pekko.{Done, NotUsed}

import scala.concurrent.{ExecutionContext, Future}

/** Messaging provider, created once per configured provider (see [[WabaseMsgProviders]]).
  * Provider owns resources shared by its readers, senders, committers and admin
  * (connections, producers, running consumers), they are released by [[close]].
  *
  * `config` parameter of factory methods contains operation specific settings,
  * provider uses its own configuration as a fallback.
  *
  * Key and value decoders passed to [[createReader]] may be `null`, then decoders are resolved by [[codecs]] from
  * `key-codec`, `value-codec` of operation config, falling back to provider config, then to
  * [[MsgCodec.defaultKeyCodec]], [[MsgCodec.defaultValueCodec]]. Sender's encoders ([[createSender]]) are resolved
  * from config the same way, message may specify its own encoders ([[OutMessage.keyEncoder]],
  * [[OutMessage.valueEncoder]]) - incoming message cannot carry its decoder, outgoing message can carry its encoder.
  *
  * Generic operation config keys (provider specific keys may be used as well):
  *  - `committer { max-batch, max-interval, parallelism, delivery, when }` - committer settings, see [[createCommitter]]
  *  - `decode-error` - `fail` or `skip` message which cannot be decoded, see [[MsgProvider.DecodeErrorPath]]
  *  - `stop-timeout` - how long consumer waits for pending commits after its source is cancelled, see
  *    [[MsgProvider.StopTimeoutPath]]
  */
trait MsgProvider {
  def createReader(
    config: Config,
    keyDecoder: MsgDecoder = null,
    valueDecoder: MsgDecoder = null,
  ): MsgReader
  def createSender(config: Config): MsgSender
  def createCommitter(config: Config): MsgCommitter
  def admin: MsgAdmin
  /** Codecs of the provider - built-in and configured, see [[MsgCodecs]]. */
  def codecs: MsgCodecs
  /** Stops running consumers (see [[Control.stop]]), then releases provider resources. */
  def close(): Future[Done]
}

object MsgProvider {
  /** Operation config key - duration, how long consumer waits for pending commits of already emitted messages
    * after its source is cancelled (stream is not stopped by [[Control.stop]] with [[Control.drainOn]]).
    * Can be `0` if consumer is stopped by drain or only after stream completes (as msg operations do) -
    * pending commits complete before consumer is stopped.
    * Provider defaults (for example kafka consumer `stop-timeout` 30s) are safe for cancellation.
    */
  val StopTimeoutPath = "stop-timeout"

  /** Operation config key - what reader does with message which cannot be decoded:
    *  - `fail` (default) - stream fails with [[MsgDecodingException]]
    *  - `skip` - message is logged (warning with message position) and skipped, see [[MsgReader]]
    */
  val DecodeErrorPath = "decode-error"
  val DecodeErrorFail = "fail"
  val DecodeErrorSkip = "skip"
  /** Whether decode error strategy is skip, error message (`Left`) for unknown strategy */
  def decodeErrorSkip(strategy: String): Either[String, Boolean] = strategy match {
    case DecodeErrorSkip => Right(true)
    case DecodeErrorFail => Right(false)
    case x => Left(s"'$DecodeErrorFail' or '$DecodeErrorSkip' expected, got: '$x'")
  }
  /** Whether reader skips messages which cannot be decoded, see [[DecodeErrorPath]] */
  def skipDecodeErrors(config: Config): Boolean =
    config.hasPath(DecodeErrorPath) && decodeErrorSkip(config.getString(DecodeErrorPath))
      .fold(e => sys.error(s"Reader configuration '$DecodeErrorPath': $e"), identity)

  // same thread execution context for future callbacks without blocking,
  // can be replaced with ExecutionContext.parasitic when scala 2.12 support is dropped
  private[msg] val syncEc: ExecutionContext = ExecutionContext.fromExecutor((r: Runnable) => r.run())
}

/** Source of messages specified by subscription.
  * Message key and value are decoded with decoders of the reader, see [[InMessage]].
  * Decoding failure fails the stream with [[MsgDecodingException]], it is permanent - the same message fails again
  * if the stream is restarted. If reader is configured to skip such messages ([[MsgProvider.DecodeErrorPath]]),
  * plain and at most once sources do not emit them, committable sources emit [[CommittableMessage]] with `null`
  * message, so that skipped message is committed in order (like message which is filtered out), with commit
  * metadata of the previous message of its partition read by the source (empty if there is none).
  */
trait MsgReader {
  /** Messages are not committed. */
  def source(subscription: Subscription): Source[InMessage, Control]
  /** Messages are committed by provider before they are emitted. */
  def atMostOnceSource(subscription: Subscription): Source[InMessage, Control]
  /** Messages are committed when their committables are passed to [[MsgCommitter.sink]]. */
  def committableSource(subscription: Subscription): Source[CommittableMessage[InMessage], Control]
  /** Like [[committableSource]], `metadata` function result is committed together with offset. */
  def committableSourceWithMetadata(
    subscription: Subscription,
    metadata: InMessage => String,
  ): Source[CommittableMessage[InMessage], Control]
}

trait MsgCommitter {
  /** Commits committables, batching them according to committer settings, emits `Done` for each committed batch.
    * Completes when upstream completes and all committables are committed, commit failure fails the stream
    * (so that it can be restarted together with the source, see [[org.apache.pekko.stream.scaladsl.RestartSource]]).
    */
  def flow: Flow[Committable, Done, NotUsed]
  /** Commits committables, see [[flow]].
    * Materialized future completes when stream completes and all committables are committed.
    */
  def sink: Sink[Committable, Future[Done]] = flow.toMat(Sink.ignore)(Keep.right)
}

trait MsgSender {
  /** Sends messages, preserving order, passes through value `P` together with send result.
    * `None` message is not sent, `P` is passed through in order with `None` result
    * (for example, committable of incoming message which is filtered out, like kafka `PassThroughMessage`).
    * Message key and value are encoded with encoders of the message, if not specified - of the sender,
    * see [[OutMessage]].
    * Encoding failure fails the stream with [[MsgEncodingException]].
    */
  def optionalFlow[P]: Flow[(Option[OutMessage], P), (Option[SendResult], P), NotUsed]

  /** Sends messages, preserving order, passes through value `P` together with send result, see [[optionalFlow]]. */
  def flow[P]: Flow[(OutMessage, P), (SendResult, P), NotUsed] =
    Flow[(OutMessage, P)]
      .map { case (m, p) => (Some(m), p) }
      .via(optionalFlow[P])
      .map {
        case (Some(r), p) => (r, p)
        case (None, _) => sys.error("Send result expected for sent message")
      }

  def send(message: OutMessage)(implicit mat: Materializer): Future[SendResult] =
    Source.single((message, NotUsed)).via(flow[NotUsed]).map(_._1).runWith(Sink.head)

  /** Sends messages, materialized future completes with sent message count. */
  def sink: Sink[OutMessage, Future[Long]] =
    Flow[OutMessage]
      .map((_, NotUsed))
      .via(flow[NotUsed])
      .toMat(Sink.fold(0L)((count, _) => count + 1))(Keep.right)

  /** Sends messages and commits incoming message committables after outgoing messages are sent,
    * in incoming message order. Message `null` is not sent, its committable is committed (incoming message
    * is filtered out).
    * Materialized future completes with sent message count when all committables are committed.
    */
  def committableSink(committer: MsgCommitter): Sink[CommittableMessage[OutMessage], Future[Long]] =
    Flow[CommittableMessage[OutMessage]]
      .map(m => (Option(m.message), m.committable))
      .via(optionalFlow[Committable])
      .alsoToMat(Sink.fold(0L)((count, r: (Option[SendResult], Committable)) =>
        if (r._1.isDefined) count + 1 else count))(Keep.right)
      .map(_._2)
      .toMat(committer.sink)(Keep.both)
      .mapMaterializedValue { case (count, done) =>
        done.flatMap(_ => count)(MsgProvider.syncEc)
      }
}

/** Specifies messages to be read.
  * Group subscriptions ([[Topics]], [[TopicPattern]]) require group,
  * for manual partition assignments group is required only if messages are committed.
  */
sealed trait Subscription {
  def group: Option[String]
}
object Subscription {
  /** Group subscription to topics, partitions are assigned by provider among group members. */
  case class Topics(topics: Set[String], group: Option[String]) extends Subscription
  /** Group subscription to topics matching regular expression. */
  case class TopicPattern(pattern: String, group: Option[String]) extends Subscription
  /** Manual assignment, reading starts from committed offset, or as configured if there is no committed offset. */
  case class Assignment(partitions: Set[TopicPartition], group: Option[String]) extends Subscription
  /** Manual assignment, reading starts from specified offsets. */
  case class AssignmentWithOffset(offsets: Map[TopicPartition, Long], group: Option[String]) extends Subscription
  /** Manual assignment, reading starts from the first offsets with timestamps (epoch millis)
    * greater than or equal to specified ones.
    */
  case class AssignmentOffsetsForTimes(timestamps: Map[TopicPartition, Long], group: Option[String])
    extends Subscription
}

/** Partition metadata, inspired by `org.apache.kafka.common.PartitionInfo`.
  * @param leader  leader node or `None` if there is no leader currently
  */
case class PartitionInfo(
  topic: String,
  partition: Int,
  leader: Option[Node],
  replicas: Seq[Node],
  inSyncReplicas: Seq[Node],
)
case class Node(id: Int, host: String, port: Int)

/** @param metadata  commit metadata or `null` if not specified */
case class OffsetAndMetadata(offset: Long, metadata: String)

trait MsgAdmin {
  def topics: Future[Map[String, List[PartitionInfo]]]
  def partitionsFor(topic: String): Future[List[PartitionInfo]]
  def beginningOffsets(partitions: Set[TopicPartition]): Future[Map[TopicPartition, Long]]
  def endOffsets(partitions: Set[TopicPartition]): Future[Map[TopicPartition, Long]]
  /** Partitions without committed offset are not included in the result. */
  def committedOffsets(group: String, partitions: Set[TopicPartition]): Future[Map[TopicPartition, OffsetAndMetadata]]
}
