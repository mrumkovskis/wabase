package org.wabase.msg

import org.apache.pekko.Done

import scala.collection.immutable.ListMap
import scala.concurrent.Future

/** Message passed through messaging streams. */
sealed trait Msg

/** Message received from a messaging provider.
  *
  * `key` and `value` are decoded by provider with key and value [[MsgDecoder]] passed to [[MsgProvider.createReader]],
  * header values are `String` if valid UTF-8, otherwise `Array[Byte]` (`null` if absent),
  * see [[MsgConversions.headerValue]].
  *
  * @param timestamp  message timestamp, epoch millis
  */
case class InMessage(
  topic: String,
  key: Any,
  value: Any,
  partition: Int,
  offset: Long,
  headers: Seq[Header],
  timestamp: Long,
) extends Msg {
  /** Message metadata - topic, partition, offset, timestamp, key, headers (list of maps with name, value) */
  def metadata: Map[String, Any] = ListMap(
    "topic"     -> topic,
    "partition" -> partition,
    "offset"    -> offset,
    "timestamp" -> timestamp,
    "key"       -> key,
    "headers"   -> headers.map(h => Map("name" -> h.name, "value" -> h.value)),
  )
  /** [[metadata]] with value */
  def toMap: Map[String, Any] = metadata + ("value" -> value)
}

/** Message to be sent to a messaging provider.
  *
  * `key` and `value` are encoded by provider with key and value [[MsgEncoder]] passed to [[MsgProvider.createSender]],
  * header values must be `String` (encoded as UTF-8), `Array[Byte]` or `null`, see [[MsgConversions.encode]].
  *
  * @param partition  target partition, if not specified, provider selects partition (for example, by key hash)
  * @param timestamp  message timestamp, epoch millis, if not specified, provider assigns it
  */
case class OutMessage(
  topic: String,
  key: Any,
  value: Any,
  partition: Option[Int] = None,
  timestamp: Option[Long] = None,
  headers: Seq[Header] = Nil,
) extends Msg

/** Message together with committable of incoming message.
  * For [[MsgReader]] sources message is [[InMessage]], for [[MsgSender.committableSink]] it is [[OutMessage]]
  * (result of incoming message processing), committable is committed after outgoing message is sent.
  * In between message can be any value - result of incoming message processing (for example, foreach body result
  * of msg read operation), committable travels together with it.
  */
case class CommittableMessage[+M](message: M, committable: Committable) extends Msg

case class Header(name: String, value: Any)

/** Result of sending [[OutMessage]].
  * @param timestamp  message timestamp, epoch millis, as assigned by provider
  */
case class SendResult(
  topic: String,
  partition: Int,
  offset: Long,
  timestamp: Long,
)

case class TopicPartition(topic: String, partition: Int)

/** Committable position of incoming message, provider specific.
  * Committable is passed to [[MsgCommitter.sink]] after message is processed.
  * Committables are batched by committer sink, according to committer settings.
  * Inspired by `org.apache.pekko.kafka.ConsumerMessage.Committable`.
  */
trait Committable

/** Committable offset of incoming message.
  * Inspired by `org.apache.pekko.kafka.ConsumerMessage.CommittableOffset` and `CommittableOffsetMetadata`.
  * @param metadata  commit metadata or `null` if not specified
  */
trait CommittableOffset extends Committable {
  def group: String
  def topicPartition: TopicPartition
  def offset: Long
  def metadata: String
}

/** Materialized value of [[MsgReader]] sources, controls underlying consumer. */
trait Control {
  /** Registers completion of the stream consuming the source, for example materialized value of
    * [[MsgCommitter.sink]] or [[MsgSender.committableSink]], so that [[stop]] can drain the stream.
    * Must be called by the code that runs the stream, before [[stop]] is called.
    */
  def drainOn(streamCompletion: Future[Any]): Unit
  /** Stops consumer.
    * If stream completion is registered with [[drainOn]], consumer stops emitting messages, waits for stream
    * completion, so that pending messages are processed and committed, then shuts down
    * (like pekko kafka `Consumer.Control.drainAndShutdown`).
    * Otherwise consumer is shut down immediately (provider may still wait for pending commits for a limited time,
    * for example kafka consumer `stop-timeout`, see [[MsgProvider.StopTimeoutPath]]).
    * Can be called more than once, returns the same future.
    */
  def stop(): Future[Done]
}
