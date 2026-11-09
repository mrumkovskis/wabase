package org.wabase.msg.kafka

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.admin.{Admin, ListConsumerGroupOffsetsSpec, OffsetSpec, TopicDescription}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{Producer, ProducerRecord}
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.apache.kafka.common.{KafkaFuture, TopicPartition => KafkaTopicPartition}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.kafka.scaladsl.{Committer, Consumer}
import org.apache.pekko.kafka.{CommitterSettings, ConsumerMessage, ConsumerSettings, ProducerMessage, ProducerSettings,
  Subscriptions, Subscription => KafkaSubscription}
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.apache.pekko.{Done, NotUsed}
import org.wabase.Loggable
import org.wabase.msg._

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.{Duration, DurationLong}
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._
import scala.util.control.NonFatal

/** Kafka messaging provider based on pekko-connectors-kafka (optional dependency of wabase).
  *
  * Provider configuration (operation specific configuration passed to `create*` methods takes precedence):
  * {{{
  * msg-provider {
  *   main {
  *     factory-class     = org.wabase.msg.kafka.KafkaProvider
  *     bootstrap-servers = "localhost:9092"  # optional, overrides bootstrap.servers of kafka-clients
  *     group-id          = my-app            # optional, default group if subscription does not specify it
  *     codecs { }                            # optional, named codecs, see MsgCodecs
  *     key-codec         = string            # optional, default key codec name or object, see MsgCodecs
  *     value-codec       = json              # optional, default value codec name or object, see MsgCodecs
  *     stop-timeout      = 30s               # optional, overrides consumer.stop-timeout, see MsgProvider.StopTimeoutPath
  *     consumer  { }                         # falls back to pekko.kafka.consumer
  *     producer  { }                         # falls back to pekko.kafka.producer
  *     committer { }                         # falls back to pekko.kafka.committer
  *     admin.kafka-clients { }               # kafka admin client properties
  *   }
  * }
  * }}}
  * Keys and values are decoded by provider with decoders passed to [[createReader]] or, if they are `null`, with
  * codecs named in configuration, encoded with encoders of message ([[OutMessage.keyEncoder]],
  * [[OutMessage.valueEncoder]]) or, if they are `null`, with codecs named in configuration (in stream stages, not as
  * kafka serializers / deserializers, so that decoding failure is a stream failure with message position, see
  * [[MsgDecodingException]]). Auto commit of consumer is disabled.
  * Consumer `stop-timeout` (wait for pending commits after source is cancelled) is `0` for plain and at most once
  * sources (no commits after message is emitted) unless `stop-timeout` is configured, committable sources use
  * configured value (pekko default 30s). Msg operations set it to `0` - their committable streams are not cancelled,
  * consumer is stopped only after stream completes and all commits are done (see `MsgStreams.run`).
  * Producer and admin client are shared and created on first use, they are closed by [[close]].
  */
class KafkaProvider(val name: String, val config: Config)(implicit system: ActorSystem) extends MsgProvider
    with Loggable {
  import KafkaProvider._

  private implicit val ec: ExecutionContext = system.dispatcher

  private def effectiveConfig(opConfig: Config) =
    if (opConfig == null || opConfig.isEmpty) config else opConfig.withFallback(config)

  private def pekkoConfig(cfg: Config, path: String) = {
    val default = system.settings.config.getConfig(s"pekko.kafka.$path")
    if (cfg.hasPath(path)) cfg.getConfig(path).withFallback(default) else default
  }

  private def bootstrapServers(cfg: Config) =
    if (cfg.hasPath("bootstrap-servers")) Option(cfg.getString("bootstrap-servers")) else None

  private def defaultGroup(cfg: Config) =
    if (cfg.hasPath("group-id")) Option(cfg.getString("group-id")) else None

  /** @param committable  messages are committed after they are emitted (committable sources),
    *                     consumer must wait for pending commits (`stop-timeout`) unless it is stopped by drain
    */
  private def consumerSettings(cfg: Config, committable: Boolean): ConsumerSettings[Array[Byte], Array[Byte]] = {
    val s = ConsumerSettings(pekkoConfig(cfg, "consumer"), new ByteArrayDeserializer, new ByteArrayDeserializer)
      .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    val st =
      if (cfg.hasPath(MsgProvider.StopTimeoutPath))
        s.withStopTimeout(cfg.getDuration(MsgProvider.StopTimeoutPath).toNanos.nanos)
      else if (!committable) s.withStopTimeout(Duration.Zero) // nothing to wait for
      else s
    bootstrapServers(cfg).map(st.withBootstrapServers).getOrElse(st)
  }

  private def producerSettings(cfg: Config): ProducerSettings[Array[Byte], Array[Byte]] = {
    val s = ProducerSettings(pekkoConfig(cfg, "producer"), new ByteArraySerializer, new ByteArraySerializer)
    bootstrapServers(cfg).map(s.withBootstrapServers).getOrElse(s)
  }

  val codecs: MsgCodecs = new MsgCodecs(config)

  /* shared resources */
  private val controls = ConcurrentHashMap.newKeySet[KafkaControl]()
  @volatile private var producerCreated = false
  private lazy val sharedProducer: Producer[Array[Byte], Array[Byte]] = {
    producerCreated = true
    producerSettings(config).createKafkaProducer()
  }
  @volatile private var adminCreated = false
  private lazy val adminClient: Admin = {
    adminCreated = true
    val props = new java.util.Properties
    if (config.hasPath("admin.kafka-clients"))
      config.getConfig("admin.kafka-clients").entrySet.asScala
        .foreach(e => props.put(e.getKey, e.getValue.unwrapped.toString))
    bootstrapServers(config).foreach(props.put("bootstrap.servers", _))
    Admin.create(props)
  }

  def createReader(opConfig: Config, keyDecoder: MsgDecoder, valueDecoder: MsgDecoder): MsgReader = new MsgReader {
    private val cfg = effectiveConfig(opConfig)
    private val kd  = codecs.keyDecoder(keyDecoder, cfg)
    private val vd  = codecs.valueDecoder(valueDecoder, cfg)
    private def toInMessage(r: ConsumerRecord[Array[Byte], Array[Byte]]): InMessage =
      KafkaProvider.toInMessage(r, kd, vd)
    private def toCommittableMessage(m: ConsumerMessage.CommittableMessage[Array[Byte], Array[Byte]]) =
      CommittableMessage(toInMessage(m.record), KafkaCommittableOffset(m.committableOffset))
    private def settings(subscription: Subscription, groupRequired: Boolean, committable: Boolean) = {
      val group = subscription.group orElse defaultGroup(cfg)
      val needsGroup = groupRequired || (subscription match {
        case _: Subscription.Topics | _: Subscription.TopicPattern => true
        case _ => false
      })
      if (needsGroup && group.isEmpty) sys.error(
        s"Kafka provider '$name': group is required for subscription $subscription" +
          (if (groupRequired) " when messages are committed" else ""))
      val s = consumerSettings(cfg, committable)
      group.map(s.withGroupId).getOrElse(s)
    }
    def source(subscription: Subscription): Source[InMessage, Control] =
      registered(Consumer.plainSource(settings(subscription, false, false), kafkaSubscription(subscription)))
        .map(toInMessage)
    def atMostOnceSource(subscription: Subscription): Source[InMessage, Control] =
      registered(Consumer.atMostOnceSource(settings(subscription, true, false), kafkaSubscription(subscription)))
        .map(toInMessage)
    def committableSource(subscription: Subscription): Source[CommittableMessage[InMessage], Control] =
      registered(Consumer.committableSource(settings(subscription, true, true), kafkaSubscription(subscription)))
        .map(toCommittableMessage)
    def committableSourceWithMetadata(
      subscription: Subscription,
      metadata: InMessage => String,
    ): Source[CommittableMessage[InMessage], Control] = {
      // metadata function receives decoded message, it is reused by the following stage to avoid decoding twice
      // (metadata function is called right before the message is emitted, decoding is repeated if record differs)
      val lastDecoded = new AtomicReference[(ConsumerRecord[Array[Byte], Array[Byte]], InMessage)]()
      registered(Consumer.commitWithMetadataSource(
        settings(subscription, true, true), kafkaSubscription(subscription), (r: ConsumerRecord[Array[Byte], Array[Byte]]) => {
          val m = toInMessage(r)
          lastDecoded.set((r, m))
          metadata(m)
        }
      )).map { m =>
        val decoded = lastDecoded.get match {
          case (r, msg) if r eq m.record => msg
          case _ => toInMessage(m.record)
        }
        CommittableMessage(decoded, KafkaCommittableOffset(m.committableOffset))
      }
    }
  }

  /** Wraps consumer control, tracks it until consumer is shut down, so that [[close]] can stop it. */
  private def registered[T](source: Source[T, Consumer.Control]): Source[T, Control] =
    source.mapMaterializedValue { c =>
      val control = new KafkaControl(c)
      controls.add(control)
      c.isShutdown.onComplete(_ => controls.remove(control))
      control: Control
    }

  def createSender(opConfig: Config): MsgSender = new MsgSender {
    private val cfg = effectiveConfig(opConfig)
    private val ke  = codecs.keyEncoder(cfg)
    private val ve  = codecs.valueEncoder(cfg)
    private val settings =
      if (opConfig == null || !opConfig.hasPath("producer") && !opConfig.hasPath("bootstrap-servers"))
        producerSettings(config).withProducer(sharedProducer)
      else producerSettings(cfg) // producer is created and closed by each stream
    def optionalFlow[P]: Flow[(Option[OutMessage], P), (Option[SendResult], P), NotUsed] =
      Flow[(Option[OutMessage], P)]
        .map {
          case (Some(m), p) => ProducerMessage.single(toProducerRecord(m, ke, ve), p)
          case (None, p) => ProducerMessage.passThrough[Array[Byte], Array[Byte], P](p)
        }
        .via(org.apache.pekko.kafka.scaladsl.Producer.flexiFlow(settings))
        .map {
          case ProducerMessage.Result(md, msg) =>
            (Some(SendResult(md.topic, md.partition, md.offset, md.timestamp)), msg.passThrough)
          case ProducerMessage.PassThroughResult(p) => (None, p)
          case x => sys.error(s"Unexpected kafka producer result: $x") // multi messages are not sent
        }
  }

  def createCommitter(opConfig: Config): MsgCommitter = new MsgCommitter {
    private val settings = CommitterSettings(pekkoConfig(effectiveConfig(opConfig), "committer"))
    def flow: Flow[Committable, Done, NotUsed] =
      Flow[Committable].map {
        case k: KafkaCommittableOffset => k.underlying: ConsumerMessage.Committable
        case x => sys.error(s"Kafka provider '$name' cannot commit $x")
      }.via(Committer.flow(settings))
  }

  lazy val admin: MsgAdmin = new MsgAdmin {
    private def describe(topics: Iterable[String]): Future[Map[String, List[PartitionInfo]]] =
      toScala(adminClient.describeTopics(topics.asJavaCollection).allTopicNames)
        .map(_.asScala.map { case (t, d) => t -> toPartitionInfos(d) }.toMap)
    def topics: Future[Map[String, List[PartitionInfo]]] =
      toScala(adminClient.listTopics.names).flatMap(names => describe(names.asScala))
    def partitionsFor(topic: String): Future[List[PartitionInfo]] =
      describe(List(topic)).map(_.getOrElse(topic, Nil)).recover {
        case e if unknownTopic(e) => Nil
      }
    private def unknownTopic(e: Throwable): Boolean = e match {
      case _: org.apache.kafka.common.errors.UnknownTopicOrPartitionException => true
      case _: java.util.concurrent.CompletionException | _: java.util.concurrent.ExecutionException =>
        e.getCause != null && unknownTopic(e.getCause)
      case _ => false
    }
    private def offsets(partitions: Set[TopicPartition], spec: OffsetSpec) =
      if (partitions.isEmpty) Future.successful(Map.empty[TopicPartition, Long])
      else toScala(adminClient.listOffsets(partitions.map(p => toKafka(p) -> spec).toMap.asJava).all)
        .map(_.asScala.map { case (tp, info) => fromKafka(tp) -> info.offset }.toMap)
    def beginningOffsets(partitions: Set[TopicPartition]): Future[Map[TopicPartition, Long]] =
      offsets(partitions, OffsetSpec.earliest)
    def endOffsets(partitions: Set[TopicPartition]): Future[Map[TopicPartition, Long]] =
      offsets(partitions, OffsetSpec.latest)
    def committedOffsets(
      group: String,
      partitions: Set[TopicPartition],
    ): Future[Map[TopicPartition, OffsetAndMetadata]] = {
      val spec = new ListConsumerGroupOffsetsSpec().topicPartitions(partitions.map(toKafka).asJavaCollection)
      toScala(adminClient.listConsumerGroupOffsets(Map(group -> spec).asJava).partitionsToOffsetAndMetadata(group))
        .map(_.asScala.collect { case (tp, om) if om != null =>
          fromKafka(tp) -> OffsetAndMetadata(om.offset, Option(om.metadata).filter(_.nonEmpty).orNull)
        }.toMap)
    }
  }

  def close(): Future[Done] = {
    val running = controls.asScala.toList
    logger.debug(s"Closing kafka provider '$name', stopping ${running.size} consumer(s)")
    Future.traverse(running)(c => c.stop().recover { case NonFatal(e) =>
      logger.warn(s"Kafka provider '$name': failed to stop consumer", e)
      Done
    }).flatMap { _ =>
      Future(blocking {
        if (producerCreated) sharedProducer.close()
        if (adminCreated) adminClient.close()
        Done
      })
    }
  }
}

object KafkaProvider {
  def create(name: String, config: Config, system: ActorSystem): MsgProvider =
    new KafkaProvider(name, Option(config).getOrElse(ConfigFactory.empty))(system)

  private class KafkaControl(control: Consumer.Control)(implicit ec: ExecutionContext) extends Control {
    private val streamCompletion = new AtomicReference[Future[Any]]()
    def drainOn(streamCompletion: Future[Any]): Unit = this.streamCompletion.set(streamCompletion)
    private lazy val stopped: Future[Done] = streamCompletion.get match {
      case null => control.shutdown()
      case completion => control.drainAndShutdown(completion).map(_ => Done)
    }
    def stop(): Future[Done] = stopped
  }

  case class KafkaCommittableOffset(underlying: ConsumerMessage.CommittableOffset) extends CommittableOffset {
    def group: String = underlying.partitionOffset.key.groupId
    def topicPartition: TopicPartition =
      TopicPartition(underlying.partitionOffset.key.topic, underlying.partitionOffset.key.partition)
    def offset: Long = underlying.partitionOffset.offset
    /** Metadata or `null` if not specified (kafka uses empty string for no metadata) */
    def metadata: String =
      underlying match {
        case metadata1: ConsumerMessage.CommittableOffsetMetadata => Option(metadata1.metadata).filter(_.nonEmpty).orNull
        case _ => null
      }
  }

  private def toScala[T](f: KafkaFuture[T]): Future[T] = f.toCompletionStage.asScala

  def toKafka(tp: TopicPartition): KafkaTopicPartition = new KafkaTopicPartition(tp.topic, tp.partition)
  def fromKafka(tp: KafkaTopicPartition): TopicPartition = TopicPartition(tp.topic, tp.partition)

  def kafkaSubscription(subscription: Subscription): KafkaSubscription = subscription match {
    case Subscription.Topics(topics, _)       => Subscriptions.topics(topics)
    case Subscription.TopicPattern(p, _)      => Subscriptions.topicPattern(p)
    case Subscription.Assignment(parts, _)    => Subscriptions.assignment(parts.map(toKafka))
    case Subscription.AssignmentWithOffset(offsets, _) =>
      Subscriptions.assignmentWithOffset(offsets.map { case (tp, o) => toKafka(tp) -> o })
    case Subscription.AssignmentOffsetsForTimes(ts, _) =>
      Subscriptions.assignmentOffsetsForTimes(ts.map { case (tp, t) => toKafka(tp) -> t })
  }

  def toInMessage(
    r: ConsumerRecord[Array[Byte], Array[Byte]],
    keyDecoder: MsgDecoder,
    valueDecoder: MsgDecoder,
  ): InMessage =
    MsgConversions.inMessage(r.topic, r.partition, r.offset, r.timestamp, r.key, r.value,
      r.headers.toArray.toList.map(h => h.key -> h.value), keyDecoder, valueDecoder)

  def toProducerRecord(
    m: OutMessage,
    keyEncoder: MsgEncoder,
    valueEncoder: MsgEncoder,
  ): ProducerRecord[Array[Byte], Array[Byte]] = {
    val encoded = MsgConversions.encode(m, keyEncoder, valueEncoder)
    new ProducerRecord[Array[Byte], Array[Byte]](
      m.topic,
      m.partition.map(Integer.valueOf).orNull,
      m.timestamp.map(java.lang.Long.valueOf).orNull,
      encoded.key,
      encoded.value,
      encoded.headers.map { case (n, v) => new RecordHeader(n, v): org.apache.kafka.common.header.Header }.asJava,
    )
  }

  private def toPartitionInfos(d: TopicDescription): List[PartitionInfo] = {
    def node(n: org.apache.kafka.common.Node) = Node(n.id, n.host, n.port)
    d.partitions.asScala.toList.map { p =>
      PartitionInfo(d.name, p.partition, Option(p.leader).filterNot(_.isEmpty).map(node),
        p.replicas.asScala.toList.map(node), p.isr.asScala.toList.map(node))
    }
  }
}
