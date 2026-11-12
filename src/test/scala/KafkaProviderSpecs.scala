package org.wabase

import com.typesafe.config.{Config, ConfigFactory}
import io.github.embeddedkafka.{EmbeddedK, EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers
import org.wabase.msg.Subscription._
import org.wabase.msg._
import org.wabase.msg.kafka.KafkaProvider

import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class KafkaProviderSpecs extends FlatSpec with Matchers with BeforeAndAfterAll {
  implicit val system: ActorSystem = ActorSystem("kafka-provider-specs")

  private var kafka: EmbeddedK = _
  private lazy val kafkaConfig: EmbeddedKafkaConfig = kafka.config
  private lazy val providerConfig: Config = ConfigFactory.parseString(
    s"""factory-class     = org.wabase.msg.kafka.KafkaProvider
       |bootstrap-servers = "localhost:${kafkaConfig.kafkaPort}"
       |consumer.kafka-clients.auto.offset.reset = earliest
       |consumer.stop-timeout = 1s  # pekko default 30s keeps finished consumer in group, delays shutdown
       |committer.max-batch = 1
       |""".stripMargin)
  private lazy val provider: MsgProvider =
    WabaseMsgProviders.createProvider("main", providerConfig, system)

  override def beforeAll(): Unit = {
    kafka = EmbeddedKafka.start()(EmbeddedKafkaConfig(
      kafkaPort = 0,
      zooKeeperPort = 0,
      customBrokerProperties = Map("group.initial.rebalance.delay.ms" -> "0"),
    ))
  }
  override def afterAll(): Unit = {
    try Await.result(provider.close(), 30.seconds)
    finally try kafka.stop(clearLogs = true) // only own instance, EmbeddedKafka.stop() stops all
    finally Await.result(system.terminate(), 30.seconds)
  }

  private def await[T](f: Future[T]): T = Await.result(f, 30.seconds)
  private val topicCounter = new java.util.concurrent.atomic.AtomicInteger
  private def newTopic(partitions: Int = 1): String = {
    val topic = s"topic-${topicCounter.incrementAndGet()}"
    EmbeddedKafka.createCustomTopic(topic, partitions = partitions)(kafkaConfig).get
    topic
  }
  private def bytes(s: String) = s.getBytes(UTF_8)
  private def str(b: Any) = new String(b.asInstanceOf[Array[Byte]], UTF_8)
  private def send(topic: String, values: String*): Long =
    await(Source(values.toList).map(v => OutMessage(topic, null, v))
      .runWith(provider.createSender(ConfigFactory.empty).sink))
  private def reader = provider.createReader(ConfigFactory.empty)
  private def committer = provider.createCommitter(ConfigFactory.empty)

  behavior of "KafkaProvider"

  it should "be created by factory function" in {
    provider shouldBe a [KafkaProvider]
    provider.asInstanceOf[KafkaProvider].name shouldBe "main"
  }

  it should "send and read messages with key, headers, partition and timestamp" in {
    val topic = newTopic(partitions = 2)
    val sender = provider.createSender(ConfigFactory.empty)
    val res = await(sender.send(OutMessage(topic, 42, Map("id" -> 1, "name" -> "n"), Some(1), Some(1000L),
      List(Header("h1", "s"), Header("h2", bytes("b")), Header("h3", null), Header("h4", Array[Byte](-1, 0, 1))))))
    res shouldBe SendResult(topic, 1, 0, 1000)
    val m = await(reader.source(Assignment(Set(TopicPartition(topic, 1)), None)).runWith(Sink.head))
    (m.topic, m.key, m.value, m.partition, m.offset, m.timestamp) shouldBe
      ((topic, "42", Map("id" -> 1, "name" -> "n"), 1, 0, 1000)) // key - string codec, value - json codec
    // valid UTF-8 header values as strings, binary as bytes
    m.headers.take(3) shouldBe List(Header("h1", "s"), Header("h2", "b"), Header("h3", null))
    m.headers(3).value.asInstanceOf[Array[Byte]].toList shouldBe List[Byte](-1, 0, 1)
    // raw encoded data
    val raw = await(provider.createReader(ConfigFactory.empty, BytesMsgCodec, BytesMsgCodec)
      .source(Assignment(Set(TopicPartition(topic, 1)), None)).runWith(Sink.head))
    (str(raw.key), str(raw.value)) shouldBe (("42", """{"id":1,"name":"n"}"""))
  }

  it should "use custom codecs" in {
    val topic = newTopic()
    val codec = new MsgCodec {
      def encode(topic: String, headers: Seq[Header], data: Any): Array[Byte] =
        bytes(s"${headers.map(_.value).mkString}:$data")
      def decode(topic: String, headers: Seq[Header], data: Array[Byte]): Any =
        s"${headers.map(_.value).mkString}/${str(data)}"
    }
    await(provider.createSender(ConfigFactory.empty)
      .send(OutMessage(topic, List(1, 2), "v", headers = List(Header("h", "x")),
        keyEncoder = CborMsgCodec, valueEncoder = codec)))
    val m = await(provider.createReader(ConfigFactory.empty, CborMsgCodec, codec)
      .source(Assignment(Set(TopicPartition(topic, 0)), None)).runWith(Sink.head))
    (m.key, m.value) shouldBe ((List(1, 2), "x/x:v"))
  }

  it should "resolve codecs from operation and provider config" in {
    val topic = newTopic()
    val p = WabaseMsgProviders.createProvider("codecs",
      ConfigFactory.parseString("key-codec = json\nvalue-codec = string").withFallback(providerConfig), system)
    try {
      def readRaw() = await(provider.createReader(ConfigFactory.empty, BytesMsgCodec, BytesMsgCodec)
        .source(Assignment(Set(TopicPartition(topic, 0)), None)).take(3).map(m => (str(m.key), str(m.value)))
        .runWith(Sink.seq))
      // provider config: key json, value string
      await(p.createSender(ConfigFactory.empty).send(OutMessage(topic, "k", "v")))
      // operation config overrides provider config
      await(p.createSender(ConfigFactory.parseString("value-codec = json")).send(OutMessage(topic, "k", "v")))
      // message encoder overrides config
      await(p.createSender(ConfigFactory.parseString("value-codec = json"))
        .send(OutMessage(topic, "k", List(1), valueEncoder = CborMsgCodec)))
      val raw = readRaw()
      raw.take(2) shouldBe List(("\"k\"", "v"), ("\"k\"", "\"v\""))
      raw(2)._1 shouldBe "\"k\""
      // reader resolves codecs the same way
      await(p.createReader(ConfigFactory.parseString("value-codec = json"), null, null)
        .source(AssignmentWithOffset(Map(TopicPartition(topic, 0) -> 1L), None)).map(m => (m.key, m.value))
        .runWith(Sink.head)) shouldBe (("k", "v"))
      await(p.createReader(ConfigFactory.empty, null, CborMsgCodec)
        .source(AssignmentWithOffset(Map(TopicPartition(topic, 0) -> 2L), None)).map(m => (m.key, m.value))
        .runWith(Sink.head)) shouldBe (("k", List(1)))
      await(p.createReader(ConfigFactory.empty)
        .source(Assignment(Set(TopicPartition(topic, 0)), None)).map(_.value).runWith(Sink.head)) shouldBe "v"
    } finally await(p.close())
  }

  it should "fail stream with message position on decoding and encoding failure" in {
    val topic = newTopic()
    await(provider.createSender(ConfigFactory.empty)
      .send(OutMessage(topic, null, "not json", valueEncoder = StringMsgCodec)))
    val ex = intercept[MsgDecodingException](await(reader.source(Assignment(Set(TopicPartition(topic, 0)), None))
      .runWith(Sink.head)))
    (ex.topic, ex.partition, ex.offset) shouldBe ((topic, 0, 0L))
    ex.getMessage should startWith (s"Failed to decode message value at $topic-0@0: ")
    intercept[MsgEncodingException](await(provider.createSender(ConfigFactory.empty)
      .send(OutMessage(topic, null, "not bytes", valueEncoder = BytesMsgCodec))))
      .getMessage should startWith (s"Failed to encode message value for topic $topic: ")
  }

  it should "skip message which cannot be decoded if configured" in {
    val topic = newTopic()
    val sender = provider.createSender(ConfigFactory.empty)
    await(sender.send(OutMessage(topic, null, "not json", valueEncoder = StringMsgCodec)))
    await(sender.send(OutMessage(topic, null, "v")))
    val skipping = provider.createReader(ConfigFactory.parseString("decode-error = skip"))
    val sub = Assignment(Set(TopicPartition(topic, 0)), Some("g-skip-decode"))
    await(skipping.source(sub).map(_.value).runWith(Sink.head)) shouldBe "v"
    // committable sources emit skipped message as null, so that it is committed
    await(skipping.committableSource(sub).take(2).map(m => Option(m.message).map(_.value)).runWith(Sink.seq)) shouldBe
      List(None, Some("v"))
    await(skipping.committableSourceWithMetadata(sub, _ => "meta").take(2)
      .map(m => Option(m.message).map(_.value)).runWith(Sink.seq)) shouldBe List(None, Some("v"))
    // skipped message is committed with metadata of the previous message
    val topic2 = newTopic()
    await(sender.send(OutMessage(topic2, null, "v")))
    await(sender.send(OutMessage(topic2, null, "not json", valueEncoder = StringMsgCodec)))
    val sub2 = Assignment(Set(TopicPartition(topic2, 0)), Some("g-skip-metadata"))
    await(skipping.committableSourceWithMetadata(sub2, _ => "meta").take(2).map(_.committable)
      .runWith(provider.createCommitter(ConfigFactory.empty).sink))
    await(provider.admin.committedOffsets("g-skip-metadata", Set(TopicPartition(topic2, 0)))) shouldBe
      Map(TopicPartition(topic2, 0) -> OffsetAndMetadata(2, "meta"))
    intercept[RuntimeException](provider.createReader(ConfigFactory.parseString("decode-error = ignore")))
      .getMessage should include ("decode-error")
  }

  it should "read from offset and from timestamp" in {
    val topic = newTopic()
    send(topic, "a", "b", "c") shouldBe 3
    await(reader.source(AssignmentWithOffset(Map(TopicPartition(topic, 0) -> 1L), None))
      .take(2).map(_.value).runWith(Sink.seq)) shouldBe List("b", "c")
    await(reader.source(AssignmentOffsetsForTimes(Map(TopicPartition(topic, 0) -> 0L), None))
      .take(3).map(_.value).runWith(Sink.seq)) shouldBe List("a", "b", "c")
  }

  it should "require group for group subscriptions and committing" in {
    val topic = newTopic()
    intercept[RuntimeException](reader.source(Topics(Set(topic), None)))
      .getMessage should include ("group is required")
    intercept[RuntimeException](reader.committableSource(Assignment(Set(TopicPartition(topic, 0)), None)))
      .getMessage should include ("group is required for subscription")
    // group from provider config
    provider.createReader(ConfigFactory.parseString("group-id = g-default")).source(Topics(Set(topic), None))
  }

  it should "commit messages and continue from committed offset" in {
    val topic = newTopic()
    send(topic, "a", "b", "c", "d")
    val sub = Topics(Set(topic), Some("g-commit"))
    await(reader.committableSource(sub).take(2).map(_.committable).runWith(committer.sink))
    await(provider.admin.committedOffsets("g-commit", Set(TopicPartition(topic, 0)))) shouldBe
      Map(TopicPartition(topic, 0) -> OffsetAndMetadata(2, null))
    await(reader.committableSource(sub).take(2).map { m =>
      m.committable.asInstanceOf[CommittableOffset].offset -> m.message.value
    }.runWith(Sink.seq)) shouldBe List(2L -> "c", 3L -> "d")
  }

  it should "commit metadata" in {
    val topic = newTopic()
    send(topic, "a", "b")
    val sub = Assignment(Set(TopicPartition(topic, 0)), Some("g-metadata"))
    await(reader.committableSourceWithMetadata(sub, m => s"meta-${m.value}")
      .take(2).map(_.committable).runWith(committer.sink))
    await(provider.admin.committedOffsets("g-metadata", Set(TopicPartition(topic, 0)))) shouldBe
      Map(TopicPartition(topic, 0) -> OffsetAndMetadata(2, "meta-b"))
  }

  it should "commit at most once" in {
    val topic = newTopic()
    send(topic, "a", "b", "c")
    await(reader.atMostOnceSource(Topics(Set(topic), Some("g-at-most-once"))).take(2).runWith(Sink.ignore))
    // next message may be committed (and lost) before take cancels upstream
    await(provider.admin.committedOffsets("g-at-most-once", Set(TopicPartition(topic, 0))))
      .apply(TopicPartition(topic, 0)).offset should (be >= 2L and be <= 3L)
  }

  it should "send transformed messages and commit incoming ones" in {
    val in  = newTopic()
    val out = newTopic()
    send(in, "a", "b")
    await(reader.committableSource(Topics(Set(in), Some("g-transform"))).take(2)
      .map(m => CommittableMessage(OutMessage(out, null, m.message.value.toString.toUpperCase), m.committable))
      .runWith(provider.createSender(ConfigFactory.empty).committableSink(committer))) shouldBe 2
    await(provider.admin.committedOffsets("g-transform", Set(TopicPartition(in, 0)))) shouldBe
      Map(TopicPartition(in, 0) -> OffsetAndMetadata(2, null))
    await(reader.source(Assignment(Set(TopicPartition(out, 0)), None)).take(2).map(_.value)
      .runWith(Sink.seq)) shouldBe List("A", "B")
  }

  it should "drain and commit on stop" in {
    val topic = newTopic()
    send(topic, "a", "b", "c")
    val received = new AtomicInteger
    val (control, done) = reader.committableSource(Topics(Set(topic), Some("g-drain")))
      .map { m => received.incrementAndGet(); m.committable }
      .toMat(committer.sink)(Keep.both).run()
    control.drainOn(done)
    while (received.get < 3) Thread.sleep(50)
    await(control.stop())
    await(done)
    await(provider.admin.committedOffsets("g-drain", Set(TopicPartition(topic, 0)))) shouldBe
      Map(TopicPartition(topic, 0) -> OffsetAndMetadata(3, null))
  }

  it should "release group partitions without stop-timeout delay" in {
    // pekko default consumer stop-timeout (30s) would delay next read of the same group
    val p = WabaseMsgProviders.createProvider("no-stop-timeout",
      providerConfig.withoutPath("consumer.stop-timeout"), system)
    try {
      val topic = newTopic()
      send(topic, "a", "b", "c")
      def timed[T](f: => T): (T, Long) = {
        val start = System.currentTimeMillis
        (f, System.currentTimeMillis - start)
      }
      // plain source - stop-timeout 0 by default
      val plain = p.createReader(ConfigFactory.empty)
      val (plainValues, plainMillis) = timed((1 to 2).map { _ =>
        await(plain.source(Topics(Set(topic), Some("g-st-plain"))).take(1).runWith(Sink.head)).value
      })
      plainValues shouldBe List("a", "a")
      plainMillis should be < 15000L
      // committable source stopped by drain - stop-timeout 0 from operation config
      val committable = p.createReader(ConfigFactory.parseString(s"${MsgProvider.StopTimeoutPath} = 0s"))
      val committer = p.createCommitter(ConfigFactory.empty)
      def readOne() = {
        val received = new AtomicInteger
        val (control, done) = committable.committableSource(Topics(Set(topic), Some("g-st-committable")))
          .map { m => received.incrementAndGet(); m.committable }
          .toMat(committer.sink)(Keep.both).run()
        control.drainOn(done)
        val deadline = System.currentTimeMillis + 20000
        while (received.get < 1 && System.currentTimeMillis < deadline) Thread.sleep(10)
        await(control.stop())
        await(done)
        received.get should be > 0
      }
      // drain processes and commits all fetched messages, next read needs a new message
      val (_, committableMillis) = timed { readOne(); send(topic, "d"); readOne() }
      committableMillis should be < 15000L
      await(p.admin.committedOffsets("g-st-committable", Set(TopicPartition(topic, 0)))) shouldBe
        Map(TopicPartition(topic, 0) -> OffsetAndMetadata(4, null))
    } finally await(p.close())
  }

  it should "stop running consumers on close" in {
    val topic = newTopic()
    val p = WabaseMsgProviders.createProvider("closing", providerConfig, system)
    val done = p.createReader(ConfigFactory.empty).source(Topics(Set(topic), Some("g-close"))).runWith(Sink.ignore)
    Thread.sleep(500)
    done.isCompleted shouldBe false
    await(p.close())
    await(done)
  }

  it should "provide topic metadata and offsets" in {
    val topic = newTopic(partitions = 2)
    send(topic, "a", "b", "c")
    val infos = await(provider.admin.partitionsFor(topic))
    infos.map(_.partition) shouldBe List(0, 1)
    infos.foreach { p =>
      p.leader.map(_.port) shouldBe Some(kafkaConfig.kafkaPort)
      p.replicas.size shouldBe 1
    }
    await(provider.admin.topics)(topic) shouldBe infos
    val tps = Set(TopicPartition(topic, 0), TopicPartition(topic, 1))
    await(provider.admin.beginningOffsets(tps)) shouldBe tps.map(_ -> 0L).toMap
    await(provider.admin.endOffsets(tps)).values.sum shouldBe 3
    await(provider.admin.committedOffsets("g-none", tps)) shouldBe Map.empty
    await(provider.admin.partitionsFor("missing-topic")) shouldBe Nil
  }
}
