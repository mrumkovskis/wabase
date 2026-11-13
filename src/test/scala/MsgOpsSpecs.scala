package org.wabase

import com.typesafe.config.{Config, ConfigFactory}
import io.github.embeddedkafka.{EmbeddedK, EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers
import org.tresql.convInt
import org.wabase.msg.MsgParams.MsgParamException
import org.wabase.msg._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

@annotation.nowarn("msg=Manifest")
class MsgOpsSpecs extends FlatSpec with Matchers with TestQuereaseInitializer {
  implicit val system: ActorSystem = ActorSystem("msg-ops-specs")

  private var kafka: EmbeddedK = _
  private lazy val kafkaConfig: EmbeddedKafkaConfig = kafka.config
  private lazy val providerConfig: Config = ConfigFactory.parseString(
    s"""factory-class     = org.wabase.msg.kafka.KafkaProvider
       |bootstrap-servers = "localhost:${kafkaConfig.kafkaPort}"
       |group-id          = msg-ops-specs
       |consumer.kafka-clients.auto.offset.reset = earliest
       |committer.max-batch = 1
       |""".stripMargin) // pekko default consumer stop-timeout (30s), msg ops must not be delayed by it
  private lazy val provider: MsgProvider = WabaseMsgProviders.createProvider("main", providerConfig, system)
  /** Provider without default group */
  private lazy val noGroupProvider: MsgProvider =
    WabaseMsgProviders.createProvider("nogroup", providerConfig.withoutPath("group-id"), system)
  /** Main provider with committer which fails once after [[failCommit]] is set, and reader which fails on
    * message with value 'boom' */
  private val failCommit = new java.util.concurrent.atomic.AtomicBoolean
  private lazy val flakyProvider: MsgProvider = new MsgProvider {
    def createReader(config: Config, keyDecoder: MsgDecoder, valueDecoder: MsgDecoder): MsgReader = new MsgReader {
      private val reader = provider.createReader(config, keyDecoder, valueDecoder)
      private def check(m: InMessage): InMessage =
        if (m != null && m.value == "boom") throw new RuntimeException("reader failed") else m
      def source(s: Subscription) = reader.source(s).map(check)
      def atMostOnceSource(s: Subscription) = reader.atMostOnceSource(s).map(check)
      def committableSource(s: Subscription) = reader.committableSource(s).map { m => check(m.message); m }
      def committableSourceWithMetadata(s: Subscription, metadata: InMessage => String) =
        reader.committableSourceWithMetadata(s, metadata).map { m => check(m.message); m }
    }
    def createSender(config: Config): MsgSender = provider.createSender(config)
    def createCommitter(config: Config): MsgCommitter = new MsgCommitter {
      def flow = org.apache.pekko.stream.scaladsl.Flow[Committable]
        .map(c => if (failCommit.compareAndSet(true, false)) throw new RuntimeException("commit failed") else c)
        .via(provider.createCommitter(config).flow)
    }
    def admin: MsgAdmin = provider.admin
    def codecs: MsgCodecs = provider.codecs
    def close(): Future[org.apache.pekko.Done] = Future.successful(org.apache.pekko.Done)
  }
  implicit var qr: QuereaseResources = _

  override def beforeAll(): Unit = {
    kafka = EmbeddedKafka.start()(EmbeddedKafkaConfig(
      kafkaPort = 0,
      zooKeeperPort = 0,
      customBrokerProperties = Map("group.initial.rebalance.delay.ms" -> "0"),
    ))
    querease = new TestQuerease("/msg-ops-specs-metadata.yaml")
    qio = new AppQuereaseIo[Dto](querease)
    super.beforeAll()
    val tresqlResources = tresqlThreadLocalResources.withConn(tresqlThreadLocalResources.conn)
    // db / transaction operations open new connection to test database
    val resourcesFactory = ResourcesFactory(
      res => (_, _) => {
        val conn = java.sql.DriverManager.getConnection(s"jdbc:hsqldb:mem:$dbNamePrefix")
        conn.setAutoCommit(false)
        res.withConn(conn)
      },
      (res, rollback, err) => {
        val conn = res.conn
        try if (rollback || err.nonEmpty) conn.rollback() else conn.commit() finally conn.close()
      },
    )(tresqlResources)
    qr = new QuereaseResources()(resourcesFactory,
      system.dispatcher, system, null, qio, WabaseFileStreamers(Map("main" -> null)), null,
      WabaseMsgProviders(Map("main" -> provider, "flaky" -> flakyProvider, "nogroup" -> noGroupProvider)), _ => PartialFunction.empty, logger)
  }
  override def afterAll(): Unit = {
    try super.afterAll()
    finally try Await.result(provider.close(), 30.seconds)
    finally try kafka.stop(clearLogs = true) // only own instance, EmbeddedKafka.stop() stops all
    finally Await.result(system.terminate(), 30.seconds)
  }

  private def await[T](f: Future[T]): T = Await.result(f, 30.seconds)
  private val topicCounter = new java.util.concurrent.atomic.AtomicInteger
  private def newTopic(partitions: Int = 1): String = {
    val topic = s"msg-ops-${topicCounter.incrementAndGet()}"
    EmbeddedKafka.createCustomTopic(topic, partitions = partitions)(kafkaConfig).get
    topic
  }
  private def send(topic: String, values: Any*): Unit =
    await(Source(values.toList).map {
      case (k, v) => OutMessage(topic, k, v)
      case v => OutMessage(topic, null, v)
    }.runWith(provider.createSender(ConfigFactory.empty).sink))

  private def action(view: String, data: Map[String, Any], actionName: String = "get"): QuereaseResult =
    await(querease.doAction(view, actionName, data, Map.empty).transform(identity, (e: Throwable) => e match {
      case e: QuereaseActionException if e.getCause != null => e.getCause
      case e => e
    })(system.dispatcher))
  private def read(view: String, data: Map[String, Any], actionName: String = "get"): Seq[Any] =
    action(view, data, actionName) match {
      case mr: MessagingResult => await(querease.runMessaging(mr, Sink.seq[Any]))
      case x => fail(s"MessagingResult expected, got: $x")
    }
  private def values(msgs: Seq[Any]) = msgs.map { case m: Map[String@unchecked, _] => m("value") }
  private def timed[T](f: => T): (T, Long) = {
    val start = System.currentTimeMillis
    (f, System.currentTimeMillis - start)
  }

  behavior of "msg read"

  it should "read messages until 'to' matches, inclusive" in {
    val topic = newTopic()
    send(topic, "a", "b", "c", "d")
    val msgs = read("msg_read_to", Map("topic" -> topic))
    values(msgs) shouldBe List("a", "b", "c")
    msgs.head shouldBe Map("topic" -> topic, "partition" -> 0, "offset" -> 0L, "timestamp" -> msgs.head
      .asInstanceOf[Map[String, Any]]("timestamp"), "key" -> null, "headers" -> Nil, "value" -> "a")
  }

  it should "bind map value fields and message key" in {
    val topic = newTopic()
    send(topic, "k1" -> Map("id" -> 1), "k2" -> Map("id" -> 2), "k3" -> Map("id" -> 3))
    val msgs = read("msg_read_map", Map("topic" -> topic))
    values(msgs) shouldBe List(Map("id" -> 1), Map("id" -> 2))
  }

  it should "read from offset and bind message metadata" in {
    val topic = newTopic()
    send(topic, "a", "b", "c", "d")
    values(read("msg_read_metadata", Map("topic" -> topic))) shouldBe List("b", "c")
  }

  it should "complete on idle timeout" in {
    val topic = newTopic()
    val (empty, millis) = timed(read("msg_read_idle", Map("topic" -> topic)))
    empty shouldBe Nil
    millis should be < 15000L
    send(topic, "a", "b")
    values(read("msg_read_idle", Map("topic" -> topic))) shouldBe List("a", "b")
  }

  it should "commit read messages with metadata and continue from committed offset without delay" in {
    val topic = newTopic()
    send(topic, "a", "b", "c")
    val data = Map("topic" -> topic, "group" -> "g-commit")
    val (first, millis) = timed {
      val first = read("msg_read_commit", data)
      send(topic, "b")
      (first, read("msg_read_commit", data))
    }
    values(first._1) shouldBe List("a", "b")
    values(first._2) shouldBe List("c", "b") // c was fetched but dropped after 'to' match, not committed
    millis should be < 15000L                  // consumer stop-timeout (30s) does not delay next read
    await(provider.admin.committedOffsets("g-commit", Set(TopicPartition(topic, 0)))) shouldBe
      Map(TopicPartition(topic, 0) -> OffsetAndMetadata(4, "m-b"))
  }

  it should "evaluate parameters with action's resources" in {
    val topic = newTopic()
    send(topic, "a", "b")
    org.tresql.Query("+msg_ops_dummy {id, value} [?, ?]", 1, topic)(qr.resourcesFactory.resources)
    values(read("msg_read_from_table", Map("id" -> 1))) shouldBe List("a", "b")
  }

  /** Inserts committed msg_ops_dummy rows if not inserted yet */
  private def insertRows(rows: (Int, String)*): Unit = {
    val conn = java.sql.DriverManager.getConnection(s"jdbc:hsqldb:mem:$dbNamePrefix")
    try {
      rows.foreach { case (id, value) =>
        val st = conn.prepareStatement(
          "merge into msg_ops_dummy t using (values(?, ?)) s(id, value) on t.id = s.id " +
          "when not matched then insert values (s.id, s.value)")
        st.setInt(1, id); st.setString(2, value); st.executeUpdate()
      }
      conn.commit()
    } finally conn.close()
  }

  it should "evaluate per message tresql with action's resources" in {
    insertRows(40 -> "stop", 41 -> "x41", 42 -> "x42")
    val topic = newTopic()
    send(topic, 41, 40, 42)
    values(read("msg_read_db_tresql", Map("topic" -> topic))).map(String.valueOf) shouldBe List("41", "40")
    val topic2 = newTopic()
    send(topic2, 41, 42)
    read("msg_read_db_tresql", Map("topic" -> topic2), "list") shouldBe List("x41", "x42")
  }

  it should "release action's resources when returned messaging result stream terminates" in {
    insertRows(40 -> "stop")
    val topic = newTopic()
    send(topic, 1, 40, 2)
    val opened = new java.util.concurrent.atomic.AtomicInteger
    val closed = new java.util.concurrent.atomic.AtomicInteger
    val factory = ResourcesFactory(
      res => (pool, extra) => { opened.incrementAndGet(); qr.resourcesFactory.initResources(res)(pool, extra) },
      (res, rollback, err) => { closed.incrementAndGet(); qr.resourcesFactory.closeResources(res, rollback, err) },
    )(qr.resourcesFactory.resources)
    val res = await(querease.QuereaseAction("msg_read_db_tresql", "get", Map("topic" -> topic), Map.empty)(
      factory, null, qio, qr.fileStreamers, null, qr.msgProviders, qr.parametersProvider, logger)
      .run(system.dispatcher, system))
    opened.get shouldBe 1
    closed.get shouldBe 0 // 'to' is evaluated on action's connection while stream runs
    val mr = res match {
      case mr: MessagingResult => mr
      case x => fail(s"MessagingResult expected, got: $x")
    }
    values(await(querease.runMessaging(mr, Sink.seq[Any]))).map(String.valueOf) shouldBe List("1", "40")
    val deadline = System.currentTimeMillis + 5000
    while (closed.get == 0 && System.currentTimeMillis < deadline) Thread.sleep(10)
    closed.get shouldBe 1
  }

  it should "collect messages for the next step" in {
    val topic = newTopic()
    send(topic, "a", "b", "c")
    action("msg_read_step", Map("topic" -> topic)) match {
      case TresqlResult(r) => r.unique[Int] shouldBe 3
      case x => fail(s"Unexpected result: $x")
    }
  }

  it should "report parameter and provider errors" in {
    val topic = "not-read" // errors are reported before reading
    intercept[MsgParamException](action("msg_read_errors", Map("topic" -> topic))).getMessage shouldBe
      "msg read parameter 'committer': unknown column 'max_bach', allowed: delivery, max_batch, max_interval, parallelism, commit_when"
    intercept[RuntimeException](action("msg_read_errors", Map("topic" -> topic), "list")).getMessage shouldBe
      "Messaging provider 'unknown' not found. Available names: [flaky, main, nogroup]"
  }

  behavior of "msg admin calls"

  private def rows(view: String, data: Map[String, Any], actionName: String = "get"): Seq[Map[String, Any]] =
    action(view, data, actionName) match {
      case IteratorResult(it) => it.toList.map { case m: Map[String@unchecked, _] => m }
      case x => fail(s"IteratorResult expected, got: $x")
    }

  it should "describe topics and partitions" in {
    val topic = newTopic(partitions = 2)
    val described = rows("msg_admin_partitions", Map("topic" -> topic))
    described.map(r => (r("topic"), r("partition"))) shouldBe List((topic, 0), (topic, 1))
    described.head("leader").asInstanceOf[Map[String, Any]]("port") shouldBe kafkaConfig.kafkaPort
    described.head("replicas").asInstanceOf[List[Map[String, Any]]].map(_("port")) shouldBe List(kafkaConfig.kafkaPort)
    described.head.keys.toList shouldBe List("topic", "partition", "leader", "replicas", "in_sync_replicas")
    rows("msg_admin_topics", Map()).filter(_("topic") == topic) shouldBe described
  }

  it should "provide beginning, end and committed offsets" in {
    val topic = newTopic(partitions = 2)
    await(Source(List(
      OutMessage(topic, null, "a", Some(0)), OutMessage(topic, null, "b", Some(0)), OutMessage(topic, null, "c", Some(1)),
    )).runWith(provider.createSender(ConfigFactory.empty).sink))
    rows("msg_admin_offsets", Map("topic" -> topic)) shouldBe List(
      Map("topic" -> topic, "partition" -> 0, "offset" -> 0L), Map("topic" -> topic, "partition" -> 1, "offset" -> 0L))
    rows("msg_admin_offsets", Map("topic" -> topic), "list") shouldBe List(
      Map("topic" -> topic, "partition" -> 0, "offset" -> 2L), Map("topic" -> topic, "partition" -> 1, "offset" -> 1L))
    read("msg_read_commit", Map("topic" -> topic, "group" -> "g-admin"))
    rows("msg_admin_committed", Map("topic" -> topic, "group" -> "g-admin")) shouldBe List(
      Map("group_id" -> "g-admin", "topic" -> topic, "partition" -> 0, "offset" -> 2L, "metadata" -> "m-b"))
  }

  behavior of "msg send"

  private def readAll(topic: String, count: Int, partition: Int = 0): Seq[InMessage] =
    await(provider.createReader(ConfigFactory.empty)
      .source(Subscription.Assignment(Set(TopicPartition(topic, partition)), None)).take(count).runWith(Sink.seq))

  private def routingTopics(regions: String*): String = {
    val prefix = s"msg-ops-route-${topicCounter.incrementAndGet()}-"
    regions.foreach(r => EmbeddedKafka.createCustomTopic(prefix + r)(kafkaConfig).get)
    prefix
  }

  it should "evaluate parameters for each message" in {
    val src = newTopic()
    val prefix = routingTopics("eu", "us")
    send(src,
      Map("region" -> "eu", "k" -> "a", "kc" -> "string"),
      Map("region" -> "us", "k" -> "b", "kc" -> "json"),
      Map("region" -> "eu", "k" -> "c", "kc" -> "json"))
    action("msg_send_per_message", Map("src" -> src, "prefix" -> prefix)) shouldBe LongResult(3)
    readAll(prefix + "eu", 2).map(m => (m.key, m.value, m.headers)) shouldBe List( // key read with string codec
      ("a!", Map("region" -> "eu", "k" -> "a", "kc" -> "string"), List(Header("region", "eu"))),
      ("\"c!\"", Map("region" -> "eu", "k" -> "c", "kc" -> "json"), List(Header("region", "eu"))))
    readAll(prefix + "us", 1).map(m => (m.key, m.headers)) shouldBe List(("\"b!\"", List(Header("region", "us"))))
  }

  it should "send rows with key, headers and partition" in {
    val topic = newTopic(partitions = 2)
    action("msg_send_rows", Map("topic" -> topic)) shouldBe LongResult(2)
    readAll(topic, 2, partition = 1).map(m => (m.key, m.value, m.headers)) shouldBe List(
      ("1", Map("name" -> "a"), List(Header("h", "v"))),
      ("2", Map("name" -> "b"), List(Header("h", "v"))),
    )
  }

  it should "send single value as one message" in {
    val topic = newTopic()
    action("msg_send_rows", Map("topic" -> topic), "list") shouldBe LongResult(1)
    readAll(topic, 1).map(m => (m.key, m.value)) shouldBe List(("k", "x"))
  }

  it should "forward read messages and commit them after they are sent" in {
    val in = newTopic()
    val out = newTopic()
    send(in, "k1" -> "a", "k2" -> "b", "k3" -> "c", "k4" -> "d")
    action("msg_send_forward", Map("src" -> in, "dst" -> out, "group" -> "g-forward")) shouldBe LongResult(3)
    readAll(out, 3).map(m => (m.key, m.value)) shouldBe List(("k1", "a"), ("k2", "b"), ("k3", "c"))
    await(provider.admin.committedOffsets("g-forward", Set(TopicPartition(in, 0)))) shouldBe
      Map(TopicPartition(in, 0) -> OffsetAndMetadata(3, null))
  }

  it should "filter forwarded messages, keep key, commit filtered ones" in {
    val src = newTopic()
    val dst = newTopic()
    send(src, "k1" -> Map("status" -> "a"), "k2" -> Map("status" -> "draft"), "k3" -> Map("status" -> "b"),
      "k4" -> Map("status" -> "c"), "k5" -> Map("status" -> "end"))
    action("msg_send_filter", Map("src" -> src, "dst" -> dst, "group" -> "g-filter")) shouldBe LongResult(3)
    readAll(dst, 3).map(m => (m.key, m.value)) shouldBe
      List(("k1", Map("status" -> "a")), ("k3", Map("status" -> "b")), ("k5", Map("status" -> "end")))
    await(provider.admin.committedOffsets("g-filter", Set(TopicPartition(src, 0)))) shouldBe
      Map(TopicPartition(src, 0) -> OffsetAndMetadata(5, null))
  }

  it should "filter rows and single values" in {
    val dst = newTopic()
    action("msg_send_filter", Map("dst" -> dst, "skip" -> 2), "list") shouldBe LongResult(2)
    readAll(dst, 2).map(_.value) shouldBe List(Map("id" -> 1), Map("id" -> 3))
    action("msg_send_filter_value", Map("dst" -> dst), "list") shouldBe LongResult(0)
    intercept[MsgParamException](action("msg_send_filter_value", Map("dst" -> dst), "count"))
      .getMessage should include ("msg send parameter 'filter': boolean expected")
  }

  it should "filter foreach results, commit filtered ones" in {
    val src = newTopic()
    val dst = newTopic()
    send(src, "a", "b", "c")
    action("msg_send_filter_value", Map("src" -> src, "dst" -> dst, "group" -> "g-filter-value")) shouldBe
      LongResult(2)
    readAll(dst, 2).map(_.value) shouldBe List("a!", "c!")
    await(provider.admin.committedOffsets("g-filter-value", Set(TopicPartition(src, 0)))) shouldBe
      Map(TopicPartition(src, 0) -> OffsetAndMetadata(3, null))
  }

  /** Inserts msg_ops_dummy rows 1000 - 1499 (read by msg_send_db view) if not inserted yet */
  private def insertDummyRows(): Unit = {
    val conn = java.sql.DriverManager.getConnection(s"jdbc:hsqldb:mem:$dbNamePrefix")
    try {
      val rs = conn.createStatement.executeQuery("select count(*) from msg_ops_dummy where id >= 1000")
      rs.next()
      if (rs.getInt(1) == 0) {
        val st = conn.prepareStatement("insert into msg_ops_dummy(id, value) values (?, ?)")
        (1000 until 1500).foreach { i => st.setInt(1, i); st.setString(2, s"v$i"); st.executeUpdate() }
        conn.commit()
      }
    } finally conn.close()
  }

  it should "stream query rows, unwrap single value" in {
    val topic = newTopic()
    insertDummyRows()
    action("msg_send_db", Map("topic" -> topic)) match { // db result is closed after send, as in processResult
      case DbResult(r, cleanup) => cleanup(None); r shouldBe LongResult(500)
      case x => fail(s"DbResult expected, got: $x")
    }
    val sent = readAll(topic, 500).map(_.value)
    sent.head shouldBe Map("id" -> 1000, "value" -> "v1000")
    sent.last shouldBe Map("id" -> 1499, "value" -> "v1499")
    action("msg_send_db", Map("topic" -> topic), "list") shouldBe LongResult(1)
    readAll(topic, 501).last.value shouldBe "v1000"
  }

  it should "send query result exceeding max result size" in {
    val topic = newTopic()
    insertDummyRows()
    val actionQr = qr
    val limitedRes = actionQr.resourcesFactory.resources.withMaxResultSize(100)
    qr = new QuereaseResources()(actionQr.resourcesFactory.copy()(resources = limitedRes),
      actionQr.ec, actionQr.as, actionQr.httpReq, actionQr.qio, actionQr.fileStreamers, actionQr.httpClients,
      actionQr.msgProviders, actionQr.parametersProvider, actionQr.logger)
    try {
      intercept[org.tresql.TooManyRowsException](
        org.tresql.Query("msg_ops_dummy[id >= 1000]")(limitedRes).toListOfMaps)
      action("msg_send_db", Map("topic" -> topic)) match {
        case DbResult(r, cleanup) => cleanup(None); r shouldBe LongResult(500)
        case x => fail(s"DbResult expected, got: $x")
      }
    } finally qr = actionQr
  }

  behavior of "foreach on msg read"

  it should "transform messages" in {
    val topic = newTopic()
    send(topic, "a", "b", "c", "d")
    read("msg_foreach", Map("topic" -> topic)) shouldBe List("a!", "b!", "c!")
  }

  it should "bind item variable, metadata and index" in {
    val topic = newTopic()
    send(topic, "a", "b")
    // untyped bind variables are returned as strings by hsqldb evaluator
    read("msg_foreach", Map("topic" -> topic), "list")
      .map { case r: Map[String@unchecked, _] => r.map { case (k, v) => k -> String.valueOf(v) } } shouldBe List(
      Map("m" -> "a", "off" -> "0", "idx" -> "0"),
      Map("m" -> "b", "off" -> "1", "idx" -> "1"),
    )
  }

  it should "fold transformed messages" in {
    val topic = newTopic()
    send(topic, 1, 2, 3)
    action("msg_foreach", Map("topic" -> topic), "count") match {
      case AnyResult(n) => n.toString shouldBe "6"
      case x => fail(s"Unexpected result: $x")
    }
  }

  it should "send transformed messages, commit filtered ones" in {
    val src = newTopic()
    val dst = newTopic()
    send(src, "a", "b", "c", "d", "e")
    action("msg_foreach_send", Map("src" -> src, "dst" -> dst, "group" -> "g-foreach")) shouldBe LongResult(3)
    readAll(dst, 3).map(_.value) shouldBe List("a!", "c!", "d!")
    await(provider.admin.committedOffsets("g-foreach", Set(TopicPartition(src, 0)))) shouldBe
      Map(TopicPartition(src, 0) -> OffsetAndMetadata(4, null))
  }

  it should "process messages in body transaction" in {
    val topic = newTopic()
    send(topic, Map("id" -> 10, "name" -> "x"), Map("id" -> 11, "name" -> "y"))
    read("msg_foreach_db", Map("topic" -> topic)) shouldBe List("x", "y")
    org.tresql.Query("msg_ops_dummy[id in (10, 11)] {value}")(qr.resourcesFactory.resources)
      .map(_.s("value")).toList.sorted shouldBe List("x", "y")
  }

  it should "collect transformed messages, commit filtered ones" in {
    val topic = newTopic()
    send(topic, "a", "b", "c", "d")
    action("msg_foreach_collect", Map("topic" -> topic, "group" -> "g-collect")) match {
      case TresqlResult(r) => r.unique[Int] shouldBe 2
      case x => fail(s"Unexpected result: $x")
    }
    await(provider.admin.committedOffsets("g-collect", Set(TopicPartition(topic, 0)))) shouldBe
      Map(TopicPartition(topic, 0) -> OffsetAndMetadata(3, null))
  }

  it should "restart reader after failure" in {
    val src = newTopic()
    val dst = newTopic()
    send(src, "a", "boom")
    intercept[RuntimeException](action("msg_read_restart", Map("src" -> src, "dst" -> dst)))
      .getMessage shouldBe "reader failed"
    readAll(dst, 3).map(_.value) shouldBe List("a", "a", "a") // initial read + 2 restarts
  }

  it should "restart committing read after commit failure" in {
    import scala.jdk.CollectionConverters._
    val topic = newTopic()
    send(topic, "a", "b", "c", "d")
    MsgOpsTestFunctions.recorded.clear()
    failCommit.set(true)
    val collected = action("msg_read_commit_restart", Map("topic" -> topic, "group" -> "g-read-restart")) match {
      case TresqlResult(r) => r.unique[Int]
      case x => fail(s"Unexpected result: $x")
    }
    failCommit.get shouldBe false
    // commit of a fails - new consumer reads from a again, messages processed before restart are processed again
    val processed = MsgOpsTestFunctions.recorded.asScala.toList
    processed.distinct shouldBe List("a", "b", "c")
    processed.count(_ == "a") shouldBe 2
    processed.takeRight(3) shouldBe List("a", "b", "c")
    collected should (be >= 3 and be <= processed.size) // includes messages emitted by failed attempt
    await(provider.admin.committedOffsets("g-read-restart", Set(TopicPartition(topic, 0)))) shouldBe
      Map(TopicPartition(topic, 0) -> OffsetAndMetadata(3, null))
  }

  it should "not restart committing read after processing failure" in {
    import scala.jdk.CollectionConverters._
    val topic = newTopic()
    send(topic, "a", "b", "c")
    MsgOpsTestFunctions.recorded.clear()
    val failure = intercept[Exception](
      read("msg_read_commit_restart", Map("topic" -> topic, "group" -> "g-read-proc"), "list"))
    Iterator.iterate[Throwable](failure)(_.getCause).takeWhile(_ != null).map(_.getMessage).toList should
      contain ("processing failed on b")
    Thread.sleep(500) // restart would process messages again
    MsgOpsTestFunctions.recorded.asScala.toList shouldBe List("a", "b")
  }

  it should "continue and commit after processing failure handled by recover" in {
    val topic = newTopic()
    send(topic, "a", "b", "c")
    MsgOpsTestFunctions.recorded.clear()
    read("msg_foreach_recover", Map("topic" -> topic, "group" -> "g-recover")) shouldBe List("a", "failed b", "c")
    await(provider.admin.committedOffsets("g-recover", Set(TopicPartition(topic, 0)))) shouldBe
      Map(TopicPartition(topic, 0) -> OffsetAndMetadata(3, null))
  }

  it should "restart committing read after reader failure until max restarts" in {
    import scala.jdk.CollectionConverters._
    val topic = newTopic()
    send(topic, "a", "boom")
    MsgOpsTestFunctions.recorded.clear()
    intercept[RuntimeException](
      read("msg_read_commit_restart", Map("topic" -> topic, "group" -> "g-read-fail"), "count"))
      .getMessage shouldBe "reader failed"
    val processed = MsgOpsTestFunctions.recorded.asScala.toList
    processed.distinct shouldBe List("a")
    processed.size should (be >= 1 and be <= 3) // initial read + 2 restarts, a is read again if not committed yet
  }

  it should "not extend timeout within by restarts of committing read" in {
    val topic = newTopic()
    send(topic, "boom") // reader fails - restarted without limit
    val (msgs, millis) = timed(read("msg_read_commit_restart", Map("topic" -> topic, "group" -> "g-read-within"), "insert"))
    msgs shouldBe Nil // stream completes when 'within' expires, also during restart backoff
    millis should (be >= 1500L and be < 15000L)
  }

  private def sendUndecodable(topic: String): Unit = // value decoding of default json codec fails
    await(Source.single(OutMessage(topic, null, "{not json", valueEncoder = provider.codecs("string")))
      .runWith(provider.createSender(ConfigFactory.empty).sink))

  it should "not restart decoding failure" in {
    import scala.jdk.CollectionConverters._
    val topic = newTopic()
    send(topic, "a")
    sendUndecodable(topic)
    MsgOpsTestFunctions.recorded.clear()
    intercept[MsgDecodingException](read("msg_read_decode_error", Map("topic" -> topic, "group" -> "g-decode")))
    Thread.sleep(500) // restart would process messages again
    MsgOpsTestFunctions.recorded.asScala.toList shouldBe List("a")
    // msg send restart, reader restart (without max restarts - would not complete)
    intercept[MsgDecodingException](
      action("msg_read_decode_error", Map("topic" -> topic, "group" -> "g-decode-send", "dst" -> newTopic()), "insert"))
    intercept[MsgDecodingException](read("msg_read_decode_error", Map("topic" -> topic), "update"))
  }

  it should "skip and commit message which cannot be decoded" in {
    import scala.jdk.CollectionConverters._
    val topic = newTopic()
    send(topic, "a")
    sendUndecodable(topic)
    send(topic, "b", "c")
    MsgOpsTestFunctions.recorded.clear()
    read("msg_read_decode_error", Map("topic" -> topic, "group" -> "g-decode-skip"), "list") shouldBe
      List("a", "b", "c")
    MsgOpsTestFunctions.recorded.asScala.toList shouldBe List("a", "b", "c")
    await(provider.admin.committedOffsets("g-decode-skip", Set(TopicPartition(topic, 0)))) shouldBe
      Map(TopicPartition(topic, 0) -> OffsetAndMetadata(4, null))
    values(read("msg_read_decode_error", Map("topic" -> topic), "count")) shouldBe List("a", "b", "c")
  }

  it should "start foreach index from 0 for each attempt of committing read" in {
    import scala.jdk.CollectionConverters._
    val topic = newTopic()
    send(topic, "a", "b", "c")
    MsgOpsTestFunctions.recorded.clear()
    failCommit.set(true)
    action("msg_read_commit_restart", Map("topic" -> topic, "group" -> "g-read-idx"), "update")
    failCommit.get shouldBe false
    MsgOpsTestFunctions.recorded.asScala.toList.takeRight(3) shouldBe List("a:0", "b:1", "c:2")
  }

  it should "fail msg read on reader configuration error, not restart it" in {
    val (e, millis) = timed(intercept[RuntimeException](
      action("msg_read_commit_restart", Map("topic" -> newTopic()), "delete")))
    e.getMessage should include ("group is required")
    millis should be < 5000L
  }

  it should "restart reading, processing, sending and committing after failure" in {
    val src = newTopic()
    val dst = newTopic()
    send(src, "a", "b", "c", "d", "e")
    MsgOpsTestFunctions.failOn.set("b")
    val sent = action("msg_send_restart", Map("src" -> src, "dst" -> dst, "group" -> "g-send-restart")) match {
      case LongResult(n) => n.toInt
      case x => fail(s"Unexpected result: $x")
    }
    MsgOpsTestFunctions.failures.get shouldBe 1
    // a! may be sent again if its commit had not completed before failure - at least once
    readAll(dst, sent).map(_.value).distinct shouldBe List("a!", "b!", "c!", "d!")
    sent should (be >= 4 and be <= 5)
    await(provider.admin.committedOffsets("g-send-restart", Set(TopicPartition(src, 0)))) shouldBe
      Map(TopicPartition(src, 0) -> OffsetAndMetadata(4, null))
  }

  it should "not restart on parameter error" in {
    val src = newTopic()
    send(src, "a")
    val (codecErr, millis) = timed(intercept[MsgParamException](
      action("msg_send_restart", Map("src" -> src, "dst" -> "d"), "count")))
    codecErr.getMessage should startWith ("msg send parameter 'using': Message codec 'jsn' not found")
    millis should be < 5000L
    intercept[MsgParamException](action("msg_send_restart", Map("src" -> src, "dst" -> "d"), "insert"))
      .getMessage should startWith ("msg send parameter evaluation failed")
    val (toErr, toMillis) = timed(intercept[MsgParamException](
      action("msg_send_restart", Map("src" -> src, "dst" -> "d"), "update")))
    toErr.getMessage should startWith ("msg read parameter 'to' evaluation failed: Missing bind variable: valu")
    toMillis should be < 5000L
    val (filterErr, filterMillis) = timed(intercept[MsgParamException](
      action("msg_send_restart", Map("src" -> src, "dst" -> "d"), "delete")))
    filterErr.getMessage should startWith ("msg send parameter 'filter' evaluation failed: Missing bind variable: valu")
    filterMillis should be < 5000L
  }

  it should "require messaging result for msg send restart" in {
    intercept[RuntimeException](action("msg_send_restart", Map("src" -> "s", "dst" -> "d"), "list")).getMessage shouldBe
      "msg send parameter 'restart' requires messaging result (msg read) of operation, got: org.wabase.TresqlResult"
  }

  behavior of "msg multi send"

  private def rows(res: QuereaseResult): List[Map[String, Any]] = res match {
    case IteratorResult(it) => it.map { case r: Map[String@unchecked, _] => r }.toList
    case x => fail(s"IteratorResult expected, got: $x")
  }

  it should "send to topic of element, count messages per destination and topic" in {
    val prefix = routingTopics("eu", "us")
    rows(action("msg_send_per_message", Map("prefix" -> prefix), "list")) shouldBe List(
      Map("provider" -> "main", "topic" -> (prefix + "eu"), "count" -> 2),
      Map("provider" -> "main", "topic" -> (prefix + "us"), "count" -> 1),
      Map("provider" -> "main", "topic" -> null, "count" -> 0),
    )
    readAll(prefix + "eu", 2).map(m => (m.key, m.value)) shouldBe
      List(("1", Map("id" -> 1, "region" -> "eu")), ("3", Map("id" -> 3, "region" -> "eu")))
    readAll(prefix + "us", 1).map(_.key) shouldBe List("2")
  }

  it should "route, transform and copy messages, commit after all destinations" in {
    val src = newTopic()
    val (eu, us, all) = (newTopic(), newTopic(), newTopic())
    send(src,
      "k1" -> Map("id" -> 1, "region" -> "eu", "status" -> "new"),
      "k2" -> Map("id" -> 2, "region" -> "us", "status" -> "new"),
      "k3" -> Map("id" -> 3, "region" -> "us", "status" -> "new"),
      "k4" -> Map("id" -> 4, "region" -> "eu", "status" -> "end"))
    rows(action("msg_multi_send",
      Map("src" -> src, "group" -> "g-multi", "eu" -> eu, "us" -> us, "all" -> all))) shouldBe List(
      Map("provider" -> "main", "topic" -> eu, "count" -> 2),
      Map("provider" -> "main", "topic" -> us, "count" -> 2),
      Map("provider" -> "main", "topic" -> all, "count" -> 4),
    )
    readAll(eu, 2).map(m => (m.key, m.value)) shouldBe List(
      ("k1", Map("id" -> 1, "region" -> "eu", "status" -> "new")),
      ("k4", Map("id" -> 4, "region" -> "eu", "status" -> "end")))
    readAll(us, 2).map(m => (m.key, m.value)) shouldBe List(
      ("2", Map("id" -> 2, "status" -> "new")), ("3", Map("id" -> 3, "status" -> "new")))
    readAll(all, 4).map(_.key) shouldBe List("k1", "k2", "k3", "k4")
    await(provider.admin.committedOffsets("g-multi", Set(TopicPartition(src, 0)))) shouldBe
      Map(TopicPartition(src, 0) -> OffsetAndMetadata(4, null))
  }

  it should "split incoming message into several messages, commit after the last one" in {
    val (src, dst, copy) = (newTopic(), newTopic(), newTopic())
    send(src, "a", "b", "c", "d")
    rows(action("msg_multi_send_split", Map("src" -> src, "dst" -> dst, "copy" -> copy, "group" -> "g-split")))
      .map(_("count")) shouldBe List(6, 3)
    readAll(dst, 6).map(_.value) shouldBe
      List("a1", "a2", "b1", "b2", "c1", "c2").map(v => Map("v" -> v))
    readAll(copy, 3).map(_.value) shouldBe List("a", "b", "c")
    await(provider.admin.committedOffsets("g-split", Set(TopicPartition(src, 0)))) shouldBe
      Map(TopicPartition(src, 0) -> OffsetAndMetadata(3, null))
  }

  it should "split rows source elements, bind index and parent scope" in {
    val (dst, dst2) = (newTopic(), newTopic())
    rows(action("msg_multi_send", Map("dst" -> dst, "dst2" -> dst2), "list")) shouldBe List(
      Map("provider" -> "main", "topic" -> dst, "count" -> 4),
      Map("provider" -> "main", "topic" -> dst2, "count" -> 1),
    )
    readAll(dst, 4).map(_.value) shouldBe List(
      Map("id" -> 1, "idx" -> 0, "tag" -> "t"), Map("id" -> 11, "idx" -> 0, "tag" -> "t"),
      Map("id" -> 2, "idx" -> 1, "tag" -> "t"), Map("id" -> 12, "idx" -> 1, "tag" -> "t"))
    readAll(dst2, 1).map(_.value) shouldBe List(Map("id" -> 2))
  }

  it should "query action's database in destination operation of query source" in {
    val dst = newTopic()
    insertDummyRows()
    rows(action("msg_multi_send_db", Map("dst" -> dst))).map(_("count")) shouldBe List(10)
    readAll(dst, 10).map(_.value) shouldBe (1000 until 1010).map(i => s"v$i")
  }

  it should "restart reading, processing, sending to all destinations and committing after failure" in {
    val (src, dst, copy) = (newTopic(), newTopic(), newTopic())
    send(src, "a", "b", "c", "d", "e")
    val failuresBefore = MsgOpsTestFunctions.failures.get
    MsgOpsTestFunctions.failOn.set("b")
    val counts = rows(action("msg_multi_send_restart",
      Map("src" -> src, "dst" -> dst, "copy" -> copy, "group" -> "g-multi-restart"))).map(_("count").asInstanceOf[Long])
    MsgOpsTestFunctions.failures.get shouldBe failuresBefore + 1
    // messages sent before failure but not committed are sent again - at least once
    readAll(copy, counts.head.toInt).map(_.value).distinct shouldBe List("a", "b", "c", "d")
    readAll(dst, counts(1).toInt).map(_.value).distinct shouldBe List("a!", "b!", "c!", "d!")
    counts.foreach(_ should (be >= 4L and be <= 6L))
    await(provider.admin.committedOffsets("g-multi-restart", Set(TopicPartition(src, 0)))) shouldBe
      Map(TopicPartition(src, 0) -> OffsetAndMetadata(4, null))
  }

  it should "require messaging result for restart" in {
    intercept[RuntimeException](action("msg_multi_send", Map("dst" -> "d"), "count")).getMessage shouldBe
      "msg multi send parameter 'restart' requires messaging result (msg read) of operation, got: org.wabase.TresqlResult"
  }

  it should "send messaging result of view call with result type, commit after sending" in {
    def check(actionName: String, group: String)(sent: QuereaseResult => Long): Unit = {
      val (src, dst) = (newTopic(), newTopic())
      send(src, "a", "b", "c", "d")
      sent(action("msg_send_compat", Map("src" -> src, "dst" -> dst, "group" -> group), actionName)) shouldBe 3
      readAll(dst, 3).map(_.value) shouldBe List("a", "b", "c")
      await(provider.admin.committedOffsets(group, Set(TopicPartition(src, 0)))) shouldBe
        Map(TopicPartition(src, 0) -> OffsetAndMetadata(3, null))
    }
    val count: QuereaseResult => Long = { case LongResult(n) => n; case x => fail(s"Unexpected result: $x") }
    check("get", "g-compat")(count)
    check("list", "g-compat-restart")(count)
    check("count", "g-compat-multi")(r => rows(r).head("count").asInstanceOf[Long])
  }

  it should "not count filtered messages in index" in {
    val (src, dst) = (newTopic(), newTopic())
    send(src, "a", "b", "c")
    rows(action("msg_multi_send_idx", Map("src" -> src, "dst" -> dst, "group" -> "g-idx"))).head("count") shouldBe 2
    readAll(dst, 2).map(_.value) shouldBe List("a0", "c1") // b filtered by foreach, not counted
  }

  it should "commit, not send and not count in index message skipped by reader" in {
    def check(actionName: String, group: String, expected: List[String])(sent: QuereaseResult => Any): Unit = {
      val (src, dst) = (newTopic(), newTopic())
      send(src, "a")
      sendUndecodable(src)
      send(src, "c")
      sent(action("msg_multi_send_idx", Map("src" -> src, "dst" -> dst, "group" -> group), actionName)) shouldBe 2
      readAll(dst, 2).map(_.value) shouldBe expected
      await(provider.admin.committedOffsets(group, Set(TopicPartition(src, 0)))) shouldBe
        Map(TopicPartition(src, 0) -> OffsetAndMetadata(3, null)) // skipped message is committed
    }
    check("list", "g-idx-skip", List("a0", "c1"))(rows(_).head("count"))
    check("count", "g-send-skip", List("a", "c")) { case LongResult(n) => n; case x => fail(s"Unexpected result: $x") }
  }

  behavior of "msg operations review fixes"

  it should "not send no result elements of stream" in {
    val topic = newTopic()
    action("msg_review_fixes", Map("topic" -> topic)) shouldBe LongResult(1)
    readAll(topic, 1).map(_.value) shouldBe List(1)
  }

  it should "process messaging result wrapped in db result with foreach" in {
    val topic = newTopic()
    send(topic, "a", "b")
    read("msg_review_fixes", Map("topic" -> topic), "list") shouldBe List("a?", "b?")
  }
}

/** Functions invoked from msg ops test actions */
object MsgOpsTestFunctions {
  val failOn = new java.util.concurrent.atomic.AtomicReference[String]()
  val failures = new java.util.concurrent.atomic.AtomicInteger
  /** Fails once for message with value [[failOn]], otherwise returns value with '!' */
  def failOnce(data: Map[String, Any]): String = {
    val value = String.valueOf(data("value"))
    val target = failOn.get
    if (value == target && failOn.compareAndSet(target, null)) { // compareAndSet compares references
      failures.incrementAndGet()
      throw new RuntimeException(s"transient failure on $value")
    }
    value + "!"
  }
  val recorded = new java.util.concurrent.ConcurrentLinkedQueue[String]
  /** Records and returns message value */
  def record(data: Map[String, Any]): String = {
    val value = String.valueOf(data("value"))
    recorded.add(value)
    value
  }
  /** Records message value with foreach index */
  def recordIdx(data: Map[String, Any]): String = {
    val value = String.valueOf(data("value"))
    recorded.add(s"$value:${data("__idx")}")
    value
  }
  /** Records message value, fails for value 'b' */
  def recordFailOnB(data: Map[String, Any]): String = {
    val value = record(data)
    if (value == "b") throw new RuntimeException(s"processing failed on $value")
    value
  }
}
