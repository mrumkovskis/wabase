package org.wabase

import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers
import org.wabase.msg.MsgParams.MsgParamException
import org.wabase.msg._

import java.time.{Instant, LocalDateTime, ZoneId}
import scala.concurrent.duration._

class MsgParamsSpecs extends FlatSpec with Matchers {
  import Subscription._

  private def err(f: => Any) = intercept[MsgParamException](f).getMessage

  behavior of "MsgReadParams"

  it should "create subscription from 'from' rows" in {
    import MsgReadParams.subscription
    val g = Some("g")
    subscription(List(Map("topic" -> "a"), Map("topic" -> "b")), g) shouldBe Topics(Set("a", "b"), g)
    subscription("a", None) shouldBe Topics(Set("a"), None)                 // scalar shorthand
    subscription(Map("_1" -> "a"), None) shouldBe Topics(Set("a"), None)    // single unnamed column shorthand
    subscription(Map("pattern" -> "a.*"), g) shouldBe TopicPattern("a.*", g)
    subscription(List(Map("pattern" -> "a.*"), Map("pattern" -> "b")), g) shouldBe TopicPattern("(?:a.*)|(?:b)", g)
    subscription(List(Map("topic" -> "a", "partition" -> 0), Map("topic" -> "a", "partition" -> BigDecimal(1))), g) shouldBe
      Assignment(Set(TopicPartition("a", 0), TopicPartition("a", 1)), g)
    subscription(Map("topic" -> "a", "partition" -> 0L, "offset" -> java.math.BigDecimal.valueOf(5)), g) shouldBe
      AssignmentWithOffset(Map(TopicPartition("a", 0) -> 5L), g)
    val ldt = LocalDateTime.of(2026, 1, 1, 10, 0)
    val millis = ldt.atZone(ZoneId.systemDefault).toInstant.toEpochMilli
    subscription(List(
      Map("topic" -> "a", "partition" -> 0, "timestamp" -> ldt),
      Map("topic" -> "a", "partition" -> 1, "timestamp" -> java.sql.Timestamp.valueOf(ldt)),
      Map("topic" -> "a", "partition" -> 2, "timestamp" -> Instant.ofEpochMilli(millis)),
      Map("topic" -> "a", "partition" -> 3, "timestamp" -> millis),
    ), None) shouldBe AssignmentOffsetsForTimes((0 to 3).map(TopicPartition("a", _) -> millis).toMap, None)
    // null columns are ignored
    subscription(Map("topic" -> "a", "partition" -> null, "offset" -> null), None) shouldBe Topics(Set("a"), None)
  }

  it should "report invalid 'from' rows" in {
    import MsgReadParams.subscription
    err(subscription(Nil, None)) shouldBe "msg read parameter 'from': at least one row expected"
    err(subscription(Map("topc" -> "a", "partition" -> 0), None)) shouldBe
      "msg read parameter 'from': unknown column 'topc', allowed: topic, partition, offset, timestamp, pattern"
    err(subscription(List(Map("topic" -> "a"), Map("topic" -> "b", "partition" -> 0)), None)) shouldBe
      "msg read parameter 'from': all rows must have the same columns, row 1 has (topic), row 2 has (topic, partition)"
    err(subscription(Map("topic" -> "a", "offset" -> 1), None)) should startWith(
      "msg read parameter 'from': unsupported column combination (topic, offset)")
    err(subscription(List(Map("topic" -> "a", "partition" -> 0), Map("topic" -> "a", "partition" -> 0)), None)) shouldBe
      "msg read parameter 'from': duplicate partitions: TopicPartition(a,0)"
    err(subscription(Map("topic" -> "a", "partition" -> 1.5), None)) shouldBe
      "msg read parameter 'from': column 'partition' integer expected, got java.lang.Double: 1.5"
    err(subscription(Map("topic" -> 1), None)) shouldBe
      "msg read parameter 'from': column 'topic' string expected, got java.lang.Integer: 1"
  }

  it should "evaluate parameters" in {
    val p = MsgReadParams.fromValues(
      group = List(Map("group" -> "g")),
      from = List(Map("topic" -> "t")),
      timeout = Map("within" -> "5s", "idle" -> 300),
      committer = Map("max_batch" -> BigDecimal(10), "max_interval" -> "2s", "delivery" -> "SendAndForget",
        "commit_when" -> "NextOffsetObserved"),
      key = List(Map("k" -> "msg_key")),
      usingCodecs = Map("key" -> "json", "value" -> "avro"),
      atMostOnce = null,
    )
    p.subscription shouldBe Topics(Set("t"), Some("g"))
    p.within shouldBe Some(5.seconds)
    p.idle shouldBe Some(300.millis)
    p.keyCodec shouldBe "json"
    p.valueCodec shouldBe "avro"
    p.atMostOnce shouldBe false
    p.keyName shouldBe "msg_key"
    val cc = p.committerConfig.getConfig(MsgReadParams.CommitterPath)
    cc.getInt("max-batch") shouldBe 10
    cc.getDuration("max-interval").toMillis shouldBe 2000
    cc.getString("delivery") shouldBe "SendAndForget"
    cc.getString("when") shouldBe "NextOffsetObserved"
    p.readerConfig.getDuration(MsgProvider.StopTimeoutPath).toMillis shouldBe 0

    val minimal = MsgReadParams.fromValues(null, "t", null, null, null, null, null)
    minimal shouldBe MsgReadParams(Topics(Set("t"), None))
    minimal.readerConfig.isEmpty shouldBe true
    minimal.committerConfig.isEmpty shouldBe true

    // boolean committer shorthand, value codec shorthand, idle only
    val p2 = MsgReadParams.fromValues(null, "t", Map("idle" -> "1 second"), Map("_1" -> true), null, "avro", false)
    p2.committer.map(_.isEmpty) shouldBe Some(true)
    p2.within shouldBe None
    p2.idle shouldBe Some(1.second)
    (p2.keyCodec, p2.valueCodec) shouldBe ((null, "avro"))
    MsgReadParams.fromValues(null, "t", null, false, null, Map("key" -> "string", "value" -> null), true) shouldBe
      MsgReadParams(Topics(Set("t"), None), keyCodec = "string", atMostOnce = true)
  }

  it should "report invalid parameters" in {
    def read(group: Any = null, timeout: Any = null, committer: Any = null, usingCodecs: Any = null, amo: Any = null) =
      MsgReadParams.fromValues(group, "t", timeout, committer, null, usingCodecs, amo)
    err(read(group = 1)) shouldBe "msg read parameter 'group': string expected, got java.lang.Integer: 1"
    err(read(group = List(Map("a" -> "x"), Map("a" -> "y")))) shouldBe
      "msg read parameter 'group': single row expected, got 2 rows"
    err(read(timeout = Map("within" -> "5 parsecs"))) shouldBe
      "msg read parameter 'timeout': invalid duration in column 'within': 5 parsecs"
    err(read(timeout = Map("withn" -> "5s"))) shouldBe
      "msg read parameter 'timeout': unknown column 'withn', allowed: within, idle"
    err(read(committer = Map("max_bach" -> 1))) shouldBe
      "msg read parameter 'committer': unknown column 'max_bach', allowed: delivery, max_batch, max_interval, parallelism, commit_when"
    err(read(committer = "yes")) shouldBe "msg read parameter 'committer': boolean expected, got java.lang.String: yes"
    MsgReadParams.isLast(List(Map("_1" -> true))) shouldBe true
    MsgReadParams.isLast(List(Map("_1" -> null))) shouldBe false
    MsgReadParams.isLast(Nil) shouldBe false
    err(MsgReadParams.isLast("end")) shouldBe "msg read parameter 'to': boolean expected, got java.lang.String: end " +
      "(comparison must be enclosed in braces, for example to={:value = 'end'})"
    MsgReadParams.commitMetadata(List(Map("_1" -> "m"))) shouldBe "m"
    err(MsgReadParams.subscription(Map("topc" -> "a"), None)) shouldBe
      "msg read parameter 'from': unknown column 'topc', allowed: topic, partition, offset, timestamp, pattern"
    err(read(committer = true, amo = true)) shouldBe
      "msg read parameter 'at_most_once': 'committer' and 'at_most_once' are mutually exclusive"
    err(read(usingCodecs = Map("key" -> "json", "valu" -> "avro"))) shouldBe
      "msg read parameter 'using': unknown column 'valu', allowed: key, value"
    err(read(usingCodecs = 1)) shouldBe "msg read parameter 'using': column 'value' string expected, got java.lang.Integer: 1"
  }

  it should "evaluate restart settings" in {
    def restart(r: Any) = MsgReadParams.fromValues(null, "t", null, null, null, null, null, r).restart
    restart(null) shouldBe None
    restart(false) shouldBe None
    restart(Map("_1" -> true)) shouldBe Some(MsgParams.DefaultRestart)
    val r = restart(Map("min_backoff" -> "100ms", "max_backoff" -> 2000, "random_factor" -> BigDecimal("0.5"),
      "max_restarts" -> 3, "max_restarts_within" -> "1 minute")).get
    (r.minBackoff, r.maxBackoff, r.randomFactor, r.maxRestarts, r.maxRestartsWithin) shouldBe
      ((100.millis, 2.seconds, 0.5, 3, 1.minute))
    val partial = restart(Map("max_restarts" -> 2)).get
    (partial.minBackoff, partial.maxBackoff, partial.maxRestarts, partial.maxRestartsWithin) shouldBe
      ((1.second, 30.seconds, 2, 1.second))
    err(restart(Map("min_backof" -> "1s"))) shouldBe "msg read parameter 'restart': unknown column 'min_backof', " +
      "allowed: min_backoff, max_backoff, random_factor, max_restarts, max_restarts_within"
    err(MsgReadParams.fromValues(null, "t", null, true, null, null, null, true)) shouldBe "msg read parameter " +
      "'restart': 'restart' and 'committer' are not supported together - committables of failed consumer cannot be committed, " +
      "use msg send 'restart' to restart reading, processing, sending and committing together"
    MsgSendParams.fromValues("t", null, null, null, Map("max_restarts" -> 1)).restart.map(_.maxRestarts) shouldBe Some(1)
  }

  behavior of "MsgSendParams"

  it should "evaluate parameters and create outgoing messages" in {
    val p = MsgSendParams.fromValues(
      to = Map("topic" -> "t", "partition" -> 1, "timestamp" -> 100L),
      key = Map("name" -> "id", "should_remove" -> true),
      headers = List(Map("name" -> "h1", "value" -> "v1"), Map("name" -> "h2", "value" -> 2), Map("name" -> "h3")),
      usingCodecs = Map("key" -> "json"),
    )
    p shouldBe MsgSendParams("t", Some(1), Some(100L), Some(MsgKey("id", None, shouldRemove = true)),
      List(Header("h1", "v1"), Header("h2", "2"), Header("h3", null)), "json", null)
    p.outMessage(Map("id" -> 1, "name" -> "x")) shouldBe
      OutMessage("t", 1, Map("name" -> "x"), Some(1), Some(100L), p.headers)

    val minimal = MsgSendParams.fromValues("t", null, null, null)
    minimal shouldBe MsgSendParams("t")
    minimal.outMessage("v") shouldBe OutMessage("t", null, "v")

    val keyValue = MsgSendParams.fromValues(Map("_1" -> "t"), Map("value" -> "k"), null, "avro")
    keyValue.valueCodec shouldBe "avro"
    keyValue.outMessage(Map("id" -> 1)) shouldBe OutMessage("t", "k", Map("id" -> 1))
    // key field retained, missing field gives null key
    val keyName = MsgSendParams.fromValues("t", Map("name" -> "id"), null, null)
    keyName.outMessage(Map("id" -> 1)) shouldBe OutMessage("t", 1, Map("id" -> 1))
    keyName.outMessage(Map("x" -> 1)) shouldBe OutMessage("t", null, Map("x" -> 1))
    err(keyName.outMessage("v")) shouldBe
      "msg send parameter 'key': key field 'id' requires map value, got java.lang.String: v"
  }

  it should "report invalid parameters" in {
    def send(to: Any = "t", key: Any = null, headers: Any = null) = MsgSendParams.fromValues(to, key, headers, null)
    err(send(to = null)) shouldBe "msg send parameter 'to': topic is required"
    err(send(to = Map("topic" -> "t", "partiton" -> 1))) shouldBe
      "msg send parameter 'to': unknown column 'partiton', allowed: topic, partition, timestamp"
    err(send(key = Map("should_remove" -> true))) shouldBe "msg send parameter 'key': 'name' or 'value' is required"
    err(send(key = Map("value" -> 1, "should_remove" -> true))) shouldBe
      "msg send parameter 'key': 'should_remove' requires 'name'"
    err(send(headers = Map("value" -> "v"))) shouldBe "msg send parameter 'headers': header name is required"
  }

  behavior of "MsgAdminParams"

  it should "evaluate arguments" in {
    MsgAdminParams.topics("topic partitions", List(Map("topic" -> "a"), Map("_1" -> "b"), Map("topic" -> "a"))) shouldBe
      List("a", "b")
    MsgAdminParams.topicPartitions("end offsets", List(Map("topic" -> "a", "partition" -> 0))) shouldBe
      Set(TopicPartition("a", 0))
    MsgAdminParams.groupPartitions("committed offsets", List(
      Map("group_id" -> "g", "topic" -> "a", "partition" -> 0),
      Map("group_id" -> "g", "topic" -> "a", "partition" -> 1),
      Map("group_id" -> "h", "topic" -> "a", "partition" -> 0),
    )) shouldBe Map("g" -> Set(TopicPartition("a", 0), TopicPartition("a", 1)), "h" -> Set(TopicPartition("a", 0)))
    err(MsgAdminParams.topicPartitions("end offsets", Map("topic" -> "a"))) shouldBe
      "msg end offsets parameter 'args': topic and partition are required"
    err(MsgAdminParams.groupPartitions("committed offsets", Map("topic" -> "a", "partition" -> 0))) shouldBe
      "msg committed offsets parameter 'args': group is required"
  }

  it should "convert call results to rows" in {
    val n1 = Node(1, "h1", 9092)
    val n2 = Node(2, "h2", 9093)
    MsgAdminParams.partitionRows(List(
      PartitionInfo("b", 0, None, Nil, Nil),
      PartitionInfo("a", 1, Some(n1), List(n1, n2), List(n1)),
      PartitionInfo("a", 0, Some(n2), List(n2), List(n2)),
    )) shouldBe List(
      Map("topic" -> "a", "partition" -> 0, "leader" -> Map("id" -> 2, "host" -> "h2", "port" -> 9093),
        "replicas" -> List(Map("id" -> 2, "host" -> "h2", "port" -> 9093)),
        "in_sync_replicas" -> List(Map("id" -> 2, "host" -> "h2", "port" -> 9093))),
      Map("topic" -> "a", "partition" -> 1, "leader" -> Map("id" -> 1, "host" -> "h1", "port" -> 9092),
        "replicas" -> List(Map("id" -> 1, "host" -> "h1", "port" -> 9092), Map("id" -> 2, "host" -> "h2", "port" -> 9093)),
        "in_sync_replicas" -> List(Map("id" -> 1, "host" -> "h1", "port" -> 9092))),
      Map("topic" -> "b", "partition" -> 0, "leader" -> null, "replicas" -> Nil, "in_sync_replicas" -> Nil),
    )
    MsgAdminParams.offsetRows(Map(TopicPartition("b", 0) -> 5L, TopicPartition("a", 1) -> 3L)) shouldBe List(
      Map("topic" -> "a", "partition" -> 1, "offset" -> 3L), Map("topic" -> "b", "partition" -> 0, "offset" -> 5L))
    MsgAdminParams.committedOffsetRows(Map(
      "h" -> Map(TopicPartition("a", 0) -> OffsetAndMetadata(1, null)),
      "g" -> Map(TopicPartition("a", 1) -> OffsetAndMetadata(2, "m"), TopicPartition("a", 0) -> OffsetAndMetadata(3, null)),
    )) shouldBe List(
      Map("group_id" -> "g", "topic" -> "a", "partition" -> 0, "offset" -> 3L, "metadata" -> null),
      Map("group_id" -> "g", "topic" -> "a", "partition" -> 1, "offset" -> 2L, "metadata" -> "m"),
      Map("group_id" -> "h", "topic" -> "a", "partition" -> 0, "offset" -> 1L, "metadata" -> null),
    )
  }
}
