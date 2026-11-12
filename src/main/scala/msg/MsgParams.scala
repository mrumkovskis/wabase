package org.wabase.msg

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.pekko.stream.RestartSettings

import scala.collection.immutable.ListMap
import scala.concurrent.duration.{DurationInt, DurationLong, FiniteDuration}
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/** Conversion of evaluated msg operation parameters (tresql results) to provider inputs -
  * subscriptions, operation configs, codec names, outgoing messages.
  *
  * Parameter value is evaluated tresql result: rows (`Seq[Map[String, Any]]`), single row (`Map[String, Any]`),
  * scalar or `null`. Row columns are named by tresql column aliases, for example `{'t' topic, 0 partition}`.
  * Scalar or single unnamed column row (tresql names unaliased columns `_1`, `_2`, ...) is a shorthand where
  * parameter supports it (for example `using='avro'` - value codec, `to='t'` - topic).
  * Unknown column is an error naming operation, parameter and allowed columns.
  */
object MsgParams {
  type Row = Map[String, Any]

  /** Column name of scalar parameter value */
  val ScalarColumn = ""

  val RestartColumns = Seq("min_backoff", "max_backoff", "random_factor", "max_restarts", "max_restarts_within")
  /** Restart defaults - min backoff 1s, max backoff 30s, random factor 0.2, unlimited restarts */
  val DefaultRestart: RestartSettings = RestartSettings(1.second, 30.seconds, 0.2)

  private val UnnamedColumnRegex = "_\\d+".r
  /** Scalar value column or tresql unaliased column (`_1`, `_2`, ...) */
  def isUnnamed(column: String): Boolean =
    column == ScalarColumn || UnnamedColumnRegex.pattern.matcher(column).matches

  class MsgParamException(message: String, cause: Throwable = null) extends IllegalArgumentException(message, cause)

  private[msg] case class Param(op: String, name: String) {
    def error(msg: String, cause: Throwable = null): Nothing =
      throw new MsgParamException(s"msg $op parameter '$name': $msg", cause)

    def rows(value: Any): Seq[Row] = value match {
      case null => Nil
      case m: Map[String@unchecked, _] => List(m)
      case m: java.util.Map[String@unchecked, _] => List(m.asScala.toMap)
      case s: Iterable[_] => s.toList.map {
        case m: Map[String@unchecked, _] => m
        case m: java.util.Map[String@unchecked, _] => m.asScala.toMap
        case x => Map(ScalarColumn -> x)
      }
      case x => List(Map(ScalarColumn -> x))
    }
    /** Single row or null, error if more than one row */
    def row(value: Any): Row = rows(value) match {
      case Nil => null
      case r :: Nil => r
      case rs => error(s"single row expected, got ${rs.size} rows")
    }
    /** Single value - scalar or single column of single row, null if no rows */
    def single(value: Any): Any = row(value) match {
      case null => null
      case r if r.size == 1 => r.head._2
      case r => error(s"single value expected, got columns: ${r.keys.mkString(", ")}")
    }
    /** Checks columns, single unnamed column is renamed to `shorthand` column if specified */
    def columns(row: Row, allowed: Seq[String], shorthand: String = null): Row = {
      val r =
        if (shorthand != null && row.size == 1 && isUnnamed(row.head._1)) Map(shorthand -> row.head._2)
        else row
      val unknown = r.keys.filterNot(allowed.contains)
      if (unknown.nonEmpty) error(
        s"unknown column${if (unknown.size > 1) "s" else ""} ${unknown.map(c => s"'$c'").mkString(", ")}, " +
          s"allowed: ${allowed.mkString(", ")}")
      r
    }

    def string(col: String, v: Any): String = v match {
      case null => null
      case s: String => s
      case x => typeError(col, "string", x)
    }
    def boolean(col: String, v: Any): java.lang.Boolean = v match {
      case null => null
      case b: Boolean => b
      case b: java.lang.Boolean => b
      case x => typeError(col, "boolean", x)
    }
    def double(col: String, v: Any): java.lang.Double = v match {
      case null => null
      case n: java.lang.Number => n.doubleValue
      case x => typeError(col, "number", x)
    }
    def long(col: String, v: Any): java.lang.Long = v match {
      case null => null
      case n: java.lang.Number if isWhole(n) => n.longValue
      case x => typeError(col, "integer", x)
    }
    def int(col: String, v: Any): java.lang.Integer = long(col, v) match {
      case null => null
      case l if l >= Int.MinValue && l <= Int.MaxValue => l.toInt
      case l => typeError(col, "int", l)
    }
    /** Epoch millis from number or date / time value, local date time in system default time zone */
    def epochMillis(col: String, v: Any): java.lang.Long = v match {
      case null => null
      case n: java.lang.Number => long(col, n)
      case d: java.util.Date => d.getTime
      case i: java.time.Instant => i.toEpochMilli
      case d: java.time.OffsetDateTime => d.toInstant.toEpochMilli
      case d: java.time.ZonedDateTime => d.toInstant.toEpochMilli
      case d: java.time.LocalDateTime => d.atZone(java.time.ZoneId.systemDefault).toInstant.toEpochMilli
      case x => typeError(col, "timestamp", x)
    }
    /** Duration from string (HOCON duration format, e.g. `5s`, `100 millis`), number - millis or java Duration */
    def duration(col: String, v: Any): FiniteDuration = v match {
      case null => null
      case d: java.time.Duration => d.toNanos.nanos
      case d: FiniteDuration => d
      case x@(_: String | _: java.lang.Number) =>
        try ConfigFactory.parseMap(Map("d" -> configValue(x)).asJava).getDuration("d").toNanos.nanos
        catch { case NonFatal(e) => error(s"invalid duration in column '$col': $x", e) }
      case x => typeError(col, "duration", x)
    }

    private def typeError(col: String, expected: String, v: Any) = error(
      (if (col == ScalarColumn) "" else s"column '$col' ") +
        s"$expected expected, got ${v.getClass.getName}: $v")
  }

  private def isWhole(n: java.lang.Number): Boolean = n match {
    case _: java.lang.Integer | _: java.lang.Long | _: java.lang.Short | _: java.lang.Byte => true
    case b: BigInt => b.isValidLong
    case b: java.math.BigInteger => b.bitLength < 64
    case b: BigDecimal => b.isWhole && b.isValidLong
    case b: java.math.BigDecimal => isWhole(BigDecimal(b))
    case d => val x = d.doubleValue; x == Math.rint(x) && !x.isInfinite
  }

  /** Converts value to type supported by config, whole numbers to Long */
  private[msg] def configValue(v: Any): AnyRef = v match {
    case n: java.lang.Number if isWhole(n) => java.lang.Long.valueOf(n.longValue)
    case n: java.lang.Number => java.lang.Double.valueOf(n.doubleValue)
    case x => x.asInstanceOf[AnyRef]
  }

  /** Config from row, column names with '_' are converted to config keys with '-', null values are omitted. */
  private[msg] def config(path: String, row: Row): Config = {
    val values = row.collect { case (k, v) if v != null => k.replace('_', '-') -> configValue(v) }
    ConfigFactory.parseMap(Map(path -> values.asJava).asJava)
  }

  /** Restart settings from `restart` parameter - row with [[RestartColumns]] (missing - [[DefaultRestart]]) or boolean -
    * restart with default settings, `null` or `false` - no restart
    */
  private[msg] def restartParam(op: String, restart: Any): Option[RestartSettings] = {
    val p = Param(op, "restart")
    p.row(restart) match {
      case null => None
      case r if r.size == 1 && isUnnamed(r.head._1) => // boolean shorthand
        if (p.boolean(ScalarColumn, r.head._2) == java.lang.Boolean.TRUE) Some(DefaultRestart) else None
      case r =>
        val row = p.columns(r, RestartColumns)
        def dur(c: String) = row.get(c).map(p.duration(c, _)).flatMap(Option(_))
        val d = DefaultRestart
        val s = RestartSettings(
          dur("min_backoff").getOrElse(d.minBackoff),
          dur("max_backoff").getOrElse(d.maxBackoff),
          row.get("random_factor").map(p.double("random_factor", _)).flatMap(Option(_)).map(_.doubleValue)
            .getOrElse(d.randomFactor),
        )
        Some(row.get("max_restarts").map(p.int("max_restarts", _)).flatMap(Option(_)) match {
          case Some(n) => s.withMaxRestarts(n, dur("max_restarts_within").getOrElse(s.minBackoff))
          case None => s
        })
    }
  }

  /** Codec names from `using` parameter, see [[MsgCodecs]].
    * @return (key codec, value codec), `null` - not specified, codec is resolved from configuration
    */
  private[msg] def codecNames(op: String, usingCodecs: Any): (String, String) = {
    val p = Param(op, "using")
    p.row(usingCodecs) match {
      case null => (null, null)
      case r =>
        val row = p.columns(r, Seq("key", "value"), shorthand = "value")
        (p.string("key", row.getOrElse("key", null)), p.string("value", row.getOrElse("value", null)))
    }
  }
}

import MsgParams._

/** Evaluated msg read parameters, evaluated once per operation (`to`, `key`, `commit_metadata` are evaluated per
  * message, by operation implementation).
  *
  * @param committer   committer config (path `committer`), `None` - messages are not committed
  * @param keyCodec    key codec name or null - resolved from configuration
  * @param valueCodec  value codec name or null - resolved from configuration
  * @param keyName     name of message bind variable for message key or null
  * @param restart     restart on failure with backoff - of reader source, with `committer` - of reading, processing
  *                    and committing (reader and commit failures), `None` - no restart
  */
case class MsgReadParams(
  subscription: Subscription,
  within: Option[FiniteDuration] = None,
  idle: Option[FiniteDuration] = None,
  committer: Option[Config] = None,
  keyCodec: String = null,
  valueCodec: String = null,
  atMostOnce: Boolean = false,
  keyName: String = null,
  restart: Option[RestartSettings] = None,
  decodeError: Option[String] = None,
) {
  /** Reader config for [[MsgProvider.createReader]].
    * Consumers of committable sources in msg operations are stopped only after stream completes and all commits
    * are done (see [[MsgStreams.termination]], [[MsgStreams.run]]), so stop timeout is 0.
    */
  def readerConfig: Config = ConfigFactory.parseMap((
    committer.map(_ => MsgProvider.StopTimeoutPath -> "0s").toList ++
      decodeError.map(MsgProvider.DecodeErrorPath -> _)).toMap.asJava)
  /** Committer config for [[MsgProvider.createCommitter]] */
  def committerConfig: Config = committer getOrElse ConfigFactory.empty
}

object MsgReadParams {
  val Op = "read"
  val FromColumns = Seq("topic", "partition", "offset", "timestamp", "pattern")
  val TimeoutColumns = Seq("within", "idle")
  /** `commit_when` maps to config key `when` (SQL reserved word as column alias) */
  val CommitterColumns = Seq("delivery", "max_batch", "max_interval", "parallelism", "commit_when")
  val CommitterPath = "committer"

  /** @param group       group name or null - from provider configuration
    * @param from        rows, see [[subscription]]
    * @param timeout     row - `within`, `idle` durations
    * @param committer   row - committer settings ([[CommitterColumns]]) or boolean - commit with configured settings,
    *                    `null` or `false` - messages are not committed
    * @param key         name of message bind variable for message key
    * @param usingCodecs row - `key`, `value` codec names or value codec name
    * @param atMostOnce  boolean, `null` - false
    * @param restart     restart settings, see [[MsgParams.restartParam]]
    * @param decodeError `'fail'` or `'skip'` - see [[MsgProvider.DecodeErrorPath]], `null` - provider configuration
    */
  def fromValues(
    group: Any,
    from: Any,
    timeout: Any,
    committer: Any,
    key: Any,
    usingCodecs: Any,
    atMostOnce: Any,
    restart: Any = null,
    decodeError: Any = null,
  ): MsgReadParams = {
    val gp = Param(Op, "group")
    val g = Option(gp.string(ScalarColumn, gp.single(group)))
    val to = Param(Op, "timeout")
    val (within, idle) = to.row(timeout) match {
      case null => (None, None)
      case r =>
        val row = to.columns(r, TimeoutColumns)
        (row.get("within").map(to.duration("within", _)).flatMap(Option(_)),
          row.get("idle").map(to.duration("idle", _)).flatMap(Option(_)))
    }
    val cp = Param(Op, "committer")
    val committerConfig = cp.row(committer) match {
      case null => None
      case r if r.size == 1 && isUnnamed(r.head._1) => // boolean shorthand
        if (cp.boolean(ScalarColumn, r.head._2) == java.lang.Boolean.TRUE) Some(ConfigFactory.empty) else None
      case r => Some(MsgParams.config(CommitterPath, cp.columns(r, CommitterColumns).map {
        case ("commit_when", v) => ("when", v)
        case kv => kv
      }))
    }
    val amp = Param(Op, "at_most_once")
    val amo = amp.boolean(ScalarColumn, amp.single(atMostOnce)) == java.lang.Boolean.TRUE
    if (amo && committerConfig.isDefined)
      amp.error("'committer' and 'at_most_once' are mutually exclusive")
    val kp = Param(Op, "key")
    val keyName = kp.string(ScalarColumn, kp.single(key))
    val (kc, vc) = codecNames(Op, usingCodecs)
    val dp = Param(Op, "decode_error")
    val de = Option(dp.string(ScalarColumn, dp.single(decodeError)))
      .map(v => MsgProvider.decodeErrorSkip(v).fold[String](dp.error(_), _ => v))
    MsgReadParams(subscription(from, g), within, idle, committerConfig, kc, vc, amo, keyName,
      restartParam(Op, restart), de)
  }


  /** Result of `to` evaluation for a message - boolean, `null` is false */
  def isLast(to: Any): Boolean = {
    val p = Param(Op, "to")
    p.single(to) match {
      case null => false
      case b: Boolean => b
      case b: java.lang.Boolean => b
      case x => p.error(s"boolean expected, got ${x.getClass.getName}: $x " +
        "(comparison must be enclosed in braces, for example to={:value = 'end'})")
    }
  }

  /** Result of `commit_metadata` evaluation for a message - string or null */
  def commitMetadata(metadata: Any): String = {
    val p = Param(Op, "commit_metadata")
    p.string(ScalarColumn, p.single(metadata))
  }

  /** Subscription from `from` rows, all rows must be of the same kind, kind is determined by non null columns:
    *  - `pattern` - [[Subscription.TopicPattern]], several patterns are joined into alternation
    *  - `topic` - [[Subscription.Topics]]
    *  - `topic, partition` - [[Subscription.Assignment]]
    *  - `topic, partition, offset` - [[Subscription.AssignmentWithOffset]]
    *  - `topic, partition, timestamp` - [[Subscription.AssignmentOffsetsForTimes]]
    * Scalar or single column row is a shorthand of topic.
    */
  def subscription(from: Any, group: Option[String]): Subscription = {
    val p = Param(Op, "from")
    val rows = p.rows(from).map(r => p.columns(r, FromColumns, shorthand = "topic").filter(_._2 != null))
    if (rows.isEmpty) p.error("at least one row expected")
    val kinds = rows.map(_.keySet)
    kinds.zipWithIndex.find(_._1 != kinds.head).foreach { case (k, i) =>
      p.error(s"all rows must have the same columns, row 1 has (${kinds.head.mkString(", ")}), " +
        s"row ${i + 1} has (${k.mkString(", ")})")
    }
    def topic(r: Row) = p.string("topic", r("topic"))
    def tp(r: Row) = TopicPartition(topic(r), p.int("partition", r("partition")))
    def unique[K, V](pairs: Seq[(K, V)], what: String): Map[K, V] = {
      val dups = pairs.groupBy(_._1).collect { case (k, vs) if vs.size > 1 => k }
      if (dups.nonEmpty) p.error(s"duplicate $what: ${dups.mkString(", ")}")
      pairs.toMap
    }
    kinds.head match {
      case k if k == Set("pattern") =>
        val patterns = rows.map(r => p.string("pattern", r("pattern")))
        Subscription.TopicPattern(
          if (patterns.size == 1) patterns.head else patterns.map(pt => s"(?:$pt)").mkString("|"), group)
      case k if k == Set("topic") =>
        Subscription.Topics(rows.map(topic).toSet, group)
      case k if k == Set("topic", "partition") =>
        Subscription.Assignment(unique(rows.map(r => tp(r) -> (())), "partitions").keySet, group)
      case k if k == Set("topic", "partition", "offset") =>
        Subscription.AssignmentWithOffset(
          unique(rows.map(r => tp(r) -> p.long("offset", r("offset")).longValue), "partitions"), group)
      case k if k == Set("topic", "partition", "timestamp") =>
        Subscription.AssignmentOffsetsForTimes(
          unique(rows.map(r => tp(r) -> p.epochMillis("timestamp", r("timestamp")).longValue), "partitions"), group)
      case k => p.error(s"unsupported column combination (${k.mkString(", ")}), expected one of: (pattern), " +
        "(topic), (topic, partition), (topic, partition, offset), (topic, partition, timestamp)")
    }
  }
}

/** Key of outgoing message.
  * @param name          name of value (map) field containing key, used if `value` is not specified
  * @param value         key value, `None` - key is taken from value field `name`
  * @param shouldRemove  remove field `name` from value (map)
  */
case class MsgKey(name: String, value: Option[Any], shouldRemove: Boolean)

/** Evaluated msg send parameters, evaluated for each outgoing element.
  * @param keyCodec    key codec name or null - resolved from configuration
  * @param valueCodec  value codec name or null - resolved from configuration
  */
case class MsgSendParams(
  topic: String,
  partition: Option[Int] = None,
  timestamp: Option[Long] = None,
  key: Option[MsgKey] = None,
  headers: Seq[Header] = Nil,
  keyCodec: String = null,
  valueCodec: String = null,
) {
  /** Outgoing message with value, key from [[key]], without encoders */
  def outMessage(value: Any): OutMessage = {
    val (k, v) = key match {
      case None => (null, value)
      case Some(MsgKey(name, kv, remove)) =>
        val m = value match {
          case m: Map[String@unchecked, _] => m
          case _ => null
        }
        val keyValue = kv getOrElse {
          if (m == null) throw new MsgParamException(
            s"msg send parameter 'key': key field '$name' requires map value, got " +
              s"${Option(value).map(_.getClass.getName).orNull}: $value")
          m.getOrElse(name, null)
        }
        (keyValue, if (remove && m != null) m - name else value)
    }
    OutMessage(topic, k, v, partition, timestamp, headers)
  }
}

object MsgSendParams {
  val Op = "send"
  val ToColumns = Seq("topic", "partition", "timestamp")
  val KeyColumns = Seq("name", "value", "should_remove")
  val HeaderColumns = Seq("name", "value")

  /** @param to       row - `topic`, `partition`, `timestamp` or topic name
    * @param key      row - `name`, `value`, `should_remove`, see [[MsgKey]]
    * @param headers  rows - `name`, `value`, value must be string, bytes or null, other values are converted to string
    * @param usingCodecs row - `key`, `value` codec names or value codec name
    */
  def fromValues(to: Any, key: Any, headers: Any, usingCodecs: Any): MsgSendParams = {
    val tp = Param(Op, "to")
    val toRow = Option(tp.row(to)).map(tp.columns(_, ToColumns, shorthand = "topic")).getOrElse(Map.empty[String, Any])
    val topic = tp.string("topic", toRow.getOrElse("topic", null))
    if (topic == null) tp.error("topic is required")
    val kp = Param(Op, "key")
    val msgKey = Option(kp.row(key)).map(kp.columns(_, KeyColumns)).map { r =>
      val name = kp.string("name", r.getOrElse("name", null))
      val value = r.get("value").filter(_ != null)
      val remove = kp.boolean("should_remove", r.getOrElse("should_remove", null)) == java.lang.Boolean.TRUE
      if (name == null && value.isEmpty) kp.error("'name' or 'value' is required")
      if (remove && name == null) kp.error("'should_remove' requires 'name'")
      MsgKey(name, value, remove)
    }
    val hp = Param(Op, "headers")
    val hs = hp.rows(headers).map(hp.columns(_, HeaderColumns)).map { r =>
      val name = hp.string("name", r.getOrElse("name", null))
      if (name == null) hp.error("header name is required")
      Header(name, r.getOrElse("value", null) match {
        case null => null
        case v@(_: String | _: Array[Byte]) => v
        case v => v.toString
      })
    }
    val (kc, vc) = codecNames(Op, usingCodecs)
    MsgSendParams(
      topic,
      Option(tp.int("partition", toRow.getOrElse("partition", null))).map(_.intValue),
      Option(tp.epochMillis("timestamp", toRow.getOrElse("timestamp", null))).map(_.longValue),
      msgKey, hs, kc, vc,
    )
  }

  /** msg send `restart` parameter, evaluated once per operation */
  def sendRestart(restart: Any): Option[RestartSettings] = restartParam(Op, restart)

  /** msg multi send `restart` parameter, see [[sendRestart]] */
  def multiSendRestart(restart: Any): Option[RestartSettings] = restartParam("multi send", restart)

  /** Result of `filter` evaluation for an outgoing element - boolean, `null` is false (element is not sent) */
  def isSent(filter: Any): Boolean = {
    val p = Param(Op, "filter")
    p.single(filter) match {
      case null => false
      case b: Boolean => b
      case b: java.lang.Boolean => b
      case x => p.error(s"boolean expected, got ${x.getClass.getName}: $x " +
        "(comparison must be enclosed in braces, for example filter={:status != 'draft'})")
    }
  }
}

/** Evaluated msg admin call arguments and call results as rows, see [[MsgAdmin]]. */
object MsgAdminParams {
  /** Topic names from rows - `topic` column or single column */
  def topics(op: String, args: Any): Seq[String] = {
    val p = Param(op, "args")
    p.rows(args).map(p.columns(_, Seq("topic"), shorthand = "topic")).map(r => p.string("topic", r("topic")))
      .filter(_ != null).distinct
  }
  /** Partitions from rows - `topic`, `partition` columns */
  def topicPartitions(op: String, args: Any): Set[TopicPartition] = {
    val p = Param(op, "args")
    p.rows(args).map(p.columns(_, Seq("topic", "partition"))).map(tp(p, _)).toSet
  }
  /** Partitions by group from rows - `group_id`, `topic`, `partition` columns */
  def groupPartitions(op: String, args: Any): Map[String, Set[TopicPartition]] = {
    val p = Param(op, "args")
    p.rows(args).map(p.columns(_, Seq("group_id", "topic", "partition"))).map { r =>
      val group = p.string("group_id", r.getOrElse("group_id", null))
      if (group == null) p.error("group is required")
      group -> tp(p, r)
    }.groupBy(_._1).map { case (g, gps) => g -> gps.map(_._2).toSet }
  }

  /** Partition rows - `topic`, `partition`, `leader`, `replicas`, `in_sync_replicas`, sorted by topic, partition.
    * Nodes are maps - `id`, `host`, `port`, leader is null if partition has no leader.
    */
  def partitionRows(partitions: Iterable[PartitionInfo]): List[Row] =
    partitions.toList.sortBy(p => (p.topic, p.partition)).map { p =>
      ListMap(
        "topic"            -> p.topic,
        "partition"        -> p.partition,
        "leader"           -> p.leader.map(nodeRow).orNull,
        "replicas"         -> p.replicas.map(nodeRow).toList,
        "in_sync_replicas" -> p.inSyncReplicas.map(nodeRow).toList,
      )
    }
  /** Offset rows - `topic`, `partition`, `offset`, sorted by topic, partition */
  def offsetRows(offsets: Map[TopicPartition, Long]): List[Row] =
    offsets.toList.sortBy { case (tp, _) => (tp.topic, tp.partition) }.map { case (tp, o) =>
      ListMap("topic" -> tp.topic, "partition" -> tp.partition, "offset" -> o)
    }
  /** Committed offset rows - `group_id`, `topic`, `partition`, `offset`, `metadata`, sorted by group, topic, partition */
  def committedOffsetRows(offsets: Map[String, Map[TopicPartition, OffsetAndMetadata]]): List[Row] =
    offsets.toList.flatMap { case (g, os) => os.map { case (tp, om) => (g, tp, om) } }
      .sortBy { case (g, tp, _) => (g, tp.topic, tp.partition) }
      .map { case (g, tp, om) =>
        ListMap("group_id" -> g, "topic" -> tp.topic, "partition" -> tp.partition, "offset" -> om.offset,
          "metadata" -> om.metadata)
      }
  private def nodeRow(n: Node): Row = ListMap("id" -> n.id, "host" -> n.host, "port" -> n.port)

  private def tp(p: Param, r: Row) = {
    val topic = p.string("topic", r.getOrElse("topic", null))
    val partition = p.int("partition", r.getOrElse("partition", null))
    if (topic == null || partition == null) p.error("topic and partition are required")
    TopicPartition(topic, partition)
  }
}
