package org.wabase

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source}
import org.apache.pekko.{Done, NotUsed}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers
import org.wabase.msg._

import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._

class TestMsgProvider(val name: String, val config: Config, val system: ActorSystem) extends MsgProvider {
  val sent      = new ConcurrentLinkedQueue[OutMessage]
  val committed = new ConcurrentLinkedQueue[Committable]
  @volatile var closed = false
  def createReader(config: Config, keyDecoder: MsgDecoder, valueDecoder: MsgDecoder): MsgReader = ???
  def createSender(config: Config): MsgSender = new MsgSender {
    def optionalFlow[P]: Flow[(Option[OutMessage], P), (Option[SendResult], P), NotUsed] =
      Flow[(Option[OutMessage], P)].zipWithIndex.map { case ((om, p), idx) =>
        (om.map { m =>
          sent.add(m)
          SendResult(m.topic, m.partition.getOrElse(0), idx, m.timestamp.getOrElse(0L))
        }, p)
      }
  }
  def createCommitter(config: Config): MsgCommitter = new MsgCommitter {
    def flow: Flow[Committable, Done, NotUsed] = Flow[Committable].map { c => committed.add(c); Done }
  }
  def admin: MsgAdmin = ???
  val codecs: MsgCodecs = new MsgCodecs(config)
  def close(): Future[Done] =
    if (name == "failing") Future.failed(new RuntimeException("close failed"))
    else { closed = true; Future.successful(Done) }
}

object TestMsgProvider {
  def create(system: ActorSystem, ec: ExecutionContext, config: Config, name: String): MsgProvider = {
    require(ec != null)
    new TestMsgProvider(name, config, system)
  }
}

case class TestCommittable(offset: Long) extends Committable

/** Codec with config, prefixes encoded string with configured prefix */
class TestPrefixMsgCodec(config: Config) extends MsgCodec {
  val prefix: String = config.getString("prefix")
  def encode(topic: String, headers: Seq[Header], data: Any): Array[Byte] = s"$prefix$data".getBytes(UTF_8)
  def decode(topic: String, headers: Seq[Header], data: Array[Byte]): Any =
    new String(data, UTF_8).stripPrefix(prefix)
}

object TestObjectMsgCodec extends MsgCodec {
  def encode(topic: String, headers: Seq[Header], data: Any): Array[Byte] = StringMsgCodec.encode(topic, headers, data)
  def decode(topic: String, headers: Seq[Header], data: Array[Byte]): Any = StringMsgCodec.decode(topic, headers, data)
}

class MsgProviderSpecs extends FlatSpec with Matchers with BeforeAndAfterAll {
  implicit val system: ActorSystem = ActorSystem("msg-provider-specs")
  override def afterAll(): Unit = Await.result(system.terminate(), 10.seconds)
  private def await[T](f: Future[T]): T = Await.result(f, 10.seconds)

  private val configs = MsgProviderConfig.configs // empty in reference.conf
  private def providerConfigs(names: String*): Map[String, Config] = names.map { n =>
    n -> ConfigFactory.parseString(
      s"""factory-class = org.wabase.TestMsgProvider
         |setting = $n-setting""".stripMargin)
  }.toMap

  behavior of "WabaseMsgProviders"

  it should "have no providers configured by default" in {
    configs shouldBe empty
  }

  it should "not treat common setting objects as providers" in {
    val root = ConfigFactory.parseString(
      """factory-class = org.wabase.TestMsgProvider
        |common-setting-objects = [codecs, consumer]
        |codecs.c.factory-class = org.wabase.TestObjectMsgCodec
        |consumer {}
        |main {}
        |other {}
        |""".stripMargin)
    MsgProviderConfig.providerConfigs(ComponentConfs(root, Map(
      "codecs" -> root.getConfig("codecs"), "consumer" -> root.getConfig("consumer"),
      "main" -> root, "other" -> root))).keySet shouldBe Set("main", "other")
  }

  it should "create providers with factory function parameters" in {
    val providers = WabaseMsgProviders(providerConfigs("main", "other"), system)
    val main = providers.provider(null).asInstanceOf[TestMsgProvider]
    main.name shouldBe "main"
    main.config.getString("setting") shouldBe "main-setting"
    main.system shouldBe system
    providers.provider("other").asInstanceOf[TestMsgProvider].name shouldBe "other"
    intercept[RuntimeException](providers.provider("missing"))
      .getMessage shouldBe "Messaging provider 'missing' not found. Available names: [main, other]"
  }

  it should "report factory failure" in {
    val ex = intercept[RuntimeException](WabaseMsgProviders(Map("main" -> ConfigFactory.parseString(
      "factory-class = org.wabase.NoSuchMsgProvider")), system))
    ex.getMessage should startWith ("Failed to create messaging provider 'main' with org.wabase.NoSuchMsgProvider.create")
  }

  it should "close all providers and report failures" in {
    // providers without coordinated shutdown task - closed by test, failing one would fail again on system terminate
    val providers = WabaseMsgProviders(providerConfigs("main", "failing", "other").map { case (n, c) =>
      n -> WabaseMsgProviders.createProvider(n, c, system)
    })
    val ex = intercept[RuntimeException](await(providers.close()))
    ex.getMessage shouldBe "Failed to close messaging provider(s): failing"
    providers.provider("main").asInstanceOf[TestMsgProvider].closed shouldBe true
    providers.provider("other").asInstanceOf[TestMsgProvider].closed shouldBe true
  }

  behavior of "MsgSender"

  it should "send single message" in {
    val p = TestMsgProvider.create(system, system.dispatcher, ConfigFactory.empty, "main")
    await(p.createSender(ConfigFactory.empty).send(OutMessage("t", null, "v".getBytes(UTF_8), Some(2), Some(5L)))) shouldBe
      SendResult("t", 2, 0, 5)
  }

  it should "send messages to sink and count them" in {
    val p = TestMsgProvider.create(system, system.dispatcher, ConfigFactory.empty, "main").asInstanceOf[TestMsgProvider]
    await(Source(1 to 3).map(i => OutMessage("t", null, i)).runWith(p.createSender(ConfigFactory.empty).sink)) shouldBe 3
    p.sent.asScala.map(_.value).toList shouldBe List(1, 2, 3)
  }

  it should "commit after messages are sent" in {
    val p = TestMsgProvider.create(system, system.dispatcher, ConfigFactory.empty, "main").asInstanceOf[TestMsgProvider]
    val sink = p.createSender(ConfigFactory.empty).committableSink(p.createCommitter(ConfigFactory.empty))
    await(Source(1 to 4)
      .map(i => CommittableMessage(if (i == 3) null else OutMessage("t", null, i), TestCommittable(i.toLong)))
      .runWith(sink)) shouldBe 3 // null message is not sent, but committed
    p.sent.asScala.map(_.value).toList shouldBe List(1, 2, 4)
    p.committed.asScala.toList shouldBe (1 to 4).map(TestCommittable(_)).toList
  }

  behavior of "MsgConversions"

  it should "decode incoming message" in {
    val m = MsgConversions.inMessage("t", 1, 2, 3, "k".getBytes(UTF_8), """{"a":1}""".getBytes(UTF_8),
      List("h1" -> "v1".getBytes(UTF_8), "h2" -> null), StringMsgCodec, JsonMsgCodec)
    m shouldBe InMessage("t", "k", Map("a" -> 1), 1, 2, List(Header("h1", "v1"), Header("h2", null)), 3)
    MsgConversions.inMessage("t", 1, 2, 3, null, null, Nil, StringMsgCodec, JsonMsgCodec) shouldBe
      InMessage("t", null, null, 1, 2, Nil, 3)
  }

  it should "decode header values as strings if valid UTF-8, otherwise keep bytes" in {
    import MsgConversions.headerValue
    Option(headerValue(null)) shouldBe None
    headerValue(Array.emptyByteArray) shouldBe ""
    headerValue("vērtība €".getBytes(UTF_8)) shouldBe "vērtība €"
    headerValue(Array[Byte](0, 0, 0, 1)) shouldBe "\u0000\u0000\u0000\u0001" // valid UTF-8, lossless
    headerValue(Array[Byte](0, 0, 0, 1)).asInstanceOf[String].getBytes(UTF_8).toList shouldBe List[Byte](0, 0, 0, 1)
    val binary = Array[Byte](-1, -2, 0, 65) // 0xFF is never valid in UTF-8
    headerValue(binary).isInstanceOf[Array[Byte]] shouldBe true
    headerValue(binary).asInstanceOf[Array[Byte]].toList shouldBe binary.toList
    headerValue(Array[Byte](-61)).isInstanceOf[Array[Byte]] shouldBe true // truncated multi-byte sequence
    val m = MsgConversions.inMessage("t", 0, 0, 0, null, null, List("b" -> binary), StringMsgCodec, StringMsgCodec)
    m.headers.map(h => h.name -> h.value.asInstanceOf[Array[Byte]].toList) shouldBe List("b" -> binary.toList)
    // binary values are base64 in json (e.g. server sent events, collected results)
    ResultEncoder.encodeAnyToJsonString(m.metadata("headers")) shouldBe """[{"name":"b","value":"//4AQQ=="}]"""
  }

  it should "report decoding failure with message position" in {
    val ex = intercept[MsgDecodingException](MsgConversions.inMessage("t", 1, 2, 3, null, "{".getBytes(UTF_8),
      Nil, StringMsgCodec, JsonMsgCodec))
    (ex.topic, ex.partition, ex.offset) shouldBe (("t", 1, 2L))
    ex.getMessage should startWith ("Failed to decode message value at t-1@2: ")
  }

  it should "encode outgoing message" in {
    val e = MsgConversions.encode(OutMessage("t", 1, Map("a" -> 1),
      headers = List(Header("h1", "v1"), Header("h2", Array[Byte](1)), Header("h3", null))), StringMsgCodec, JsonMsgCodec)
    new String(e.key, UTF_8) shouldBe "1"
    new String(e.value, UTF_8) shouldBe """{"a":1}"""
    e.headers.map { case (n, v) => n -> Option(v).map(_.toList).orNull } shouldBe
      List("h1" -> "v1".getBytes(UTF_8).toList, "h2" -> List[Byte](1), "h3" -> null)
    intercept[MsgEncodingException](MsgConversions.encode(OutMessage("t", null, null,
      headers = List(Header("h", 1))), StringMsgCodec, JsonMsgCodec))
      .getMessage shouldBe "Failed to encode message header 'h' for topic t: String or Array[Byte] expected, got: java.lang.Integer"
  }

  behavior of "MsgCodec"

  it should "encode and decode json, cbor" in {
    val value = Map("id" -> 1, "name" -> "n", "items" -> List(Map("x" -> true)), "empty" -> null)
    List(JsonMsgCodec, CborMsgCodec) foreach { c =>
      c.decode("t", Nil, c.encode("t", Nil, value)) shouldBe value
      c.encode("t", Nil, null) shouldBe null
      c.decode("t", Nil, null) shouldBe (null: Any)
    }
    new String(JsonMsgCodec.encode("t", Nil, value), UTF_8) shouldBe
      """{"id":1,"name":"n","items":[{"x":true}],"empty":null}"""
  }

  it should "encode and decode string, bytes" in {
    new String(StringMsgCodec.encode("t", Nil, 42), UTF_8) shouldBe "42"
    StringMsgCodec.decode("t", Nil, "key".getBytes(UTF_8)) shouldBe "key"
    val bytes = Array[Byte](1, 2)
    BytesMsgCodec.encode("t", Nil, bytes) shouldBe bytes
    BytesMsgCodec.decode("t", Nil, bytes) shouldBe bytes
    intercept[RuntimeException](BytesMsgCodec.encode("t", Nil, "x"))
  }

  it should "find codec by name" in {
    MsgCodec("json") shouldBe JsonMsgCodec
    MsgCodec.defaultKeyCodec shouldBe StringMsgCodec
    intercept[RuntimeException](MsgCodec("xml"))
      .getMessage shouldBe "Message codec 'xml' not found. Available codecs: [bytes, cbor, json, string]"
  }

  behavior of "MsgCodecs"

  private val codecsConfig = ConfigFactory.parseString(
    """codecs {
      |  prefixed { factory-class = org.wabase.TestPrefixMsgCodec, prefix = "p:" }
      |  obj.factory-class  = org.wabase.TestObjectMsgCodec
      |  json.factory-class = org.wabase.TestObjectMsgCodec # overrides built-in
      |}
      |""".stripMargin)

  it should "find configured and built-in codecs by name" in {
    val codecs = new MsgCodecs(codecsConfig)
    codecs("prefixed").asInstanceOf[TestPrefixMsgCodec].prefix shouldBe "p:"
    codecs("obj") shouldBe TestObjectMsgCodec
    codecs("json") shouldBe TestObjectMsgCodec
    codecs("cbor") shouldBe CborMsgCodec
    codecs("prefixed") should be theSameInstanceAs codecs("prefixed")
    intercept[RuntimeException](codecs("xml")).getMessage shouldBe
      "Message codec 'xml' not found. Available codecs: [bytes, cbor, json, obj, prefixed, string]"
    new MsgCodecs(null)("json") shouldBe JsonMsgCodec
  }

  it should "report codec creation failure" in {
    intercept[RuntimeException](new MsgCodecs(ConfigFactory.parseString(
      "codecs.bad.factory-class = org.wabase.NoSuchCodec"))).getMessage should startWith (
      "Failed to get message codec 'bad' instance, please configure factory-class properly")
  }

  it should "resolve codec from explicit value, config name or object, or default" in {
    val codecs = new MsgCodecs(codecsConfig)
    val cfg = ConfigFactory.parseString(
      """key-codec = prefixed
        |value-codec { factory-class = org.wabase.TestPrefixMsgCodec, prefix = "v:" }
        |""".stripMargin)
    codecs.keyDecoder(BytesMsgCodec, cfg) shouldBe BytesMsgCodec
    codecs.keyDecoder(null, cfg) shouldBe codecs("prefixed")
    codecs.keyEncoder(cfg) shouldBe codecs("prefixed")
    val inline = codecs.valueDecoder(null, cfg).asInstanceOf[TestPrefixMsgCodec]
    inline.prefix shouldBe "v:"
    codecs.valueEncoder(ConfigFactory.parseString(cfg.root.render)) should be theSameInstanceAs inline // cached
    codecs.valueEncoder(ConfigFactory.parseString(
      "value-codec { factory-class = org.wabase.TestPrefixMsgCodec, prefix = \"w:\" }"))
      .asInstanceOf[TestPrefixMsgCodec].prefix shouldBe "w:"
    codecs.keyDecoder(null, ConfigFactory.empty) shouldBe MsgCodec.defaultKeyCodec
    codecs.valueDecoder(null, null) shouldBe MsgCodec.defaultValueCodec
    codecs.valueEncoder(ConfigFactory.empty) shouldBe MsgCodec.defaultValueCodec
    intercept[RuntimeException](codecs.valueEncoder(ConfigFactory.parseString("value-codec = xml")))
      .getMessage should startWith ("Message codec 'xml' not found")
    intercept[RuntimeException](codecs.valueEncoder(ConfigFactory.parseString("value-codec = [x]")))
      .getMessage shouldBe "Message codec name or object expected at 'value-codec', got: LIST"
  }
}
