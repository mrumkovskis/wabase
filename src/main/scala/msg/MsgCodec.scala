package org.wabase.msg

import com.typesafe.config.{Config, ConfigValue, ConfigValueType}
import io.bullet.borer.{Cbor, Json}
import org.apache.pekko.util.ByteString
import org.wabase.{CborOrJsonAnyValueDecoder, ResultEncoder, getObjectOrNewInstance}

import java.nio.ByteBuffer
import java.nio.charset.{CharacterCodingException, CodingErrorAction}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

/** Message key or value encoder (serializer). `null` data is encoded as `null`. */
trait MsgEncoder {
  def encode(topic: String, headers: Seq[Header], data: Any): Array[Byte]
}

/** Message key or value decoder (deserializer). `null` data is decoded as `null`. */
trait MsgDecoder {
  def decode(topic: String, headers: Seq[Header], data: Array[Byte]): Any
}

trait MsgCodec extends MsgEncoder with MsgDecoder

/** Json codec. Maps are decoded to `Map[String, Any]`, arrays to `Seq[Any]`. */
object JsonMsgCodec extends MsgCodec {
  def encode(topic: String, headers: Seq[Header], data: Any): Array[Byte] =
    if (data == null) null else ResultEncoder.encodeAnyToJsonBytes(data)
  def decode(topic: String, headers: Seq[Header], data: Array[Byte]): Any =
    if (data == null) null else CborOrJsonAnyValueDecoder.decode(ByteString.fromArrayUnsafe(data), Json)
}

/** Cbor codec. Maps are decoded to `Map[String, Any]`, arrays to `Seq[Any]`. */
object CborMsgCodec extends MsgCodec {
  def encode(topic: String, headers: Seq[Header], data: Any): Array[Byte] =
    if (data == null) null else {
      import ResultEncoder.JsonEncoder._
      Cbor.encode(data)(ResultEncoder.jsValEncoder).toByteArray
    }
  def decode(topic: String, headers: Seq[Header], data: Array[Byte]): Any =
    if (data == null) null else CborOrJsonAnyValueDecoder.decode(ByteString.fromArrayUnsafe(data), Cbor)
}

/** UTF-8 string codec, encodes `Array[Byte]` as is, other values as `toString`. Default codec for message keys. */
object StringMsgCodec extends MsgCodec {
  def encode(topic: String, headers: Seq[Header], data: Any): Array[Byte] = data match {
    case null => null
    case b: Array[Byte] => b
    case x => x.toString.getBytes(UTF_8)
  }
  def decode(topic: String, headers: Seq[Header], data: Array[Byte]): Any =
    if (data == null) null else new String(data, UTF_8)
}

/** Pass-through codec, value to be encoded must be `Array[Byte]`. */
object BytesMsgCodec extends MsgCodec {
  def encode(topic: String, headers: Seq[Header], data: Any): Array[Byte] = data match {
    case null => null
    case b: Array[Byte] => b
    case x => sys.error(s"Bytes codec can encode only Array[Byte], got: ${x.getClass.getName}")
  }
  def decode(topic: String, headers: Seq[Header], data: Array[Byte]): Any = data
}

object MsgCodec {
  /** Built-in codecs, available by name, see [[MsgCodecs]] for configured codecs. */
  val codecs: Map[String, MsgCodec] = Map(
    "json"    -> JsonMsgCodec,
    "cbor"    -> CborMsgCodec,
    "string"  -> StringMsgCodec,
    "bytes"   -> BytesMsgCodec,
  )
  val defaultKeyCodec:    MsgCodec = StringMsgCodec
  val defaultValueCodec:  MsgCodec = JsonMsgCodec

  /** Built-in codec by name, see [[MsgCodecs.apply]] for configured codecs. */
  def apply(name: String): MsgCodec =
    codecs.getOrElse(name, sys.error(
      s"Message codec '$name' not found. Available codecs: [${codecs.keys.toSeq.sorted.mkString(", ")}]"))
}

/** Codecs of a messaging provider, created once by provider from its configuration.
  *
  * Codec in configuration (`key-codec`, `value-codec` or entry of `codecs`) is specified either by name
  * (configured in `codecs` or built-in, see [[MsgCodec.codecs]]) or inline, as object with `factory-class`
  * of [[MsgCodec]] implementation - scala object or class with no-arg or `(Config)` constructor, codec config is
  * passed to the constructor. If the same codec class is used for keys and values and codec needs to distinguish
  * them, use codec config (for example `is-key = true`).
  * {{{
  * codecs {                                   # named codecs, created when provider is created
  *   avro { factory-class = com.example.AvroCodec, schema-registry-url = "..." }
  * }
  * key-codec   = string                       # name of configured or built-in codec
  * value-codec { factory-class = com.example.MyCodec }  # inline codec, created once per distinct config
  * }}}
  * Named codecs may override built-in codecs.
  *
  * @param config provider configuration
  */
class MsgCodecs(config: Config) {
  import MsgCodecs._

  private val configured: Map[String, MsgCodec] =
    if (config != null && config.hasPath(codecsPath)) {
      val codecsConfig = config.getConfig(codecsPath)
      codecsConfig.root.keySet.asScala.toList.map { name =>
        // quote name - it may contain dots
        val path = com.typesafe.config.ConfigUtil.joinPath(name)
        name -> newCodec(codecsConfig.getConfig(path), s"message codec '$name'")
      }.toMap
    } else Map.empty

  /** Configured codec names together with built-in ones, configured codecs take precedence. */
  val codecs: Map[String, MsgCodec] = MsgCodec.codecs ++ configured

  private val inlineCodecs = new ConcurrentHashMap[ConfigValue, MsgCodec]

  /** Codec by name, configured or built-in. */
  def apply(name: String): MsgCodec =
    codecs.getOrElse(name, sys.error(
      s"Message codec '$name' not found. Available codecs: [${codecs.keys.toSeq.sorted.mkString(", ")}]"))

  /** Codec specified in `cfg` (may be `null`) at `path` by name or inline, `None` if `cfg` does not have the path. */
  def fromConfig(cfg: Config, path: String): Option[MsgCodec] =
    if (cfg == null || !cfg.hasPath(path)) None
    else {
      val value = cfg.getValue(path)
      value.valueType match {
        case ConfigValueType.STRING => Some(apply(cfg.getString(path)))
        case ConfigValueType.OBJECT =>
          Some(inlineCodecs.computeIfAbsent(value, _ => newCodec(cfg.getConfig(path), s"message codec '$path'")))
        case t => sys.error(s"Message codec name or object expected at '$path', got: $t")
      }
    }

  /** Explicit decoder if not `null`, otherwise `key-codec` from `cfg` or [[MsgCodec.defaultKeyCodec]].
    * Provider passes operation config with fallback to provider config.
    */
  def keyDecoder(explicit: MsgDecoder, cfg: Config): MsgDecoder =
    if (explicit != null) explicit else fromConfig(cfg, keyCodecPath).getOrElse(MsgCodec.defaultKeyCodec)
  /** Explicit decoder if not `null`, otherwise `value-codec` from `cfg` or [[MsgCodec.defaultValueCodec]]. */
  def valueDecoder(explicit: MsgDecoder, cfg: Config): MsgDecoder =
    if (explicit != null) explicit else fromConfig(cfg, valueCodecPath).getOrElse(MsgCodec.defaultValueCodec)
  /** Explicit encoder if not `null`, otherwise `key-codec` from `cfg` or [[MsgCodec.defaultKeyCodec]]. */
  def keyEncoder(explicit: MsgEncoder, cfg: Config): MsgEncoder =
    if (explicit != null) explicit else fromConfig(cfg, keyCodecPath).getOrElse(MsgCodec.defaultKeyCodec)
  /** Explicit encoder if not `null`, otherwise `value-codec` from `cfg` or [[MsgCodec.defaultValueCodec]]. */
  def valueEncoder(explicit: MsgEncoder, cfg: Config): MsgEncoder =
    if (explicit != null) explicit else fromConfig(cfg, valueCodecPath).getOrElse(MsgCodec.defaultValueCodec)
}

object MsgCodecs {
  /** Config path of named codecs */
  val codecsPath      = "codecs"
  /** Config path of key codec name or object */
  val keyCodecPath    = "key-codec"
  /** Config path of value codec name or object */
  val valueCodecPath  = "value-codec"
  /** Config path of codec class name in codec object */
  val factoryClassPath = "factory-class"

  @annotation.nowarn("msg=Manifest")
  private def newCodec(codecConfig: Config, description: String): MsgCodec =
    getObjectOrNewInstance[MsgCodec](codecConfig, factoryClassPath, description, Seq(codecConfig), Seq(classOf[Config]))
}

/** Message decoding failure, message position is included in exception message. */
class MsgDecodingException(
  val topic: String,
  val partition: Int,
  val offset: Long,
  what: String,
  cause: Throwable,
) extends RuntimeException(
  s"Failed to decode message $what at $topic-$partition@$offset: ${cause.getMessage}", cause)

/** Message encoding failure. */
class MsgEncodingException(
  val topic: String,
  what: String,
  cause: Throwable,
) extends RuntimeException(s"Failed to encode message $what for topic $topic: ${cause.getMessage}", cause)

/** Conversions between provider records and messages, to be used by providers. */
object MsgConversions {
  import scala.util.control.NonFatal

  /** Encoded outgoing message.
    * @param headers  header values encoded as bytes (or `null`)
    */
  case class EncodedMessage(key: Array[Byte], value: Array[Byte], headers: Seq[(String, Array[Byte])])

  /** Creates incoming message decoding headers with [[headerValue]], key and value with decoders.
    * Decoders receive decoded headers.
    */
  def inMessage(
    topic: String,
    partition: Int,
    offset: Long,
    timestamp: Long,
    key: Array[Byte],
    value: Array[Byte],
    headers: Seq[(String, Array[Byte])],
    keyDecoder: MsgDecoder,
    valueDecoder: MsgDecoder,
  ): InMessage = {
    def decode[T](what: String)(f: => T): T =
      try f catch { case NonFatal(e) => throw new MsgDecodingException(topic, partition, offset, what, e) }
    val hs = headers.map { case (n, v) => Header(n, headerValue(v)) }
    InMessage(
      topic     = topic,
      key       = decode("key")  (keyDecoder.decode(topic, hs, key)),
      value     = decode("value")(valueDecoder.decode(topic, hs, value)),
      partition = partition,
      offset    = offset,
      headers   = hs,
      timestamp = timestamp,
    )
  }

  /** Incoming header value - `String` if bytes are valid UTF-8 (strict decoding), otherwise bytes as is
    * (`Array[Byte]`), `null` for `null`. Lossless - string of valid UTF-8 encodes back to the same bytes,
    * so code expecting binary value can normalize it with `getBytes(UTF_8)`.
    */
  def headerValue(bytes: Array[Byte]): Any =
    if (bytes == null) null
    else try UTF_8.newDecoder
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)
      .decode(ByteBuffer.wrap(bytes)).toString
    catch { case _: CharacterCodingException => bytes }

  /** Encodes outgoing message key and value with encoders, header values (`String` as UTF-8, `Array[Byte]` as is).
    * Encoders receive headers of outgoing message.
    */
  def encode(message: OutMessage, keyEncoder: MsgEncoder, valueEncoder: MsgEncoder): EncodedMessage = {
    import message.{topic, headers}
    def encoding[T](what: String)(f: => T): T =
      try f catch { case NonFatal(e) => throw new MsgEncodingException(topic, what, e) }
    EncodedMessage(
      key     = encoding("key")  (keyEncoder.encode(topic, headers, message.key)),
      value   = encoding("value")(valueEncoder.encode(topic, headers, message.value)),
      headers = headers.map { h =>
        h.name -> encoding(s"header '${h.name}'")(h.value match {
          case null => null
          case s: String => s.getBytes(UTF_8)
          case b: Array[Byte] => b
          case x => sys.error(s"String or Array[Byte] expected, got: ${x.getClass.getName}")
        })
      },
    )
  }
}
