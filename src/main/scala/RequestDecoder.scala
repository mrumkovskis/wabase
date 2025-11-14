package org.wabase

import com.typesafe.config.Config
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source}
import org.apache.pekko.stream.connectors.csv.scaladsl.{CsvParsing, CsvToMap}
import org.apache.pekko.stream.connectors.xml.scaladsl.XmlParsing
import org.apache.pekko.util.ByteString
import io.bullet.borer.compat.pekko.ByteStringProvider
import io.bullet.borer.encodings.BaseEncoding
import io.bullet.borer.{Borer, Cbor, Decoder, DecodingSetup, Input, Json, Tag, Target, DataItem => DI}
import org.apache.pekko.http.scaladsl.model.HttpEntity
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.common.JsonEntityStreamingSupport
import org.mojoz.metadata.{Type, TypeDef, ViewDef}
import org.w3c.dom.{Element, Node, NodeList}
import org.wabase.BorerDatetimeDecoders._

import java.io.InputStream
import java.lang.{Boolean => JBoolean, Double => JDouble, Long => JLong}
import java.math.{BigDecimal => JBigDecimal, BigInteger => JBigInteger}
import java.nio.charset.Charset
import java.time.{LocalDate, LocalDateTime, LocalTime}
import scala.annotation.tailrec
import scala.collection.immutable.{ListMap, Map, Seq}
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, DurationInt}
import scala.jdk.CollectionConverters._
import scala.language.{higherKinds, postfixOps}
import scala.reflect.ClassTag

/** Decodes cbor or json according to view and type metadata */
class CborOrJsonDecoder(typeDefs: Seq[TypeDef], nameToViewDef: Map[String, ViewDef]) {
  lazy val typeNameToScalaTypeName =
    typeDefs
      .map(td => td.name -> td.targetNames.get("scala").orNull)
      .filter(_._2 != null)
      .toMap
  def simpleValueDecoder(type_ : Type): Decoder[Any] =
    (typeNameToScalaTypeName.get(type_.name).orNull match {
      case "String"             => Decoder.forString
      case "java.lang.Long"     => Decoder.forBoxedLong
      case "java.lang.Integer"  => Decoder.forBoxedInt
      case "java.sql.Date"      => javaSqlDateDecoder
      case "java.sql.Time"      => javaSqlTimeDecoder
      case "java.sql.Timestamp" => javaSqlTimestampDecoder
      case "java.time.Instant"        => javaTimeInstantDecoder
      case "java.time.LocalDate"      => localDateDecoder
      case "java.time.LocalTime"      => localTimeDecoder
      case "java.time.LocalDateTime"  => localDateTimeDecoder
      case "java.time.OffsetDateTime" => offsetDateTimeDecoder
      case "java.time.ZonedDateTime"  => zonedDateTimeDecoder
      case "scala.math.BigInt"     => Decoder.forBigInt
      case "scala.math.BigDecimal" => Decoder.forBigDecimal
      case "java.lang.Double"   => Decoder.forBoxedDouble
      case "java.lang.Boolean"  => Decoder.forBoxedBoolean
      case "Array[Byte]"        => Decoder { r =>
        r.dataItem() match {
          case DI.Null          => r.readNull()
          case DI.Bytes  |
               DI.BytesStart    => r[Array[Byte]]
          case _                => BaseEncoding.base64.decode(r.readChars())
        }
      }
      case _                    => Decoder.forString
    }).asInstanceOf[Decoder[Any]]

  protected def toSeq[T](array: Array[T]): Seq[T] = array.toList
  protected def anyJsonMapZero: Map[String, Any]  = ListMap[String, Any]() // Preserve ordering? No TreeSeqMap in scala 2.12

  def toMapDecoder[M <: Map[String, Any] : ClassTag](
    viewName: String,
    viewNameToMapZero: String => M,
  ): Decoder[M] = Decoder { r => try {
    val view = nameToViewDef(viewName)
    def updated(map: Map[String, Any]): Map[String, Any] = {
      val key = r.readString()
      view.fieldOpt(key) match {
        case Some(field) =>
          try {
            if (r.dataItem() == DI.Null)
              map.updated(key, r.readNull())
            else if (field.type_.isComplexType) {
              implicit val decoder: Decoder[M] = toMapDecoder(field.type_.name, viewNameToMapZero)
              map.updated(key, if (field.isCollection) toSeq(r[Array[M]]) else r[M])
            } else if (field.type_.name == "json") {
              implicit val decoder: Decoder[Any] = CborOrJsonAnyValueDecoder.anyValueDecoder(() => anyJsonMapZero)
              map.updated(key,
                (if (field.isCollection) toSeq(r[Array[Any]]) else r[Any]) match {
                  case s: String => Json.encode(s).toUtf8String
                  case x => x
                }
              )
            } else {
              implicit val decoder: Decoder[Any] = simpleValueDecoder(field.type_)
              map.updated(key, if (field.isCollection) toSeq(r[Array[Any]]) else r[Any])
            }
          } catch {
            case util.control.NonFatal(ex) =>
              throw new BusinessException(
                s"Failed to read ${field.name} of type ${field.type_.name}: ${ex.getMessage}", ex)
          }
        case None =>
          r.skipElement() // no such field in this view - skip
          map
      }
    }
    if (r.hasMapHeader) {
      @tailrec def rec(remaining: Int, map: Map[String, Any]): M = {
        if (remaining > 0) rec(remaining - 1, updated(map)) else map.asInstanceOf[M]
      }
      val size = r.readMapHeader()
      if (size <= Int.MaxValue) rec(size.toInt, viewNameToMapZero(viewName))
      else r.overflow(s"Cannot deserialize Map with size $size (> Int.MaxValue)")
    } else if (r.hasMapStart) {
      r.readMapStart()
      @tailrec def rec(map: Map[String, Any]): M =
        if (r.tryReadBreak()) map.asInstanceOf[M] else rec(updated(map))
      rec(viewNameToMapZero(viewName))
    } else r.unexpectedDataItem(expected = "Map")
  } catch {
    case util.control.NonFatal(ex) =>
      throw new BusinessException(s"Failed to read to map for $viewName: ${ex.getMessage}", ex)
  }}

  protected def decoding(data: ByteString, decodeFrom: Target): DecodingSetup.Api[_] = decodeFrom match {
    case _: Cbor.type => Cbor.decode(data)
    case _: Json.type => Json.decode(data).withConfig(Json.DecodingConfig.default.copy(
      maxNumberAbsExponent = 308, // to accept up to Double.MaxValue
    ))
  }

  protected def to[T: Decoder](decoding: DecodingSetup.Api[_]): T =
    try decoding.to[T].value catch {
      case boer: Borer.Error[_] => boer.getCause match {
        case biex: BusinessException => throw biex
        case _ => throw new BusinessException(s"Failed to decode data: ${boer.getMessage}", boer)
      }
      case util.control.NonFatal(ex) => throw ex
    }

  def decodeToMap[M <: Map[String, Any] : ClassTag](
    data:       ByteString,
    viewName:   String,
    decodeFrom: Target = Json,
  )(viewNameToMapZero: String => M): M = {
    implicit val decoder: Decoder[M] = toMapDecoder(viewName, viewNameToMapZero)
    to[M](decoding(data, decodeFrom))
  }

  def decodeToSeqOfMaps[M <: Map[String, Any] : ClassTag](
    data:       ByteString,
    viewName:   String,
    decodeFrom: Target = Json,
  )(viewNameToMapZero: String => M): Seq[M] = {
    implicit val decoder: Decoder[M] = toMapDecoder(viewName, viewNameToMapZero)
    toSeq(to[Array[M]](decoding(data, decodeFrom)))
  }
}

/** Decodes cbor or json according to view and type metadata,
 *  accepts and decodes strings to booleans and numbers
 */
class CborOrJsonLenientDecoder(typeDefs: Seq[TypeDef], nameToViewDef: Map[String, ViewDef])
  extends CborOrJsonDecoder(typeDefs, nameToViewDef) {
  private val lenientBigIntDecoder: Decoder[BigInt] =
    Decoder(r => if (r.hasString) BigInt(r.readString()) else r[BigInt])
  private val lenientBigDecimalDecoder: Decoder[BigDecimal] =
    Decoder(r => if (r.hasString) BigDecimal(r.readString()) else r[BigDecimal])
  override def simpleValueDecoder(type_ : Type): Decoder[Any] =
    (typeNameToScalaTypeName.get(type_.name).orNull match {
      case "java.lang.Long"     => Decoder.StringNumbers.longDecoder.asInstanceOf[Decoder[JLong]]
      case "java.lang.Integer"  => Decoder.StringNumbers.intDecoder.asInstanceOf[Decoder[Integer]]
      case "java.lang.Double"   => Decoder.StringNumbers.doubleDecoder.asInstanceOf[Decoder[JDouble]]
      case "java.lang.Boolean"  => Decoder.StringBooleans.booleanDecoder.asInstanceOf[Decoder[JBoolean]]
      case "scala.math.BigInt"     => lenientBigIntDecoder
      case "scala.math.BigDecimal" => lenientBigDecimalDecoder
      case _                    => super.simpleValueDecoder(type_)
    }).asInstanceOf[Decoder[Any]]
}

/** Decodes cbor or json - unrestricted structure and value types, string keys for maps.
 *  When decoding from json, dates and similar will be decoded as strings
 */
class CborOrJsonAnyValueDecoder() {
  def anyValueDecoder[M <: Map[String, Any] : ClassTag](
    mapZero: () => M,
  ): Decoder[Any] = Decoder { r =>
    import BorerDatetimeDecoders._
    r.dataItem() match {
      case DI.Null          => r.readNull()
      case DI.Undefined     => r.readUndefined(); null
      case DI.Boolean       => r[Boolean]
      case DI.Int           => r[Int]
      case DI.Long          => r[Long]
      case DI.OverLong      => r[JBigInteger]
      case DI.Float16       => r[Float]
      case DI.Float         => r[Float]
      case DI.Double        => r[Double]
      case DI.NumberString  => r[JBigDecimal]
      case DI.String        => r[String]
      case DI.Chars         => r[String]
      case DI.Text          => r[String]
      case DI.TextStart     => r[String]
      case DI.Bytes         => r[Array[Byte]]
      case DI.BytesStart    => r[Array[Byte]]
      case DI.ArrayHeader   => implicit val d: Decoder[Any] = anyValueDecoder(mapZero); toSeq(r[Array[Any]])
      case DI.ArrayStart    => implicit val d: Decoder[Any] = anyValueDecoder(mapZero); toSeq(r[Array[Any]])
      case DI.MapHeader     => implicit val d: Decoder[M]   = toMapDecoder(mapZero);    r[M]
      case DI.MapStart      => implicit val d: Decoder[M]   = toMapDecoder(mapZero);    r[M]
      case DI.Tag           =>
        if      (r.hasTag(Tag.PositiveBigNum))   r[JBigInteger]
        else if (r.hasTag(Tag.NegativeBigNum))   r[JBigInteger]
        else if (r.hasTag(Tag.DecimalFraction))  r[JBigDecimal]
        else if (r.hasTag(Tag.DateTimeString))   r[LocalDateTime]
        else if (r.hasTag(Tag.EpochDateTime)) {
          r.readTag()
          r.dataItem() match {
            case DI.Int | DI.Long   => r[LocalDate]
            case _                  => r[LocalDateTime]
          }
        }
        else if (r.hasTag(BorerDatetimeEncoders.TimeTag)) r[LocalTime]
        else r[String]
      case DI.SimpleValue   => r[Int]
    }
  }

  protected def toSeq[T](array: Array[T]): Seq[T] = array.toList

  def toMapDecoder[M <: Map[String, Any] : ClassTag](
    mapZero: () => M,
  ): Decoder[M] = Decoder { r =>
    def updated(map: Map[String, Any]): Map[String, Any] = {
      val key = r.readString()
      r.dataItem() match {
        case DI.MapStart =>
          implicit val decoder: Decoder[M] = toMapDecoder(mapZero)
          map.updated(key, r[M])
        case DI.ArrayStart =>
          implicit val decoder: Decoder[Any] = anyValueDecoder(mapZero)
          map.updated(key, toSeq(r[Array[Any]]))
        case _ =>
          implicit val decoder: Decoder[Any] = anyValueDecoder(mapZero)
          map.updated(key, r[Any])
      }
    }
    if (r.hasMapHeader) {
      @tailrec def rec(remaining: Int, map: Map[String, Any]): M = {
        if (remaining > 0) rec(remaining - 1, updated(map)) else map.asInstanceOf[M]
      }
      val size = r.readMapHeader()
      if (size <= Int.MaxValue) rec(size.toInt, mapZero())
      else r.overflow(s"Cannot deserialize Map with size $size (> Int.MaxValue)")
    } else if (r.hasMapStart) {
      r.readMapStart()
      @tailrec def rec(map: Map[String, Any]): M =
        if (r.tryReadBreak()) map.asInstanceOf[M] else rec(updated(map))
      rec(mapZero())
    } else r.unexpectedDataItem(expected = "Map")
  }

  protected def decoding[T: Input.Provider](data: T, decodeFrom: Target) = decodeFrom match {
    case _: Cbor.type => Cbor.decode(data)
    case _: Json.type => Json.decode(data).withConfig(Json.DecodingConfig.default.copy(
      maxNumberAbsExponent = 308, // to accept up to Double.MaxValue
    ))
  }

  protected def to[T: Decoder](decoding: DecodingSetup.Api[_]): T =
    try decoding.to[T].value catch {
      case boer: Borer.Error[_] => boer.getCause match {
        case biex: BusinessException => throw biex
        case _ => throw new BusinessException(s"Failed to decode data: ${boer.getMessage}", boer)
      }
      case util.control.NonFatal(ex) => throw ex
    }

  def decode[M <: Map[String, Any] : ClassTag](
    data:       ByteString,
    decodeFrom: Target = Json,
    mapZero:    () => M = () => Map.empty[String, Any],
  ): Any = {
    implicit val decoder: Decoder[Any] = anyValueDecoder(mapZero)
    to[Any](decoding(data, decodeFrom))
  }

  def decodeFromInputStream[M <: Map[String, Any] : ClassTag](
    data:       InputStream,
    decodeFrom: Target = Json,
    mapZero:    () => M = () => Map.empty[String, Any],
  ): Any = {
    implicit val decoder: Decoder[Any] = anyValueDecoder(mapZero)
    to[Any](decoding(data, decodeFrom))
  }

  def decodeToMap[M <: Map[String, Any] : ClassTag](
    data:       ByteString,
    decodeFrom: Target = Json,
    mapZero:    () => M = () => Map.empty[String, Any],
  ): M = {
    implicit val decoder: Decoder[M] = toMapDecoder(mapZero)
    to[M](decoding(data, decodeFrom))
  }

  def decodeToSeqOfMaps[M <: Map[String, Any] : ClassTag](
    data:       ByteString,
    decodeFrom: Target = Json,
    mapZero:    () => M = () => Map.empty[String, Any],
  ): Seq[M] = {
    implicit val decoder: Decoder[M] = toMapDecoder(mapZero)
    toSeq(to[Array[M]](decoding(data, decodeFrom)))
  }
}

object CborOrJsonAnyValueDecoder extends CborOrJsonAnyValueDecoder

object CsvDecoderConfig {
  lazy val componentConfs = ComponentConf.getConfigs("data-parsers-csv")
  lazy val configs: Map[String, Config] = componentConfs.confs.toMap
  lazy val csvDecoderFactory: CsvDecoderFactory =
    getObjectOrNewInstance[CsvDecoderFactory](componentConfs.root, "factory-class", "csv decoder factory")
}

trait CsvDecoderFactory {
  def createCsvStreamDecoders: Map[String, Flow[ByteString, Map[String, String], NotUsed]]
}

object CsvDecoderFactory extends CsvDecoderFactory {
  def createCsvStreamDecoder(n: String, csvCfg: Config): Flow[ByteString, Map[String, String], NotUsed] = {
    def getByte(setting: String) =
      csvCfg.getString(setting) match {
        case b if b.length == 1 => b.toCharArray.head.toByte
        case x => throw new RuntimeException(s"Unsupported $setting for csv parser $n: '$x'. Expecting single byte")
      }
    val delimiter  = getByte("delimiter")
    val quoteChar  = getByte("quote-char")
    val escapeChar = getByte("escape-char")
    val maxLineLen = csvCfg.getInt("maximum-line-length")
    val charset    = Charset.forName(csvCfg.getString("charset"))
    val headersOpt = Option("headers").filter(csvCfg.hasPath).map(csvCfg.getStringList).map(_.asScala.toSeq)
    val toMapConverter = headersOpt match {
      case Some(headers) => CsvToMap.withHeadersAsStrings(charset, headers: _*)
      case None          => CsvToMap.toMapAsStrings(charset)
    }
    Flow[ByteString]
      .via(CsvParsing.lineScanner(delimiter, quoteChar, escapeChar, maxLineLen))
      .via(toMapConverter)
  }

  def createCsvStreamDecoders: Map[String, Flow[ByteString, Map[String, String], NotUsed]] = {
    CsvDecoderConfig.configs.map { case (n, csvCfg) =>
      n -> createCsvStreamDecoder(n, csvCfg)
    }.toMap
  }
}

object JsonDecoderConfig {
  lazy val componentConfs: ComponentConfs = ComponentConf.getConfigs("data-parsers-json")
  lazy val configs: Map[String, Config] = componentConfs.confs.toMap
  lazy val jsonDecoderFactory: JsonDecoderFactory =
    getObjectOrNewInstance[JsonDecoderFactory](componentConfs.root, "factory-class", "json decoder factory")
}

trait JsonDecoderFactory {
  def createJsonStreamDecoders: Map[String, Flow[ByteString, Map[String, Any], NotUsed]]
}

object JsonDecoderFactory extends JsonDecoderFactory {
  def createJsonStreamDecoder(n: String, jsonCfg: Config): Flow[ByteString, Map[String, Any], NotUsed] = {
    val maxObjectSize = jsonCfg.getInt("max-object-size")
    val ess = new JsonEntityStreamingSupport(maxObjectSize = maxObjectSize)
    Flow[ByteString].via(ess.framingDecoder).map(CborOrJsonAnyValueDecoder.decodeToMap(_))
  }

  def createJsonStreamDecoders: Map[String, Flow[ByteString, Map[String, Any], NotUsed]] = {
    JsonDecoderConfig.configs.map { case (n, jsonCfg) =>
      n -> createJsonStreamDecoder(n, jsonCfg)
    }.toMap
  }
}

object XmlDecoderConfig {
  lazy val componentConfs = ComponentConf.getConfigs("data-parsers-xml")
  lazy val configs: Map[String, Config] = componentConfs.confs.toMap
  lazy val xmlDecoderFactory: XmlDecoderFactory =
    getObjectOrNewInstance[XmlDecoderFactory](componentConfs.root, "factory-class", "xml decoder factory")
}

trait XmlDecoderFactory {
  def createXmlStreamDecoders: Map[String, Flow[ByteString, Map[String, Any], NotUsed]]
}

object XmlDecoderFactory extends XmlDecoderFactory {
  def nodeListToMap(nodeList: NodeList): Map[String, Any] = {
    val children = (0 until nodeList.getLength).map(nodeList.item)

    // Concatenate all text nodes into a single string
    val textContent = children
      .filter(_.getNodeType == Node.TEXT_NODE)
      .map(_.getTextContent)
      .filter(_.trim.nonEmpty)  // Ignore whitespace-only text nodes
      .mkString("")
      .trim

    // Filter out element children
    val elementChildren = children.filter(_.getNodeType == Node.ELEMENT_NODE)

    // Build a map from element children
    val elementsMap = elementChildren.foldLeft(Map[String, Any]()) { (acc, node) =>
      val elem = node.asInstanceOf[Element]
      val name = elem.getTagName
      val childMap = elementToMap(elem)
      // Unwrap child element if it contains only "#text"
      val childValue = if (childMap.size == 1 && childMap.contains("#text")) childMap("#text") else childMap

      acc.get(name) match {
        case Some(existing) =>
          existing match {
            case vector: Vector[_] =>
              acc.updated(name, vector :+ childValue)
            case _ =>
              acc.updated(name, Vector(existing, childValue))
          }
        case None =>
          acc + (name -> childValue)
      }
    }

    // Include text content in the map if non-empty
    if (textContent.nonEmpty) {
      elementsMap + ("#text" -> textContent)
    } else {
      elementsMap
    }
  }
  def attributesToMap(element: Element) =
    element.getAttributes match {
      case null => Map.empty[String, Any]
      case attrs =>
        (0 until attrs.getLength)
          .map(attrs.item)
          .map(attr => attr.getNodeName -> attr.getNodeValue)
          .toMap
    }
  def elementToMap(element: Element): Map[String, Any] =
    attributesToMap(element) ++ nodeListToMap(element.getChildNodes)
  def createXmlStreamDecoder(n: String, xmlCfg: Config): Flow[ByteString, Map[String, Any], NotUsed] = {
    val path = xmlCfg.getStringList("path").asScala.toVector
    Flow[ByteString]
      .via(XmlParsing.parser)
      .via(XmlParsing.subtree(path))
      .map(elementToMap)
  }
  def createXmlStreamDecoders: Map[String, Flow[ByteString, Map[String, Any], NotUsed]] = {
    XmlDecoderConfig.configs.map { case (n, xmlCfg) =>
      n -> createXmlStreamDecoder(n, xmlCfg)
    }.toMap
  }
}

object RequestDecoders {
  /** Decodes http entity according to view structure (can be null) */
  type RequestDecoder = String => HttpEntity => Source[Any, _]
  type Decoders       = Map[String, RequestDecoder]
  def decoders(qe: AppQuerease): Decoders = {
    def requestDecoder(transformer: Flow[ByteString, Map[String, Any], _]): RequestDecoder = {
      viewName => httpEnt => {
        val vd = Option(viewName).map(qe.viewDef).orNull
        httpEnt.dataBytes.via(transformer)
          .map(data => if (vd == null) data else qe.toCompatibleMap(data, vd))
      }
    }
    CsvDecoderConfig.csvDecoderFactory.createCsvStreamDecoders.map { case (n, d) => (n, requestDecoder(d)) } ++
      JsonDecoderConfig.jsonDecoderFactory.createJsonStreamDecoders.map { case (n, d) => (n, requestDecoder(d)) } ++
      XmlDecoderConfig.xmlDecoderFactory.createXmlStreamDecoders.map { case (n, d) => (n, requestDecoder(d)) }
  }
  def sourceToIterator[T](src: Source[T, _])(implicit as: ActorSystem): Iterator[T] = new Iterator[T] {
    private var currentSource = src
    private var hasNextElement: Boolean = true
    private var nextElement: Option[T] = None

    private def fetchNext(): Unit = {
      if (hasNextElement) {
        val currentF = currentSource.idleTimeout(10.seconds).prefixAndTail(1).runWith(Sink.head)
        Await.result(currentF, Duration.Inf) match {
          case (Seq(element), tailSource) =>
            nextElement = Some(element)
            currentSource = tailSource
          case _ =>
            hasNextElement = false
            nextElement = None
        }
      }
    }

    override def hasNext: Boolean = {
      if (nextElement.isEmpty && hasNextElement) fetchNext()
      nextElement.isDefined
    }

    override def next(): T = {
      if (!hasNext) throw new NoSuchElementException("End of iterator")
      val elem = nextElement.get
      nextElement = None
      elem
    }
  }
}

trait RequestDecodersFactory {
  def createRequestDecoders(qe: AppQuerease): RequestDecoders.Decoders
}

object RequestDecodersFactory extends RequestDecodersFactory {
  override def createRequestDecoders(qe: AppQuerease): RequestDecoders.Decoders = RequestDecoders.decoders(qe)
}
