package org.wabase

import akka.util.ByteString
import io.bullet.borer.compat.akka.ByteStringProvider
import io.bullet.borer.encodings.BaseEncoding
import io.bullet.borer.{Cbor, DataItem => DI, Decoder, Json, Tag, Target}
import org.mojoz.metadata.{ViewDef, Type, TypeDef}
import org.wabase.BorerDatetimeDecoders._

import java.lang.{Boolean => JBoolean, Double => JDouble, Long => JLong}
import java.math.{BigDecimal => JBigDecimal, BigInteger => JBigInteger}
import java.time.{LocalDate, LocalTime, LocalDateTime}
import scala.annotation.tailrec
import scala.collection.immutable.{Map, Seq}
import scala.language.postfixOps
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

  protected def reader(data: ByteString, decodeFrom: Target) = decodeFrom match {
    case _: Cbor.type => Cbor.reader(data)
    case _: Json.type => Json.reader(data, Json.DecodingConfig.default.copy(
      maxNumberAbsExponent = 308, // to accept up to Double.MaxValue
    ))
  }

  def decodeToMap[M <: Map[String, Any] : ClassTag](
    data:       ByteString,
    viewName:   String,
    decodeFrom: Target = Json,
  )(viewNameToMapZero: String => M): M = {
    implicit val decoder: Decoder[M] = toMapDecoder(viewName, viewNameToMapZero)
    reader(data, decodeFrom)[M]
  }

  def decodeToSeqOfMaps[M <: Map[String, Any] : ClassTag](
    data:       ByteString,
    viewName:   String,
    decodeFrom: Target = Json,
  )(viewNameToMapZero: String => M): Seq[M] = {
    implicit val decoder: Decoder[M] = toMapDecoder(viewName, viewNameToMapZero)
    try {
      toSeq(reader(data, decodeFrom)[Array[M]])
    } catch {
      case util.control.NonFatal(ex) =>
        throw new BusinessException(s"Failed to read array for $viewName: ${ex.getMessage}", ex)
    }
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

  protected def reader(data: ByteString, decodeFrom: Target) = decodeFrom match {
    case _: Cbor.type => Cbor.reader(data)
    case _: Json.type => Json.reader(data, Json.DecodingConfig.default.copy(
      maxNumberAbsExponent = 308, // to accept up to Double.MaxValue
    ))
  }

  def decode[M <: Map[String, Any] : ClassTag](
    data:       ByteString,
    decodeFrom: Target = Json,
    mapZero:    () => M = () => Map.empty[String, Any],
  ): Any = {
    implicit val decoder: Decoder[Any] = anyValueDecoder(mapZero)
    reader(data, decodeFrom)[Any]
  }

  def decodeToMap[M <: Map[String, Any] : ClassTag](
    data:       ByteString,
    decodeFrom: Target = Json,
    mapZero:    () => M = () => Map.empty[String, Any],
  ): M = {
    implicit val decoder: Decoder[M] = toMapDecoder(mapZero)
    reader(data, decodeFrom)[M]
  }

  def decodeToSeqOfMaps[M <: Map[String, Any] : ClassTag](
    data:       ByteString,
    decodeFrom: Target = Json,
    mapZero:    () => M = () => Map.empty[String, Any],
  ): Seq[M] = {
    implicit val decoder: Decoder[M] = toMapDecoder(mapZero)
    toSeq(reader(data, decodeFrom)[Array[M]])
  }
}
