package org.wabase

import akka.util.ByteString
import io.bullet.borer.compat.akka.ByteStringProvider
import io.bullet.borer.encodings.BaseEncoding
import io.bullet.borer.{Cbor, Decoder, Json, Target, DataItem => DI}
import org.mojoz.metadata.{MojozViewDef, Type, TypeDef}
import org.wabase.BorerDatetimeDecoders._

import java.lang.{Double => JDouble, Long => JLong}
import scala.annotation.tailrec
import scala.language.postfixOps
import scala.reflect.ClassTag

class CborOrJsonDecoder(typeDefs: Seq[TypeDef], nameToViewDef: Map[String, MojozViewDef]) {
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
      case "java.sql.Timestamp" => javaSqlTimestampDecoder
      case "BigInt"             => Decoder.forBigInt
      case "BigDecimal"         => Decoder.forBigDecimal
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

  def toMapDecoder[M <: Map[String, Any] : ClassTag](viewName: String, empty: M): Decoder[M] = Decoder { r =>
    val view = nameToViewDef(viewName)
    def updated(map: Map[String, Any]): Map[String, Any] = {
      val key = r.readString()
      view.fieldOpt(key) match {
        case Some(field) =>
          try {
            if (r.dataItem() == DI.Null)
              map.updated(key, r.readNull())
            else if (field.type_.isComplexType) {
              implicit val decoder = toMapDecoder(field.type_.name, empty)
              map.updated(key, if (field.isCollection) toSeq(r[Array[M]]) else r[M])
            } else {
              implicit val decoder = simpleValueDecoder(field.type_)
              map.updated(key, if (field.isCollection) toSeq(r[Array[Any]]) else r[Any])
            }
          } catch {
            case util.control.NonFatal(ex) =>
              throw new RuntimeException(s"Failed to load field ${field.name} of type ${field.type_.name}", ex)
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
      if (size <= Int.MaxValue) rec(size.toInt, empty)
      else r.overflow(s"Cannot deserialize Map with size $size (> Int.MaxValue)")
    } else if (r.hasMapStart) {
      r.readMapStart()
      @tailrec def rec(map: Map[String, Any]): M =
        if (r.tryReadBreak()) map.asInstanceOf[M] else rec(updated(map))
      rec(empty)
    } else r.unexpectedDataItem(expected = "Map")
  }

  def decodeToMap[M <: Map[String, Any] : ClassTag](
    data:       ByteString,
    viewName:   String,
    decodeFrom: Target = Json,
  )(empty: M): M = {
    val reader =
      decodeFrom match {
        case _: Cbor.type => Cbor.reader(data)
        case _: Json.type => Json.reader(data, Json.DecodingConfig.default.copy(
          maxNumberAbsExponent = 308, // to accept up to Double.MaxValue
        ))
      }
    implicit val decoder = toMapDecoder(viewName, empty)
    reader[M]
  }
}