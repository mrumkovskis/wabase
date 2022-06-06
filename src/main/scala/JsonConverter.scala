package org.wabase

import Format._
import akka.util.ByteString
import scala.collection.immutable.TreeMap
import scala.language.postfixOps
import spray.json._

import java.sql

object JsonToAny {
  def apply(value: JsValue): Any = {
    value match {
      case JsObject(fields) => fields map { case (k, v) => k -> apply(v) }
      case JsArray(elements) => elements map apply
      case JsString(v) => v
      case JsNumber(v) => if (v isWhole) v longValue else v
      case b: JsBoolean => b.value
      case JsNull => null
    }
  }
}

trait JsonConverterProvider {
  final val jsonConverter: JsonConverter = initJsonConverter
  /** Override this method in subclass to initialize {{{jsonConverter}}} */
  protected def initJsonConverter: JsonConverter
}

trait JsonConverter { self: AppQuerease =>
  private[this] def r(value: JsValue): Any = JsonToAny(value)
  private[this] def w(value: Any): JsValue = value match {
    case m: Map[String @unchecked, Any @unchecked] =>
      JsObject(
        TreeMap()(new self.FieldOrdering(m.keys.zipWithIndex.toMap)) ++
          (m.map { case (k, v) => (k, w(v)) }))
    case l: Iterable[Any] => JsArray((l map w) toSeq : _*)
    case d: DTO @unchecked => DtoJsonFormat.write(d)
    case s: String => JsString(s)
    case n: Int => JsNumber(n)
    case n: Long => JsNumber(n)
    case n: Double => JsNumber(n)
    case n: BigInt => JsNumber(n)
    case n: java.lang.Number => JsNumber(String.valueOf(n))
    case b: Boolean => JsBoolean(b)
    case t: sql.Time => JsString(t.toString)
    case t: Timestamp => JsString(humanDateTime(t))
    case d: jDate => JsString(xsdDate(d))
    case jv: JsValue => jv
    case null => JsNull
    case b: Array[Byte] => JsString(ByteString.fromArrayUnsafe(b).encodeBase64.utf8String)
    case x => JsString(String.valueOf(x))
  }
  implicit object DtoJsonFormat extends RootJsonFormat[DTO] {
    def read(value: JsValue) = sys.error("not implemented yet!")
    def write(value: DTO) = w(self.toMap(value))
  }
  implicit object DtoListJsonFormat extends RootJsonFormat[List[DTO]] {
    def read(value: JsValue) = sys.error("not implemented yet!")
    def write(value: List[DTO]) = w(value map (self.toMap))
  }

  implicit object MapJsonFormat extends JsonFormat[Map[String, Any]] {
    def read(value: JsValue) = {
      value match {
        case _: JsObject => r(value).asInstanceOf[Map[String, Any]]
        case x => sys.error("Invalid JsValue object, unable to produce map: " + x)
      }
    }
    def write(value: Map[String, Any]) = {
      w(value).asInstanceOf[JsObject]
    }
  }
  implicit object TupleJsonFormat extends JsonFormat[(String, Any)] {
    def read(value: JsValue) = {
      value match {
        case JsObject(f) if f.size == 1 => r(value).asInstanceOf[Map[String, Any]].head
        case x => sys.error("Invalid JsValue object, unable to produce tuple: " + x)
      }
    }
    def write(value: (String, Any)) = {
      w(Map(value)).asInstanceOf[JsObject]
    }
  }
  implicit object ListJsonFormat extends JsonFormat[List[Any]] {
    def read(value: JsValue) = {
      value match {
        case JsArray(elements: Vector[JsValue]) => r(value).asInstanceOf[Seq[_]].toList
        case x => sys.error("Invalid JsValue object, unable to produce list: " + x)
      }
    }
    def write(value: List[Any]) = {
      w(value).asInstanceOf[JsArray]
    }
  }
}
