package org.wabase

import scala.collection.immutable.TreeMap
import scala.language.postfixOps
import spray.json._


object JsonToAny {
  def apply(value: JsValue): Any = {
    value match {
      case JsObject(fields) => fields map (f => f._1 -> apply(f._2)) toMap
      case JsArray(elements) => (elements map apply) toList
      case JsString(v) => v
      case JsNumber(v) => v
      case b: JsBoolean => b.value
      case JsNull => null
    }
  }
}

trait JsonConverter[DTO <: Dto] { self: AppQuereaseIo[DTO] =>
  private[this] def r(value: JsValue): Any = JsonToAny(value)
  private[this] def w(value: Any): JsValue = value match {
    case m: Map[String @unchecked, Any @unchecked] =>
      JsObject(
        TreeMap()(new self.qe.FieldOrdering(m.keys.zipWithIndex.toMap)) ++
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
    case t: java.time.temporal.Temporal => JsString(qe.convertToString(t))
    case t: java.util.Date              => JsString(qe.convertToString(t))
    case jv: JsValue => jv
    case null => JsNull
    case b: Array[Byte]                 => JsString(qe.convertToString(b))
    case x => JsString(String.valueOf(x))
  }
  implicit object DtoJsonFormat extends RootJsonFormat[DTO] {
    def read(value: JsValue): DTO = sys.error("not implemented yet!")
    def write(value: DTO): JsValue = w(self.toMap(value))
  }
  implicit object DtoListJsonFormat extends RootJsonFormat[List[DTO]] {
    def read(value: JsValue): List[DTO] = sys.error("not implemented yet!")
    def write(value: List[DTO]): JsValue = w(value map (self.toMap))
  }

  implicit object MapJsonFormat extends JsonFormat[Map[String, Any]] {
    def read(value: JsValue): Map[String,Any] = {
      value match {
        case _: JsObject => r(value).asInstanceOf[Map[String, Any]]
        case x => sys.error("Invalid JsValue object, unable to produce map: " + x)
      }
    }
    def write(value: Map[String, Any]): JsObject = {
      w(value).asInstanceOf[JsObject]
    }
  }
  implicit object TupleJsonFormat extends JsonFormat[(String, Any)] {
    def read(value: JsValue): (String, Any) = {
      value match {
        case JsObject(f) if f.size == 1 => r(value).asInstanceOf[Map[String, Any]].head
        case x => sys.error("Invalid JsValue object, unable to produce tuple: " + x)
      }
    }
    def write(value: (String, Any)): JsObject = {
      w(Map(value)).asInstanceOf[JsObject]
    }
  }
  implicit object ListJsonFormat extends JsonFormat[List[Any]] {
    def read(value: JsValue): List[Any] = {
      value match {
        case JsArray(elements: Vector[JsValue]) => r(value).asInstanceOf[Seq[_]].toList
        case x => sys.error("Invalid JsValue object, unable to produce list: " + x)
      }
    }
    def write(value: List[Any]): JsArray = {
      w(value).asInstanceOf[JsArray]
    }
  }
}
