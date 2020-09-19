package org.wabase

import org.tresql._
import org.mojoz.querease.Querease
import org.mojoz.querease.{ValidationException, ValidationResult}
import spray.json.DefaultJsonProtocol.{StringJsonFormat, jsonFormat2, listFormat}

import scala.reflect.ManifestFactory
import spray.json._

trait QuereaseProvider {
  type QE <: AppQuerease
  final implicit val qe: QE = initQuerease
  /** Override this method in subclass to initialize {{{qe}}} */
  protected def initQuerease: QE
}

trait AppQuereaseIo extends org.mojoz.querease.ScalaDtoQuereaseIo with JsonConverter { self: AppQuerease =>

  override type DTO >: Null <: Dto
  override type DWI >: Null <: DTO with DtoWithId

  private [wabase] val FieldRefRegexp_ = FieldRefRegexp

  def fill[B <: DTO: Manifest](jsObject: JsObject): B =
    implicitly[Manifest[B]].runtimeClass.getConstructor().newInstance().asInstanceOf[B {type QE = AppQuerease}].fill(jsObject)(this)
}

abstract class AppQuerease extends Querease with AppQuereaseIo with AppMetadata {
  override def validate[B <: DTO](pojo: B, params: Map[String, Any])(implicit resources: Resources): Unit = {
    try super.validate(pojo, params) catch {
      case vExc: ValidationException =>
        implicit val f02 = jsonFormat2(ValidationResult)
        throw new BusinessException(vExc.details.toJson.compactPrint, vExc)
    }
  }
}

trait Dto extends org.mojoz.querease.Dto { self =>

  override protected type QE = AppQuerease
  override protected type QDto >: Null <: this.type

  import AppMetadata._

  private val auth = scala.collection.mutable.Map[String, Any]()
  override protected def setExtras(dbName: String, r: RowLike)(implicit qe: QE): AnyRef = {
    r(dbName) match {
      case result: Result[_] => updateDynamic(dbName)(result.toListOfMaps)
      case x => updateDynamic(dbName)(x)
    }
    dbName //return key name for auth data
  }
  override def toMapWithOrdering(fieldOrdering: Ordering[String])(implicit qe: QE): Map[String, Any] =
    super.toMapWithOrdering(fieldOrdering) ++ (if (auth.isEmpty) Map() else Map("auth" -> auth.toMap))

  override protected def toString(fieldNames: Seq[String])(implicit qe: QE): String = {
    super.toString(fieldNames) +
      (if (auth.isEmpty) "" else ", auth: " + auth.toString)
  }
  private def isSavableFieldAppExtra(
                                         field: AppQuerease#FieldDef,
                                         view: AppQuerease#ViewDef,
                                         saveToMulti: Boolean,
                                         saveToTableNames: Seq[String]) = {
    def isForInsert = // TODO isForInsert
      this match {
        case id: DtoWithId => id.id == null
        case _ => sys.error(s"isForInsert() for ${getClass.getName} not supported yet")
      }
    def isForUpdate = // TODO isForUpdate
      this match {
        case id: DtoWithId => id.id != null
        case _ => sys.error(s"isForUpdate() for ${getClass.getName} not supported yet")
      }
    val field_db = field.db

    (field_db.insertable && field_db.updatable ||
      field_db.insertable && isForInsert ||
      field_db.updatable && isForUpdate)
  }

  override protected def isSavableField(
      field: AppQuerease#FieldDef,
      view: AppQuerease#ViewDef,
      saveToMulti: Boolean,
      saveToTableNames: Seq[String]) =
    isSavableFieldAppExtra(field, view, saveToMulti, saveToTableNames) &&
      super.isSavableField(field, view, saveToMulti, saveToTableNames)

  override protected def isSavableChildField(
      field: AppQuerease#FieldDef,
      view: AppQuerease#ViewDef,
      saveToMulti: Boolean,
      saveToTableNames: Seq[String],
      childView: AppQuerease#ViewDef): Boolean =
    isSavableFieldAppExtra(field, view, saveToMulti, saveToTableNames)

  protected def parseJsValue(fieldName: String, emptyStringsToNull: Boolean)(implicit qe: QE): PartialFunction[JsValue, Any] = {
    import scala.language.existentials
    val (typ, parType) = setters(fieldName)._2
    def throwUnsupportedType =
      throw new BusinessException(s"Unsupported json type setting field ${
        fieldName} of type $typ in ${getClass.getName}")
    val parseFunc: PartialFunction[JsValue, Any] = {
      case v: JsString =>
        if (ManifestFactory.classType(classOf[java.sql.Date]) == typ) {
          val jdate = Format.parseDate(v.value)
          new java.sql.Date(jdate.getTime)
        }
        else if (ManifestFactory.classType(classOf[java.sql.Timestamp]) == typ) {
          val jdate = Format.parseDateTime(v.value)
          new java.sql.Timestamp(jdate.getTime)
        } else if (ManifestFactory.classType(classOf[java.lang.Long]) == typ ||
          ManifestFactory.Long == typ) {
          if (v.value.trim == "")
            null
          else
            v.value.toLong
        } else if (ManifestFactory.classType(classOf[java.lang.Integer]) == typ ||
          ManifestFactory.Int == typ) {
          if (v.value.trim == "")
            null
          else
            v.value.toInt
        } else if (ManifestFactory.singleType(v.value) == typ) {
          if (emptyStringsToNull && v.value.trim == "")
            null
          else
            v.value
        } else throwUnsupportedType
      case v: JsNumber =>
        if (ManifestFactory.singleType(v.value) == typ) v.value
        else if (ManifestFactory.classType(classOf[java.lang.Long]) == typ) v.value.longValue
        else if (ManifestFactory.Long == typ) v.value.longValue
        else if (ManifestFactory.classType(classOf[java.lang.Integer]) == typ) v.value.intValue
        else if (ManifestFactory.Int == typ) v.value.intValue
        else if (ManifestFactory.classType(classOf[java.lang.Double]) == typ) v.value.doubleValue
        else if (ManifestFactory.Double == typ) v.value.doubleValue
        else if (ManifestFactory.classType(classOf[String]) == typ) v.toString
        else throwUnsupportedType
      case v: JsBoolean =>
        if (ManifestFactory.classType(classOf[java.lang.Boolean]) == typ) v.value
        else if (ManifestFactory.Boolean == typ) v.value
        else if (ManifestFactory.classType(classOf[String]) == typ) v.toString
        else throwUnsupportedType
      case v: JsObject if typ.runtimeClass.isAssignableFrom(classOf[JsObject]) => v
      case v: JsArray if typ.runtimeClass.isAssignableFrom(classOf[JsArray]) => v
      case v: JsObject =>
        if (classOf[Dto].isAssignableFrom(typ.runtimeClass)) {
          typ.runtimeClass.getConstructor().newInstance().asInstanceOf[QDto].fill(v, emptyStringsToNull)
        } else throwUnsupportedType
      case v: JsArray =>
        val c = typ.runtimeClass
        val isList = c.isAssignableFrom(classOf[List[_]])
        val isVector = c.isAssignableFrom(classOf[Vector[_]])
        if (classOf[Seq[_]].isAssignableFrom(c) && (isList || isVector) &&
          parType != null && classOf[Dto].isAssignableFrom(parType.runtimeClass)) {
          val chClass = parType.runtimeClass
          val res = v.elements
            .map(o => chClass.getConstructor().newInstance().asInstanceOf[QDto]
              .fill(o.asInstanceOf[JsObject], emptyStringsToNull))
          if(isList) res.toList else res
        } else if (parType != null &&
          parType.toString == "java.lang.String" &&
          classOf[Seq[_]].isAssignableFrom(c) && (isList || isVector) )  {

          val res = v.elements.map{
            case JsString(s) => s
            case JsNull => null
            case n => sys.error("Can not parse "+n.getClass+" value "+n)
          }

          if(isList) res.toList else res

        } else throwUnsupportedType
      case JsNull => null
    }
    parseFunc
  }

  //creating dto from JsObject
  def fill(js: JsObject, emptyStringsToNull: Boolean = true)(implicit qe: QE): this.type = {
    js.fields foreach { case (name, value) =>
      setters.get(name).map { case (met, _) =>
        met.invoke(this, parseJsValue(name, emptyStringsToNull)(qe)(value).asInstanceOf[Object])
      }.getOrElse(updateDynamic(name)(JsonToAny(value)))
    }
    this
  }

  def updateDynamic(field: String)(value: Any)(implicit qe: QE) {
    if (qe.authFieldNames contains field)
      setAuthField(field, value.asInstanceOf[Boolean])
    //ignore any other field
  }

  def setAuthField(field: String, value: Boolean)(implicit qe: QE) {
    if (qe.authFieldNames contains field) auth(field) = value
    else sys.error(s"Unsupported auth field: $field")
  }
}

trait DtoWithId extends Dto with org.mojoz.querease.DtoWithId

object DefaultAppQuerease extends AppQuerease {
  override type DTO = Dto
  override type DWI = DtoWithId
}
