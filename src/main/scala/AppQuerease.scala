package org.wabase

import org.tresql._
import org.mojoz.querease.{NotFoundException, Querease}

import scala.reflect.ManifestFactory
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

trait QuereaseProvider {
  type QE <: AppQuerease
  final implicit val qe: QE = initQuerease
  /** Override this method in subclass to initialize {{{qe}}} */
  protected def initQuerease: QE
}

trait QuereaseResult
case class TresqlResult(result: Result[RowLike]) extends QuereaseResult
// TODO after decoupling QereaseIo from Querease this class should be refactored to PojoResult[X]
case class MapResult(result: Map[String, Any]) extends QuereaseResult
// TODO after decoupling QereaseIo from Querease this class should be refactored to PojoResult[X]
case class PojoResult(result: AppQuerease#DTO) extends QuereaseResult
// TODO after decoupling QereaseIo from Querease this class should be refactored to ListResult[X]
case class ListResult(result: List[Any]) extends QuereaseResult
// TODO after decoupling QereaseIo from Querease this class should be refactored to IteratorResult[X]
case class IteratorResult(result: AppQuerease#CloseableResult[AppQuerease#DTO]) extends QuereaseResult
// TODO after decoupling QereaseIo from Querease this class should be refactored to OptionResult[X]
case class OptionResult(result: Option[AppQuerease#DTO]) extends QuereaseResult
case class NumberResult(id: Long) extends QuereaseResult
case class CodeResult(code: String) extends QuereaseResult

trait AppQuereaseIo extends org.mojoz.querease.ScalaDtoQuereaseIo with JsonConverter { self: AppQuerease =>

  override type DTO >: Null <: Dto
  override type DWI >: Null <: DTO with DtoWithId

  private [wabase] val FieldRefRegexp_ = FieldRefRegexp

  def fill[B <: DTO: Manifest](jsObject: JsObject): B =
    implicitly[Manifest[B]].runtimeClass.getConstructor().newInstance().asInstanceOf[B {type QE = AppQuerease}].fill(jsObject)(this)
}

abstract class AppQuerease extends Querease with AppQuereaseIo with AppMetadata {

  import AppMetadata._
  override def get[B <: DTO](id: Long, extraFilter: String = null, extraParams: Map[String, Any] = null)(
      implicit mf: Manifest[B], resources: Resources): Option[B] = {
    val v = viewDef[B]
    val extraFilterAndAuth =
      Option((Option(extraFilter).toSeq ++ v.auth.forGet).map(a => s"($a)").mkString(" & "))
        .filterNot(_.isEmpty).orNull
    super.get(id, extraFilterAndAuth, extraParams)
  }
  override def result[B <: DTO: Manifest](params: Map[String, Any],
      offset: Int = 0, limit: Int = 0, orderBy: String = null,
      extraFilter: String = null, extraParams: Map[String, Any] = Map())(
      implicit resources: Resources): CloseableResult[B] = {
    val v = viewDef[B]
    val extraFilterAndAuth =
      Option((Option(extraFilter).toSeq ++ v.auth.forList).map(a => s"($a)").mkString(" & "))
        .filterNot(_.isEmpty).orNull
    super.result(params, offset, limit, orderBy, extraFilterAndAuth, extraParams)
  }
  override protected def countAll_(viewDef: ViewDef, params: Map[String, Any],
      extraFilter: String = null, extraParams: Map[String, Any] = Map())(implicit resources: Resources): Int = {
    val extraFilterAndAuth =
      Option((Option(extraFilter).toSeq ++ viewDef.auth.forList).map(a => s"($a)").mkString(" & "))
        .filterNot(_.isEmpty).orNull
    super.countAll_(viewDef, params, extraFilterAndAuth, extraParams)
  }
  override protected def insert[B <: DTO](
      tables: Seq[String],
      pojo: B,
      filter: String = null,
      propMap: Map[String, Any])(implicit resources: Resources): Long = {
    val v = viewDef(ManifestFactory.classType(pojo.getClass))
    val filterAndAuth =
      Option((Option(filter).toSeq ++ v.auth.forInsert).map(a => s"($a)").mkString(" & "))
        .filterNot(_.isEmpty).orNull
    super.insert(tables, pojo, filterAndAuth, propMap)
  }
  override protected def update[B <: DTO](
      tables: Seq[String],
      pojo: B,
      filter: String,
      propMap: Map[String, Any])(implicit resources: Resources): Unit = {
    val v = viewDef(ManifestFactory.classType(pojo.getClass))
    val filterAndAuth =
      Option((Option(filter).toSeq ++ v.auth.forUpdate).map(a => s"($a)").mkString(" & "))
        .filterNot(_.isEmpty).orNull
    super.update(tables, pojo, filterAndAuth, propMap)
  }

  // TODO after decoupling QereaseIo from Querease this method should be removed
  def saveMap(view: ViewDef,
           data: Map[String, Any],
           forceInsert: Boolean = false,
           filter: String = null,
           params: Map[String, Any])(implicit resources: Resources): Long = {
    val mf = ManifestFactory.classType(viewNameToClassMap(view.name)).asInstanceOf[Manifest[DTO]]
    val dto = fill(data.toJson.asJsObject)(mf)
    save(dto, null, forceInsert, filter, params)
  }

  override def delete[B <: DTO](instance: B, filter: String = null, params: Map[String, Any] = null)(
    implicit resources: Resources) = {
    val v = viewDef(ManifestFactory.classType(instance.getClass))
    val filterAndAuth =
      Option((Option(filter).toSeq ++ v.auth.forDelete).map(a => s"($a)").mkString(" & "))
        .filterNot(_.isEmpty).orNull
    super.delete(instance, filterAndAuth, params)
  }

  // TODO after decoupling QereaseIo from Querease this method should be removed
  def deleteById(view: ViewDef, id: Any, filter: String = null, params: Map[String, Any] = null)(
    implicit resources: Resources): Int = {
    val filterAndAuth =
      Option((Option(filter).toSeq ++ view.auth.forDelete).map(a => s"($a)").mkString(" & "))
        .filterNot(_.isEmpty).orNull
    val result = ORT.delete(
      view.table + Option(view.tableAlias).map(" " + _).getOrElse(""),
      id,
      filterAndAuth,
      params) match { case r: DeleteResult => r.count.get }
    if (result == 0)
      throw new NotFoundException(s"Record not deleted in table ${view.table}")
    else result
  }

  def doAction(view: String,
               actionName: String,
               data: Map[String, Any],
               env: Map[String, Any])(
    implicit resources: Resources, ec: ExecutionContext): Future[QuereaseResult] = {
    import Action._
    def doStep(step: Step): Future[QuereaseResult] = step match {
      case Evaluation(name, ops) => Future.successful(null)
      case Return(name, ops) => Future.successful(null)
      case Validations(validations) => Future.successful(null)
    }
    def doSteps(steps: List[Step],
                curRes: Future[QuereaseResult]): Future[QuereaseResult] = {
      steps match {
        case Nil => curRes
        case l => curRes.flatMap {
          case x @ TresqlResult(tr) => Future.successful(x)
          //case PojoResult
        }
      }
    }
    val vd = viewDef(view)
    vd.actions.get(actionName)
      .map(a => doSteps(a.steps, Future.successful(MapResult(data))))
      .getOrElse(Future.successful(doViewCall(actionName, view, data, env)))
  }

  protected def doViewCall(method: String,
                           view: String,
                           data: Map[String, Any],
                           env: Map[String, Any])(implicit res: Resources): QuereaseResult = {
    import Action._
    val v = viewDef(view)
    implicit val mf = ManifestFactory.classType(viewNameToClassMap(v.name)).asInstanceOf[Manifest[DTO]]
    def long(name: String) = data.get(name).map {
      case x: Long => x
      case x: Number => x.longValue
      case x: String => x.toLong
      case x => x.toString.toLong
    }
    def int(name: String) = data.get(name).map {
      case x: Int => x
      case x: Number => x.intValue
      case x: String => x.toInt
      case x => x.toString.toInt
    }
    def string(name: String) = data.get(name) map String.valueOf
    method match {
      case Get =>
        OptionResult(long("id").flatMap(get(_, null, data)(mf, res)))
      case Action.List =>
        IteratorResult(result(data, int(OffsetKey).getOrElse(0), int(LimitKey).getOrElse(0),
          string(OrderKey).orNull)(mf, res))
      case Save =>
        NumberResult(saveMap(v, data, false, null, env))
      case Delete =>
        NumberResult(long("id")
          .map { deleteById(v, _, null, env) }
          .getOrElse(sys.error(s"id not found in data")))
      case Create =>
        PojoResult(create(data)(mf, res))
      case x =>
        sys.error(s"Unknown view action $x")
    }
  }

  protected def doInvocation(className: String,
                             function: String,
                             data: Map[String, Any],
                             env: Map[String, Any])(
                            implicit res: Resources,
                            ec: ExecutionContext): Future[QuereaseResult] = {
    def qresult(r: Any) = r match {
      case m: Map[String, Any]@unchecked => MapResult(m)
      case l: List[Any] => ListResult(l)
      case r: Result[_] => TresqlResult(r)
      case l: Long => NumberResult(l)
      case s: String => CodeResult(s)
      case r: CloseableResult[DTO]@unchecked => IteratorResult(r)
      case d: DTO@unchecked => PojoResult(d)
      case o: Option[DTO]@unchecked => OptionResult(o)
      case x => sys.error(s"Unrecognized result type: ${x.getClass}, value: $x")
    }
    val clazz = Class.forName(className)
    Try {
      clazz
        .getMethod(function, classOf[Map[_, _]], classOf[Map[_, _]], classOf[Resources])
        .invoke(clazz.newInstance, data, env, res)
    }.recover {
      case _: NoSuchMethodException =>
        clazz
          .getMethod(function, classOf[Map[_, _]], classOf[Map[_, _]])
          .invoke(clazz.newInstance, data, env)
    }.map {
      case f: Future[_] => f map qresult
      case x => Future.successful(qresult(x))
    }.get
  }

  protected def doActionOp(op: Action.Op, data: Map[String, Any], env: Map[String, Any])(
    implicit res: Resources, ec: ExecutionContext): Future[QuereaseResult] = {
    import Action._
    op match {
      case Tresql(tresql) =>
        Future.successful(TresqlResult(Query(tresql)(res.withParams(data))))
      case ViewCall(method, view) =>
        Future.successful(doViewCall(method, view, data, env))
      case Invocation(className, function) =>
        doInvocation(className, function, data, env)
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

  def updateDynamic(field: String)(value: Any)(implicit qe: QE): Unit = {
    if (qe.authFieldNames contains field)
      setAuthField(field, value.asInstanceOf[Boolean])
    //ignore any other field
  }

  def setAuthField(field: String, value: Boolean)(implicit qe: QE): Unit = {
    if (qe.authFieldNames contains field) auth(field) = value
    else sys.error(s"Unsupported auth field: $field")
  }
}

trait DtoWithId extends Dto with org.mojoz.querease.DtoWithId

object DefaultAppQuerease extends AppQuerease {
  override type DTO = Dto
  override type DWI = DtoWithId
}
