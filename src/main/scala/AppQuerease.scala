package org.wabase

import org.mojoz.querease.QuereaseExpressions.DefaultParser
import org.tresql._
import org.mojoz.querease.{NotFoundException, Querease, QuereaseExpressions, ValidationException, ValidationResult}
import org.tresql.parsing.{Exp, Fun, Join, Obj, Variable, With, Query => PQuery}
import org.wabase.AppMetadata.Action.{VariableTransform, VariableTransforms}

import scala.reflect.ManifestFactory
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal

trait QuereaseProvider {
  type QE <: AppQuerease
  final implicit val qe: QE = initQuerease
  /** Override this method in subclass to initialize {{{qe}}} */
  protected def initQuerease: QE
}

sealed trait QuereaseResult
case class TresqlResult(result: Result[RowLike]) extends QuereaseResult
// TODO after decoupling QereaseIo from Querease this class should be refactored to PojoResult[X]
case class MapResult(result: Map[String, Any]) extends QuereaseResult
// TODO after decoupling QereaseIo from Querease this class should be refactored to PojoResult[X]
case class PojoResult(result: AppQuerease#DTO) extends QuereaseResult
// TODO after decoupling QereaseIo from Querease this class should be refactored to ListResult[X]
case class ListResult(result: List[Any]) extends QuereaseResult
// TODO after decoupling QereaseIo from Querease this class should be refactored to IteratorResult[X]
case class IteratorResult(result: Iterator[AppQuerease#DTO] with AutoCloseable) extends QuereaseResult
// TODO after decoupling QereaseIo from Querease this class should be refactored to OptionResult[X]
case class OptionResult(result: Option[AppQuerease#DTO]) extends QuereaseResult
case class NumberResult(id: Long) extends QuereaseResult
case class CodeResult(code: String) extends QuereaseResult
case class IdResult(id: Any) extends QuereaseResult
case class RedirectResult(uri: String) extends QuereaseResult
case object NoResult extends QuereaseResult
case class DeferredQuereaseResult[T](result: Iterator[T], cleanup: Option[Throwable] => Unit) extends QuereaseResult {
  def flatMap(f: Iterator[T] => QuereaseResult): QuereaseResult = {
    Try(f(result)).map { r =>
      cleanup(None)
      r
    }.recover {
      case NonFatal(e) =>
        cleanup(Option(e))
        throw e
    }.get
  }
}

trait AppQuereaseIo extends org.mojoz.querease.ScalaDtoQuereaseIo with JsonConverter { self: AppQuerease =>

  override type DTO >: Null <: Dto
  override type DWI >: Null <: DTO with DtoWithId

  private [wabase] val FieldRefRegexp_ = FieldRefRegexp

  def fill[B <: DTO: Manifest](jsObject: JsObject): B =
    implicitly[Manifest[B]].runtimeClass.getConstructor().newInstance().asInstanceOf[B {type QE = AppQuerease}].fill(jsObject)(this)
}

class QuereaseEnvException(val env: Map[String, Any], cause: Exception) extends Exception(cause) {
  override def getMessage: String = s"Error occured while processing env: ${cause.getMessage}. Env: ${
    String.valueOf(env)}"
}

abstract class AppQuerease extends Querease with AppQuereaseIo with AppMetadata with Loggable {

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
              params: Map[String, Any],
              forceInsert: Boolean = false,
              filter: String = null)(implicit resources: Resources): Long = {
    val mf = ManifestFactory.classType(viewNameToClassMap(view.name)).asInstanceOf[Manifest[DTO]]
    val dto = fill(data.toJson.asJsObject)(mf)
    save(dto, null, forceInsert, filter, data ++ params)
  }
  def extractKeyMap(viewName: String, instance: Map[String, Any]): Map[String, Any] =
    // TODO extractKeyMap - support any key (i.e. not only "id"). Move to querease?
    Option(instance)
      .filter(_ => classOf[DtoWithId] isAssignableFrom viewNameToClassMap(viewName))
      .map(_ => Map("id" -> instance.get("id").orNull).filter(_._2 != null))
      .getOrElse(Map.empty)

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

  private def tryOp[T](op: => T, env: Map[String, Any]) = try op catch {
    case e: Exception => throw new QuereaseEnvException(env, e)
  }

  /********************************
   ******** Querease actions ******
  *********************************/
  trait QuereaseAction[A] {
    def run(implicit ec: ExecutionContext): Future[A]
    def map[B](f: A => B)(implicit ec: ExecutionContext): QuereaseAction[B] =
      (_: ExecutionContext) => QuereaseAction.this.run.map(f)
    def flatMap[B](f: A => QuereaseAction[B])(implicit ec: ExecutionContext): QuereaseAction[B] =
      (_: ExecutionContext) => QuereaseAction.this.run.flatMap(f(_).run)
    def andThen[U](pf: PartialFunction[Try[A], U])(implicit ec: ExecutionContext): QuereaseAction[A] =
      (_: ExecutionContext) => QuereaseAction.this.run.andThen(pf)
    def recover[U >: A](pf: PartialFunction[Throwable, U])(implicit ec: ExecutionContext): QuereaseAction[U] =
      (_: ExecutionContext) => QuereaseAction.this.run.recover(pf)
  }
  object QuereaseAction {
    def apply(viewName: String,
              actionName: String,
              data: Map[String, Any],
              env: Map[String, Any])(initResources: String => String => Resources, // viewName, actionName
                                     closeResources: Resources => Option[Throwable] => Unit): QuereaseAction[QuereaseResult] = {
      implicit ec: ExecutionContext => {
        implicit val res = initResources(viewName)(actionName)
        try {
          doAction(viewName, actionName, data, env).map {
            case TresqlResult(r) => DeferredQuereaseResult(r, closeResources(res))
            case IteratorResult(r) => DeferredQuereaseResult(r, closeResources(res))
            case r: QuereaseResult =>
              closeResources(res)(None)
              r
          }
        } catch {
          case NonFatal(e) =>
            closeResources(res)(Option(e))
            throw e
        }
      }
    }
    def value[A](a: => A): QuereaseAction[A] = (_: ExecutionContext) => Future.successful(a)
  }

  case class ActionContext(name: String, env: Map[String, Any], view: Option[ViewDef])
  def doAction(view: String,
               actionName: String,
               data: Map[String, Any],
               env: Map[String, Any])(
      implicit resources: Resources, ec: ExecutionContext): Future[QuereaseResult] = {
    val vd = viewDef(view)

    val ctx = ActionContext(s"$view.$actionName", env, Some(vd))
    logger.debug(s"Doing action '${ctx.name}'.\n Env: $env")
    val steps =
      vd.actions.get(actionName)
        .map(_.steps)
        .getOrElse(List(Action.Return(None, Nil, Action.ViewCall(actionName, view))))
    doSteps(steps, ctx, Future.successful(data))
  }

  protected def doSteps(steps: List[Action.Step],
                        context: ActionContext,
                        curData: Future[Map[String, Any]])(
      implicit resources: Resources, ec: ExecutionContext): Future[QuereaseResult] = {
    import Action._

    def dataForNewStep(res: QuereaseResult) = res match {
      case TresqlResult(tr) => tr match {
        case dml: DMLResult =>
          dml.id.map(IdResult(_)) orElse dml.count getOrElse 0
        case SingleValueResult(v) => v
        case ar: ArrayResult[_] => ar.values.toList
        case r: Result[_] => r.toListOfMaps match {
          case row :: Nil if row.size == 1 => row.head._2 // unwrap if result is one row, one column
          case rows => rows
        }
      }
      case MapResult(mr) => mr
      case PojoResult(pr) => pr.toMap(this)
      case ListResult(lr) => lr
      case IteratorResult(ir) => ir.map(_.toMap(this)).toList
      case OptionResult(or) => or.map(_.toMap(this)).getOrElse(null)
      case NumberResult(nr) => nr
      case CodeResult(code) => code
      case id: IdResult => id
      case rd: RedirectResult => rd
      case NoResult => NoResult
      case x: DeferredQuereaseResult[_] => sys.error(s"${x.getClass.getName} not expected here!")
    }
    def updateCurRes(cr: Map[String, Any], key: Option[String], res: Any) = res match {
      case IdResult(id) => cr
        .get("id")
        .filter(i => i != null && i != id)
        .flatMap(_ => key)
        .map(k => cr + (k -> Map("id" -> id)))
        .getOrElse(cr + ("id" -> id))
      case NoResult => cr
      case r => key.map(k => cr + (k -> r)).getOrElse(cr)
    }
    def doStep(step: Step, stepDataF: Future[Map[String, Any]]): Future[QuereaseResult] = {
      stepDataF flatMap { stepData =>
        logger.debug(s"Doing action '${context.name}' step '$step'.\nData: $stepData")
        step match {
          case Evaluation(_, vts, op) =>
            doActionOp(op, doVarsTransforms(vts, stepData, stepData).result, context.env, context.view)
          case Return(_, vts, op) =>
            doActionOp(op, doVarsTransforms(vts, stepData, stepData).result, context.env, context.view)
          case Validations(_, validations, db) =>
            context.view.map { vd =>
              Future(doValidationStep(validations, db, stepData ++ context.env, vd))
                .map(_ => MapResult(stepData))
            }.getOrElse(Future.failed(
              new RuntimeException(s"Validation cannot be performed without view in context -" +
                s"(${context.name})")))
        }
      }
    }

    steps match {
      case Nil => curData map MapResult
      case s :: Nil => doStep(s, curData) flatMap { finalRes =>
        s match {
          case e: Evaluation =>
            doSteps(Nil, context, curData.map(updateCurRes(_, e.name, dataForNewStep(finalRes))))
          case _ => Future.successful(finalRes)
        }
      }
      case s :: tail =>
        val newData =
          doStep(s, curData) flatMap { stepRes =>
            s match {
              case e: Evaluation =>
                curData.map(updateCurRes(_, e.name, dataForNewStep(stepRes)))
              case r: Return => dataForNewStep(stepRes) match {
                case m: Map[String, Any]@unchecked => Future.successful(m)
                case x =>
                  //in the case of primitive value return step must have name
                  r.name.map(n => Future.successful(Map(n -> x))).getOrElse(curData)
              }
              case _ => curData
            }
          }
        doSteps(tail, context, newData)
    }
  }

  protected def doValidationStep(validations: Seq[String],
                                 dbkey: Option[DbAccessKey],
                                 params: Map[String, Any],
                                 view: ViewDef)(implicit res: Resources): Unit = {
    validationsQueryString(view, params, validations) map { vs =>
      Query(dbkey.flatMap(k => Option(k.db)).map("|" + _ + ":").mkString("", "", vs), params)
        .map(_.s("msg"))
        .filter(_ != null).filter(_ != "")
        .toList match {
        case messages if messages.nonEmpty =>
          throw new ValidationException(messages.mkString("\n"), List(ValidationResult(Nil, messages)))
        case _ => ()
      }
    }
  }

  protected def doTresql(tresql: String,
                         bindVars: Map[String, Any],
                         context: Option[ViewDef])(implicit resources: Resources): QuereaseResult = {
    def maybeExpandWithBindVarsCursors(tresql: String, env: Map[String, Any]): String = {
      import CoreTypes._
      if (tresql.indexOf(Action.BindVarCursorsFunctionName) == -1) tresql
      else {
        def bindVarsCursorsCreator(bindVars: Map[String, Any]): parser.Transformer = {
          parser.transformer {
            case q @ PQuery(List(
            Obj(_, _, Join(_,
              Fun(fn @
                (Action.BindVarCursorsFunctionName | Action.BindVarCursorsForViewFunctionName),
                varPars, _, _, _), _
            ), _, _), _*), _, _, _, _, _, _) =>
              def calcBindVars(args: List[Exp]) = {
                if (args.isEmpty) bindVars else {
                  args.foldLeft(Map[String, Any]()) { (res, var_par) =>
                    var_par match {
                      case Variable(var_name, _, _) =>
                        singleValue[Any](var_par.tresql, bindVars) match {
                          case r: Map[String@unchecked, _] => res ++ r
                          case x => res + (var_name -> x)
                        }
                      case x =>
                        sys.error(s"Unsupported parameter: function parameter ${x.tresql} in tresql: $tresql")
                    }
                  }
                }
              }
              val (vd, bv) =
                if (fn == Action.BindVarCursorsFunctionName) {
                  context.getOrElse(sys.error(s"view context missing for tresql: $tresql")) ->
                    calcBindVars(varPars)
                } else {
                  varPars.headOption
                    .map(p => singleValue[String](p.tresql, bindVars))
                    .map(viewDef)
                    .getOrElse(sys.error(s"view name parameter missing in tresql function ${
                      Action.BindVarCursorsForViewFunctionName
                    }: $tresql")) -> calcBindVars(varPars.tail)
                }
              parser.parseExp(cursorsFromViewBindVars(bv, vd)) match {
                case With(tables, _) => With(tables, q)
                case _ => q
              }
            case With(tables, query) => bindVarsCursorsCreator(bindVars)(query) match {
              case w: With => With(w.tables ++ tables, w.query) //put bind var cursor tables first
              case q: Exp => With(tables, q)
            }
          }
        }
        bindVarsCursorsCreator(env)(parser.parseExp(tresql)).tresql
      }
    }

    TresqlResult(Query(maybeExpandWithBindVarsCursors(tresql, bindVars))(
      resources.withParams(bindVars)))
  }

  protected def doViewCall(method: String,
                           view: String,
                           data: Map[String, Any],
                           env: Map[String, Any])(implicit res: Resources): QuereaseResult = {
    import Action._
    import CoreTypes._
    val callData = data ++ env
    val v = viewDef(if (view.startsWith(":")) singleValue[String](view, callData) else view)
    implicit val mf = ManifestFactory.classType(viewNameToClassMap(v.name)).asInstanceOf[Manifest[DTO]]
    def long(name: String) = tryOp(callData.get(name).map {
      case x: Long => x
      case x: Number => x.longValue
      case x: String => x.toLong
      case x => x.toString.toLong
    }, callData)
    def int(name: String) = tryOp(callData.get(name).map {
      case x: Int => x
      case x: Number => x.intValue
      case x: String => x.toInt
      case x => x.toString.toInt
    }, callData)
    def string(name: String) = callData.get(name) map String.valueOf
    method match {
      case Get =>
        OptionResult(long("id").flatMap(get(_, null, data)(mf, res)))
      case Action.List =>
        IteratorResult(result(callData, int(OffsetKey).getOrElse(0), int(LimitKey).getOrElse(0),
          string(OrderKey).orNull)(mf, res))
      case Save =>
        IdResult(saveMap(v, callData, env, false, null))
      case Delete =>
        NumberResult(long("id")
          .map { deleteById(v, _, null, env) }
          .getOrElse(sys.error(s"id not found in data")))
      case Create =>
        PojoResult(create(callData)(mf, res))
      case Count =>
        NumberResult(countAll(callData))
      case x =>
        sys.error(s"Unknown view action $x")
    }
  }

  protected def doInvocation(className: String,
                             function: String,
                             data: Map[String, Any])(
                            implicit res: Resources,
                            ec: ExecutionContext): Future[QuereaseResult] = {
    def qresult(r: Any) = r match {
      case null => NoResult // reflection call on function with Unit (void) return type returns null
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
        .getMethod(function, classOf[Map[_, _]], classOf[Resources])
        .invoke(clazz.getDeclaredConstructor().newInstance(), data, res)
    }.recover {
      case _: NoSuchMethodException =>
        clazz
          .getMethod(function, classOf[Map[_, _]])
          .invoke(clazz.getDeclaredConstructor().newInstance(), data)
    }.map {
      case f: Future[_] => f map qresult
      case x => Future.successful(qresult(x))
    }.get
  }

  protected def doJobCall(jobName: String,
                          data: Map[String, Any],
                          env: Map[String, Any])(implicit
                                                 resources: Resources,
                                                 ec: ExecutionContext): Future[QuereaseResult] = {
    val job = null: Job
    // TODO get job from metadata
    //getJob(if (jobName.startsWith(":")) stringValue(jobName, data, env) else jobName)
    val ctx = ActionContext(job.name, env, None)
    job.condition
      .collect { case cond if Query(cond, data ++ env).unique[Boolean] =>
        doSteps(job.action.steps, ctx, Future.successful(data))
      }
      .getOrElse(Future.successful(NoResult))
  }

  protected def doVarsTransforms(transforms: List[VariableTransform],
                                 seed: Map[String, Any],
                                 data: Map[String, Any]): MapResult = {
    val transRes = transforms.foldLeft(seed) { (sd, vt) =>
      data(vt.form) match {
        case m: Map[String, _]@unchecked => sd ++ m
        case x => sd + (vt.to.getOrElse(vt.form) -> x)
      }
    }
    MapResult(transRes)
  }

  protected def doActionOp(op: Action.Op,
                           data: Map[String, Any],
                           env: Map[String, Any],
                           context: Option[ViewDef])(implicit res: Resources,
                                                     ec: ExecutionContext): Future[QuereaseResult] = {
    import Action._
    op match {
      case Tresql(tresql) =>
        Future.successful(doTresql(tresql, data ++ env, context))
      case ViewCall(method, view) =>
        Future.successful(doViewCall(method, view, data, env))
      case Invocation(className, function) =>
        doInvocation(className, function, data ++ env)
      case JobCall(job) =>
        doJobCall(job, data, env)
      case VariableTransforms(vts) =>
        Future.successful(doVarsTransforms(vts, Map[String, Any](), data ++ env))
    }
  }

  // used for example in view, job operations when name is referenced by bind variable
  private def singleValue[T: Converter: Manifest](tresql: String, data: Map[String, Any])(
      implicit res: Resources): T = {
    Query(tresql, data) match {
      case SingleValueResult(value) => value.asInstanceOf[T]
      case r: Result[_] => r.unique[T]
    }
  }

  abstract class AppQuereaseDefaultParser extends DefaultParser {
    private def varsTransform: MemParser[VariableTransform] = {
      def v2s(v: Variable) = (v.variable :: v.members) mkString "."
      (variable | ("(" ~> ident ~ "=" ~ variable <~ ")")) ^^ {
        case v: Variable => VariableTransform(v2s(v), None)
        case (v1: String) ~ _ ~ (v2: Variable) => VariableTransform(v2s(v2), Option(v1))
      }
    }
    def varsTransforms: MemParser[VariableTransforms] = {
      rep1sep(varsTransform, "+") ^^ (VariableTransforms(_)) named "var-transforms"
    }
    def stepWithVarsTransform: MemParser[(List[VariableTransform], String)] = {
      (varsTransforms ~ ("->" ~> "(?s).*".r)) ^^ {
        case VariableTransforms(vts) ~ step => vts -> step
      } named "step-with-vars-transform"
    }
  }

  object AppQuereaseDefaultParser extends AppQuereaseDefaultParser {
    override val cache = Some(new SimpleCacheBase[Exp](tresqlParserCacheSize))
  }

  val tresqlParserCacheSize: Int = 4096
  override val parser: QuereaseExpressions.Parser = this.AppQuereaseDefaultParser
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

  protected def throwUnsupportedConversion(
      v: JsValue, targetType: Manifest[_], fieldName: String, cause: Throwable = null): Unit = {
    throw new UnprocessableEntityException(
      "Illegal value or unsupported type conversion from %s to %s - failed to populate %s", cause,
       v.getClass.getName, targetType.toString, s"${getClass.getName}.$fieldName")
  }

  // TODO Drop or extract string parsing for number and boolean fields! Then rename parseJsValue() to exclude 'parse'!
  protected def parseJsValue(fieldName: String, emptyStringsToNull: Boolean)(implicit qe: QE): PartialFunction[JsValue, Any] = {
    import scala.language.existentials
    val (typ, parType) = setters(fieldName)._2
    val parseFunc: PartialFunction[JsValue, Any] = {
      case v: JsString =>
        val converted = try {
          if (ManifestFactory.singleType(v.value) == typ) {
            if (emptyStringsToNull && v.value.trim == "")
              null
            else v.value
          } else if (ManifestFactory.classType(classOf[java.sql.Date     ]) == typ) {
            val jdate = Format.parseDate(v.value)
            new java.sql.Date(jdate.getTime)
          } else if (ManifestFactory.classType(classOf[java.sql.Timestamp]) == typ) {
            val jdate = Format.parseDateTime(v.value)
            new java.sql.Timestamp(jdate.getTime)
          } else if (ManifestFactory.classType(classOf[java.lang.Long    ]) == typ || ManifestFactory.Long    == typ) {
            if (v.value.trim == "") null else v.value.toLong
          } else if (ManifestFactory.classType(classOf[java.lang.Integer ]) == typ || ManifestFactory.Int     == typ) {
            if (v.value.trim == "") null else v.value.toInt
          } else if (ManifestFactory.classType(classOf[java.lang.Double  ]) == typ || ManifestFactory.Double  == typ) {
            if (v.value.trim == "") null else v.value.toDouble
          } else if (ManifestFactory.classType(classOf[BigDecimal        ]) == typ) {
            if (v.value.trim == "") null else BigDecimal(v.value)
          } else if (ManifestFactory.classType(classOf[BigInt            ]) == typ) {
            if (v.value.trim == "") null else BigInt(v.value)
          } else if (ManifestFactory.classType(classOf[java.lang.Boolean ]) == typ || ManifestFactory.Boolean == typ) {
            v.value.toBoolean
          } else this // XXX
        } catch {
          case util.control.NonFatal(ex) =>
            throwUnsupportedConversion(v, typ, fieldName, ex)
        }
        if (converted == this)
          throwUnsupportedConversion(v, typ, fieldName)
        else converted
      case v: JsNumber =>
        val converted = try {
          if (ManifestFactory.singleType(v.value) == typ) v.value
          else if (ManifestFactory.classType(classOf[java.lang.Long   ]) == typ) v.value.longValue
          else if (ManifestFactory.Long   == typ) v.value.longValue
          else if (ManifestFactory.classType(classOf[java.lang.Integer]) == typ) v.value.intValue
          else if (ManifestFactory.Int    == typ) v.value.intValue
          else if (ManifestFactory.classType(classOf[java.lang.Double ]) == typ) v.value.doubleValue
          else if (ManifestFactory.Double == typ) v.value.doubleValue
          else if (ManifestFactory.classType(classOf[String]) == typ) v.toString
          else this // XXX
        } catch {
          case util.control.NonFatal(ex) =>
            throwUnsupportedConversion(v, typ, fieldName, ex)
        }
        if (converted == this)
          throwUnsupportedConversion(v, typ, fieldName)
        else converted
      case v: JsBoolean =>
        if (ManifestFactory.classType(classOf[java.lang.Boolean]) == typ) v.value
        else if (ManifestFactory.Boolean == typ) v.value
        else if (ManifestFactory.classType(classOf[String]) == typ) v.toString
        else throwUnsupportedConversion(v, typ, fieldName)
      case v: JsObject if typ.runtimeClass.isAssignableFrom(classOf[JsObject]) => v
      case v: JsArray if typ.runtimeClass.isAssignableFrom(classOf[JsArray]) => v
      case v: JsObject =>
        if (classOf[Dto].isAssignableFrom(typ.runtimeClass)) {
          typ.runtimeClass.getConstructor().newInstance().asInstanceOf[QDto].fill(v, emptyStringsToNull)
        } else throwUnsupportedConversion(v, typ, fieldName)
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
            case x => throwUnsupportedConversion(x, parType, fieldName)
          }

          if(isList) res.toList else res

        } else throwUnsupportedConversion(v, typ, fieldName)
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
