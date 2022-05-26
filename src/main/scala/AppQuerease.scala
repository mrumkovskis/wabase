package org.wabase

import org.mojoz.querease.QuereaseExpressions.DefaultParser
import org.tresql._
import org.mojoz.querease.{NotFoundException, Querease, QuereaseExpressions, ValidationException, ValidationResult}
import org.mojoz.querease.SaveMethod
import org.mojoz.metadata.Type
import org.tresql.parsing.{Exp, Fun, Join, Obj, Variable, With, Query => PQuery}
import org.wabase.AppMetadata.Action.{VariableTransform, VariableTransforms}

import scala.reflect.ManifestFactory
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Try}
import scala.util.control.NonFatal

trait QuereaseProvider {
  type QE <: AppQuerease
  final implicit val qe: QE = initQuerease
  /** Override this method in subclass to initialize {{{qe}}} */
  protected def initQuerease: QE
}

sealed trait QuereaseResult
sealed trait QuereaseCloseableResult extends QuereaseResult
case class TresqlResult(result: Result[RowLike]) extends QuereaseCloseableResult
case class TresqlSingleRowResult(row: RowLike) extends QuereaseCloseableResult
case class MapResult(result: Map[String, Any]) extends QuereaseResult
case class ListResult(result: List[AppQuerease#DTO]) extends QuereaseResult
// TODO after decoupling QereaseIo from Querease this class should be refactored to PojoResult[X]
case class PojoResult(result: AppQuerease#DTO) extends QuereaseResult
// TODO after decoupling QereaseIo from Querease this class should be refactored to IteratorResult[X]
case class IteratorResult(result: AppQuerease#QuereaseIteratorResult[AppQuerease#DTO]) extends QuereaseCloseableResult
// TODO after decoupling QereaseIo from Querease this class should be refactored to OptionResult[X]
case class OptionResult(result: Option[AppQuerease#DTO]) extends QuereaseResult
case class NumberResult(id: Long) extends QuereaseResult
case class CodeResult(code: String) extends QuereaseResult
case class IdResult(id: Any) extends QuereaseResult
case class QuereaseDeleteResult(count: Int) extends QuereaseResult
case class RedirectResult(uri: String, key: Seq[Any] = Nil, params: Map[String, Any] = Map()) extends QuereaseResult
case object NoResult extends QuereaseResult
case class QuereaseResultWithCleanup(result: QuereaseCloseableResult, cleanup: Option[Throwable] => Unit)
  extends QuereaseResult {
  def flatMap(f: QuereaseCloseableResult => QuereaseResult): QuereaseResult = {
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
case class QuereaseSerializedResult(result: SerializedResult, isCollection: Boolean) extends QuereaseResult

trait AppQuereaseIo extends org.mojoz.querease.ScalaDtoQuereaseIo with JsonConverter { self: AppQuerease =>

  override type DTO >: Null <: Dto
  override type DWI >: Null <: DTO with DtoWithId

  private [wabase] val FieldRefRegexp_ = FieldRefRegexp

  def fill[B <: DTO: Manifest](jsObject: JsObject): B = {
    implicitly[Manifest[B]].runtimeClass.getConstructor().newInstance().asInstanceOf[B {type QE = AppQuerease}].fill(jsObject)(this)
  }
  def fill[B <: DTO: Manifest](values: Map[String, Any]): B = {
    implicitly[Manifest[B]].runtimeClass.getConstructor().newInstance().asInstanceOf[B {type QE = AppQuerease}].fill(values)(this)
  }
}

class QuereaseEnvException(val env: Map[String, Any], cause: Exception) extends Exception(cause) {
  override def getMessage: String = s"Error occured while processing env: ${cause.getMessage}. Env: ${
    String.valueOf(env)}"
}

abstract class AppQuerease extends Querease with AppQuereaseIo with AppMetadata with Loggable {

  import AppMetadata._

  private lazy val typeNameToScalaTypeName =
    typeDefs
      .map(td => td.name -> td.targetNames.get("scala").orNull)
      .filter(_._2 != null)
      .toMap
  def convertToType(type_ : Type, value: Any): Any = {
    value match {
      case s: String =>
        (typeNameToScalaTypeName.get(type_.name).orNull match {
          case "String"             => s
          case "java.lang.Long"     => s.toLong
          case "java.lang.Integer"  => s.toInt
          case "java.sql.Date"      => new java.sql.Date(Format.parseDate(s).getTime)
          case "java.sql.Timestamp" => new java.sql.Timestamp(Format.parseDateTime(s).getTime)
          case "BigInt"             => BigInt(s)
          case "BigDecimal"         => BigDecimal(s)
          case "java.lang.Double"   => s.toDouble
          case "java.lang.Boolean"  => s.toBoolean
          case _ /* "Array[Byte]" */=> s
        })
      case x => x
    }
  }

  override protected def persistenceFilters(
    view: ViewDef,
  ): OrtMetadata.Filters = {
    OrtMetadata.Filters(
      insert = Option(view.auth.forInsert).filter(_.nonEmpty).map(_.map(a => s"($a)").mkString(" & ")),
      update = Option(view.auth.forUpdate).filter(_.nonEmpty).map(_.map(a => s"($a)").mkString(" & ")),
      delete = Option(view.auth.forDelete).filter(_.nonEmpty).map(_.map(a => s"($a)").mkString(" & ")),
    )
  }
  override def get[B <: DTO](
    keyValues:   Seq[Any],
    keyColNames: Seq[String],
    extraFilter: String,
    extraParams: Map[String, Any],
  )(implicit mf: Manifest[B], resources: Resources): Option[B] = {
    val v = viewDef[B]
    val extraFilterAndAuth =
      Option((Option(extraFilter).toSeq ++ v.auth.forGet).map(a => s"($a)").mkString(" & "))
        .filterNot(_.isEmpty).orNull
    super.get(keyValues, keyColNames, extraFilterAndAuth, extraParams)
  }
  override def result[B <: DTO: Manifest](params: Map[String, Any],
      offset: Int = 0, limit: Int = 0, orderBy: String = null,
      extraFilter: String = null, extraParams: Map[String, Any] = Map())(
      implicit resources: Resources): QuereaseIteratorResult[B] = {
    val v = viewDef[B]
    val extraFilterAndAuth =
      Option((Option(extraFilter).toSeq ++ v.auth.forList).map(a => s"($a)").mkString(" & "))
        .filterNot(_.isEmpty).orNull
    // TODO call super and move rows below to Querease later
    val (q, p) = queryStringAndParams(viewDef[B], params,
      offset, limit, orderBy, extraFilterAndAuth, extraParams)
    result(q, p)
  }
  // TODO move this method to Querease later
  override def result[B <: DTO: Manifest](query: String, params: Map[String, Any])(
    implicit resources: Resources): QuereaseIteratorResult[B] =
    new QuereaseIteratorResult[B] {
      private val result = Query(query, params)
      override def hasNext = result.hasNext
      override def next() = convertRow[B](result.next())
      override def close = result.close
      override def view: ViewDef = viewDef[B]
    }

  override protected def countAll_(viewDef: ViewDef, params: Map[String, Any],
      extraFilter: String = null, extraParams: Map[String, Any] = Map())(implicit resources: Resources): Int = {
    val extraFilterAndAuth =
      Option((Option(extraFilter).toSeq ++ viewDef.auth.forList).map(a => s"($a)").mkString(" & "))
        .filterNot(_.isEmpty).orNull
    super.countAll_(viewDef, params, extraFilterAndAuth, extraParams)
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
                                     closeResources: (Resources, Option[Throwable]) => Unit): QuereaseAction[QuereaseResult] = {
      implicit ec: ExecutionContext => {
        implicit val res = initResources(viewName)(actionName)
        try {
          doAction(viewName, actionName, data, env).map {
            case TresqlResult(r: DMLResult) =>
              closeResources(res, None)
              r match {
                case _: InsertResult | _: UpdateResult => IdResult(r.id)
                case _: DeleteResult => QuereaseDeleteResult(r.count.getOrElse(0))
              }
            case r: QuereaseCloseableResult => QuereaseResultWithCleanup(r, closeResources(res, _))
            case r: QuereaseResult =>
              closeResources(res, None)
              r
            }
            .andThen {
              case Failure(NonFatal(exception)) => closeResources(res, Option(exception))
            }
        } catch { // catch exception also here in the case doAction is not executed into separate thread
          case NonFatal(e) =>
            closeResources(res, Option(e))
            throw e
        }
      }
    }
    def value[A](a: => A): QuereaseAction[A] = (_: ExecutionContext) => Future.successful(a)
  }

  trait QuereaseIteratorResult[+B <: DTO] extends Iterator[B] with AutoCloseable {
    def view: ViewDef
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
      case TresqlSingleRowResult(row) => row.toMap
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
      case x => sys.error(s"${x.getClass.getName} not expected here!")
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
          case SetEnv(_, vts, op) =>
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
      case s :: Nil => doStep(s, curData)
      case s :: tail =>
        doStep(s, curData) flatMap { stepRes =>
          s match {
            case e: Evaluation =>
              doSteps(tail, context, curData.map(updateCurRes(_, e.name, dataForNewStep(stepRes))))
            case se: SetEnv =>
              val newData =
                dataForNewStep(stepRes) match {
                  case m: Map[String, Any]@unchecked => Future.successful(m)
                  case x =>
                    //in the case of primitive value return step must have name
                    se.name.map(n => Future.successful(Map(n -> x))).getOrElse(curData)
                }
              doSteps(tail, context, newData)
            case _: Return => Future.successful(stepRes)
            case _ => doSteps(tail, context, curData)
          }
        }
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
                         context: Option[ViewDef])(implicit resources: Resources): TresqlResult = {
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

  private[wabase] val onSaveDoActionNameKey = s"on save do"

  protected def doViewCall(method: String,
                           view: String,
                           data: Map[String, Any],
                           env: Map[String, Any],
                           context: Option[ViewDef])(implicit res: Resources,
                                                     ec: ExecutionContext): Future[QuereaseResult] = {
    import Action._
    import CoreTypes._
    val callData = data ++ env
    val v = viewDef(if (view.startsWith(":")) singleValue[String](view, callData) else view)
    implicit val mf = ManifestFactory.classType(viewNameToClassMap(v.name)).asInstanceOf[Manifest[DTO]]
    def keyValuesAndColNames(props: Map[String, Any]) = tryOp({
      val keyFields = viewNameToKeyFields(view)
      val keyColNames = keyFields.map(_.name)
      val keyFieldNames = viewNameToKeyFieldNames(view)
      val keyValues = tryOp(
        keyFieldNames.map(n => props.getOrElse(n, sys.error(s"Mapping not found for key field $n of view $view")))
          .zip(keyFields).map { case (v, f) => convertToType(f.type_, v) },
        props
      )
      (keyValues, keyColNames)
    }, callData)
    def int(name: String) = tryOp(callData.get(name).map {
      case x: Int => x
      case x: Number => x.intValue
      case x: String => x.toInt
      case x => x.toString.toInt
    }, callData)
    def string(name: String) = callData.get(name) map String.valueOf
    if (context.exists(_.name == view)) {
      Future.successful {
        method match {
          case Get =>
            val (keyValues, keyColNames) = keyValuesAndColNames(callData)
            OptionResult(get(keyValues, keyColNames, null, callData)(mf, res))
          case Action.List =>
            IteratorResult(result(callData, int(OffsetKey).getOrElse(0), int(LimitKey).getOrElse(0),
              string(OrderKey).orNull)(mf, res))
          case Save =>
            val saveMethod = callData.getOrElse(onSaveDoActionNameKey, null) match {
              case Insert => SaveMethod.Insert
              case Update => SaveMethod.Update
              case Upsert => SaveMethod.Upsert
              case _      => SaveMethod.Save
            }
            IdResult(save(v, callData, null, saveMethod,        null, env))
          case Insert =>
            IdResult(save(v, callData, null, SaveMethod.Insert, null, env))
          case Update =>
            IdResult(save(v, callData, null, SaveMethod.Update, null, env))
          case Upsert =>
            IdResult(save(v, callData, null, SaveMethod.Upsert, null, env))
          case Delete =>
            keyValuesAndColNames(data) // check mappings for key exist
            NumberResult(delete(v, data, null, env))
          case Create =>
            PojoResult(create(callData)(mf, res))
          case Count =>
            NumberResult(countAll(callData))
          case x =>
            sys.error(s"Unknown view action $x")
        }
      }
    } else {
      doAction(view, method, data, env)
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
      case m: java.util.Map[String, Any]@unchecked => MapResult(m.asScala.toMap)
      case l: List[DTO@unchecked] => ListResult(l)
      case r: Result[_] => TresqlResult(r)
      case l: Long => NumberResult(l)
      case s: String => CodeResult(s)
      case r: QuereaseIteratorResult[DTO]@unchecked => IteratorResult(r)
      case d: DTO@unchecked => PojoResult(d)
      case o: Option[DTO]@unchecked => OptionResult(o)
      case x => sys.error(s"Unrecognized result type: ${x.getClass}, value: $x from function $className.$function")
    }
    def param(parType: Class[_]) = {
      if (classOf[Dto].isAssignableFrom(parType))
        fill(data.toJson.asJsObject)(Manifest.classType(parType)) // specify manifest explicitly so it is not Nothing
      else if (parType.isAssignableFrom(classOf[java.util.Map[_, _]]))
        data.asJava
      else data
    }

    val clazz = Class.forName(className)
    clazz.getMethods.find(_.getName == function).map { method =>
      val parTypes = method.getParameterTypes
      val parCount = parTypes.length
      val obj = clazz.getDeclaredConstructor().newInstance()
      if (parCount == 0) method.invoke(obj)
      else if (parCount == 1) method.invoke(obj, param(parTypes(0)))
      else if (parCount == 2) method.invoke(obj, param(parTypes(0)), res)
    }.map {
      case f: Future[_] => f map qresult
      case x => Future.successful(qresult(x))
    }.getOrElse(sys.error(s"Method $function not found in class $className"))
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
        doViewCall(method, view, data, env, context)
      case UniqueOpt(innerOp) =>
        def createGetResult(res: QuereaseResult): QuereaseResult = res match {
          case TresqlResult(r) if !r.isInstanceOf[DMLResult] =>
            r.uniqueOption map TresqlSingleRowResult getOrElse OptionResult(None)
          case IteratorResult(r) =>
            try r.hasNext match {
              case true =>
                val v = r.next()
                if (r.hasNext) sys.error("More than one row for unique result") else OptionResult(Option(v))
              case false => OptionResult(None)
            } finally r.close
          case r => r
        }
        doActionOp(innerOp, data, env, context) map createGetResult
      case Invocation(className, function) =>
        doInvocation(className, function, data ++ env)
      case Redirect(pathAndKeyTresql, paramsTresql) =>
        val uriAndKeys = doTresql(pathAndKeyTresql, data ++ env, context).result.unique.values
        val params =
          if (paramsTresql == null) Map[String, Any]()
          else doTresql(paramsTresql, data ++ env, context).result.unique.toMap
        Future.successful(RedirectResult(String.valueOf(uriAndKeys.head), uriAndKeys.tail, params))
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
      value: Any, targetType: Manifest[_], fieldName: String, cause: Throwable = null): Unit = {
    throw new UnprocessableEntityException(
      "Illegal value or unsupported type conversion from %s to %s - failed to populate %s", cause,
       value.getClass.getName, targetType.toString, s"${getClass.getName}.$fieldName")
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
  def fill(js: JsObject)(implicit qe: QE): this.type = fill(js, emptyStringsToNull = true)(qe)
  def fill(js: JsObject, emptyStringsToNull: Boolean)(implicit qe: QE): this.type = {
    js.fields foreach { case (name, value) =>
      setters.get(name).map { case (met, _) =>
        met.invoke(this, parseJsValue(name, emptyStringsToNull)(qe)(value).asInstanceOf[Object])
      }
    }
    this
  }

  //creating dto from Map[String, Any]
  def fill(values: Map[String, Any])(implicit qe: QE): this.type = fill(values, emptyStringsToNull = true)(qe)
  def fill(values: Map[String, Any], emptyStringsToNull: Boolean)(implicit qe: QE): this.type = {
    values foreach { case (name, value) =>
      setters.get(name).map { case (met, (typ, parType)) =>
        val converted = value match {
          case s: String if emptyStringsToNull && s.trim == "" => null
          case m: Map[String@unchecked, _] =>
            typ.runtimeClass.getConstructor().newInstance().asInstanceOf[QDto].fill(m, emptyStringsToNull)
          case s: Seq[_] =>
            val c = typ.runtimeClass
            val isList = c.isAssignableFrom(classOf[List[_]])
            val isVector = c.isAssignableFrom(classOf[Vector[_]])
            if (classOf[Seq[_]].isAssignableFrom(c) && (isList || isVector) &&
              parType != null && classOf[Dto].isAssignableFrom(parType.runtimeClass)) {
              val chClass = parType.runtimeClass
              val res =
                s.map(o => chClass.getConstructor().newInstance().asInstanceOf[QDto]
                  .fill(o.asInstanceOf[Map[String, Any]], emptyStringsToNull))
              if (isList) res.toList else res
            } else s
          case x => x
        }
        try met.invoke(this, converted.asInstanceOf[Object]) catch {
          case util.control.NonFatal(ex) =>
            throwUnsupportedConversion(value, typ, name)
        }
      }
    }
    this
  }
}

trait DtoWithId extends Dto with org.mojoz.querease.DtoWithId

object DefaultAppQuerease extends AppQuerease {
  override type DTO = Dto
  override type DWI = DtoWithId
}
