package org.wabase

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.mojoz.querease.QuereaseExpressions.DefaultParser
import org.tresql._
import org.mojoz.querease.{Querease, QuereaseExpressions, ValidationException, ValidationResult}
import org.mojoz.querease.SaveMethod
import org.mojoz.metadata.Type
import org.tresql.parsing.{Exp, Fun, Join, Obj, Variable, With, Query => PQuery}
import org.wabase.AppFileStreamer.FileInfo
import org.wabase.AppMetadata.Action.{VariableTransform, VariableTransforms}
import org.wabase.ResultEncoder.EncoderFactory

import scala.reflect.ManifestFactory
import spray.json._

import java.sql.Connection
import scala.collection.immutable.ListMap
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
case class TresqlSingleRowResult(row: RowLike) extends QuereaseCloseableResult {
  /** map, close row (i.e. result), return mapped */
  def map[T](f: RowLike => T): T = try f(row) finally row.close()
}
case class MapResult(result: Map[String, Any]) extends QuereaseResult
case class ListResult(result: List[AppQuerease#DTO]) extends QuereaseResult
// TODO after decoupling QereaseIo from Querease this class should be refactored to PojoResult[X]
case class PojoResult(result: AppQuerease#DTO) extends QuereaseResult
// TODO after decoupling QereaseIo from Querease this class should be refactored to IteratorResult[X]
case class IteratorResult(result: AppQuerease#QuereaseIteratorResult[AppQuerease#DTO]) extends QuereaseCloseableResult
// TODO after decoupling QereaseIo from Querease this class should be refactored to OptionResult[X]
case class OptionResult(result: Option[AppQuerease#DTO]) extends QuereaseResult
case class LongResult(value: Long) extends QuereaseResult
case class StringResult(value: String) extends QuereaseResult
case class NumberResult(value: java.lang.Number) extends QuereaseResult
case class IdResult(id: Any, name: String) extends QuereaseResult {
  def toMap: Map[String, Any] =
    if (id == null || id == 0L) Map.empty else Map((if (name == null) "id" else name) -> id)
}
case class KeyResult(ir: IdResult, viewName: String, key: Seq[Any]) extends QuereaseResult
case class QuereaseDeleteResult(count: Int) extends QuereaseResult
case class StatusResult(code: Int, value: String, key: Seq[Any] = Nil, params: ListMap[String, String] = ListMap()) extends QuereaseResult
case class FileInfoResult(fileInfo: FileInfo) extends QuereaseResult
case class FileResult(fileInfo: FileInfo, fileStreamer: FileStreamer) extends QuereaseResult
case object NoResult extends QuereaseResult
case class QuereaseResultWithCleanup(result: QuereaseCloseableResult, cleanup: Option[Throwable] => Unit)
  extends QuereaseResult {
  def map[T](f: QuereaseCloseableResult => T): T = {
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

  def convertToType(type_ : Type, value: Any): Any = {
    value match {
      case s: String =>
        (typeNameToScalaTypeName.get(type_.name).orNull match {
          case "String"             => s
          case "java.lang.Long"     => s.toLong
          case "java.lang.Integer"  => s.toInt
          case "java.sql.Date"      => new java.sql.Date(Format.parseDate(s).getTime)
          case "java.sql.Time"      => BorerDatetimeDecoders.toSqlTime(s)
          case "java.sql.Timestamp" => new java.sql.Timestamp(Format.parseDateTime(s).getTime)
          case "java.time.LocalDate"     => new java.sql.Date(Format.parseDate(s).getTime).toLocalDate
          case "java.time.LocalTime"     => BorerDatetimeDecoders.toSqlTime(s).toLocalTime
          case "java.time.LocalDateTime" => new java.sql.Timestamp(Format.parseDateTime(s).getTime).toLocalDateTime
          case "scala.math.BigInt"     => BigInt(s)
          case "scala.math.BigDecimal" => BigDecimal(s)
          case "java.lang.Double"   => s.toDouble
          case "java.lang.Boolean"  => s.toBoolean
          case _ /* "Array[Byte]" */=> s
        })
      case x => x
    }
  }

  val resultRenderers: ResultRenderers = new ResultRenderers

  override protected def persistenceFilters(
    view: ViewDef,
  ): OrtMetadata.Filters = {
    OrtMetadata.Filters(
      insert = Option(view.auth.forInsert).filter(_.nonEmpty).map(_.map(a => s"($a)").mkString(" & ")),
      update = Option(view.auth.forUpdate).filter(_.nonEmpty).map(_.map(a => s"($a)").mkString(" & ")),
      delete = Option(view.auth.forDelete).filter(_.nonEmpty).map(_.map(a => s"($a)").mkString(" & ")),
    )
  }

  private def extraFilterAndAuthString(extraFilter: String, auth: Seq[String]): String =
    Option(Option(extraFilter).filter(_ != "").toSeq ++ auth)
      .filter(_.nonEmpty).map(_.map(a => s"($a)").mkString(" & "))
      .orNull

  override def get(
    viewDef:     ViewDef,
    keyValues:   Seq[Any],
    keyColNames: Seq[String],
    extraFilter: String,
    extraParams: Map[String, Any],
  )(implicit resources: Resources): Option[RowLike] = {
    val extraFilterAndAuth =
      extraFilterAndAuthString(extraFilter, viewDef.auth.forGet)
    super.get(viewDef, keyValues, keyColNames, extraFilterAndAuth, extraParams)
  }
  override def result[B <: DTO: Manifest](params: Map[String, Any],
      offset: Int = 0, limit: Int = 0, orderBy: String = null,
      extraFilter: String = null, extraParams: Map[String, Any] = Map())(
      implicit resources: Resources): QuereaseIteratorResult[B] = {
    val v = viewDef[B]
    val extraFilterAndAuth =
      extraFilterAndAuthString(extraFilter, v.auth.forList)
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
  override def rowsResult(viewDef: ViewDef, params: Map[String, Any],
      offset: Int, limit: Int, orderBy: String, extraFilter: String)(
        implicit resources: Resources): Result[RowLike] = {
    val extraFilterAndAuth =
      extraFilterAndAuthString(extraFilter, viewDef.auth.forList)
    super.rowsResult(viewDef, params, offset, limit, orderBy, extraFilterAndAuth)
  }


  override protected def countAll_(viewDef: ViewDef, params: Map[String, Any],
      extraFilter: String = null, extraParams: Map[String, Any] = Map())(implicit resources: Resources): Int = {
    val extraFilterAndAuth =
      extraFilterAndAuthString(extraFilter, viewDef.auth.forList)
    super.countAll_(viewDef, params, extraFilterAndAuth, extraParams)
  }

  private def tryOp[T](op: => T, env: Map[String, Any]) = try op catch {
    case e: Exception => throw new QuereaseEnvException(env, e)
  }

  /* For action IdResult.name - field or column name */
  lazy val viewNameToIdName: Map[String, String] =
    nameToViewDef.map { case (name, viewDef) => (name, idName(viewDef)) }.filter(_._2 != null).toMap
  protected def idName(view: ViewDef): String = {
    def tableTo(v: ViewDef) =
      if (v.saveTo != null && v.saveTo.nonEmpty)
        v.saveTo.head
      else if (v.saveTo == Nil)
        null
      else if (v.table != null)
        v.table
      else
        null
    tableMetadata.tableDefOption(tableTo(view), view.db).flatMap { t =>
      t.pk
        .map(_.cols)
        .filter(_.size == 1)
        .map(_.head)
        .flatMap { pk =>
          view.fields.find { f => f.table == t.name && f.name == pk }
            .map(_.fieldName)
            .orElse(t.pk.map(_.cols.head))
        }
    }.orNull
  }

  protected def getKeyValues(
      viewName: String, data: Map[String, Any], forApi: Boolean = false) = tryOp({
    val keyFields     = if (forApi) viewNameToApiKeyFields(viewName)     else viewNameToKeyFields(viewName)
    val keyFieldNames = if (forApi) viewNameToApiKeyFieldNames(viewName) else viewNameToKeyFieldNames(viewName)
    val keyValues = tryOp(
      keyFieldNames.map(n => data.getOrElse(n, sys.error(s"Mapping not found for key field $n of view $viewName")))
        .zip(keyFields).map { case (v, f) =>
          try convertToType(f.type_, v)
          catch {
            case util.control.NonFatal(ex) => throw new BusinessException(
              s"Failed to convert value for key field ${f.name} to type ${f.type_.name}", ex)
          }
        },
      data
    )
    keyValues
  }, data)

  protected def keyResult(ir: IdResult, viewName: String, data: Map[String, Any]) = {
    KeyResult(ir, viewName, getKeyValues(viewName, data ++ ir.toMap, forApi = true))
  }

  /********************************
   ******** Querease actions ******
   ********************************/
  trait QuereaseAction[A] {
    def run(implicit ec: ExecutionContext, as: ActorSystem): Future[A]
    def map[B](f: A => B)(implicit ec: ExecutionContext, as: ActorSystem): QuereaseAction[B] =
      (_: ExecutionContext, _: ActorSystem) => QuereaseAction.this.run.map(f)
    def flatMap[B](f: A => QuereaseAction[B])(implicit ec: ExecutionContext, as: ActorSystem): QuereaseAction[B] =
      (_: ExecutionContext, _: ActorSystem) => QuereaseAction.this.run.flatMap(f(_).run)
    def andThen[U](pf: PartialFunction[Try[A], U])(implicit ec: ExecutionContext, as: ActorSystem): QuereaseAction[A] =
      (_: ExecutionContext, _: ActorSystem) => QuereaseAction.this.run.andThen(pf)
    def recover[U >: A](pf: PartialFunction[Throwable, U])(implicit ec: ExecutionContext, as: ActorSystem): QuereaseAction[U] =
      (_: ExecutionContext, _: ActorSystem) => QuereaseAction.this.run.recover(pf)
  }
  object QuereaseAction {
    def apply(
      viewName: String,
      actionName: String,
      data: Map[String, Any],
      env: Map[String, Any],
    )(initResources: () => Resources,
      closeResources: (Resources, Option[Throwable]) => Unit,
      fileStreamer: FileStreamer,
    ): QuereaseAction[QuereaseResult] = {
        new QuereaseAction[QuereaseResult] {
          def run(implicit ec: ExecutionContext, as: ActorSystem) = {
            implicit val res = initResources()
            implicit val fs = fileStreamer
            try {
              doAction(viewName, actionName, data, env).map {
                case r: QuereaseCloseableResult => QuereaseResultWithCleanup(
                  r,
                  r match {
                    case srr: TresqlSingleRowResult => mbe => try srr.row.close finally closeResources(res, mbe)
                    case _ => closeResources(res, _)
                  }
                )
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
    }
    def value[A](a: => A): QuereaseAction[A] = (_: ExecutionContext, _: ActorSystem) => Future.successful(a)
  }

  trait QuereaseIteratorResult[+B] extends Iterator[B] with AutoCloseable {
    def view: ViewDef
  }

  case class ActionContext(
    viewName: String,
    actionName: String,
    env: Map[String, Any],
    view: Option[ViewDef],
    stepName: String = null,
    contextStack: List[ActionContext] = Nil,
  ) {
    val name = s"$viewName.$actionName" + Option(stepName).map(s => s".$s").getOrElse("")
  }
  private[wabase] def quereaseActionOpt(view: ViewDef, actionName: String) =
    view.actions.get(actionName)
      .orElse(actionName match {
        case Action.Insert | Action.Update | Action.Upsert =>
          view.actions.get(Action.Save)
        case _ => None
      })
  def doAction(
    view: String,
    actionName: String,
    data: Map[String, Any],
    env: Map[String, Any],
    contextStack: List[ActionContext] = Nil,
  )(implicit
    resources: Resources,
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
  ): Future[QuereaseResult] = {
    val vd = viewDef(view)

    val ctx = ActionContext(view, actionName, env, Some(vd), null, contextStack)
    logger.debug(s"Doing action '${ctx.name}'.\n Env: $env")
    val steps =
      quereaseActionOpt(vd, actionName)
        .map(_.steps)
        .getOrElse(List(Action.Return(None, Nil, Action.ViewCall(actionName, view))))
    doSteps(steps, ctx, Future.successful(data))
  }

  protected def doSteps(
    steps: List[Action.Step],
    context: ActionContext,
    curData: Future[Map[String, Any]],
  )(implicit
    resources: Resources,
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
  ): Future[QuereaseResult] = {
    import Action._

    def v(view: String) = viewDef(
      if (view == "this") context.view.map(_.name) getOrElse view
      else                view
    )

    def dataForNewStep(oldStep: Step, res: QuereaseResult) = res match {
      case TresqlResult(tr) => tr match {
        case dml: DMLResult =>
          dml.id.map(IdResult(_, null)) orElse dml.count getOrElse 0
        case SingleValueResult(v) => v
        case ar: ArrayResult[_] => ar.values.toList
        case r: Result[_] => (oldStep match {
          case Evaluation(_, _, ViewCall(_, viewName)) =>
            toCompatibleSeqOfMaps(r, v(viewName))
          case _  =>
            r.toListOfMaps
        }) match {
          case row :: Nil if row.size == 1 => row.head._2 // unwrap if result is one row, one column
          case rows => rows
        }
      }
      case srr: TresqlSingleRowResult =>
        oldStep match {
          case Evaluation(_, _, ViewCall(_, viewName)) =>
            srr.map(toCompatibleMap(_, v(viewName)))
          case _ =>
            srr.map(_.toMap)
        }
      case MapResult(mr) => mr
      case PojoResult(pr) => pr.toMap(this)
      case ListResult(lr) => lr
      case IteratorResult(ir) => ir.map(_.toMap(this)).toList
      case OptionResult(or) => or.map(_.toMap(this)).orNull
      case LongResult(nr) => nr
      case NumberResult(nr) => nr
      case StringResult(str) => str
      case id: IdResult => id
      case kr: KeyResult => kr.ir
      case sr: StatusResult => sr
      case fi: FileInfoResult => fi.fileInfo.toMap
      case fr: FileResult => fr.fileInfo.toMap
      case NoResult => null
      case x => sys.error(s"${x.getClass.getName} not expected here!")
    }
    def updateCurRes(cr: Map[String, Any], key: Option[String], res: Any) = res match {
      case ir: IdResult => key
        .map(k => cr + (k -> ir.id))
        .getOrElse(cr ++ ir.toMap)  // id result always updates current result
      case r => key.map(k => cr + (k -> r)).getOrElse(cr)
    }
    def doStep(step: Step, stepDataF: Future[Map[String, Any]]): Future[QuereaseResult] = {
      stepDataF flatMap { stepData =>
        logger.debug(s"Doing action '${context.name}' step '$step'.\nData: $stepData")
        step match {
          case Evaluation(_, vts, op) =>
            doActionOp(op, doVarsTransforms(vts, stepData, stepData).result, context.env, context)
          case SetEnv(_, vts, op) =>
            doActionOp(op, doVarsTransforms(vts, stepData, stepData).result, context.env, context)
          case Return(_, vts, op) =>
            doActionOp(op, doVarsTransforms(vts, stepData, stepData).result, context.env, context)
          case RemoveVar(name) => Future.successful(stepData - name.get) map MapResult
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
      case s :: Nil =>
        doStep(s, curData) flatMap {
          case ir: IdResult =>
            curData.map(keyResult(ir, context.viewName, _))
          case kr: KeyResult =>
            s match {
              // FIXME enable simple redirect from If
              case Evaluation(_, _, RedirectToKey(_)) => Future.successful(kr)
              case _ => curData.map(keyResult(kr.ir, context.viewName, _)) // FIXME apply kr.toMap
            }
          case TresqlResult(r: DMLResult) if context.stepName == null && context.contextStack.isEmpty =>
            r match {
              case _: InsertResult | _: UpdateResult =>
                val idName = viewNameToIdName.getOrElse(context.viewName, null)
                curData.map(keyResult(IdResult(r.id, idName), context.viewName, _))
              case _: DeleteResult =>
                Future.successful(QuereaseDeleteResult(r.count.getOrElse(0)))
            }
          case x => Future.successful(x)
        }
      case s :: tail =>
        doStep(s, curData) flatMap { stepRes =>
          s match {
            case e: Evaluation =>
              doSteps(tail, context, curData.map(updateCurRes(_, e.name, dataForNewStep(s, stepRes))))
            case se: SetEnv =>
              val newData =
                dataForNewStep(se, stepRes) match {
                  case m: Map[String, Any]@unchecked => Future.successful(m)
                  case x =>
                    //in the case of primitive value return step must have name
                    se.name.map(n => Future.successful(Map(n -> x))).getOrElse(curData)
                }
              doSteps(tail, context, newData)
            case rv: RemoveVar =>
              val newData = dataForNewStep(rv, stepRes) match {
                case m: Map[String, Any]@unchecked => Future.successful(m)
                case x => sys.error(s"Remove var step cannot produce anyting but Map, instead got $x")
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

  protected def doTresql(
    tresql: String,
    bindVars: Map[String, Any],
    context: ActionContext,
  )(implicit
    resources: Resources,
  ): TresqlResult = {
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
                  context.view.getOrElse(sys.error(s"view context missing for tresql: $tresql")) ->
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

  protected def doViewCall(
    method: String,
    view: String,
    data: Map[String, Any],
    env: Map[String, Any],
    context: ActionContext,
  )(implicit
    res: Resources,
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
  ): Future[QuereaseResult] = {
    import Action._
    import CoreTypes._
    val callData = data ++ env
    val v = viewDef(
      if (view == "this") context.view.map(_.name) getOrElse view
      else                view
    )
    val viewName = v.name
    lazy val idName = viewNameToIdName(viewName)
    def int(name: String) = tryOp(callData.get(name).map {
      case x: Int => x
      case x: Number => x.intValue
      case x: String => x.toInt
      case x => x.toString.toInt
    }, callData)
    def string(name: String) = callData.get(name) map String.valueOf
    if (context.view.exists(_.name == viewName)) {
      Future.successful {
        method match {
          case Get =>
            val keyValues   = getKeyValues(viewName, callData)
            val keyColNames = viewNameToKeyColNames(viewName)
            get(v, keyValues, keyColNames, null, callData).map(TresqlSingleRowResult) getOrElse OptionResult(None)
          case Action.List =>
            TresqlResult(rowsResult(v, callData, int(OffsetKey).getOrElse(0), int(LimitKey).getOrElse(0),
              string(OrderKey).orNull, null))
          case Save =>
            val saveMethod = context.actionName match {
              case Insert => SaveMethod.Insert
              case Update => SaveMethod.Update
              case Upsert => SaveMethod.Upsert
              case _      => SaveMethod.Save
            }
            IdResult(save(v, callData, null, saveMethod,        null, env), idName)
          case Insert =>
            IdResult(save(v, callData, null, SaveMethod.Insert, null, env), idName)
          case Update =>
            IdResult(save(v, callData, null, SaveMethod.Update, null, env), idName)
          case Upsert =>
            IdResult(save(v, callData, null, SaveMethod.Upsert, null, env), idName)
          case Delete =>
            getKeyValues(viewName, data) // check mappings for key exist
            LongResult(delete(v, data, null, env))
          case Create =>
            TresqlSingleRowResult(create(v, callData)(res))
          case Count =>
            LongResult(countAll_(v, callData))
          case x =>
            sys.error(s"Unknown view action $x")
        }
      }
    } else {
      doAction(viewName, method, data, env, context :: context.contextStack)
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
      case l: Long => LongResult(l)
      case s: String => StringResult(s)
      case n: java.lang.Number => NumberResult(n)
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

  /* [jobs]
  protected def doJobCall(jobName: String,
                          data: Map[String, Any],
                          env: Map[String, Any])(implicit
                                                 resources: Resources,
                                                 ec: ExecutionContext): Future[QuereaseResult] = {
    val job = null: Job
    // TODO get job from metadata
    //getJob(if (jobName.startsWith(":")) stringValue(jobName, data, env) else jobName)
    val ctx = ActionContext(null, job.name, env, None)
    job.condition
      .collect { case cond if Query(cond, data ++ env).unique[Boolean] =>
        doSteps(job.action.steps, ctx, Future.successful(data))
      }
      .getOrElse(Future.successful(NoResult))
  }
  [jobs] */

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

  protected def doIf(
    op: Action.If,
    data: Map[String, Any],
    env: Map[String, Any],
    context: ActionContext,
  )(implicit
    res: Resources,
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
  ): Future[QuereaseResult] = {
    doActionOp(op.cond, data, env, context).map {
      case TresqlResult(tr) => tr.unique[Boolean]
      case r: TresqlSingleRowResult => r.map(_.boolean(0))
      case x => sys.error(s"Conditional operator must be whether TresqlResult or TresqlSingleRowResult. " +
        s"Instead found: $x")
    }.flatMap { cond =>
      if (cond) doSteps(op.action.steps, context.copy(stepName = "if"), Future.successful(data))
      else Future.successful(NoResult)
    }
  }

  protected def doForeach(
    op: Action.Foreach,
    data: Map[String, Any],
    env: Map[String, Any],
    context: ActionContext,
  )(implicit
    res: Resources,
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
  ): Future[QuereaseResult] = {
    def addParentData(map: Map[String, Any]) = {
      var key = ".."
//      while (map.contains(key)) key += "_" + key // hopefully no .. key is in data map
      map + (key -> data)
    }
    doActionOp(op.initOp, data, env, context).map {
      case TresqlResult(tr) => tr match {
        case SingleValueResult(sr) => sr match {
          case s: Seq[Map[String, _]]@unchecked => s map addParentData
          case m: Map[String@unchecked, _] => List(m) map addParentData
          case x => sys.error(s"Not iterable result for foreach operation: $x")
        }
        case r: Result[_] => r.map(_.toMap) map addParentData
      }
      case r: TresqlSingleRowResult => List(r.map(_.toMap)) map addParentData
      case x => sys.error(s"Not iterable result for foreach operation: $x")
    }
    .flatMap { mapIterator =>
      Future.traverse(mapIterator.toSeq) { itData =>
        doSteps(op.action.steps, context.copy(stepName = "foreach"), Future.successful(itData))
      }
      .map(_ => NoResult)
    }
  }

  protected def doFileAction(
    op: Action.File,
    data: Map[String, Any],
    env: Map[String, Any],
    context: ActionContext,
  )(implicit
    res: Resources,
    ec: ExecutionContext,
    fs: FileStreamer): Future[FileResult] = {
    val (id, sha) = Query(op.idShaTresql)(res.withParams(data ++ env)).unique[Long, String]
    Future.successful(FileResult(fs.getFileInfo(id, sha).map(_.file_info).orNull, fs))
  }

  protected def doToFileAction(
    op: Action.ToFile,
    data: Map[String, Any],
    env: Map[String, Any],
    context: ActionContext,
  )(implicit
    res: Resources,
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer): Future[FileInfoResult] = {
    import akka.http.scaladsl.model.{MediaTypes, ContentType}
    val bindVars = data ++ env
    def getVal(tr: String) = Query(tr)(res.withParams(bindVars)).unique[String]
    val fn = if (op.nameTresql != null) getVal(op.nameTresql) else "file"
    val ct =
      if (op.contentTypeTresql != null) {
        val ctStr = getVal(op.contentTypeTresql)
        ContentType.parse(ctStr)
          .toOption
          .getOrElse(sys.error(s"Invalid content type: '$ctStr'"))
      } else ContentType(MediaTypes.`application/json`)
    def renderer(viewName: String, isCollection: Boolean): EncoderFactory =
      resultRenderers.renderers.get(ct)
        .map(_(nameToViewDef)(viewName, isCollection))
        .getOrElse(sys.error(s"Renderer not found for content type: $ct"))
    def fi(src: Source[ByteString, _], encFactory: EncoderFactory) =
      src.via(BorerNestedArraysTransformer.flow(encFactory)).runWith(fs.fileSink(fn, ct.value))
    doActionOp(op.contentOp, data, env, context).flatMap {
      case TresqlResult(tr) =>
        val src = TresqlResultSerializer.source(() => tr)
        op.contentOp match {
          case Action.ViewCall(_, view) =>
            val vn = if (view == "this") context.viewName else view
            fi(src, renderer(vn, true))
          case _ =>
            fi(src, renderer(null, true))
        }
      case TresqlSingleRowResult(row) =>
        val src = TresqlResultSerializer.rowSource(() => row)
        op.contentOp match {
          case Action.ViewCall(_, view) =>
            val vn = if (view == "this") context.viewName else view
            fi(src, renderer(vn, false))
          case _ =>
            fi(src, renderer(null, false))
        }
      case x => sys.error(s"Currently unable to save result to file: $x")
    }.map(FileInfoResult)
  }

  protected def doActionOp(
    op: Action.Op,
    data: Map[String, Any],
    env: Map[String, Any],
    context: ActionContext,
  )(implicit
    res: Resources,
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
  ): Future[QuereaseResult] = {
    op match {
      case Action.Tresql(tresql) =>
        Future.successful(doTresql(tresql, data ++ env, context))
      case Action.ViewCall(method, view) =>
        doViewCall(method, view, data, env, context)
      case Action.UniqueOpt(innerOp) =>
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
      case Action.Invocation(className, function) =>
        doInvocation(className, function, data ++ env)
      case Action.RedirectToKey(name) =>
        val viewName = if (name == "this") context.viewName else name
        val idName = viewNameToIdName.getOrElse(viewName, null)
        val dataWithEnv = data ++ env
        val id = dataWithEnv.getOrElse(idName, null)
        Future.successful(keyResult(IdResult(id, idName), viewName, dataWithEnv))
      case Action.Status(maybeCode, bodyTresql, parameterIndex) =>
          Option(bodyTresql).map { bt =>
            doActionOp(Action.UniqueOpt(Action.Tresql(bt)), data, env, context).map {
              case srr: TresqlSingleRowResult => srr.map { row =>
                val colCount = row.columnCount
                val (code, idx) = maybeCode.map(_ -> 0).getOrElse(row.int(0) -> Math.min(1, colCount - 1))
                val pi = if (parameterIndex == -1) colCount else parameterIndex
                val (value, (key, params)) = (row.string(idx),
                  ((idx + 1) until colCount).foldLeft(List[String]() -> ListMap[String, String]()) {
                    case ((k, p), i) if i < pi  => (row.string(i) :: k, p)
                    case ((k, p), i) if i > pi  => (k, p + (row.column(i).name -> row.string(i)))
                    case (r, _)                 => r // i == pi - parameter separator - '?'
                  }
                )
                StatusResult(code, value, key.reverse, params)
              }
              case _ =>
                require(maybeCode.nonEmpty, s"Tresql: '$bt' returned no rows. In this case status code cannot be empty.")
                StatusResult(maybeCode.get, null)
            }
          }
          .getOrElse(Future.successful(StatusResult(maybeCode.get, null)))
      case Action.Commit =>
        def commit(c: Connection) = Option(c).foreach(_.commit())
        commit(res.conn)
        res.extraResources.foreach { case (_, r) => commit(r.conn) }
        Future.successful(NoResult)
      case cond: Action.If => doIf(cond, data, env, context)
      case foreach: Action.Foreach => doForeach(foreach, data, env, context)
      case file: Action.File => doFileAction(file, data, env, context)
      case toFile: Action.ToFile => doToFileAction(toFile, data, env, context)
      /* [jobs]
      case Action.JobCall(job) =>
        doJobCall(job, data, env)
      [jobs] */
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
      rep1sep(varsTransform, "+") ^^ (VariableTransforms) named "var-transforms"
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
