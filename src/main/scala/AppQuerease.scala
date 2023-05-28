package org.wabase

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpHeader.ParsingResult.{Error, Ok}
import akka.http.scaladsl.model.{ContentType, ContentTypes, HttpEntity, HttpHeader, HttpMethods, HttpRequest, HttpResponse, MediaTypes, StatusCodes, UniversalEntity}
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.mojoz.querease.QuereaseExpressions.DefaultParser
import org.tresql._
import org.mojoz.querease._
import org.mojoz.querease.SaveMethod
import org.mojoz.metadata.Type
import org.mojoz.metadata.{FieldDef, ViewDef}
import org.tresql.ast.{Exp, Fun, Join, Obj, Variable, With, Query => PQuery}
import org.wabase.AppFileStreamer.FileInfo
import org.wabase.AppMetadata.Action
import org.wabase.AppMetadata.Action.{VariableTransform, VariableTransforms}

import scala.reflect.ManifestFactory
import spray.json._

import java.sql.Connection
import scala.collection.immutable.{ListMap, Seq}
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Try}
import scala.util.control.NonFatal

trait QuereaseProvider {
  final implicit lazy val qe: AppQuerease = initQuerease
  final implicit lazy val qio: AppQuereaseIo[Dto] = initQuereaseIo
  /** Override this method in subclass to initialize {{{qe}}} */
  protected def initQuerease: AppQuerease
  protected def initQuereaseIo: AppQuereaseIo[Dto] = new AppQuereaseIo[Dto](qe)
}

case class ResourcesFactory(
  initResources: () => Resources,
  closeResources: (Resources, Boolean, Option[Throwable]) => Unit,
)(implicit val resources: Resources)

sealed trait StatusValue
case class RedirectStatus(value: TresqlUri.Uri) extends StatusValue
case class StringStatus(value: String) extends StatusValue
sealed trait QuereaseResult
sealed trait QuereaseCloseableResult extends QuereaseResult
/** Data result can conform to view structure */
sealed trait DataResult extends QuereaseResult
case class TresqlResult(result: Result[RowLike]) extends QuereaseCloseableResult with DataResult
case class TresqlSingleRowResult(row: RowLike) extends QuereaseCloseableResult with DataResult {
  /** map, close row (i.e. result), return mapped */
  def map[T](f: RowLike => T): T = try f(row) finally row.close()
}
case class MapResult(result: Map[String, Any]) extends DataResult
case class IteratorResult(result: Iterator[Map[String, Any]]) extends QuereaseCloseableResult with DataResult
case class LongResult(value: Long) extends QuereaseResult
case class StringResult(value: String) extends QuereaseResult
case class NumberResult(value: java.lang.Number) extends QuereaseResult
case class IdResult(id: Any, name: String) extends QuereaseResult {
  def toMap: Map[String, Any] =
    if (id == null || id == 0L) Map.empty else Map((if (name == null) "id" else name) -> id)
}
case class KeyResult(ir: IdResult, viewName: String, key: Seq[Any]) extends QuereaseResult
case class QuereaseDeleteResult(count: Int) extends QuereaseResult
case class StatusResult(code: Int, value: StatusValue) extends DataResult
case class FileInfoResult(fileInfo: FileInfo) extends QuereaseResult
case class FileResult(fileInfo: FileInfo, fileStreamer: FileStreamer) extends DataResult
case class HttpResult(response: HttpResponse) extends DataResult
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
case class QuereaseSerializedResult(result: SerializedResult,
                                    resultFilter: ResultRenderer.ResultFilter,
                                    isCollection: Boolean) extends QuereaseResult
case class CompatibleResult(result: DataResult,
                            resultFilter: ResultRenderer.ResultFilter = null,
                            isCollection: Boolean = false)
  extends DataResult with QuereaseCloseableResult
case class DbResult(result: QuereaseResult, cleanup: Option[Throwable] => Unit)
  extends QuereaseResult
case class ConfResult(param: String, result: Any) extends QuereaseResult

class AppQuereaseIo[DTO <: Dto](val qe: QuereaseMetadata with QuereaseResolvers)
  extends ScalaDtoQuereaseIo[DTO](qe) with JsonConverter[DTO] {

  def fill[B <: DTO: Manifest](jsObject: JsObject): B = {
    implicitly[Manifest[B]].runtimeClass.getConstructor().newInstance().asInstanceOf[B].fill(jsObject)(qe)
  }
  def fill[B <: DTO: Manifest](values: Map[String, Any]): B = {
    implicitly[Manifest[B]].runtimeClass.getConstructor().newInstance().asInstanceOf[B].fill(values)(qe)
  }
}

class QuereaseEnvException(val env: Map[String, Any], cause: Exception) extends Exception(cause) {
  override def getMessage: String = s"Error occured while processing env: ${cause.getMessage}. Env: ${
    String.valueOf(env)}"
}

class AppQuerease extends Querease with AppMetadata with Loggable {

 private [wabase] val FieldRefRegexp_ = FieldRefRegexp

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
  val tresqlUri: TresqlUri = new TresqlUri()
  lazy val cborOrJsonDecoder = new CborOrJsonDecoder(typeDefs, nameToViewDef)
  /** Override this to override default scala value (like String, Number, Boolean, null, Iterable, Map) json encoding.
    * Default implementation is {{{Writer => PartialFunction.empty}}}
    * */
  val jsonValueEncoder: ResultEncoder.JsValueEncoderPF = _ => PartialFunction.empty

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
    fieldFilter: FieldFilter,
  )(implicit resources: Resources): Option[RowLike] = {
    val extraFilterAndAuth =
      extraFilterAndAuthString(extraFilter, viewDef.auth.forGet)
    super.get(viewDef, keyValues, keyColNames, extraFilterAndAuth, extraParams, fieldFilter)
  }
  override def result[B <: AnyRef: Manifest](params: Map[String, Any],
      offset: Int = 0, limit: Int = 0, orderBy: String = null,
      extraFilter: String = null, extraParams: Map[String, Any] = Map(),
      fieldFilter: FieldFilter = null)(
      implicit resources: Resources, qio: QuereaseIo[B]): QuereaseIteratorResult[B] = {
    val v = viewDef[B]
    val extraFilterAndAuth =
      extraFilterAndAuthString(extraFilter, v.auth.forList)
    super.result(params, offset, limit, orderBy, extraFilterAndAuth, extraParams, fieldFilter)
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
      fieldFilter: FieldFilter = null,
    )(resourcesFactory: ResourcesFactory,
      fileStreamer: FileStreamer,
      req: HttpRequest,
      qio: AppQuereaseIo[Dto],
    ): QuereaseAction[QuereaseResult] = {
        new QuereaseAction[QuereaseResult] {
          def run(implicit ec: ExecutionContext, as: ActorSystem) = {
            val vd = viewDef(viewName)
            implicit val resFac =
              if (AugmentedAppViewDef(vd).explicitDb) resourcesFactory
              else resourcesFactory.copy()(resources = resourcesFactory.initResources())
            implicit val fs = fileStreamer
            implicit val httpReq = req
            implicit val io = qio
            import resFac._
            def processResult(res: QuereaseResult, cleanup: Option[Throwable] => Unit): QuereaseResult = res match {
              case DbResult(result, cl) =>
                // close outer resources
                cleanup(None)
                processResult(result, cl)
              case r: QuereaseCloseableResult => QuereaseResultWithCleanup(r, cleanup)
              case r: QuereaseResult =>
                cleanup(None)
                r
            }

            try {
              doAction(viewName, actionName, data, env, fieldFilter).map {
                processResult(_, closeResources(resources, false, _))
              }.andThen {
                case Failure(NonFatal(exception)) => closeResources(resources, true, Option(exception))
              }
            } catch { // catch exception also here in the case doAction is not executed into separate thread
                case NonFatal(e) =>
                  closeResources(resources, true, Option(e))
                  throw e
            }
          }
        }
    }
    def value[A](a: => A): QuereaseAction[A] = (_: ExecutionContext, _: ActorSystem) => Future.successful(a)
  }

  case class ActionContext(
    viewName: String,
    actionName: String,
    env: Map[String, Any],
    view: Option[ViewDef],
    fieldFilter: FieldFilter = null,
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
    fieldFilter: FieldFilter = null,
  )(implicit
    resourcesFactory: ResourcesFactory,
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
    req: HttpRequest,
    qio: AppQuereaseIo[Dto],
  ): Future[QuereaseResult] = {
    do_action(view, actionName, data, env, fieldFilter)
  }

  private def do_action(
    view: String,
    actionName: String,
    data: Map[String, Any],
    env: Map[String, Any],
    fieldFilter: FieldFilter = null,
    contextStack: List[ActionContext] = Nil,
  )(implicit
    resourcesFactory: ResourcesFactory,
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
    req: HttpRequest,
    qio: AppQuereaseIo[Dto],
  ): Future[QuereaseResult] = {
    val vd = viewDef(view)

    val ctx = ActionContext(view, actionName, env, Some(vd), fieldFilter, null, contextStack)
    logger.debug(s"Doing action '${ctx.name}'.\n Env: $env\nCtx stack: [${contextStack.map(_.name).mkString(", ")}]")
    val steps =
      quereaseActionOpt(vd, actionName)
        .map(_.steps)
        .getOrElse(List(Action.Return(None, Nil, Action.ViewCall(actionName, view, null))))
    doSteps(steps, ctx, Future.successful(data))
  }

  protected def doSteps(
    steps: List[Action.Step],
    context: ActionContext,
    curData: Future[Map[String, Any]],
  )(implicit
    resourcesFactory: ResourcesFactory,
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
    req: HttpRequest,
    qio: AppQuereaseIo[Dto],
  ): Future[QuereaseResult] = {
    import Action._

    def updateCurRes(cr: Map[String, Any], key: Option[String], resF: Future[_]) = {
      def upd(d: Map[String, _], k: String, v: Any) = {
        def rec(m: Map[String, _], kp: List[String]): Map[String, _] = kp match {
          case k :: Nil => m + (k -> v)
          case k :: tail => m.get(k).map {
            case cm: Map[String@unchecked, _] => m + (k -> rec(cm, tail))
            case _ => m
          }.getOrElse(m + (k -> rec(Map[String, Any](), tail)))
          case Nil => m
        }
        rec(d, k.split("\\.").toList)
      }
      resF map {
        case ir: IdResult =>
          key
            .map(k => upd(cr, k, ir.id))
            .getOrElse(cr ++ ir.toMap)
        // id result always updates current result
        case NoResult => key.map(k => if (cr.contains(k)) cr else upd(cr, k, null)).getOrElse(cr)
        case r => key.map(k => upd(cr, k, r)).getOrElse(cr)
      }
    }
    def doStep(step: Step, stepDataF: Future[Map[String, Any]]): Future[QuereaseResult] = {
      import resourcesFactory._
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
        } flatMap { res => s match {
          case Evaluation(n@Some(_), _, _) => curData
            .flatMap(updateCurRes(_, n, dataForNextStep(res, context, false)))
            .map(MapResult)
          case _ => Future.successful(res)
        }}
      case s :: tail =>
        doStep(s, curData) flatMap { stepRes =>
          s match {
            case e: Evaluation =>
              doSteps(tail, context, curData
                .flatMap(updateCurRes(_, e.name, dataForNextStep(stepRes, context, true))))
            case se: SetEnv =>
              val newData =
                dataForNextStep(stepRes, context, true) flatMap {
                  case m: Map[String, Any]@unchecked => Future.successful(m)
                  case x =>
                    //in the case of primitive value return step must have name
                    se.name.map(n => Future.successful(Map(n -> x))).getOrElse(curData)
                }
              doSteps(tail, context, newData)
            case rv: RemoveVar =>
              val newData = dataForNextStep(stepRes, context, true) flatMap {
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
    validationsQueryString(view, params, validations) foreach { vs =>
      Query(dbkey.flatMap(k => Option(k.db)).map("|" + _ + ":").mkString("", "", vs), params)
        .map(_.s("msg"))
        .filter(_ != null).filter(_ != "")
        .toList match {
        case messages if messages.nonEmpty =>
          throw new ValidationException(messages.mkString("\n"), List(ValidationResult(Nil, messages)))
        case _ =>
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
        val qp = new QueryParser(macros = resources, cache = resources.cache)
        def bindVarsCursorsCreator(bindVars: Map[String, Any]): qp.Transformer = {
          def singleValue[T: Converter : Manifest](tresql: String, data: Map[String, Any]): T = {
            Query(tresql, data) match {
              case SingleValueResult(value) => value.asInstanceOf[T]
              case r: Result[_] => r.unique[T]
            }
          }

          qp.transformer {
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
              qp.parseExp(cursorsFromViewBindVars(bv, vd)) match {
                case With(tables, _) => With(tables, q)
                case _ => q
              }
            case With(tables, query) => bindVarsCursorsCreator(bindVars)(query) match {
              case w: With => With(w.tables ++ tables, w.query) //put bind var cursor tables first
              case q: Exp => With(tables, q)
            }
          }
        }
        bindVarsCursorsCreator(env)(qp.parseExp(tresql)).tresql
      }
    }

    TresqlResult(Query(maybeExpandWithBindVarsCursors(tresql, bindVars))(resources.withParams(bindVars)))
  }

  protected def doViewCall(
    method: String,
    view: String,
    viewOp: Action.Op,
    data: Map[String, Any],
    env: Map[String, Any],
    context: ActionContext,
  )(implicit
    resFac: ResourcesFactory,
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
    req: HttpRequest,
    qio: AppQuereaseIo[Dto],
  ): Future[QuereaseResult] = {
    import Action._
    import CoreTypes._
    import resFac._
    val v = viewDef(
      if (view == "this") context.view.map(_.name) getOrElse view
      else                view
    )
    val viewName = v.name
    val callDataF =
      if (viewOp == null) Future.successful(data ++ env)
      else {
        def unwrapSingleRow(d: Any): Map[String, _] = d match {
          case r: Map[String@unchecked, _] => r ++ env
          case r: Seq[_] if r.size == 1 => unwrapSingleRow(r.head)
          case x => sys.error(s"Invalid view op result. Currently unable to create Map[String, _] from $x")
        }
        doActionOp(viewOp, data, env, context).flatMap(dataForNextStep(_, context, false))
          .map(unwrapSingleRow)
      }
    callDataF.flatMap { callData =>
      // execute querease call if context view name and method corresponds to this view name and method
      def isThisMethod(ctxMethod: String, thisMethod: String) = {
        ctxMethod ==
          thisMethod ||
          (Set(Action.Insert, Action.Update, Action.Upsert).contains(ctxMethod) && Action.Save == thisMethod) ||
          !v.actions.contains(thisMethod)
      }

      if (context.view.exists(_.name == viewName) && isThisMethod(context.actionName, method)) {
        lazy val idName = viewNameToIdName(viewName)

        def int(name: String) = tryOp(callData.get(name).map {
          case x: Int => x
          case x: Number => x.intValue
          case x: String => x.toInt
          case x => x.toString.toInt
        }, callData)

        def string(name: String) = callData.get(name) map String.valueOf
        val res =
          (method match {
            case Get =>
              val keyValues = getKeyValues(viewName, callData)
              val keyColNames = viewNameToKeyColNames(viewName)
              val fieldFilter: FieldFilter = context.fieldFilter
              get(v, keyValues, keyColNames, null, callData, fieldFilter)
                .map(TresqlSingleRowResult) getOrElse notFound
            case Action.List =>
              TresqlResult(rowsResult(v, callData, int(OffsetKey).getOrElse(0), int(LimitKey).getOrElse(0),
                string(OrderKey).orNull, null, context.fieldFilter))
            case Save =>
              val saveMethod = context.actionName match {
                case Insert => SaveMethod.Insert
                case Update => SaveMethod.Update
                case Upsert => SaveMethod.Upsert
                case _ => SaveMethod.Save
              }
              IdResult(save(v, callData, null, saveMethod, null, env), idName)
            case Insert =>
              IdResult(save(v, callData, null, SaveMethod.Insert, null, env), idName)
            case Update =>
              IdResult(save(v, callData, null, SaveMethod.Update, null, env), idName)
            case Upsert =>
              IdResult(save(v, callData, null, SaveMethod.Upsert, null, env), idName)
            case Delete =>
              getKeyValues(viewName, callData) // check mappings for key exist
              LongResult(delete(v, callData, null, env))
            case Create =>
              TresqlSingleRowResult(create(v, callData))
            case Count =>
              LongResult(countAll_(v, callData))
            case x =>
              sys.error(s"Unknown view action $x")
          }) match {
            case r: TresqlSingleRowResult => comp_res(r, Action.OpResultType(viewName, false))
            case r: DataResult => comp_res(r, Action.OpResultType(viewName, true))
            case r => r
          }
        Future.successful(res)
      } else {
        do_action(viewName, method, callData, env, context.fieldFilter, context :: context.contextStack)
      }
    }
  }

  protected def doInvocation(op: Action.Invocation,
                             data: Map[String, Any])(
    implicit res: Resources,
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
    req: HttpRequest,
    qio: AppQuereaseIo[Dto],
  ): Future[QuereaseResult] = {
    import op._
    def comp_q_result(r: Any) = {
      def createCompatibleResult(result: QuereaseResult, conformTo: Action.OpResultType) = result match {
        case c: CompatibleResult =>
          require(c.isCollection == conformTo.isCollection, s"Incompatible results $c != $conformTo")
          val c1 = comp_res(c.result, conformTo)
          c.copy(resultFilter = new ResultRenderer.IntersectionFilter(c1.resultFilter, c.resultFilter))
        case r: DataResult => comp_res(r, conformTo)
        case x => throw new IllegalArgumentException(s"Result argument must be of type DataResult, instead encountered: $x")
      }
      val qr = qresult(r)
      conformTo.map(createCompatibleResult(qr, _)).getOrElse(qr)
    }
    def qresult(r: Any): QuereaseResult = r match {
      case null | () => NoResult // reflection call on function with Unit (void) return type returns null
      case r: Result[_] => TresqlResult(r)
      case r: RowLike => TresqlSingleRowResult(r)
      case i: Iterator[_] => IteratorResult(i.map {
        case m: Map[String, Any]@unchecked => m
        case m: java.util.Map[String, Any]@unchecked => m.asScala.toMap
        case d: Dto => d.toMap(this)
      })
      case m: Map[String, Any]@unchecked => MapResult(m)
      case l: Seq[_] => qresult(l.iterator)
      case m: java.util.Map[String, Any]@unchecked => MapResult(m.asScala.toMap)
      case l: Long => LongResult(l)
      case s: String => StringResult(s)
      case n: java.lang.Number => NumberResult(n)
      case d: Dto => MapResult(d.toMap(this))
      case o: Option[Dto]@unchecked => o.map(d => MapResult(d.toMap(this))).getOrElse(notFound)
      case h: HttpResponse => HttpResult(h)
      case q: QuereaseResult => q
      case x => sys.error(s"Unrecognized result type: ${x.getClass}, value: $x from function $className.$function")
    }

    import scala.language.existentials
    val (clazz, obj) = Try {
      val c = Class.forName(className + "$")
      (c, c.getField("MODULE$").get(null))
    }.recover {
      case _: ClassNotFoundException =>
        val c = Class.forName(className)
        (c, c.getDeclaredConstructor().newInstance())
    }.get
    val result =
      clazz.getMethods.filter(_.getName == function) match {
        case Array(method) =>
          def param(parType: Class[_]) = {
            import qio.MapJsonFormat
            if (classOf[Dto].isAssignableFrom(parType))
              qio.fill(data.toJson.asJsObject)(Manifest.classType(parType)) // specify manifest explicitly so it is not Nothing
            else if (parType.isAssignableFrom(classOf[java.util.Map[_, _]])) data.asJava
            else if (parType.isAssignableFrom(classOf[Resources])) res
            else if (parType.isAssignableFrom(classOf[ExecutionContext])) ec
            else if (parType.isAssignableFrom(classOf[ActorSystem])) as
            else if (parType.isAssignableFrom(classOf[FileStreamer])) fs
            else if (parType.isAssignableFrom(classOf[HttpRequest])) req
            else data
          }
          val parTypes = method.getParameterTypes
          method.invoke(obj, parTypes map param: _*)
        case Array() => sys.error(s"Method $function not found in class $className")
        case m => sys.error(s"Multiple methods '$function' found: (${m.toList}) in class $className")
      }
    result match {
      case f: Future[_] => f map comp_q_result
      case x => Future.successful(comp_q_result(x))
    }
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
      if (vt.form == "_") sd ++ data //indicates all call data map
      else data(vt.form) match {
        case m: Map[String, _]@unchecked => sd ++ m
        case x => sd + (vt.to.getOrElse(vt.form) -> x)
      }
    }
    MapResult(transRes)
  }

  protected def doUniqueOpt(
    op: Action.UniqueOpt,
    data: Map[String, Any],
    env: Map[String, Any],
    context: ActionContext,
  )(implicit
    resFac: ResourcesFactory,
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
    req: HttpRequest,
    qio: AppQuereaseIo[Dto],
  ): Future[QuereaseResult] = {
    def createGetResult(res: QuereaseResult): DataResult = res match {
      case TresqlResult(r) if !r.isInstanceOf[DMLResult] =>
        r.uniqueOption map TresqlSingleRowResult getOrElse notFound
      case IteratorResult(r) =>
        try r.hasNext match {
          case true =>
            val v = r.next()
            if (r.hasNext) sys.error("More than one row for unique result") else MapResult(v)
          case false => notFound
        } finally r match {
          case c: AutoCloseable => c.close()
          case _ =>
        }
      case c: CompatibleResult => c.copy(result = createGetResult(c.result))
      case r => sys.error(s"unique opt can only process DataResult type, instead encountered: $r")
    }
    doActionOp(op.innerOp, data, env, context) map createGetResult
  }

  protected def doStatus(
   op: Action.Status,
   data: Map[String, Any],
   env: Map[String, Any],
   context: ActionContext,
  )(implicit
   resFac: ResourcesFactory,
   ec: ExecutionContext,
   as: ActorSystem,
   fs: FileStreamer,
   req: HttpRequest,
   qio: AppQuereaseIo[Dto],
  ): Future[QuereaseResult] = {
    val Action.Status(maybeCode, bodyTresql, parameterIndex) = op
    Option(bodyTresql).map { bt =>
      doActionOp(Action.UniqueOpt(Action.Tresql(bt)), data, env, context).map {
        case srr: TresqlSingleRowResult => srr.map { row =>
          val colCount = row.columnCount
          val (code, idx) = maybeCode.map(_ -> 0).getOrElse(row.int(0) -> Math.min(1, colCount - 1))
          import akka.http.scaladsl.model.StatusCode._
          val statusValue =
            if (code.isRedirection()) RedirectStatus(tresqlUri.uriValue(row, idx, parameterIndex))
            else if (colCount > idx) StringStatus(row.string(idx))
            else null
          StatusResult(code, statusValue)
        }
        case _ =>
          require(maybeCode.nonEmpty, s"Tresql: '$bt' returned no rows. In this case status code cannot be empty.")
          StatusResult(maybeCode.get, null)
      }
    }
    .getOrElse(Future.successful(StatusResult(maybeCode.get, null)))
  }

  protected def doIf(
    op: Action.If,
    data: Map[String, Any],
    env: Map[String, Any],
    context: ActionContext,
  )(implicit
    resFac: ResourcesFactory,
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
    req: HttpRequest,
    qio: AppQuereaseIo[Dto],
  ): Future[QuereaseResult] = {
    doActionOp(op.cond, data, env, context).map {
      case TresqlResult(tr) => tr.unique[Boolean]
      case r: TresqlSingleRowResult => r.map(_.boolean(0))
      case x => sys.error(s"Conditional operator must be whether TresqlResult or TresqlSingleRowResult or" +
        s"StringResult(true|false). Instead found: $x")
    }.flatMap { cond =>
      if (cond)
        doSteps(op.action.steps, context.copy(stepName = "if"), Future.successful(data))
      else if (op.elseAct != null)
        doSteps(op.elseAct.steps, context.copy(stepName = "else"), Future.successful(data))
      else Future.successful(NoResult)
    }
  }

  protected def doForeach(
    op: Action.Foreach,
    data: Map[String, Any],
    env: Map[String, Any],
    context: ActionContext,
  )(implicit
    resFac: ResourcesFactory,
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
    req: HttpRequest,
    qio: AppQuereaseIo[Dto],
  ): Future[IteratorResult] = {
    def iterator(res: QuereaseResult): Iterator[Map[String, Any]] = {
      def addParentData(map: Map[String, Any]) = {
        var key = ".."
        //      while (map.contains(key)) key += "_" + key // hopefully no .. key is in data map
        map + (key -> data)
      }
      res match {
        case TresqlResult(tr) => tr match {
          case SingleValueResult(sr) => sr match {
            case s: Seq[Map[String, _]@unchecked] => (s map addParentData).iterator
            case m: Map[String@unchecked, _] => (List(m) map addParentData).iterator
            case x => sys.error(s"Not iterable result for foreach operation: $x")
          }
          case r: Result[_] => r.map(_.toMap) map addParentData
        }
        case r: TresqlSingleRowResult => (List(r.map(_.toMap)) map addParentData).iterator
        case CompatibleResult(r, _, _) => iterator(r) // TODO Execute to compatible map
        case x => sys.error(s"Not iterable result for foreach operation: $x")
      }
    }
    doActionOp(op.initOp, data, env, context).map(iterator)
    .flatMap { mapIterator =>
      Future.traverse(mapIterator.toSeq) { itData =>
        doSteps(op.action.steps, context.copy(stepName = "foreach"), Future.successful(itData))
          .flatMap {
            case MapResult(r) => Future.successful(r)
            case _ => Future.successful(itData)
          }
      }
    }.map(it => IteratorResult(it.iterator))
  }

  protected def doFile(
    op: Action.File,
    data: Map[String, Any],
    env: Map[String, Any],
    context: ActionContext,
  )(implicit
    res: Resources,
    ec: ExecutionContext,
    fs: FileStreamer): Future[DataResult] = {
    val (id, sha) = Query(op.idShaTresql)(res.withParams(data ++ env)).unique[Long, String]
    val r = FileResult(fs.getFileInfo(id, sha).map(_.file_info).orNull, fs)
    Future.successful { op.conformTo.map(comp_res(r, _)).getOrElse(r) }
  }

  protected def doToFile(
    op: Action.ToFile,
    data: Map[String, Any],
    env: Map[String, Any],
    context: ActionContext,
  )(implicit
    resFac: ResourcesFactory,
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
    req: HttpRequest,
    qio: AppQuereaseIo[Dto],
  ): Future[FileInfoResult] = {
    import akka.http.scaladsl.model.{MediaTypes, ContentType}
    import resFac._
    val bindVars = data ++ env
    def getVal(tr: String) = Query(tr)(resources.withParams(bindVars)).unique[String]
    val fn = if (op.nameTresql != null) getVal(op.nameTresql) else "file"
    val contentType =
      if (op.contentTypeTresql != null) {
        val ctStr = getVal(op.contentTypeTresql)
        ContentType.parse(ctStr)
          .toOption
          .getOrElse(sys.error(s"Invalid content type: '$ctStr'"))
      } else ContentType(MediaTypes.`application/json`)

    doActionOpAndRender(contentType, op.contentOp, data, env, context).flatMap { case (src, ct, _) =>
      src.runWith(fs.fileSink(fn, ct.value))
    }.map(FileInfoResult)
  }

  protected def doHttp(
    op: Action.Http,
    data: Map[String, Any],
    env: Map[String, Any],
    context: ActionContext,
  )(implicit
    resFac: ResourcesFactory,
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
    req: HttpRequest,
    qio: AppQuereaseIo[Dto],
  ): Future[DataResult] = {
    import resFac._
    val opData = data ++ env
    val httpMeth = HttpMethods.getForKeyCaseInsensitive(op.method).get
    val uri = {
      val trUri = tresqlUri.tresqlUriValue(op.uriTresql)(Query, opData, implicitly[Resources])
      tresqlUri.uri(trUri)
    }
    val (optContentType, headers) = if (op.headerTresql == null) (Some(null) -> Nil) else {
      // content type is used for request body if present
      val parsedValues = (Query(op.headerTresql, opData) match {
        case SingleValueResult(r) => r match { // unwrap header values from list of maps
          case i: Iterable[_] => i.map {
            case m: Map[_, _] if m.size > 1 =>
              val h = m.toList
              h.head._2.toString -> h.tail.head._2.toString // extract values - 1st value header name, 2nd - header value
            case x => sys.error(s"Cannot retrieve http header from structure: [$x], two element Map[_, _] is required")
          }.toList
          case x => sys.error(s"Cannot retrieve http headers from structure: [$x], Iterable[Map[_, _]] is required")
        }
        case r: Result[_] => r.list[String, String]
      }).map {
        case (name, value) => HttpHeader.parse(name, value)
      }
      val (ok, errs) = parsedValues.partition(_.isInstanceOf[Ok])
      require(errs.isEmpty, s"Error(s) parsing http headers:\n${
        errs.map(e => e.asInstanceOf[Error].error.formatPretty).mkString("\n")}")
      ok.map(_.asInstanceOf[Ok].header).partition(_.is("content-type")) match {
        case (cts, h) => cts.map(cth => ContentType.parse(cth.value)).collect {
          case Right(ct) => ct
          case Left(errs) => throw new IllegalArgumentException(s"Error(s) parsing content type:\n${
            errs.map(_.formatPretty).mkString("\n")
          }")
        }.lift(0) -> h
      }
    }
    val reqF = {
      def reqWithoutBody = HttpRequest(httpMeth, uri, headers)
      if (op.body == null) Future.successful(reqWithoutBody)
      else doActionOpAndRender(optContentType.orNull, op.body, data, env, context).map {
        case (src, ct, clo) =>
          reqWithoutBody.withEntity(clo.map(HttpEntity(ct, _, src)).getOrElse(HttpEntity(ct, src)))
      }
    }
    doHttpRequest(reqF)
      .map(HttpResult)
      .map { r => op.conformTo.map(comp_res(r, _)).getOrElse(r) }
  }

  protected def doExtractHeader(
    op: Action.HttpHeader,
    data: Map[String, Any],
    env: Map[String, Any],
    context: ActionContext,
  )(implicit
    resFac: ResourcesFactory,
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
    req: HttpRequest,
  ): Future[QuereaseResult] = {
    val headerVal = Option(req).map(_.headers).flatMap(_.collectFirst {
      case h if h.is(op.name.toLowerCase) => StringResult(h.value())
    }).getOrElse(NoResult)
    Future.successful(headerVal)
  }

  protected def doDb(
    op: Action.Db,
    data: Map[String, Any],
    env: Map[String, Any],
    context: ActionContext,
  )(implicit
    resFac: ResourcesFactory,
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
    req: HttpRequest,
    qio: AppQuereaseIo[Dto],
  ): Future[DbResult] = {
    val newRes = resFac.initResources()
    val closeRes = resFac.closeResources(newRes, op.doRollback, _)
    val newResFact = resFac.copy()(resources = newRes)
    doSteps(op.action.steps, context.copy(stepName = "db"), Future.successful(data))(newResFact, ec, as, fs, req, qio).map {
      case DbResult(r, cl) => DbResult(r, cl.andThen(_ => closeRes(None)))
      case r => DbResult(r, closeRes)
    }.andThen {
      case Failure(NonFatal(ex)) => closeRes(Option(ex))
    }
  }

  protected def doConf(
    op: Action.Conf,
    data: Map[String, Any],
    env: Map[String, Any],
    context: ActionContext
  )(implicit
    resFac: ResourcesFactory,
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer): Future[ConfResult] = {
    def value = op.paramType match {
      case Action.NumberConf => config.getNumber(op.param)
      case Action.StringConf => config.getString(op.param)
      case Action.BooleanConf => config.getBoolean(op.param)
      case _ => config.getValue(op.param).unwrapped()
    }
    def scalaType(value: Any): Any = value match {
      case m: java.util.Map[_, _] => m.asScala.map { case (k, v) => String.valueOf(k) -> scalaType(v) }.toMap
      case l: java.util.List[_] => l.asScala.map(scalaType).toList
      case v => v
    }
    Future.successful(ConfResult(op.param, scalaType(value)))
  }

  protected def doJsonCodec(
    op: Action.JsonCodec,
    data: Map[String, Any],
    env: Map[String, Any],
    context: ActionContext
  )(implicit
    resFac: ResourcesFactory,
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
    req: HttpRequest,
    qio: AppQuereaseIo[Dto],
  ): Future[QuereaseResult] = {
    doActionOp(op.op, data, env, context)
      .flatMap(dataForNextStep(_, context, true))
      .map { res =>
        if (op.encode) {
          import ResultEncoder._
          implicit lazy val enc: JsValueEncoderPF = JsonEncoder.extendableJsValueEncoderPF(enc)(jsonValueEncoder)
          StringResult(new String(encodeJsValue(res), "UTF-8"))
        } else {
          new CborOrJsonAnyValueDecoder().decode(ByteString(String.valueOf(res))) match {
            case m: Map[String@unchecked, _] => MapResult(m)
            case s: Seq[Map[String, _]@unchecked] => IteratorResult(s.iterator)
            case n: java.lang.Number => NumberResult(n)
            case null => NoResult
            case x => StringResult(String.valueOf(x))
          }
        }
      }
  }

  protected def doActionOp(
    op: Action.Op,
    data: Map[String, Any],
    env: Map[String, Any],
    context: ActionContext,
  )(implicit
    resFac: ResourcesFactory,
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
    req: HttpRequest,
    qio: AppQuereaseIo[Dto],
  ): Future[QuereaseResult] = {
    import resFac._
    op match {
      case Action.Tresql(tresql) => Future.successful(doTresql(tresql, data ++ env, context))
      case Action.ViewCall(method, view, viewOp) => doViewCall(method, view, viewOp, data, env, context)
      case op: Action.UniqueOpt => doUniqueOpt(op, data, env, context)
      case inv: Action.Invocation => doInvocation(inv, data ++ env)
      case Action.RedirectToKey(name) =>
        val viewName = if (name == "this") context.viewName else name
        val idName = viewNameToIdName.getOrElse(viewName, null)
        val dataWithEnv = data ++ env
        val id = dataWithEnv.getOrElse(idName, null)
        Future.successful(keyResult(IdResult(id, idName), viewName, dataWithEnv))
      case st: Action.Status => doStatus(st, data, env, context)
      case Action.Commit =>
        def commit(c: Connection) = Option(c).foreach(_.commit())
        commit(resources.conn)
        resources.extraResources.foreach { case (_, r) => commit(r.conn) }
        Future.successful(NoResult)
      case cond: Action.If => doIf(cond, data, env, context)
      case foreach: Action.Foreach => doForeach(foreach, data, env, context)
      case file: Action.File => doFile(file, data, env, context)
      case toFile: Action.ToFile => doToFile(toFile, data, env, context)
      case http: Action.Http => doHttp(http, data, env, context)
      case eh: Action.HttpHeader => doExtractHeader(eh, data, env, context)
      case db: Action.Db => doDb(db, data, env, context)
      case c: Action.Conf => doConf(c, data, env, context)
      case j: Action.JsonCodec => doJsonCodec(j, data, env, context)
      /* [jobs]
      case Action.JobCall(job) =>
        doJobCall(job, data, env)
      [jobs] */
      case VariableTransforms(vts) =>
        Future.successful(doVarsTransforms(vts, Map[String, Any](), data ++ env))
      case _: Action.Else => sys.error(s"Integrity error. Else operation cannot be here, must be coalesced into if operation")
    }
  }

  private def doActionOpAndRender(
    contentType: ContentType,
    op: Action.Op,
    data: Map[String, Any],
    env: Map[String, Any],
    context: ActionContext,
   )(implicit
     resFac: ResourcesFactory,
     ec: ExecutionContext,
     as: ActorSystem,
     fs: FileStreamer,
     req: HttpRequest,
     qio: AppQuereaseIo[Dto],
   ): Future[(Source[ByteString, _], ContentType, Option[Long])] = {
    val ct: ContentType = if (contentType == null) MediaTypes.`application/json` else contentType

    def renderedResult(res: QuereaseResult, resFil: ResultRenderer.ResultFilter, isColl: Option[Boolean]):
      (Source[ByteString, _], ContentType, Option[Long]) = {
      res match {
        case StringResult(s) =>
          val data = ByteString(s)
          (Source.single(data), ct, Option(data.size))
        case TresqlResult(tr) => tr match {
          case SingleValueResult(r: Iterable[_]) =>
            (renderedSource(DataSerializer.source(() => r.iterator), resFil, isColl.getOrElse(true), ct),
              contentType, None)
          case SingleValueResult(r) =>
            (renderedSource(DataSerializer.source(() => Iterator(r)), resFil, isColl.getOrElse(false), ct),
              contentType, None)
          case r =>
            (renderedSource(TresqlResultSerializer.source(() => r), resFil, isColl.getOrElse(true), ct),
              contentType, None)
        }
        case TresqlSingleRowResult(row) =>
          (renderedSource(TresqlResultSerializer.rowSource(() => row), resFil, isColl.getOrElse(false), ct),
            contentType, None)
        case fileResult: FileResult =>
          fileHttpEntity(fileResult)
            .map(e => (e.dataBytes, e.contentType, e.contentLengthOption))
            .getOrElse(sys.error(s"File not found: ${fileResult.fileInfo}"))
        case HttpResult(res) => (res.entity.dataBytes, res.entity.contentType, res.entity.contentLengthOption)
        case CompatibleResult(r, fil, isCollection) => renderedResult(r, fil, Option(isCollection))
        case x => sys.error(s"Currently unable to create rendered source from result: $x")
      }
    }

    doActionOp(op, data, env, context).map(renderedResult(_, null, None))
  }

  protected def doHttpRequest(reqF: Future[HttpRequest])(implicit as: ActorSystem): Future[HttpResponse] = {
    reqF.flatMap(req => akka.http.scaladsl.Http().singleRequest(req))(as.dispatcher)
  }

  private def renderedSource(serializedSource: Source[ByteString, _],
                             resFilter: ResultRenderer.ResultFilter,
                             isCollection: Boolean,
                             contentType: ContentType): Source[ByteString, _] = {
    val viewDef = if (resFilter == null) null else nameToViewDef(resFilter.name)
    val renderer =
      resultRenderers.renderers.get(contentType)
        .map(_ (isCollection, resFilter, viewDef))
        .getOrElse(sys.error(s"Renderer not found for content type: $contentType"))
    serializedSource.via(BorerNestedArraysTransformer.flow(renderer))
  }

  def fileHttpEntity(fileResult: FileResult): Option[UniversalEntity] = {
    import fileResult._
    fileStreamer.getFileInfo(fileInfo.id, fileInfo.sha_256).map { fi =>
      val ct = ContentType.parse(fi.content_type).toOption.getOrElse(sys.error(s"Invalid content type: '${fi.content_type}'"))
      HttpEntity(ct, fi.size, fi.source)
    }
  }

  private def objFromHttpEntity(ent: HttpEntity, viewName: String, isCollection: Boolean)(implicit
                                                                                          as: ActorSystem) = {
    import scala.concurrent.duration._
    implicit val ec = as.dispatcher

    def decodeToMap(bs: ByteString) =
      if (viewName == null) new CborOrJsonAnyValueDecoder().decode(bs)
      else cborOrJsonDecoder.decodeToMap(bs, viewName)(viewNameToMapZero)
    def decodeToSeqOfMaps(bs: ByteString) =
      if (viewName == null) new CborOrJsonAnyValueDecoder().decode(bs)
      else cborOrJsonDecoder.decodeToSeqOfMaps(bs, viewName)(viewNameToMapZero)

    ent.toStrict(1.second).map { se =>
      if (ent.contentType == ContentTypes.`application/json`)
        if (isCollection) decodeToSeqOfMaps(se.data) else decodeToMap(se.data)
      else se.data.decodeString("UTF-8")
    }
  }

  private def dataForNextStep(res: QuereaseResult, context: ActionContext,
                              unwrapSingleValue: Boolean)(implicit as: ActorSystem): Future[_] = {
    def v(view: String) = viewDef(
      if (view == "this") context.view.map(_.name) getOrElse view
      else view
    )

    def mayBeUnwrapSingleVal(l: Seq[Map[String, Any]]) = l match {
      case row :: Nil if row.size == 1 => row.head._2
      case rows => rows
    }

    (res match {
      case TresqlResult(tr) => tr match {
        case dml: DMLResult =>
          dml.id.map(IdResult(_, null)) orElse dml.count getOrElse 0
        case SingleValueResult(v) => v
        case ar: ArrayResult[_] => ar.values.toList
        case r: Result[_] =>
          val l = r.toListOfMaps
          if (unwrapSingleValue) mayBeUnwrapSingleVal(l) else l
      }
      case srr: TresqlSingleRowResult => srr.map(_.toMap)
      case MapResult(mr) => mr
      case IteratorResult(ir) => ir.toList
      case LongResult(nr) => nr
      case NumberResult(nr) => nr
      case StringResult(str) => str
      case id: IdResult => id
      case kr: KeyResult => kr.ir
      case StatusResult(code, value) => Map("code" -> code, "value" ->
        (value match {
          case StringStatus(v) => v
          case RedirectStatus(value) => tresqlUri.uri(value).toString()
        }))
      case fi: FileInfoResult => fi.fileInfo.toMap
      case fr: FileResult => fileHttpEntity(fr).map(objFromHttpEntity(_, null, false))
        .getOrElse(sys.error(s"File not found: ${fr.fileInfo}"))
      case HttpResult(r) =>
        if (r.status.isRedirection())
          r.headers.find(_.is("location")).map(_.value()).getOrElse("")
        else objFromHttpEntity(r.entity, null, false)
      case NoResult => NoResult
      case CompatibleResult(r, filter, isCollection) => r match {
        case TresqlResult(r: Result[_]) =>
          val l = toCompatibleSeqOfMaps(r, v(filter.name)) // FIXME assumes that filter name matches view name, refactor!
          if (unwrapSingleValue) mayBeUnwrapSingleVal(l) else l
        case r: TresqlSingleRowResult => r.map(toCompatibleMap(_, v(filter.name))) // FIXME assumes that filter name matches view name
        case fr: FileResult => fileHttpEntity(fr).map(objFromHttpEntity(_, filter.name, isCollection)) // FIXME assumes that filter matches view name
          .getOrElse(sys.error(s"File not found: ${fr.fileInfo}"))
        case HttpResult(r) => objFromHttpEntity(r.entity, filter.name, isCollection) // FIXME assumes that filter name matches view name
        case r => dataForNextStep(r, context, unwrapSingleValue)
      }
      case DbResult(dbr, cl) => dataForNextStep(dbr, context, unwrapSingleValue).andThen {
        case r => cl(r.failed.toOption) // close db resources
      } (as.dispatcher)
      case ConfResult(_, r) => r
      case x => sys.error(s"${x.getClass.getName} not expected here!")
    }) match {
      case f: Future[_] => f
      case x => Future.successful(x)
    }
  }

  private def comp_res(res: DataResult, conformTo: Action.OpResultType) =
    CompatibleResult(res,
      new ResultRenderer.ViewFieldFilter(conformTo.viewName, nameToViewDef), conformTo.isCollection)

  private def notFound = StatusResult(StatusCodes.NotFound.intValue, StringStatus("not found"))

  // TODO add macros from resources
  abstract class AppQuereaseDefaultParser(cache: Option[Cache]) extends DefaultParser(cache) {
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

  object AppQuereaseDefaultParser extends AppQuereaseDefaultParser(createParserCache)

  val tresqlParserCacheSize: Int = 4096
  override val parser: QuereaseExpressions.Parser = this.AppQuereaseDefaultParser
}

trait Dto extends org.mojoz.querease.Dto { self =>

  override protected type QDto >: Null <: this.type

  import AppMetadata._

  /* TODO Dto.auth?
  private val auth = scala.collection.mutable.Map[String, Any]()

  override def toMapWithOrdering(fieldOrdering: Ordering[String])(implicit qe: QuereaseMetadata): Map[String, Any] =
    super.toMapWithOrdering(fieldOrdering) ++ (if (auth.isEmpty) Map() else Map("auth" -> auth.toMap))

  override protected def toString(fieldNames: Seq[String])(implicit qe: QuereaseMetadata): String = {
    super.toString(fieldNames) +
      (if (auth.isEmpty) "" else ", auth: " + auth.toString)
  }
  */
  private def isSavableFieldAppExtra(
    field: FieldDef,
    view: ViewDef,
    saveToMulti: Boolean,
    saveToTableNames: Seq[String],
  ) = {
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
    lazy val idxSlash = field.options.indexOf('/')
    lazy val idxPlus  = field.options.indexOf('+')
    lazy val idxEq    = field.options.indexOf('=')
    lazy val idxExcl  = field.options.indexOf('!')
    lazy val optsIns  = idxExcl < 0 && idxPlus >= 0 && (idxSlash < 0 || idxPlus < idxSlash)
    lazy val optsUpd  = idxExcl < 0 && idxEq   >= 0 && (idxSlash < 0 || idxEq   < idxSlash)

    (field.options == null  ||
     optsIns && optsUpd     ||
     optsIns && isForInsert ||
     optsUpd && isForUpdate)
  }

  override protected def isSavableField(
      field: FieldDef,
      view: ViewDef,
      saveToMulti: Boolean,
      saveToTableNames: Seq[String]) =
    isSavableFieldAppExtra(field, view, saveToMulti, saveToTableNames) &&
      super.isSavableField(field, view, saveToMulti, saveToTableNames)

  override protected def isSavableChildField(
      field: FieldDef,
      view: ViewDef,
      saveToMulti: Boolean,
      saveToTableNames: Seq[String],
      childView: ViewDef): Boolean =
    isSavableFieldAppExtra(field, view, saveToMulti, saveToTableNames)

  override protected def throwUnsupportedConversion(
      value: Any, targetType: Manifest[_], fieldName: String, cause: Throwable = null): Unit = {
    throw new UnprocessableEntityException(
      "Illegal value or unsupported type conversion from %s to %s - failed to populate %s", cause,
       value.getClass.getName, targetType.toString, s"${getClass.getName}.$fieldName")
  }

  // TODO Drop or extract string parsing for number and boolean fields! Then rename parseJsValue() to exclude 'parse'!
  protected def parseJsValue(fieldName: String, emptyStringsToNull: Boolean)(implicit qe: QuereaseMetadata): PartialFunction[JsValue, Any] = {
    import scala.language.existentials
    val (typ, parType) = setters(fieldName) match {
      case DtoSetter(_, met, mOpt, mSeq, mDto, mOth) =>
        (if (mSeq != null) mSeq else if (mDto != null) mDto else mOth,
         if (mSeq == null) null else if (mDto != null) mDto else mOth,
        )
    }
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
  def fill(js: JsObject)(implicit qe: QuereaseMetadata): this.type = fill(js, emptyStringsToNull = true)(qe)
  def fill(js: JsObject, emptyStringsToNull: Boolean)(implicit qe: QuereaseMetadata): this.type = {
    js.fields foreach { case (name, value) =>
      setters.get(name).map { case s =>
        val converted = parseJsValue(name, emptyStringsToNull)(qe)(value).asInstanceOf[Object]
        if  (s.mfOpt == null)
             s.method.invoke(this, converted)
        else s.method invoke(this, Some(converted))
      }
    }
    this
  }
}

trait DtoWithId extends Dto with org.mojoz.querease.DtoWithId

object DefaultAppQuerease extends AppQuerease
object DefaultAppQuereaseIo extends AppQuereaseIo[Dto](DefaultAppQuerease)
