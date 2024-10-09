package org.wabase

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpHeader.ParsingResult.{Error, Ok}
import akka.http.scaladsl.model.headers.ContentDispositionTypes.attachment
import akka.http.scaladsl.model.headers.`Content-Disposition`
import akka.http.scaladsl.model.{ContentType, ContentTypes, HttpCharsets, HttpEntity, HttpHeader, HttpMethods, HttpRequest, HttpResponse, MediaTypes, Multipart, StatusCodes, UniversalEntity}
import akka.http.scaladsl.server.RequestContext
import akka.http.scaladsl.server.directives.ContentTypeResolver
import akka.http.scaladsl.server.directives.FileAndResourceDirectives.ResourceFile
import akka.stream.scaladsl.{Source, StreamConverters}
import akka.util.ByteString
import com.typesafe.scalalogging.Logger
import org.tresql._
import org.mojoz.querease._
import org.mojoz.querease.SaveMethod

import org.mojoz.metadata.Type
import org.mojoz.metadata.{FieldDef, ViewDef}
import org.slf4j.LoggerFactory
import org.wabase.AppFileStreamer.FileInfo
import org.wabase.AppMetadata.Action.{VariableTransform, VariableTransforms}
import org.wabase.AppMetadata.DbAccessKey

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
  initResources: (PoolName, Seq[DbAccessKey]) => Resources,
  closeResources: (Resources, Boolean, Option[Throwable]) => Unit,
)(implicit val resources: Resources)
{
  def focus(name: String): ResourcesFactory =
    if (resources.extraResources.contains(name))
      copy()(resources.extraResources(name).withExtraResources(resources.extraResources))
    else this
}

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
case class AnyResult(result: Any) extends QuereaseResult
case class QuereaseDeleteResult(count: Int) extends QuereaseResult
case class StatusResult(code: Int, value: StatusValue) extends QuereaseResult
case class ResourceResult(resource: String, contentType: ContentType, httpCtx: RequestContext) extends DataResult
case class FileInfoResult(fileInfo: FileInfo) extends QuereaseResult
case class FileResult(fileInfo: FileInfo, fileStreamer: FileStreamer) extends DataResult
case class RequestPartResult(result: Source[RequestPart, Any]) extends DataResult
case class RequestPart(name: String, contentType: ContentType, filename: String, data: Source[ByteString, Any])
sealed trait TemplateResult extends QuereaseResult
  { def contentString: String }
case class StringTemplateResult(content: String) extends TemplateResult
  { override def contentString: String = content }
case class FileTemplateResult(filename: String, contentType: String, content: Array[Byte]) extends TemplateResult
  { override def contentString: String = new String(content, "UTF-8") }
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

class AppQuereaseIo[DTO <: Dto](val qe: QuereaseMetadata with QuereaseResolvers with ValueTransformer)
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

  override def convertToType(value: Any, targetClass: Class[_]): Any =
    Format.convertToType(value, targetClass)

  val resultRenderers: ResultRenderers = new ResultRenderers
  val tresqlUri: TresqlUri = new TresqlUri()
  lazy val cborOrJsonDecoder = new CborOrJsonDecoder(typeDefs, nameToViewDef)
  /** Override this to override default scala value (like String, Number, Boolean, null, Iterable, Map) json encoding.
    * Default implementation is {{{Writer => PartialFunction.empty}}}
    * */
  val jsonValueEncoder: ResultEncoder.JsValueEncoderPF = _ => PartialFunction.empty
  lazy val templateEngine: WabaseTemplate = createTemplateEngine
  protected def createTemplateEngine: WabaseTemplate = {
    getObjectOrNewInstance(config.getString("app.template.engine"), "template engine") match {
      case wt: WabaseTemplate => wt
      case x => sys.error(s"Expected type WabaseTemplate, got: ${x.getClass.getName}")
    }
  }
  lazy val emailSender: WabaseEmail = createEmailSender
  protected def createEmailSender: WabaseEmail = {
    getObjectOrNewInstance(config.getString("app.email.sender"), "email sender") match {
      case es: WabaseEmail => es
      case x => sys.error(s"Expected type WabaseEmail, got: ${x.getClass.getName}")
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
  override def rowsResult(viewDef: ViewDef, params: Map[String, Any],
      offset: Int, limit: Int, orderBy: String,
      extraFilter: String, extraParams: Map[String, Any],
      fieldFilter: FieldFilter)(
      implicit resources: Resources): Result[RowLike] = {
    val extraFilterAndAuth =
      extraFilterAndAuthString(extraFilter, viewDef.auth.forList)
    super.rowsResult(viewDef, params, offset, limit, orderBy, extraFilterAndAuth, extraParams, fieldFilter)
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
          try convertToType(v, f.type_)
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
      objName: String,
      actionName: String,
      data: Map[String, Any],
      env: Map[String, Any],
      fieldFilter: FieldFilter = null,
      doCleanup: Boolean = false,
    )(resourcesFactory: ResourcesFactory,
      fileStreamer: FileStreamer,
      reqCtx: RequestContext,
      qio: AppQuereaseIo[Dto],
    ): QuereaseAction[QuereaseResult] = {
        new QuereaseAction[QuereaseResult] {
          def run(implicit ec: ExecutionContext, as: ActorSystem) = {
            implicit val resFac =
              if (isExplicitDb(objName, actionName)) resourcesFactory
              else {
                val (poolName, extraDbs) = dbResourceNames(objName, actionName)
                resourcesFactory.copy()(resources = resourcesFactory.initResources(poolName, extraDbs))
              }
            implicit val fs = fileStreamer
            implicit val httpReqCtx = reqCtx
            implicit val io = qio
            import resFac._
            def processResult(res: QuereaseResult, cleanup: Option[Throwable] => Unit): QuereaseResult = res match {
              case DbResult(result, cl) =>
                // close outer resources
                cleanup(None)
                processResult(result, cl)
              case r: QuereaseCloseableResult if !doCleanup => QuereaseResultWithCleanup(r, cleanup)
              case r: QuereaseResult =>
                cleanup(None)
                r
            }

            try {
              doAction(objName, actionName, data, env, fieldFilter).map {
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
    log: (=> String) => Unit,
    fieldFilter: FieldFilter = null,
    stepName: String = null,
    contextStack: List[ActionContext] = Nil,
  ) {
    val name = s"$viewName.$actionName" + Option(stepName).map(s => s".$s").getOrElse("")
  }

  private[wabase] def quereaseActionOpt(objectName: String, actionName: String) = {
    if (actionName == JobAct) Option(jobDef(objectName).action)
    else {
      val vd = viewDef(objectName)
      vd.actions.get(actionName)
        .orElse(actionName match {
          case Action.Insert | Action.Update | Action.Upsert =>
            vd.actions.get(Action.Save)
          case _ => None
        })
    }
  }

  private def isExplicitDb(objectName: String, actionName: String) = {
    if (actionName == JobAct) jobDef(objectName).explicitDb
    else viewDef(objectName).explicitDb
  }

  def dbResourceNames(objectName: String, actionName: String): (PoolName, Seq[DbAccessKey]) = {
    if (actionName == JobAct) {
      val jdo = jobDefOption(objectName)
      val poolName = jdo.flatMap(j => Option(j.db)).map(PoolName) getOrElse PoolName(defaultCpName)
      val extraDbs = jdo.map(_.dbAccessKeys.filter(_.db != null)).getOrElse(Nil)
      (poolName, extraDbs)
    } else {
      val vdo = viewDefOption(objectName)
      val poolName = vdo.flatMap(v => Option(v.db)).map(PoolName) getOrElse PoolName(defaultCpName)
      val extraDbs = vdo.map(_.actionToDbAccessKeys(actionName).filter(_.db != null).toList).getOrElse(Nil)
      (poolName, extraDbs)
    }
  }

  def requestPartsToMap(result: RequestPartResult)(
    implicit fs: FileStreamer, as: ActorSystem): Future[Map[String, Any]] =
    AppQuerease.requestPartsToMap(result)

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
    reqCtx: RequestContext,
    qio: AppQuereaseIo[Dto],
  ): Future[QuereaseResult] = {
    do_action(view, actionName, data, env, fieldFilter)
  }

  private def loggable(res: Resources, x: Any): String = {
    def hinted(v: Any, s: String) = v match {
      case _: Map[String @unchecked, _] => s"{$s}"
      case _: Seq[_]                    => s"[$s]"
      case _                            =>     s
    }
    x match {
      case m: scala.collection.Map[String @unchecked, _] =>
        m.map {
          case (k, v) =>
            val vs   = loggable(res, v)
            val safe = Option(res.bindVarLogFilter).filter(_.isDefinedAt((k, vs))).map(_((k, vs))).getOrElse(vs)
            s"$k -> ${hinted(v, safe)}"
        }.mkString(", ")
      case s: String => s
      case s: scala.collection.Seq[_] =>
        s.map(sv => hinted(sv, loggable(res, sv))).mkString(", ")
      case "" => "\"\""
      case x => s"$x"
    }
  }
  private def logContext(ctx: ActionContext, env: Map[String, Any], rf: ResourcesFactory) = {
    val res = rf.resources
    ctx.log(s"Doing action '${ctx.name}'")
    ctx.log(s"Ctx stack: [${ctx.contextStack.map(_.name).mkString(", ")}]")
    ctx.log(s"Database connections: [${(("[main]", res.conn) ::
      res.extraResources.map{case (n, r) => n -> r.conn}.toList).mkString(", ")}]")
    ctx.log(s"Env: {${loggable(res, env)}}")
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
    reqCtx: RequestContext,
    qio: AppQuereaseIo[Dto],
  ): Future[QuereaseResult] = {
    val ctx = ActionContext(view, actionName, env, viewDefOption(view), quereaseActionLogger(s"$view.$actionName.ctx"),
      fieldFilter, null, contextStack)
    logContext(ctx, env, resourcesFactory)
    val steps =
      quereaseActionOpt(view, actionName)
        .map(_.steps)
        .getOrElse(List(Action.Return(None, Nil, Action.ViewCall(actionName, view, null))))
    doSteps(steps, ctx, Future.successful(data))
  }

  def quereaseActionLogger(name: String): (=>String) => Unit = {
    val logger = Logger(LoggerFactory.getLogger(name))
    msg => {
      logger.debug(msg)
      if(!logger.underlying.isDebugEnabled()) this.logger.debug(msg)
    }
  }

  def doSteps(
    steps: List[Action.Step],
    context: ActionContext,
    curData: Future[Map[String, Any]],
  )(implicit
    resourcesFactory: ResourcesFactory,
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
    reqCtx: RequestContext,
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
        context.log(s"Doing action '${context.name}' step '$step'.")
        context.log(s"Step data: {${loggable(resourcesFactory.resources, stepData)}}")
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
            .flatMap(updateCurRes(_, n, dataForNextStep(res, context, true)))
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
    validationsQueryString(view, validations) foreach { vs =>
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
    tresql: Action.Tresql,
    bindVars: Map[String, Any],
    context: ActionContext,
  )(implicit
    resources: Resources,
  ): DataResult = {
    val r = TresqlResult(Query(tresql.tresql)(resources.withParams(bindVars)))
    tresql.conformTo.map(comp_res(r, _)).getOrElse(r)
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
    reqCtx: RequestContext,
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
          case NoResult => env
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
                string(OrderKey).orNull, null, Map(), context.fieldFilter))
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
        do_action(viewName, method, callData, env, context.fieldFilter, context :: context.contextStack)(
          resFac.focus(if (v.db != null) v.db else defaultCpName), ec, as, fs, reqCtx, qio)
      }
    }
  }

  protected def getInvocationParameter(defaultGetParameter: Class[_] => Any)(parameterClass: Class[_]): Any =
    defaultGetParameter(parameterClass)

  protected def doInvocation(
    op: Action.Invocation,
    data: Map[String, Any],
    env: Map[String, Any],
    context: ActionContext,
  )(
    implicit resFac: ResourcesFactory,
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
    reqCtx: RequestContext,
    qio: AppQuereaseIo[Dto],
  ): Future[QuereaseResult] = {
    import op._
    def invokeFunction(className: String, function: String, params: Seq[(Class[_], Class[_] => Any)]): Any = {
      val contextParams = Seq[(Class[_], Class[_] => Any)](
        (classOf[Resources], _ => resFac.resources),
        (classOf[ResourcesFactory], _ => resFac),
        (classOf[ExecutionContext], _ => ec),
        (classOf[ActorSystem], _ => as),
        (classOf[FileStreamer], _ => fs),
        (classOf[HttpRequest], _ => Option(reqCtx).map(_.request).orNull),
        (classOf[RequestContext], _ => reqCtx),
        (classOf[AppQuereaseIo[Dto]], _ => qio),
      )
      org.wabase.invokeFunction(className, function,
        getInvocationParameter(org.wabase.invocationParameter(params ++ contextParams)(_))(_))
    }

    def wrongRes(x: Any) =
      sys.error(s"Unrecognized result type: ${x.getClass}, value: $x from function $className.$function. You " +
        s"may want to prefix invocation with 'as any'")

    def comp_q_result(r: Any) = {
      val allowAny = op.conformTo.exists(_.viewName == null)
      def qresult(r: Any): QuereaseResult = r match {
        case null | () => NoResult // reflection call on function with Unit (void) return type returns null
        case r: Result[_] => TresqlResult(r)
        case r: RowLike => TresqlSingleRowResult(r)
        case l: Long => LongResult(l)
        case s: String => StringResult(s)
        case n: java.lang.Number => NumberResult(n)
        case d: Dto => MapResult(d.toMap(this))
        case o: Option[Dto]@unchecked => o.map(d => MapResult(d.toMap(this))).getOrElse(notFound)
        case h: HttpResponse => HttpResult(h)
        case q: QuereaseResult => q
        // view compatible collections if not allow any
        case i: Iterator[_] if !allowAny => IteratorResult(i.map {
          case m: Map[String, Any]@unchecked => m
          case m: java.util.Map[String, Any]@unchecked => m.asScala.toMap
          case d: Dto => d.toMap(this)
          case x => wrongRes(x)
        })
        case m: Map[String, Any]@unchecked if !allowAny => MapResult(m)
        case l: Iterable[_] if !allowAny => qresult(l.iterator)
        // view compatible collections if not allow any for java types
        case m: java.util.Map[_, _] => qresult(m.asScala.toMap)
        case i: java.lang.Iterable[_] => qresult(i.asScala)
        case i: java.util.Iterator[_] => qresult(i.asScala)
        case a: Array[_] => qresult(a.iterator)
        //any res
        case x if allowAny => (x match { // convert dto(s) in collections to map for json encoder
          case v: Map[_, _] => v
          case v: Iterable[_] => qresult(v.iterator)
          case v: Iterator[_] => v.map {
            case d: Dto => d.toMap(this)
            case v => v
          }
          case v => v
        }) match {
          case v: AnyResult => v
          case v => AnyResult(v)
        }
        case x => wrongRes(x)
      }

      def createCompatibleResult(result: QuereaseResult, conformTo: Action.OpResultType) = result match {
        case c: CompatibleResult =>
          require(c.isCollection == conformTo.isCollection, s"Incompatible results $c != $conformTo")
          val c1 = comp_res(c.result, conformTo)
          c.copy(resultFilter = new ResultRenderer.IntersectionFilter(c1.resultFilter, c.resultFilter))
        case r: DataResult => comp_res(r, conformTo)
        case x => x
      }
      val qr = qresult(r)
      conformTo.map(createCompatibleResult(qr, _)).getOrElse(qr)
    }

    (if (op.arg == null) {
      val invocationData = data ++ env
      invokeFunction(className, function,
        Seq(
          (classOf[scala.collection.immutable.Map[_, _]], _ => invocationData),
          (classOf[Dto], parClass => {
            import qio.MapJsonFormat
            val mf = Manifest.classType[Dto](parClass) // somehow need to specify method type parameter Dto for not to fail in runtime on next line??
            qio.fill(invocationData.toJson.asJsObject)(mf) // specify manifest explicitly so it is not Nothing
          }),
          (classOf[java.util.Map[_, _]], _ => invocationData.asJava),
          (classOf[MapResult], _ => MapResult(invocationData)),
        )
      )
    } else {
      doActionOp(op.arg, data, env, context).flatMap { opRes =>
        invokeFunction(className, function, Seq((classOf[QuereaseResult], _ => opRes))) match {
          case f: Future[_] => f
          case x => Future.successful(x)
        }
      }
    }) match {
      case f: Future[_] => f map comp_q_result
      case x => Future.successful(comp_q_result(x))
    }
  }

  protected def doJob(
    job: Action.Job,
    data: Map[String, Any],
    env: Map[String, Any],
    context: ActionContext,
  )(implicit
    resFac: ResourcesFactory,
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
    reqCtx: RequestContext,
    qio: AppQuereaseIo[Dto],
  ): Future[QuereaseResult] = {
    import resFac._
    val jobName =
      if (job.isDynamic) Query(job.nameTresql).unique[String] else job.nameTresql
    val ctx = ActionContext(jobName, JobAct, env, None, context.log,
      contextStack = context :: context.contextStack)
    val jd = jobDef(jobName)
    doSteps(jd.action.steps, ctx, Future.successful(data))
  }

  protected def doVarsTransforms(transforms: List[VariableTransform],
                                 seed: Map[String, Any],
                                 data: Map[String, Any]): MapResult = {
    val transRes = transforms.foldLeft(seed) { (sd, vt) =>
      if (vt.from == "_") sd ++ data //indicates all call data map
      else Query(":" + vt.from)(new Resources {}.withParams(data)) match {
        case SingleValueResult(m: Map[String, _]@unchecked) => sd ++ m
        case SingleValueResult(x) =>
          val f = vt.from
          val from = f.substring(f.lastIndexOf(".") + 1, f.length)
          sd + (vt.to.getOrElse(from) -> x)
        case x => sys.error(s"Unexpected variable transformation result: $x, expected SingleValueResult")
      }
    }
    MapResult(transRes)
  }

  protected def doUnique(
    op: Action.Unique,
    data: Map[String, Any],
    env: Map[String, Any],
    context: ActionContext,
  )(implicit
    resFac: ResourcesFactory,
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
    reqCtx: RequestContext,
    qio: AppQuereaseIo[Dto],
  ): Future[QuereaseResult] = {
    def createGetResult(res: QuereaseResult): QuereaseResult = res match {
      case TresqlResult(r) if !r.isInstanceOf[DMLResult] =>
        if (op.opt) r.uniqueOption map TresqlSingleRowResult getOrElse notFound
        else TresqlSingleRowResult(r.unique)
      case IteratorResult(r) =>
        try r.hasNext match {
          case true =>
            val v = r.next()
            if (r.hasNext) sys.error("More than one row for unique result") else MapResult(v)
          case false => if (op.opt) notFound else throw new NoSuchElementException(s"No rows in result")
        } finally r match {
          case c: AutoCloseable => c.close()
          case _ =>
        }
      case c: CompatibleResult => createGetResult(c.result) match {
        case dr: DataResult => c.copy(result = dr)
        case r => r
      }
      case r => sys.error(s"unique opt can only process DataResult type, instead encountered: $r")
    }
    val r = doActionOp(op.innerOp, data, env, context) map createGetResult
    op.conformTo.map(rf => r.map {
      case dr: DataResult => comp_res(dr, rf)
      case x => x
    }).getOrElse(r)
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
   reqCtx: RequestContext,
   qio: AppQuereaseIo[Dto],
  ): Future[QuereaseResult] = {
    val Action.Status(maybeCode, bodyTresql, parameterIndex) = op
    Option(bodyTresql).map { bt =>
      doActionOp(Action.Unique(Action.Tresql(bt), opt = true), data, env, context).map {
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
    reqCtx: RequestContext,
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
    reqCtx: RequestContext,
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
      var idx = 0
      Future.traverse(mapIterator.toSeq) { itData =>
        val IdxName = "__idx"
        val dataWithIdx = if (itData.contains(IdxName)) itData else itData + (IdxName -> idx)
        idx += 1
        doSteps(op.action.steps, context.copy(stepName = "foreach"), Future.successful(dataWithIdx))
          .flatMap {
            case MapResult(r) => Future.successful(r)
            case _ => Future.successful(dataWithIdx)
          }
      }
    }.map(it => IteratorResult(it.iterator))
  }

  protected def doResource(
    op: Action.Resource,
    data: Map[String, Any],
    env: Map[String, Any],
    context: ActionContext,
  )(implicit
    res: Resources,
    reqCtx: RequestContext,
  ): Future[ResourceResult] = {
    val resource = Query(op.nameTresql.tresql)(res.withParams(data ++ env)).unique[String]
    val ct = Option(op.contentTypeTresql)
      .map { ctt =>
        val ct = Query(ctt.tresql)(res.withParams(data ++ env)).unique[String]
        ContentType.parse(ct)
          .toOption
          .getOrElse(sys.error(s"Invalid content type: $ct"))
      }
      .getOrElse {
        ContentTypeResolver.withDefaultCharset(HttpCharsets.`UTF-8`)(resource)
      }
    Future.successful(ResourceResult(resource, ct, reqCtx))
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
    val (id, sha) = Query(op.idShaTresql.tresql)(res.withParams(data ++ env)).unique[Long, String]
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
    reqCtx: RequestContext,
    qio: AppQuereaseIo[Dto],
  ): Future[FileInfoResult] = {
    import akka.http.scaladsl.model.{MediaTypes, ContentType}
    import resFac._
    val bindVars = data ++ env
    def getVal(tr: Action.Tresql) = Query(tr.tresql)(resources.withParams(bindVars)).unique[String]
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

  protected def doTemplate(
    op: Action.Template,
    data: Map[String, Any],
    env: Map[String, Any],
    context: ActionContext,
  )(implicit
    resFac: ResourcesFactory,
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
    reqCtx: RequestContext,
    qio: AppQuereaseIo[Dto],
  ): Future[TemplateResult] = {
    import resFac._
    val bindVars = data ++ env
    val template = Query(op.templateTresql.tresql)(resources.withParams(bindVars)).unique[String]
    val resF =
      if (op.dataOp == null) {
        templateEngine(template, bindVars)
      } else {
        doActionOp(op.dataOp, data, env, context)
          .flatMap(dataForNextStep(_, context, false))
          .flatMap {
            case m: Map[String@unchecked, _] => templateEngine(template, m)
            case s: Seq[Map[String, _]@unchecked] => templateEngine(template, s)
            case NoResult => templateEngine(template, Map.empty)
            case x =>
              val className = Option(x).map(_.getClass.getName).orNull
              sys.error(s"Unexpected template data class: $className. Expecting Map[String, _] or Seq[_]")
          }
      }
    Option(op.filenameTresql)
      .map(t => Query(t.tresql)(resources.withParams(bindVars)).unique[String])
      .map { filename =>
        resF.map {
          case ft: FileTemplateResult => ft.copy(filename = filename)
          case StringTemplateResult(r) => FileTemplateResult(
            filename, ContentTypes.`text/plain(UTF-8)`.toString, r.getBytes("UTF8"))
        }
      }.getOrElse(resF)
  }

  protected def doEmail(
    op: Action.Email,
    data: Map[String, Any],
    env: Map[String, Any],
    context: ActionContext,
  )(implicit
    resFac: ResourcesFactory,
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
    reqCtx: RequestContext,
    qio: AppQuereaseIo[Dto],
  ): Future[LongResult] = {
    def subj_body(bv: Map[String, Any]) = {
      def stringContent(qr: QuereaseResult) = qr match {
        case TresqlResult(r) => Future.successful(r.unique[String])
        case _ => renderedResult(qr, null, null, Option(false))
          ._1.runReduce(_ ++ _).map(_.decodeString("UTF8"))
      }
      Future.traverse(List(op.subject, op.body))(doActionOp(_, bv, env, context).flatMap(stringContent))
    }
    def s(v: Any): String = if (v == null) null else String.valueOf(v)
    import resFac._
    val bindVars = data ++ env
    val emails = {
      val r = Query(op.emailTresql.tresql)(resources.withParams(bindVars))
      if (op.isBatch) r else (try r.uniqueOption catch {
        case _: TooManyRowsException => throw new TooManyRowsException(s"Tresql '${op.emailTresql.tresql}' returned more than one row. " +
          s"Use 'email batch' to send more than one email.")
      }).iterator
    }
    val count = emails.foldLeft(Future.successful(0)) { (c, row) =>
        val email = row.toMap
        val to = s(email.getOrElse("to", sys.error(s"""Missing "to" address - email can not be sent""")))
        val cc = s(email.getOrElse("cc", null))
        val bcc = s(email.getOrElse("bcc", null))
        val from = s(email.getOrElse("from", null))
        val replyTo = s(email.getOrElse("replyTo", null))
        val opData = bindVars ++ email
        subj_body(opData).flatMap { sb =>
          val List(subject, body) = sb
          Future.traverse(op.attachmentsOp)(doActionOp(_, opData, env, context)
            .map(
              renderedResult(_, null, null, Option(false)) match {
                case (src, fn, ct, _) => EmailAttachment(fn, ct.value, src)
              }
            )
          ).flatMap { att =>
            emailSender.sendMail(to, subject, body, att, cc, bcc, from, replyTo)
          }
        }.flatMap(_ => c.map(_ + 1))
      }
    count.map(LongResult(_))
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
    reqCtx: RequestContext,
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
      val parsedValues = (Query(op.headerTresql.tresql, opData) match {
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
    def do_http: HttpRequest => Future[HttpResponse] = {
      import context._
      val http_logger = Logger(LoggerFactory.getLogger(s"$viewName.$actionName.http"))
      req => {
        http_logger.debug(s"HTTP ${req.method.value} ${req.uri}")
        doHttpRequest(req)
      }
    }
    reqF
      .flatMap(do_http)
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
    reqCtx: RequestContext,
  ): Future[QuereaseResult] = {
    val headerVal = Option(reqCtx).map(_.request.headers).flatMap(_.collectFirst {
      case h if h.is(op.name.toLowerCase) => StringResult(h.value())
    }).getOrElse(NoResult)
    Future.successful(headerVal)
  }

  protected def doExtractCookie(
    op: Action.Cookie,
    data: Map[String, Any],
    env: Map[String, Any],
    context: ActionContext,
  )(implicit
    resFac: ResourcesFactory,
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
    reqCtx: RequestContext,
  ): Future[QuereaseResult] = {
    val cookie = Option(reqCtx).map(_.request.cookies).flatMap(_.collectFirst {
      case h if h.name.toLowerCase == op.name.toLowerCase => StringResult(h.value)
    }).getOrElse(NoResult)
    Future.successful(cookie)
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
    reqCtx: RequestContext,
    qio: AppQuereaseIo[Dto],
  ): Future[DbResult] = {
    val (poolName, extraDbs) =
      if (op.dbs.nonEmpty) {
        def may_be_add_extra(pn: PoolName, edb: Seq[DbAccessKey]) =
          if (pn.connectionPoolName == defaultCpName || edb.exists(_.db == pn.connectionPoolName)) (pn, edb)
          else (pn, edb ++ Seq(DbAccessKey(pn.connectionPoolName)))
        may_be_add_extra(PoolName(op.dbs.head.db), op.dbs.tail)
      }
      else dbResourceNames(context.viewName, context.actionName)
    val newResFact = resFac.copy()(resources = resFac.initResources(poolName, extraDbs))
      .focus(context.view.map(_.db).filter(_ != null).getOrElse(defaultCpName))
    logContext(context, env, newResFact)
    val newRes = newResFact.resources
    val closeRes = resFac.closeResources(newRes, op.doRollback, _)
    doSteps(op.action.steps, context.copy(stepName = "db"), Future.successful(data))(newResFact, ec, as, fs, reqCtx, qio).map {
      case DbResult(r, cl) => DbResult(r, cl.andThen(_ => closeRes(None)))
      case r => DbResult(r, closeRes)
    }.andThen {
      case Failure(NonFatal(ex)) => closeRes(Option(ex))
    }
  }

  protected def doBlock(
    op: Action.Block,
    data: Map[String, Any],
    env: Map[String, Any],
    context: ActionContext,
  )(implicit
    resFac: ResourcesFactory,
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
    reqCtx: RequestContext,
    qio: AppQuereaseIo[Dto],
  ): Future[QuereaseResult] = {
    doSteps(op.action.steps, context.copy(stepName = "block"), Future.successful(data))
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
    reqCtx: RequestContext,
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
          try new CborOrJsonAnyValueDecoder().decode(ByteString(String.valueOf(res))) match {
            case m: Map[String@unchecked, _] => MapResult(m)
            case s: Seq[Map[String, _]@unchecked] => IteratorResult(s.iterator)
            case n: java.lang.Number => NumberResult(n)
            case s: String => StringResult(s)
            case null => NoResult
            case x => AnyResult(x)
          } catch {
            case NonFatal(e) => throw new RuntimeException(s"ERROR decoding result: $res", e)
          }
        }
      }
  }

  protected def doExtractParts(implicit reqCtx: RequestContext, as: ActorSystem): Future[RequestPartResult] = {
    val req = reqCtx.request
    val entity = req.entity
    if (entity.contentType.mediaType.isMultipart) {
      import akka.http.scaladsl.unmarshalling.MultipartUnmarshallers._
      import akka.http.scaladsl.server.directives.MarshallingDirectives
      val um = MarshallingDirectives.as[Multipart.FormData]
      implicit val ec = as.dispatcher
      um(req).map { formdata =>
        val src = formdata.parts.map {
          case filePart if filePart.filename.isDefined =>
            RequestPart(filePart.name, filePart.entity.contentType, filePart.filename.get, filePart.entity.dataBytes)
          case dataPart =>
            RequestPart(dataPart.name, dataPart.entity.contentType, null, dataPart.entity.dataBytes)
        }
        RequestPartResult(src)
      }
    } else {
      val filename = req.uri.path.reverse.head.toString
      Future.successful(
        RequestPartResult(
          Source.single(
            RequestPart(null, entity.contentType, if (filename.isEmpty) null else filename, entity.dataBytes)
          )
        )
      )
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
    reqCtx: RequestContext,
    qio: AppQuereaseIo[Dto],
  ): Future[QuereaseResult] = {
    import resFac._
    op match {
      case to: Action.Tresql => Future.successful(doTresql(to, data ++ env, context))
      case Action.ViewCall(method, view, viewOp) => doViewCall(method, view, viewOp, data, env, context)
      case op: Action.Unique => doUnique(op, data, env, context)
      case inv: Action.Invocation => doInvocation(inv, data, env, context)
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
      case resource: Action.Resource => doResource(resource, data, env, context)
      case file: Action.File => doFile(file, data, env, context)
      case toFile: Action.ToFile => doToFile(toFile, data, env, context)
      case template: Action.Template => doTemplate(template, data, env, context)
      case email: Action.Email => doEmail(email, data, env, context)
      case http: Action.Http => doHttp(http, data, env, context)
      case eh: Action.HttpHeader => doExtractHeader(eh, data, env, context)
      case exc: Action.Cookie => doExtractCookie(exc, data, env, context)
      case db: Action.Db => doDb(db, data, env, context)
      case block: Action.Block => doBlock(block, data, env, context)
      case c: Action.Conf => doConf(c, data, env, context)
      case j: Action.JsonCodec => doJsonCodec(j, data, env, context)
      case job: Action.Job => doJob(job, data, env, context)
      case Action.ExtractParts => doExtractParts
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
     reqCtx: RequestContext,
     qio: AppQuereaseIo[Dto],
   ): Future[(Source[ByteString, _], ContentType, Option[Long])] = {
    doActionOp(op, data, env, context)
      .map(renderedResult(_, contentType, null, None))
      .map { case (src, _, ct, l) => (src, ct, l) }
  }

  private def renderedResult(
    res: QuereaseResult,
    contentType: ContentType,
    resFil: ResultRenderer.ResultFilter,
    isCollection: Option[Boolean],
  ): (Source[ByteString, _], String, ContentType, Option[Long]) = {
    val ct: ContentType = if (contentType == null) MediaTypes.`application/json` else contentType

    def encodeJson(data: Any): (Source[ByteString, _], String, ContentType, Option[Long]) = {
      import ResultEncoder._
      implicit lazy val enc: JsValueEncoderPF = JsonEncoder.extendableJsValueEncoderPF(enc)(jsonValueEncoder)
      val res = encodeJsValue(data)
      (Source.single(ByteString(res)), null, ct, Option(res.length))
    }

    def encodePrimitive(v: Any): (Source[ByteString, _], String, ContentType, Option[Long]) = {
      val b = String.valueOf(v).getBytes("UTF8")
      (Source.single(ByteString(b)), null, ContentTypes.`text/plain(UTF-8)`, Option(b.length))
    }

    res match {
      case StringResult(v) => encodePrimitive(v)
      case LongResult(v) => encodePrimitive(v)
      case NumberResult(v) => encodePrimitive(v)
      case ConfResult(_, v) => v match {
        case i: Iterable[_] => encodeJson(i)
        case _ => encodePrimitive(v)
      }
      case AnyResult(v) => encodeJson(v)
      case MapResult(data) =>
        (renderedSource(DataSerializer.source(() => Seq(data).iterator), resFil, false, ct),
          null, contentType, None)
      case IteratorResult(data) =>
        (renderedSource(DataSerializer.source(() => data), resFil, true, ct), null, contentType, None)
      case TresqlResult(tr) => tr match {
        case SingleValueResult(r: Iterable[_]) =>
          (renderedSource(DataSerializer.source(() => r.iterator), resFil, isCollection.getOrElse(true), ct),
            null, contentType, None)
        case SingleValueResult(r) =>
          (renderedSource(DataSerializer.source(() => Iterator(r)), resFil, isCollection.getOrElse(false), ct),
            null, contentType, None)
        case r =>
          (renderedSource(TresqlResultSerializer.source(() => r), resFil, isCollection.getOrElse(true), ct),
            null, contentType, None)
      }
      case TresqlSingleRowResult(row) =>
        (renderedSource(TresqlResultSerializer.rowSource(() => row), resFil, isCollection.getOrElse(false), ct),
          null, contentType, None)
      case fileResult: FileResult =>
        fileHttpEntity(fileResult)
          .map(e => (e.dataBytes, fileResult.fileInfo.filename, e.contentType, e.contentLengthOption))
          .getOrElse(sys.error(s"File not found: ${fileResult.fileInfo}"))
      case resourceResult: ResourceResult =>
        ResourceFile(classOf[AppQuerease].getResource(resourceResult.resource)).map { rf =>
          ( StreamConverters.fromInputStream(() => rf.url.openStream()),
            null,
            if (contentType == null) resourceResult.contentType else contentType,
            Some(rf.length)
          )
        }.getOrElse(sys.error(s"Resource not found: ${resourceResult.resource}"))
      case templateResult: TemplateResult => templateResult match {
        case StringTemplateResult(content) =>
          val data = ByteString(content)
          (Source.single(data), null, ContentTypes.`text/plain(UTF-8)`, Option(data.size))
        case FileTemplateResult(fn, contentType, content) =>
          val ct = ContentType.parse(contentType).toOption
            .getOrElse(sys.error(s"Error parsing template result content type: $contentType"))
          (Source.single(ByteString(content)), fn, ct, Option(content.size))
      }
      case HttpResult(res) =>
        ( res.entity.dataBytes,
          res.header[`Content-Disposition`]
            .filter(_.dispositionType == attachment)
            .flatMap(_.params.get("filename"))
            .orNull,
          res.entity.contentType,
          res.entity.contentLengthOption
        )
      case CompatibleResult(r, fil, isCollection) => renderedResult(r, ct, fil, Option(isCollection))
      case NoResult => encodePrimitive("")
      case x => sys.error(s"Currently unable to create rendered source from result: $x")
    }
  }

  protected lazy val doHttpRequest: HttpRequest => Future[HttpResponse] = {
    val httpClient =
      new org.wabase.client.RestClient {}
    httpClient.doRequest _
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
                              unwrapSingleValue: Boolean)(implicit fs: FileStreamer, as: ActorSystem): Future[_] = {
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
      case AnyResult(ar) => ar match {
        case v: Iterator[_] => v.toList
        case v => v // TODO may be need to convert java collections to scala?
      }
      case StatusResult(code, value) => Map("code" -> code, "value" ->
        (value match {
          case StringStatus(v) => v
          case RedirectStatus(value) => tresqlUri.uri(value).toString()
        }))
      case fi: FileInfoResult => fi.fileInfo.toMap
      case fr: FileResult => fileHttpEntity(fr).map(objFromHttpEntity(_, null, false))
        .getOrElse(sys.error(s"File not found: ${fr.fileInfo}"))
      case rs: ResourceResult =>
        ResourceFile(classOf[AppQuerease].getResource(rs.resource))
          .map(rf => StreamConverters.fromInputStream(() => rf.url.openStream()).runReduce(_ ++ _))
          .orNull
      case tr: TemplateResult => tr match {
        case StringTemplateResult(content) => content
        case FileTemplateResult(_, _, content) => content
      }
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
      case r: RequestPartResult => requestPartsToMap(r)
      case x => sys.error(s"${x.getClass.getName} not expected here!")
    }) match {
      case f: Future[_] => f
      case x => Future.successful(x)
    }
  }

  private def comp_res(res: DataResult, conformTo: Action.OpResultType) =
    CompatibleResult(
      res,
      if (conformTo.viewName != null)
        new ResultRenderer.ViewFieldFilter(conformTo.viewName, nameToViewDef)
      else
        ResultRenderer.NoFilter,
      conformTo.isCollection
    )

  private def notFound = StatusResult(StatusCodes.NotFound.intValue, StringStatus("not found"))
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

  override protected def throwUnsupportedConversion(
      value: Any, targetType: Manifest[_], fieldName: String, cause: Throwable = null): Unit = {
    throw new UnprocessableEntityException(
      "Illegal value or unsupported type conversion from %s to %s - failed to populate %s", cause,
       value.getClass.getName, targetType.toString, s"${getClass.getName}.$fieldName")
  }

  protected def convertJsValueTypeForField(
    fieldName: String,
    emptyStringsToNull: Boolean
  )(implicit qe: QuereaseMetadata with ValueConverter): PartialFunction[JsValue, Any] = {
    import scala.language.existentials
    val (typ, parType) = setters(fieldName) match {
      case DtoSetter(_, met, mOpt, mSeq, mDto, mOth) =>
        (if (mSeq != null) mSeq else if (mDto != null) mDto else mOth,
         if (mSeq == null) null else if (mDto != null) mDto else mOth,
        )
    }
    val parseFunc: PartialFunction[JsValue, Any] = {
      case v: JsString =>
        try qe.convertToType(v.value, typ.runtimeClass) catch {
          case util.control.NonFatal(ex) =>
            throwUnsupportedConversion(v, typ, fieldName, ex)
        }
      case v: JsNumber =>
        try qe.convertToType(v.value, typ.runtimeClass) catch {
          case util.control.NonFatal(ex) =>
            throwUnsupportedConversion(v, typ, fieldName, ex)
        }
      case v: JsBoolean =>
        try qe.convertToType(v.value, typ.runtimeClass) catch {
          case util.control.NonFatal(ex) =>
            throwUnsupportedConversion(v, typ, fieldName, ex)
        }
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
          classOf[Seq[_]].isAssignableFrom(c) && (isList || isVector) )  {
          val parTypeClass = parType.runtimeClass
          try { val res =
            v.elements.map {
              case v: JsString  => qe.convertToType(v.value, parTypeClass)
              case v: JsNumber  => qe.convertToType(v.value, parTypeClass)
              case v: JsBoolean => qe.convertToType(v.value, parTypeClass)
              case    JsNull    => null
              case x            => throwUnsupportedConversion(x, parType, fieldName)
            }
            if(isList) res.toList else res
          } catch {
            case util.control.NonFatal(ex) =>
              throwUnsupportedConversion(v, typ, fieldName, ex)
          }
        } else throwUnsupportedConversion(v, typ, fieldName)
      case JsNull => null
    }
    parseFunc
  }

  //creating dto from JsObject
  def fill(js: JsObject)(implicit qe: QuereaseMetadata with ValueConverter): this.type = fill(js, emptyStringsToNull = true)(qe)
  def fill(js: JsObject, emptyStringsToNull: Boolean)(implicit qe: QuereaseMetadata with ValueConverter): this.type = {
    js.fields foreach { case (name, value) =>
      setters.get(name).map { case s =>
        val converted = convertJsValueTypeForField(name, emptyStringsToNull)(qe)(value).asInstanceOf[Object]
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

object AppQuerease {
  def requestPartsToMap(parts: RequestPartResult)(
    implicit fs: FileStreamer, as: ActorSystem): Future[Map[String, Any]] = {
    implicit val ec = as.dispatcher
    parts.result.mapAsync(1) {
      case p if p.filename != null =>
        p.data.runWith(fs.fileSink(p.filename, p.contentType.toString))
          .map(_.toMap)
          .map(m => if (p.name == null) m else Map(p.name -> m))
      case p => p.data.runFold(ByteString.empty)(_ ++ _).map(v => Map(p.name -> v.utf8String))
    }.runFold(Map[String, Any]())(_ ++ _)
  }
}
