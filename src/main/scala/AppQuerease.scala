package org.wabase

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.HttpHeader.ParsingResult.{Error, Ok}
import org.apache.pekko.http.scaladsl.model.headers.ContentDispositionTypes.attachment
import org.apache.pekko.http.scaladsl.model.headers.{Cookie, HttpCookie, HttpCookiePair, `Content-Disposition`, `Set-Cookie`}
import org.apache.pekko.http.scaladsl.model.{ContentType, ContentTypes, ErrorInfo, HttpCharsets, HttpEntity, HttpHeader, HttpMethods, HttpRequest, HttpResponse, MediaTypes, Multipart, StatusCodes, UniversalEntity}
import org.apache.pekko.http.scaladsl.server.directives.ContentTypeResolver
import org.apache.pekko.http.scaladsl.server.directives.FileAndResourceDirectives.ResourceFile
import org.apache.pekko.stream.scaladsl.{Source, StreamConverters}
import org.apache.pekko.util.{ByteString, Timeout}
import com.typesafe.scalalogging.Logger
import org.tresql._
import org.mojoz.querease._
import org.mojoz.querease.SaveMethod
import org.mojoz.metadata.ViewDef
import org.wabase.AppFileStreamer.FileInfo
import org.wabase.AppMetadata.Action.{VariableTransform, VariableTransforms}
import org.wabase.AppMetadata.DbAccessKey
import org.wabase.AppQuerease.{InjectionParametersContext, InjectionParametersProvider, Scope, configValueAsScala, httpResponseToMap, listOfStringTuples}
import org.wabase.client.HttpClient

import java.lang.reflect.Parameter
import java.sql.Connection
import scala.annotation.tailrec
import scala.collection.immutable.Seq
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Try}
import scala.util.control.NonFatal

trait QuereaseProvider {
  final implicit lazy val qe: AppQuerease = initQuerease
  final implicit lazy val qio: AppQuereaseIo[Dto] = initQuereaseIo
  /** Override this method in subclass to initialize {{{qe}}} */
  protected def initQuerease: AppQuerease = DefaultAppQuerease
  protected def initQuereaseIo: AppQuereaseIo[Dto] = new AppQuereaseIo[Dto](qe)
}

class QuereaseActionException(message: String, cause: Throwable) extends Exception(message, cause)

case class QuereaseResources()(implicit
  val resourcesFactory: ResourcesFactory,
  val ec: ExecutionContext,
  val as: ActorSystem,
  val httpReq: HttpRequest,
  val qio: AppQuereaseIo[Dto],
  val fileStreamers: WabaseFileStreamers,
  val httpClients: WabaseHttpClients,
  val parametersProvider: InjectionParametersProvider,
  val logger: Logger,
)

case class ResourcesFactory(
  initResources: (PoolName, Seq[DbAccessKey]) => Resources,
  closeResources: (Resources, Boolean, Option[Throwable]) => Unit,
)(implicit val resources: Resources)
{
  def focus(name: String, defaultName: String): ResourcesFactory =
    copy()(resources = AppQuerease.focusResource(name, defaultName)(resources))
}

sealed trait ResponseValue
case class RedirectValue(value: TresqlUri.Uri) extends ResponseValue
case class ResultValue(value: QuereaseResult) extends ResponseValue
sealed trait QuereaseResult
sealed trait QuereaseCloseableResult extends QuereaseResult
/** Data result can conform to view structure */
case class TresqlResult(result: Result[RowLike]) extends QuereaseCloseableResult
case class TresqlSingleRowResult(row: RowLike) extends QuereaseCloseableResult {
  /** map, close row (i.e. result), return mapped */
  def map[T](f: RowLike => T): T = try f(row) finally row.close()
}
case class MapResult(result: Map[String, Any]) extends QuereaseResult
case class IteratorResult(result: Iterator[Any]) extends QuereaseCloseableResult
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
case class ResponseResult(code: Int, value: ResponseValue, headers: List[HttpHeader] = Nil, user: WabaseUser = null) extends QuereaseResult
case class ResourceResult(resource: String, contentType: ContentType, httpReq: HttpRequest) extends QuereaseResult
case class FileInfoResult(fileInfo: FileInfo) extends QuereaseResult
case class FileResult(fileInfo: FileInfo, fileStreamer: FileStreamer) extends QuereaseResult
case class RequestPartResult(result: Source[RequestPart, Any], fs: FileStreamer) extends QuereaseResult
case class RequestPart(name: String, filename: String, entity: HttpEntity)
sealed trait TemplateResult extends QuereaseResult
  { def contentString: String }
case class StringTemplateResult(content: String) extends TemplateResult
  { override def contentString: String = content }
case class FileTemplateResult(filename: String, contentType: String, content: Array[Byte]) extends TemplateResult
  { override def contentString: String = new String(content, "UTF-8") }
case class HttpEntityResult(entity: HttpEntity, decoder: RequestDecoders.RequestDecoder) extends QuereaseResult
case class HttpResult(response: HttpResponse, isProxy: Boolean = false) extends QuereaseResult
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
case class CompatibleResult(result: QuereaseResult,
                            resultFilter: ResultRenderer.ResultFilter,
                            isCollection: Boolean = false) extends QuereaseCloseableResult
case class DbResult(result: QuereaseResult, cleanup: Option[Throwable] => Unit) extends QuereaseResult
case class ConfResult(param: String, result: Any) extends QuereaseResult

class AppQuereaseIo[DTO <: Dto](val qe: QuereaseMetadata with QuereaseResolvers with ValueTransformer)
  extends ScalaDtoQuereaseIo[DTO](qe) with JsonConverter[DTO] {
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

  lazy val resultRenderersFactory = getObjectOrNewInstance[ResultRenderersFactory](
    config, "result-renderers.factory-class", "result renderers factory"
  )
  lazy val resultRenderers: ResultRenderers = resultRenderersFactory.createResultRenderers

  lazy val requestDecodersFactory = getObjectOrNewInstance[RequestDecodersFactory](
    config, "request-decoders.factory-class", "request decoders factory"
  )
  lazy val requestDecoders: RequestDecoders.Decoders = requestDecodersFactory.createRequestDecoders(this)

  val tresqlUri: TresqlUri = new TresqlUri()
  lazy val cborOrJsonDecoder = new CborOrJsonDecoder(typeDefs, nameToViewDef)

  protected val maxStackDepth: Int = config.getInt("wabase.max-stack-depth")
  lazy val templateEngine: WabaseTemplate = createTemplateEngine
  protected def createTemplateEngine: WabaseTemplate =
    getObjectOrNewInstance[WabaseTemplate](config, "app.template.engine", "template engine")
  lazy val emailSender: WabaseEmail = createEmailSender
  protected def createEmailSender: WabaseEmail =
    getObjectOrNewInstance[WabaseEmail](config, "app.email.sender", "email sender")
  protected def evaluatorConn(): Connection = {
    val evaluatorPoolName = config.getString("app.wabase.evaluator.pool")
    ConnectionPools(PoolName(evaluatorPoolName)).getConnection
  }
  protected def evaluatorResources(defaultResources: Resources): Resources = {
    val evaluatorPoolName = config.getString("app.wabase.evaluator.pool")
    TresqlResourcesConf.confs.get(evaluatorPoolName).map(_.dialect).orNull match {
      case null    => defaultResources
      case dialect => defaultResources.withDialect(dialect)
    }
  }
  private def useResourcesConnOrEvaluator[T](resources: Resources, f: Resources => T): T =
    if (resources.conn != null)
      f(resources)
    else {
      val r = evaluatorResources(resources)
      val c = evaluatorConn()     // do fallback to evaluator connection
      try f(r.withConn(c)) finally c.close()
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
    def run(ec: ExecutionContext, as: ActorSystem): Future[A]
    def map[B](f: A => B)(implicit ec: ExecutionContext, as: ActorSystem): QuereaseAction[B] =
      (_: ExecutionContext, _: ActorSystem) => QuereaseAction.this.run(ec, as).map(f)(ec)
    def flatMap[B](f: A => QuereaseAction[B])(implicit ec: ExecutionContext, as: ActorSystem): QuereaseAction[B] =
      (_: ExecutionContext, _: ActorSystem) => QuereaseAction.this.run(ec, as).flatMap(f(_).run(ec, as))(ec)
    def andThen[U](pf: PartialFunction[Try[A], U])(implicit ec: ExecutionContext, as: ActorSystem): QuereaseAction[A] =
      (_: ExecutionContext, _: ActorSystem) => QuereaseAction.this.run(ec, as).andThen(pf)(ec)
    def recover[U >: A](pf: PartialFunction[Throwable, U])(implicit ec: ExecutionContext, as: ActorSystem): QuereaseAction[U] =
      (_: ExecutionContext, _: ActorSystem) => QuereaseAction.this.run(ec, as).recover(pf)(ec)
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
      httpReq: HttpRequest,
      qio: AppQuereaseIo[Dto],
      fileStreamers: WabaseFileStreamers,
      httpClients: WabaseHttpClients,
      parameterProvider: InjectionParametersProvider,
      logger: Logger,
    ): QuereaseAction[QuereaseResult] = {
        new QuereaseAction[QuereaseResult] {
          override def run(ec: ExecutionContext, as: ActorSystem) = {
            implicit val resFac =
              if (isExplicitDb(objName, actionName)) resourcesFactory
              else {
                val (poolName, extraDbs) = dbResourceNames(objName, actionName)
                resourcesFactory.copy()(resources = resourcesFactory.initResources(poolName, extraDbs))
              }
            implicit val qr = new QuereaseResources()(resFac, ec, as, httpReq, qio, fileStreamers, httpClients,
              parameterProvider, logger)
            import resFac._
            def processResult(res: QuereaseResult, cleanup: Option[Throwable] => Unit): QuereaseResult = res match {
              case sr@ResponseResult(_, ResultValue(result), _, _) =>
                sr.copy(value = ResultValue(processResult(result, cleanup)))
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
              }(ec).andThen {
                case Failure(NonFatal(exception)) => closeResources(resources, true, Option(exception))
              }(ec)
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
    def stackStr: String = (name :: contextStack.map(_.name)).mkString("[", ",", "]")
  }

  private[wabase] def quereaseActionOpt(objectName: String, actionName: String) = {
    val vd = viewDef(objectName)
    vd.actions.get(actionName)
      .orElse(actionName match {
        case Action.Insert | Action.Update | Action.Upsert =>
          vd.actions.get(Action.Save)
        case _ => None
      })
  }

  private def isExplicitDb(objectName: String, actionName: String) = {
    viewDef(objectName).explicitDb
  }

  def dbResourceNames(objectName: String, actionName: String): (PoolName, Seq[DbAccessKey]) = {
    val vdo = viewDefOption(objectName)
    val poolName = vdo.flatMap(v => Option(v.db)).map(PoolName) getOrElse PoolName(defaultCpName)
    val extraDbs = vdo.map(_.actionToDbAccessKeys(actionName).filter(_.db != null).toList).getOrElse(Nil)
    (poolName, extraDbs)
  }

  def saveRequestParts(result: RequestPartResult)(implicit as: ActorSystem): Future[Map[String, Any]] =
    AppQuerease.saveRequestParts(result)

  def exceptionHandler(e: Throwable, src: String, context: ActionContext): Throwable = e match {
    case e: QuereaseActionException => e
    case e => new QuereaseActionException(s"Action: ${context.viewName}.${context.actionName}, step - '$src'", e)
  }

  def doAction(
    view: String,
    actionName: String,
    data: Map[String, Any],
    env: Map[String, Any],
    fieldFilter: FieldFilter = null,
  )(implicit qr: QuereaseResources): Future[QuereaseResult] = {
    do_action(view, actionName, Scope(data), env, fieldFilter)
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
  private def logContext(ctx: ActionContext, env: Map[String, Any], qr: QuereaseResources) = {
    val res = qr.resourcesFactory.resources
    qr.logger.debug(s"Doing action '${ctx.name}'")
    qr.logger.debug(s"Ctx stack: [${ctx.contextStack.map(_.name).mkString(", ")}]")
    qr.logger.debug(s"Database connections: [${(("[main]", res.conn) ::
      res.extraResources.map{case (n, r) => n -> r.conn}.toList).mkString(", ")}]")
    qr.logger.debug(s"Env: {${loggable(res, env)}}")
  }

  private def do_action(
    view: String,
    actionName: String,
    scope: Scope,
    env: Map[String, Any],
    fieldFilter: FieldFilter = null,
    contextStack: List[ActionContext] = Nil,
  )(implicit qr: QuereaseResources): Future[QuereaseResult] = {
    val ctx = ActionContext(view, actionName, env, viewDefOption(view), fieldFilter, null, contextStack)
    logContext(ctx, env, qr)
    val steps =
      quereaseActionOpt(view, actionName)
        .map(_.steps)
        .getOrElse(List(Action.Return(None, Nil, Action.ViewCall(actionName, view, null)) -> ""))
    doSteps(steps, ctx, Future.successful(scope))
  }

  def doSteps(
    steps: List[(Action.Step, String)],
    context: ActionContext,
    curData: Future[Scope],
  )(implicit qr: QuereaseResources): Future[QuereaseResult] = {
    if (context.contextStack.size > maxStackDepth)
      throw new IllegalStateException(s"Action call stack depth exceeds $maxStackDepth (consider configuration parameter wabase.max-stack-depth). Stack - ${context.stackStr}")
    import Action._
    import qr._
    def updateCurRes(cr: Map[String, Any], key: Option[String], resF: Future[_]) = {
      def upd_key(d: Map[String, _], k: String, v: Any) = {
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
      def upd(res: Any): Map[String, _] = res match {
        case kr: KeyResult => upd(kr.ir)
        case ir: IdResult =>
          // id result always updates current result
          key
            .map(k => upd_key(cr, k, ir.id))
            .getOrElse(cr ++ ir.toMap)
        case NoResult => key.map(upd_key(cr, _, null)).getOrElse(cr)
        case r => key.map(k => upd_key(cr, k, r)).getOrElse(cr)
      }
      resF map upd
    }
    def scopeBindVars(scope: Scope) = scope.toBindeableMap(context.env)
    def doStep(step: Step, stepDataF: Future[Scope], src: String): Future[QuereaseResult] = {
      import resourcesFactory._
      stepDataF flatMap { stepScope =>
        val stepData = stepScope.data
        def doActionStep(vts: List[VariableTransform], op: Action.Op) =
          doActionOp(op, if (vts.isEmpty) stepScope
            else stepScope.copy(data = doVarsTransforms(vts, stepData, stepData).result), context)
        qr.logger.debug(s"Doing action '${context.name}' step '$src', $step.")
        qr.logger.debug(s"Step data: {${loggable(resourcesFactory.resources, scopeBindVars(stepScope))}}")
        step match {
          case Evaluation(_, vts, op) => doActionStep(vts, op)
          case SetEnv(_, vts, op, _) => doActionStep(vts, op)
          case Return(_, vts, op) => doActionStep(vts, op)
          case RemoveVar(name) => Future.successful(stepData - name.get) map MapResult
          case Validations(_, validations, db) =>
            context.view.map { vd =>
              Future(doValidationStep(validations, db, scopeBindVars(stepScope), vd))
                .map(_ => MapResult(stepData))
            }.getOrElse(Future.failed(
              new RuntimeException(s"Validation cannot be performed without view in context -" +
                s"(${context.name})")))
        }
      } transform(identity, exceptionHandler(_, src, context))
    }

    steps match {
      case Nil => curData.map(sc => MapResult(sc.data))
      case (s, src) :: Nil =>
        doStep(s, curData, src) flatMap {
          case ir: IdResult =>
            curData.map(sc => keyResult(ir, context.viewName, scopeBindVars(sc)))
          case kr: KeyResult =>
            s match {
              // FIXME enable simple redirect from If
              case Evaluation(_, _, RedirectToKey(_)) => Future.successful(kr)
              case _ => curData.map(sc => keyResult(kr.ir, context.viewName, scopeBindVars(sc))) // FIXME apply kr.toMap
            }
          case TresqlResult(r: DMLResult) if context.stepName == null && context.contextStack.isEmpty =>
            r match {
              case _: InsertResult | _: UpdateResult =>
                r.id.map { id =>
                  val idName = viewNameToIdName.getOrElse(context.viewName, null)
                  curData.map(sc => keyResult(IdResult(id, idName), context.viewName, scopeBindVars(sc)))
                }.getOrElse {
                  viewDefOption(context.viewName)
                    .filter(hasExplicitKey)
                    .map(_ => curData.map(sc => keyResult(IdResult(null, null), context.viewName, scopeBindVars(sc))))
                    .getOrElse(Future.successful(NoResult))
                }
              case _: DeleteResult =>
                Future.successful(QuereaseDeleteResult(r.count.getOrElse(0)))
            }
          case x => Future.successful(x)
        } flatMap { res => s match {
          case Evaluation(n@Some(_), _, _) => curData
            .flatMap(sc => updateCurRes(sc.data, n, dataForNextStep(res, context, true)))
            .map(MapResult)
          case _ => Future.successful(res)
        }}
      case (s, src) :: tail =>
        doStep(s, curData, src) flatMap { stepRes =>
          s match {
            case e: Evaluation =>
              val ns = for {
                sc <- curData
                cr <- updateCurRes(
                  sc.data, e.name,
                  if (e.name.isEmpty) consumeResult(stepRes)
                  else dataForNextStep(stepRes, context, true)
                )
              } yield sc.copy(data = cr)
              doSteps(tail, context, ns)
            case se: SetEnv =>
              val ns = for {
                sc <- curData
                cr <- dataForNextStep(stepRes, context, true)
              } yield cr match {
                case m: Map[String, Any]@unchecked =>
                  sc.copy(data = if (se.add) sc.data ++ m else m)
                case x =>
                  //in the case of primitive value return step must have name
                  se.name.map(n => sc.copy(data = Map(n -> x))).getOrElse(sc)
              }
              doSteps(tail, context, ns)
            case _: RemoveVar =>
              val ns = for {
                sc <- curData
                cr <- dataForNextStep(stepRes, context, true)
              } yield cr match {
                case m: Map[String, Any]@unchecked => sc.copy(data = m)
                case x => sys.error(s"Remove var step cannot produce anyting but Map, instead got $x")
              }
              doSteps(tail, context, ns)
            case _: Return => Future.successful(stepRes)  // stop execution and return
            case _ => doSteps(tail, context, curData)     // validation continue execution
          }
        }
    }
  }

  protected def doValidationStep(validations: Seq[String],
                                 dbkey: Option[DbAccessKey],
                                 params: Map[String, Any],
                                 view: ViewDef)(implicit res: Resources): Unit = {
    validationsQueryString(view, validations) foreach { vs =>
      useResourcesConnOrEvaluator(res, r =>
        Query(dbkey.flatMap(k => Option(k.db)).map("|" + _ + ":").mkString("", "", vs), toSaveableMap(params, view))(r))
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
    scope: Scope,
    context: ActionContext,
  )(implicit
    resources: Resources,
  ): QuereaseResult = {
    val result = useResourcesConnOrEvaluator(resources, res =>
     Query(tresql.tresql)(res.withParams(scope.toBindeableMap(context.env))) match {
      case sel: SelectResult[_] if resources.conn == null =>
        // convert select result to list or single value so evaluator conn can be closed
        val r = sel.toListOfMaps
        if (r.size == 1 && r.head.size == 1) TresqlResult(SingleValueResult(r.head.head._2))
        else IteratorResult(r.iterator)
      case arraySel: DynamicArraySelectResult =>
        if (resources.conn == null) IteratorResult(arraySel.elIterator.toSeq.iterator)
        else IteratorResult(arraySel.elIterator)
      case r => TresqlResult(r)
     }
    )
    tresql.conformTo.map(comp_res(result, _)).getOrElse(result)
  }

  protected def doViewCall(
    op: Action.ViewCall,
    scope: Scope,
    context: ActionContext,
  )(implicit qr: QuereaseResources): Future[QuereaseResult] = {
    import Action._
    import qr._, resourcesFactory._, context.env
    implicit val fs: FileStreamer = fileStreamers.fs(null)
    val v = viewDef(
      if (op.view == "this") context.view.map(_.name) getOrElse op.view
      else                op.view
    )
    val viewName = v.name
    val callDataF =
      if (op.data == null) Future.successful(scope)
      else {
        @tailrec
        def unwrapSingleRow(d: Any): Map[String, _] = d match {
          case r: Map[String@unchecked, _] => r ++ env
          case r: Seq[_] if r.size == 1 => unwrapSingleRow(r.head)
          case NoResult => env
          case x => sys.error(s"Invalid view op result. Currently unable to create Map[String, _] from $x")
        }
        doActionOp(op.data, scope, context).flatMap(dataForNextStep(_, context, false))
          .map(unwrapSingleRow)
          .map(Scope(_))
      }
    callDataF.flatMap { callScope =>
      val callData = callScope.toBindeableMap(env)
      // execute querease call if context view name and method corresponds to this view name and method
      def isThisMethod(ctxMethod: String, thisMethod: String) = {
        ctxMethod ==
          thisMethod ||
          (Set(Action.Insert, Action.Update, Action.Upsert).contains(ctxMethod) && Action.Save == thisMethod) ||
          !v.actions.contains(thisMethod)
      }

      if (context.view.exists(_.name == viewName) && isThisMethod(context.actionName, op.method)) {
        lazy val idName = viewNameToIdName.getOrElse(viewName, null)

        def int(name: String) = tryOp(callData.get(name).map {
          case x: Int => x
          case x: Number => x.intValue
          case x: String => x.toInt
          case x => x.toString.toInt
        }, callData)

        def string(name: String) = callData.get(name) map String.valueOf
        def castedResult(qr: QuereaseResult): QuereaseResult = {
          def cr(isColl: Boolean) =
            op.conformTo.orElse(Option(Action.ViewResultType(viewName, isColl))).map(comp_res(qr, _)).get
          qr match {
            case r: TresqlSingleRowResult => cr(false)
            case r: TresqlResult => cr(true)
            case r => r
          }
        }
        val res =
          (op.method match {
            case Get =>
              val keyValues = getKeyValues(viewName, callData)
              val keyColNames = viewNameToKeyColNames(viewName)
              val fieldFilter: FieldFilter = context.fieldFilter
              get(v, keyValues, keyColNames, null, callData, fieldFilter)
                .map(TresqlSingleRowResult) getOrElse NoResult
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
            case JobCall =>
              quereaseActionOpt(viewName, Job)
                .map(a => doSteps(a.steps, context, callDataF))
                .getOrElse(NoResult)
            case x =>
              sys.error(s"Unknown view action $x")
          }) match {
            case f: Future[QuereaseResult@unchecked] => f // job call, do not cast, may be casted at the end
            case r: QuereaseResult => Future.successful(castedResult(r))
          }
        res
      } else {
        val nqr = qr.copy()(resourcesFactory = resourcesFactory
          .focus(if (v.db != null) v.db else defaultCpName, defaultCpName),
          ec, as, httpReq, qio, fileStreamers, httpClients, parametersProvider, qr.logger)
        do_action(viewName, op.method, callScope, env, context.fieldFilter, context :: context.contextStack)(nqr)
      }
    }.map(result => op.conformTo.map(comp_res(result, _)).getOrElse(result))
  }

  protected def doInvocation(
    op: Action.Invocation,
    scope: Scope,
    context: ActionContext,
  )(implicit qr: QuereaseResources): Future[QuereaseResult] = {
    import op._, qr._, context.env
    val invocationData = scope.toBindeableMap(env)
    def invokeFunction(className: String, function: String, pf: InvocationParameterFun): Any = {
      this.invokeFunction(className, function, invocationData, pf,
        InjectionParametersContext(httpReq, invocationData), qr)
    }

    def wrongRes(x: Any) =
      sys.error(s"Unrecognized result type: ${x.getClass}, value: $x from function $className.$function. You " +
        s"may want to prefix invocation with 'as any'")

    def comp_q_result(r: Any) = {
      val allowAny = op.conformTo.collectFirst{ case Action.ViewResultType(null, _) => }.isDefined
      def qresult(r: Any): QuereaseResult = r match {
        case null | () => NoResult // reflection call on function with Unit (void) return type returns null
        case r: Result[_] => TresqlResult(r)
        case r: RowLike => TresqlSingleRowResult(r)
        case l: Long => LongResult(l)
        case s: String => StringResult(s)
        case n: java.lang.Number => NumberResult(n)
        case b: Boolean => AnyResult(b)
        case d: Dto => MapResult(d.toMap(this))
        case o: Option[Dto]@unchecked => o.map(d => MapResult(d.toMap(this))).getOrElse(NoResult)
        case e: HttpEntity => HttpEntityResult(e, null)
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
        case c: CompatibleResult => conformTo match {
          case Action.ViewResultType(_, isColl) =>
            require(c.isCollection == isColl, s"Incompatible results $c != $conformTo")
            val c1 = comp_res(c.result, conformTo)
            c.copy(resultFilter = new ResultRenderer.IntersectionFilter(c1.resultFilter, c.resultFilter))
          case _ => c.copy(resultFilter = null)
        }
        case r => comp_res(r, conformTo)
      }
      val qr = qresult(r)
      conformTo.map(createCompatibleResult(qr, _)).getOrElse(qr)
    }

    val dtoParamFun = AppQuerease.dtoParameterFromMap(() => invocationData)(qio)
    (if (op.args.isEmpty) {
      invokeFunction(className, function, dtoParamFun)
    } else {
      Future.sequence(op.args.map(doActionOp(_, scope, context))).flatMap { opResults =>
        val valFuns = opResults.zipWithIndex.map { case (opRes, idx) =>
          def unwrappedVal(qres: QuereaseResult) = qres match {
            case TresqlResult(SingleValueResult(qr: QuereaseResult)) => qr // unwrap bind variable value
            case x => x
          }
          AppQuerease.explicitInvocationParameter(unwrappedVal(opRes), idx, function)(
            AppQuerease.this, dataForNextStep(_, context, unwrapSingleValue = false))
        }
        invokeFunction(className, function, valFuns.reduce(_ orElse _) orElse dtoParamFun) match {
          case f: Future[_] => f
          case x => Future.successful(x)
        }
      }
    }) match {
      case f: Future[_] => f map comp_q_result
      case x => Future.successful(comp_q_result(x))
    }
  }

  protected def doVarsTransforms(transforms: List[VariableTransform],
                                 seed: Map[String, Any],
                                 data: Map[String, Any]): MapResult = {
    def evalConcats(names: List[String]) = names.map(evalVar).reduce[Any] {
      case (x: Seq[_], y: Seq[_]) => x ++ y
      case (x: Seq[_], y) => x :+ y
      case (x, y: Seq[_]) => y.+:(x)
      case (x, y) => Seq(x) :+ y
    }
    def evalVar(name: String) =
      if (name == "_") data
      else Query(":" + name)(new Resources {}.withParams(data)) match {
        case SingleValueResult(r) => r
        case x => sys.error(s"Unexpected variable transformation result: $x, expected SingleValueResult")
      }
    def updRes(from: List[String], to: Option[String], curRes: Map[String, Any]) = {
      val res = evalConcats(from)
      to.map(name => curRes + (name -> res)).getOrElse(res match {
        case m: Map[String, _]@unchecked => curRes ++ m
        case x =>
          val vn = from.head
          curRes + (to.getOrElse(vn.substring(vn.lastIndexOf(".") + 1, vn.length)) -> x)
      })
    }
    val transRes = transforms.foldLeft(seed) ((res, vt) => updRes(vt.from.vars, vt.to, res))
    MapResult(transRes)
  }

  protected def doUnique(
    op: Action.Unique,
    scope: Scope,
    context: ActionContext,
  )(implicit qr: QuereaseResources): Future[QuereaseResult] = {
    import qr.ec
    def createGetResult(res: QuereaseResult): QuereaseResult = res match {
      case TresqlResult(r) if !r.isInstanceOf[DMLResult] =>
        if (op.opt) r.uniqueOption map TresqlSingleRowResult getOrElse NoResult
        else TresqlSingleRowResult(r.unique)
      case IteratorResult(r) =>
        try r.hasNext match {
          case true =>
            val v = r.next()
            if (r.hasNext) sys.error("More than one row for unique result") else v match {
              case m: Map[String@unchecked, _] => MapResult(m)
              case x => AnyResult(x)
            }
          case false => if (op.opt) NoResult else throw new NoSuchElementException(s"No rows in result")
        } finally r match {
          case c: AutoCloseable => c.close()
          case _ =>
        }
      case c: CompatibleResult => createGetResult(c.result) match {
        case r => c.copy(result = r)
      }
      case r => sys.error(s"unique opt can only process Iterator type, instead encountered: $r")
    }
    val r = doActionOp(op.innerOp, scope, context) map createGetResult
    op.conformTo.map(rf => r.map (r => comp_res(r, rf))).getOrElse(r)
  }

  protected def doResponse(
    op: Action.Response,
    scope: Scope,
    context: ActionContext,
  )(implicit qr: QuereaseResources): Future[QuereaseResult] = {
    import qr.ec, context.env
    val Action.Response(codeTresql, statusMode, hops, body) = op
    val code =  useResourcesConnOrEvaluator(qr.resourcesFactory.resources, r =>
      Query(codeTresql.tresql)(r.withParams(scope.toBindeableMap(env))) match {
        case SingleValueResult(n: Number) => n.intValue
        case r: Result[_] => r.unique[Int]
      }
    )
    val (ua, hs) = hops.partition(_.isInstanceOf[Action.SetUserAttributes])
    val user = if (ua.isEmpty) null else ua.foldLeft(WabaseUser(Map())) { (u, ua) =>
      WabaseUser(u.properties ++
        doSetUserAttributes(ua.asInstanceOf[Action.SetUserAttributes], scope, context).properties)
    }
    val headers = hs.foldLeft(scala.collection.mutable.ArrayBuffer[HttpHeader]())((r, hop) => hop match {
      case sc: Action.SetCookie => r ++= doSetCookie(sc, scope, context)
      case dc: Action.DeleteCookie => r ++= doDeleteCookie(dc, scope, context)
      case sh: Action.SetHttpHeaders => r ++= doSetHeaders(sh, scope, context)
      case x => sys.error(s"Cannot convert to http header: $x")
    }).toList
    Option(body).map { b =>
      import org.apache.pekko.http.scaladsl.model.StatusCode._
      if (code.isRedirection()) {
        b match {
          case Action.Tresql(tresql, _, _) =>
            val truri = useResourcesConnOrEvaluator(
              qr.resourcesFactory.resources,
              res => tresqlUri.tresqlUriValue(TresqlUri.Tresql(tresql))(Query, scope.toBindeableMap(env), res)
            )
            Future.successful(RedirectValue(truri))
          case _ => sys.error(s"Redirect operation body must be tresql returning single row, instead found: '$b'")
        }
      } else {
        doActionOp(b, scope, context)
          .flatMap(r =>
            if (statusMode) dataForNextStep(r, context, true)
              .map { // for status mode return string result so that content is marshalled as text/plain not json
                case null => StringResult(null)
                case s: String => StringResult(s)
                case x => AnyResult(x)
              }
            else Future.successful(r))
          .map(ResultValue(_))
      }
    }
      .map(_.map(ResponseResult(code, _, headers, user)))
      .getOrElse(Future.successful(ResponseResult(code, null, headers, user)))
  }

  protected def doSetOrDeleteCookie(
    op: Action.SetHttpHeadersOp,
    scope: Scope,
    context: ActionContext,
  )(implicit qr: QuereaseResources): List[HttpCookie] = {
    require(op.isInstanceOf[Action.SetCookie] || op.isInstanceOf[Action.DeleteCookie])
    useResourcesConnOrEvaluator(qr.resourcesFactory.resources, implicit res => {
      val params = scope.toBindeableMap(context.env)
      val cookieResult = Query(op.tresql.tresql, params)
      val (nameValueCols, otherCols) = cookieResult.columns.partition(c => c.name == "name" || c.name == "value")
      cookieResult.map { cookieRow =>
        otherCols.foldLeft(HttpCookie(
          name = cookieRow.s("name"),
          value = if (nameValueCols.exists(_.name == "value")) cookieRow.s("value") else "",
          expires = if (nameValueCols.exists(_.name == "value")) None else Some(org.apache.pekko.http.scaladsl.model.DateTime.MinValue),
        )) { (cookie, col) => col.name match {
          case "expires" =>
            val d = convertToType(cookieRow.t("expires"), ValueConverter.ClassOfString).toString.replace(" ", "T")
            cookie.withExpires(org.apache.pekko.http.scaladsl.model.DateTime.fromIsoDateTimeString(d).get)
          case "max_age" => cookie.withMaxAge(cookieRow.l("max_age"))
          case "domain" => cookie.withDomain(cookieRow.s("domain"))
          case "path" => cookie.withPath(cookieRow.s("path"))
          case "secure" => cookie.withSecure(cookieRow.boolean("secure"))
          case "http_only" => cookie.withHttpOnly(cookieRow.boolean("http_only"))
          case "extension" => cookie.withExtension(cookieRow.s("extension"))
        }}
      }.toList
    })
  }

  protected def doSetCookie(
    op: Action.SetCookie,
    scope: Scope,
    context: ActionContext,
  )(implicit qr: QuereaseResources): List[`Set-Cookie`] = {
    doSetOrDeleteCookie(op, scope, context).map(`Set-Cookie`(_))
  }

  protected def doDeleteCookie(
    op: Action.DeleteCookie,
    scope: Scope,
    context: ActionContext,
  )(implicit qr: QuereaseResources): List[`Set-Cookie`] = {
    doSetOrDeleteCookie(op, scope, context)
      .map(c => `Set-Cookie`(c.withExpires(org.apache.pekko.http.scaladsl.model.DateTime.MinValue)))
  }

  protected def doSetHeaders(
    op: Action.SetHttpHeaders,
    scope: Scope,
    context: ActionContext,
  )(implicit qr: QuereaseResources): List[HttpHeader] = {
    useResourcesConnOrEvaluator(qr.resourcesFactory.resources, implicit res => {
      val params = scope.toBindeableMap(context.env)
      Query
        .list[String, String](op.tresql.tresql, params)
        .map { case (n, v) => HttpHeader.parse(n, v) }
        .map {
          case Ok(header, _) => header
          case Error(e: ErrorInfo) => sys.error(e.formatPretty)
        }
    })
  }

  protected def doSetUserAttributes(
    op: Action.SetUserAttributes,
    scope: Scope,
    context: ActionContext,
  )(implicit qr: QuereaseResources): WabaseUser = {
    useResourcesConnOrEvaluator(qr.resourcesFactory.resources, implicit res => {
      val params = scope.toBindeableMap(context.env)
      WabaseUser(
        Query.list[String, Any](op.tresql.tresql, params)
          .map {
            case (x, r: DynamicArraySelectResult) => (x, r.elIterator.toSeq)
            case (x, r: Result[_]) => (x, r.toListOfMaps)
            case x => x
          }
          .toMap
      )
    })
  }

  protected def doIf(
    op: Action.If,
    scope: Scope,
    context: ActionContext,
  )(implicit qr: QuereaseResources): Future[QuereaseResult] = {
    import qr.ec
    doActionOp(op.cond, scope, context).map {
      case TresqlResult(tr) => tr.unique[Boolean]
      case r: TresqlSingleRowResult => r.map(_.boolean(0))
      case x => sys.error(s"Conditional operator must be whether TresqlResult or TresqlSingleRowResult or" +
        s"StringResult(true|false). Instead found: $x")
    }.flatMap { cond =>
      if (cond)
        doSteps(op.action.steps, context.copy(stepName = "if"), Future.successful(Scope(Map(), parent = scope)))
      else if (op.elseAct != null)
        doSteps(op.elseAct.steps, context.copy(stepName = "else"), Future.successful(Scope(Map(), parent = scope)))
      else Future.successful(NoResult)
    }
  }

  protected def doForeach(
    op: Action.Foreach,
    scope: Scope,
    context: ActionContext,
  )(implicit qr: QuereaseResources): Future[QuereaseResult] = {
    import qr.{ec, as}
    def source(res: Any, vd: ViewDef): Future[Source[Map[String, Any], _]] = {
      def maybeCompatible(map: Map[String, Any]) =
        Option(vd).map(toCompatibleMap(map, _)).getOrElse(map)
      res match {
        case s: Source[Map[String, _]@unchecked, _] => Future.successful(s)
        case i: Iterator[Map[String, _]@unchecked] =>
          Future.successful(Source.fromIterator(() => i map maybeCompatible))
        case s: Seq[Map[String, _]@unchecked] => source(s.iterator, vd)
        case m: Map[String@unchecked, _] => source(Seq(m).iterator, vd)
        case TresqlResult(tr) => tr match {
          case SingleValueResult(sr) => source(sr, vd)
          case r: Result[_] => source(r.map(_.toMap), vd)
        }
        case r: TresqlSingleRowResult => source(r.map(_.toMap), vd)
        case HttpEntityResult(ent, dec) =>
          decodeHttpEntity(ent, null, true, dec)(qr.as).flatMap(source(_, vd))(qr.ec)
        case fr: FileResult => source(HttpEntityResult(fileHttpEntity(fr)
          .getOrElse(sys.error(s"Cannot find file data: ${fr.fileInfo}")), null), vd)
        case HttpResult(resp, _) => source(HttpEntityResult(resp.entity, null), vd)
        case RequestPartResult(parts, fs) =>
          Future.successful(parts.mapAsync(1)(AppQuerease.saveRequestPart(_, fs)))
        case IteratorResult(it: Iterator[Map[String, _]@unchecked]) => source(it, vd)
        case CompatibleResult(r, rf, _) => source(r, Option(rf).flatMap(f => viewDefOption(f.name)).orNull)
        case x => sys.error(s"Not iterable result for foreach operation: $x")
      }
    }
    @volatile var idx = 0
    doActionOp(op.initOp, scope, context).flatMap(source(_, null))
      .map ( _.mapAsync(1) { itData => // paralellism is 1 so that idx is incremented correctly
        val itScope = Scope(itData, Map("__idx" -> idx), parent = scope, transparent = false)
        idx += 1
        doSteps(op.action.steps, context.copy(stepName = "foreach"), Future.successful(itScope))
          .flatMap(dataForNextStep(_, context, unwrapSingleValue = true))
      })
      .flatMap { src =>
        if (op.foldOp == null) Future.successful(IteratorResult(RequestDecoders.sourceToIterator(src)))
        else src.runFold(Future.successful(scope(op.foldOp.resVar))) { (resF, el) =>
          for {
            res <- resF
            foldOpRes <- doActionOp(op.foldOp.op, Scope(Map(op.foldOp.resVar -> res, op.foldOp.elVar -> el)), context)
            new_res <- dataForNextStep(foldOpRes, context, unwrapSingleValue = true)
          } yield new_res
        }.flatten.map(AnyResult)
      }
  }

  protected def doResource(
    op: Action.Resource,
    scope: Scope,
    context: ActionContext,
  )(implicit
    res: Resources,
    httpReq: HttpRequest,
  ): Future[ResourceResult] = {
    import context.env
    val resource = useResourcesConnOrEvaluator(res, r =>
      Query(op.nameTresql.tresql)(r.withParams(scope.toBindeableMap(env))).unique[String])
    val ct = Option(op.contentTypeTresql)
      .map { ctt =>
        val ct = useResourcesConnOrEvaluator(res, r =>
          Query(ctt.tresql)(r.withParams(scope.toBindeableMap(env))).unique[String])
        ContentType.parse(ct)
          .toOption
          .getOrElse(sys.error(s"Invalid content type: $ct"))
      }
      .getOrElse {
        ContentTypeResolver.withDefaultCharset(HttpCharsets.`UTF-8`)(resource)
      }
    Future.successful(ResourceResult(resource, ct, httpReq))
  }

  protected def doFile(
    op: Action.File,
    scope: Scope,
    context: ActionContext,
  )(implicit
    res: Resources,
    ec: ExecutionContext,
    fss: WabaseFileStreamers): Future[QuereaseResult] = {
    import context.env
    val fs = fss.fs(op.fileStreamerName)
    val (id, sha) = useResourcesConnOrEvaluator(implicitly[Resources],
      r => Query(op.idShaTresql.tresql)(r.withParams(scope.toBindeableMap(env))).unique[Long, String])
    val r = FileResult(fs.getFileInfo(id, sha).map(_.file_info).orNull, fs)
    Future.successful { op.conformTo.map(comp_res(r, _)).getOrElse(r) }
  }

  protected def doToFile(
    op: Action.ToFile,
    scope: Scope,
    context: ActionContext,
  )(implicit qr: QuereaseResources): Future[FileInfoResult] = {
    import org.apache.pekko.http.scaladsl.model.{MediaTypes, ContentType}
    import qr._, resourcesFactory._, context.env
    val bindVars = scope.toBindeableMap(env)
    def getVal(tr: Action.Tresql) = useResourcesConnOrEvaluator(resources,
      res => Query(tr.tresql)(res.withParams(bindVars)).unique[String])
    val fn = if (op.nameTresql != null) getVal(op.nameTresql) else "file"
    val contentType =
      if (op.contentTypeTresql != null) {
        val ctStr = getVal(op.contentTypeTresql)
        ContentType.parse(ctStr)
          .toOption
          .getOrElse(sys.error(s"Invalid content type: '$ctStr'"))
      } else ContentType(MediaTypes.`application/json`)

    val fs = fileStreamers.fs(op.fileStreamerName)
    doActionOpAndRender(contentType, op.contentOp, scope, context).flatMap { case (src, ct, _) =>
      src.runWith(fs.fileSink(fn, ct.value))
    }.map(FileInfoResult)
  }

  protected def doTemplate(
    op: Action.Template,
    scope: Scope,
    context: ActionContext,
  )(implicit qr: QuereaseResources): Future[TemplateResult] = {
    import qr._, resourcesFactory._, context.env
    implicit val fs: FileStreamer = fileStreamers.fs(null)
    def template(res: Any): Future[String] = res match {
      case TresqlResult(r) => Future.successful(r.unique[String])
      case HttpResult(resp, _) => template(resp)
      case fr: FileResult => template(fileHttpEntity(fr))
      case Some(ent) => template(ent)
      case ent: HttpEntity => template(ent.dataBytes)
      case HttpEntityResult(ent, _) => template(ent)
      case src: Source[ByteString@unchecked, _] => src.runFold(ByteString.empty)( _ ++ _).map(_.utf8String)
      case x => sys.error(s"Cannot extract template source string from $x")
    }
    def doTemplate(template: String) =
      if (op.dataOp == null) {
        templateEngine(template, scope.toBindeableMap(env))
      } else {
        doActionOp(op.dataOp, scope, context)
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
    for {
      template  <- doActionOp(op.template, scope, context) flatMap template
      res       <- doTemplate(template)
    } yield
      Option(op.filenameTresql)
        .map(t => useResourcesConnOrEvaluator(resources,
        res => Query(t.tresql)(res.withParams(scope.toBindeableMap(env))).unique[String]))
        .map { filename => res match {
          case ft: FileTemplateResult => ft.copy(filename = filename)
          case StringTemplateResult(r) => FileTemplateResult(
            filename, ContentTypes.`text/plain(UTF-8)`.toString, r.getBytes("UTF8"))
        }}.getOrElse(res)
  }

  protected def doEmail(
    op: Action.Email,
    scope: Scope,
    context: ActionContext,
  )(implicit qr: QuereaseResources): Future[LongResult] = {
    import qr._, context.env
    val bindVars = scope.toBindeableMap(env)
    @tailrec
    def recipients(qr: QuereaseResult, vn: String): Source[Map[String, Any], _] = qr match {
      case TresqlResult(result) => Source.fromIterator(() => result.map(_.toMap))
      case HttpEntityResult(ent, decoder) if decoder != null => decoder(vn)(ent)
        .map {
          case m: Map[String, Any]@unchecked => m
          case x => sys.error(s"Email recipient has to be of type Map[String, Any], instead got '$x'")
        }
      case CompatibleResult(r, f, _) => recipients(r, Option(f).map(_.name).orNull)
      case x => sys.error(s"Cannot extract email recipients from '$x'. " +
        s"Supported types are tresql and extract entity operations.")
    }
    doActionOp(op.recipients, scope, context)
      .map(recipients(_, null))
      .map { rec => if (op.isBatch) rec else rec.limit(1) }
      .flatMap(_.runFoldAsync(0) { (c, email) =>
        def s(v: Any): String = if (v == null) null else String.valueOf(v)
        val to = s(email.getOrElse("to", sys.error(s"""Missing "to" address - email can not be sent""")))
        val cc = s(email.getOrElse("cc", null))
        val bcc = s(email.getOrElse("bcc", null))
        val from = s(email.getOrElse("from", null))
        val replyTo = s(email.getOrElse("replyTo", null))
        def subj_body(bv: Map[String, Any]) = {
          def stringContent(qr: QuereaseResult) = qr match {
            case TresqlResult(r) => Future.successful(r.unique[String])
            case _ => renderedResult(qr, null, null, Option(false), context)
              .flatMap(_._1.runReduce(_ ++ _).map(_.decodeString("UTF8")))
          }
          Future.traverse(List(op.subject, op.body))(doActionOp(_, Scope(bv), context).flatMap(stringContent))
        }
        subj_body(bindVars ++ email).flatMap { sb =>
          val List(subject, body) = sb
          Future.traverse(op.attachmentsOp)(doActionOp(_, Scope(scope.data ++ email), context)
            .flatMap(
              renderedResult(_, null, null, Option(false), context).map {
                case (src, fn, ct, _) => EmailAttachment(fn, ct.value, src)
              }
            )
          ).flatMap { att =>
            emailSender.sendMail(to, subject, body, att, cc, bcc, from, replyTo)
          }
        }.map(_ => c + 1)
      }).map(LongResult(_))
  }

  protected def doHttp(
    op: Action.Http,
    scope: Scope,
    context: ActionContext,
  )(implicit qr: QuereaseResources): Future[QuereaseResult] = {
    import qr._, resourcesFactory._, context.env
    val opData = scope.toBindeableMap(env)
    val httpMeth = HttpMethods.getForKeyCaseInsensitive(op.method).get
    val uri = useResourcesConnOrEvaluator(implicitly[Resources], res =>
      tresqlUri.fromTresqlUri(op.uriTresql)(Query, opData, res))
    val (optContentType, headers) = if (op.headerTresql == null) (Some(null) -> Nil) else {
      // content type is used for request body if present
      val parsedValues = useResourcesConnOrEvaluator(implicitly[Resources], res =>
        listOfStringTuples(Query(op.headerTresql.tresql, opData)(res))
          .map { case (name, value) => HttpHeader.parse(name, value) }
      )
      val (ok, errs) = parsedValues.partition(_.isInstanceOf[Ok])
      require(errs.isEmpty, s"Error(s) parsing http headers:\n${
        errs.map(e => e.asInstanceOf[Error].error.formatPretty).mkString("\n")}")
      WabaseService.partitionHeaders(ok.map(_.asInstanceOf[Ok].header))
    }
    val reqF = {
      def reqWithoutBody = HttpRequest(httpMeth, uri, headers)
      if (op.body == null) Future.successful(reqWithoutBody)
      else doActionOpAndRender(optContentType.orNull, op.body, scope, context).map { case (src, ct, clo) =>
          reqWithoutBody.withEntity(clo.map(
            HttpEntity(Option(ct).getOrElse(MediaTypes.`application/octet-stream`), _, src)).getOrElse(
            HttpEntity(ct, src))
          )
      }
    }
    def do_http: HttpRequest => Future[HttpResponse] = {
      req => {
        qr.logger.debug(s"HTTP ${req.method.value} ${req.uri}")
        val httpClientFactory = Option(op.httpClientName)
          .map(httpClients.httpClients.getOrElse(_, sys.error(s"Http client not found: ${op.httpClientName}")))
          .getOrElse(
            if (httpClients.httpClients.size == 1) httpClients.httpClients.head._2
            else sys.error(s"Http client name not specified, expected one http client, got: $httpClients"))
        val httpClient = httpClientFactory(InjectionParametersContext(httpReq, opData))
        val maybeProxyReq =
          if (op.isProxy) req.addAttribute(HttpClient.ModeKey, HttpClient.ProxyMode) else req
        doHttpRequest(httpClient, viewDefOption(context.viewName).map(_.maxContentSize).orNull, maybeProxyReq)
      }
    }
    reqF
      .flatMap(do_http)
      .map(HttpResult(_, op.isProxy))
      .map { r => op.conformTo.map(comp_res(r, _)).getOrElse(r) }
  }

  protected def doExtractHeader(
    op: Action.HttpHeader,
    scope: Scope,
    context: ActionContext,
  )(implicit qr: QuereaseResources): Future[QuereaseResult] = {
    import qr._
    @tailrec def httpRes(qr: QuereaseResult): HttpResponse = (qr: @unchecked) match {
      case HttpResult(response, _) =>
        response.entity.discardBytes(as) // discard bytes since we are interested only in http header
        response
      case cr: CompatibleResult => httpRes(cr.result)
      case TresqlResult(SingleValueResult(qr: QuereaseResult)) => httpRes(qr)
    }
    Option(op.httpOp)
      .map(doActionOp(_, scope, context))
      .map(_.map(httpRes))
      .getOrElse(Future.successful(httpReq))
      .map { msg => Option(msg).flatMap(_.headers.collectFirst {
        case h if h.is(op.name.toLowerCase) => StringResult(h.value())
      }).getOrElse {
        if (op.isOpt) NoResult
        else throw new HttpException(StatusCodes.BadRequest, s"HTTP message is missing required header '${op.name}'")
      } }
  }

  protected def doExtractCookie(
    op: Action.Cookie,
    scope: Scope,
    context: ActionContext,
  )(implicit
    resFac: ResourcesFactory,
    ec: ExecutionContext,
    as: ActorSystem,
    fs: FileStreamer,
    httpReq: HttpRequest,
  ): Future[QuereaseResult] = {
    val cookie = Option(httpReq).map(_.cookies).flatMap(_.collectFirst {
      case h if h.name.toLowerCase == op.name.toLowerCase => StringResult(h.value)
    }).getOrElse(NoResult)
    Future.successful(cookie)
  }

  protected def doExtractEntity(
    exe: Action.ExtractHttpEntity,
    scope: Scope,
    context: ActionContext,
  )(implicit qr: QuereaseResources): Future[QuereaseResult] = {
    import qr._
    Option(exe.op).map { op =>
      doActionOp(op, scope, context)
        .map {
          case HttpResult(response, _) => response.entity
          case fr: FileResult => fileHttpEntity(fr)
            .getOrElse(sys.error(s"Cannot find file data: ${fr.fileInfo}"))
          case x => sys.error(s"Cannot extract entity from $x. Currently only HttpResult and FileResult are supported")
        }
    } .getOrElse(Future.successful(qr.httpReq.entity))
      .map { ent =>
        val res = HttpEntityResult(
          ent,
          Option(exe.decoder)
            .map(dn =>
              requestDecoders.getOrElse(dn,
                sys.error(s"Cannot decode http entity data. Request decoder '$dn' not found."))
            )
            .orNull
        )
        exe.conformTo
          .map(comp_res(res, _))
          .getOrElse(res)
      }
  }

  protected def doDb(
    op: Action.Db,
    scope: Scope,
    context: ActionContext,
  )(implicit qr: QuereaseResources): Future[DbResult] = {
    import qr._, context.env
    val (poolName, extraDbs) =
      if (op.dbs.nonEmpty) {
        def may_be_add_extra(pn: PoolName, edb: Seq[DbAccessKey]) =
          if (pn.connectionPoolName == defaultCpName || edb.exists(_.db == pn.connectionPoolName)) (pn, edb)
          else (pn, edb ++ Seq(DbAccessKey(pn.connectionPoolName)))
        may_be_add_extra(PoolName(op.dbs.head.db), op.dbs.tail)
      }
      else dbResourceNames(context.viewName, context.actionName)
    val newResFact = resourcesFactory
       .focus(poolName.connectionPoolName, defaultCpName)
       .copy()(resources = resourcesFactory.initResources(poolName, extraDbs))
    val closeRes = resourcesFactory.closeResources(newResFact.resources, op.doRollback, _)
    val nqr = new QuereaseResources()(
      newResFact, ec, as, httpReq, qio, fileStreamers, httpClients, parametersProvider, qr.logger)
    logContext(context, env, nqr)
    doSteps(op.action.steps, context.copy(stepName = "db"),
      Future.successful(Scope(Map(), parent = scope)))(nqr).map {
      case DbResult(r, cl) => DbResult(r, cl.andThen(_ => closeRes(None)))
      case r => DbResult(r, closeRes)
    }.andThen {
      case Failure(NonFatal(ex)) => closeRes(Option(ex))
    }
  }

  protected def doBlock(
    op: Action.Block,
    scope: Scope,
    context: ActionContext,
  )(implicit qr: QuereaseResources): Future[QuereaseResult] = {
    doSteps(op.action.steps, context.copy(stepName = "block"), Future.successful(Scope(Map(), parent = scope)))
  }

  protected def doConf(
    op: Action.Conf,
    scope: Scope,
    context: ActionContext
  )(implicit
    resFac: ResourcesFactory,
    ec: ExecutionContext,
    as: ActorSystem): Future[ConfResult] = {
    def value = op.paramType match {
      case Action.NumberConf => config.getNumber(op.param)
      case Action.StringConf => config.getString(op.param)
      case Action.BooleanConf => config.getBoolean(op.param)
      case _ => config.getValue(op.param).unwrapped()
    }
    Future.successful(ConfResult(op.param, configValueAsScala(value)))
  }

  protected def doJsonCodec(
    op: Action.JsonCodec,
    scope: Scope,
    context: ActionContext
  )(implicit qr: QuereaseResources): Future[QuereaseResult] = {
    import qr._
    implicit val fs: FileStreamer = fileStreamers.fs(null)
    doActionOp(op.op, scope, context)
      .flatMap(dataForNextStep(_, context, true))
      .map { res =>
        if (op.encode) {
          StringResult(ResultEncoder.encodeAnyToJsonString(res))
        } else {
          try {
            (res match {
              case in: java.io.InputStream => CborOrJsonAnyValueDecoder.decodeFromInputStream(in)
              case _ => CborOrJsonAnyValueDecoder.decode(ByteString(String.valueOf(res)))
            }) match {
              case m: Map[String@unchecked, _] => MapResult(m)
              case s: Seq[Map[String, _]@unchecked] => IteratorResult(s.iterator)
              case n: java.lang.Number => NumberResult(n)
              case s: String => StringResult(s)
              case null => NoResult
              case x => AnyResult(x)
            }
          } catch {
            case NonFatal(e) => throw new RuntimeException(s"ERROR decoding result: $res", e)
          }
        }
      }
  }

  protected def doExtractParts(
    op: Action.ExtractParts,
    scope: Scope,
    context: ActionContext,
  )(implicit qr: QuereaseResources): Future[RequestPartResult] = {
    import qr._, context.env
    val entity = httpReq.entity
    val fs = fileStreamers.fs(op.fileStreamerName)
    if (entity.contentType.mediaType.isMultipart) {
      import org.apache.pekko.http.scaladsl.unmarshalling.MultipartUnmarshallers._
      import org.apache.pekko.http.scaladsl.server.directives.MarshallingDirectives
      val um = MarshallingDirectives.as[Multipart.FormData]
      implicit val ec = as.dispatcher
      um(httpReq).map { formdata =>
        val src = formdata.parts.map {
          case filePart if filePart.filename.isDefined =>
            RequestPart(filePart.name, filePart.filename.get, filePart.entity)
          case dataPart =>
            RequestPart(dataPart.name, null, dataPart.entity)
        }
        RequestPartResult(src, fs)
      }
    } else {
      val filename = viewDefOption(context.viewName)
        .filter(_.keyFieldNames.size == 1)
        .flatMap(vd => scope.toBindeableMap(env).get(vd.keyFieldNames.head).map(String.valueOf))
        .getOrElse(httpReq.uri.path.reverse.head.toString)
      Future.successful(
        RequestPartResult(
          Source.single(
            RequestPart(null, if (filename.isEmpty) null else filename, entity)
          ),
          fs
        )
      )
    }
  }

  protected def doRedirectToKey(
    op: Action.RedirectToKey,
    scope: Scope,
    context: ActionContext
  ): Future[QuereaseResult] = {
    import context.env
    val name = op.name
    val viewName = if (name == "this") context.viewName else name
    val idName = viewNameToIdName.getOrElse(viewName, null)
    val dataWithEnv = scope.toBindeableMap(env)
    val id = dataWithEnv.getOrElse(idName, null)
    val kr = keyResult(IdResult(id, idName), viewName, dataWithEnv)
    Future.successful(ResponseResult(303, RedirectValue(redirectTresqlUri(kr))))
  }

  protected def doCommit(resources: Resources): Future[QuereaseResult] = {
    def commit(c: Connection): Unit = Option(c).foreach(_.commit())
    commit(resources.conn)
    resources.extraResources.foreach { case (_, r) => commit(r.conn) }
    Future.successful(NoResult)
  }

  protected def doThis(
    op: Action.This,
    scope: Scope,
    context: ActionContext
  ): Future[QuereaseResult] = {
    val data = MapResult(scope.data)
    Future.successful(op.conformTo.map(comp_res(data, _)).getOrElse(data))
  }

  // XXX copied from Marshalling
  private val crudRedirectsPrefix =
    Option("app.crud-redirects-prefix").filter(config.hasPath).map(config.getString).getOrElse("")
  private def redirectTresqlUri(kr: KeyResult): TresqlUri.Uri =
    TresqlUri.Uri(Seq(s"${crudRedirectsPrefix}${kr.viewName}"), kr.key)

  protected def doActionOp(
    op: Action.Op,
    scope: Scope,
    context: ActionContext,
  )(implicit qr: QuereaseResources): Future[QuereaseResult] = {
    import qr._
    import resourcesFactory._
    import context.env
    implicit val fs: FileStreamer = fileStreamers.fs(null)
    op match {
      case to: Action.Tresql => Future.successful(doTresql(to, scope, context))
      case vc: Action.ViewCall => doViewCall(vc, scope, context)
      case op: Action.Unique => doUnique(op, scope, context)
      case inv: Action.Invocation => doInvocation(inv, scope, context)
      case rtk: Action.RedirectToKey => doRedirectToKey(rtk, scope, context)
      case st: Action.Response => doResponse(st, scope, context)
      case Action.Commit => doCommit(resources)
      case cond: Action.If => doIf(cond, scope, context)
      case foreach: Action.Foreach => doForeach(foreach, scope, context)
      case resource: Action.Resource => doResource(resource, scope, context)
      case file: Action.File => doFile(file, scope, context)
      case toFile: Action.ToFile => doToFile(toFile, scope, context)
      case template: Action.Template => doTemplate(template, scope, context)
      case email: Action.Email => doEmail(email, scope, context)
      case http: Action.Http => doHttp(http, scope, context)
      case eh: Action.HttpHeader => doExtractHeader(eh, scope, context)
      case exc: Action.Cookie => doExtractCookie(exc, scope, context)
      case exe: Action.ExtractHttpEntity => doExtractEntity(exe, scope, context)
      case db: Action.Db => doDb(db, scope, context)
      case block: Action.Block => doBlock(block, scope, context)
      case c: Action.Conf => doConf(c, scope, context)
      case j: Action.JsonCodec => doJsonCodec(j, scope, context)
      case ep: Action.ExtractParts => doExtractParts(ep, scope, context)
      case th: Action.This => doThis(th, scope, context)
      case VariableTransforms(vts) =>
        Future.successful(doVarsTransforms(vts, Map[String, Any](), scope.data ++ env))
      case _: Action.Else => sys.error(s"Integrity error. Else operation cannot be here, must be coalesced into if operation")
    }
  }

  private def doActionOpAndRender(
    contentType: ContentType,
    op: Action.Op,
    scope: Scope,
    context: ActionContext,
   )(implicit qr: QuereaseResources): Future[(Source[ByteString, _], ContentType, Option[Long])] = {
    import qr._
    doActionOp(op, scope, context)
      .flatMap(renderedResult(_, contentType, null, None, context))
      .map { case (src, _, ct, l) => (src, ct, l) }
  }

  private def renderedResult(
    res: QuereaseResult,
    contentType: ContentType,
    resFil: ResultRenderer.ResultFilter,
    isCollection: Option[Boolean],
    context: ActionContext,
  )(implicit
    as: ActorSystem,
    ec: ExecutionContext,
  ): Future[(Source[ByteString, _], String, ContentType, Option[Long])] = {
    val ct: ContentType = if (contentType == null) MediaTypes.`application/json` else contentType

    def encodeJson(data: Any): Future[(Source[ByteString, _], String, ContentType, Option[Long])] = {
      val res = ResultEncoder.encodeAnyToJsonByteString(data)
      Future.successful((Source.single(res), null, ct, Option(res.length)))
    }
    def encodePrimitive(
       v: Any,
       pct: ContentType = ContentTypes.`text/plain(UTF-8)`
    ): Future[(Source[ByteString, _], String, ContentType, Option[Long])] = {
      val b = String.valueOf(v).getBytes("UTF8")
      Future.successful((Source.single(ByteString(b)), null,
        pct,
        Option(b.length)
      ))
    }
    def encodeStructure(data: Any, isColl: Boolean) = {
      renderedSource(data, ct, resFil, isColl)(
        WabaseAppConfig.SerializationBufferSize,
        WabaseAppConfig.viewSerializationBufferMaxFileSize(context.viewName)
      ).map { case (src, l) => (src, null, ct, l) }
    }
    def isUnfilteredJson =
      (ct == ContentTypes.`application/json`) && (resFil == null || resFil == ResultRenderer.NoFilter)
    def encodeMap(m: Map[_, _]) =
      if  (isUnfilteredJson)
           encodeJson(if (isCollection.getOrElse(false)) Seq(m) else m)
      else encodeStructure(Seq(m).iterator, isCollection.getOrElse(false))

    res match {
      case StringResult(v) => encodePrimitive(v, ct)
      case LongResult(v) => encodePrimitive(v)
      case NumberResult(v) => encodePrimitive(v)
      case ConfResult(_, v) => v match {
        case i: Iterable[_] => encodeJson(i)
        case _ => encodePrimitive(v)
      }
      case AnyResult(v) => encodeJson(v)
      case MapResult(m) => encodeMap(m)
      case IteratorResult(data) => encodeStructure(data, true)
      case TresqlResult(tr) => tr match {
        case SingleValueResult(m: Map[_, _]) => encodeMap(m)
        case SingleValueResult(r: Iterable[_]) => encodeStructure(r.iterator, isCollection.getOrElse(true))
        case SingleValueResult(s: String) => encodePrimitive(s, ct)
        // single value can be querease result if action step keepResult is set like 'as result variable = ...'
        case SingleValueResult(qr: QuereaseResult) => renderedResult(qr, contentType, resFil, isCollection, context)
        case SingleValueResult(r) => encodeStructure(Iterator(r), isCollection.getOrElse(false))
        case r => encodeStructure(r, isCollection.getOrElse(true))
      }
      case TresqlSingleRowResult(row) => encodeStructure(row, isCollection.getOrElse(false))
      case fileResult: FileResult =>
        fileHttpEntity(fileResult)
          .map(e => Future
            .successful((e.dataBytes, fileResult.fileInfo.filename, e.contentType, e.contentLengthOption)))
          .getOrElse(sys.error(s"File not found: ${fileResult.fileInfo}"))
      case resourceResult: ResourceResult =>
        ResourceFile(classOf[AppQuerease].getResource(resourceResult.resource)).map { rf =>
          Future.successful(( StreamConverters.fromInputStream(() => rf.url.openStream()),
            null,
            if (contentType == null) resourceResult.contentType else contentType,
            Some(rf.length)
          ))
        }.getOrElse(sys.error(s"Resource not found: ${resourceResult.resource}"))
      case templateResult: TemplateResult => templateResult match {
        case StringTemplateResult(content) =>
          val data = ByteString(content)
          Future.successful((Source.single(data), null, ContentTypes.`text/plain(UTF-8)`, Option(data.size)))
        case FileTemplateResult(fn, contentType, content) =>
          val ct = ContentType.parse(contentType).toOption
            .getOrElse(sys.error(s"Error parsing template result content type: $contentType"))
          Future.successful((Source.single(ByteString(content)), fn, ct, Option(content.size)))
      }
      case HttpEntityResult(res, _) =>
        Future.successful((res.dataBytes,
          null,
          res.contentType,
          res.contentLengthOption
        ))
      case HttpResult(res, _) =>
        Future.successful(( res.entity.dataBytes,
          res.header[`Content-Disposition`]
            .filter(_.dispositionType == attachment)
            .flatMap(_.params.get("filename"))
            .orNull,
          res.entity.contentType,
          res.entity.contentLengthOption
        ))
      case CompatibleResult(r, fil, isCollection) => renderedResult(r, ct, fil, Option(isCollection), context)
      case NoResult => encodePrimitive("")
      case x => sys.error(s"Currently unable to create rendered source from result: $x")
    }
  }

  /**
   * Render data into source in format specified by content type.
   * @return formatted source, optional length of data (if [[CompleteResult]] is returned from serialization)
   * Params:
   * @param data - data to be rendered, accepted types are [[org.tresql.Result]], [[org.tresql.RowLike]], [[Iterator]]
   * @param contentType - required result format - must be supported [[AppQuerease#resultRenderers]]
   * @param resultFilter - one for [[ResultRenderer.ResultFilter]]
   * @param isCollection - is used in case of application/json format requiring single element list to unwrap from array tags
   * @param bufferSize - memory buffer size to store data in the case of slower downstream
   * @param maxFileSize - max file size for data storage in the case of slower downstream
   * @param as - [[ActorSystem]]
   * @param ec - [[ExecutionContext]]
   * */
  def renderedSource(
    data: Any,
    contentType: ContentType,
    resultFilter: ResultRenderer.ResultFilter,
    isCollection: Boolean,
  )(
    bufferSize: Int,
    maxFileSize: Long,
  )(implicit
    as: ActorSystem,
    ec: ExecutionContext,
  ): Future[(Source[ByteString, _], Option[Long])] = {
    val viewDef = if (resultFilter == null) null else nameToViewDef(resultFilter.name)
    val renderer =
      resultRenderers.renderers.get(contentType)
        .map(_ (isCollection, resultFilter, viewDef))
        .getOrElse(sys.error(s"Renderer not found for content type: $contentType"))
    val dataSource = data match {
      case r: Result[_]   =>
        if (isCollection) TresqlResultSerializer.source(() => r, createEncoder = renderer)
        else TresqlResultSerializer.rowSource(() => r, createEncoder = renderer)
      case r: RowLike     => TresqlResultSerializer.rowSource(() => r, createEncoder = renderer)
      case r: Iterator[_] => DataSerializer.source(() => r, createEncoder = renderer)
      case x              => sys.error(s"Unable to render data: '$x'. Only tresql Result, RowLike or Iterator allowed")
    }
    ResultSerializer.serializeResult(bufferSize, maxFileSize, dataSource)
      .map(_.head)
      .map {
        case CompleteResult(bytes) => (Source.single(bytes), Option(bytes.length))
        case IncompleteResultSource(result) => (result, None)
      }
  }

  protected def doHttpRequest(
    httpClient: HttpRequest => Future[HttpResponse],
    responseMaxSize: jLong,
    req: HttpRequest,
  )(implicit ec: ExecutionContext): Future[HttpResponse] = {
    httpClient(req).map {
      res => if (responseMaxSize == null) res else res.withEntity(res.entity.withSizeLimit(responseMaxSize))
    }
  }

  def fileHttpEntity(fileResult: FileResult): Option[UniversalEntity] = {
    import fileResult._
    fileStreamer.getFileInfo(fileInfo.id, fileInfo.sha_256).map { fi =>
      val ct = ContentType.parse(fi.content_type).toOption.getOrElse(sys.error(s"Invalid content type: '${fi.content_type}'"))
      HttpEntity(ct, fi.size, fi.source)
    }
  }

  private def decodeHttpEntity(
    ent: HttpEntity,
    viewName: String,
    isCollection: Boolean,
    decoder: RequestDecoders.RequestDecoder,
  )(implicit as: ActorSystem) = {
    import scala.concurrent.duration._
    implicit val ec = as.dispatcher

    def decodeToMap(bs: ByteString) =
      if (viewName == null) CborOrJsonAnyValueDecoder.decode(bs)
      else cborOrJsonDecoder.decodeToMap(bs, viewName)(viewNameToMapZero)
    def decodeToSeqOfMaps(bs: ByteString) =
      if (viewName == null) CborOrJsonAnyValueDecoder.decode(bs)
      else cborOrJsonDecoder.decodeToSeqOfMaps(bs, viewName)(viewNameToMapZero)
    def decodeUsingDecoder = decoder(viewName)(ent)

    if (decoder != null) Future.successful(decodeUsingDecoder)
    else ent.toStrict(1.second).map { se =>
      if (ent.contentType == ContentTypes.`application/json`)
        if (isCollection) decodeToSeqOfMaps(se.data) else decodeToMap(se.data)
      else se.data.decodeString("UTF-8")
    }
  }

  private def dataForNextStep(res: QuereaseResult, context: ActionContext,
                              unwrapSingleValue: Boolean)(implicit qr: QuereaseResources): Future[_] = {
    import qr._
    def v(view: String) = viewDef(
      if (view == "this") context.view.map(_.name) getOrElse view
      else view
    )

    def maybeUnwrapSingleVal(l: Seq[Map[String, Any]]) = l match {
      case row :: Nil if row.size == 1 => row.head._2
      case rows => rows
    }
    (res match {
      case TresqlResult(tr) => tr match {
        case dml: DMLResult =>
          dml.id.map(IdResult(_, null)) orElse dml.count getOrElse 0
        case SingleValueResult(v) => v
        case ar: ArrayResult[_] => ar.values.toVector
        case r: Result[_] =>
          val l = r.toListOfMaps
          if (unwrapSingleValue) maybeUnwrapSingleVal(l) else l
      }
      case srr: TresqlSingleRowResult => srr.map(_.toMap)
      case MapResult(mr) => mr
      case IteratorResult(ir) => ir.toVector
      case LongResult(nr) => nr
      case NumberResult(nr) => nr
      case StringResult(str) => str
      case id: IdResult => id
      case kr: KeyResult => kr.ir
      case AnyResult(ar) => ar match {
        case v: Iterator[_] => v.toVector
        case v => v // TODO may be need to convert java collections to scala?
      }
      case ResponseResult(code, value, _, _) => Map("code" -> code, "value" ->
        (value match {
          case ResultValue(v) => v
          case RedirectValue(value) => tresqlUri.uri(value).toString()
        }))
      case fi: FileInfoResult => fi.fileInfo.toMap
      case FileResult(fi, fs) => fs.getFileInfo(fi.id, fi.sha_256)
        .map(f => f.source.runWith(StreamConverters.asInputStream()))
        .getOrElse(
          sys.error(s"Cannot bind FileResult value. File ${fi.filename} (sha_256 - ${fi.sha_256}) not found!"))
      case rs: ResourceResult =>
        ResourceFile(classOf[AppQuerease].getResource(rs.resource))
          .map(rf => StreamConverters.fromInputStream(() => rf.url.openStream()).runReduce(_ ++ _))
          .orNull
      case tr: TemplateResult => tr match {
        case StringTemplateResult(content) => content
        case FileTemplateResult(_, _, content) => content
      }
      case HttpResult(r, isProxy) =>
        if (!isProxy)
          if (r.status.isRedirection()) r.headers.find(_.is("location")).map(_.value()).getOrElse("")
          else r.entity.dataBytes.runWith(StreamConverters.asInputStream())
        else httpResponseToMap(r,
          ent => Future.successful(ent.dataBytes.runWith(StreamConverters.asInputStream())),
          ent => decodeHttpEntity(ent, null, false, null)
        )
      case HttpEntityResult(r, d) => decodeHttpEntity(r, null, false, d)
      case NoResult => NoResult
      case CompatibleResult(r, null, _) => r  // return result as non bindable value
      case CompatibleResult(r, filter, isCollection) => r match {
        case TresqlResult(r: Result[_]) =>
          val l = toCompatibleSeqOfMaps(r, v(filter.name)) // FIXME assumes that filter name matches view name, refactor!
          if (unwrapSingleValue) maybeUnwrapSingleVal(l) else l
        case r: TresqlSingleRowResult => r.map(toCompatibleMap(_, v(filter.name))) // FIXME assumes that filter name matches view name
        case fr: FileResult => fileHttpEntity(fr).map(decodeHttpEntity(_, filter.name, isCollection, null)) // FIXME assumes that filter matches view name
          .getOrElse(sys.error(s"File not found: ${fr.fileInfo}"))
        case HttpEntityResult(r, d) => decodeHttpEntity(r, filter.name, isCollection, d)  // FIXME assumes that filter name matches view name
        case HttpResult(r, isProxy) =>
          if (!isProxy) decodeHttpEntity(r.entity, filter.name, isCollection, null) // FIXME assumes that filter name matches view name
          else httpResponseToMap(r,
            ent => decodeHttpEntity(ent, filter.name, isCollection, null),
            ent => decodeHttpEntity(ent, null, false, null)
          )
        case r => dataForNextStep(r, context, unwrapSingleValue)
      }
      case DbResult(dbr, cl) => dataForNextStep(dbr, context, unwrapSingleValue).andThen {
        case r => cl(r.failed.toOption) // close db resources
      } (as.dispatcher)
      case ConfResult(_, r) => r
      case r: RequestPartResult => saveRequestParts(r)
      case x => sys.error(s"${x.getClass.getName} not expected here!")
    }) match {
      case f: Future[_] => f
      case x => Future.successful(x)
    }
  }

  private def consumeResult(res: Any)(implicit qr: QuereaseResources): Future[Any] = {
    import qr._
    (res match {
      case TresqlResult(tr) => tr.close()
      case TresqlSingleRowResult(sr) => sr.close()
      case IteratorResult(ir) => consumeResult(ir)
      case it: Iterator[_] => while(it.hasNext) it.next()
      case AnyResult(ar) => consumeResult(ar)
      case ResponseResult(_, ResultValue(r), _, _) => consumeResult(r)
      case ent: HttpEntity => ent.discardBytes()
      case HttpResult(res, _) => consumeResult(res.entity)
      case HttpEntityResult(ent, _) => consumeResult(ent)
      case CompatibleResult(res, _, _) => consumeResult(res)
      case DbResult(res, cl) => consumeResult(res)
        .andThen { case r => cl(r.failed.toOption) }
      case RequestPartResult(res, _) => res.runForeach(_.entity.discardBytes())
      case x => x
    }) match {
      case f: Future[_] => f
      case x => Future.successful(x)
    }
  }

  private def comp_res(res: QuereaseResult, conformTo: Action.OpResultType) = {
    val (fil, isColl) = conformTo match {
      case Action.ViewResultType(vn, isColl) => (
        if (vn != null) new ResultRenderer.ViewFieldFilter(vn, nameToViewDef) else ResultRenderer.NoFilter,
        isColl
      )
      case Action.NonBindableResultType => (null, false)
    }
    CompatibleResult(res, fil, isColl)
  }

  private def invokeFunction(
    className: String,
    function: String,
    stepData: Map[String, Any],
    parameterFun: InvocationParameterFun,
    injectionContext: InjectionParametersContext,
    qr: QuereaseResources,
  ): Any = {
    import qr._
    val stepParameters = Seq(
      (classOf[scala.collection.immutable.Map[_, _]], () => stepData),
      (classOf[java.util.Map[_, _]], () => stepData.asJava),
      (classOf[MapResult], () => MapResult(stepData)),
    )

    val contextParams = Seq[(Class[_], () => Any)](
      (classOf[QuereaseResources], () => qr),
      (classOf[Resources], () => resourcesFactory.resources),
      (classOf[ResourcesFactory], () => resourcesFactory),
      (classOf[ExecutionContext], () => ec),
      (classOf[ActorSystem], () => as),
      (classOf[WabaseFileStreamers], () => fileStreamers),
      (classOf[HttpRequest], () => httpReq),
      (classOf[AppQuereaseIo[Dto]], () => qio),
      (classOf[AppQuerease], () => AppQuerease.this),
      (classOf[WabaseHttpClients], () => httpClients),
    )
    val providerFun = parametersProvider(injectionContext)
    org.wabase.invokeFunction(className, function, stepParameters ++ contextParams,
      parameterFun orElse { case (p, _) if providerFun.isDefinedAt(p) => providerFun(p) })
  }
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
}

trait DtoWithId extends Dto with org.mojoz.querease.DtoWithId

object DefaultAppQuerease extends AppQuerease
object DefaultAppQuereaseIo extends AppQuereaseIo[Dto](DefaultAppQuerease)

object AppQuerease {
  case class InjectionParametersContext(
    req:  HttpRequest,
    data: Map[String, Any]  = Map(),	  // action current step data
  )
  type InjectionParametersProvider = InjectionParametersContext => PartialFunction[Parameter, Any]

  trait InjectionParametersProviderFactory {
    def createInjectionParametersProvider: InjectionParametersProvider
  }

  def injectionParametersProviderFactory: InjectionParametersProviderFactory =
    getObjectOrNewInstance[InjectionParametersProviderFactory](
      config, "app.wabase-injection-parameters-provider-factory", "injection parameters provider factory")

  object InjectionParametersProviderFactory extends AppQuerease.InjectionParametersProviderFactory {
    def createInjectionParametersProvider: InjectionParametersProvider = _ => PartialFunction.empty
  }

  def saveRequestParts(parts: RequestPartResult)(implicit as: ActorSystem): Future[Map[String, Any]] = {
    implicit val ec = as.dispatcher
    parts.result.mapAsync(1) {
      case p if p.filename != null =>
        p.entity.dataBytes.runWith(parts.fs.fileSink(p.filename, p.entity.contentType.toString))
          .map(_.toMap)
          .map(m => if (p.name == null) m else Map(p.name -> m))
      case p => p.entity.dataBytes.runFold(ByteString.empty)(_ ++ _).map(v => Map(p.name -> v.utf8String))
    }.runFold(Map[String, Any]())(_ ++ _)
  }

  def saveRequestPart(part: RequestPart, fs: FileStreamer)(implicit as: ActorSystem): Future[Map[String, Any]] = {
    implicit val ec = as.dispatcher
    if (part.filename != null)
      part.entity.dataBytes.runWith(fs.fileSink(part.filename, part.entity.contentType.toString))
        .map(_.toMap)
        .map(m => if (part.name == null) m else m + ("name" -> part.name))
    else part.entity.dataBytes.runFold(ByteString.empty)(_ ++ _).map(v => Map(part.name -> v.utf8String))
  }

  /** Function used for http headers construction.
   * NOTE: Returned tuple elements are trimmed since sql may return trailing spaces from union select
   * */
  def listOfStringTuples(result: Result[_]): List[(String, String)] = {
    result match {
      case SingleValueResult(r) => r match { // unwrap header values from list of maps
        case m: Map[_, _] => m.map { case (k, v) => (k.toString.trim, v.toString.trim) }.toList
        case i: Iterable[_] => i.map {
          case m: Map[_, _] if m.size > 1 =>
            val h = m.toList
            h.head._2.toString.trim -> h.tail.head._2.toString.trim // extract values - 1st value header name, 2nd - header value
          case x => sys.error(s"Cannot retrieve values from structure: [$x], Map[_, _] is required")
        }.toList
        case x => sys.error(s"Cannot retrieve values from structure: [$x], Iterable[Map[_, _]] is required")
      }
      case r: Result[_] => r.list[String, String].map{ case (n, v) => (n.trim, v.trim) }
    }
  }

  def buildCookieHeaderValue(tresql: TresqlResult): String = {
    val pairs = listOfStringTuples(tresql.result).map(HttpCookiePair(_))
    Cookie(pairs).value
  }

  def dtoParameterFromMap(data: () => Map[String, Any])(
    qio: AppQuereaseIo[Dto]): PartialFunction[InvocationParameter, Dto] = {
    case (par, _) if classOf[Dto].isAssignableFrom(par.getType) =>
      val mf = Manifest.classType[Dto](par.getType)    // somehow need to specify method type parameter Dto for not to fail in runtime on next line??
      qio.fill(data())(mf)                             // specify manifest explicitly so it is not Nothing
  }

  def dtoParameterFromMapF(data: () => Future[Map[String, Any]])(
    qio: AppQuereaseIo[Dto])(implicit ec: ExecutionContext): PartialFunction[InvocationParameter, Future[Dto]] = {
    case (par, idx) if classOf[Dto].isAssignableFrom(par.getType) =>
      data().map(m => dtoParameterFromMap(() => m)(qio)(par -> idx))
  }
  /** Returns [[InvocationParameterFun]] which is defined if parameter index matches and
   * [[QuereaseResult]] can be conformed to function parameter type
   * */
  def explicitInvocationParameter(qr: QuereaseResult, idx: Int, function: String)(
    qe: AppQuerease,
    qrToAny: QuereaseResult => Future[_],
  )(implicit resources: QuereaseResources): InvocationParameterFun = {
    val tresqlResult = qr match { case TresqlResult(result) => result case _ => null }
    ({ // convert TresqlResult to primitive value
      case (par, i) if i == idx && tresqlResult != null &&
        tresqlResult.typedPf(0).isDefinedAt(scala.reflect.Manifest.classType(par.getType).toString()) =>
        tresqlResult match {
          case SingleValueResult(v) => qe.convertToType(v, par.getType)
          case r => try if (r.hasNext) {
            r.next()
            r.typedPf(0)(scala.reflect.Manifest.classType(par.getType).toString())
          } else null finally r.close()
        }
    }: InvocationParameterFun) orElse ({ // convert TresqlResult to org.tresql.Result
      case (par, i) if i == idx && tresqlResult != null && par.getType.isAssignableFrom(classOf[Result[_]]) =>
        tresqlResult
    }: InvocationParameterFun) orElse {
      case (par, i) if i == idx => convertResultToParType(par.getType, qr)(
        qrToAny, () => throw new IllegalArgumentException(
          s"Cannot find value for function's '$function' ${idx + 1} parameter '${par.getName}: ${
            par.getType.getName}'.\nInstead got: '$qr'"))
    }
  }

  def convertResultToParType(parType: Class[_], qr: QuereaseResult)(
    qrToAny: QuereaseResult => Future[_], error: () => Nothing)(
    implicit resources: QuereaseResources): Any = {
    if (parType.isAssignableFrom(qr.getClass)) qr
    else {
      import resources._
      def cf(r: Any): Any = r match {
        case x if parType.isAssignableFrom(x.getClass) => x
        case m: Map[String, Any]@unchecked =>
          if (classOf[Dto].isAssignableFrom(parType)) qio.fill(m)(Manifest.classType[Dto](parType))
          else error()
        case l: Seq[Map[String, Any]@unchecked] =>
          val elType = parType.getComponentType
          if (elType != null && classOf[Dto].isAssignableFrom(elType)) {
            val mf = Manifest.classType[Dto](elType)
            l.map(qio.fill(_)(mf)).toArray(mf)
          } else error()
        case in: java.io.InputStream => cf(CborOrJsonAnyValueDecoder.decodeFromInputStream(in))
        case _ => error()
      }
      qrToAny(qr) map cf
    }
  }

  def focusResource(name: String, defaultName: String)(res: Resources): Resources = {
    if (res.extraResources.contains(name)) {
      val resWithDefault =
        if (name != defaultName && !res.extraResources.contains(defaultName)) {
          res.withExtraResources(res.extraResources + (defaultName -> res.withExtraResources(Map())))
        } else res
      resWithDefault.extraResources(name).withExtraResources(resWithDefault.extraResources)
    } else if (res.extraResources.contains(defaultName)) {
      res.extraResources(defaultName).withExtraResources(res.extraResources)
    } else res
  }

  def configValueAsScala(value: Any): Any = value match {
    case m: java.util.Map[_, _] => m.asScala.map { case (k, v) => String.valueOf(k) -> configValueAsScala(v) }.toMap
    case l: java.util.List[_] => l.asScala.map(configValueAsScala).toList
    case v => v
  }

  def httpResponseToMap(
    resp: HttpResponse,
    contentSuccess: HttpEntity => Future[Any],
    contentFailure: HttpEntity => Future[Any]
  )(implicit ec: ExecutionContext): Future[Map[String, Any]] = {
    for {
      content <- if (resp.status.isSuccess) contentSuccess(resp.entity)
      else contentFailure(resp.entity)
    } yield Map(
      "status" -> resp.status.intValue,
      "headers" -> resp.headers.map(h => (h.name, h.value)).toMap,
      "content_type" -> resp.entity.contentType.value,
      "content" -> content,
    )
  }

  def startJob(jobName: String, params: Map[String, Any])(implicit
    as: ActorSystem,
    ec: ExecutionContext,
    qio: AppQuereaseIo[Dto],
  ): Future[Int] = {
    qio.qe.viewDefOption(jobName).map { job =>
      val jobControlActorName = config.getString("app.job.actor-name")
      import org.apache.pekko.pattern.ask
      implicit val timeout: Timeout = 1.second
      for {
        jobControActor <- as.actorSelection(as / jobControlActorName).resolveOne(1.second)
        msg <- jobControActor ? WabaseScheduler.Tick(job, params)
      } yield msg match {
        case WabaseScheduler.JobStarted => StatusCodes.OK
        case WabaseScheduler.JobRunning => StatusCodes.Conflict
        case x => throw sys.error(s"Unknown message from scheduler '$x' for job '$jobName'")
      }
    }.getOrElse {
      Future.successful(StatusCodes.NotFound)
    }.map(_.intValue)
  }

  /** Can be used in actions since Thread.sleep cannot be invoked directly due to method overload */
  def sleep(millis: Long): Unit = Thread.sleep(millis)

  /**
   * Creates nested hierarchical data based on tresql result.
   *
   * @param levelParamName result column indicating hierarchy level. Parameter value must be non negative integer
   * @param nestedParamName parameter name for nested structure
   * @param result tresql result. Result must be ordered according to hierarchy path.
   * @return sequence of nested maps
   * */
  def toHierarchy(levelParamName: String, nestedParamName: String, result: Result[RowLike]): scala.collection.Seq[Map[String, Any]] = {
    import scala.collection.mutable.{Stack => MS, ArrayBuffer => AB}
    type Rows = AB[Map[String, Any]]
    type HierEl = (java.lang.Number, Rows)
    val res = result.map(_.toMap).foldLeft(MS[HierEl]((Integer.MIN_VALUE, null))) { (res, row) =>
      val (cur_level, rows) = res.top
      val level = row(levelParamName).asInstanceOf[Number]
      if (cur_level == level) {
        rows += row
        res
      } else if (cur_level.intValue() < level.intValue()) {
        res.push(level -> AB(row))
      } else {
        def coalesce(rows: List[Rows]): Rows = (rows: @unchecked) match {
          case List(row: Rows) => row
          case h :: tail => h(h.size - 1) = h.last + (nestedParamName -> coalesce(tail).toSeq); h
        }
        @tailrec def popWhile(st: MS[HierEl], cond: Int => Boolean, res: List[HierEl]): List[HierEl] = {
          if (!cond(st.top._1.intValue())) res
          else popWhile(st, cond, st.pop() :: res)
        }
        val seq = popWhile(res, _ >= level.intValue(), Nil)
        res.push(seq.head._1 -> (coalesce(seq.map(_._2)) += row))
      }
    }
    res.pop()._2.toSeq
  }

  case class Scope(
    data: Map[String, Any],
    initBindVars: Map[String, Any] = Map(),
    parent: Scope = null,
    transparent: Boolean = true,
  ) {
    def apply(name: String): Any =
      data.getOrElse(name, throw new NoSuchElementException(s"Variable not found in scope: $name"))
    def -(name: String): Scope = copy(data = data - name)
    def +(name: String, value: Any): Scope = copy(data = data + (name -> value))
    /** Creates tresql bindeable map from action scope and env data. */
    def toBindeableMap(env: Map[String, Any] = Map()): Map[String, Any] =
      initBindVars ++ (if (parent == null) Map[String, Any]() else {
        val pbm = parent.toBindeableMap()
        (if (transparent) pbm else Map[String, Any]()) + (".." -> pbm)
      }) ++ data ++ env
  }
}
