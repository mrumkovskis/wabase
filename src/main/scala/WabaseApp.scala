package org.wabase

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpRequest, StatusCodes}
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Source}
import akka.util.ByteString
import org.mojoz.metadata.{FieldDef, ViewDef}
import org.mojoz.querease.FieldFilter
import org.tresql.{Resources, SingleValueResult}
import org.wabase.AppMetadata.{Action, AugmentedAppFieldDef, AugmentedAppViewDef}
import org.wabase.AppMetadata.Action.{LimitKey, OffsetKey, OrderKey}

import java.util.Locale
import scala.concurrent.{ExecutionContext, Future}
import scala.language.{existentials, implicitConversions}
import scala.util.{Failure, Success, Try}

trait WabaseApp[User] {
  this:  AppBase[User]
    with Audit[User]
    with Authorization[User]
    with DbAccess
    with Authorization[User]
    with DbConstraintMessage
    with ValidationEngine
    =>

  import qe.viewDefOption

  val DefaultCp: PoolName = WabaseAppConfig.DefaultCp
  val SerializationBufferSize: Int = WabaseAppConfig.SerializationBufferSize
  val SerializationBufferMaxFileSize: Long = WabaseAppConfig.SerializationBufferMaxFileSize
  val SerializationBufferMaxFileSizes: Map[String, Long] = WabaseAppConfig.SerializationBufferMaxFileSizes

  def viewSerializationBufferMaxFileSize(viewName: String): Long =
    SerializationBufferMaxFileSizes.getOrElse(viewName, SerializationBufferMaxFileSize)

  type ActionHandlerResult = qe.QuereaseAction[WabaseResult]
  type ActionHandler       = AppActionContext => ActionHandlerResult

  case class AppActionContext(
    actionName: String,
    viewName:   String,
    keyValues:  Seq[Any],
    params:     Map[String, Any],
    values:     Map[String, Any],
    resultFilter: ResultRenderer.ResultFilter = null,
    oldValue:   Map[String, Any] = null, // for save and delete
    serializedResult: Source[ByteString, _] = null,
  )(implicit
    val user:     User,
    val state:    ApplicationState,
    val timeout:  QueryTimeout,
    val ec:       ExecutionContext,
    val as:       ActorSystem,
    val appFs:    AppFileStreamer[User],
    val req:      HttpRequest,
  ) {
    lazy val env: Map[String, Any] = state ++ current_user_param(user)
    val fileStreamer = if (appFs == null) null else appFs.fileStreamer
    def withResultFilter(resFil: ResultRenderer.ResultFilter): AppActionContext =
      copy(resultFilter = resFil)
  }

  protected def fieldFilter(context: AppActionContext): FieldFilter = {
    def fieldFilter(resultFilter: ResultRenderer.ResultFilter): FieldFilter = {
      if (resultFilter == null) null else new FieldFilter {
        override def shouldQuery(field: String) = resultFilter.shouldRender(field)
        override def childFilter(field: String) = fieldFilter(resultFilter.childFilter(field))
      }
    }
    fieldFilter(context.resultFilter)
  }

  case class WabaseResult(ctx: AppActionContext, result: QuereaseResult)

  /** Override to request serialized result source in AppActionContext, e.g., for auditing */
  protected def shouldAddResultToContext(context: AppActionContext): Boolean = false

  def doWabaseAction(
    actionName: String,
    viewName:   String,
    keyValues:  Seq[Any],
    params:     Map[String, Any],
    values:     Map[String, Any] = Map(),
    resultFilter: ResultRenderer.ResultFilter = null,
    doApiCheck: Boolean = true,
  )(implicit
    user:     User,
    state:    ApplicationState,
    timeout:  QueryTimeout,
    ec:       ExecutionContext,
    as:       ActorSystem,
    appFs:    AppFileStreamer[User],
    req:      HttpRequest,
  ): Future[WabaseResult] = {
    doWabaseAction(
      AppActionContext(actionName, viewName, keyValues, params, values ++ params, resultFilter),
      doApiCheck)
  }

  def doWabaseAction(
    context:    AppActionContext,
    doApiCheck: Boolean,
  ): Future[WabaseResult] =
    doWabaseAction(getActionHandler(context), context, doApiCheck)

  def doWabaseAction(
    action:     ActionHandler,
    context:    AppActionContext,
    doApiCheck: Boolean,
  ): Future[WabaseResult] = {
    val actionContext = beforeWabaseAction(context, doApiCheck)
    import context.ec
    import context.as
    action(actionContext)
      .run
      .flatMap {
        case WabaseResult(ac, QuereaseResultWithCleanup(result, cleanup)) =>
          sealed trait Res
          case class SourceRes(src: Source[ByteString, _],
                               filter: ResultRenderer.ResultFilter,
                               isCollection: Boolean) extends Res
          case class StrictRes(result: QuereaseResult) extends Res
          def res(r: QuereaseResult, filter: ResultRenderer.ResultFilter): Res = {
            def single_res(sr: SingleValueResult[_]) = sr.value match {
              case m: Map[String@unchecked, _] => SourceRes(DataSerializer.source(() => Seq(m).iterator), filter, false)
              case s: Seq[Map[String, _]@unchecked] => SourceRes(DataSerializer.source(() => s.iterator), filter, true)
              case x => StrictRes(StringResult(String.valueOf(x)))
            }
            r match {
              case TresqlResult(tr) => tr match {
                case r: SingleValueResult[_] => single_res(r)
                case _ => SourceRes(TresqlResultSerializer.source(() => tr), filter, true)
              }
              case TresqlSingleRowResult(row) => row match {
                case r: SingleValueResult[_] => single_res(r)
                case _ => SourceRes(TresqlResultSerializer.rowSource(() => row), filter, false)
              }
              case IteratorResult(ir) =>
                SourceRes(DataSerializer.source(() => ir), filter, true)
              case CompatibleResult(dr: QuereaseCloseableResult, filter, _) => res(dr, filter)
              case x => StrictRes(x)
            }
          }
          res(result, null) match {
            case SourceRes(resultSource, filter, isCollection) =>
              val addResultToContext = shouldAddResultToContext(context)
              serializeResult(SerializationBufferSize, viewSerializationBufferMaxFileSize(ac.viewName),
                resultSource, cleanup, if (addResultToContext) 2 else 1)
                .map { srs =>
                  val qsr = QuereaseSerializedResult(srs.head, filter, isCollection)
                  import context._
                  val ac =
                    if (addResultToContext) srs.tail.head match {
                      case CompleteResult(bs) => context.copy(serializedResult = Source.single(bs))
                      case IncompleteResultSource(s) => context.copy(serializedResult = s)
                    } else context
                  WabaseResult(ac, qsr)
                }
            case StrictRes(strictRes) =>
              cleanup(None)
              Future.successful(WabaseResult(ac, strictRes))
          }
        case wr => Future.successful(wr)
      }
      .andThen {
        case Success(WabaseResult(ctx, res)) => this.afterWabaseAction(ctx, Success(res))
        case Failure(error) => this.afterWabaseAction(context, Failure[QuereaseResult](error))
      }
  }

  def simpleAction(context: AppActionContext): ActionHandlerResult = {
    import context._
    val rf = ResourcesFactory(resourceFactory(context), closeResources)(tresqlResources.resourcesTemplate)
    qe.QuereaseAction(viewName, actionName, values, env, fieldFilter(context))(rf, fileStreamer, req, qio)
      .map(WabaseResult(context, _))
  }

  def list(context: AppActionContext): ActionHandlerResult = {
    import context._
    val offset  = values.get(OffsetKey).map(_.toString.toInt) getOrElse 0
    val limit   = values.get(LimitKey ).map(_.toString.toInt) getOrElse 0
    val orderBy = values.get(OrderKey ).map(_.toString).orNull
    val viewDef = qe.viewDef(viewName)
    checkLimit(viewDef, limit)
    checkOffset(viewDef, offset)
    checkOrderBy(viewDef, orderBy)
    val forcedLimit =
      Option(limit).filter(_ > 0) getOrElse Option(viewDef.limit).filter(_ > 0).map(_ + 1).getOrElse(0)
    val trustedLimitOffsetOrderBy = Map(
      OffsetKey -> offset,
      LimitKey  -> forcedLimit,
      OrderKey  -> stableOrderBy(viewDef, orderBy),
    ).filterNot(_._2 == null)
    val trusted = values ++ trustedLimitOffsetOrderBy ++ current_user_param(user)
    simpleAction(context.copy(values = trusted))
  }

  protected def maybeGetOldValue(context: AppActionContext): qe.QuereaseAction[Map[String, Any]] = {
    qe.QuereaseAction.value(context.values)
  }

  /** In subclass can place this method call in {{{maybeGetOldValue}}} method */
  protected def getOldValue(context: AppActionContext): qe.QuereaseAction[Map[String, Any]] = {
    import context._
    def throwUnexpectedResultClass(qr: QuereaseResult) =
      sys.error(s"Unexpected result class getting old-value for '$actionName' of $viewName: ${qr.getClass.getName}")
    def oldVal(ov: QuereaseResult): Map[String, Any] = ov match {
      case StatusResult(StatusCodes.NotFound.intValue, _) => null
      case MapResult(oldMap) => oldMap
      case srr: TresqlSingleRowResult => srr.map(qe.toCompatibleMap(_, qe.viewDef(viewName)))
      case CompatibleResult(r, _, _) => oldVal(r)
      case qrwc: QuereaseResultWithCleanup => qrwc map oldVal
      case x => throwUnexpectedResultClass(x)

    }
    val fFilter = fieldFilter(context)
    val rf = ResourcesFactory(resourceFactory(context), closeResources)(tresqlResources.resourcesTemplate)
    qe.QuereaseAction(viewName, Action.Get, values, env, fFilter)(rf, fileStreamer, req, qio).map(oldVal)
  }
  protected def throwOldValueNotFound(message: String, locale: Locale): Nothing =
    throw new org.mojoz.querease.NotFoundException(translate(message)(locale))

  def save(context: AppActionContext): ActionHandlerResult = {
    import context._
    val viewDef = qe.viewDef(viewName)
    val keyAsMap =
      if  (actionName == Action.Update)
           values.getOrElse(qe.oldKeyParamName, Map.empty: Map[String, Any]) match {
             case keyAsMap: Map[String, Any] @unchecked => keyAsMap
             case x => sys.error("Unexpected old key type, expecting Map")
           }
      else prepareKey(viewName, keyValues, actionName)
    Option(keyAsMap)
      .filter(_.nonEmpty)
      .map(_ => maybeGetOldValue(context.copy(values = keyAsMap)))
      .getOrElse(qe.QuereaseAction.value(null: Map[String, Any]))
      .flatMap { oldValue =>
        val richContext = context.copy(oldValue = oldValue)
        val saveable = applyReadonlyValues(viewDef, oldValue, values)
        val saveableContext = richContext.copy(values = saveable)
        validateFields(viewName, saveable)
        this.customValidations(saveableContext)(state.locale)
        val rf = ResourcesFactory(resourceFactory(context), closeResources)(tresqlResources.resourcesTemplate)
        val fFilter = fieldFilter(context)
        qe.QuereaseAction(viewName, context.actionName, saveable, env, fFilter)(rf, fileStreamer, req, qio)
          .map(WabaseResult(saveableContext, _))
          .recover { case ex => friendlyConstraintErrorMessage(viewDef, throw ex)(state.locale) }
      }
  }

  def delete(context: AppActionContext): ActionHandlerResult = {
    import context._
    maybeGetOldValue(context).flatMap { oldValue =>
      val richContext = context.copy(oldValue = oldValue)
      val rf = ResourcesFactory(resourceFactory(richContext), closeResources)(tresqlResources.resourcesTemplate)
      qe.QuereaseAction(viewName, actionName, values, env, fieldFilter(context))(rf, fileStreamer, req, qio)
        .map(WabaseResult(richContext, _))
        .recover { case ex => friendlyConstraintErrorMessage(throw ex)(state.locale) }
    }
  }

  protected def getActionHandler(context: AppActionContext): ActionHandler = {
    import context._
    actionName match {
      case Action.List    => list
      case Action.Save    => save
      case Action.Insert  => save
      case Action.Update  => save
      case Action.Upsert  => save
      case Action.Delete  => delete
      case x              => simpleAction
    }
  }

  def resourceFactory(context: AppActionContext): () => Resources = {
    import context.{actionName, viewName}
    val vdo = viewDefOption(viewName)
    val poolName = vdo.flatMap(v => Option(v.cp)).map(PoolName) getOrElse DefaultCp
    val extraDbs = extraDb(vdo.map(_.actionToDbAccessKeys(actionName).toList).getOrElse(Nil))
    () => initResources(tresqlResources.resourcesTemplate)(poolName, extraDbs)
  }

  /** Runs {{{src}}} via {{{FileBufferedFlow}}} of {{{bufferSize}}} with {{{maxFileSize}}} to {{{CheckCompletedSink}}}
    * On FileBufferedFlow upstream finish calss cleanup function.
    * */
  def serializeResult(
    bufferSize: Int,
    maxFileSize: Long,
    result: Source[ByteString, _],
    cleanupFun: Option[Throwable] => Unit = null,
    resultCount: Int = 1,
  )(implicit
    ec: ExecutionContext,
    mat: Materializer,
  ): Future[Seq[SerializedResult]] = {
    val (cleanupF, resultF) =
      result
        .viaMat(FileBufferedFlow.create(bufferSize, maxFileSize))(Keep.right) // keep flow's materialized value
        .toMat(new ResultCompletionSink(resultCount))(Keep.both)
        .run()
    if (cleanupFun != null)
      cleanupF.onComplete { // materialized future of file buffered flow completes when flow's upstream finishes, then call cleanup fun
        case Failure(ex) => cleanupFun(Some(ex))
        case _ => cleanupFun(None)
      }
    resultF
  }

  /** Converts key value from uri representation to appropriate type.
    * Default implementation also converts "null" to null.
    */
  def prepareKeyValue(field: FieldDef, value: Any): Any =
    if (value == "null") null else qe.convertToType(field.type_, value)
  def prepareKey(viewName: String, keyValues: Seq[Any], actionName: String): Map[String, Any] = {
    val keyFields = qe.viewNameToKeyFields(viewName).filterNot(_.api.excluded)
    if (keyValues.nonEmpty) {
      if (keyValues.length != keyFields.length)
        throw new BusinessException(
          s"Unexpected key length for $actionName of $viewName - expecting ${keyFields.length}, got ${keyValues.length}")
      else
        keyFields.zip(keyValues).map { case (f, v) =>
          try f.fieldName -> prepareKeyValue(f, v)
          catch {
            case util.control.NonFatal(ex) => throw new BusinessException(
              s"Failed to convert value for key field ${f.name} to type ${f.type_.name}", ex)
          }
        }.toMap
    } else Map.empty
  }


  protected def addResultFilter(context: AppActionContext): AppActionContext = {
    if (context.resultFilter != null) context
    else context.actionName match {
      case Action.Get | Action.List | Action.Create =>
        val allowed = context.params.get("cols").map {
          case null => null
          case seq: Seq[_] => seq.map(_.toString).toSet
          case cols => s"$cols".split(",").map(_.trim).toSet
        }.orNull
        logger.info(s"Adding result filter. allowed: ${allowed}")
        if (allowed != null) {
          class ColsFilter(viewName: String, nameToViewDef: Map[String, ViewDef])
            extends ResultRenderer.ViewFieldFilter(viewName, nameToViewDef) {
            override def shouldRender(field: String) =
              allowed.contains(field) && super.shouldRender(field)
            override def childFilter(field: String) = viewDef.fieldOpt(field)
              .map(_.type_.name)
              .map(new ColsFilter(_, nameToViewDef))
              .orNull
          }
          context.withResultFilter(new ColsFilter(context.viewName, qe.nameToViewDef))
        } else context
      case _ => context
    }
  }

  protected def beforeWabaseAction(
    context:    AppActionContext,
    doApiCheck: Boolean,
  ): AppActionContext = {
    import context._
    if (doApiCheck)
      checkApi(viewName, actionName, user, keyValues)
    val keyAsMap = prepareKey(viewName, keyValues, actionName)
    val key_params =
      if  (context.actionName == Action.Update && keyAsMap != null && keyAsMap.nonEmpty)
           Map(qe.oldKeyParamName -> keyAsMap)
      else keyAsMap
    addResultFilter(
      context.copy(values = values ++ key_params)
    )
  }

  protected def afterWabaseAction(context: AppActionContext, result: Try[QuereaseResult]): Unit = {}

  protected def applyReadonlyValues(
    viewDef: ViewDef, old: Map[String, Any], instance: Map[String, Any]
  ): Map[String, Any] = {
    // overwrite incoming values of non-updatable fields with old values from db
    // FIXME for lookups and children?
    val fieldsToCopy =
      if (old == null) Nil
      else viewDef.fields.filterNot(_.api.updatable)
    if (fieldsToCopy.nonEmpty) {
      val fieldsToCopyNames = fieldsToCopy.map(_.fieldName).toSet
      instance ++ old.filter { case (k, v) => fieldsToCopyNames.contains(k) }
    } else
      instance
  }

  protected def stableOrderBy(viewDef: ViewDef, orderBy: String): String = {
    if (orderBy != null && orderBy != "" && viewDef.orderBy != null && viewDef.orderBy.nonEmpty) {
      val forcedSortCols = orderBy.replace("~", "").split("[\\s\\,]+").toSet
      Option(viewDef.orderBy)
        .map(_.filter(c => c != null && c != "" && !forcedSortCols.contains(c.replace("~", ""))))
        .filter(_.nonEmpty)
        .map(s => (orderBy :: s.toList).mkString(", "))
        .getOrElse(orderBy)
    } else orderBy
  }
  private val ident = "[_\\p{IsLatin}][_\\p{IsLatin}0-9]*"
  private val qualifiedIdent = s"$ident(\\.$ident)*"
  private val validViewNameRegex = s"^$qualifiedIdent$$".r
  protected def noApiException(viewName: String, method: String, user: User): Exception =
    if (validViewNameRegex.pattern.matcher(viewName).matches())
         new BusinessException(s"$viewName.$method is not a part of this API")
    else new BusinessException(s"Strange name.$method is not a part of this API")
  def checkApi[F](viewName: String, method: String, user: User, keyValues: Seq[Any]): Unit = {
    (for {
      view <- viewDefOption(viewName)
      roles <- view.apiMethodToRoles.get(method).orElse(method match {
        case Action.Insert |
             Action.Update => view.apiMethodToRoles.get(Action.Save)
        case x => None
      })
      if hasRole(user, roles)
    } yield true).getOrElse(
      throw noApiException(viewName, method, user)
    )
  }
  protected def checkLimit(viewDef: ViewDef, limit: Int): Unit = {
    val maxLimitForView = viewDef.limit
    if (maxLimitForView > 0 && limit > maxLimitForView)
      throw new BusinessException(
        s"limit $limit exceeds max limit allowed for ${viewDef.name}: $maxLimitForView")
  }
  protected def checkOffset(viewDef: ViewDef, offset: Int): Unit =
    if (offset < 0)
      throw new BusinessException("offset must not be negative")
  protected def checkOrderBy(viewDef: ViewDef, orderBy: String): Unit = {
    if (orderBy != null) {
      val sortableFields =
        viewDef.fields.filter(_.sortable).map(_.fieldName).toSet
      val sortCols = orderBy.replace("~", "").split("[\\s\\,]+").toList
      val notSortable = sortCols.filterNot(sortableFields.contains)
      if (notSortable.nonEmpty)
        throw new BusinessException(s"Not sortable: ${viewDef.name} by " + notSortable.mkString(", "), null)
    }
  }
  protected def customValidations(ctx: AppActionContext)(implicit locale: Locale): Unit = {}
}

object WabaseAppConfig extends AppBase.AppConfig {
  val DefaultCp: PoolName = DEFAULT_CP
  val SerializationBufferSize: Int = appConfig.getBytes("serialization-buffer-size").toInt
  val SerializationBufferMaxFileSize: Long = MarshallingConfig.dbDataFileMaxSize
  val SerializationBufferMaxFileSizes: Map[String, Long] = MarshallingConfig.customDataFileMaxSizes
}
