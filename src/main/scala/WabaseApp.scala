package org.wabase

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.tresql.Resources
import org.wabase.AppMetadata.{Action, AugmentedAppFieldDef, AugmentedAppViewDef}
import org.wabase.AppMetadata.Action.{LimitKey, OffsetKey, OrderKey}

import java.util.Locale
import scala.concurrent.{ExecutionContext, Future}
import scala.language.{existentials, implicitConversions}
import scala.util.{Failure, Success, Try}

trait WabaseApp[User] {
  this:  AppBase[User]
    with DbAccess
    with Authorization[User]
    with DbConstraintMessage
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
    values:     Map[String, Any],
    oldValue:   Map[String, Any] = null, // for save and delete
    serializedResult: Source[ByteString, _] = null,
  )(implicit
    val user:     User,
    val state:    ApplicationState,
    val timeout:  QueryTimeout,
    val ec:       ExecutionContext,
    val as:       ActorSystem
  ) {
    lazy val env: Map[String, Any] = state ++ current_user_param(user)
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
    doApiCheck: Boolean = true,
  )(implicit
    user:     User,
    state:    ApplicationState,
    timeout:  QueryTimeout,
    ec:       ExecutionContext,
    as:       ActorSystem
  ): Future[WabaseResult] = {
    doWabaseAction(AppActionContext(actionName, viewName, keyValues, values ++ params), doApiCheck)
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
      .andThen {
        case Success(WabaseResult(ctx, res)) => afterWabaseAction(ctx, Success(res))
        case Failure(error) => afterWabaseAction(context, Failure[QuereaseResult](error))
      }
      .run
      .flatMap {
        case WabaseResult(ac, QuereaseResultWithCleanup(result, cleanup)) =>
          //do serialization phase if result is based on open database cursor
          val (resultSource, isCollection) = result match {
            case TresqlResult(tr) =>
              (TresqlResultSerializer.source(() => tr), true)
            case TresqlSingleRowResult(row) =>
              (TresqlResultSerializer.rowSource(() => row), false)
            case IteratorResult(ir) =>
              (DtoDataSerializer.source(() => ir), true)
          }
          val addResultToContext = shouldAddResultToContext(context)
          serializeResult(SerializationBufferSize, viewSerializationBufferMaxFileSize(ac.viewName),
            resultSource, cleanup, if (addResultToContext) 2 else 1)
            .map { srs =>
              import context._
              val qsr = QuereaseSerializedResult(srs.head, isCollection)
              val ac =
                if (addResultToContext) srs.tail.head match {
                  case CompleteResult(bs)        => context.copy(serializedResult = Source.single(bs))
                  case IncompleteResultSource(s) => context.copy(serializedResult = s)
                } else context
                WabaseResult(ac, qsr)
            }
        case wr => Future.successful(wr)
      }
  }

  def simpleAction(context: AppActionContext): ActionHandlerResult = {
    import context._
    qe.QuereaseAction(viewName, actionName, values, env)(resourceFactory(context), closeResources)
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

  protected def getOldValue(context: AppActionContext): qe.QuereaseAction[Map[String, Any]] = {
    import context._
    def throwUnexpectedResultClass(qr: QuereaseResult) =
      sys.error(s"Unexpected result class getting old-value for '$actionName' of $viewName: ${qr.getClass.getName}")
    qe.QuereaseAction(viewName, Action.Get, values, env)(resourceFactory(context), closeResources).map {
      case OptionResult(None)         =>  null
      case MapResult(oldMap)          =>  oldMap
      case srr: TresqlSingleRowResult =>  srr.map(qe.toCompatibleMap(_, qe.viewDef(viewName)))
      case OptionResult(Some(old))    =>  old.toMap
      case PojoResult(old)            =>  old.toMap
      case qrwc: QuereaseResultWithCleanup =>
        qrwc.map {
          case srr: TresqlSingleRowResult  => srr.map(qe.toCompatibleMap(_, qe.viewDef(viewName)))
          case x                           => throwUnexpectedResultClass(x)
        }
      case x                          => throwUnexpectedResultClass(x)
    }
  }
  protected def throwOldValueNotFound(message: String, locale: Locale): Nothing =
    throw new org.mojoz.querease.NotFoundException(translate(message)(locale))

  def save(context: AppActionContext): ActionHandlerResult = {
    import context._
    val viewDef = qe.viewDef(viewName)
    val keyAsMap =
      if  (actionName == Action.Update)
           values(qe.oldKeyParamName) match {
             case keyAsMap: Map[String, Any] @unchecked => keyAsMap
             case x => sys.error("Unexpected old key type, expecting Map")
           }
      else prepareKey(viewName, keyValues, actionName)
    Option(keyAsMap)
      .filter(_.nonEmpty)
      .map(_ => getOldValue(context.copy(values = values ++ keyAsMap)))
      .getOrElse(qe.QuereaseAction.value(null: Map[String, Any]))
      .flatMap { oldValue =>
        if (oldValue == null && keyAsMap.nonEmpty && (actionName == Action.Save || actionName == Action.Update))
          throwOldValueNotFound("Record not found, cannot edit", state.locale)
        val richContext = context.copy(oldValue = oldValue)
        val saveable = applyReadonlyValues(viewDef, oldValue, values)
        val saveableContext = richContext.copy(values = saveable)
        validateFields(viewName, saveable)
        customValidations(saveableContext)(state.locale)
        qe.QuereaseAction(viewName, context.actionName, saveable, env)(resourceFactory(context), closeResources)
          .map(WabaseResult(saveableContext, _))
          .recover { case ex => friendlyConstraintErrorMessage(viewDef, throw ex)(state.locale) }
      }
  }

  def delete(context: AppActionContext): ActionHandlerResult = {
    import context._
    getOldValue(context).flatMap { oldValue =>
      if (oldValue == null)
        throwOldValueNotFound("Record not found, cannot delete", state.locale)
      val richContext = context.copy(oldValue = oldValue)
      qe.QuereaseAction(viewName, actionName, values, env)(resourceFactory(richContext), closeResources)
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

  /** Runs {{{src}}} via {{{FileBufferedFlow}}} of {{{bufferSize}}} with {{{maxFileSize}}} to {{{CheckCompletedSink}}} */
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
    result
      .via(FileBufferedFlow.create(bufferSize, maxFileSize))
      .runWith(new ResultCompletionSink(cleanupFun, resultCount))
  }

  /** Converts key value from uri representation to appropriate type.
    * Default implementation also converts "null" to null.
    */
  def prepareKeyValue(field: qe.FieldDef, value: Any): Any =
    if (value == "null") null else qe.convertToType(field.type_, value)
  def prepareKey(viewName: String, keyValues: Seq[Any], actionName: String): Map[String, Any] = {
    val keyFields = qe.viewNameToKeyFields(viewName).filterNot(_.api.excluded)
    if (keyValues.length > 0) {
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

  protected def beforeWabaseAction(
    context:    AppActionContext,
    doApiCheck: Boolean,
  ): AppActionContext = {
    import context._
    if (doApiCheck)
      checkApi(viewName, actionName, user)
    val keyAsMap = prepareKey(viewName, keyValues, actionName)
    val key_params =
      if  (context.actionName == Action.Update)
           Map(qe.oldKeyParamName -> keyAsMap)
      else keyAsMap
    context.copy(values = values ++ key_params)
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
  protected def noApiException(viewName: String, method: String, user: User): Exception =
    new BusinessException(s"$viewName.$method is not a part of this API")
  def checkApi[F](viewName: String, method: String, user: User): Unit = {
    (for {
      view <- viewDefOption(viewName)
      role <- view.apiMethodToRole.get(method).orElse(method match {
        case Action.Insert |
             Action.Update => view.apiMethodToRole.get(Action.Save)
        case x => None
      })
      if hasRole(user, role)
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
