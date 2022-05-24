package org.wabase

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.mojoz.querease.NotFoundException
import org.tresql.{Resources, RowLike}
import org.wabase.AppMetadata.{Action, AugmentedAppFieldDef, AugmentedAppViewDef}
import org.wabase.AppMetadata.Action.{LimitKey, OffsetKey, OrderKey}

import scala.collection.immutable.Map
import scala.concurrent.{ExecutionContext, Future}
import scala.language.{existentials, implicitConversions}
import scala.util.Success

trait WabaseApp[User] {
  this:  AppBase[User]
    with Audit[User]
    with DbAccess
    with Authorization[User]
    with ValidationEngine
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
  type ActionHandler       = ActionContext => ActionHandlerResult

  case class ActionContext(
    actionName: String,
    viewName:   String,
    key:        Seq[Any],
    values:     Map[String, Any],
    oldValue:   Map[String, Any] = null, // for save and delete
  )(implicit
    val user:     User,
    val state:    ApplicationState,
    val timeout:  QueryTimeout,
    val ec:       ExecutionContext,
    val as:       ActorSystem
  )

  case class WabaseResult(ctx: ActionContext, result: QuereaseResult)

  def doWabaseAction(
    actionName: String,
    viewName:   String,
    key:        Seq[Any],
    values:     Map[String, Any] = Map(),
  )(implicit
    user:     User,
    state:    ApplicationState,
    timeout:  QueryTimeout,
    ec:       ExecutionContext,
    as:       ActorSystem
  ): Future[WabaseResult] = {
    doWabaseAction(ActionContext(actionName, viewName, key, values))
  }

  def doWabaseAction(context: ActionContext): Future[WabaseResult] =
    doWabaseAction(getActionHandler(context), context)

  def doWabaseAction(
    action:   ActionHandler,
    context:  ActionContext,
  ): Future[WabaseResult] = {
    val actionContext = beforeWabaseAction(context)
    import context.ec
    import context.as
    action(actionContext)
      .andThen { case Success(WabaseResult(ctx, res)) => afterWabaseAction(ctx, res) }
      .run
      .flatMap {
        case WabaseResult(ac, QuereaseResultWithCleanup(result, cleanup)) =>
          //do serialization phase if result is based on open database cursor
          val (resultSource, isCollection) = result match {
            case TresqlResult(tr) =>
              (TresqlResultSerializer.source(() => tr), true)
            case TresqlSingleRowResult(row) =>
              (TresqlResultSerializer.source(() => row), false)
            case IteratorResult(ir) =>
              (DtoDataSerializer.source(() => ir), true)
          }
          serializeResult(SerializationBufferSize, viewSerializationBufferMaxFileSize(ac.viewName),
            resultSource, isCollection, cleanup)
            .map(WabaseResult(ac, _))
        case wr => Future.successful(wr)
      }
  }

  def simpleAction(context: ActionContext): ActionHandlerResult = {
    import context._
    qe.QuereaseAction(viewName, actionName, values, Map())(initViewResources, closeResources)
      .map(WabaseResult(context, _))
  }

  def list(context: ActionContext): ActionHandlerResult = {
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

  def save(context: ActionContext): ActionHandlerResult = {
    import context._
    val viewDef = qe.viewDef(viewName)
    val keyMap = qe.extractKeyMap(viewName, values)
    val oldValueResult =
      Option(keyMap)
        .filter(_.nonEmpty)
        .map(_ => qe.QuereaseAction(viewName, Action.Get, values, Map.empty)(initViewResources, closeResources))
        .getOrElse(qe.QuereaseAction.value(OptionResult(None)))
    oldValueResult.flatMap { qr =>
      def insertOrUpdate(oldValue: Map[String, Any]) = {
        val richContext = context.copy(oldValue = oldValue)
        val saveable = applyReadonlyValues(viewDef, oldValue, values)
        val saveableContext = richContext.copy(values = saveable)
        validateFields(viewName, saveable)
        validate(viewName, saveable)(state.locale)
        qe.QuereaseAction(viewName, Action.Save, saveable, Map.empty)(initViewResources, closeResources)
          .map(WabaseResult(saveableContext, _))
          .recover { case ex => friendlyConstraintErrorMessage(viewDef, throw ex)(state.locale) }
      }
      qr match {
        case MapResult(oldValue) =>
          insertOrUpdate(oldValue = oldValue)
        case TresqlSingleRowResult(row) =>
          insertOrUpdate(oldValue = row.toMap)
        case OptionResult(oldOpt) =>
          if (keyMap.nonEmpty && oldOpt.isEmpty)
            throw new BusinessException(translate("Record not found, cannot edit")(state.locale))
          insertOrUpdate(oldValue = oldOpt.map(_.toMap).orNull)
        case PojoResult(oldValue) =>
          insertOrUpdate(oldValue = oldValue.toMap)
        case x => sys.error(s"Unexpected querease result class: ${x.getClass.getName}")
      }
    }
  }

  def delete(context: ActionContext): ActionHandlerResult = {
    import context._
    qe.QuereaseAction(viewName, Action.Get, values, Map.empty)(initViewResources, closeResources).flatMap { qr =>
      def delete(oldValue: Map[String, Any]) = {
        val richContext = context.copy(oldValue = oldValue)
        qe.QuereaseAction(viewName, actionName, values, Map.empty)(initViewResources, closeResources)
          .map(WabaseResult(richContext, _))
          .recover { case ex => friendlyConstraintErrorMessage(throw ex)(state.locale) }
      }
      qr match {
        case MapResult(oldValue) =>
          delete(oldValue = oldValue)
        case TresqlSingleRowResult(row) =>
          delete(oldValue = row.toMap)
        case OptionResult(None) => throw new NotFoundException(
          s"Record not found, cannot delete. View name: $viewName, values: $values")
        case OptionResult(oldOpt) =>
          delete(oldValue = oldOpt.map(_.toMap).orNull)
        case PojoResult(oldValue) =>
          delete(oldValue = oldValue.toMap)
        case x => sys.error(s"Unexpected querease result class: ${x.getClass.getName}")
      }
    }
  }

  protected def getActionHandler(context: ActionContext): ActionHandler = {
    import context._
    actionName match {
      case Action.List    => list
      case Action.Save    => save
      case Action.Insert  => save
      case Action.Update  => save
      case Action.Delete  => delete
      case x              => simpleAction
    }
  }

  protected def initViewResources(viewName: String)(actionName: String): Resources = {
    val vdo = viewDefOption(viewName)
    val poolName = vdo.flatMap(v => Option(v.cp)).map(PoolName) getOrElse DefaultCp
    val extraDbs = extraDb(vdo.map(_.actionToDbAccessKeys(actionName).toList).getOrElse(Nil))
    initResources(tresqlResources.resourcesTemplate)(poolName, extraDbs)
  }

  /** Runs {{{src}}} via {{{FileBufferedFlow}}} of {{{bufferSize}}} with {{{maxFileSize}}} to {{{CheckCompletedSink}}} */
  def serializeResult(
    bufferSize: Int,
    maxFileSize: Long,
    result: Source[ByteString, _],
    isCollection: Boolean,
    cleanupFun: Option[Throwable] => Unit = null,
  )(implicit
    ec: ExecutionContext,
    mat: Materializer,
  ): Future[QuereaseSerializedResult] = {
    result
      .via(FileBufferedFlow.create(bufferSize, maxFileSize))
      .runWith(new ResultCompletionSink(cleanupFun))
      .map(QuereaseSerializedResult(_, isCollection))
  }

  protected def prepareKey(context: ActionContext): Map[String, Any] = {
    import context._
    val keyFields = qe.viewNameToKeyFields(viewName)
    if (key.length > 0) {
      if (key.length != keyFields.length)
        throw new BusinessException(
          s"Unexpected key length for $actionName of $viewName - expecting ${keyFields.length}, got ${key.length}")
      else
        keyFields.zip(key).map { case (f, v) => f.fieldName -> qe.convertToType(f.type_, v) }.toMap
    } else Map.empty
  }

  protected def beforeWabaseAction(context: ActionContext): ActionContext = {
    import context._
    checkApi(viewName, actionName, user)
    val preparedKey = prepareKey(context)
    val key_params =
      if  (context.actionName == Action.Update)
           Map("_old_key" -> preparedKey)
      else preparedKey
    val onSaveParams =
      if  (context.actionName == Action.Insert ||
           context.actionName == Action.Update
          ) Map(qe.onSaveDoInsertOrUpdateKey(context.viewName) -> context.actionName)
      else  Map.empty
    val trusted = state ++ values ++ key_params ++ onSaveParams ++ current_user_param(user)
    context.copy(values = trusted)
  }

  protected def afterWabaseAction(context: ActionContext, result: QuereaseResult): Unit = {
    audit(context, result)
  }

  protected def applyReadonlyValues(
    viewDef: ViewDef, old: Map[String, Any], instance: Map[String, Any]
  ): Map[String, Any] = {
    // overwrite incoming values of non-updatable fields with old values from db
    // FIXME for lookups and children?
    val fieldsToCopy =
      if (old == null) Nil
      else viewDef.fields.filter(f => !qe.authFieldNames.contains(f.name)).filterNot(_.api.updatable)
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
  protected def checkApi[F](viewName: String, method: String, user: User): Unit = {
    val checkApiMethod = method match { // TODO add insert, update to api methods?
      case Action.Insert |
           Action.Update => Action.Save
      case x => x
    }
    (for {
      view <- viewDefOption(viewName)
      role <- view.apiMethodToRole.get(checkApiMethod)
      if hasRole(user, role)
    } yield true).getOrElse(
      throw noApiException(viewName, checkApiMethod, user)
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
}

object WabaseAppConfig extends AppBase.AppConfig {
  val DefaultCp: PoolName = DEFAULT_CP
  val SerializationBufferSize: Int =
    if (appConfig.hasPath("serialization-buffer-size"))
      appConfig.getInt("serialization-buffer-size")
    else 1024 * 32

  val SerializationBufferMaxFileSize: Long = MarshallingConfig.dbDataFileMaxSize
  val SerializationBufferMaxFileSizes: Map[String, Long] = MarshallingConfig.customDataFileMaxSizes
}
