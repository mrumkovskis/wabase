package org.wabase

import org.mojoz.querease.NotFoundException
import org.tresql.Resources
import org.wabase.AppMetadata.{Action, AugmentedAppFieldDef, AugmentedAppViewDef}
import org.wabase.AppMetadata.Action.{LimitKey, OffsetKey, OrderKey}

import scala.collection.immutable.Map
import scala.concurrent.ExecutionContext
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

  val DefaultCp: PoolName = DEFAULT_CP

  type ActionHandlerResult = qe.QuereaseAction[(ActionContext, QuereaseResult)]
  type ActionHandler       = ActionContext => ActionHandlerResult

  case class ActionContext(
    actionName: String,
    viewName:   String,
    values:     Map[String, Any],
    oldValue:   Map[String, Any] = null, // for save and delete
  )(implicit
    val user:     User,
    val state:    ApplicationState,
    val timeout:  QueryTimeout,
    val ec:       ExecutionContext,
  )

  protected def getActionHandler(context: ActionContext): ActionHandler = {
    import context._
    actionName match {
      case Action.List    => list
      case Action.Save    => save
      case Action.Delete  => delete
      case x              => simpleAction
    }
  }

  def initViewResources(viewName: String)(actionName: String): Resources = {
    val vdo = viewDefOption(viewName)
    val poolName = vdo.flatMap(v => Option(v.cp)).map(PoolName) getOrElse DefaultCp
    val extraDbs = extraDb(vdo.map(_.actionToDbAccessKeys(actionName).toList).getOrElse(Nil))
    initResources(tresqlResources.resourcesTemplate)(poolName, extraDbs)
  }

  def doWabaseAction(
    actionName: String,
    viewName:   String,
    values:     Map[String, Any] = Map(),
  )(implicit
    user:     User,
    state:    ApplicationState,
    timeout:  QueryTimeout,
    ec:       ExecutionContext,
  ): ActionHandlerResult = {
    doWabaseAction(ActionContext(actionName, viewName, values))
  }

  def doWabaseAction(context: ActionContext): ActionHandlerResult =
    doWabaseAction(getActionHandler(context), context)

  def doWabaseAction(
    action:   ActionHandler,
    context:  ActionContext,
  ): ActionHandlerResult = {
    val actionContext = beforeWabaseAction(context)
    import context.ec
    action(actionContext)
      .andThen { case Success((ctx, res)) => afterWabaseAction(ctx, res) }
  }

  def beforeWabaseAction(context: ActionContext): ActionContext = {
    import context._
    checkApi(viewName, actionName, user)
    val trusted = state ++ values ++ current_user_param(user)
    context.copy(values = trusted)
  }

  def afterWabaseAction(context: ActionContext, result: QuereaseResult): Unit = {
    audit(context, result)
  }

  def simpleAction(context: ActionContext): ActionHandlerResult = {
    import context._
    qe.QuereaseAction(viewName, actionName, values, Map())(initViewResources, closeResources)
      .map(context -> _)
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
        qe.QuereaseAction(viewName, actionName, saveable, Map.empty)(initViewResources, closeResources)
          .map(saveableContext -> _)
          .recover { case ex => friendlyConstraintErrorMessage(viewDef, throw ex)(state.locale) }
      }
      qr match {
        case MapResult(oldValue) =>
          insertOrUpdate(oldValue = oldValue)
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
          .map(richContext -> _)
          .recover { case ex => friendlyConstraintErrorMessage(throw ex)(state.locale) }
      }
      qr match {
        case MapResult(oldValue) =>
          delete(oldValue = oldValue)
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

  protected def applyReadonlyValues(
    viewDef: ViewDef, old: Map[String, Any], instance: Map[String, Any]
  ): Map[String, Any] = {
    // overwrite incoming values of non-updatable fields with old values from db
    // FIXME for lookups and children?
    val fieldsToCopy =
      if (old == null) Nil
      else viewDef.fields.filter(f => !qe.authFieldNames.contains(f.name)).filterNot(_.api.updatable)
    if (fieldsToCopy.nonEmpty) {
      val fieldsToCopyNames = fieldsToCopy.map(f => Option(f.alias) getOrElse f.name).toSet
      instance ++ old.filter { case (k, v) => fieldsToCopyNames.contains(k) }
    } else
      instance
  }


  def stableOrderBy(viewDef: ViewDef, orderBy: String): String = {
    if (orderBy != null && orderBy != "" && viewDef.orderBy != null && viewDef.orderBy.size > 0) {
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
  protected def checkApi[F](viewName: String, method: String, user: User): Unit =
    (for {
      view <- viewDefOption(viewName)
      role <- view.apiMethodToRole.get(method)
      if hasRole(user, role)
    } yield true).getOrElse(
      throw noApiException(viewName, method, user)
    )
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
        viewDef.fields.filter(_.sortable).map(n => Option(n.alias).getOrElse(n.name)).toSet
      val sortCols = orderBy.replace("~", "").split("[\\s\\,]+").toList
      val notSortable = sortCols.filterNot(sortableFields.contains)
      if (notSortable.nonEmpty)
        throw new BusinessException(s"Not sortable: ${viewDef.name} by " + notSortable.mkString(", "), null)
    }
  }
}
