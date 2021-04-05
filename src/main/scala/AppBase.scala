package org.wabase

import java.util.Locale

import org.mojoz.metadata.Type
import org.mojoz.querease.NotFoundException
import org.mojoz.querease.FilterType
import org.mojoz.querease.FilterType._
import org.tresql._
import spray.json._
import com.typesafe.config.Config

import scala.concurrent.Promise
import scala.language.existentials
import scala.language.implicitConversions
import scala.collection.immutable.TreeMap
import scala.reflect.ManifestFactory
import scala.util.Try
import org.tresql.{Resources, RowLike}
import AppMetadata._
import ValidationEngine.CustomValidationFunctions.is_valid_email

import scala.util.control.NonFatal

object AppBase {
  trait AppConfig {
    lazy val appConfig: Config = config.getConfig("app")
  }
}

case class ApplicationState(state: Map[String, Any], locale: Locale = Locale.getDefault)

trait AppBase[User] extends Loggable with QuereaseProvider with DbAccessProvider with I18n {
  this: DbAccess
    with Authorization[User]
    with ValidationEngine
    with DbConstraintMessage
    with Audit[User] =>

  override def dbAccess = this

  type Dto = qe.DTO
  type DtoWithId = qe.DWI
  type ViewDef = qe.ViewDef

  import qe.{viewDef, viewDefOption, classToViewNameMap, viewNameToClassMap}

  implicit def rowLikeToDto[B <: Dto](r: RowLike, m: Manifest[B]): B = qe.rowLikeToDto(r, m)

  implicit def toAppListResult[T <: Dto: Manifest](list: Seq[T]) = new AppListResult[T] {
    private val iter = list.iterator
    override def resources = ???
    override def view = qe.viewDef[T]
    override protected def hasNextInternal = if (!iter.hasNext) { close; false } else true
    override protected def nextInternal = iter.next()
  }

  trait AppListResult[+T] extends Iterator[T] with AutoCloseable { self =>
    def view: ViewDef
    def resources: Resources
    /** this is to be overriden in subclasses instead of {{{hasNext}}} */
    protected def hasNextInternal: Boolean
    /** this is to be overriden in subclasses instead of {{{next}}} */
    protected def nextInternal: T
    /** Override {{{hasNextInternal}}} method instead of this.
        This method calls {{{hasNextInternal}}} and in the case of non fatal error calls {{{close}}} */
    override final def hasNext = exe(hasNextInternal)
    /** Override {{{nextInternal}}} method instead of this.
        This method calls {{{nextInternal}}} and in the case of non fatal error calls {{{close}}}*/
    override final def next(): T = exe(nextInternal)
    private def exe[A](block: => A): A = try block catch {
      case NonFatal(e) =>
        try close catch { case NonFatal(e) =>
          logger.error("Exception in exception handler - list result close() failed: ", e)
        }
        throw e
    }
    private var onCloseAction: Unit => Unit = identity
    trait Wrapper[+A] extends AppListResult[A] { wrapper =>
      override def view = self.view
      override def resources = self.resources
      override protected def hasNextInternal: Boolean = self.hasNext
      override def close: Unit = self.close
      override def andThen(action: => Unit): AppListResult[A] = {
        self.andThen(action)
        wrapper
      }
    }
    def mapRow[R](f: T => R) = new Wrapper[R] {
      override protected def nextInternal: R = f(self.next())
    }
    def mapRowWithResources[R](f: Resources => T => R) = new Wrapper[R] {
      override protected def nextInternal: R = f(resources)(self.next())
    }
    def andThen(action: => Unit): AppListResult[T] = {
      onCloseAction = onCloseAction andThen (_ => action)
      self
    }
    def close() = onCloseAction(())
  }

  implicit def appStateToMap(state: ApplicationState): Map[String, Any] = state.state
  implicit def mapToAppState(state: Map[String, Any]) = ApplicationState(state)

  sealed trait RequestContext[+T] {
    def user: User
    def inParams: Map[String, Any]
    def state: ApplicationState
    def result: T
    def viewName: String
    def params: Map[String, Any]
  }

  case class ViewContext[+T <: Dto](viewName: String, id: Long,
    inParams: Map[String, Any] = Map(),
    user: User,
    state: ApplicationState = Map[String, Any](), result: Option[T] = null)
    extends RequestContext[Option[T]] {
    val params = state ++ inParams ++ current_user_param(user)
  }

  case class CreateContext[+T <: Dto](viewName: String,
    inParams: Map[String, Any] = Map(),
    user: User,
    state: ApplicationState = Map[String, Any](), result: T = null)
    extends RequestContext[T] {
    val params = state ++ inParams ++ current_user_param(user)
  }

  case class ListContext[+T <: Dto](
    viewName: String,
    inParams: Map[String, Any] = Map(),
    offset: Int = 0,
    limit: Int = 0,
    orderBy: String = null,
    user: User,
    state: ApplicationState = Map[String, Any](),
    completePromise: Promise[Unit],
    doCount: Boolean = false,
    timeoutSeconds: QueryTimeout,
    poolName: PoolName,
    result: AppListResult[T] = null,
    count: Long = -1)
    extends RequestContext[AppListResult[T]] {
    val params = state ++ inParams ++ current_user_param(user)

    def mapRow[R <: Dto](f: T => R) = copy(result = result.mapRow(f))
    def foreachRow(f: T => Unit) = mapRow{t => f(t);t}
    def mapRowWithResources[R <: Dto](f: Resources => T => R) =
      copy(result = result.mapRowWithResources(f))
    def foreachRowWithResources(f: Resources => T => Unit) =
      mapRowWithResources { r => t => { f(r)(t); t } }
    def andThen(action: => Unit) = copy(result = result.andThen(action))
  }

  case class SaveContext[+T <: Dto](
      viewName: String,
      old: T,
      obj: T,
      inParams: Map[String, Any] = Map(),
      user: User,
      completePromise: Promise[Long],
      state: ApplicationState = Map[String, Any](),
      extraPropsToSave: Map[String, Any] = Map(),
      result: Long = -1) extends RequestContext[Long] {
    val params = state ++ inParams ++ current_user_param(user)
  }

  case class RemoveContext[+T <: DtoWithId](
      viewName: String,
      id: Long,
      inParams: Map[String, Any] = Map(),
      user: User,
      completePromise: Promise[Unit],
      state: ApplicationState = Map[String, Any](),
      result: Long = -1,
      old: T = null)
    extends RequestContext[Long] {
    val keyMap = Map("id" -> id)
    val viewDef = qe.viewDef(viewName)
    val params = state ++ inParams ++ keyMap ++ current_user_param(user)
  }

 /** before(), after() and on() methods can be used from business code. */
  /** Names of date or time fields updated automatically on save */
  val autoTimeFieldNames = Set("update_time")
  implicit object View extends HExt[ViewContext[Dto]] {
    override def defaultAction(ctx: ViewContext[Dto]): ViewContext[Dto] =
      defaultView(ctx)
  }
  def defaultView(ctx: ViewContext[Dto]): ViewContext[Dto] = {
    import ctx._
    if (id == -1) {
      //get by name
      qe.list[Dto](params, 0, 2)(
        ManifestFactory.classType(viewNameToClassMap(viewName)), implicitly[Resources]) match {
        case List(result) => ctx.copy(result = Some(result))
        case Nil => ctx.copy(result = None)
        case _ => throw new BusinessException("Too many rows returned")
      }
    } else
      ctx.copy(result = qe.get(id, null, params)(
        ManifestFactory.classType(viewNameToClassMap(viewName)), implicitly[Resources]))
  }
  implicit object Create extends HExt[CreateContext[Dto]] {
    override def defaultAction(ctx: CreateContext[Dto]): CreateContext[Dto] =
      defaultCreate(ctx)
  }
  def defaultCreate(ctx: CreateContext[Dto]): CreateContext[Dto] = {
    import ctx._
    // type annotation to fix strange java.lang.ClassCastException: dto.my_view cannot be cast to scala.runtime.Nothing$
    val result: Dto = qe.create(params)(
      ManifestFactory.classType(viewNameToClassMap(viewName)), tresqlResources)
    ctx.copy(result = result)
  }
  implicit object BList extends HExt[ListContext[Dto]] {
    override def defaultAction(ctx: ListContext[Dto]): ListContext[Dto] =
      defaultList(ctx)
    override def register(
      typ: String,
      mf: Manifest[ListContext[Dto]],
      a: ListContext[Dto] => ListContext[Dto]) = {
      if (typ == "after") {
        val f = (ctx: ListContext[Dto]) => if (!ctx.doCount) a(ctx) else ctx
        super.register(typ, mf, f)
      } else super.register(typ, mf, a)
    }
  }
  def defaultList(ctx: ListContext[Dto]): ListContext[Dto] = {
      import ctx._
      val viewDef = qe.viewDef(viewName)
      if (orderBy != null) {
        // orderBy validation
        val sortableFields =
          viewDef.fields.filter(_.sortable).map(n => Option(n.alias).getOrElse(n.name)).toSet
        val sortCols = orderBy.replace("~", "").split("[\\s\\,]+").toList
        val notSortable = sortCols.filterNot(sortableFields.contains)
        if (notSortable.size > 0)
          throw new BusinessException(s"Not sortable: $viewName by " + notSortable.mkString(", "), null)
      }
      if (ctx.doCount)
        ctx.copy(count = qe.countAll(params)(
          ManifestFactory.classType(viewNameToClassMap(viewName)), tresqlResources))
      else {
        ctx.copy(result = new AppListResult[Dto] {
          private [this] var closed = false
          private val connection = ConnectionPools(poolName).getConnection
          override val resources = tresqlResources
            .withConn(connection)
            .withQueryTimeout(timeoutSeconds.timeoutSeconds)
          private val result: qe.CloseableResult[Dto] =
            try qe.result[Dto](
              params,
              offset,
              limit,
              stableOrderBy(viewDef, orderBy),
              )(
              ManifestFactory.classType(viewNameToClassMap(viewName).asInstanceOf[Class[Dto]]),
              resources)
            catch {
              case NonFatal(e) =>
                close
                throw e
            }
          override def view = viewDef
          override protected def hasNextInternal = if (!result.hasNext) { close; false } else true
          override protected def nextInternal = result.next()
          override def close =
            if (!closed) {
              try super.close finally
                try if (result != null) result.close finally
                  try connection.rollback finally connection.close
              closed = true
            }
        })
      }
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
  implicit object Save extends HExt[SaveContext[Dto]] {
    override def defaultAction(ctx: SaveContext[Dto]): SaveContext[Dto] =
      defaultSave(ctx)
  }
  def defaultSave(ctx: SaveContext[Dto]): SaveContext[Dto] = {
      import ctx._
      if (!ctx.obj.isInstanceOf[org.wabase.DtoWithId])
        throw new RuntimeException(
          "Default save may only be used for instances of DtoWithId, use 'on save' hook to save "
             + ctx.obj.getClass.getName)
      val obj = ctx.obj.asInstanceOf[DtoWithId]
      val viewDef = qe.viewDef(viewName)
      val implicitProps = viewDef.fields
       .map(_.name)
       .filter(autoTimeFieldNames)
       .map(_ -> CommonFunctions.now).toMap
      obj.id = qe.save(
        obj,
        Option(extraPropsToSave).getOrElse(Map.empty) ++ implicitProps,
        false,
        null,
        params
      )(tresqlResources)
      ctx.copy(result = obj.id)
  }
  implicit object Remove extends HExt[RemoveContext[DtoWithId]] {
    override def defaultAction(ctx: RemoveContext[DtoWithId]): RemoveContext[DtoWithId] =
      defaultRemove(ctx)
  }
  def defaultRemove(ctx: RemoveContext[DtoWithId]): RemoveContext[DtoWithId] = {
      import ctx._
      val viewDef = qe.viewDef(viewName)
      val result = qe.delete(old,
        null,
        state ++ ctx.keyMap ++ current_user_param(user))(tresqlResources)
      ctx.copy(result = result.toString.toLong)
  }

  def before(actions: Magnet*) = register("before", actions: _*)
  def after(actions: Magnet*) = register("after", actions: _*)
  def on(actions: Magnet*) = register("on", actions: _*)

  /*
  def chainAndCollectBizEx[T](actions: Magnet[T]*): T => T = {
    def fun(f: (T) => T, p: T, e: Option[Error]) = try f(p) -> e catch {
      case bex: BusinessException =>
        (p, (for (nerr <- Option(bex.error).orElse(Some(Error(List(bex.getMessage)))))
          yield (for (err <- e)
          yield Error(err.messages ++ nerr.messages, err.fieldMessages ++ nerr.fieldMessages))
          getOrElse nerr)
          orElse e)
    }
    val fs = actions map { case m: FunctionMagnet[T] => m.fun }
    val fun1 = fs.tail.foldLeft(Function.tupled(fun(fs.head, _: T, _: Option[Error]))) { (rf, f) =>
      rf andThen Function.tupled(fun(f, _: T, _: Option[Error]))
    }
    (p: T) => fun1(p, None) match {
      case (_, Some(e)) => throw new BusinessException("", null, e)
      case (r, _) => r.asInstanceOf[T]
    }
  }
  */

  private def register(typ: String, actions: Magnet*) = actions foreach (_.register(typ))

  //implicit conversions from functions to magnets
  sealed abstract class Magnet { def register(actionType: String): Unit }
  implicit class FunctionMagnet[T](private[AppBase] val fun: T => T)(
    implicit ext: Ext[T], mf: Manifest[T]) extends Magnet {
    override def register(actionType: String) = ext.asInstanceOf[HExt[T]].register(actionType, mf, fun)
  }
  implicit class VoidFunctionMagnet[T: Ext: Manifest](fun: T => Unit)
    extends FunctionMagnet[T]((x: T) => { fun(x); x })

  sealed abstract class Ext[-A]
  //helper class due to scalac error: contravariant type T occurs in covariant position in type => T => T
  sealed abstract class HExt[T] extends Ext[T] {
    private var actions: Map[(String, Class[_]), T => T] = Map()
    def register(typ: String, mf: Manifest[T], a: T => T) = {
      val clazz = classFromManifest(mf)
      actions += ((typ, clazz) -> actions.get((typ, clazz)).map(_ andThen a).getOrElse(a))
    }
    private def classFromManifest(mf: Manifest[_]) = {
      //assume that manfifest is one of <XXX>Context[T <: Dto] and retrieve Dto subclass
      mf.typeArguments.head.runtimeClass
    }
    protected def defaultAction(ctx: T): T

    def action(clazz: Class[_]): T => T = {
      val before = action("before", clazz)
      val on = action("on", clazz, defaultAction)
      val after = action("after", clazz)
      before andThen on andThen after
    }
    private def action(act: String, clazz: Class[_], defaultAction: T => T = identity,
      boundaryClass: Class[_] = classOf[org.wabase.Dto]): T => T =
      actions.getOrElse(act -> clazz,
        if (clazz.getSuperclass != null && boundaryClass.isAssignableFrom(clazz.getSuperclass))
          action(act, clazz.getSuperclass, defaultAction, boundaryClass) else defaultAction)
  }
  //end persistance hook

  //rest services entry points
  def getRaw(viewName: String, id: Long, params: Map[String, Any] = Map())(
    implicit user: User, state: ApplicationState, timeoutSeconds: QueryTimeout, poolName: PoolName) =
    checkApi(viewName, "get", user) {
      dbUse {
        implicit val clazz = viewNameToClassMap(viewName)
        rest(
          createViewCtx(ViewContext[Dto](viewName, id, params, user, state))
        )
      }
    }

  def get(viewName: String, id: Long, params: Map[String, Any] = Map())(
    implicit user: User, state: ApplicationState, timeoutSeconds: QueryTimeout,
      poolName: PoolName = PoolName(viewDef(viewName).cp)) =
    createViewResult(getRaw(viewName, id, params))

  def createRaw(viewName: String, params: Map[String, Any] = Map.empty)(
    implicit user: User, state: ApplicationState, timeoutSeconds: QueryTimeout, poolName: PoolName) =
    checkApi(viewName, "get", user) {
      dbUse {
        implicit val clazz = viewNameToClassMap(viewName)
        rest(
          createCreateCtx(CreateContext[Dto](viewName, params, user, state))
        )
      }
    }

  def create(viewName: String, params: Map[String, Any] = Map.empty)(
    implicit user: User, state: ApplicationState, timeoutSeconds: QueryTimeout,
      poolName: PoolName = PoolName(viewDef(viewName).cp)) =
    createCreateResult(createRaw(viewName, params))

  def listRaw(
    viewName: String,
    params: Map[String, Any],
    offset: Int = 0,
    limit: Int = 0,
    orderBy: String = null,
    doCount: Boolean = false)(
      implicit user: User,
      state: ApplicationState,
      timeoutSeconds: QueryTimeout,
      poolName: PoolName) =
    checkApi(viewName, "list", user) {
      val maxLimitForView = viewDef(viewName).limit
      if (maxLimitForView > 0 && limit > maxLimitForView)
        throw new BusinessException(
          s"limit $limit exceeds max limit allowed for $viewName: $maxLimitForView")
      if (offset < 0)
        throw new BusinessException("offset must not be negative")
      val forcedLimit = Option(limit).filter(_ > 0) getOrElse Option(maxLimitForView).filter(_ > 0).map(_ + 1).getOrElse(0)
      listInternal(viewName, params, offset, limit, forcedLimit, orderBy, doCount)
    }

  def list(
    viewName: String,
    params: Map[String, Any],
    offset: Int = 0,
    limit: Int = 0,
    orderBy: String = null,
    doCount: Boolean = false)(
      implicit user: User,
      state: ApplicationState,
      timeoutSeconds: QueryTimeout,
      poolName: PoolName = PoolName(viewDef(viewName).cp)) =
    createListResult(listRaw(viewName, params, offset, limit, orderBy, doCount))

  def count(viewName: String, params: Map[String, Any])(
    implicit user: User,
    state: ApplicationState,
    timeoutSeconds: QueryTimeout,
    poolName: PoolName = PoolName(viewDef(viewName).cp)) = checkApi(viewName, "list", user) {
    val result = listInternal(viewName, params, doCount = true)
    createCountResult(result)
  }

  private def listInternal(
    viewName: String,
    params: Map[String, Any],
    offset: Int = 0,
    limit: Int = 0,
    forcedLimit: Int = 0,
    orderBy: String = null,
    doCount: Boolean = false)(
      implicit user: User,
      state: ApplicationState,
      timeoutSeconds: QueryTimeout,
      poolName: PoolName) = {

    implicit val clazz = viewNameToClassMap(viewName)
    dbUse {
      val promise = Promise[Unit]()
      try{
        val result = rest(createListCtx(ListContext[Dto](viewName, params, offset, forcedLimit, orderBy,
          user, state, promise, doCount, timeoutSeconds, poolName)))
        promise.success(())
        result
      }catch{
        case NonFatal(e) =>
          promise.failure(e)
          throw e
      }
    }
  }

  def save(viewName: String, obj: JsObject, params: Map[String, Any] = Map(), emptyStringsToNull: Boolean = true)(
    implicit user: User, state: ApplicationState, timeoutSeconds: QueryTimeout,
      poolName: PoolName = PoolName(viewDef(viewName).cp)) = {
    val instance = qe.fill[Dto](obj)(Manifest.classType(viewNameToClassMap(viewName)))
    saveInternal(viewName, instance, params, emptyStringsToNull)
  }

  def saveDto(instance: Dto, params: Map[String, Any] = Map(), emptyStringsToNull: Boolean = true, extraPropsToSave: Map[String, Any] = Map())(
    implicit user: User, state: ApplicationState, timeoutSeconds: QueryTimeout,
      poolName: PoolName = PoolName(viewDef(classToViewNameMap(instance.getClass)).cp)) = {
    saveInternal(classToViewNameMap(instance.getClass), instance, params, emptyStringsToNull, extraPropsToSave)
  }

  private def saveInternal(
    viewName: String,
    instance: Dto,
    params: Map[String, Any] = Map(),
    emptyStringsToNull: Boolean = true,
    extraPropsToSave: Map[String, Any] = Map()
  )(
    implicit user: User,
    state: ApplicationState,
    timeoutSeconds: QueryTimeout,
    poolName: PoolName
  ) = {
    implicit val clazz = instance.getClass
    val viewDef = qe.viewDef(classToViewNameMap(clazz))
    checkApi(viewName, "save", user) {
      val idOpt = Option(instance)
        .filter(_.isInstanceOf[org.wabase.DtoWithId])
        .map(_.asInstanceOf[DtoWithId])
        .map(_.id)
        .filter(_ != null)
      val old = dbUse{
        validateFields(instance)
        validate(instance)(state.locale)
        idOpt.flatMap { id =>
          rest(ViewContext[DtoWithId](viewName, id, params, user, state)).result
        }.orNull
      }
      if (idOpt.isDefined && old == null)
        throw new BusinessException(translate("Record not found, cannot edit")(state.locale))
      if (old != null)
        // overwrite incoming values of non-updatable fields with old values from db
        // TODO for lookups and children?
        viewDef.fields.filter(f => !qe.authFieldNames.contains(f.name)).filterNot(_.api.updatable) foreach { f =>
          val getterName = Option(f.alias) getOrElse f.name
          val getter = clazz.getMethod(getterName)
          val setter = clazz.getMethod(getterName + "_$eq", getter.getReturnType)
          setter.invoke(instance, getter.invoke(old))
        }
      val promise = Promise[Long]()
      try {
        val res = friendlyConstraintErrorMessage(viewDef, {
          transaction {
            implicit val clazz = instance.getClass
            rest(createSaveCtx(SaveContext(viewName, old, instance, params, user, promise, state, extraPropsToSave = extraPropsToSave)))
          }
        })(state.locale)
        val result = createSaveResult(res)
        promise.success(result)
        result
      } catch {
        case ex: Exception =>
          promise.failure(ex)
          throw ex
      }
    }
  }

  def delete(viewName: String, id: Long, params: Map[String, Any] = Map())(
    implicit user: User, state: ApplicationState, timeoutSeconds: QueryTimeout,
      poolName: PoolName = PoolName(viewDef(viewName).cp)) =
    checkApi(viewName, "delete", user) {
      val promise = Promise[Unit]()
      try {
        val res = friendlyConstraintErrorMessage {
          transaction {
            implicit val clazz = viewNameToClassMap(viewName).asInstanceOf[Class[DtoWithId]]
            val ctx = createDeleteCtx(RemoveContext[DtoWithId](viewName, id, params, user, promise, state))
            qe.get[DtoWithId](id, null, ctx.params)(ManifestFactory.classType(clazz), tresqlResources) match {
              case None => throw new NotFoundException(s"$viewName not found, id: $id, params: $params")
              case Some(oldValue) => rest(ctx.copy(old = oldValue))
            }
          }
        }(state.locale)
        promise.success(())
        createDeleteResult(res)
      } catch {
        case ex: Exception =>
          promise.failure(ex)
          throw ex
      }
    }

  def rest[C <: RequestContext[_]](ctx: C)(implicit mgr: Ext[C], clazz: Class[_]) = auth(ctx, clazz){audit(ctx){
    try mgr.asInstanceOf[HExt[C]].action(clazz)(ctx) catch {
      case e: java.lang.Error =>
        logger.error(s"Error occured! Request context:\n$ctx", e)
        throw e
    }
  }}

  def createSaveCtx[T <: Dto](ctx: SaveContext[T]): SaveContext[T] = ctx
  def createDeleteCtx[T <: DtoWithId](ctx: RemoveContext[T]): RemoveContext[T] = ctx
  def createListCtx[T <: Dto](ctx: ListContext[T]): ListContext[T] = ctx
  def createViewCtx[T <: Dto](ctx: ViewContext[T]): ViewContext[T] = ctx
  def createCreateCtx[T <: Dto](ctx: CreateContext[T]): CreateContext[T] = ctx

  def createListResult[T <: Dto](ctx: ListContext[T]) = ctx.result
  def createCountResult[T <: Dto](ctx: ListContext[T]) = ctx.count
  def createViewResult[T <: Dto](ctx: ViewContext[T]) = ctx.result
  def createCreateResult[T <: Dto](ctx: CreateContext[T]) = ctx.result
  def createSaveResult[T <: Dto](ctx: SaveContext[T]) = ctx.result
  def createDeleteResult[T <: DtoWithId](ctx: RemoveContext[T]) = ctx.result

  lazy val metadataVersionString = org.apache.commons.codec.binary.Base64.encodeBase64URLSafeString(
    java.security.MessageDigest.getInstance("MD5").digest(qe.collectViews{ case v => v }.toList.toString.getBytes))

  def metadata(viewName: String)(implicit user: User, state: ApplicationState): JsObject = {
    metadata(viewDef(viewName))
  }

  def metadata(viewDef: ViewDef)(implicit user: User, state: ApplicationState): JsObject = {
    import qe.{ FieldRefRegexp_ => FieldRefRegexp }
    JsObject(Map(
      "name" -> JsString(viewDef.name),
      "fields" -> JsArray(viewDef.fields.map(f => JsObject(Map(
        "name" -> JsString(Option(f.alias) getOrElse f.name),
        "type" -> JsString(f.type_.name),
        "isCollection" -> JsBoolean(f.isCollection),
        "isComplexType" -> JsBoolean(f.type_.isComplexType),
        "nullable" -> JsBoolean(f.nullable),
        "required" -> JsBoolean(f.required),
        "sortable" -> JsBoolean(f.sortable),
        "label" -> Option(f.label).map(JsString(_)).getOrElse(JsNull),
        "visible" -> JsBoolean(f.visible),
        "insertable" -> JsBoolean(f.api.insertable && !(f.isExpression && f.resolver == null && f.saveTo == null)),
        "updatable" ->  JsBoolean(f.api.updatable  && !(f.isExpression && f.resolver == null && f.saveTo == null)),
        "enum" -> Option(f)
          .filterNot(_.type_.isComplexType)
          .map(_.enum)
          .filter(_ != null)
          .orElse(Option(f)
            .filterNot(_.type_.isComplexType)
            .filter(_.table != null)
            .map(_ => qe.tableMetadata.columnDef(viewDef, f))
            .map(_.enum)
            .filter(_ != null)
          )
          .map(e => JsArray(e.map(JsString(_)): _*)).getOrElse(JsNull),
        "refViewName" -> Option(f.expression)
           .filter(FieldRefRegexp.pattern.matcher(_).matches)
           .map {
             case FieldRefRegexp(refViewName, refFieldName, _, _) => refViewName
           }.map(JsString(_)).getOrElse(JsNull),
        "comments" -> Option(f.comments).map(JsString(_)).getOrElse(JsNull),
        "jsonType" -> JsString(
          qe.typeDefs
            .find(_.name == f.type_.name)
            .flatMap(_.targetNames.get("json"))
            .getOrElse("string")
        ))
        ++
        List(
          "length" -> f.type_.length,
          "totalDigits" -> f.type_.totalDigits,
          "fractionDigits" -> f.type_.fractionDigits)
        .filter(_._2.isDefined)
        .map(x => x._1 -> JsNumber(x._2.get))
        .toMap
        ++
        extraMetadata(f)
      )): _*)) ++
      filterMetadata(viewDef) ++
      extraMetadata(viewDef))
  }
  def extraMetadata(fieldDef: qe.FieldDef)(implicit user: User): Map[String, JsValue] = Map.empty
  def extraMetadata(viewDef: ViewDef)(implicit user: User): Map[String, JsValue] = Map.empty
  def extraMetadata(filter: FilterParameter)(implicit user: User): Map[String, JsValue] = Map.empty

  private val ident = "[_\\p{IsLatin}][_\\p{IsLatin}0-9]*"
  private val ContainsOpFilterDef = s"^.*%~+%\\s*:($ident)\\??$$".r
  private val EndsWithOpFilterDef = s"^.*%~+\\s*:($ident)\\??$$".r
  private val StartsWithOpFilterDef = s"^.*~+%\\s*:($ident)\\??$$".r
  def filterFieldLabel(name: String, colLabel: String, filterType: FilterType): FilterLabel = {
    import org.mojoz.querease.FilterType._
    filterType match {
      case ComparisonFilter(col, op, name, opt) =>
        op match {
          // TODO more operators?
          case "%~~~%" | "%~~%" | "%~%" => FilterLabel(colLabel, "contains")
          case "%~~~" | "%~~" | "%~" => FilterLabel(colLabel, "ends with")
          case "~~~%" | "~~%" | "~%" => FilterLabel(colLabel, "begins with")
          case _ => FilterLabel(colLabel, null)
        }
      case IntervalFilter(nameFrom, optFrom, opFrom, col, opTo, nameTo, optTo) =>
        if (name == nameFrom) FilterLabel(colLabel.replace(" from", ""), "from")
        else if (name == nameTo) FilterLabel(colLabel.replace(" to", ""), "to")
        else FilterLabel(colLabel, null)
      case OtherFilter(fExpr) => fExpr match {
        case ContainsOpFilterDef(vName) if vName == name => FilterLabel(colLabel, "contains")
        case EndsWithOpFilterDef(vName) if vName == name => FilterLabel(colLabel, "ends with")
        case StartsWithOpFilterDef(vName) if vName == name => FilterLabel(colLabel, "begins with")
        case _ => FilterLabel(colLabel, null)
      }
      case _ => FilterLabel(colLabel, null)
    }
  }

  def filterMetadata(view: ViewDef)(implicit user: User, state: ApplicationState): Map[String, JsValue] =
    Option(view.name).filter(viewNameToFilterMetadata.contains(_)).map(viewName => //bi reports isn't presented in viewNameToFilterMetadata
      Map(
        "filter" -> JsArray(viewNameToFilterMetadata(viewName).map(f => JsObject(Map(
          "name" -> JsString(f.name),
          "type" -> JsString(f.type_.name),
          "nullable" -> JsBoolean(f.nullable),
          "required" -> JsBoolean(f.required),
          "label" -> Option(f.label).map(fl => JsString(fl.render(state.locale))).getOrElse(JsNull),
          "enum" -> Option(f)
            .map(_.enum)
            .filter(_ != null)
            .map(e => JsArray(e.map(JsString(_)): _*)).getOrElse(JsNull),
          "refViewName" -> Option(f.refViewName).map(JsString(_)).getOrElse(JsNull),
          "jsonType" ->
            JsString(
              qe.typeDefs
                .find(_.name == f.type_.name)
                .flatMap(_.targetNames.get("json"))
                .getOrElse("string")
            )
          )
          ++
          List(
            "length" -> f.type_.length,
            "totalDigits" -> f.type_.totalDigits,
            "fractionDigits" -> f.type_.fractionDigits)
          .filter(_._2.isDefined)
          .map(x => x._1 -> JsNumber(x._2.get))
          .toMap
          ++
          extraMetadata(f)
        )): _*)
      )).getOrElse(Map.empty)

  case class FilterLabel(fieldName: String, filterName: String) {
    def render(locale: Locale) = fieldName +
      Option(filterName).map(fn => s" (${translate(fn)(locale)})").getOrElse("")
  }
  case class FilterParameter(
    name: String, table: String, label: FilterLabel, nullable: Boolean, required: Boolean, type_ : Type, enum: Seq[String],
    refViewName: String, filterType: FilterType,
  )
  def currentUserParamNames: Set[String] = Set.empty
  private lazy val current_user_param_names = currentUserParamNames
  def filterToParameterNamesAndCols(filter: FilterType): Seq[(String, String)] = filter match {
    case BooleanFilter(b) =>
      Nil
    case IdentFilter(col, name, opt) =>
      Seq(name -> col)
    case ComparisonFilter(col, op, name, opt) =>
      Seq(name -> col)
    case IntervalFilter(nameFrom, optFrom, opFrom, col, opTo, nameTo, optTo) =>
      Seq(nameFrom -> col, nameTo -> col)
    case RefFilter(col, name, opt, refViewName, refFieldName, refCol) =>
      Seq(name -> col)
    case OtherFilter(_) =>
      Nil
    case _ =>
      Nil
  }
  def filterToParameterNames(filter: FilterType): Seq[String] = filter match {
    case BooleanFilter(b) =>
      Nil
    case IdentFilter(col, name, opt) =>
      Seq(name)
    case ComparisonFilter(col, op, name, opt) =>
      Seq(name)
    case IntervalFilter(nameFrom, optFrom, opFrom, col, opTo, nameTo, optTo) =>
      Seq(nameFrom, nameTo)
    case RefFilter(col, name, opt, refViewName, refFieldName, refCol) =>
      Seq(name)
    case OtherFilter(fExpr) =>
      qe.parser.extractVariables(fExpr)
        .map(_.variable)
    case _ =>
      Nil
  }
  def filterParameters(view: ViewDef): Seq[FilterParameter] = {
    def fieldNameToLabel(n: String) =
      n.replace("_", " ").capitalize
    val v = view
    if (v.apiMethodToRole != null && v.apiMethodToRole.nonEmpty && (v.table != null || v.joins != null && v.joins.size > 0)) {
      val filters =
        Option(v.filter).getOrElse(Nil) flatMap { f =>
          qe.analyzeFilter(f, v, v.tableAlias)
        }
      // TODO duplicate code, reuse querease code!
      val joinsParser = new org.mojoz.querease.TresqlJoinsParser(qe.tresqlMetadata)
      val (needsBaseTable, parsedJoins) =
        Option(v.joins)
          .map(joins =>
            Try(joinsParser(null, joins)).toOption
              .map(joins => (false, joins))
              .getOrElse((true, joinsParser(qe.tableAndAlias(v), joins))))
          .getOrElse((false, Nil))
      val joinAliasToJoin =
        parsedJoins.map(j => (Option(j.alias) getOrElse j.table, j)).toMap
      val parameterNameToCol =
        filters.flatMap(filterToParameterNamesAndCols).toMap
      val parameterNameToFilterType =
        filters.flatMap(filter => filterToParameterNames(filter).map(_ -> filter)).toMap
      val q = qe.queryStringAndParams(v, Map.empty)._1
      val allVariables = new QueryParser(tresqlResources, tresqlResources.cache).extractVariables(q)
      // TODO? fromAndPathToAlias(v): (String, Map[List[String], String])
      allVariables
        .distinct // FIXME aggregate v.opt!
        .filterNot(current_user_param_names contains _.variable)
        .map { v =>
          val colQName = parameterNameToCol.getOrElse(v.variable, "")
          val filterType = parameterNameToFilterType.get(v.variable).orNull
          val refViewName = Option(filterType).map {
            case RefFilter(col, name, opt, refViewName, refFieldName, refCol) => refViewName
            case _ => null
          }.orNull
          val tableAlias =
            if (colQName.indexOf(".") > 0)
              colQName.substring(0, colQName.indexOf("."))
            else view.tableAlias
          val colName =
            if (colQName.indexOf(".") > 0)
              colQName.substring(colQName.indexOf(".") + 1)
            else colQName
          val col = joinAliasToJoin
            .get(tableAlias)
            .map(j => qe.tableMetadata.tableDefOption(j.table).map(_.cols) getOrElse Nil)
            .flatMap(_.find(_.name == colName))
            .orNull
          val name = v.variable
          val conventionsType =
            qe.metadataConventions.typeFromExternal(name, None)
          val table = joinAliasToJoin.get(tableAlias).map(_.table).orNull
          val label = Option(col)
            .map(_.comments)
            .filter(_ != null)
            .filter(_ != "")
            .map(qe.splitToLabelAndComments(_)._1)
            .filter(_ != null)
            .orElse(Option(fieldNameToLabel(name)))
            .map(filterFieldLabel(name, _, filterType))
            .orNull
          val nullable = v.opt
          val required = !v.opt
          val type_ = Option(col)
            .filter { col =>
              filterType.isInstanceOf[IdentFilter] ||
              filterType.isInstanceOf[ComparisonFilter] ||
              filterType.isInstanceOf[IntervalFilter]
            }
            .map(_.type_)
            .getOrElse(conventionsType)
          val enum = Option(col).map(_.enum).orNull
          FilterParameter(name, table, label, nullable, required, type_, enum, refViewName, filterType)
        }
    } else Nil
  }
  lazy val viewNameToFilterMetadata = qe.viewNameToClassMap.keys.toList.sorted
    .map(viewName => viewName -> filterParameters(qe.viewDef(viewName))).toMap

  def apiMetadata(implicit user: User, state: ApplicationState) = {
    // TODO duplicate code, just filter differs
    val q = new collection.mutable.Queue[ViewDef]
    val names = collection.mutable.Set[String]()
    q ++= qe.collectViews { case v if v.apiMethodToRole != null && v.apiMethodToRole.nonEmpty => v }
    while (q.nonEmpty) {
      val v = q.dequeue()
      names += v.name
      v.fields.foreach { f =>
        if (f.type_.isComplexType && !names.contains(f.type_.name))
          q += viewDef(f.type_.name)
      }
    }
    JsObject(TreeMap[String, JsValue]() ++ names.toSeq.sorted.map(n => n -> metadata(n)))
  }

  def auth[C <: RequestContext[_]](ctx: C, clazz: Class[_])(action: => C) = {
    check(ctx, clazz)
    relevant(action, clazz)
  }

  //helper methods
  /** Query parameter overrides related to current user, for example, user_id, person_id etc.
    * Override with something useful, like:
    * {{{
    * Option(user).map(u => Map("current_user_id" -> u.id)) getOrElse Map.empty
    * }}}
    */
  def current_user_param(user: User): Map[String,Any] = Map.empty

  def filterByHasRole(someRoles: Set[String], user: User): Set[String] =
    someRoles.filter(hasRole(user, _))

  def api(implicit user: User) = {
    val views = qe.collectViews{ case v => v}.toSeq.sortBy(_.name)
    val allApiRelatedRoles =
      views.map(_.apiMethodToRole).filter(_ != null).flatMap(_.values).filter(_ != null).toSet
    val roles = filterByHasRole(allApiRelatedRoles, user)
    JsObject(TreeMap[String, JsValue]() ++
      views
        .map(v => v.name -> v.apiMethodToRole)
        .filter(_._2 != null)
        .map(v => v.copy(_2 = v._2.filter(roles contains _._2)))
        .filter(_._2.nonEmpty)
        .map(v => v.copy(_2 = JsArray(v._2.keys.toSeq.map(JsString(_)): _*))))
  }

  def noApiException(viewName: String, method: String, user: User): Exception =
    new BusinessException(s"$viewName.$method is not a part of this API")
  def checkApi[F](viewName: String, method: String, user: User)(f: => F) = (for {
    view <- viewDefOption(viewName)
    role <- view.apiMethodToRole.get(method)
    if hasRole(user, role)
  } yield f).getOrElse(
    throw noApiException(viewName, method, user)
  )

  def impliedIdForGetOverList[F](viewName: String): Option[Long] =
    viewDefOption(viewName)
      .filter(v => v.apiMethodToRole.contains("get") && !v.apiMethodToRole.contains("list"))
      .map(_ => 0)

  def fieldRequiredErrorMessage(viewName: String, field: qe.FieldDef)(implicit locale: Locale): String =
    translate("""Field %1$s is mandatory.""", field.label)
  def isFieldRequiredViolated(viewName: String, field: qe.FieldDef, value: Any): Boolean =
    field.required &&
    (value match {
      case null => true
      case s: String if s.trim == "" => true
      case _ => false
    })

  def fieldValueTooLongErrorMessage(viewName: String, field: qe.FieldDef, value: Any)(implicit locale: Locale): String =
    translate("""Field "%1$s" value length %2$s exceeds maximum limit %3$s.""",
      field.label, value.toString.length.toString, field.type_.length.get.toString)
  def isFieldValueMaxLengthViolated(viewName: String, field: qe.FieldDef, value: Any): Boolean =
    value != null &&
    field.type_.name == "string" &&
    field.type_.length.isDefined &&
    value.toString.length > field.type_.length.get

  def fieldValueNotInEnumErrorMessage(viewName: String, field: qe.FieldDef, value: Any)(implicit locale: Locale): String =
    translate("""Field "$%1$s" value must be from available value list.""", field.label)
  def isFieldValueEnumViolated(viewName: String, field: qe.FieldDef, value: Any): Boolean =
    value != null &&
    field.enum != null &&
    field.enum.size > 0 &&
    !field.enum.contains(value.toString)

  def badEmailAddressErrorMessage(viewName: String, field: qe.FieldDef, value: Any)(implicit locale: Locale): String =
    translate("""Field "%1$s" is not valid e-mail address""", field.label)
  def isEmailAddressField(viewName: String, field: qe.FieldDef): Boolean =
    field.type_.name == "email"  ||
    field.type_.name == "epasts" ||
    field.type_.name == "string" && (field.name == "email" || field.name == "epasts")
  def isEmailAddressTemplateViolated(viewName: String, field: qe.FieldDef, value: Any): Boolean =
    value != null &&
    value != "" &&
    isEmailAddressField(viewName, field) &&
    !is_valid_email(value.toString)

  def validationErrorMessage(viewName: String, field: qe.FieldDef, value: Any)(implicit locale: Locale): Option[String] = {
    if (isFieldRequiredViolated(viewName, field, value))
      Option(fieldRequiredErrorMessage(viewName, field))
    else if (isFieldValueMaxLengthViolated(viewName, field, value))
      Option(fieldValueTooLongErrorMessage(viewName, field, value))
    else if (isFieldValueEnumViolated(viewName, field, value))
      Option(fieldValueNotInEnumErrorMessage(viewName, field, value))
    else if (isEmailAddressTemplateViolated(viewName, field, value))
      Option(badEmailAddressErrorMessage(viewName, field, value))
    else None
  }

  def checkFieldValues(instance: Dto)(implicit state: ApplicationState): Unit = {
    def validateFields(viewName: String, map: Map[String, Any]): Unit = {
      val viewDef = qe.viewDef(viewName)
      // TODO ensure field ordering
      val errorMessages = viewDef.fields
        .map(fld =>
          validationErrorMessage(viewName, fld, map.getOrElse(Option(fld.alias).getOrElse(fld.name), null))(state.locale))
        .filter(_.isDefined)
        .map(_.get)
        .filter(_ != null)
      if (errorMessages.nonEmpty)
        throw new BusinessException(errorMessages.mkString("\n"))

      // TODO merge all errorMessages?
      val complexFields = viewDef.fields.filter(_.type_.isComplexType).map(fld => Option(fld.alias).getOrElse(fld.name) -> fld.type_.name)
      complexFields.foreach { case (fieldName, typeName) =>
        map.getOrElse(fieldName, null) match {
          case m: Map[String, Any] @unchecked => validateFields(typeName, m)
          case l: List[Map[String, Any]] @unchecked => l.foreach(validateFields(typeName, _))
          case null =>
        }
      }
    }

    validateFields(classToViewNameMap(instance.getClass), qe.toMap(instance))
  }

  def validateFields(instance: Dto)(implicit state: ApplicationState) = {
    checkFieldValues(instance)
  }
}
