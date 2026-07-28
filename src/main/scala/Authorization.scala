package org.wabase

import com.typesafe.scalalogging.Logger
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.wabase.AppMetadata.Action
import org.wabase.ds.QueryTimeout
import org.tresql._

import scala.concurrent.{ExecutionContext, Future}

/** Used for calling wabase authorization view */
case class AuthContext(
  as: ActorSystem,
  httpReq: HttpRequest,
  queryTimeout: QueryTimeout,
  log: Logger,
)(implicit val ec: ExecutionContext)

trait Authorization[User] {
  this: AppBase[User] with Audit[User] with DbAccess with DbConstraintMessage =>

  @annotation.nowarn("msg=Manifest")
  private val wabaseAuth = {
    val WaClassProp = "app.wabase-authorization.class"
    if (!config.getIsNull(WaClassProp))
      getObjectOrNewInstance[WabaseAuthorization](config, WaClassProp, "wabase authorization")
    else null
  }
  private val wabaseAuthView = {
    val WaViewProp = "app.wabase-authorization.view"
    if (!config.getIsNull(WaViewProp)) config.getString(WaViewProp) else null
  }
  require(wabaseAuth == null || wabaseAuthView == null,
    "Cannot specify both configuration parameters - app.wabase-authorization.class and app.wabase-authorization.view")

  /** legacy flow - performs authorization, on failure throws Exception, otherwise returns */
  def check[C <: RequestContext[_]](ctx: C, clazz: Class[_]): Unit = ???
  /** legacy flow - adds authZ information regarding result to context, i.e is result editable, etc... */
  def relevant[C <: RequestContext[_]](ctx: C, clazz: Class[_]): C = ???

  @annotation.nowarn("msg=Manifest")
  def hasRole(user: User, roles: Set[String])(authCtx: AuthContext): Future[Boolean] = {
    if (wabaseAuthView != null) user match {
      case _: WabaseUser =>
        import authCtx._
        val res_fac = resourceFactory(wabaseAuthView, log.underlying.getName, queryTimeout)
        def err(r: QuereaseResult) = sys.error(
          s"Unexpected return from authorization view $wabaseAuthView - '$r'. " +
          s"Only allowed boolean value.")
        qe.QuereaseAction(wabaseAuthView, Action.Get, Map("roles" -> roles),
            current_user_param(user), null)(res_fac, httpReq, qio, fileStreamers,
            httpClients, injectionParametersProvider, log)
          .run(ec, as)
          .map {
            case AnyResult(result: Boolean)                 => result
            case QuereaseResultWithCleanup(result, cleanup) => try result match {
              case TresqlResult(result)                     => result.unique[Boolean]
              case x                                        => err(x)
            } finally cleanup(None)
            case x                                          => err(x)
          }
      case x => Future.successful(false)
    } else if (wabaseAuth != null) user match {
      case wabaseUser: WabaseUser =>
        wabaseAuth.hasRole(this.asInstanceOf[WabaseService.Wabase], wabaseUser, roles)
      case x                      => Future.successful(false)
    } else Future.successful(false)
  }
}

trait WabaseAuthorization {
  def hasRole(wabase: WabaseService.Wabase, user: WabaseUser, roles: Set[String]): Future[Boolean]
}

class DefaultWabaseAuthorization extends WabaseAuthorization {
  def hasRole(
    wabase: WabaseService.Wabase,
    user: WabaseUser,
    roles: Set[String],
  ): Future[Boolean] = Future.successful(user.roles.intersect(roles).nonEmpty)
}
