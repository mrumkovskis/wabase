package org.wabase

import scala.collection.immutable.Set

trait Authorization[User] {
  this: AppBase[User] with Audit[User] with DbAccess with ValidationEngine with DbConstraintMessage =>

  private val wabaseAuth =
    getObjectOrNewInstance[WabaseAuthorizationFactory](
      config, "app.wabase-authorization-factory", "wabase authorization factory"
    ).initialize()

  /** performs authorization, on failure throws UnauthorizedException, otherwise returns */
  def check[C <: RequestContext[_]](ctx: C, clazz: Class[_]): Unit
  /** adds authZ information regarding result to context, i.e is result editable, etc... */
  def relevant[C <: RequestContext[_]](ctx: C, clazz: Class[_]): C
  /** Override with something useful, like:
    * {{{
    * qe.list(classOf[HasRoleHelper], Map("current_user_id" -> user.id, "role" -> role))
    *   .headOption.map(_.has_role.booleanValue) getOrElse false
    * }}}
    */
  def hasRole(user: User, roles: Set[String]): Boolean = user match {
    case wabaseUser: WabaseUser =>
      wabaseAuth.hasRole(this.asInstanceOf[WabaseService.Wabase], wabaseUser, roles)
    case x                      => false
  }
}

trait WabaseAuthorizationFactory {
  def initialize(): WabaseAuthorization
}

class WabaseAuthorization {
  def hasRole(
    wabase: WabaseService.Wabase,
    user: WabaseUser,
    roles: Set[String],
  ): Boolean = user.roles.intersect(roles).nonEmpty
}

object Authorization extends WabaseAuthorizationFactory {
  class UnauthorizedException(msg: String) extends BusinessException(msg)

  override def initialize(): WabaseAuthorization = new WabaseAuthorization

  trait NoAuthorization[User] extends Authorization[User] {
    this: AppBase[User] with Audit[User] with DbAccess with ValidationEngine with DbConstraintMessage =>
    override def check[C <: RequestContext[_]](ctx: C, clazz: Class[_]): Unit = {}
    override def relevant[C <: RequestContext[_]](ctx: C, clazz: Class[_]) = ctx
    override def hasRole(user: User, roles: Set[String]): Boolean = true
  }
}
