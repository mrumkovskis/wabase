package org.wabase

import scala.collection.immutable.Set

trait Authorization[User] {
  this: AppBase[User] with Audit[User] with DbAccess with DbConstraintMessage =>

  private val wabaseAuth =
    getObjectOrNewInstance[WabaseAuthorization](
      config, "app.wabase-authorization", "wabase authorization"
    )

  /** legacy flow - performs authorization, on failure throws Exception, otherwise returns */
  def check[C <: RequestContext[_]](ctx: C, clazz: Class[_]): Unit = ???
  /** legacy flow - adds authZ information regarding result to context, i.e is result editable, etc... */
  def relevant[C <: RequestContext[_]](ctx: C, clazz: Class[_]): C = ???
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

trait WabaseAuthorization {
  def hasRole(wabase: WabaseService.Wabase, user: WabaseUser, roles: Set[String]): Boolean
}

class DefaultWabaseAuthorization extends WabaseAuthorization {
  def hasRole(
    wabase: WabaseService.Wabase,
    user: WabaseUser,
    roles: Set[String],
  ): Boolean = user.roles.intersect(roles).nonEmpty
}
