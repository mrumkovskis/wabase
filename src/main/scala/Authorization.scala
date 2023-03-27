package org.wabase

import scala.collection.immutable.Set

trait Authorization[User] {
  this: AppBase[User] with Audit[User] with DbAccess with ValidationEngine with DbConstraintMessage =>
  /** performs authorization, on failure throws UnauthorizedException, otherwise returns */
  def check[C <: RequestContext[_]](ctx: C, clazz: Class[_]): Unit
  /** performs authorization, on success returns true otherwise false */
  def can[C <: RequestContext[_]](ctx: C, clazz: Class[_]): Boolean
  /** adds authZ information regarding result to context, i.e is result editable, etc... */
  def relevant[C <: RequestContext[_]](ctx: C, clazz: Class[_]): C
  /** Override with something useful, like:
    * {{{
    * qe.list(classOf[HasRoleHelper], Map("current_user_id" -> user.id, "role" -> role))
    *   .headOption.map(_.has_role.booleanValue) getOrElse false
    * }}}
    */
  def hasRole(user: User, roles: Set[String]): Boolean
}

object Authorization {
  class UnauthorizedException(msg: String) extends BusinessException(msg)

  trait NoAuthorization[User] extends Authorization[User] {
    this: AppBase[User] with Audit[User] with DbAccess with ValidationEngine with DbConstraintMessage =>
    override def check[C <: RequestContext[_]](ctx: C, clazz: Class[_]): Unit = {}
    override def can[C <: RequestContext[_]](ctx: C, clazz: Class[_]) = true
    override def relevant[C <: RequestContext[_]](ctx: C, clazz: Class[_]) = ctx
    override def hasRole(user: User, roles: Set[String]): Boolean = true
  }
}
