package org.wabase

trait Authorization[User] { this: AppBase[User] =>
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
  def hasRole(user: User, role: String): Boolean
}

object Authorization {
  class UnauthorizedException(msg: String) extends BusinessException(msg)

  trait NoAuthorization[User] extends Authorization[User] { this: AppBase[User] =>
    override def check[C <: RequestContext[_]](ctx: C, clazz: Class[_]): Unit = {}
    override def can[C <: RequestContext[_]](ctx: C, clazz: Class[_]) = true
    override def relevant[C <: RequestContext[_]](ctx: C, clazz: Class[_]) = ctx
    override def hasRole(user: User, role: String): Boolean = true
  }

  trait RowAuthorization { this: AppBase[_] =>
    //horizontal auth filters
    import qe.viewDef
    import AppMetadata._
    def viewFilter(viewName: String) =
      viewDef(viewName).auth.forGet.map(a => s"($a)").mkString(" & ")
    def listFilter(viewName: String) =
      viewDef(viewName).auth.forList.map(a => s"($a)").mkString(" & ")
    def insertFilter(viewName: String) =
      viewDef(viewName).auth.forInsert.map(a => s"($a)").mkString(" & ")
    def updateFilter(viewName: String) =
      viewDef(viewName).auth.forUpdate.map(a => s"($a)").mkString(" & ")
    def deleteFilter(viewName: String) =
      viewDef(viewName).auth.forDelete.map(a => s"($a)").mkString(" & ")
  }
}
