package wabase.app

import org.wabase.{WabaseAuthorization, WabaseUser}
import org.wabase.WabaseService.Wabase
import org.tresql._

object TestAuthorization extends WabaseAuthorization {
  override def hasRole(wabase: Wabase, user: WabaseUser, roles: Set[String]): Boolean = {
    wabase.withConn() { implicit res =>
      Query("""exists(user_role[user_id = :id & role in :roles] {1})""", Map("roles" -> roles, "id" -> user.id))
        .unique[Boolean]
    }
  }
}
