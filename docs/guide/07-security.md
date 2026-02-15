# Part 7: Security & Roles

Wabase has built-in RBAC (Role-Based Access Control).

## 1. Defining Roles

Roles are strings. Common ones: `admin`, `manager`, `user`.

## 2. Restricting Views

In your view definition:

```yaml
name:   user_view
# Only admin can list/delete. Everyone can get/save (registration).
api:    admin list delete, user get save
```

## 3. Implementing Authentication

You need to tell Wabase *who* the current user is. Override `WabaseApp.auth` or implement a custom `WabaseAuthentication`.

Example `TMSAuthentication.scala`:

```scala
package com.example.tms

import org.wabase.{WabaseAuthentication, WabaseUser}
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Route

trait TMSAuthentication extends WabaseAuthentication[WabaseUser] {

  // Minimal example: Read 'X-User-Id' header (Insecure! Use JWT/Session in production)
  override def authenticate: Route = {
    optionalHeaderValueByName("X-User-Id") { userIdOpt =>
      userIdOpt match {
        case Some(userId) =>
          // Mock lookup user roles from DB
          val roles = if (userId == "1") Set("admin") else Set("user")
          val user = WabaseUser(Map("id" -> userId.toLong, "roles" -> roles))
          provide(user)
        case None =>
          reject // 401 Unauthorized
      }
    }
  }
}
```

Mix this trait into your `TMSApp` / `WabaseServer`.

## 4. Row-Level Security

You can restrict data based on the logged-in user.

```yaml
name:   task_view
# ...
filter:
  # Only show tasks assigned to me OR if I am admin
  - assignee_id = :current_user.id | :current_user.roles ? 'admin'
```

`:current_user` is a special variable populated from `WabaseUser`.

## Conclusion

You have built a secure, scalable Task Management System with advanced features!
Check the [Reference](../reference/01-views.md) for more details.
