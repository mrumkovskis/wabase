# Authentication and Sessions

## Use This When

You need login/logout, session cookies, role checks, or JWT credentials.

## Core Surface

- `src/main/scala/Authentication.scala`
- `src/main/scala/WabaseAuthentication.scala`
- `src/main/resources/reference.conf` (`session.*`, `auth.*`, `jwt-decoder.*`)

## Simple Example

```yaml
on: /(login_using_json_or_urlencoded)
do: extractFormDataCredentials setSessionCookie doAction $1

on: POST /logout
do: removeSessionCookie doAction('logout')
```

## Complex Example

```hocon
session.cookie.name   = "session-id"
session.cookie.path   = "/app"
session.cookie.secure = true
session.timeout       = 1200s

jwt-decoder {
  allowed-algorithms = [RS256]
  accept.issuer = "https://issuer.example"
  accept.audience = "wabase-api"
}
```

```yaml
on: GET /admin/(.+)
do: authenticateOpt checkRole('admin') doAction $1
```

## Key Notes

1. `authenticate` requires a valid user, `authenticateOpt` allows anonymous.
2. `checkRole` must run after authentication handlers.
3. Cookie `path` must match application route prefix.

## Related Docs

- `../reference/07-security-authentication-and-csrf.md`
- `../reference/04-configuration.md`
