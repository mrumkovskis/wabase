# Security, Authentication, and CSRF

This chapter documents Wabase security primitives implemented in runtime handlers and supporting utilities.

## 1. Session Authentication Model

Session handling is implemented by:
- `src/main/scala/Authentication.scala`
- `src/main/scala/WabaseAuthentication.scala`

Session cookie behavior:
- Cookie name: `session.cookie.name`
- Cookie path: `session.cookie.path`
- Secure flag: `session.cookie.secure`
- Expiration: `session.timeout`

Session payload includes:
- Unique session ID
- User identity / role payload
- Client metadata (IP/User-Agent context)
- Expiration timestamp

Security properties:
1. Session payload encryption (AES).
2. Integrity validation (HMAC).
3. Explicit timeout validation.

## 2. Authentication Handlers for Routes

Main handlers used in route chains:
- `authenticate`
- `authenticateOpt`
- `setSessionCookie` / `setSessionCookieOpt`
- `removeSessionCookie`
- `checkRole`
- `extractBasicHttpCredentials`
- `extractFormDataCredentials`
- `extractJwtTokenCredentials`

Example route patterns from integration resources:

```yaml
on: /(login_using_json_or_urlencoded)
do: extractFormDataCredentials setDomainAndPathSessionCookie(null, '/') doAction $1

on: POST /logout
do: removeSessionCookie doAction('logout')

on: /role-check/(.+)
do: authenticateOpt audit checkRole($1) ok
```

## 3. Credentials Sources

Wabase supports multiple credential extraction strategies:
1. HTTP Basic credentials.
2. Form-urlencoded / multipart credentials.
3. JWT bearer token extraction and validation.

JWT support file:
- `src/main/scala/JwtDecoder.scala`

Key loader support file:
- `src/main/scala/KeyLoader.scala`

Typical JWT configuration areas:
- Allowed algorithms
- Clock and leeway handling
- Claim name mappings
- Accepted issuer and audience
- Public/secret key material

## 4. CSRF and Same-Origin Checks

Implemented in:
- `src/main/scala/CSRFDefence.scala`
- `src/it/resources/routes/csrf.yaml`
- `src/it/resources/http_tests/csrf-defence/*.yaml`

Provided operations:
- `setCsrfCookie`
- `checkCsrfToken`
- `deleteCsrfCookie`
- `checkSameOrigin`

Default names:
- Cookie: `XSRF-TOKEN`
- Header: `X-XSRF-TOKEN`

Validation model:
1. Same-origin verification using target/source origins.
2. Cookie/header token match verification.

## 5. Public API and Role Exposure

Config controls:
- `app.public-api.role-name`
- `app.public-views.location-pattern`

Authorization contract files:
- `src/main/scala/Authorization.scala`
- `src/main/scala/AppBase.scala` (API filtering by roles)

## 6. Error Semantics

Security failures are surfaced as:
- HTTP 401 (authentication failures)
- HTTP 403 (authorization failures)
- HTTP 400 (CSRF/same-origin validation failures)

Support files:
- `src/main/scala/WabaseErrorHandler.scala`
- `src/main/scala/BusinessException.scala`

## 7. Hardening Checklist

1. Set strong `auth.crypto.key` and `auth.mac.key` per environment.
2. Set `session.cookie.secure = true` in HTTPS deployments.
3. Scope cookie path via `session.cookie.path` to minimum required application path.
4. Enforce explicit JWT algorithm allowlists and issuer/audience checks.
5. Keep CSRF token checks on all state-changing routes used by browser clients.
6. Keep `authenticate` / `checkRole` near the beginning of route chains.
7. Add integration tests for login, role checks, logout, and unauthenticated paths.

## 8. Test Coverage Sources

Core specs and scenarios:
- `src/test/scala/AuthenticationSpecs.scala`
- `src/test/scala/ExtractCredentialsTest.scala`
- `src/it/resources/http_tests/auth/*.yaml`
- `src/it/resources/http_tests/csrf-defence/*.yaml`

## Feature Guides

*   [Authentication and Sessions](../features/01-authentication-and-sessions.md)
*   [CSRF Protection](../features/02-csrf-protection.md)
