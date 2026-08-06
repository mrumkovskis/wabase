# CSRF Protection

## Use This When

You have browser clients and need protection for state-changing requests.

## Core Surface

- `src/main/scala/CSRFDefence.scala`
- `src/it/resources/routes/csrf.yaml`
- `src/it/resources/http_tests/csrf-defence/*.yaml`

## Simple Example

```yaml
on: POST /set-csrf-cookie
do: setCsrfCookie ok

on: GET /check-csrf-token
do: checkCsrfToken ok
```

## Complex Example

```yaml
on: POST /account/update
do: checkSameOrigin checkCsrfToken authenticate doAction('account_update')
```

```http
Cookie: XSRF-TOKEN=abc123
X-XSRF-TOKEN: abc123
Origin: https://app.example.com
```

## Key Notes

1. Cookie/header mismatch is rejected.
2. Same-origin checks validate request source host.
3. Keep CSRF enabled for browser POST/PUT/PATCH/DELETE flows.

## Related Docs

- `../reference/07-security-authentication-and-csrf.md`
