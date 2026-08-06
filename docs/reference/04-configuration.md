# Configuration Reference

Wabase is configured through `application.conf` (with defaults in `src/main/resources/reference.conf`).

This page documents the main key groups, how they affect runtime behavior, and the keys you typically override in real projects.

## 1. Runtime Core (`app.*`)

| Key | Purpose | Typical Override |
| :--- | :--- | :--- |
| `app.home` | Base application path used by other defaults. | Per environment |
| `app.files.path` | Root file storage path for file streamer(s). | Per environment |
| `app.current-user-key-name` | Name of current-user bind variable. | Rarely |
| `app.user-credentials-key-name` | Name used for parsed credentials in request state. | Rarely |
| `app.action-for-http.post` | POST mapping (`post`/legacy insert mapping). | Legacy migrations |
| `app.action-for-http.put` | PUT mapping (`put`/legacy update mapping). | Legacy migrations |
| `app.action-for-key-update` | Action name for key-based update shortcut. | Legacy migrations |
| `app.action-legacy-mapping` | Enables older action mapping behavior. | Legacy migrations |
| `app.field-filter-parameter-name` | Query parameter used for response field filtering (default `fields`). | API design choice |
| `app.serialization-buffer-size` | Buffer used by serializers/streaming transforms. | Large payload tuning |
| `app.function-invocation-cache-size` | Invocation reflection cache size. | High-throughput tuning |
| `app.marshal_key_as_json` | Serialize action key as JSON structure. | Integration preference |
| `app.wabase.evaluator.pool` | Fallback pool for evaluator resources. | Multi-pool setups |
| `app.wabase-authorization` | Authorization strategy class. | Custom auth rules |
| `app.wabase-error-handler` | Root error handler function. | Custom error model |
| `app.wabase-error-handler-and-then` | Post-processing error hook. | Auditing/observability |
| `app.wabase-logger-name-factory` | Logger name strategy by request path. | Logging conventions |
| `wabase.max-stack-depth` | Max nested action / posted-structure depth. | Safety tuning |

## 2. HTTP Server (`app.server.*`)

| Key | Purpose | Default |
| :--- | :--- | :--- |
| `app.server.bind-address` | Interface bind address. | `0.0.0.0` |
| `app.server.invoke-before-start` | Optional method invoked before server start. | empty |
| `app.server.shutdown-on-keypress-enter` | Enables ENTER-triggered shutdown in CLI mode. | `false` |
| `app.server.shutdown-on-bind-failed` | Shutdown if port bind fails. | `true` |
| `port` | HTTP port. | (app-specific) |
| `ssl-config` / `app.server.ssl-config` | TLS settings used by Pekko HTTP SSL context. | optional |

## 3. Session & Authentication

| Key | Purpose | Default |
| :--- | :--- | :--- |
| `session.cookie.name` | Session cookie key. | `session-id` |
| `session.cookie.path` | Session cookie path scope. | `/` |
| `session.cookie.secure` | Secure cookie flag. | `false` |
| `session.timeout` | Session expiration duration. | `900s` |
| `auth.crypto.key` | AES key for encrypted session payload. | `null` |
| `auth.mac.key` | HMAC key for session integrity. | `null` |
| `jwt-decoder.*` | JWT parsing, keys, accepted claims/algorithms, clock options. | configured by app |

Important behavior:
1. Session payloads are encrypted and HMAC-protected (`Authentication.Crypto`).
2. Session validation includes timeout and may include IP/User-Agent checks depending on integration.
3. JWT extraction is available via `extractJwtTokenCredentials` in route handler chains.

## 4. Deferred Requests (`app.deferred-requests.*`)

| Key | Purpose |
| :--- | :--- |
| `enabled` | Master toggle for deferred processing. |
| `worker-count` | Number of parallel deferred workers. |
| `default-timeout` | Default timeout used for deferred request execution. |
| `cleanup-job-interval` | Cleanup job interval for old deferred entries. |
| `requests` | Explicit request/view allowlist for deferral. |
| `timeouts` | Per-request timeout overrides. |
| `modules` | Optional per-module worker-count distribution. |
| `factory-class` | Deferred control implementation (default Wabase). |
| `storage-factory-class` | Deferred storage implementation factory. |
| `storage.file-streamer.*` | File streamer used for deferred response body persistence. |

## 5. Jobs & Scheduler (`app.job.*`)

| Key | Purpose |
| :--- | :--- |
| `scheduler-initializer` | Optional scheduler bootstrap function (e.g. Quartz). |
| `actor` / `actor-name` | Job actor class and actor name. |
| `max-time` | DB interval for stale lock takeover. |
| `job-status-cp` | Pool name for `cron_job_status` writes. |
| `on-start-job` | Job view to run at startup. |
| `clean-jobs-on-start` | Cleanup/reset job statuses on app start. |

## 6. File Streaming (`file-streamer.*`)

| Key | Purpose |
| :--- | :--- |
| `factory-class` | File streamer factory implementation. |
| `files.path` | Root file storage path. |
| `file-info-table` | Metadata table for logical file records. |
| `file-body-info-table` | Blob-body table keyed by SHA. |
| `sha-col-name` | Digest column used for deduplication and lookup. |
| `main` / named configs | Named streamer instances with inherited defaults. |

## 7. Request Parsers (`data-parsers-*`)

| Key Group | Purpose |
| :--- | :--- |
| `data-parsers-json.*` | JSON stream object parser configuration (`max-object-size`, named decoders). |
| `data-parsers-csv.*` | CSV delimiter/quote/escape/charset/header strategy. |
| `data-parsers-xml.*` | XML path-driven extraction parser configuration. |
| `request-decoders.factory-class` | Decoder registry factory hook. |

## 8. Result Rendering

| Key | Purpose |
| :--- | :--- |
| `result-renderers.factory-class` | Renderer registry factory hook (JSON/CBOR/CSV/ODS/XLS/etc). |
| `app.use-serialized-result-blocking-transformer` | Debugging option for serializer execution mode. |

## 9. DB Connectivity

| Key | Purpose |
| :--- | :--- |
| `jdbc.data-source-factory` | Data source factory class (default Hikari). |
| `jdbc.cp.<name>.*` | Named JDBC pool settings. |
| `jdbc.query-timeout` | Query timeout default. |
| `tresql.max-result-size` | Max TresQL rows. |
| `tresql.cache-size` | TresQL cache size. |

## 10. Notification, I18n, Script Validation

| Key Group | Purpose |
| :--- | :--- |
| `app.server-notifications.*` | SSE/WS notification pipeline settings. |
| `app.language-cookie-postfix` | Language cookie suffix for i18n state. |
| `app.script-validations.*` | Dynamic script validation engine and bootstrap functions. |

## 11. Public API Exposure

| Key | Purpose |
| :--- | :--- |
| `app.public-api.role-name` | Pseudo-role for unauthenticated API access. |
| `app.public-views.location-pattern` | View-path regex for public view access. |

## 12. Handler Aliases (`app.wabase-call-alias`)

`wabase-call-alias` maps short route handler names to fully-qualified methods, for example:

```hocon
app.wabase-call-alias {
  doAction       = org.wabase.WabaseService.doActionWithKeyToPath
  authenticate   = org.wabase.WabaseAuthentication.authenticatePlusSession
  maybeDeferred  = org.wabase.WabaseDeferredControl.maybeDeferred
  deferredResult = org.wabase.WabaseDeferredControl.deferredResult
}
```

This alias map is the main contract between YAML routes and Scala handler implementations.

## 13. Production Checklist

1. Set `auth.crypto.key` and `auth.mac.key` (never keep `null` outside local dev).
2. Enable `session.cookie.secure = true` behind HTTPS.
3. Configure `jdbc.cp.*` pools explicitly for app, audit, and optional evaluator/deferred workloads.
4. Pin `app.host` when generating externally-consumed Swagger docs.
5. Place `app.audit-queue.path` and deferred storage paths on persistent local disks.
6. Configure parser limits (`data-parsers-json.max-object-size`, upload limits) to match expected payloads.
7. Validate `wabase-call-alias` overrides in tests before rollout.

## Feature Guides

*   [Authentication and Sessions](../features/01-authentication-and-sessions.md)
*   [Email Sending](../features/11-email-sending.md)
*   [Deferred Requests](../features/05-deferred-requests.md)
*   [Request Decoding and Content Types](../features/15-request-decoding-and-content-types.md)
