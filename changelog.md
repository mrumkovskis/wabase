# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
`update+` action is now handled by the `save` handler (was `simpleAction`), so field and script
validations, readonly value protection, old value lookup (by old key) and friendly constraint error
messages now apply to it as for `update`. Script validation defines values whose names are not valid
javascript variable names (e.g. `old key`) as global object properties, accessible as `this['old key']`,
instead of failing with syntax error.

No-op job status controller added (`org.wabase.NoOpWabaseJobStatusController`).
Always allows the job to start and does not use `cron_job_status`. Intended for
a single node when a lock / status table is not needed. Set
`app.job.status-controller = org.wabase.NoOpWabaseJobStatusController`.
`WabaseJobStatusController` and `DefaultWabaseJobStatusController` documented.

Fixed-rate job scheduler added (`org.wabase.scheduler.FixedRateScheduler`). Set
`app.job.scheduler-initializer = org.wabase.scheduler.FixedRateScheduler.init` and define jobs under
`app.job.schedules.<job-name>` with required `interval` and optional `enabled`, `initial-delay` and
`params`. Unless `initial-delay` is set, the first run is delayed by a random 1 to min(interval, 59)
seconds so jobs do not all start at once after process start.

Job executor and name validator are configurable (`app.job.executor`, `app.job.name-validator`).
The job is identified by name; the name is checked before the status lock is taken.
`WabaseJobStatusController.acquireIsRunnningLock` renamed to `acquireIsRunningLock`
(deprecated alias kept).

`columnPairsToMap` action function converts a single-row tresql result to a map, taking odd
columns as keys and even columns as values. Nested query results and array queries (`|[…]`)
are converted recursively. Documented in `docs/action-functions.md`.

View action `create` renamed to `new`. View definitions and `api` lists that used `create`
must be updated. Previously generated querease action cache (`querease-action-cache.cbor`)
must be regenerated.

`extract entity` and `extract parts` take the source in a `from` clause. `extract entity`
accepts `StringResult`. Op parser keywords are guarded by a word boundary.

`rethrow` is argumentless and must be inside a `recover` step. New `throw` op fails the
action with a `Throwable` or message returned by the expression.

Email action op has new `html` option - `email [batch] [html] <recipients> <subject> <body> (<attachment> [...])`.
Body is sent as html instead of plain text (no plain text alternative part is added).
Embedded image attachments are supported. Since `Action.Email` has new fields, previously
generated querease action cache (`querease-action-cache.cbor`) must be regenerated.

`unique` and `unique_opt` action ops throw specific exceptions instead of generic ones.
Empty row set for `unique` throws `org.mojoz.querease.NotFoundException` (previously
`NoSuchElementException`), which is mapped to http status 404 by default exception handlers.
More than one row for `unique` and `unique_opt` throws new `org.wabase.NotUniqueException`
(previously `org.tresql.TooManyRowsException` or `RuntimeException`), which is not mapped to
any status code, i.e. results in http status 500. Applications can map it to some other
status code, for example 409, by adding own exception handler.
Also `unique` and `unique_opt` on stream (`foreach`) result throw on more than one element
instead of silently returning the first one.

Querease upgraded to 11.0.0. Validations accept error message parameter expressions following the
error message expression - `[<cursor definitions>, ] <require condition>, <error message> [, <message parameter> …]`.
Parameters are intended for i18n - error message is a static template with `%1$s`, `%2$s`, … placeholders,
which are replaced with parameter values on translation.
Validation messages are returned as objects with message and parameters instead of plain strings -
`org.mojoz.querease.ValidationResult.messages` is `List[ValidationMessage]` (previously `List[String]`),
where `ValidationMessage` has fields `msg` and `params`. Accordingly, json body of `400 Bad Request`
validation error response changed from `[{"location": [...], "messages": ["..."]}]` to
`[{"location": [...], "messages": [{"msg": "...", "params": [...]}]}]`, clients must be updated.
All messages of one validations step or view have the same parameter count, missing parameters are padded
with nulls. Parameters at the same position must be of compatible types across validations of a step or view.

Script (javascript) validation messages support parameters. Validation message may evaluate to string - message template
without parameters, array - message template followed by parameters, i.e. `['Should be %1$s, found %2$s', 43, my_int_field]`,
or object with `msg` and optional `params`, i.e. `{msg: 'Should be %1$s', params: [43]}`. Message which is not valid
javascript is used as is. Validation expression may evaluate to `true` - validation passes, `false` - fails with validation
message, array or object - fails with this message, validation message is not used, string - fails with message
`Error (validation "%1$s"): %2$s`, where first parameter is validation message as `{msg, params}` object and second one -
expression result (previously message text was concatenated).
Wrong validation definition is developer error and throws `RuntimeException` (http status `500 Internal Server Error`,
logged as error) instead of reporting validation error - expression evaluating to other value (i.e. `null`, `undefined`,
number), expression evaluation failure (previously `BusinessException`), message evaluating to other value or malformed
array or object. `BusinessException` thrown by custom function is propagated as is.
Custom functions `current_date()` and `now()` return strings in the format of date and timestamp variables, so they can be
compared with them (previously `java.sql.Date` and `java.sql.Timestamp` objects, which could not be compared in javascript).
Script validation documented in `docs/script-validation.md`.
Unused i18n resource `Validation error " %1$s ": Wrong validation result type: %2$s` removed.

Tresql upgraded to 13.6.0. `Result.rowView` exposes the current row as `RowLike` that is not a
`Result`, so nested results can be distinguished when traversing from outside.
Array select (`|[…]`) with more than one column returns a map per element instead of a vector.

Mojoz upgraded to 7.2.1. View fields keep their table when `column` is set and a column of that
name exists on the table. Querease 10.2.1 CRUD for column-stored views is included.

Extra view metadata can be loaded via `app.wabase-extra-metadata.loader` (default
`org.wabase.AppMetadata.metadataFromFiles`) and `app.wabase-extra-metadata.paths`.

Request decoders are configured under `request-decoders` with a factory per format.
Scalar json decoder frames a json stream on array elements so scalars and arrays, not only
objects, can be decoded.

`app.file-cleanup.min-age` (default 24 h) — files and `file_info` records younger than this
are not cleaned.

Action context moved to `AppQuerease` object and is available in the injection parameters
context.

View actions, routes and action functions documented in `docs/view-actions.md`,
`docs/routes.md` and `docs/action-functions.md`.

Business scenario YAML tests support `not_null()` and array size checks at response and field
level: `size(n)`, `size(>n)`, `size(>=n)`, `size(<n)`,
`size(<=n)`, `size(!=n)` (or `size(<>n)`), `size(=n)`, and inclusive range
`size(a..b)`. Optional capture works the same way: `size(3) -> captured_items`.

## [8.1.2] - 2026-09-02
`AppFileCleanup` optimized for NFS — fewer metadata operations on networked file stores.

## [8.1.1] - 2026-09-01
`AppFileCleanup` logs more detail under logger `wabase.file-cleanup`.

## [8.1.0] - 2026-08-22
Job status controller is configurable (`app.job.status-controller`, default
`org.wabase.DefaultWabaseJobStatusController`). The default implementation uses
`cron_job_status` to lock a job name across nodes.

`foreach` action op supports an iteration variable and scalar collection elements.
`Action.Foreach` has a new field; previously generated querease action cache
(`querease-action-cache.cbor`) must be regenerated.

`QuereaseProvider.initQuerease` uses `wabase.querease.class` (default
`org.wabase.DefaultAppQuerease`).

Pekko upgraded to 1.7.0. Mojoz 7.2.0, querease 10.2.0. sbt 1.13.0.
simple-java-mail 9.3.2, swagger-jaxrs2-jakarta 2.2.54, logback-classic 1.6.3.

## [8.0.0] - 2026-08-12
Major release. Apache Pekko replaces Akka (Pekko 1.6.0, Pekko HTTP 1.4.0, Pekko Connectors
1.3.0). Cross-built for Scala 3.3.8, 2.13.18 and 2.12.21. Java 11 is required at build time.

Mojoz upgraded from 5.3.3 to 7.1.1, querease from 7.0.1 to 10.1.0, tresql from 12.0.1 to
13.5.1. Tresql 13 includes array select (`|[…]`) and macros `if_empty`, `if_empty_or_else`,
`if_nonempty`, `split_to_array`.

YAML route definitions and `WabaseService` / `WabaseServer` are the request pipeline —
handlers are composed from `app.wabase-call-alias` (authentication, CSRF, i18n, metadata,
actions, deferred, files, notifications). `WabaseServer` binds Pekko HTTP, optional SSL,
and loads routes from a folder.

View actions expanded: jobs (`job` / `call` / `startJob`), `try` / `recover` / `rethrow`,
`foreach`, http client ops, email, templates (filesystem and classpath, PDF assets with
allowed directories and prefixes), extract entity and extract parts, set/delete cookie and
headers, user attributes, `if` / `else`, response and redirect ops. Deprecated by-name
paths and actions (`getByNameAction`, `getByNamePath`, `viewWithNamePath`) removed.

Jobs are view actions. Scheduler is started from `WabaseServer`; Quartz schedules may set
`pekko.quartz.schedules.<job>.enabled` and `params`. `app.job.clean-jobs-on-start` clears
finished rows in `cron_job_status`.

Script (javascript) validation replaces `ValidationEngine`. JWT decoder and `KeyLoader` for
HMAC/PEM/DER/PKCS12 keys. Swagger is generated from routes and views, with overrides and
merger. Request decoders for json, csv and xml. `ComponentConf` and factories for
filestreamers, http clients, deferred storage, result renderers.

`app.key-in-query` (default true) encodes resource keys in the query string (`?/key/parts`).
`app.crud-redirects` (default `relative`) controls Location / redirect path style.
Deferred requests return http 202 Accepted. `KeyResult` marshalling defaults to http 200
with the key as json.

Rest client cookie jar is thread-safe and follows RFC domain, path and `Secure` matching;
expired cookies are evicted; redirects 301/302 use GET and drop content headers.
`throwHttpErrors` and `followRedirects` are per-request.

`Authentication.checkPassword` throws `AuthenticationException` instead of
`BusinessException` and no longer accepts MD5 hashes. Session crypto uses a constant-time
MAC, random IV and a valid AES key length. Error handler redacts request body and URI in
logs and does not expose internal messages to the client by default (`app.error-handler`).
Hidden values redact secrets in audit and email debug logs.

HikariCP 7.1.0, logback-classic 1.6.2, postgresql 42.7.13, borer 1.17.0 (Scala 3),
simple-java-mail 9.3.1, jwt-scala 11.0.4.

## [7.0.0] - 2024-12-25
Scala 3 support (cross-built for 3.3.4, 2.13.15, 2.12.20). Mojoz 5.3.3, querease 7.0.1,
tresql 12.0.1.

View actions gain http, file, email, template, job, `db` / transaction, `conf`, json codec,
extract header / entity, `keep` result, invocation with parameter injection, multiple roles
per api method, and field filter (`cols`). Wabase scheduler and `cron_job_status` job lock.
Mustache templates (loader / renderer). Buffered audit. File streamer without a user.
Result renderers. Evaluator connection pool (`app.wabase.evaluator.pool`) when no db block
is open. Cursors (`build_cursors`). `java.time` types (`LocalDate`, `LocalDateTime`,
`OffsetDateTime`, `Instant`). Horizontal authorization tests for get and list.

Akka 2.6.21, Akka HTTP 10.2.10.

## [6.4.6] - 2025-12-12
Connection pool initialization error handling fix.

## [6.4.5] - 2024-12-04
Mojoz 4.3.3, querease 6.3.4. sbt 1.10.6.

## [6.4.4] - 2024-11-29
Mojoz 4.3.2, querease 6.3.3.

## [6.4.3] - 2024-11-28
Mojoz 4.3.1, querease 6.3.2. Scala 2.13.15, hsqldb 2.7.4, sbt 1.10.5.

## [6.4.2] - 2024-09-23
Buffered audit reader improvement and fix.

## [6.4.1] - 2024-09-13
Querease 6.3.1. Scala 2.12.20, postgresql 42.7.4.

## [6.4.0] - 2024-08-16
CSRF errors include the requested URL; CSRF error message is customizable again.
`wsNotificationsAction` undeprecated. Scala 2.13.14.

## [6.3.0] - 2024-04-07
Mojoz 4.3.0, querease 6.3.0. `uploadMultipleAsSource` directive. CSRF throws exceptions
instead of rejections. JDBC drivers loaded sequentially from one place.
Akka 2.6.21, Akka HTTP 10.2.10, HikariCP 4.0.3, logback-classic 1.3.14, postgresql 42.7.3.

## [6.2.1] - 2024-03-05
Querease 6.2.2. Do not validate readonly fields. Result serializer string chunker fix.
Scala 2.13.13 / 2.12.19, logback-classic 1.2.13, postgresql 42.5.5.

## [6.2.0] - 2023-02-24
Buffered streams fixes. Do not reflect HTML from a bad URI (XSS).

## [6.1.2] - 2023-02-03
Business tests `full_compare` option for result filter testing. Rest client accepts
HTTP 206 Partial Content.

## [6.1.1] - 2022-11-16
Querease 6.2.1, tresql 11.2.2.

## [6.1.0] - 2022-11-10
Mojoz 4.2.0, querease 6.2.0, tresql 11.2.1.

## [6.0.4] - 2022-10-16
Update does not use savable as filter for the old-value getter. Querease 6.1.4 with save
fix for recursive hierarchy. Scala 2.13.10.

## [6.0.3] - 2022-09-23
`NoResult` as null; unwrap `IdResult` if key is defined. Number-like string keys.
Variable removal from action data. FileBufferedFlow starts a new buffer file after
downstream catches up. Querease 6.1.3, tresql 11.1.3. Akka 2.6.20, Akka HTTP 10.2.10.

## [6.0.2] - 2022-08-17
Tresql 11.1.2.

## [6.0.1] - 2022-08-16
Querease 6.1.2. Optional millis for timestamps in serializer and business tests.
Rest client allows key and query in path. `afterWabaseAction` call fix.
XlsXml / CSV renderer flush fix.

## [6.0.0] - 2022-08-09
View actions (querease actions) become the main way to define get / list / insert / update /
save / delete / count / create: `if` / `foreach`, `status`, `redirect this`, `this` view,
`commit`, named steps, invocation. CRUD by key in the path or query string; `count:view`
and `create:view` URI segments; hidden (api-excluded) keys; datetime keys.
`useLegacyFlow(viewName, actionName)` for applications that still use the old handlers.

Querease 6.1.1, tresql 11.1.1. Marshalling of querease results according to the view
without spray-json DTOs. `java.time.LocalDate` / `LocalTime` / `LocalDateTime` and
`java.sql.Time`. Datasource initialization from factory functions. `NotFoundException`
on missing row for save/delete, handled by exception handlers (http 400 for
`BusinessException`). `authFieldNames` automagic (`is_update_relevant`,
`is_delete_relevant`) removed. `getByNamePath` detached and deprecated.
`defaultApiRoleName` already `ADMIN` from 2.0.0.

Scala 2.13.8 / 2.12.16, Akka 2.6.19, Akka HTTP 10.2.9.

## [5.5.0] - 2024-04-07
`uploadMultipleAsSource` directive. Querease 5.1.0. Akka 2.6.21, Akka HTTP 10.2.10,
HikariCP 4.0.3, logback-classic 1.3.14, postgresql 42.7.3. Scala 2.13.13 / 2.12.19.
JDBC drivers loaded sequentially from one place.

## [5.4.1] - 2024-01-22
CSRF errors include the requested URL.

## [5.4] - 2024-01-18
CSRF throws exceptions instead of rejections; CSRF exceptions logged at info.

## [5.3.2] - 2022-07-27
Business tests SQL date format fix.

## [5.3.1] - 2022-01-18
`BusinessException` message auto-format for `printStackTrace()`; use `messageTemplate`
for the unformatted message. `Dto.fill(JsObject)` error handling and type conversion
improvements. Audit does not log business exceptions as errors.

## [5.2.0] - 2021-09-03
Rest client: explicit timeout on HTTP requests and better timeout error handling.

## [5.1.1] - 2021-07-22
Akka 2.6.15, postgresql 42.2.23, Scala 2.12.14.

## [5.1.0] - 2021-06-01
Querease 5.0.1. Unwrap `SqlException` from `ChildSaveException` for constraint messages.
Handle `PostgresTimeoutException` wrapped in `TresqlException`. Rest client redirect
logging and errors. Scala 2.13.6, Akka 2.6.14, Akka HTTP 10.2.4.

## [5.0.1] - 2021-05-21
Tresql 10.1.2. `uploadMultiple` directive (fixed content type, default filename).
CSFR renamed to CSRF. CSV export escapes field labels. Executor removed from list
methods and list context. Akka 2.6.11, Akka HTTP 10.2.3.

## [4.0.5] - 2021-05-21
Reverts closing the list connection on service failure (4.0.4) to restore binary
compatibility with 4.0.3.

## [4.0.4] - 2021-05-20
Close list connection on service failure (reverted in 4.0.5).

## [4.0.3] - 2021-03-06
Audit `viewContext` refactoring. Error-on-error processing.

## [4.0.2] - 2021-02-24
`SameSite` attribute on cookies. JSON converter preserves field order. Scala 2.12.13.

## [4.0.0] - 2021-01-20
Deferred request timeout limit (`deferredTimeout`, `extractTimeout` on crud action).
Content-Disposition header fixes; fallback filename strips accents.
`decodeParam` sanitizes the message on exception (reflected XSS).
Get with implied id if list api is not defined. OdsStreamer flexibility.

## [3.0.2] - 2020-12-14
Scala 2.13.4, Akka HTTP 10.2.2. sbt-version-policy / `versionPolicyCheck`, semver.

## [3.0.1] - 2020-11-24
Tresql 10.1.1.

## [3.0.0] - 2020-11-24
Rest client method rename: `httpGet` / `httpPost` / `httpGetAsync` / `httpPostAsync` to
`httpGetAwait` / `httpPostAwait` / `httpGet` / `httpPost`. Rest client errors bubble the
causing status. Db constraint message localization (`PostgreSqlConstraintMessage`).
Tresql resources re-initialized from template when taking a connection from the pool.
Child-view save uses the proper field when parent and child share a name.
Akka 2.6.10, Akka HTTP 10.2.1, HikariCP 3.4.5.

## [2.0.0] - 2020-09-22
I18n: property bundles, chaining, UTF-8 `.properties`, `initI18n`, locale on validation.
Default api role name `ADMIN` (`defaultApiRoleName`). `ApplicationState` case class instead
of a raw `Map`. Querease 5.0.0, validations as query strings (stack overflow protection).
Validation exception handler. Child view validation. Dynamic error messages (javascript if
the message starts with a quote). `getByNameAction` uses application get instead of list.
Akka 2.6.9, Akka HTTP 10.2.0. Cross-build Scala 2.12.12.

## [1.0.0] - 2020-08-03
Initial release. Akka HTTP JSON REST services on querease / tresql SQL. File upload and
download, deferred requests, request audit, stateless session, field validations, pluggable
business logic. Scala 2.13.3, Akka 2.6.5, Akka HTTP 10.1.12, mojoz 1.2.1, querease 4.0.0,
tresql 10.0.0.
