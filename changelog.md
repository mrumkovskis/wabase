# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
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
