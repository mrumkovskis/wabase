# Async Processing: Jobs, Deferred Requests, Events, Audit

This chapter documents asynchronous and background execution capabilities.

## 1. Deferred HTTP Requests

Core runtime:
- `src/main/scala/DeferredControl.scala`
- `src/main/scala/WabaseDeferredControl.scala`
- `src/it/resources/routes/deferred.yaml`
- `src/it/resources/tables/deferred.yaml`

Primary handlers:
- `maybeDeferred`
- `doDeferred`
- `deferredResult`

Deferred status model:
- `QUEUE`, `EXE`, `OK`, `ERR`, `DEL` (+ duplicate marker behavior)

Typical route shape:

```yaml
on: GET /slow-respond
do: authenticateOpt maybeDeferred sleepAndRespond(500) response(200, 'slow done')

on: GET /deferred-result/{deferred_id}
do: authenticateOpt deferredResult($deferred_id)
```

Operational notes:
1. Configure worker counts with available DB pool capacity in mind.
2. Configure cleanup interval for deferred table growth control.
3. Keep deferred storage tables and file bodies in backup scope.

## 2. Background Jobs and Scheduling

Core runtime:
- `src/main/scala/WabaseScheduler.scala`
- `src/main/scala/scheduler/QuartzScheduler.scala`
- `src/it/resources/routes/job.yaml`
- `src/it/resources/views/job.yaml`
- `src/it/resources/tables/cron_job.yaml`

Key behaviors:
1. Job definitions are view-driven.
2. A status table lock mechanism prevents duplicate concurrent job runs.
3. Startup hooks can trigger initial jobs.
4. Quartz integration is optional via scheduler initializer.

Route shape:

```yaml
on: POST /start-job/(?<jobname>.*)
do: startJob $1
```

## 3. Server Notifications (SSE / WebSocket)

Core runtime:
- `src/main/scala/EventBus.scala`
- `src/main/scala/EventNotifications.scala`
- `src/it/resources/views/server-events.yaml`

Capabilities:
- Per-user event publication
- SSE subscription streams
- WebSocket message streams
- Initial publication hooks when subscriber connects

Developer guidance:
1. Use typed envelope payloads for stable client contracts.
2. Keep event payloads small and versioned.
3. Add retry/id handling at client side for robust streaming UX.

## 4. Audit Pipeline

Core runtime:
- `src/main/scala/Audit.scala`
- `src/main/scala/audit/Audit.scala`
- `src/main/scala/audit/BufferedAudit.scala`
- `src/it/resources/views/audit.yaml`
- `src/it/resources/tables/audit.yaml`

Modes:
1. Direct audit writes.
2. Buffered queue-based writes for high-throughput scenarios.

Key settings:
- `app.audit-max-content-size`
- `app.audit-pool-name`
- `app.audit-queue.path`

## 5. Email and Outbound HTTP in Actions

Relevant files:
- `src/main/scala/WabaseEmail.scala`
- `src/main/scala/client/HttpClient.scala`
- `src/main/scala/client/RestClient.scala`
- `src/main/scala/client/WabaseHttpClient.scala`

Use cases:
- Triggering external APIs in action steps.
- Sending transactional emails from view workflows.

Guidance:
1. Keep external calls timeout-bounded (`http-client.request-timeout`).
2. Surface remote errors as business/HTTP exceptions with context.
3. Cover outbound paths in integration tests with deterministic stubs when possible.

## 6. Async Reliability Checklist

1. Separate DB pools for main traffic, audit writes, and heavy background paths.
2. Use bounded deferred worker counts.
3. Add monitoring on deferred backlog size and job lock collision counters.
4. Ensure queue/storage paths are durable and not shared temp mounts.
5. Validate restart behavior using deferred/job integration scenarios.

## 7. Test Coverage Sources

- `src/test/scala/DeferredTests.scala`
- `src/test/scala/BufferedAuditSpecs.scala`
- `src/it/resources/http_tests/deferred/*.yaml`
- `src/it/resources/http_tests/job/*.yaml`
- `src/it/resources/http_tests/api/*.yaml`

## Feature Guides

*   [Deferred Requests](../features/05-deferred-requests.md)
*   [Background Jobs and Scheduler](../features/06-background-jobs.md)
*   [Auditing](../features/07-auditing.md)
*   [Email Sending](../features/11-email-sending.md)
*   [Outbound HTTP Client Calls](../features/12-outbound-http-client-calls.md)
*   [Server Notifications (SSE/WS)](../features/13-server-notifications.md)
