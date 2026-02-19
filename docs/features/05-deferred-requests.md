# Deferred Requests

## Use This When

You need long-running operations to return immediately with a polling token.

## Core Surface

- `src/main/scala/DeferredControl.scala`
- `src/main/scala/WabaseDeferredControl.scala`
- `src/it/resources/routes/deferred.yaml`

## Simple Example

```yaml
on: GET /slow-respond
do: authenticateOpt maybeDeferred sleepAndRespond(500) response(200, 'done')

on: GET /deferred-result/{deferred_id}
do: authenticateOpt deferredResult($deferred_id)
```

## Complex Example

```hocon
app.deferred-requests {
  enabled = true
  worker-count = 3
  requests = [slow-respond, heavy-report]
  default-timeout = 180s
  timeouts.heavy-report = 600s
  modules.reporting.worker-count = 2
}
```

```http
GET /slow-respond
X-Deferred: 120
```

Returns `202` with JSON `{ "deferred": "<id>" }`.

## Key Notes

1. Tune worker count against available DB connections.
2. Keep deferred storage tables and cleanup configured.
3. Poll with `deferredResult` route using returned token.

## Related Docs

- `../reference/09-async-jobs-deferred-events.md`
