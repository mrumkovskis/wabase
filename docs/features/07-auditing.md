# Auditing

## Use This When

You need persistent request/response trace records.

## Core Surface

- `src/main/scala/audit/Audit.scala`
- `src/main/scala/audit/BufferedAudit.scala`
- `src/it/resources/views/audit.yaml`

## Simple Example

```yaml
on: /data/(\w+)(/.+)?
do: authenticateOpt audit doAction $1
```

```hocon
app.audit-pool-name = "audit_write"
```

## Complex Example

```hocon
app {
  audit-pool-name = "audit_write"
  audit-max-content-size = 256 K
  audit-queue.path = "/var/lib/wabase/audit-queue"
}
```

```yaml
name: audit
table: audit
api: list
fields:
- request_time
- request
- response
```

## Key Notes

1. Buffered audit mode is better for high write volume.
2. Keep audit queue path on durable local storage.
3. Limit recorded payload size with `audit-max-content-size`.

## Related Docs

- `../reference/09-async-jobs-deferred-events.md`
