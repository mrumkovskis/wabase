# Outbound HTTP Client Calls

## Use This When

You need actions that call other HTTP services.

## Core Surface

- `src/main/scala/client/HttpClient.scala`
- `src/main/scala/client/RestClient.scala`
- `src/main/scala/AppMetadata.scala` (`http`/`http_proxy` op)
- `src/main/scala/AppQuerease.scala` (`doHttp`)

## Simple Example

```yaml
get:
- remote = http get {'/health'}
- return :remote
```

## Complex Example

```yaml
insert:
- payload = extract entity
- auth_header = {'Authorization', 'Bearer ' || :token}
- response = as any http [main] post {'/v1/orders'} :payload :auth_header
- status = extract header X-Result-Code :response
- proxy_resp = as any http_proxy [main] get {'/v1/orders/' || :response.id}
- return :proxy_resp + { result_code = :status }
```

```hocon
http-client {
  request-timeout = 10 s
  server-path = "https://api.partner.local/"
}
```

## Key Notes

1. Use named client `[name]` when multiple clients are configured.
2. Header TresQL can set auth and content-type.
3. `http_proxy` mode preserves proxy semantics for downstream handling.

## Related Docs

- `../reference/09-async-jobs-deferred-events.md`
- `../reference/04-configuration.md`
