# Static Resources and Cache Controls

## Use This When

You need to serve static assets and support cache-aware responses.

## Core Surface

- `src/main/scala/WabaseService.scala` (`getFromResource`)
- `src/main/scala/CacheConditionHandlers.scala`
- `src/main/scala/CacheIo.scala`
- `src/it/resources/routes/static-resources.yaml`

## Simple Example

```yaml
on: GET /
do: getFromResource('web', '/index.html')

on: GET (?<filename>/.+\.(?:css|js|png|svg|woff2))
do: getFromResource('web', $1)
```

## Complex Example

```yaml
on: GET /assets/(?<filename>/.+\.(?:js|css|png))
do: getFromResource('web', $1)
recover: status 404
```

```http
GET /scripts/app.js
If-None-Match: "etag-value"
If-Modified-Since: Tue, 18 Feb 2026 10:00:00 GMT
```

Expected behavior: conditional cache validation can return `304 Not Modified` when conditions match.

## Key Notes

1. Keep static routes separate from `/data` and API routes.
2. Use cache condition headers for bandwidth reduction.
3. Validate content-type mapping for all served extensions.

## Related Docs

- `../reference/08-input-output-and-renderers.md`
- `../reference/05-routes.md`
