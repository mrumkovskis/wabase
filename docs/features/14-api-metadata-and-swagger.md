# API Metadata and Swagger

## Use This When

You need generated API metadata and OpenAPI docs.

## Core Surface

- `src/main/scala/WabaseService.scala`
- `src/main/scala/swagger/WabaseSwaggerGenerator.scala`
- `src/main/scala/swagger/SwaggerMerger.scala`
- `src/it/resources/routes/api.yaml`

## Simple Example

```yaml
on: GET /api
do: api

on: GET /swagger\.json
do: swaggerJson
```

## Complex Example

```yaml
on: GET /metadata/(.+)
do: metadata $1

on: GET /swagger-for-redirects\.json
do: generateSwaggerJsonForRedirects
swagger:
  summary: Get swagger json with redirect responses
  responses:
    '200':
      content:
        application/json:
          type: object
```

```hocon
app.host = "https://api.example.com"
```

## Key Notes

1. Route-level swagger fragments merge with generated schema.
2. `app.host` controls generated absolute host/base values.
3. Use metadata endpoint for per-view introspection.

## Related Docs

- `../reference/05-routes.md`
- `../reference/06-core-runtime-and-extension-points.md`
