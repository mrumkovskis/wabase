# Swagger

OpenAPI 3.1 document generated from [routes](routes.md) and [view](view-actions.md)
definitions. YAML `swagger` extras on routes, views and fields are merged into the
generated paths and schemas.

## Serving

`MetadataHandlers`

| Alias | Description |
| --- | --- |
| `swaggerJson` | Returns generated `swagger.json`. |
| `swaggerYaml` | Returns generated `swagger.yaml`. |

Both are conditional — they respond `304` on matching `If-None-Match` /
`If-Modified-Since`, using metadata version and application startup time. `app.host`
must be set; it is written to `servers`.

```yaml
on: GET /swagger\.json
do: swaggerJson

on: GET /swagger\.yaml
do: swaggerYaml
```

The document is generated on each request that is not answered from the cache
headers. Generation is not filtered by the caller's roles — it describes the full
api.

## Generation

`WabaseService.createSwaggerGenerator` builds a generator from
`app.wabase-swagger-generator-factory` (default
`org.wabase.WabaseSwaggerGeneratorFactory`). The factory receives the request
context and returns a `WabaseSwaggerGenerator`.

Default generator:

- OpenAPI 3.1.0
- empty `info` (`title`, `description`, `version`, `termsOfService` are empty
  strings)
- `servers` from `app.host`
- no security schemes
- views that declare `api` — paths they expose; schemas only when `$ref`'d from a
  generated path, including nested `$ref`s of those schemas
- all routes

Path contributions from routes are merged first, then views — views win on
conflicts. Each source's `swagger` extra is applied only to the paths and http
methods that source contributed. See [Merge](#merge).

## Routes

A route contributes one OpenAPI path per [path name](#path-names) extracted from
its `on` pattern, with an operation for each http method listed on the route. If
`on` has no methods, every method is generated — `GET`, `POST`, `PUT`, `PATCH`,
`DELETE`, `HEAD`, `OPTIONS`, `TRACE`.

Default route operations are thin: path parameters from the pattern, and a success
response — `200` for `GET` / `HEAD` / `OPTIONS` / `TRACE` / `PATCH` / `PUT`,
`201` for `POST`, `204` for `DELETE`. [Overrides](#overrides) fill in summary,
body, error responses and the rest.

Catch-all routes that only dispatch to views usually skip swagger, because the
views already generate the real paths:

```yaml
on: /data/(\w+(?::(?:new|count))?)(/.+)?
do: authenticateOpt audit doAction $1
swagger:
  paths = : {}
```

### Path names

`on` accepts a regexp or an OpenAPI-style path.

An OpenAPI-style path is used as the swagger path. `{name}` segments become path
parameters of type string:

```
/users/{id}
/report.{format}
/{a}/{b}/c{d}e
```

A regexp is converted to one or more OpenAPI paths:

| Regexp | Swagger path | Parameters |
| --- | --- | --- |
| `/hello` | `/hello` | — |
| `/a/(\\w+)` | `/a/{p1}` | `p1` with pattern `^\\w+$` |
| `/data/(?<name>\\w+)/(?<id>\\d+)` | `/data/{name}/{id}` | named groups keep their names |
| `/a\|/b` | `/a` and `/b` | — |
| `/a(/\\w+)?` | `/a` and `/a/{p1}` | optional slash group yields two paths |

Unnamed capturing groups are `{p1}`, `{p2}`, … A regexp that cannot be converted
is used as a single path name with no parameters.

## Views

A view with `api` contributes paths for the actions it exposes. Http methods follow
[action mapping](view-actions.md#http-method-mapping). `GET` also produces `new`
and `count` when those actions are in `api`. Only key sizes accepted by
[action resolution](view-actions.md#action-resolution) are generated.

### View paths

If the view has no `paths` extra, the resource path is
`{prefix}{viewName}`:

| Prefix | Config | Default |
| --- | --- | --- |
| Ordinary views | `app.views-api.uri-prefix` | `/` |
| Public views | `app.public-api.views-uri-prefix` | `/public` |

`count` and `new` add `:{action}` to that path — `/person:count`, `/person:new`.
Key fields are appended as `{field}` segments — `/person/{code}` — the OpenAPI
form of the resource key, including when `app.key-in-query` is true at runtime.
A view that declares `paths` uses those instead; a path that does not start with
`/` is prefixed with `app.views-api.uri-prefix`.

A view served from a dedicated route typically skips its own generated paths, so
that only the route path remains:

```yaml
name:     current_user
api:      get
paths:    /current-user
swagger:
  paths = : {}
```

```yaml
on: GET /current-user
do: authenticate doAction('current_user')
swagger:
  get:
    summary: Get current user
    description: Returns current user info
    responses:
      '200':
        content:
          application/json:
            type: current_user
```

### Operations

Default summary is `{Action} '{view}' by '{key fields}'`. Description is the view
comment, except `delete` which has none.

| Action | Request | Success | Errors |
| --- | --- | --- | --- |
| `get`, `new` | — | `200` view schema | `400`, `403`, `404`, `503` |
| `list` | — | `200` array of view schema | `400`, `403`, `503` |
| `count` | — | `200` integer, `text/plain` | `400`, `403`, `404`, `503` |
| `insert`, `update`, `update+`, `upsert`, `save`, `put`, `post` | json view schema | `200` and `201` | `400`, `503` |
| `delete` | — | `204` | `404` |
| `head`, `options` | json view schema | — | `400`, `503` |

`403` is omitted for public views. Path parameters are the key fields used by that
path. `list` adds query parameters from the view filter, excluding internal
parameters.

When `app.marshal_key_as_json` is true (the default), insert-style success
responses use schema `{view}_key_response` — the api key fields — instead of the
view body.

## Schemas

`components.schemas` holds only types reachable from generated paths: a `$ref`
from a path (or from a schema already included) to `#/components/schemas/{name}`.
A view with `api` and fields is omitted when no path `$ref`s it — typically
when its `swagger` extra skips paths (`paths = : {}`) and no route `$ref`s the
view. Nested complex-type views are included when a reachable schema `$ref`s
them. `{view}_key_response` is included when an insert-style success response
`$ref`s it.

A skipped-path view that is still used, such as `current_user` served from a
dedicated route, must be `$ref`'d from that route's swagger extra.

The schema itself is an object for a view that has at least one field not marked
`field api: excluded`, and `{view}_key_response` when insert-style actions
return a json key.

| Field | Schema |
| --- | --- |
| Simple type | OpenAPI type — `string`, `integer` / `int64`, `number`, `boolean`, `date`, `date-time`, … |
| `string` with length | `maxLength` |
| Collection | `array` of the field type |
| Complex type | `$ref` to that view |
| Enum | `enum` on the schema |
| Not insertable and not updatable | `readOnly` |
| Required or not nullable, and not read-only | listed in `required` |

Field description is comments, otherwise the label. A view named `count` is not
emitted as an object schema.

Field `swagger` is merged into that field's schema:

```yaml
fields:
- password:
    swagger:
      example: password
```

## Overrides

The `swagger` extra is a map. Each root key is classified and applied at its own
level. Values are OpenAPI fragments. Keys that end with space and `=` (` =`)
replace the subtree instead of deep-merging; replace with `null` removes that key.

| Root key | Level | Applied to |
| --- | --- | --- |
| `paths` | document | contributed path map of this source |
| starts with `/` | path | that path, if this source contributed it |
| `get` / `post` / `put` / `delete` / `options` / `head` / `patch` / `trace` | method | that method on every contributed path that already has it |
| `200` / `default` / `2XX` | response | `responses` of every contributed operation |
| any other | operation | every contributed operation |

Quote status codes in YAML so they stay strings: `'200'`, `'2XX'`.

### Skip and replace paths

```yaml
swagger:
  paths = : {}
```

This source contributes no paths. Remaining overrides have nothing to apply to.
The view is also omitted from `components.schemas` unless another generated path
`$ref`s it.

```yaml
swagger:
  /localized-data/save_person_email = :        null
  /localized-data/save_person_email/{code} = : null
```

Removes those paths from this view. `paths:` on the view still serves them.

`swagger: null` on a view or route cancels a `swagger` extra inherited from a
parent view.

### Operation and response

Method-level override — only methods already present are updated, new methods are
not invented:

```yaml
on: GET /(user-with-roles-array)
do: doAction $1
swagger:
  get:
    summary: Get user with roles
    description: Returns user info
    responses:
      '200':
        content:
          application/json:
            schema:
              type: object
              properties:
                name:
                  type: string
                  example: John
                roles:
                  type: array
                  example: [admin, guest]
                  items:
                    type: string
```

Response-level override applies to every method of the source:

```yaml
on: GET /hello
do: response(200, 'Hello from wabase!')
swagger:
  '200':
    content:
      text/plain:
        type: string
```

Replace the whole `responses` map — default `200` is dropped:

```yaml
swagger:
  responses = :
    '200': {}
    '401': {}
```

```yaml
on: GET /slow-respond
do: authenticateOpt maybeDeferred sleepAndRespond(500) response(200, 'slow done')
swagger:
  responses = :
    '202':
      description: The request has been accepted for processing, but the processing has not been completed. The response contains a deferred identifier.
      content:
        application/json:
          schema:
            type: object
            required:
              - deferred
            properties:
              deferred:
                type: string
                example: hGLDObRtjluGLjD4mrxZX2rIOaI=
```

An empty response description is filled in from the http reason phrase after
merge — `200` becomes `OK`, `201` `Created`, `204` `No Content`.

### Types

In a schema position, a string is an OpenAPI type or a view name:

| Value | Result |
| --- | --- |
| `string`, `integer`, `number`, `boolean`, `array`, `object`, `null` | `{ type: … }` |
| view name | `$ref: '#/components/schemas/{view}'` |
| other name | schema from `typeNameToSchema` — same mapping as view fields |

Under a media type key (`application/json`, `text/plain`, …), a map that contains
`type` is the schema of that media type — `schema:` may be omitted:

```yaml
'200':
  content:
    application/json:
      type: person
```

`items: string` is `items: { type: string }`. `schema = :` replaces the schema
instead of merging properties.

### Parameters

`parameters` is merged by `name` and `in`. A map form defaults `in` to `query`:

```yaml
swagger:
  get:
    parameters:
      limit:
        in: query
        schema:
          type: integer
```

An array form is the OpenAPI list. `parameters = :` replaces the list.
`"limit =": null` inside a parameters map removes parameters named `limit` (any
`in`). Plain `limit: null` without ` =` does not remove.

## Merge

`SwaggerMerger.mergePathSources` walks sources in order — routes, then views.

1. `paths` / `paths =` decide which paths this source contributes (`paths = : {}`
   → none).
2. Contributed path items are deep-merged; later sources win.
3. Each source's remaining overrides are applied only to that source's paths and
   http methods, then written back without dropping sibling operations from other
   sources.

Path-level override keys for paths this source did not generate are ignored. A
child view that `extends` a parent therefore does not recreate or overwrite the
parent's path through inherited `swagger`.

When several sources contribute the same path and method, the later source's
operation is deep-merged over the earlier one. A view `responses =` after that
merge replaces the merged responses, so route-only status codes are dropped.

## Custom generator

Subclass `WabaseSwaggerGenerator` and return it from a factory:

```scala
trait WabaseSwaggerGeneratorFactory {
  def createSwaggerGenerator(ctx: WabaseRequestContext): WabaseSwaggerGenerator
}
```

```
app.wabase-swagger-generator-factory = com.example.MySwaggerGeneratorFactory
```

Overridable on the generator: `info`, `host`, `schemes`, `basePath`, `components`,
`security`, `securitySchemes`, `externalDocs`, `vendorExtensions`, `specVersion`
(default `V31`). `WabaseDefaultSwaggerGenerator` already drops internal query
parameters; keep that filter if the factory is replaced.

## Configuration

| Setting | Default | Purpose |
| --- | --- | --- |
| `app.host` | `null` | Api url written to `servers`. Required to serve swagger. |
| `app.wabase-swagger-generator-factory` | `org.wabase.WabaseSwaggerGeneratorFactory` | Creates the generator. |
| `app.marshal_key_as_json` | `true` | Insert-style success content uses `{view}_key_response`. |
| `app.views-api.uri-prefix` | `/` | Default view path prefix. |
| `app.public-api.views-uri-prefix` | `/public` | Path prefix for public views. |
| `app.action-for-http.post` | `post` | Default action for view `POST`. |
| `app.action-for-http.put` | `put` | Default action for view `PUT`. |
