# Route processing

Routes are defined in YAML files under `routes/`:

```
on: [<space separated http method(s)>] <path regexp> | <openapi style path>
do: <handler expression>
[recover: error handler]
```

## Handlers

Handler as a type:

```scala
type RequestHandler = WabaseRequestContext => Future[HttpResponse]
```

Handler as a handler chain member in handler expression:

```
<member 1> <member 2> … <member N>
```

Handler chain resulting type is `RequestHandler`.

Handlers can have ordered and injectable (implicit) parameters. Ordered parameters
must be specified in route `do` expression, injectable parameters are provided
automatically.

An ordered parameter can be a string constant, a number constant, `null`, or a
reference to a path regexp group like `$<group nr>`. Such parameters are put in comma
separated parentheses, for example `response(200, 'slow done')`.

The last ordered parameter can instead be an inner handler invocation. It is written
after the parentheses rather than inside them — that is what forms a handler chain, so
in `maybeDeferred sleepAndRespond(500)` the handler `sleepAndRespond(500)` is the last
ordered parameter of `maybeDeferred`.

Example:

```yaml
on: /data/(\w+(?::(?:new|count))?)(/.+)?
do: authenticateOpt doAction $1

on: GET /slow-respond
do: authenticateOpt maybeDeferred sleepAndRespond(500) response(200, 'slow done')

on: POST PUT /request-info
do: wabase.app.BusinessScenariosSpecs.requestInfo
```

`sleepAndRespond` above is not a wabase handler — it is an application defined alias, as
declared in the integration test `reference.conf`. See [Handler aliases](#handler-aliases).

### Handler implementation

Handlers are implemented as scala/java functions.

Since handlers can be chained, each chain element can be a context transformer,
response transformer, handler with inner handler or simple handler.

Handlers can have following parameters:

1. `WabaseRequestContext`
2. `HttpRequest`
3. `HttpResponse` or `Future[HttpResponse]` — this is response transformer
4. `ActorSystem`
5. `ExecutionContext`
6. `RequestHandler`
7. `WabaseUser`
8. `ApplicationState`
9. `Uri`
10. `Map[String, Any]`, `Seq[Any]`, `java.util.Map[_, _]`, `java.util.List[_]`, `String` — as decoded request entity

Context transformer handlers can return values of following types or `Future` of them:

1. `WabaseRequestContext`
2. `HttpRequest`
3. `Uri`
4. `ApplicationState`
5. `WabaseUser`

Other types of handler can return following types or `Future` of them:

1. `HttpResponse`
2. `String`
3. Different types of scala and java collections — `Map`, `List`, `Iterable`, `Option` …
4. `Dto`
5. `RequestHandler`

Handler with inner handler typically returns handler.

### Handler aliases

Default handler aliases are defined in `reference.conf` parameter section
`app.wabase-call-alias`. Application can define its own handler aliases.

An alias without a dot is looked up in `app.wabase-call-alias`, and resolution is
recursive, so an alias may point to another alias. A name containing a dot is treated
as a fully qualified `<class or object>.<function>` and used as is — that is why
`do: wabase.app.BusinessScenariosSpecs.requestInfo` in the examples above needs no
alias.

Note that scala default parameter values are not applied, because handlers are invoked
reflectively. Every ordered parameter must be given explicitly, for example
`hstsHeaders(31536000, 'true')`.

## Provided handlers

Handlers below are grouped as in `app.wabase-call-alias`. Unless stated otherwise they
are implemented in `org.wabase.handlers`.

### Audit

`org.wabase.audit.Audit`

| Alias | Description |
| --- | --- |
| `audit` | Takes inner handler, records request and response of the wrapped chain to the audit db pool (`audit-pool-name`). |

### CSRF defence

`CSRFHandlers`

| Alias | Description |
| --- | --- |
| `checkCsrfToken` | Request transformer. Requires `XSRF-TOKEN` cookie to match `X-XSRF-TOKEN` header, throws `CSRFException` otherwise. |
| `checkSameOrigin` | Request transformer. Requires request `Origin` or `Referer` to match `app.host` (or `X-Forwarded-Host` when `app.host` is not set). |
| `setCsrfCookie` | Response transformer. Sets `XSRF-TOKEN` cookie to a fresh unique value. Cookie can be adjusted via `app.csrf.cookie-transformer`. |
| `deleteCsrfCookie` | Response transformer. Deletes `XSRF-TOKEN` cookie. |

### I18n

`I18nHandlers`

| Alias | Description |
| --- | --- |
| `setLanguage(lang)` | Response transformer. Sets application state language cookie. |
| `i18nTranslate(name, key, params)` | Translates `key` from bundle `name` in current locale. `params` is a slash separated segment list. |
| `i18nResources` | Returns all i18n resources for current locale. |
| `i18nResourcesFromBundle(bundleName)` | Returns i18n resources of one bundle for current locale. |

Current locale comes from application state, so these are normally preceded by
`extractState` or by an action handler that extracts state itself.

### Metadata

`MetadataHandlers`

| Alias | Description |
| --- | --- |
| `api` | Returns json of api available to current user. |
| `metadata(viewName)` | Returns view metadata json. `*` returns metadata of the whole api. |
| `swaggerJson` | Returns generated `swagger.json`. |
| `swaggerYaml` | Returns generated `swagger.yaml`. |

`metadata`, `swaggerJson` and `swaggerYaml` are conditional — they respond `304` on
matching `If-None-Match` / `If-Modified-Since`, using metadata version and application
startup time.

### Action

`ActionHandlers`

| Alias | Description |
| --- | --- |
| `doAction(view_action)` | Main crud entry point. Resolves view, action and key from `view_action`, request path and http method, then runs the view action and marshals its result. |
| `doRequest(handlerName)` | Invokes handler `handlerName` with key taken from path after the handler name segment. |
| `startJob(jobName)` | `POST` only. Starts named job with query and entity parameters. Responds `200`, `409` when job is already running, `404` when job is not found. |

`view_action` is `<view name>` optionally suffixed with `:count` or `:new`. Without
a suffix the action is chosen by http method — `GET` to get, `DELETE` to delete, `HEAD`
to head, `OPTIONS` to options, and `POST` / `PUT` according to `app.action-for-http.post`
and `app.action-for-http.put`.

### Authentication

`AuthenticationHandlers`

| Alias | Description |
| --- | --- |
| `authenticate` | Takes inner handler. Requires valid session, fails with `AuthenticationException` otherwise. Renews session cookie on successful response. |
| `authenticateOpt` | Same, but a missing or invalid session is not an error — inner handler simply runs without user. |
| `authenticateDomainAndPath(domain, path)` | `authenticate` with explicit session cookie domain and path. |
| `authenticateDomainAndPathOpt(domain, path)` | `authenticateOpt` with explicit session cookie domain and path. |
| `checkRole(role)` | Context transformer. `401` when there is no user, `403` when user does not have the role. |
| `extractBasicHttpCredentials` | Returns `WabaseUser` holding credentials from `Authorization: Basic` header. |
| `extractFormDataCredentials` | Returns `WabaseUser` holding credentials from request entity. |
| `extractJwtTokenCredentials` | Returns `WabaseUser` holding claims of `Authorization: Bearer` jwt token. |
| `setSessionCookie` | Response transformer. Sets encrypted session cookie. |
| `setSessionCookieOpt` | Same, but does nothing when there is no user. |
| `setDomainAndPathSessionCookie(domain, path)` | `setSessionCookie` with explicit cookie domain and path. |
| `setDomainAndPathSessionCookieOpt(domain, path)` | `setSessionCookieOpt` with explicit cookie domain and path. |
| `removeSessionCookie` | Response transformer. Deletes session cookie. |
| `setAnonSessionCookie` | Response transformer. Sets session cookie holding a new anonymous session id. |

The `extract*Credentials` handlers only build a `WabaseUser` carrying credentials — they
do not verify them. Verification belongs to the login view action, which is typically
followed by `setSessionCookie`.

Session cookies are set only on a successful response status.

### Deferred

`DeferredHandlers`

| Alias | Description |
| --- | --- |
| `doDeferred` | Takes inner handler. Always defers — responds `202` with `{"deferred": "<hash>"}` and runs the inner handler in the deferred module. |
| `maybeDeferred` | Takes inner handler. Defers only when request path or request headers ask for it, otherwise runs the inner handler directly. |
| `deferredResult(deferred_id)` | Returns result of a previously deferred request. |

Both require the deferred module to be initialized.

### File

`FileHandlers`

| Alias | Description |
| --- | --- |
| `fileUpload(name)` | Streams request entity into the default file streamer, returns file info map. `name` defaults to `file` when empty. |
| `fileUploadUsing(name, fsName)` | `fileUpload` using named file streamer. |
| `fileDownload(id, hash)` | Responds with file from the default file streamer, `404` when not found. |
| `fileDownloadUsing(id, hash, fsName)` | `fileDownload` using named file streamer. |

`doFileCleanup` is listed in the same block, but it is `org.wabase.FileCleanup.doCleanup`
and takes querease, resources and file streamers — it is a view action function, not a
route handler. Its view definition must have `explicit db: true`.

### Event notifications

`EventNotificationsHandlers`

| Alias | Description |
| --- | --- |
| `subscribeToServerSentEvents(topic)` | Subscribes to topic and responds with a server sent events stream. |
| `subscribeToWebSocketMessages(topic)` | Subscribes to topic and upgrades the request to a web socket. |

### Request

`RequestHandlers`

| Alias | Description |
| --- | --- |
| `extractState` | Context transformer. Builds `ApplicationState` from request cookies named `current_*`, resolving locale from the language cookie or from `Accept-Language`. |
| `keyFromQueryToPath` | Context transformer. Moves key from the special query string form `?/key/parts` into the request path. |

`doAction` applies both itself when they have not been applied earlier in the chain,
so they are needed only in chains that do not go through `doAction`.

### Response

`ResponseHandlers`

| Alias | Description |
| --- | --- |
| `ok` | Responds `200` with empty entity. |
| `status(statusCode)` | Responds with given status and empty entity. |
| `response(statusCode, text)` | Responds with given status and text entity. |
| `responseWithContentType(statusCode, contentType, content)` | Responds with given status, content type and content. |
| `getFromResource(resourcesRootPath, resourcePathAndName)` | Serves a classpath resource, `404` when not found. Supports `If-None-Match` / `If-Modified-Since`. |

### Security headers

`SecurityHeaderHandlers` — all are response transformers. This is the handler chain
counterpart of `org.wabase.SecurityHeaderDirectives`.

| Alias | Description |
| --- | --- |
| `frameHeader(option)` | Adds `X-Frame-Options`, for example `SAMEORIGIN` or `DENY`. |
| `xssHeaders` | Adds `X-XSS-Protection: 1; mode=block` and `X-Content-Type-Options: nosniff`. |
| `hstsHeaders(maxAge, includeSubDomains)` | Adds `Strict-Transport-Security`. |
| `noCacheHeaders` | Adds `Cache-Control: no-cache, no-store, must-revalidate`. |

Example:

```yaml
on: GET /security-headers
do: noCacheHeaders xssHeaders frameHeader('SAMEORIGIN') hstsHeaders(31536000, 'true') response(200, 'ok')
```

### Miscellaneous

The last block of `app.wabase-call-alias` does not hold route handlers. These are
functions callable from view action definitions, aliased so that they can be referenced
by short name there.

| Alias | Description |
| --- | --- |
| `buildCookieHeaderValue` | Builds a `Cookie` header value from a tresql result. |
| `sleep(millis)` | Sleeps. Exists because `Thread.sleep` cannot be invoked directly from an action due to method overload. |
| `startJobAction(jobName)` | Starts named job, returns http status code as int. |
| `toHierarchy(levelParamName, nestedParamName)` | Builds nested maps from a flat, hierarchy ordered tresql result. |
| `publishEvent(topic, value)` | Publishes a value to a server notification topic. |
| `error(msg)` | Fails the action with given message. |
