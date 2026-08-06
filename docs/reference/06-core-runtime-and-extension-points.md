# Core Runtime and Extension Points

This chapter describes how Wabase processes a request end-to-end and where you can safely extend behavior.

## 1. Boot Sequence

Primary startup path:
1. `org.wabase.WabaseServer.main`
2. `ExecutionImpl` creation
3. App instance creation (`new App(executionImpl)` in your project)
4. HTTP bind with optional TLS
5. Request dispatch through `WabaseServer.handle`

Core runtime files:
- `src/main/scala/WabaseServer.scala`
- `src/main/scala/WabaseService.scala`
- `src/main/scala/WabaseApp.scala`
- `src/main/scala/AppBase.scala`

## 2. Request Lifecycle

Typical flow for a route request:
1. Route match (`WabaseService.findRoute` + loaded route defs).
2. Build handler chain (`buildRequestHandlerChain`).
3. Execute handlers (`authenticate`, `doAction`, `response`, custom functions, etc).
4. Decode body if needed (`toMapEntityDecoder`, `toSeqEntityDecoder`, parser registry).
5. Execute view/action via Querease/TresQL.
6. Encode response via renderer selected from content type / result type.
7. Apply error handler (`wabase-error-handler`, optional `wabase-error-handler-and-then`).

## 3. Route Definition Engine

Route definitions are loaded by `YamlRouteDefLoader` from route metadata sources.

Supported path styles:
- Regex paths (`on: /data/(...)`)
- OpenAPI-like params (`on: GET /deferred-result/{deferred_id}`)

Handler argument model:
- Literal args (`'text'`, `123`, `null`)
- Regex group args (`$1`)
- Named path params converted to group references internally
- Injectable params (request context, user, state, maps, actor system, response, etc.)

Relevant files:
- `src/main/scala/YamlRouteDefLoader.scala`
- `src/main/scala/WabaseService.scala`
- `src/it/resources/routes/*.yaml`

## 4. Action and View Dispatch

`doAction` bridges HTTP requests to view actions.

Key behavior:
1. Resolve `view` + `action` from route arg/path/query.
2. Map HTTP method to action (`get/list/insert/update/delete` rules + config overrides).
3. Apply request max-size/timeout from view metadata.
4. Decode request body to map/list values when needed.
5. Call app action pipeline (`AppBase.rest` with auth+audit wrappers).

Core files:
- `src/main/scala/WabaseService.scala`
- `src/main/scala/AppBase.scala`
- `src/main/scala/AppQuerease.scala`

## 5. App Action Pipeline (AppBase)

`AppBase` provides default implementations for CRUD and metadata generation.

Action hooks:
- `before(...)`
- `on(...)`
- `after(...)`

Default behaviors:
- `defaultCreate`
- `defaultList`
- `defaultSave`
- `defaultRemove`
- `validateFields` for required/length/enum/email checks

Extension model:
- Override `create*Ctx` methods for context mutation.
- Override `create*Result` methods for output shaping.
- Register custom hook chains for specific request contexts.

## 6. Metadata and API Surface

Wabase generates API metadata from view definitions and authorization context.

Key APIs:
- `api` endpoint (`WabaseService.api`)
- `metadata` endpoint (`WabaseService.metadata`)
- `swaggerJson` / `swaggerYaml` endpoints

Supporting files:
- `src/main/scala/AppMetadata.scala`
- `src/main/scala/WabaseService.scala`
- `src/main/scala/swagger/WabaseSwaggerGenerator.scala`
- `src/main/scala/swagger/SwaggerMerger.scala`

## 7. DB and Resource Management

DB access is abstracted through `DbAccess` and resource factories.

Patterns in runtime:
- Per-request resources initialized for relevant pools.
- Action-level DB switching via view/action metadata.
- Optional evaluator fallback pool (`app.wabase.evaluator.pool`).

Core files:
- `src/main/scala/DbAccess.scala`
- `src/main/scala/ds/ConnectionPools.scala`
- `src/main/scala/ds/HikariDs.scala`
- `src/main/scala/TresqlResourcesConf.scala`

## 8. Common Extension Points

| Area | Extension Point | File |
| :--- | :--- | :--- |
| Handler aliases | `app.wabase-call-alias` | `src/main/resources/reference.conf` |
| Request decoders | `request-decoders.factory-class` | `src/main/scala/RequestDecoder.scala` |
| Result renderers | `result-renderers.factory-class` | `src/main/scala/ResultEncoder.scala` |
| Error handling | `app.wabase-error-handler` | `src/main/scala/WabaseErrorHandler.scala` |
| Auth strategy | `app.wabase-authorization` | `src/main/scala/Authorization.scala` |
| Injection provider | `app.wabase-injection-parameters-provider-factory` | `src/main/scala/AppQuerease.scala` |
| Logger naming | `app.wabase-logger-name-factory` | `src/main/scala/WabaseService.scala` |

## 9. Practical Customization Example

```yaml
on: POST /custom-action
# auth + custom input mapping + action call + explicit response
# each token is resolved via handler alias / fully-qualified method
#do: authenticateOpt myCustomMapper doAction('my_view.my_action') response(200, 'ok')
```

```hocon
app.wabase-call-alias {
  myCustomMapper = com.example.MyRouteHandlers.mapInput
}
```

## 10. Developer Notes

1. Prefer alias mappings over hard-coded class names in route YAML.
2. Keep route handlers side-effect free unless they are explicit workflow endpoints.
3. Test route parsing edge cases (regex groups, named params, optional methods) in isolated route loader specs.
4. When extending `AppBase`, keep hook behavior deterministic and avoid hidden DB transaction assumptions.
