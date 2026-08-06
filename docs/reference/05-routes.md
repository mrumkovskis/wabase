# Routes Reference

The **Routes Engine** maps incoming HTTP requests to Handlers. It is configured via `routes.yaml` (or other files specified in metadata configuration).

## Route Definition Syntax

A route definition consists of:
1.  **`on`**: The matching criteria (HTTP method and path).
2.  **`do`**: The handler chain to execute.
3.  **`recover`** (optional): Error handler.

```yaml
on: [<space separated http methods>] <path regexp> | <openapi style path>
do: <handler expression>
[recover: <error handler expression>]
```

### Path Matching

*   **Regular Expressions**: The path is treated as a regex. Groups can be captured using `()`.
*   **Method Matching**: If HTTP methods are specified (e.g., `GET POST`), only those match. If omitted, all methods match.

**Examples**:
```yaml
# Match /api/users or /api/users/123
on: /api/users(/.+)?

# Match only GET requests
on: GET /slow-respond

# Capture group for dynamic action
on: /data/((?:create:|count:)?\w+)(/.+)?
```

## Handlers

Handlers are Scala/Java functions that process the request. They can be chained.

### Syntax
`handler1 [args] handler2 [args] ...`

*   **Ordered Parameters**: Passed explicitly in the `do` expression.
*   **Injectable Parameters**: Provided automatically by the framework (e.g., `HttpRequest`).
*   **Chaining**: The result of one handler can be passed to the next.

**Arguments**:
*   String or Number constants: `'value'`, `123`.
*   Path Group References: `$1`, `$2` (referring to regex groups in `on`).
*   Inner Handler Invocation: The last parameter can be another handler call.

**Example**:
```yaml
do: authenticateOpt doAction $1
```
Here, `authenticateOpt` is called first. If successful, it calls `doAction`, passing `$1` (the first regex group) as an argument.

### Handler Implementation

Handlers are functions defined in Scala objects. They must have a specific signature allowing for dependency injection.

**Injectable Parameter Types**:
The framework automatically injects these types if requested by the handler function:
*   `WabaseRequestContext`
*   `HttpRequest`
*   `HttpResponse` or `Future[HttpResponse]` (for response transformers)
*   `ActorSystem`, `ExecutionContext`
*   `WabaseUser` (if authenticated)
*   `ApplicationState`
*   `Uri`
*   `Map[String, Any]`, `Seq[Any]` (Decoded request entity)

**Return Types**:
Handlers can return:
*   `Future[HttpResponse]` (Standard result)
*   `WabaseRequestContext` (Context transformer)
*   `Dto`, `Map`, `List` (Data to be serialized)
*   `RequestHandler` (Function `WabaseRequestContext => Future[HttpResponse]`)

### Built-in Handlers

Wabase provides standard handlers (often aliased in `application.conf`):

| Alias | Description |
| :--- | :--- |
| `doAction` | Main dispatch entry point. Extracts view/action from path/args. |
| `authenticate` | Enforces authentication. |
| `authenticateOpt` | Optional authentication (continues even if anonymous). |
| `maybeDeferred` | Handles `Deferred` header processing. |
| `api` | Returns API metadata. |

### Handler Aliases

You can map short names to fully qualified class methods in `application.conf`:

```hocon
app.wabase-call-alias {
  myHandler = com.example.MyObject.myHandlerMethod
}
```

## The `doAction` Handler

The standard `doAction` handler is the bridge to the **View/Action** engine.

**Signature**: `view_action` (Ordered parameter).

**Logic**:
1.  **Extracts**: View name, Action name, Key from path/arguments.
2.  **Method Mapping**:
    *   `GET` -> `get` (if key present) or `list`.
    *   `POST` -> `insert` (or `save`).
    *   `PUT` -> `update`.
    *   `DELETE` -> `delete`.
3.  **Context**: Builds `AppActionContext` with `WabaseUser`, `ApplicationState`.
4.  **Delegation**: Calls `wabase.doWabaseAction`.

## Example Configurations

```yaml
# CRUD for data views
on: /data/((?:create:|count:)?\w+)(/.+)?
do: authenticateOpt doAction $1

# Custom logic endpoint
on: POST /custom-logic
do: com.example.MyService.handleCustomLogic

# Static response
on: GET /ping
do: response(200, 'pong')
```

## Related Deep Dives

*   [Core Runtime and Extension Points](06-core-runtime-and-extension-points.md)
*   [Security, Authentication, and CSRF](07-security-authentication-and-csrf.md)
*   [Async Processing (Deferred/Jobs/Events)](09-async-jobs-deferred-events.md)
*   [Feature Guide: Views, Routes, and CRUD API](../features/03-views-routes-and-crud.md)
