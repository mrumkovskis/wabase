# Action Language Reference

Wabase Actions are defined as a list of steps. Steps are executed sequentially.

## 1. Execution Steps

### Evaluation & Assignment
Evaluate an expression and assign it to a variable.
`[[as result] <variable name> =] [variable transformations ->] <expression>`

*   **`as result`**: The expression value will not be converted to a Tresql bindable value. Useful for HTTP entity values.
*   **Variable Transformations**: `<var1> + (<alias> = <var2>) -> <expression>`.

```yaml
- my_var = user[id = :id] { name }
- as result raw_response = http get 'http://example.com'
```

### Remove Variable
Removes a variable from the current action data scope.
`<variable name> -=`

```yaml
- password -=
```

### Set Environment
Sets the action data (scope) to the expression value.
`setenv [variable transformations ->] <expression>`

```yaml
- setenv get this
```

### Validations
Assert conditions before proceeding.
`validations [<name>] [[<db name>]]:`

```yaml
- validations check_balance:
    - balance > 0, "Insufficient funds"
    - exists(user[id = :id]), "User not found"
```

### Return
Explicitly return a value and stop execution.
`return [variable transformations ->] <expression>`

```yaml
- return :id
```

## 2. Operations (Ops)

### Tresql
Execute a Tresql query. Can be cast to a view.
`[as (any|[`]<view name>[`])[*]] <tresql expression>`

**`build_cursors` macro**: Generates cursors from bind variables matching view structure.
`[build_cursors(this)] accounts{count(*)}`

```yaml
- as `user_view` user[id = :id] { name, email }
- unique_opt |med_db:person[id = :id]
```

### Invocation
Call a static Scala/Java method.
`[as (any|[`]<view name>[`])[*]] <class.method> [<arguments>]`

Arguments can be explicit ops or implicitly injected (e.g., `HttpRequest`, `WabaseUser`).

```yaml
- com.example.Utils.sendEmail(:email, "Welcome")
- org.wabase.QuereaseActionTestManagerObj.person_dtos_list
```

### View Call
Call another view's action.
`[as (any|[`]<view name>[`])[*]] <action> <view name> [<expression>]`

Actions: `get`, `list`, `save`, `insert`, `update`, `delete`, `count`, `create`.

```yaml
- save child_view :child_data
- list other_view
```

### Flow Control

**If / Else**:
```yaml
- if (:count > 0):
    - result = 'Found'
- else:
    - result = 'None'
```

**Foreach**:
Iterate over a collection (Tresql result, HTTP parts, etc.).
Inside the loop:
*   `..`: Access parent scope.
*   `__idx`: Iteration index (0-based).

```yaml
- foreach :items:
    - total = :total + :amount
    - parent_val = :'..'.some_val
```

### HTTP Operations
Make HTTP requests.
`http (get|post|put|delete|...) [<client name>] <tresql uri> [<body>] [<headers>]`

*   **URI**: `'/path'` or `{'path', '?', :param_name param_value}`.
*   **Headers**: Tresql returning `name`, `value`.

```yaml
- http post {'/api/data'} { :data } { 'Content-Type', 'application/json' }
- res = http [my-client] get {'/secure/resource'}
```

### File Operations

**Read File**:
`file [<client name>] <id-sha-tresql>`
Returns an `InputStream`.

```yaml
- stream = file { :id, :sha_256 }
```

**Save File**:
`to file [<client name>] <content op> [[filename=] <name>] [[content_type=] <type>]`
Returns file info (`id`, `sha_256`, `size`, etc.).

```yaml
- file_info = to file (list this) 'report.csv' 'text/csv'
```

**Extract Parts**:
Handle multipart uploads.
`extract parts [<file streamer>]`

```yaml
- parts = extract parts
- save_file = file { :parts.attachment.id, :parts.attachment.sha_256 }
```

### Response
Return a specific HTTP response.
`response|status <code|ok> [<cookies>] [<headers>] [<user attrs>] [<body>]`

*   **Cookies**: `set_cookie(name=..., value=...)`, `delete_cookie(...)`
*   **Headers**: `set_headers({...})`
*   **Redirect**: `redirect <tresql uri>` (Shorthand for status 303)

```yaml
- status 200 { 'status': 'ok' }
- response ok set_cookie(name='lang', value='lv') 'Done'
- redirect { '/home' }
```

### Database Control
Execute within a specific DB or transaction.

```yaml
- transaction [main]:
    - save this
- db use [other_db] user { name }
- commit
```

### JSON
Encode/Decode JSON.

```yaml
- json_str = to json :map_data
- map_data = from json :json_str
```

### Configuration
Read `application.conf` values.
`conf [number|string|boolean] <path>`

```yaml
- max_size = conf number app.upload.limit
```

### Job
**Define Job**: Action named `job` in view.
**Call Job**: `call <job name>`

```yaml
- job_result = call my_background_job
```

### This
References current action data.
`this`

```yaml
- save this
```

## Feature Guides

*   [Action Language Workflows](../features/04-action-language-workflows.md)
*   [Email Sending](../features/11-email-sending.md)
*   [Outbound HTTP Client Calls](../features/12-outbound-http-client-calls.md)
