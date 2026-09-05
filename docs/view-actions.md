# View actions

## View action execution steps

### Variable name

```
<segment>[.<segment> …]
```

`<variable name>` is one or more segments separated by `.`, each segment being either a
plain identifier or a quoted string.

| Segment form | Notes |
| --- | --- |
| identifier | Letters, digits and underscore, not starting with a digit. Must not be one of the tresql reserved words `in`, `null`, `false`, `true`. |
| `'quoted'` or `"quoted"` | Any name, including one that is not a valid identifier. Embed the quote character by doubling it — `'it''s'`. |

A segment must not itself contain `.` — dots separate segments, they are not part of one. A
quoted segment holding a dot is rejected with `Variable name segment must not contain '.'`.

Quoting mirrors tresql variable reference, so a name assigned as `'my-key' =` is read back as
`:'my-key'`. Quoting is also how a step command keyword is used as a variable name. An
identifier that merely starts with a keyword needs no quoting.

Example:

```
- result.result = if {:result.status < 400} 'This is ok' else 'This is ere'
- "'setenv' = 'quoted setenv'"    # keyword as variable name does not shadow step command
- setenvious = 'setenvious value' # identifier starting with keyword is a variable name
```

`<variable name>` is used for the assigned variable of an evaluation step, for a block name,
and for the loop variable of `foreach <variable name> in <collection expression>`.

Remove variable step is the exception — it takes a single identifier or quoted string, so a
dotted name cannot be removed.

### Evaluation

```
[<variable name> =] [variable transformations ->] <expression>
```

### Remove variable

```
<variable name> -=
```

Removes variable from action data. If variable does not exist in action data does nothing.

Example:

```
- var1 -=   # removes variable from action data
```

### Set env

```
setenv [variable transformations ->] <expression>
addenv [variable transformations ->] <expression>
```

`setenv` replaces action data with expression value. `addenv` merges expression value into
current action data instead, keeping variables that are not overwritten.

If expression value is not a map, the step must be named, otherwise action data is left
unchanged.

Example:

```
- setenv get this
- setenv org.wabase.QuereaseActionTestManager.personSaveBizMethod
- addenv unique { 'AddedName' name, 'M' sex }   # add variables name and sex to current data
- addenv if :cond:
  - unique {'c' c}
```

### Named block

```
<variable name>:
  - <expression>
  - …
```

A bare variable name in place of a step command starts a block. Steps of the block are
executed as a nested action and its result is assigned to that variable.

Example:

```
- "'block name'":
  - "'my-key' = 'in block'"
  - this
```

### Recover

```
recover <expression>

recover:
  - <expression>
  - …
```

A `recover` step handles failure of the steps preceding it, up to the previous `recover`
step or block start — those steps form a guarded region. Steps not followed by a `recover`
step form a trailing unguarded region. A `recover` step with no steps to guard is rejected.

On failure of a guarded region, db changes are rolled back to a savepoint taken at region
start, the recover expression is executed in the scope of the failed step, and execution
continues with the steps following the recover step.

Authentication and authorization exceptions are not recoverable, so that a recover step
cannot turn them into a successful response.

The failure is available to the recover expression as variable `wabase_error` with fields:

| Field | Value |
| --- | --- |
| `exception` | The `Throwable` itself, for use with [rethrow](#rethrow). |
| `errors` | List of maps with keys `error` — exception class name — and `message`, one per exception in the cause chain. |

Example:

```
- x = 'A'
- recover:
  - x = 'R'
- y = 'B'

- x = 'A'
- recover 'R'     # one step recover, equivalent of recover block with single step
```

### Validation

```
validations [<validation name>] [ [<db name>] ]:
  - [build cursors]
  - [cursor definition, ] <require condition>, <error message>
  - …
```

Example:

```
- validations beneficiary:
  - exists(^person_choice[^name = :beneficiary_name]{1}), "Beneficiary not found - '" ||
      concat_ws('', :beneficiary_name, "'")

- validations amount:
  - :amount > 0, 'Wrong amount ' || :amount || '. Amount must be greater than 0'

- validations balance [transaction_db]:
  - balance(# s) { account[number = :originator] {balance} }, ((balance{s}) = null | (balance{s}) >= :amount),
      "Insufficient funds for account '" || :originator || "'"
```

### Return

```
return [variable transformations ->] <expression>
```

`return` is optional keyword.

Example:

```
- return :person + (main_account = :main_account_data)
- return this
```

## View action operations

### Result type

Most operations accept an optional result type expression as a prefix. It is referred to as
`<result type>` in the operation syntax below.

```
as (any | result | [`]<view name>[`]) [*]
```

| Form | Meaning |
| --- | --- |
| `as any` | Result is not conformed to any view — no field filtering is applied. |
| `as <view name>` | Result is rendered according to that view definition, keeping only its fields. Name may be enclosed in backticks, which is needed when it would otherwise clash with parser keywords. |
| `as result` | Keeps the value as a querease result instead of converting it to a tresql bindable value. Useful for example for http entity values. |

A trailing `*` marks the result as a collection. It is meaningful for `any` and for a view
name, and is ignored after `result`.

Operations accepting a result type expression are tresql, `unique` / `unique_opt`,
invocation, view call, read file, `http` / `http_proxy`, `extract entity` and `this`.

In an evaluation step the result type belongs to the expression, so it goes after the
variable name and `=`, not before it.

Examples:

```
as any ({'r1' rn, 'v1' c} + {'r2' rn, 'v2' c})#(1,2)
as `result_render_test` unique { 'text' string_field, '2024-01-31'::date date_field, 'x' filtered_field }
as vaccine * file [main] {:vaccine.id, :vaccine.sha_256}
- extracted_entity = as result extract entity   # step result not converted to bindable values
- res = as result org.wabase.QuereaseActionTestManagerObj.httpRequest
```

### Braces

```
( <operation> )
```

Any operation can be enclosed in braces to group it, which is how an operation is passed
where a single argument is expected — as an invocation argument, a `foreach` collection, an
email subject, body or attachment, or a fold expression.

Example:

```
foreach x in ([{1} + {2} + {3}]) (:x) fold (sum, x) {:sum + :x}
email ({ 'c@c.c' 'to' }) (template 'Subject') (template 'Content')
```

### Tresql

```
[<result type>] <tresql expression>
```

First table in tresql expression can be prefixed with `build_cursors(view name, [bind variable])`
macro call. This will generate cursors from bind variables corresponding to view structure
name of which is provided as an argument. For current view `this` keyword can be used.

Example:

```
[build_cursors(this)] accounts{count(*)}
[build_cursors(this, :books)]books { title, year }
```

### unique, unique_opt

Tresql expression combined with `unique` or `unique_opt`:

```
[<result type>] (unique | unique_opt) <tresql expression>
```

Example:

```
unique { 'Mr. Mario' name, 'Moderna' 'vaccine', '2022-04-11' 'manipulation_date' }
unique_opt |med_db:person_health[name = :name]{id}
as `result_render_test` unique { 'text' string_field, '2024-01-31'::date date_field, 'x' filtered_field }
```

### Invocation

```
[<result type>] <fully qualified function name or alias> [<arguments>]
```

If function is invoked with more than one arguments last argument optionally can be left
outside braces — `function_name (arg1, …, argN-1) argN`.

Function invocation with explicit and implicitly injectable parameters. Non
`QuereaseResult` type explicit parameters are attempted to convert to required function
parameter type.

#### Injectable parameter types

1. Partial function returned from function of type
   `InjectionParametersProvider = InjectionParametersContext => PartialFunction[Parameter, Any]`.
   `InjectionParametersProvider` is initialized from `InjectionParametersProviderFactory`
   configured in parameter `app.wabase-injection-parameters-provider-factory`. Default value
   returns `PartialFunction.empty`.
2. Action data as type:
   1. `scala.collection.immutable.Map[String, Any]`
   2. `java.util.Map[String, Any]`
   3. `MapResult`
   4. Subtype of `Dto`
   5. Array of subtype of `Dto`
3. `QuereaseResources`
4. `Resources`
5. `ResourcesFactory`
6. `ExecutionContext`
7. `ActorSystem`
8. `WabaseFileStreamers`
9. `HttpRequest`
10. `AppQuereaseIo[Dto]`
11. `AppQuerease`
12. `WabaseHttpClients`

Function aliases are defined under configuration parameter `app.wabase-call-alias`.

Examples:

```
org.wabase.QuereaseActionTestManagerObj.person_dtos_list
multipleStringArguments({'a'}, {'b'}) {'c'}
multipleArguments({'2'}, '{:value}') multipleArguments({'1'}, '{:value - 1}', {'3'})
```

### View call

Calls view action. Optional expression argument must return `MapResult`, if omitted current
action data is used.

```
[<result type>] <get | list | insert | update | delete | save | count | create> <view name | this> [<expression>]
```

`this` argument means the current view name.

Examples:

```
get this
save person_health
get person_accounts { :main_acc_id id }   # calls view with parameter 'id' -> :main_acc_id
```

### response (status)

```
response|status <status code> [<set cookies>] [<delete cookies>] [<set http headers>] [<set user attributes>] [<expression>]
```

`status` is different from `response` that string data for status mode is marshalled as
`text/plain` not json.

#### Status code

```
ok | <http status code as integer>
```

#### set cookies

```
set_cookie(name = <tresql>, value = <tresql>, secure = true|false, http_only = true|false, expires = <tresql>, max_age = <tresql>)
```

#### delete cookies

```
delete_cookie(name = <tresql>, path = <tresql>, domain = <tresql>)
```

#### set http headers

```
set_headers(<tresql with two string columns representing header name and value, can be joined with union>)
```

Example:

```
set_headers({'header1', 'value1'} + {'header2', 'value2'})
```

#### set user attributes

```
user_attrs(<tresql with two string columns representing attribute name and value, can be joined with union>)
```

Example:

```
user_attrs({'attr1', 'val1'} ++ {'attr2', 'val2'})
```

Full response examples:

```
status ok
status 303 { '/data', '?/', 'path', :status }   # redirect command is shorthand of status 303
status ok :status
status ok { :uri || 'about' }
response ok
    set_cookie(name = :name, value = :value, secure = false, http_only = :http_only,
      expires = {`date_add(timestamp '2025-02-05 09:37:40', interval 1 month)`})
response ok
   set_headers({:hn, :hv})
      as any ({'r1' rn, 'v1' c} + {'r2' rn, 'v2' c})#(1,2)
status ok
  user_attrs({'id', 10})
  user_attrs({'roles', |:'roles'})
  'ok'
```

### redirect

Shorthand of `response 303 …`

```
redirect [<set cookies>] [<delete cookies>] [<set http headers>] [<set user attributes>] <tresql>
```

Redirects to current view get api method using key values if get api is allowed, otherwise
returns http 404 not found:

```
redirect (<view name> | this)
```

Examples:

```
redirect { 'data/path', '?', :id id }
redirect
      set_headers({'h1', 'v1'} + {'h2', 'v2'})
      delete_cookie(name = 'x', domain = 'abc.com')
      set_cookie(name = 'test', value = 'test_val', secure = true, http_only = true, max_age = 1000)
      set_cookie(name = 'test1', value = 'test_val1', expires = '2025-04-3 13:30:25'::timestamp)
      {'/redirect_path', 'view'}
```

### if else

```
if <tresql returning boolean value> <expression> [else <expression>]
```

```
if <tresql returning boolean value> :
  - <expression>
  - …
[else:
  - <expression>
  - …
]
```

### foreach

Iteration through a sequence of elements.

Collection argument can contain ops: tresql, http, file, extract entity, extract parts.

```
foreach [<variable name> in] <collection expression> <iteration expression> [<fold>]
```

```
foreach [<variable name> in] <collection expression> [<fold>] :
  - <expression>
  - …
```

Without a loop variable each element must be a `Map[String, Any]` and becomes the iteration
data itself, so its keys are addressed directly. With a loop variable each element is bound
to that name instead, which is what makes iteration over scalars possible.

Iteration data contains two additional keys:

- `..` — access to parent step data. An outer scope variable is reachable only through it,
  as `:'..'.<name>`
- `__idx` — iteration index starting from 0

Examples:

```
foreach extract entity:
  - name = {:name || ' ' || :name}
foreach value in (from json '[1, 2, 3]')            # json array of scalars
foreach value in ([({'value3'} + {'value2'}){*}#(1)])   # union select in array [] gives scalar values
```

#### fold

```
fold (<accumulator variable name>, <element variable name>) <expression>
```

Folds iteration results into a single value, which becomes the result of the `foreach`
operation instead of the sequence of iteration results.

The accumulator variable must already exist in action data — its value is the initial
accumulator value. For each iteration the expression is evaluated with the accumulator
variable bound to the value so far and the element variable bound to that iteration result,
and its value becomes the new accumulator. No other action data is visible to the fold
expression.

Example:

```
- sum = 0   # accumulator must be defined to create initial fold op value
- foreach x in ([{1} + {2} + {3}]) (:x) fold (sum, x) {:sum + :x}

- sum = 0
- foreach x in ([{1} + {2} + {3}]) fold (sum, x) {:sum + :x} :
  - x = :x + 1
  - :x
```

### Variable transformations

Transforms action data. Select or rename variable(s).

```
<variable tresql> | (<variable name> = <variable tresql>) [ + …]
```

Example:

```
(count = :c) + (data = :d) + :var.a
```

### resource

```
resource <resource name tresql> [<content type tresql>]
```

Returns a classpath resource. If content type is omitted it is resolved from resource name,
with `UTF-8` as default charset. An invalid content type is an error.

Example:

```
- resource '/resource.txt'
```

### file (read, write)

#### Read file

```
[<result type>] file [<[client name]>] <id-sha-tresql>
```

Returns context dependent value. When assigned to variable returns `InputStream`. Can be
used as an argument in extract parts, http, to file, email operations.

Example:

```
as vaccine * file [main] {:vaccine.id, :vaccine.sha_256}
```

#### Save file

```
to file [<[client name]>] <content op> [[ filename = ] <file name tresql>] [[content_type = ] <content type tresql>]
```

Returns saved file info object with following fields: `id`, `filename`, `upload_time`,
`content_type`, `sha_256`, `size`.

Example:

```
to file [main] (list this) 'persons' 'application/json'
to file ({ 'a@a.a' 'to', 'Hannah' name }) content_type = 'text/csv; charset=UTF-8'
```

### Template processing

```
template <template tresql> [[data = ] <expression>] [[filename = ] <expression>]
```

Example:

```
template 'Hello {{name}} in {{action}}!' filename='file name' data={:name name, 'update' action}
template 'Subject for {{name}}!'
```

### email

```
email [batch] <data tresql> <subject expr> <body expr> (<attachment expr> […])
```

If the `batch` option is used, email is sent for each row returned by the recipient's
operation. Otherwise the recipient's operation must return no more than one row. Currently
tresql or extract entity with decoder statement is supported for recipients.

Following data tresql columns will be used in corresponding email fields: `to`, `cc`, `bcc`,
`from`, `replyTo`.

Subject and body expressions typically are template operations.

Example:

```
recipients = to file ({ 'a@a.a' 'to', 'Hannah' name } + { 'b@b.b' 'to', 'Baiba' name })
    content_type = 'text/csv; charset=UTF-8'
email batch
    extract entity using default_csv_decoder file {:recipients.id, :recipients.sha_256}
    (template 'Subject for {{name}}!')
    (template 'Content for {{recipient}}.' {trim(:name) recipient})
    (http { '/email_test1', '?', :name name })
    (file {:file.id, :file.sha_256})
    (template 'Template attachment for {{name}}' {trim(:name) name} 'attachment name')

email
    ({ 'c@c.c' 'to', 'Minna' name, 'c1@.c1.c1' cc, 'f@f.f' 'from', 'r@r.r' replyTo })
    (template 'Subject for {{name}}!')
    (template 'Content for {{recipient}}.' {trim(:name) recipient})

email null[false]{'n@n.n' 'to'} 'no mail' 'no content'
```

### http

```
[<result type>] (http | http_proxy) (get|post|put|delete|head|options|patch|trace|connect) [<[client name]>] <tresql uri> [<body expression>] <header tresql>
```

Client name parameter comes from configuration `http-client` parameter section, see
`reference.conf`.

Tresql format http uri:

```
{<uri start> [, <path element>, … ] [, '?', (<query parameter value> [<query parameter name>], …) ]}
```

Tresql uri example:

```
'/forest'
{'/download', :id, :sha_256}
{ '/tree', '?', :nr nr }
{ '/http_forest', '?', 'Nr1' nr, 'Owner5' owner, 12.4 area, 'Fig' trees }
{ '/forest', :nr, :xx? }   # query parameters can be optional
```

Header tresql is statement with two string type columns representing header name and value,
can be joined with union.

Examples:

```
http { '/email_test1', '?', :name name }              # get method is implied
http post { '/not_decode_request_insert_test' }       # uri
      http get {'/not_decode_request_insert_test', '?', 'value' name }   # post body is http get result
http post
        {'/form_urlencoded_test'}                     # uri
        { :name name, :surname surname }              # body
        { 'Content-Type', 'application/x-www-form-urlencoded' }   # headers
http [default-wabase-http-client] {'/download_test', :id, :sha_256}   # http with client name
cookie = buildCookieHeaderValue ({ 'current_lang', 'lv' } + { 'current_user', 'dzidzis' })   # sets cookie value
http get { '/extract_http_cookie_test', '1' } ({ 'Cookie', :cookie })   # uses cookie header value
```

#### http_proxy

`http_proxy` accepts exactly the same syntax as `http` and issues the same request. It
differs in how the response is treated — `http` is for consuming a service, `http_proxy` is
for passing a response through.

| | `http` | `http_proxy` |
| --- | --- | --- |
| Non success status | Fails with `ClientException`, response body included in the message. | Returned as is, no error is raised. |
| `301` / `302` / `303` with `Location` | Redirect is followed, up to `maxRedirects`. | Not followed, the redirect response is returned as is. |
| Result value | Response entity. For a redirection status the `Location` header value is returned instead. | Map with keys `status`, `headers`, `content_type` and `content`. |

Explicit `throwHttpErrors` and `followRedirects` settings on the http client take precedence
— the proxy behaviour applies only where those are left unset.

With `http_proxy` the result type expression applies to `content` of the resulting map, and
only when the response status is a success. Content of an unsuccessful response is decoded
without a view filter.

Example:

```
# command does not terminate action execution on http error
- result = as any http_proxy get {'/http_proxy_test1', '?', :status status}
- result.result = if {:result.status < 400} 'This is ok' else 'This is ere'
- :result
```

### Http request processing (headers, cookie, entity, parts)

#### extract header

Extracts header from http request or response.

```
extract header <header name> [<http expression>]
```

Example:

```
extract header Test-Header1
extract header Last-Modified http head {'/head'}
```

#### extract cookie

Extracts cookie from http request.

```
extract cookie <cookie name>
```

Example:

```
extract cookie current_lang
```

#### extract entity

Extracts entity from http request, response or file optionally using decoder.

```
[<result type>] extract entity [using <decoder name>] [<expression>]
```

Supported expressions are file, http. If expression is omitted, the http request entity is
used.

##### Decoder configuration

```
request-decoders.factory-class = org.wabase.RequestDecodersFactory
```

Default configuration factory `org.wabase.RequestDecodersFactory` loads csv and xml decoders
from respective configurations — `data-parsers-csv` and `data-parsers-xml`. See
`reference.conf`.

Example:

```
http post '/extract_http_entity_test1' extract entity   # entity is extracted from incoming request
foreach extract entity:
  - name = {:name || ' ' || :name}
as extract_http_entity_test4 * extract entity using default_csv_decoder file [main] {:data_file.id, :data_file.sha_256}
foreach (extract entity using test_xml_decoder) this
```

#### extract parts

Extracts multipart form data from http request as a `Source[RequestPart, _]` or in the case
of simple request as a source with one part — `Source.single(RequestPart(...))`.

When assigned to variable returns map of file or single value references, where key is file
or field name and value is file info (see to file operation) or field value.

```
extract parts <file streamer for part data>
```

Example:

```
extract parts
extract parts [main]
result = extract parts
```

### db use, transaction, commit, rollback

Creates new connection on view or specified database and executes statements. On `db use` at
the end rollback is performed, on `transaction` — commit.

```
db use [<db name>] <expression>
db use [<db name>] :
  - <expression>
  - …

transaction [<db name>] <expression>
transaction [<db name>] :
  - <expression>
  - …

commit
rollback
```

`commit` explicitly commits current db connection, `rollback` explicitly rolls it back. Both
apply to the current connection and to every extra resource connection.

Example:

```
db use [shop_db] purchase[customer ~~ :name]{purchase_time, item, amount}#(1)
vaccines = db use [med_db]:
  - person_health[name ~~ :name] {manipulation_date, vaccine}#(1)

transaction save this

- +simple_table{id = nextval('seq'), value = :value}
- rollback
```

### conf

```
conf [number|string|boolean] <parameter name>
```

If parameter type is omitted `config.getValue(<parameter name>).unwrapped()` is used.

Returns typesafe conf configuration parameter.

Example:

```
uri = conf string conf.test.uri
conf conf.test
```

### Json processing

Encodes decodes expression to and from json data.

#### from json

```
from json <expression>
```

Example:

```
from json http get { '/http_forest', '?', 'Nr1' nr, 'Owner5' owner, 12.4 area, 'Fig' trees }
```

#### to json

```
to json <expression>
```

Example:

```
to json unique { :forest.nr nr, :forest.owner owner, :forest.trees trees, :forest.area::decimal area }
```

### job

#### Job definition

Job is an action in view definition.

```yaml
name: <job name>
…
job:
  - <expression>
  - …
```

Example:

```yaml
name: test_job_insert
job:
  - +simple_table {id = nextval('seq'), value = 'ABC'}
```

#### Job invocation

```
call <job name>
```

Example:

```
job_res = call test_job1
```

### rethrow

```
rethrow <variable>
```

Fails the action with a `Throwable` held in an action variable. It is an error if the
variable value is not a `Throwable`.

Intended for [recover](#recover) steps, to handle some failures and let the rest propagate.
An exception rethrown from a recover step action is not handled again by that step.

The variable is deliberately not evaluated as a tresql operation, so that the `Throwable`
value is not passed to query evaluation.

Example:

```
- businessError 'rethrown failure'
- recover:
  - rethrow :wabase_error.exception
```

### this

References current action data. Can be used as an argument in view call,
`build_cursors(...)` macro, return statement.

```
[<result type>] this
```

Example:

```
insert this
list this
[build_cursors(this, :books)] books { title, year }
foreach (extract entity using test_xml_decoder) this
```
