# Business scenarios

YAML tests run by `org.wabase.BusinessScenariosBaseSpecs`. A suite subclasses it
and passes resource directories. Each directory under those paths is a scenario.
Each YAML file in a scenario is one step. Steps in a scenario share a context:
values captured in one step are available to the next.

```scala
class BusinessScenariosSpecs extends BusinessScenariosBaseSpecs("http_tests")
```

Scenario files are read from `resources/<path>/` relative to the sbt project base.
`resources/http_tests/auth/44-login-using-json-ok.yaml` is step `44-login-using-json-ok.yaml`
of scenario `auth`.

Directories are discovered recursively and run in canonical-path order. A directory
is a scenario when its name matches the scenario pattern and it contains at least
one step file. Step files are the directory's own files, in file-name order.
Files in nested directories belong to those nested scenarios.

The base class opens one HTTP client, `HttpClientConfig("test")`, so
`http-client.test` must be configured (settings are inherited from `http-client`,
including `server-path`). Override `initHttpClient` to supply another client.
The client keeps a cookie jar for the whole suite and sends stored cookies on
later requests. `scenariosAutoLogin` (default `true`) logs in before each
scenario. `scenariosAutoLogout` (default `true`) clears cookies after the
scenario's last step.

## Config

Both keys are read with `ConfigFactory.load()`, so they can be set in the test
`application.conf` or as a system property of the test JVM. When tests are
forked, a `-D` on the sbt command line is not enough; add it to
`Test / javaOptions`.

| Key | Default | Meaning |
| --- | --- | --- |
| `business-scenarios.test-only` | `.+` | Regular expression matched against the scenario directory name. The match is of the whole name. |
| `business-scenarios.test-only-files` | `^.*\.yaml$` | Regular expression matched against the step file name. The match is of the whole name. |

```hocon
business-scenarios.test-only       = auth
business-scenarios.test-only-files = "^.*\\.yaml$"
```

```
sbt 'set it / Test / javaOptions += "-Dbusiness-scenarios.test-only=auth"' "it/testOnly wabase.app.BusinessScenariosSpecs"
```

`auth` runs only the `auth` scenario. `api|hello` runs those two. A file named
`74-update-person-2-not-found.yaml.disabled` does not match the default file
pattern, so it is skipped.

## Step file

A step is one YAML map. `response` and `error` cannot both be set.

| Key | Default | Meaning |
| --- | --- | --- |
| `path` | | Request path, or a `/backdoor/...` path. Query parameters may be written in the path. |
| `method` | `GET` | `GET`, `POST`, `PUT`, or `DELETE`. |
| `params` | | Query parameters. Sent for `GET`. |
| `headers` | | Request headers. `Content-Type` selects the body encoding. |
| `request` | | Body. A map or list is JSON. A string is `text/plain`. Absent `POST` and `PUT` send `{}`. |
| `request-body-file` | | File path whose bytes are the body. Content type is taken from the file name when `Content-Type` is not set. |
| `request-parts` | | Multipart parts. See [Request body](#request-body). |
| `response_status` | | Full status line, such as `200 OK` or `303 See Other`. |
| `response_headers` | | Headers that must be present. Further headers are allowed. |
| `response` | | Expected body. See [Response checks](#response-checks). |
| `error` | | Expected failure. See [Errors](#errors). |
| `full_compare` | `true` | When `true`, an object fails if it has a field the expectation does not list. Arrays are unchanged. |
| `merge_response` | `false` | Build the expected object by laying `response` over the request object. |
| `follow_redirects` | see below | Follow redirects and check the final response. |
| `throw_http_errors` | see below | Fail the step on a non-success status before body checks. |
| `sleep` | | Pause before the attempt. A Scala duration, such as `100 ms` or `1s`. |
| `retries` | `0` | Extra attempts after a failure. `sleep` runs before every attempt. |
| `debug_response` | on | Boolean `false` stops the response body from being written to the log. |
| `template` | | YAML file, or list of files, under `resources/templates/`, merged under this step. The key is removed after the merge. Nested maps and lists of maps are processed the same way. |

`follow_redirects` defaults to `false` when `response_status` is a `3xx` status
or `response_headers` expects `Location`. Otherwise it defaults to `true`.

`throw_http_errors` defaults to `true` only when the step sets none of
`response_status`, `response`, `error`, and `response_headers`. Otherwise it
defaults to `false`, so the status and body can be checked.

`full_compare`, `merge_response`, `follow_redirects`, and `throw_http_errors`
are true for boolean `true` and the string `"true"`. `debug_response` stays on
unless the value is boolean `false`.

On failure the step writes `<file>.received`, `<file>.received-headers`, and
`<file>.exception` next to the YAML file. Those files are deleted when the step
passes.

```yaml
path:     /hello
response_headers:
  Content-Type: text/plain; charset=UTF-8
response: "Hello from wabase!"
```

## Request body

`GET` sends no body. `params` are its query string. `DELETE` sends an empty
body. A `request` on `GET` or `DELETE` is rejected.
`POST` and `PUT` choose the body from the first of these that is present:
`request-parts`, a multipart or form-urlencoded `request` map, `request` string,
`request-body-file`, `request` list, `request` map.

A map or list is sent as `application/json`. A string is sent as
`text/plain; charset=UTF-8`. With `Content-Type: application/x-www-form-urlencoded`,
a request map is encoded as a form body. Nested maps and lists in that form are
JSON strings.

```yaml
path:       /request-info
method:     POST
headers:
  Content-Type: application/x-www-form-urlencoded
request:
  name: john
  password: secret
```

`request-parts` is `multipart/form-data`. Each part has `name` and one of
`value` (a string, map, or list) or `file` (a path). `filename` overrides the
name taken from `file`. Part `headers` are sent with the part; `Content-Type`
overrides the default. A file part without `Content-Type` uses the type guessed
from the file name.

```yaml
request-parts:
  - name:  name
    value: john
  - name:  file
    file:  resources/http_tests/form/some-text-file.txt
```

With `Content-Type: multipart/form-data` and no `request-parts`, each entry of
the request map is a part. Map and list values are JSON parts. Other values are
text parts.

For `POST` and `PUT`, put query parameters in `path`. `params` is sent only
with `GET`.

## Response checks

A successful JSON body is decoded before it is compared, except when the whole
expected body is an ordinary string. `size(...)` is not an ordinary string, so
a body expected to be `size(3)` is decoded. An `error` string is also kept as
text and is described under [Errors](#errors).

Scalars compare through `String.valueOf`, so the number `1` matches the string
`"1"`. `null` matches `null`.

### Objects

Every listed field must be present. With `full_compare: true` (the default), a
field that is not listed fails the step. `full_compare: false` allows those
fields. The same rule applies to objects nested in arrays and to objects inside
`contains()`.

```yaml
full_compare: false
response:
  code: p1
  name: John
```

That passes for an object that also has `surname`.

`merge_response: true` starts from the request object and overlays `response`.
Nested objects are merged. A value `.del` drops that field from the expectation.

### Arrays

An array matches an expected list only when the lengths are equal, and then
only by index. `full_compare` does not change the length or the order.
`contains()` is the check that allows further elements and ignores order.

### `not_null()`, `size()`, `contains()`

These checks can stand for the whole body or for any nested value.

`not_null()` passes for any value other than `null`.

`size(...)` checks the length of an array, a Java collection, or a Scala array:

| Check | Meaning |
| --- | --- |
| `size(3)`, `size(=3)`, `size(==3)` | Length is 3. |
| `size(>2)`, `size(>=2)`, `size(<2)`, `size(<=2)` | Comparison. |
| `size(!=0)`, `size(<>0)` | Length is not 0. |
| `size(2..4)` | Length is from 2 through 4, inclusive. |

Quote a size check that contains `>`, `<`, or `=`, because those characters are
special in YAML. `size(3)` and `size(2..4)` can stay unquoted.

```yaml
response:
  exact:  size(3)
  gt:     "size(>2)"
  range:  size(2..4)
  empty:  size(0)
```

`contains()` is a one-entry map. The array must include every listed element.
Further elements are allowed. Order does not matter. Each listed element
consumes one response element, so listing `a` twice requires two `a`s. A broad
entry such as `not_null()` does not keep the only element a later entry needs.
The search tries other assignments until one fits.

```yaml
response:
  roles:
    contains(): [admin, guest]
```

```yaml
response:
  contains():
    - code: p1
      name: John
    - not_null()
```

`contains(): []` matches any array, including an empty one. The value under
`contains()` must be a list. A response value that is not an array fails this
check. When the response value is an object, a field named `contains()` is
compared as a normal field, and a list in that field keeps the exact length
and order.

### Capture and substitution

`-> name` stores the matched value in the scenario context under `name`.
The same arrow can be added to the checks above. `-> _` stores the value under
`_` and is the usual way to accept a field without checking it.

| Form | What is stored |
| --- | --- |
| `-> file_id` | The value in that position. |
| `not_null() -> captured_token` | The non-null value. |
| `size(3) -> captured_items` | The array. |
| `contains() -> captured_roles` | The whole array, as a key: `contains() -> captured_roles:`. |

```yaml
response:
  token: not_null() -> captured_token
  upload_time: -> _
```

```yaml
response:
  roles:
    contains() -> captured_roles: [admin, guest]
```

A later step reads context with `<- name`. The scalar must be only that
reference. The stored value keeps its type, so a captured list can be sent as
a JSON array. `<- name -> copy` also stores the value under `copy`.
`-> copy <- name` does the same.

```yaml
request:
  token: <- captured_token
  roles: <- captured_roles
```

`{{name}}` is replaced inside a string. When the whole scalar is `{{name}}`,
the stored value is used as it is. Inside a longer string the value is
inserted as text. `{{randomString(8)}}` and `<- randomString(8)` generate an
alphanumeric string of that length.

A `text/event-stream` response whose expectation is `-> name` stores a
server-sent-events handler under `name` instead of comparing the body. The
[backdoor](#server-sent-events) reads and clears that handler. Handlers are
closed after the scenario's last step.

## Status, headers, and errors

`response_status` is compared to the status line, including the reason:
`201 Created`, `303 See Other`, `202 Accepted`.

Each entry of `response_headers` must be present. The comparison is the header
text, such as `X-Frame-Options: SAMEORIGIN`. `Content-Type` is checked against
the response entity. Extra headers do not fail the step.

```yaml
response_status: 303 See Other
response_headers:
  Location: ../person_redirect?/rd1
```

### Errors

`error` and `response` cannot both be set. A non-success status is required.

A string is a substring of the error text. For an HTTP error that text is three
lines: the status line (`401 Unauthorized`), the status default message, and
the response body.

```yaml
error: 401 Unauthorized
```

A map or a list is compared with the same rules as `response`, after the body
is decoded. Use that form for a JSON error body:

```yaml
error:
  - location: []
    messages:
      - msg: "Quantity %1$s over 50"
        params: [70]
```

Set `throw_http_errors: true` to turn a non-success status into a client
exception before those checks. A string `error` is then a substring of the
exception message.

## Default backdoor

A path that starts with `/backdoor/` runs in the test process. It is not an
HTTP request. The request body is still parsed, and tresql steps read it as
text. The scenario context is the tresql bind map.

Override `backdoorAction` to add paths. Call `super` for a path the override
does not handle.

### `GET /backdoor/current_time`

Returns the current instant:

```yaml
path: /backdoor/current_time
response:
  current_time: not_null() -> captured_time
```

`current_time` is `Instant.now.toString`.

### Server-sent events

Subscribe with an HTTP step that captures the stream handler, then read or
clear it through the backdoor. The name in the backdoor path is the context
key.

```yaml
path:       /server_events_subscription/test_topic
response_headers:
  Content-Type: text/event-stream
response_status: 200 OK
response: -> test_topic_events
```

`GET /backdoor/server_sent_events/<name>` returns the events received so far,
in arrival order. Each event is an object. `data` is always present.
`eventType`, `id`, and `retry` are present when the event has them.

```yaml
path:       /backdoor/server_sent_events/test_topic_events
response:   [{"data": "value1"}]
```

`POST /backdoor/clear_server_sent_events/<name>` drops the events stored for
that handler. The step has no response body.

```yaml
method:       POST
path:         /backdoor/clear_server_sent_events/test_topic_events
```

### Tresql

The request string is the tresql statement. Bind variables are the scenario
context, so a value captured earlier can be used as `:captured_time`. Date and
time values in the result are strings.

`/backdoor/tresql_row` returns the first row as an object, or `{}` when the
query returns no row.

```yaml
path:     /backdoor/tresql_row
request:  "cron_job_status[job_name = 'create_person']{job_name, status}"
response:
  job_name: create_person
  status: success
```

`/backdoor/tresql_list` returns every row.

`/backdoor/tresql_transaction` runs the statement in a transaction and returns
`{result: ok}`.

```yaml
path:     /backdoor/tresql_transaction
request:  "+bundle {id, name, locale} [1, 'it_db', 'en']"
response:
  result: ok
```

### Mail inbox

`/backdoor/inbox` and `/backdoor/purge-inbox` use `org.wabase.GreenMailServer`.
The suite starts that server, usually with `GreenMailServer.startIfEnabled`,
when `app.email.enabled` is true and `simplejavamail.smtp` is configured.
Mail sent through those SMTP settings can then be read from the test.

`/backdoor/inbox` returns one object per message, ordered by `mailbox` and
`subject`.

| Field | Meaning |
| --- | --- |
| `mailbox` | Recipient mailbox the message was stored in. |
| `from`, `to`, `cc`, `bcc`, `reply_to` | Addresses, joined with `, ` when there are several. |
| `subject` | Subject. |
| `body` | First non-attachment part, as text. |
| `body_content_type` | Base content type of that part, without parameters. |
| `attachments` | Attachment list, in message order. |

Each attachment has `filename`, `disposition`, `content_id`, `content_type`,
and `content`. `disposition` is the `Content-Disposition` header with folded
whitespace collapsed. `content_type` is the base type, without parameters.
`content` is the part body decoded as UTF-8. `content_id` is often generated
by the mail library; `not_null()` accepts it.

```yaml
path:     /backdoor/inbox
response:
  - mailbox:  "to@wabase.test"
    from:     "sender@wabase.test"
    subject:  "Subject for Hannah!"
    body:     "Content for Hannah."
    body_content_type: text/plain
    attachments:
      - filename: attachment.txt
        content_type: text/plain
        content: "Attachment for Hannah"
        content_id: not_null()
```

`POST /backdoor/purge-inbox` deletes the mail from every mailbox. The step has
no response body.

```yaml
method:   POST
path:     /backdoor/purge-inbox
```

## Integration-suite backdoor

`wabase.app.BusinessScenariosSpecs` adds schema helpers and then calls the
default backdoor. These paths are not part of `BusinessScenariosBaseSpecs`.

| Path | Effect |
| --- | --- |
| `/backdoor/create-tables/<names>` | Create tables from metadata. Names are comma-separated. |
| `/backdoor/drop-tables/<names>` | Drop those tables. |
| `/backdoor/create-sequences/<names>` | `create sequence <name> start with 1` for each name. |
| `/backdoor/drop-sequences/<names>` | Drop those sequences. |

```yaml
path:     /backdoor/create-tables/person,validation
```
