# Action functions

Built in functions callable from view action definitions. They are aliased in the
miscellaneous block of `reference.conf` parameter section `app.wabase-call-alias`, so that
they can be referenced by short name. They are called as an
[invocation](view-actions.md#invocation) step or operation, arguments are passed as described
there.

| Alias | Function |
| --- | --- |
| [`buildCookieHeaderValue`](#buildcookieheadervalue) | `org.wabase.AppQuerease.buildCookieHeaderValue` |
| [`sleep`](#sleep) | `org.wabase.AppQuerease.sleep` |
| [`startJobAction`](#startjobaction) | `org.wabase.AppQuerease.startJob` |
| [`toHierarchy`](#tohierarchy) | `org.wabase.AppQuerease.toHierarchy` |
| [`publishEvent`](#publishevent) | `org.wabase.ServerNotifications.publishEvent` |
| [`error`](#error) | `org.wabase.AppQuerease.error` |
| [`columnPairsToMap`](#columnpairstomap) | `org.wabase.AppQuerease.columnPairsToMap` |

## buildCookieHeaderValue

```
buildCookieHeaderValue <tresql expression>
```

Builds a `Cookie` header value from cookie name, value pairs. The tresql result must have two
columns — cookie name and cookie value, one row per cookie. Names and values are trimmed.
Returns string.

Example:

```
- cookie = buildCookieHeaderValue ({ 'current_lang', 'lv' } + { 'current_user', 'dzidzis' })
- lang_user = as any http get { '/extract_http_cookie_test', '1' } ({ 'Cookie', :cookie })
```

## sleep

```
sleep <millis>
```

Blocks the current thread for given number of milliseconds, produces no result. Exists because
`Thread.sleep` cannot be invoked directly from an action due to method overload.

Example:

```
- sleep 100
```

## startJobAction

```
startJobAction (<job name>) [<parameters expression>]
```

Asks the scheduler actor (configuration parameter `app.job.actor-name`) to start the named
[job](view-actions.md#job) and returns without waiting for the job to finish. Job parameters are
taken from the parameters expression, which must return a map. If it is omitted, the current
action data is passed.

Returns http status code as int:

| Status | Meaning |
| --- | --- |
| `200` | Job started. |
| `409` | Job is already running. |
| `404` | Job not found. |

Scheduler must respond within 5 seconds, otherwise the action fails.

Example:

```
- result = startJobAction ('test_job2') unique{:value value}
- status :result
```

## toHierarchy

```
toHierarchy (<level column name>, <nested field name>) <tresql expression>
```

Builds nested maps from a flat tresql result. Each row becomes a map. A row with a greater level
than the previous row is nested under the previous row in a list named by nested field name.
The level column value must be a non negative integer. The result must be ordered by hierarchy
path (depth first), so that each row comes right after its parent or after the subtree of its
preceding sibling. Rows without children do not have the nested field.

Returns a list of top level maps. Use `as any` if the view fields do not describe the result.

Example:

```yaml
name: hierarchy_test
api: list
list:
- as any toHierarchy('level', 'children') data(# id, parent_id, value) {
    {1, null, 'v1'} + {2, null, 'v2'} + {3, null, 'v3'} +
    {6, 2, 'v21'} + {7, 2, 'v22'} + {4, 1, 'v11'} + {5, 4, 'v111'}
  }, hierarchy(id, value, level, path) {
    data[parent_id = null] {id, value, 1, `array[1, id]` /* path ordering by level and id */} +
    data d[d.parent_id = h.id]hierarchy h {
      d.id, d.value, level + 1,
      path || `array[level + 1, d.id]` /* path ordering by level and id */
    }
  } hierarchy {trim(value) value, level}#(path)
```

Result:

```json
[
  {"value": "v1", "level": 1, "children": [
    {"value": "v11", "level": 2, "children": [{"value": "v111", "level": 3}]}
  ]},
  {"value": "v2", "level": 1, "children": [
    {"value": "v21", "level": 2},
    {"value": "v22", "level": 2}
  ]},
  {"value": "v3", "level": 1}
]
```

## publishEvent

```
publishEvent (<topic>) <value>
```

Publishes string value to a server notification topic. Subscribers receive it via
`subscribeToServerSentEvents` or `subscribeToWebSocketMessages`
[route handlers](routes.md#event-notifications). Produces no result.

Example:

```yaml
name: server_events
api: post
decoder: none
key: topic
fields:
- topic
- value
post:
- publishEvent(:topic, :value)
- status 202
```

## error

```
error <message>
```

Fails the action with `RuntimeException` with given message. To fail the action with a client
error (`BusinessException`, mapped to a 400 response) use [throw](view-actions.md#throw)
operation instead.

Example:

```
- error 'unexpected state'
```

## columnPairsToMap

```
columnPairsToMap <tresql expression>
```

Converts a single row tresql result into a map, taking odd columns as keys and even columns as
values — `{k1, v1, k2, v2, …}` becomes `{k1: v1, k2: v2, …}`. Keys are converted to strings, key
order is preserved.

Values are converted as follows:

| Value | Result |
| --- | --- |
| Nested query result | List of maps, one per row, each row converted by the same column pairs rule. Empty list for no rows. |
| Array query (`\|[…]`) result | List of values for a single column query, otherwise list of maps converted by the same column pairs rule. |
| sql array | Array. |
| Any other value | The value itself. |

Result shape checks:

| Result | Outcome |
| --- | --- |
| No rows | Empty map. |
| More than one row | Action fails, message contains row count and the beginning of the first row. |
| Odd column count, in top level or nested row | Action fails. |

Use `as any` if the view fields do not describe the result.

Examples:

```
- as any columnPairsToMap {
    'name' k1, 'John' v1,
    'accounts' k2, |({'number' k, 'X64' v} + {'number', 'X94'})#(2) 'accounts',
    'roles' k3, |[({'guest'} + {'admin'})#(1)] 'roles',
    'contacts' k4, |[({'type' k, 'email' v} + {'type', 'phone'})#(2)] 'contacts'
  }
```

Result:

```json
{
  "name": "John",
  "accounts": [{"number": "X64"}, {"number": "X94"}],
  "roles": ["admin", "guest"],
  "contacts": [{"type": "email"}, {"type": "phone"}]
}
```
