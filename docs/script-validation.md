# Script validation

Script validations are javascript validations of view data stored in a database table, so that
they can be changed without changing view definitions or application code. They are evaluated
when a view is saved, and their failures are reported in the same validation error response as
[tresql validations](view-actions.md#validation).

## Configuration

Script validation is disabled by default. It is enabled and configured under
`app.script-validations`:

```
app.script-validations {
  enabled = true
  init = org.wabase.WabaseScriptValidation.initWabaseScriptValidation
  script-engine-factory = org.wabase.WabaseScriptValidation.scriptEngineFactory
  script-engine-init = org.wabase.WabaseScriptValidation.initScriptEngine
  custom-functions-init = org.wabase.WabaseScriptValidation.customFunctions
  load-validations = org.wabase.WabaseScriptValidation.loadValidations
}
```

All settings except `enabled` are names of functions, so that each part can be replaced by
application. Function parameters are passed by type — `DbAccess`, `AppQuerease` — and, for
`load-validations`, view name and action name by position.

| Setting | Default function | Purpose |
| --- | --- | --- |
| `enabled` | `false` | Enables script validation. |
| `init` | `initWabaseScriptValidation(db: DbAccess, qe: AppQuerease)(implicit ec: ExecutionContext)` | Creates `ScriptValidation` module which runs validations. |
| `load-validations` | `loadValidations(viewName: String, actionName: String, dbAccess: DbAccess)(implicit qe: AppQuerease): List[Validation]` | Loads validations of the view, see [validation table](#validation-table). |
| `script-engine-factory` | `scriptEngineFactory(): ScriptEngine` | Creates GraalJS script engine with `nashorn-compat` option and explicit host access. |
| `script-engine-init` | `initScriptEngine(instance: Map[String, Any], engine: ScriptEngine, customFunctions: AnyRef): ScriptEngine` | Defines [variables](#variables) from validated data and [custom functions](#custom-functions). |
| `custom-functions-init` | `customFunctions(): AnyRef` | Returns object with custom functions, `CustomScriptValidationFunctions` by default. |

A new script engine is created for each validated request, validations of one request share it.

## When validations are evaluated

Script validations are evaluated by `save`, `insert`, `update` and `upsert` actions, before
action steps are executed — also before [validations](view-actions.md#validation) steps of the
action — and after field checks like mandatory field or maximum length. Other actions, including
`update+`, do not evaluate script validations.

All validations of the view are evaluated, and failures of all of them are reported together.
If no validation fails, the action continues.

## Validation table

Default loader loads validations with tresql query:

```
validation[context ~~ :context] {context, expression, message}#(context)
```

where `:context` is the view name. The table can be defined as:

```yaml
table:   validation
comments: Validation scripts for views
columns:
  - id
  - context              ! 100 : Name of the view
  - expression           ! 500 : Script validation expression
  - message              ! 500 : Error message if expression evaluates to false
```

| Column | Content |
| --- | --- |
| `context` | Name of the view the validation belongs to. |
| `expression` | Javascript [expression](#expression) which checks the data. |
| `message` | Javascript expression or plain text of the error [message](#message). |

Notes on the default loader:

- `context` is matched with case insensitive `like` (`~~`) where the view name is the pattern, so
  `_` and `%` in view name act as wildcards;
- action name is not used — validations of the view are evaluated for all actions listed
  [above](#when-validations-are-evaluated);
- validations are ordered by `context` only, so the order of validations of one view, and of their
  failures in the response, is not defined.

To load validations differently — from another table, by action name, in defined order —
configure own `load-validations` function returning `List[org.wabase.WabaseScriptValidation.Validation]`.

## Script environment

### Variables

Each value of the validated data — saved view fields and action environment — is defined as a
global javascript variable with the field name, encoded as json:

| Scala value | Javascript value |
| --- | --- |
| `String` | string |
| number | number, e.g. `12.5` for decimal `12.50` |
| `Boolean` | boolean |
| `null` | `null` |
| date, timestamp | string, e.g. `'2026-03-01'`, `'2026-03-01 10:20:30'`, `'2026-03-01 10:20:30.123'` |
| child view collection | array of objects, e.g. `lines[1].qty` |

Dates and timestamps are strings, which compare in time order: `valid_from <= valid_till`. Fractional
seconds are present only if not zero, without trailing zeros, which does not break the order.

### Custom functions

Methods annotated with `org.graalvm.polyglot.HostAccess.Export` of the custom functions object are
available as global javascript functions. Default functions are:

| Function | Result |
| --- | --- |
| `is_valid_email(email)` | `true` if `email` is valid e-mail address. |
| `current_date()` | Current date as string in the format of date variables, e.g. `'2026-03-01'`. |
| `now()` | Current time as string in the format of timestamp variables, e.g. `'2026-03-01 10:20:30.41'`. |

Current date and time can be compared with date and timestamp variables: `valid_from >= current_date()`.

Application can add own functions by extending `CustomScriptValidationFunctions`:

```scala
import org.graalvm.polyglot.HostAccess.Export

object AppScriptValidationFunctions extends org.wabase.CustomScriptValidationFunctions {
  @Export def is_valid_code(code: String): Boolean = code != null && code.matches("[A-Z]{2}\\d+")
}
```

```
app.script-validations.custom-functions-init = app.AppScriptValidations.customFunctions
```

where `app.AppScriptValidations.customFunctions()` returns `AppScriptValidationFunctions`.

`org.wabase.BusinessException` thrown by a custom function fails the request with that exception —
by default exception handlers with http status `400 Bad Request` and exception message as response.

## Expression

The expression is evaluated and its result determines the outcome:

| Result | Outcome |
| --- | --- |
| `true` | Validation passes. |
| `false` | Validation fails with validation [message](#message). |
| array or object | Validation fails with this message — the result is evaluated by [message](#message) rules for array or object, validation message is not used. |
| string | Validation fails with message `Error (validation "%1$s"): %2$s`, where first parameter is validation message as `{msg, params}` object and second parameter is the string. |
| anything else | [Definition error](#definition-errors). |

Only `true` passes — javascript truthiness is not applied, so number `1`, non empty string or
`null` do not pass. `null` is also the result of `undefined`, of a missing object property, of a
statement like `var x = …` or of a function without return value, while undefined variable is an
evaluation failure. Any string fails, including empty one.

Examples:

```
qty > 0
is_valid_email(email)
valid_from >= current_date()
qty <= 100 || ['Quantity %1$s exceeds %2$s', qty, 100]
code == 'nope' ? 'reserved word' : true
```

## Message

The message is evaluated as javascript expression. If it is not valid javascript, or its
evaluation fails, the message text is used as is. The result must be one of:

| Result | Message |
| --- | --- |
| string | Message template without parameters. |
| array | First element — message template string, remaining elements — parameters. |
| object | `msg` — message template string, optional `params` — array of parameters. No other keys are allowed. |
| anything else, or malformed array or object | [Definition error](#definition-errors). |

Message starting with `{` is evaluated as object literal, parentheses around it are not needed.

The message template is a static i18n template with placeholders `%1$s`, `%2$s`, … replaced with
parameter values on translation, see [message parameters](view-actions.md#message-parameters).
Put values into the message through parameters, not by concatenation.

Examples:

```
Code is too short
['Field %1$s must be positive, got %2$s', 'qty', qty]
{msg: 'Code %1$s is longer than %2$s', params: [code, 5]}
```

Parameter values are javascript values converted to scala values — strings, numbers, booleans,
`null`, lists for arrays and maps for objects.

## Error response

Failures are thrown as `org.mojoz.querease.ValidationException`, which default exception handlers
map to http status `400 Bad Request` with json body. Script validation failures have empty
location:

```json
[ {"location": [], "messages": [{"msg": "Field %1$s must be positive, got %2$s", "params": ["qty", 0]}]}
, {"location": [], "messages": [{"msg": "Error (validation \"%1$s\"): %2$s", "params": [{"msg": "Code %1$s", "params": ["nope"]}, "reserved word"]}]}
]
```

Parameter which is `{msg, params}` object is a message itself — translate it first, then use it as
parameter value.

## Definition errors

Wrong validation definition is a developer error, not a failure of validated data, so it is not
reported as validation failure. `RuntimeException` is thrown, which default exception handlers log
as error and map to http status `500 Internal Server Error` with empty body. Exception message
contains view name, action name, expression, message and error detail:

```
Validation definition error (view person, action insert, expression "qty", message "not used"): Wrong validation result type: java.lang.Integer
```

Definition errors are:

- expression evaluation failure, for example undefined variable or syntax error — the failure is
  exception cause, unless it is `BusinessException` from a [custom function](#custom-functions);
- expression result of wrong type, for example `null` or number;
- malformed array or object returned by expression;
- message result of wrong type or malformed array or object.
