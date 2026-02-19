# Script Validations

## Use This When

You need dynamic validation rules loaded and executed at runtime.

## Core Surface

- `src/main/scala/ScriptValidation.scala`
- `src/it/resources/tables/validation.yaml`
- `app.script-validations.*`

## Simple Example

```hocon
app.script-validations.enabled = true
```

Validation row concept:

```yaml
table: validation
columns:
  - context
  - expression
  - message
```

## Complex Example

```hocon
app.script-validations {
  enabled = true
  init = org.wabase.WabaseScriptValidation.initWabaseScriptValidation
  script-engine-factory = org.wabase.WabaseScriptValidation.scriptEngineFactory
  script-engine-init = org.wabase.WabaseScriptValidation.initScriptEngine
  custom-functions-init = org.wabase.WabaseScriptValidation.customFunctions
  load-validations = org.wabase.WabaseScriptValidation.loadValidations
}
```

```yaml
# Example logical rule for view context "person"
# expression: "is_valid_email(email) && name != null"
```

## Key Notes

1. Keep scripts deterministic and side-effect free.
2. Register only vetted custom functions.
3. Validate script changes in test/staging before production.

## Related Docs

- `../reference/04-configuration.md`
- `../reference/06-core-runtime-and-extension-points.md`
