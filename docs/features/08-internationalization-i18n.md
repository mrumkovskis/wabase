# Internationalization (I18n)

## Use This When

You need locale-aware messages and runtime language switching.

## Core Surface

- `src/main/scala/I18n.scala`
- `src/main/resources/wabase_en.properties`
- `src/main/resources/wabase_lv.properties`
- `src/it/resources/routes/i18n.yaml`

## Simple Example

```properties
hello.world=Hello World
```

```yaml
on: GET /i18n/hello.world
do: extractState i18nTranslate('wabase', $1, null)
```

## Complex Example

```properties
person.not.found=Person {0} not found in {1}
```

```yaml
on: GET /i18n/(?<key>[^/]+)(?:/(?<parameters>.*))?
do: extractState i18nTranslate('wabase', $1, $2)

on: POST /set-language/(\w+)
do: setLanguage($1) ok
```

Request: `GET /i18n/person.not.found/John,Riga`

## Key Notes

1. Parameterized translations are supported.
2. Language switching is cookie-driven via `setLanguage`.
3. Keep default bundle complete for fallback behavior.

## Related Docs

- `../reference/04-configuration.md`
- `../guide/09-additional-features.md`
