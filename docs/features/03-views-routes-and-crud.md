# Views, Routes, and CRUD API

## Use This When

You need to expose data quickly through Wabase REST endpoints.

## Core Surface

- `src/it/resources/views/*.yaml`
- `src/it/resources/routes/data.yaml`
- `src/main/scala/WabaseService.scala` (`doAction`)

## Simple Example

```yaml
name: person
table: person
api: get, list, insert, update, delete
key: code
fields:
- code
- name
```

```yaml
on: /data/(\w+)(/.+)?
do: doAction $1
```

## Complex Example

```yaml
name: project
table: project
api: manager list, manager get, admin delete, save
key: id
fields:
- id
- name
- tasks *:
    table: task
    fields:
    - id
    - title
```

```yaml
on: /data/((?:create:|count:)?\w+)(/.+)?
do: authenticateOpt audit doAction $1
```

## Key Notes

1. Route capture `$1` is passed as `view_action`.
2. HTTP method + key presence decides final action mapping.
3. Use role-scoped API declarations for permission control.

## Related Docs

- `../reference/01-views.md`
- `../reference/05-routes.md`
