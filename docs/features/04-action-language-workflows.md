# Action Language Workflows

## Use This When

You need business logic in metadata, not custom Scala handlers.

## Core Surface

- `src/main/scala/AppMetadata.scala`
- `src/main/scala/AppQuerease.scala`
- `src/test/resources/querease-action-specs-metadata.yaml`

## Simple Example

```yaml
insert:
- if exists(user[name = :name]{1}):
  - status 409
- else:
  - +user{name = :name, password = :password}
  - status ok
```

## Complex Example

```yaml
insert:
- payload = extract entity
- remote = http post {'/enrich'} :payload {'Content-Type', 'application/json'}
- html = template '<h1>{{title}}</h1><p>{{msg}}</p>' data={:payload.title title, :remote.msg msg}
- report = to file :html filename='report.html' content_type='text/html; charset=UTF-8'
- response 201 file {:report.id, :report.sha_256}
```

## Key Notes

1. Ops can chain data between steps using variables.
2. `extract entity`, `http`, `template`, and `to file` cover most integration workflows.
3. Validate complex action chains with integration tests.

## Related Docs

- `../reference/02-action-language.md`
- `../features/11-email-sending.md`
