# Request Decoding and Content Types

## Use This When

You need reliable body decoding for JSON/CSV/XML/multipart inputs.

## Core Surface

- `src/main/scala/RequestDecoder.scala`
- `src/main/scala/Marshalling.scala`
- `src/main/resources/reference.conf` (`data-parsers-*`)

## Simple Example

```yaml
insert:
- payload = extract entity
- +person{name = :payload.name, surname = :payload.surname}
- status ok
```

## Complex Example

```hocon
data-parsers-csv {
  default_csv_decoder {
    headers = ["name", "surname", "email"]
  }
}
```

```yaml
insert:
- rows = extract entity using default_csv_decoder
- foreach :rows:
  - +person{name = :name, surname = :surname, email = :email}
- status 201
```

## Key Notes

1. Decoder choice depends on content type and decoder name.
2. `extract parts` is preferred for multipart flows with files.
3. Keep parser limits strict to avoid oversized payload issues.

## Related Docs

- `../reference/08-input-output-and-renderers.md`
- `../reference/04-configuration.md`
