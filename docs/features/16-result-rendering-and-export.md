# Result Rendering and Export

## Use This When

You need JSON/CSV/spreadsheet outputs or explicit file export responses.

## Core Surface

- `src/main/scala/ResultEncoder.scala`
- `src/main/scala/ResultSerializer.scala`
- `src/main/scala/spreadsheet/OdsStreamer.scala`
- `src/main/scala/spreadsheet/XlsXmlStreamer.scala`

## Simple Example

```yaml
list:
- list person {code, name, surname}
```

Client-side field filtering example:

```http
GET /data/person?fields=code,name
```

## Complex Example

```yaml
list:
- csv = to file (list person {code, name, surname}) filename='persons.csv' content_type='text/csv; charset=UTF-8'
- ods = to file (list person {code, name, surname}) filename='persons.ods' content_type='application/vnd.oasis.opendocument.spreadsheet'
- xls = to file (list person {code, name, surname}) filename='persons.xml' content_type='application/vnd.ms-excel'
- return {csv = :csv, ods = :ods, xls = :xls}
```

## Key Notes

1. Renderer is selected by response content type.
2. Spreadsheet renderers are suited for large tabular outputs.
3. Use streaming-friendly responses for high row counts.

## Related Docs

- `../reference/08-input-output-and-renderers.md`
