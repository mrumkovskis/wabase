# Templates and Document Generation

## Use This When

You need generated text/HTML/PDF files from action data.

## Core Surface

- `src/main/scala/WabaseTemplate.scala`
- `src/it/resources/views/template.yaml`

## Simple Example

```yaml
get:
- template {'Hello {{name}}!'} {:name name}
```

## Complex Example

```yaml
insert:
- html = template '<h1>{{title}}</h1><p>{{body}}</p>' data={:title title, :text body} filename='report.html'
- pdf = template (file {:html.id, :html.sha_256}) data={:title title, :text body} filename='report.pdf'
- response 200 file {:pdf.id, :pdf.sha_256}
```

## Key Notes

1. Template source can be inline, file, or classpath resource.
2. Filename extension controls output renderer behavior.
3. Template data can be map or sequence-of-map.

## Related Docs

- `../reference/08-input-output-and-renderers.md`
