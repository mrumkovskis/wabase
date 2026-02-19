# Input, Output, and Renderers

This chapter documents request decoding, response encoding, file/template rendering, and content-type behavior.

## 1. Request Decoding Pipeline

Main implementation:
- `src/main/scala/RequestDecoder.scala`
- `src/main/scala/Marshalling.scala`
- `src/main/scala/WabaseService.scala`

Supported decoding families:
1. JSON (object stream / map conversion)
2. CBOR (borer decoder)
3. CSV (configurable delimiter/quote/headers)
4. XML (path-driven extraction)

Config groups:
- `data-parsers-json.*`
- `data-parsers-csv.*`
- `data-parsers-xml.*`
- `request-decoders.factory-class`

## 2. Decoder Selection Rules

At runtime, decoder choice is influenced by:
1. Content type (`application/json`, csv/xml media types, etc).
2. View decoder settings (for views that opt out or customize decoding).
3. Explicit parser names in configuration (named parser instances).

Practical note:
- For high-volume endpoints, set parser size limits (`max-object-size`, line length limits) and test malformed payload behavior.

## 3. Response Encoding and Rendering

Main implementation:
- `src/main/scala/ResultEncoder.scala`
- `src/main/scala/ResultSerializer.scala`
- `src/main/scala/Marshalling.scala`

Built-in renderers include:
- JSON
- CBOR
- CSV
- ODS
- XLS XML
- Form URL encoded

Config hook:
- `result-renderers.factory-class`

Field filtering:
- Query parameter name comes from `app.field-filter-parameter-name` (default `fields`).
- Filtering is applied to create/get/list outputs where supported.

## 4. Structured Result Semantics

Wabase result rendering supports:
1. Primitive responses (`String`, numeric, booleans)
2. Map/object responses
3. Collection responses
4. Nested structures with filtered field projections
5. Streamed outputs (useful for large data and tabular exports)

Relevant files:
- `src/main/scala/ResultEncoder.scala`
- `src/main/scala/RowWriter.scala`
- `src/main/scala/BufferStreams.scala`

## 5. File Streaming and Uploads

Main implementation:
- `src/main/scala/AppFileStreamer.scala`
- `src/main/scala/AppFileCleanup.scala`
- `src/main/scala/WabaseService.scala` (file result handling)

Features:
- Multipart file ingestion
- SHA-256 based body deduplication
- DB metadata + filesystem body split
- Content type and size tracking
- File cleanup support

Config roots:
- `app.files.path`
- `file-streamer.*`
- `app.db-data-file-max-size`
- `app.db-data-file-max-sizes.*`

## 6. Templates and Document Rendering

Template stack:
- `src/main/scala/WabaseTemplate.scala`
- `src/main/scala/spreadsheet/OdsStreamer.scala`
- `src/main/scala/spreadsheet/XlsXmlStreamer.scala`

Capabilities:
1. Mustache template rendering
2. HTML output
3. PDF rendering pipeline (`MustacheAndPdfTemplateRenderer`)
4. Spreadsheet export formats for tabular data

Integration examples:
- `src/it/resources/views/template.yaml`
- `src/it/resources/http_tests/template/*.yaml`

## 7. Static Resources and Cache Conditions

Relevant files:
- `src/main/scala/WabaseService.scala` (`getFromResource`)
- `src/main/scala/CacheConditionHandlers.scala`
- `src/main/scala/CacheIo.scala`
- `src/it/resources/routes/static-resources.yaml`
- `src/it/resources/http_tests/get-from-resource/*.yaml`

Behavior:
- Resources can be served from classpath roots.
- ETag / Last-Modified logic is available for cache-aware responses.

## 8. Operational Guidance

1. Keep parser limits strict for externally-facing APIs.
2. Use explicit content types for custom handlers returning raw strings.
3. For large datasets, prefer streaming-friendly outputs (CSV/ODS/XLS) instead of huge inline JSON.
4. Test both happy and invalid payload paths using YAML integration tests.
5. For file-heavy workloads, isolate file-streamer storage on local durable disks.

## 9. Test Coverage Sources

- `src/test/scala/JsonDecoderSpecs.scala`
- `src/test/scala/XmlDecoderSpecs.scala`
- `src/test/scala/UnmarshallingSpecs.scala`
- `src/test/scala/MarshallingSpecs.scala`
- `src/test/scala/FileUploadSpecs.scala`
- `src/test/scala/SerializerStreamsSpecs.scala`
- `src/it/resources/http_tests/form/*.yaml`
- `src/it/resources/http_tests/template/*.yaml`

## Feature Guides

*   [Files and Attachments](../features/09-files-and-attachments.md)
*   [Templates and Document Generation](../features/10-templates-and-document-generation.md)
*   [Request Decoding and Content Types](../features/15-request-decoding-and-content-types.md)
*   [Result Rendering and Export](../features/16-result-rendering-and-export.md)
*   [Static Resources and Cache Controls](../features/18-static-resources-and-cache-controls.md)
