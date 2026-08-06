# Files and Attachments

## Use This When

You need multipart upload, persistent file storage, and API-linked file retrieval.

## Core Surface

- `src/main/scala/AppFileStreamer.scala`
- `src/main/scala/AppFileCleanup.scala`
- `src/it/resources/http_tests/form/*.yaml`

## Simple Example

```hocon
file-streamer {
  files.path = ${app.files.path}
  file-info-table = file_info
  file-body-info-table = file_body_info
  sha-col-name = sha_256
}
```

```yaml
insert:
- uploaded = to file (extract entity) filename='upload.bin'
- return :uploaded
```

## Complex Example

```yaml
insert:
- parts = extract parts [main]
- image = to file [main] :parts.filename filename='avatar.png' content_type='image/png'
- thumb = http post {'/thumbnail'} file {:image.id, :image.sha_256}
- +user_file{user_id = :id, file_id = :image.id, thumb = :thumb}
```

## Key Notes

1. File bodies are deduplicated by SHA-256.
2. Metadata and file body storage are separated.
3. Use named file streamers when storage classes differ.

## Related Docs

- `../reference/08-input-output-and-renderers.md`
- `../guide/04-files-and-attachments.md`
