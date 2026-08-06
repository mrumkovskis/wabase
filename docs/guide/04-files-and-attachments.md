# Part 4: Files and Attachments

Tasks usually need file attachments (screenshots, specs, exports). Wabase handles upload/download through `extract parts`, `to file`, and `file` operations backed by `file-streamer`.

## 1. File Storage Tables

Create the standard file tables:

```sql
create table file_info (
  id bigserial primary key,
  filename varchar(255) not null,
  upload_time timestamp not null default now(),
  content_type varchar(100) not null,
  sha_256 varchar(64) not null
);

create table file_body_info (
  sha_256 varchar(64) primary key,
  size bigint not null,
  path varchar(1024) not null
);
```

## 2. Configure File Streamer

In `application.conf`:

```hocon
file-streamer {
  files.path = "./data/files"
  file-info-table = file_info
  file-body-info-table = file_body_info
  sha-col-name = sha_256
}
```

## 3. Link Task to Uploaded File

Add attachment reference columns to `task`:

```sql
alter table task add column attachment_file_id bigint;
alter table task add column attachment_sha_256 varchar(64);
alter table task add column attachment_filename varchar(255);
```

Update `src/main/resources/views/tms.yaml`:

```yaml
name: task
table: task
api: count, create, get, list, save, delete
key: id
fields:
- id
- project_id
- assignee_id
- summary
- details
- due_date
- priority
- status
- attachment_file_id
- attachment_sha_256
- attachment_filename
```

## 4. Attachment Upload/Download Views

Add dedicated views in `src/main/resources/views/task-attachments.yaml`:

```yaml
name: task_attachment_upload
decoder: none
key: task_id
api: put
fields:
- task_id
put:
- parts = extract parts
- =task[id = :task_id] {
    attachment_file_id = :parts.attachment.id,
    attachment_sha_256 = :parts.attachment.sha_256,
    attachment_filename = :parts.attachment.filename
  }
- status ok


name: task_attachment_download
table: task
key: id
api: get
fields:
- id
- attachment_file_id
- attachment_sha_256
get:
- file { :attachment_file_id::long, :attachment_sha_256 }
```

## 5. Routes

Add `src/main/resources/routes/task-attachments.yaml`:

```yaml
on: /api/((?:task_attachment_upload|task_attachment_download))(/.+)?
do: doAction $1
```

## 6. Usage

Upload:

```bash
curl -X PUT "http://localhost:8080/api/task_attachment_upload/1" \
  -F "attachment=@./example.pdf"
```

Download:

```bash
curl -OJ "http://localhost:8080/api/task_attachment_download/1"
```

**Next Step:** [Custom Actions](05-custom-actions.md)
