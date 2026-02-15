# Part 4: Files and Attachments

Tasks often have attachments (screenshots, docs). Wabase handles this elegantly using `FileStreamer`.

## 1. Database for Files

We need two tables to store file metadata and content. (Or we can store content on disk/S3). Wabase standardizes this.

Run SQL:
```sql
CREATE TABLE file_info (
    id SERIAL PRIMARY KEY,
    filename VARCHAR(255) NOT NULL,
    upload_time TIMESTAMP DEFAULT NOW(),
    content_type VARCHAR(100),
    sha_256 VARCHAR(64) NOT NULL
);

CREATE TABLE file_body_info (
    sha_256 VARCHAR(64) PRIMARY KEY,
    size BIGINT NOT NULL,
    path VARCHAR(1024) -- Path on disk
);
```

## 2. Configure File Streamer

In `application.conf`:

```hocon
file-streamer {
  files.path = "./data/files" # Ensure this directory exists!
  file-info-table = file_info
  file-body-info-table = file_body_info
  sha-col-name = sha_256
}
```

## 3. Link Tasks to Files

Let's say a task can have one attachment. Add a column to `task`.

```sql
ALTER TABLE task ADD COLUMN attachment_sha_256 VARCHAR(64);
ALTER TABLE task ADD COLUMN attachment_filename VARCHAR(255);
```

Update `task_view` in `tms-views.yaml`:

```yaml
name:   task_view
# ...
fields:
  # ...
  # When saving: upload file, get SHA and filename, save to these columns.
  # When getting: create a download link object.
  - attachment_sha_256
  - attachment_filename
```

Actually, it's better to abstract this into a "field group" or handle it via a dedicated download action.

Let's define a dedicated download view.

```yaml
name: task_attachment_download
table: task
key: id
api: get
fields:
  - id
  - attachment_sha_256
  # 'file' is a special action op
get:
  - file { :id, :attachment_sha_256 }
```

## 4. Uploading Files

The `save` action automatically detects `multipart/form-data` requests.

If you send a POST request with a file part named `attachment`, Wabase needs to know where to put it.

A common pattern is to upload files first, get an ID/SHA back, and then save the Task with that reference.

Or, use the `save` action with file handling.

```yaml
name:   task_view
api:    save, get, list
# ...
save:
  # 'extract parts' saves uploaded files to storage and returns metadata
  - file_meta = extract parts
  # Now update the task. :file_meta.attachment references the part named 'attachment'
  - attachment_sha_256 = :file_meta.attachment.sha_256
  - attachment_filename = :file_meta.attachment.filename
  - save this
```

## 5. Downloading Files

Add a route for the download view:

```yaml
on: /api/tasks/(?<id>\d+)/attachment
do:
  - org.wabase.WabaseServer.crudAction: task_attachment_download
```

Now `GET /api/tasks/1/attachment` will stream the file.

**Next Step:** [Custom Actions](05-custom-actions.md)
