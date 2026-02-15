# Configuration Reference

The `application.conf` file controls the runtime behavior.

| Key | Description | Default |
| :--- | :--- | :--- |
| `jdbc.cp.main.jdbcUrl` | JDBC connection string. | - |
| `app.host` | Hostname for generating absolute URLs. | - |
| `app.files.path` | Directory for file storage. | - |
| `app.deferred-requests.enabled` | Enable background job processing. | false |
| `app.deferred-requests.worker-count` | Number of concurrent jobs. | 4 |
| `file-streamer.file-info-table` | Table name for file metadata. | `file_info` |
| `file-streamer.sha-col-name` | Column name for SHA hash. | `sha_256` |
| `session.timeout` | Session duration. | `900s` |
| `wabase.max-stack-depth` | Recursion limit for actions. | 50 |
