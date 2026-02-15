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

## Audit Configuration

| Key | Description | Default |
| :--- | :--- | :--- |
| `app.audit-pool-name` | Database pool name for audit logs. | - |
| `app.audit-max-content-size` | Max size of request/response body to log. | `256 K` |
| `app.audit-queue.path` | Local directory for audit buffer. | - |

## I18n Configuration

| Key | Description | Default |
| :--- | :--- | :--- |
| `app.language-cookie-postfix` | Postfix for language cookie name. | `lang` |

## Server Notifications

| Key | Description | Default |
| :--- | :--- | :--- |
| `app.server-notifications.enabled` | Enable server-side events. | `true` |

## Advanced Integration

| Key | Description | Default |
| :--- | :--- | :--- |
| `app.wabase-call-alias` | Map of alias names to fully qualified method names. | - |
| `app.wabase-injection-parameters-provider-factory` | Class implementing `InjectionParametersProviderFactory`. | `org.wabase.AppQuerease.InjectionParametersProviderFactory` |
| `request-decoders.factory-class` | Class factory for request decoders. | `org.wabase.RequestDecodersFactory` |
| `data-parsers-csv` | Configuration for CSV decoder. | - |
| `data-parsers-xml` | Configuration for XML decoder. | - |
