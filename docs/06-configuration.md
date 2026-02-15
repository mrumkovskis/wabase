# Configuration

Wabase is highly configurable through `application.conf`. Here are the most important settings.

## Database Connection

Configure your database connection pools under `jdbc.cp`.

```hocon
jdbc.cp {
  main {
    jdbcUrl = "jdbc:postgresql://localhost:5432/mydb"
    username = "dbuser"
    password = "dbpassword"
    maximumPoolSize = 10
  }
}
```

## Application Settings

General application settings under `app`.

```hocon
app {
  host = "http://localhost:8080"

  # File storage path for uploads
  files.path = "/var/lib/wabase/files"

  # Security keys (must be generated securely in production)
  auth.crypto.key = "..."
  auth.mac.key    = "..."
}
```

## Security & Session

```hocon
session {
  timeout = 900s          # Session timeout
  cookie.name = "SID"     # Session cookie name
  cookie.secure = true    # Set to true in production (HTTPS)
}
```

## File Streamer

Configure how files are stored and accessed.

```hocon
file-streamer {
  files.path = ${app.files.path}
  # Database tables to store file metadata
  file-info-table = file_info
  file-body-info-table = file_body_info
}
```

## Deferred Requests

Configure background job processing.

```hocon
app.deferred-requests {
  enabled = true
  worker-count = 4
}
```

## Email

Configure email sender.

```hocon
app.email {
  enabled = true
  # Implement org.wabase.WabaseEmail interface or use default
  sender = org.wabase.DefaultWabaseEmailSender
}
```

## Tuning

You can tune various parameters for performance.

```hocon
wabase.max-stack-depth = 50       # Max recursion depth for actions
tresql.cache-size = 4096          # Tresql query cache size
tresql.max-result-size = 10000    # Max rows returned by list actions
```
