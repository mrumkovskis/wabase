# Part 9: Additional Features

Wabase includes several powerful features "out of the box" that are essential for enterprise applications.

## 1. Auditing

Wabase provides a robust auditing system that can capture every request and response, including who performed the action, when, and what data was changed.

### Enabling Audit

Enable the `audit` route handler in your metadata routes:

```yaml
on: /api/((?:create:|count:)?\w+)(/.+)?
do: authenticateOpt audit doAction $1
```

This uses the built-in audit handler alias from `app.wabase-call-alias.audit`.

### Configuration

In `application.conf`:

```hocon
app {
  audit-pool-name = "audit_write"   # Database pool for writing logs
  audit-max-content-size = 256 K    # Max body size to log
  audit-queue.path = "./audit-queue" # Local buffer path
}
```

### Database

You need a table to store the audit logs. The default implementation expects a view named `audit` (which usually maps to a table).

```sql
CREATE TABLE audit (
    id SERIAL PRIMARY KEY,
    action VARCHAR(20),
    entity_id BIGINT,
    user_name VARCHAR(100),
    time TIMESTAMP,
    entity VARCHAR(100),
    new_data TEXT,
    old_data TEXT,
    diff TEXT,
    error TEXT
);
```

## 2. Internationalization (I18n)

Wabase uses standard Java `ResourceBundle` (`.properties` files) for translations.

### Setup

Create property files in `src/main/resources`:
*   `wabase_en.properties` (English)
*   `wabase_lv.properties` (Latvian)
*   `wabase_fr.properties` (French, optional)

**Example `wabase_en.properties`**:
```properties
hello.world=Hello World!
user.not.found=User {0} not found.
```

### Usage in Scala

```scala
// Implicit locale is usually available in request context
val msg = ctx.wabase.translate("hello.world")
val err = ctx.wabase.translate("user.not.found", userId)
```

### Switching Languages

Wabase looks for a specific cookie (default `current_lang`) or the `Accept-Language` header.
You can set the language using `I18nService.setLanguage`.

## 3. CSRF Protection

Wabase includes built-in protection against Cross-Site Request Forgery (CSRF) using the **Double Submit Cookie** pattern.

### Enabling

Add CSRF handlers to routes that change state:

```yaml
on: POST /set-csrf-cookie
do: setCsrfCookie ok

on: POST /api/(.+)
do: checkSameOrigin checkCsrfToken authenticate doAction $1
```

### How it Works

1.  Server sets a cookie `XSRF-TOKEN`.
2.  Client reads this cookie and sends its value in a header `X-XSRF-TOKEN` for every state-changing request (POST, PUT, DELETE).
3.  Server verifies that the cookie matches the header.

It also checks `Origin` and `Referer` headers to ensure the request is coming from a trusted domain.

## 4. Server Notifications (SSE)

You can push real-time updates to clients using Server-Sent Events (SSE) or WebSockets.

### Define Notification View

Create `src/main/resources/views/server-events.yaml`:

```yaml
name: server_events
api: get, list, save
key: topic
fields:
- topic
- value
get:
- wabase.app.EventsFunctions.subscribeToEvent :topic
list:
- wabase.app.EventsFunctions.subscribeToWsMessages :topic
insert:
- wabase.app.EventsFunctions.publishEvent(:topic, :value)
```

### Publish Events

In your code (e.g., inside a custom action):

```scala
import org.wabase.ServerNotifications

// Publish to a specific user
ServerNotifications.publishUserEvent(userId, Map("type" -> "task_assigned", "taskId" -> 123))
```

### Subscribe (Client)

The client connects to an SSE endpoint.

```javascript
const eventSource = new EventSource("/api/server_events/tasks");
eventSource.onmessage = function(event) {
    const data = JSON.parse(event.data);
    console.log("New Event:", data);
};
```

## 5. Spreadsheet Export

Wabase can export list results to CSV, ODS, or Excel XML formats.

### Usage in Actions

Use the `to file` action with a specific content type.

```yaml
list:
  - csv = to file (list this) 'report.csv' 'text/csv; charset=UTF-8'
  - ods = to file (list this) 'report.ods' 'application/vnd.oasis.opendocument.spreadsheet'
  - xls = to file (list this) 'report.xml' 'application/vnd.ms-excel'
  - return {csv = :csv, ods = :ods, xls = :xls}
```

Supported types:
*   `text/csv; charset=UTF-8` (CSV)
*   `application/vnd.oasis.opendocument.spreadsheet` (ODS)
*   `application/vnd.ms-excel` (Excel XML)

Wabase uses metadata (field labels, types) to format the spreadsheet columns correctly.

Related reference chapters:
*   [Input, Output, and Renderers](../reference/08-input-output-and-renderers.md)
*   [Async Processing: Jobs, Deferred Requests, Events, Audit](../reference/09-async-jobs-deferred-events.md)
