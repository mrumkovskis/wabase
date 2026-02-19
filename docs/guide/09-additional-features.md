# Part 9: Additional Features

Wabase includes several powerful features "out of the box" that are essential for enterprise applications.

## 1. Auditing

Wabase provides a robust auditing system that can capture every request and response, including who performed the action, when, and what data was changed.

### Enabling Audit

The `WabaseApp` trait usually mixes in `NoAudit`. To enable auditing, your application class should mix in `Audit` (or a subclass like `BufferedAudit` which writes to a queue first for performance).

In `WabaseServer.scala` or your custom app class:

```scala
class App(exec: Execution) extends WabaseApp[WabaseUser]
  with Execution
  // ... other traits ...
  with org.wabase.audit.Audit // Use the default Audit implementation
```

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
*   `wabase.properties` (Default/English)
*   `wabase_lv.properties` (Latvian)
*   `wabase_fr.properties` (French)

**Example `wabase.properties`**:
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

Use the `CSRFDefence` trait or helper methods in your routes/directives.

### How it Works

1.  Server sets a cookie `XSRF-TOKEN`.
2.  Client reads this cookie and sends its value in a header `X-XSRF-TOKEN` for every state-changing request (POST, PUT, DELETE).
3.  Server verifies that the cookie matches the header.

It also checks `Origin` and `Referer` headers to ensure the request is coming from a trusted domain.

## 4. Server Notifications (SSE)

You can push real-time updates to clients using Server-Sent Events (SSE) or WebSockets.

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
const eventSource = new EventSource("/api/events");
eventSource.onmessage = function(event) {
    const data = JSON.parse(event.data);
    console.log("New Event:", data);
};
```

## 5. Spreadsheet Export

Wabase can export any List view to Excel or OpenDocument Spreadsheet (ODS) format automatically.

### Usage in Actions

Use the `to file` action with a specific content type.

```yaml
list:
  # Export list result to Excel
  - to file (list this) 'report.xlsx' 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
```

Supported types:
*   `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet` (Excel .xlsx)
*   `application/vnd.oasis.opendocument.spreadsheet` (ODS)

Wabase uses metadata (field labels, types) to format the spreadsheet columns correctly.

Related reference chapters:
*   [Input, Output, and Renderers](../reference/08-input-output-and-renderers.md)
*   [Async Processing: Jobs, Deferred Requests, Events, Audit](../reference/09-async-jobs-deferred-events.md)
