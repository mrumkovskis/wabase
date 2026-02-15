# Advanced Features

Wabase offers a rich set of features for building complex applications beyond simple CRUD operations.

## 1. Custom Actions & Workflows

You can define custom workflows in your view definitions using a powerful action language.

### Control Flow (If/Else, Foreach)

```yaml
save:
  - if (:status == 'active'):
      - send_email_notification(:id)
      - log_activity('activated')
  - else:
      - log_activity('pending')
  - save this
```

### Variable Manipulation

You can set and use variables within an action execution context.

```yaml
get:
  - user_count = person[active = true] { count(*) }
  - addenv { :user_count count }  # Add 'count' to the result
  - this                          # Return the standard view result
```

### Invoking Scala/Java Methods

You can call static methods from your Scala/Java code directly in YAML.

```yaml
save:
  - org.example.MyService.validateBusinessRules(:id, :amount)
  - save this
```

## 2. Authorization (Roles)

Wabase supports role-based access control (RBAC) at the view and action level.

### Defining Roles

In `application.conf` or a dedicated metadata file, you can define roles. In views, you restrict access like this:

```yaml
name: sensitive_data_view
table: secrets
api:  admin list, user get
# 'list' is only for 'admin' role
# 'get' is for 'user' role
```

## 3. Validations

You can enforce data integrity with custom validation rules in metadata.

```yaml
validations:
  - amount > 0, "Amount must be positive"
  - exists(account[id = :account_id]), "Account does not exist"
```

These validations are checked before saving data. If a validation fails, Wabase returns a structured error response.

## 4. File Handling

Wabase simplifies file uploads and downloads.

### Setup
Configure `file-streamer` in `application.conf`.

### Upload (Save)
When a file is uploaded (multipart/form-data), Wabase automatically saves it to the configured storage and links it to your record if you map the file fields.

```yaml
table: document
columns:
  - id
  - filename
  - content_type
  - sha_256       # Link to file storage
```

### Download (Get)
To serve a file:

```yaml
get:
  - file { :id, :sha_256 }  # Stream the file content to the client
```

### Advanced File Operations
You can manipulate files in actions:

```yaml
save:
  - report = to file (list report_view) 'my-report.json'
  - email_report(:report.id)
```

## 5. Deferred Requests (Jobs)

For long-running tasks, you can offload processing to a background job.

```yaml
save:
  - result = startJobAction('my_long_running_job_view') { :id }
  - status :result # Returns '202 Accepted' immediately
```

The job view defines the steps to execute in the background.

## 6. Templating & Email

Wabase integrates with template engines (like Mustache) and email services.

### Templating

```yaml
get:
  - template 'Hello {{name}}!' { :name }
```

### Email

```yaml
save:
  - save this
  - email ({ 'user@example.com' to, 'Welcome' subject })
      (template 'Welcome {{name}}!' { :name })
```

## 7. HTTP Calls

You can make HTTP requests to external services directly from your actions.

```yaml
save:
  - response = http post 'https://api.external.com/notify' { :id, :status }
  - save this
```

## 8. Exporting Data

You can export data to various formats (Excel, CSV, ODT).

```yaml
list:
  - to file (list this) 'export.xlsx'
```

This leverages [Querease](https://github.com/guntiso/querease) reporting capabilities.
