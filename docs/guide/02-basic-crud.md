# Part 2: Basic CRUD

Now that we have a database and a server, let's expose our tables as REST API endpoints.

## 1. Defining Views

Views are defined in YAML files in `src/main/resources`. They tell Wabase which tables to use and which fields to expose.

Create `src/main/resources/tms-views.yaml`:

```yaml
# Table definitions (optional if metadata is loaded from DB, but good for reference)
table: tms_user
columns:
- id
- username
- full_name
- email
- is_active

table: project
columns:
- id
- name
- description
- owner_id
- status

table: task
columns:
- id
- project_id
- assignee_id
- summary
- details
- due_date
- priority
- status

# -----------------------------------------------------------------------------
# Views
# -----------------------------------------------------------------------------

name:   user_view
table:  tms_user
api:    list, get, save, delete
fields:
  - id
  - username
  - full_name
  - email
  - is_active
filter:
  - username ~% :username?   # Case-insensitive substring search (ilike)
order:
  - full_name

name:   project_view
table:  project
api:    list, get, save, delete
fields:
  - id
  - name
  - description
  - owner_id
  - status
order:
  - id desc

name:   task_view
table:  task
api:    list, get, save, delete
fields:
  - id
  - project_id
  - assignee_id
  - summary
  - details
  - due_date
  - priority
  - status
```

## 2. Defining Routes

We need to map URL paths to these views. Create `src/main/resources/routes.yaml`:

```yaml
# User routes
on: /api/users
do:
  - org.wabase.WabaseServer.crudAction: user_view

# Project routes
on: /api/projects
do:
  - org.wabase.WabaseServer.crudAction: project_view

# Task routes
on: /api/tasks
do:
  - org.wabase.WabaseServer.crudAction: task_view
```

*Note: `org.wabase.WabaseServer.crudAction` is a helper that inspects the request. `GET /api/users` becomes `list`, `POST /api/users` becomes `save`, `GET /api/users/1` becomes `get`, etc.*

## 3. Testing the API

Restart your application (`sbt run`).

### Create a User
**POST** `http://localhost:8080/api/users`
```json
{
  "username": "alice",
  "full_name": "Alice Smith",
  "email": "alice@example.com"
}
```
Response: `{"id": 1}`

### List Users
**GET** `http://localhost:8080/api/users`
Response:
```json
[
  {
    "id": 1,
    "username": "alice",
    "full_name": "Alice Smith",
    "email": "alice@example.com",
    "is_active": true
  }
]
```

### Create a Project
**POST** `http://localhost:8080/api/projects`
```json
{
  "name": "Wabase Tutorial",
  "owner_id": 1
}
```

### Create a Task
**POST** `http://localhost:8080/api/tasks`
```json
{
  "project_id": 1,
  "assignee_id": 1,
  "summary": "Write documentation",
  "priority": "HIGH"
}
```

## How It Works

1.  **Request**: `GET /api/users?username=ali`
2.  **Routing**: Matches `/api/users` in `routes.yaml`, invokes `crudAction` with `user_view`.
3.  **Action**: Determines method is `GET` and path has no ID, so it executes `list` action.
4.  **Query Generation**: Wabase looks at `user_view`.
    *   Generates Tresql: `tms_user[username ~% :username] { id, username, full_name, email, is_active } order by full_name`.
    *   Binds `:username` to "ali".
5.  **Execution**: Executes query against PostgreSQL.
6.  **Serialization**: Converts result to JSON and sends response.

**Next Step:** [Relationships and Validation](03-relationships-and-validation.md)
