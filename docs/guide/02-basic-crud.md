# Part 2: Data Model & Basic CRUD

In this part, we leverage Wabase's superpower: **Metadata-Driven Development**. Instead of writing SQL DDL manually, we define our data model in YAML, and Wabase (via Mojoz) generates the SQL schema and Scala DTOs for us.

## 1. Defining the Data Model

Create `src/main/resources/tms-model.yaml`. This file defines your tables (entities).

```yaml
# -----------------------------------------------------------------------------
# Table Definitions (The Source of Truth)
# -----------------------------------------------------------------------------

table: tms_user
columns:
- id          bigint  pk auto
- username    string  !  uk
- full_name   string  !
- email       string
- is_active   boolean = true

table: project
columns:
- id          bigint  pk auto
- name        string  !
- description text
- owner_id    bigint  ref tms_user
- status      string  = 'PLANNING'

table: task
columns:
- id          bigint  pk auto
- project_id  bigint  ref project
- assignee_id bigint  ref tms_user
- summary     string  !
- details     text
- due_date    date
- priority    string  = 'MEDIUM'
- status      string  = 'OPEN'
```

*Note: The syntax `pk auto` means Primary Key, Auto Increment. `!` means Not Null. `ref table` creates a Foreign Key.*

## 2. Generating Code & Schema

Run the following command in sbt:

```bash
sbt compile
```

The **MojozPlugin** will now:
1.  Read `tms-model.yaml`.
2.  Generate PostgreSQL DDL script at `db/db-schema.sql`.
3.  Generate Scala case classes (DTOs) in `target/scala-*/src_managed/main/...`.

## 3. Applying the Schema

Open `db/db-schema.sql`. It should contain the `CREATE TABLE` statements. Run this script against your database.

```bash
psql -U tms_user -d tms_db -f db/db-schema.sql
```

## 4. Defining Views

Now that the physical model is set, we define the API Views in `src/main/resources/tms-views.yaml`.

```yaml
# Import table definitions if in a separate file, or define views here.
# Since we already defined tables in tms-model.yaml, we can just reference them.

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
  - username ~% :username?
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

## 5. Defining Routes

Create `src/main/resources/routes.yaml`:

```yaml
on: /api/users
do:
  - org.wabase.WabaseServer.crudAction: user_view

on: /api/projects
do:
  - org.wabase.WabaseServer.crudAction: project_view

on: /api/tasks
do:
  - org.wabase.WabaseServer.crudAction: task_view
```

## 6. Running the API

Run `sbt run`. Your API is live!

*   **Users**: `http://localhost:8080/api/users`
*   **Projects**: `http://localhost:8080/api/projects`

**Next Step:** [Relationships and Validation](03-relationships-and-validation.md)
