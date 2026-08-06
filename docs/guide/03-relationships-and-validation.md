# Part 3: Relationships and Validation

Our API now works, but it still exposes raw IDs and does little validation. In this part we add relation fields, nested data, and business validations.

## 1. Showing Related Data

In `project`, `owner_id` is just a number. We want a readable owner field.

Modify `src/main/resources/views/tms.yaml`:

```yaml
name:   project
table:  project p  # Alias 'p'
api:    list, get, save, delete
fields:
  - id
  - name
  - description
  - owner_id
  # Read related value using owner_id
  - owner = owner_id -> tms_user[id = :owner_id] { username }
  - status
```

Now `GET /api/project` returns:
```json
[
  {
    "id": 1,
    "name": "Wabase Tutorial",
    "owner_id": 1,
    "owner": "alice",
    "status": "PLANNING"
  }
]
```

You can also make `owner` writeable by username:

```yaml
fields:
  - id
  - name
  - description
  # Reads username from owner_id and writes owner_id by username.
  - owner = tms_user.username -> = tms_user[username = _]{id}
  - status
```

## 2. Nested Data (One-to-Many)

Let's show all tasks inside a project when we fetch a project.

Add `tasks` field to `project`:

```yaml
fields:
  - id
  # ... other fields ...
  - tasks * :
      table: task
      fields:
        - id
        - summary
        - status
```

Now `GET /api/project/1` returns:
```json
{
  "id": 1,
  "name": "Wabase Tutorial",
  "tasks": [
    { "id": 1, "summary": "Write documentation", "status": "OPEN" }
  ]
}
```

**Crucially**, if you send this JSON back with a new task added to the array, Wabase will automatically insert the new task!

## 3. Validations

We want to ensure:
1.  Project name is at least 3 chars.
2.  Task due date is not in the past.

Add `validations` to the views:

```yaml
name:   project
# ...
validations:
  - length(:name) >= 3, "Project name must be at least 3 characters long"

name:   task
# ...
validations:
  # Using Tresql expression for validation
  - :due_date == null | :due_date >= now()::date, "Due date cannot be in the past"
```

Try to create a project with name "Hi". You will get:
**400 Bad Request**
```json
{
  "message": "Project name must be at least 3 characters long"
}
```

## 4. Default Values

Set default status for new tasks:

```yaml
name:   task
# ...
fields:
  # ...
  - status:
      initial: "'OPEN'"
```

This sets the `status` field to "OPEN" in the metadata, which the UI can use to pre-fill forms.

**Next Step:** [Files and Attachments](04-files-and-attachments.md)
