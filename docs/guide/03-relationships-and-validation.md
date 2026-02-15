# Part 3: Relationships and Validation

Our current API is functional but naive. We need to fetch related data (e.g., project owner name, not just ID) and enforce rules (e.g., due date must be future).

## 1. Showing Related Data

In `project_view`, `owner_id` is just a number. We want to see the owner's name.

Modify `src/main/resources/tms-views.yaml`:

```yaml
name:   project_view
table:  project p  # Alias 'p'
api:    list, get, save, delete
fields:
  - id
  - name
  - description
  - owner_id
  # Complex Field: Read name from tms_user table based on owner_id
  # The arrow -> means: "When saving, use the ID from the referenced table"
  - owner = owner_id -> tms_user[id = :owner_id] { full_name }
  - status
```

Now, `GET /api/projects` will return:
```json
{
  "id": 1,
  "name": "Wabase Tutorial",
  "owner_id": 1,
  "owner": "Alice Smith",
  "status": "PLANNING"
}
```

Wait, we can do better. Let's make `owner` the primary field and `owner_id` implicit.

```yaml
fields:
  - id
  - name
  - description
  # Syntax: field_name = lookup_table.lookup_col -> lookup_table[lookup_col = :field_name]{id}
  # Meaning: Display 'username', but when saving, find user by username and save their ID to 'owner_id'
  - owner = tms_user.username -> tms_user[username = _]{id}
  - status
```

## 2. Nested Data (One-to-Many)

Let's show all tasks inside a project when we fetch a project.

Add `tasks` field to `project_view`:

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

Now `GET /api/projects/1` returns:
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

Add `validations` section to the views.

```yaml
name:   project_view
# ...
validations:
  - length(:name) >= 3, "Project name must be at least 3 characters long"

name:   task_view
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

Let's set default status for new tasks.

```yaml
name:   task_view
# ...
fields:
  # ...
  - status:
      initial: "'OPEN'"
```

This sets the `status` field to "OPEN" in the metadata, which the UI can use to pre-fill forms.

**Next Step:** [Files and Attachments](04-files-and-attachments.md)
