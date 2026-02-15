# Core Concepts

Understanding Wabase revolves around three main concepts: **Views**, **Routes**, and **Actions**.

## 1. Views

Views are the heart of Wabase applications. They define the data structure, database mapping, and API capabilities. They are defined in YAML files (typically `*.yaml` in `src/main/resources`).

### View Definition Structure

A view definition consists of several properties:

*   **name**: The unique identifier of the view.
*   **table**: The database table this view maps to (optional if not saving to DB).
*   **api**: A comma-separated list of allowed actions (`list`, `get`, `save`, `delete`, `count`, `insert`, `update`).
*   **fields**: A list of fields to include in the view.
*   **filter**: Default filters applied to `list` actions.
*   **order**: Default sorting order.

### Example View

```yaml
name:   person_view
table:  person
api:    list, get, save, delete
fields:
  - id
  - name
  - surname
  - birthdate
  - full_name = name || ' ' || surname  # Calculated field (Tresql expression)
filter:
  - name ~% :name_filter?               # Filter by name if parameter is present
order:
  - surname
  - name
```

### Fields

Fields can be simple column names or complex expressions.

*   **Simple Field**: `- name` (maps to `name` column in table)
*   **Calculated Field**: `- full_name = name || ' ' || surname`
*   **Read-Only Field**: `- created_at [!]` (marked with `[!]` or `[+]`)
*   **Hidden Field**: `- password [-]` (not returned in API)
*   **Child Views**: Fields can be other views (nested objects or lists).

```yaml
fields:
  - id
  - name
  - accounts * :                # List of child objects
      table: account
      fields:
        - number
        - balance
```

## 2. Routes

Routes map HTTP requests to actions. They are defined in `routes.yaml`.

### Route Definition

*   **on**: The URL path pattern. Supports regular expressions and named parameters.
*   **do**: The action handler to invoke.

### Example Routes

```yaml
# Map /api/person to the CRUD action for 'person_view'
on: /api/person
do:
  - org.wabase.WabaseServer.crudAction

# Map /api/person/{id} to a specific GET action
on: /api/person/(?<id>\d+)
do:
  - org.wabase.WabaseServer.crudAction
```

In `routes.yaml`, you can map paths to specific Scala methods or rely on standard handlers like `crudAction` which inspects the request method and path to determine the correct view and action.

## 3. Actions

Actions define what happens when a view is accessed. Standard actions are automatically provided if listed in the `api` property of a view.

### Standard Actions

*   **list**: Returns a list of records. Supports filtering, sorting, and pagination.
*   **get**: Returns a single record by ID.
*   **save**: Creates or updates a record.
    *   **insert**: Only allows creating new records.
    *   **update**: Only allows updating existing records.
    *   **upsert**: Tries to update, if not found, inserts.
*   **delete**: Deletes a record.
*   **count**: Returns the number of records matching a filter.

### Custom Actions

You can override standard actions or define new ones in the view definition using Tresql or Wabase action language.

```yaml
name: person
# ... fields ...
save:
  - name = :name || ' (Saved)'  # Modify data before saving
  - save this                   # Call standard save
  - log_action(:id, 'saved')    # Call a custom function
```

This powerful feature allows you to build complex business logic directly in your metadata. See **[Advanced Features](05-advanced-features.md)** for more details.
