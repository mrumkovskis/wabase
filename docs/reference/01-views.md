# View Definition Reference

Views are defined in YAML. A view corresponds to a database abstraction layer (DAL) object.

## Properties

| Property | Type | Description |
| :--- | :--- | :--- |
| `name` | String | **Required**. Unique identifier for the view. Used in routes and API calls. |
| `table` | String | Database table name. Optional if view is aggregation-only. |
| `db` | String | Database name (from connection pool config). Defaults to main. |
| `api` | String | Comma-separated list of methods: `get`, `list`, `save`, `delete`, `count`, `insert`, `update`. Can be prefixed with roles. |
| `fields` | List | **Required**. List of field definitions. |
| `filter` | List | List of Tresql filter expressions applied to `list` action. |
| `order` | List | List of order by expressions. |
| `limit` | Int | Default limit for list results. |
| `validations`| List | List of validation expressions. |
| `save` | Action | Custom workflow for save operation. |
| `get` | Action | Custom workflow for get operation. |
| `delete` | Action | Custom workflow for delete operation. |

## Field Definition

A field can be defined as a simple string or a detailed object.

**Simple Syntax:**
`- field_name`

**Complex Syntax:**
```yaml
- field_name:
    label: "User Name"
    required: true
    readonly: true
    hidden: false
    sortable: true
    initial: "'Default'"
    api: excluded # or readwrite, readonly, no insert, no update
```

**Calculated Field:**
`- full_name = name || ' ' || surname`

**Lookup Field:**
`- owner = owner_id -> user[id = :owner_id]{name}`

**Child View (Collection):**
```yaml
- items * :
    table: item
    fields: [...]
```

## API Methods

*   **get**: Retrieve by ID.
*   **list**: Retrieve multiple records.
*   **save**: Upsert (Insert or Update).
*   **insert**: Insert only (fails if exists).
*   **update**: Update only (fails if missing).
*   **delete**: Remove record.
*   **count**: Return count of matching records.
