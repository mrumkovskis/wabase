# Wabase Metadata Cheat Sheet

A high-density reference for the most common metadata properties and syntax in Wabase.

---

## 1. Table Metadata (`tables/*.yaml`)
| Shorthand | Description | Example |
| :--- | :--- | :--- |
| `!` | **Required** (NOT NULL) | `id ! 12` |
| `name.id` | **Foreign Key** | `owner_id ! tms_user.id` |
| `type` | Data Type | `is_active ! boolean`, `due_date date` |
| `pk` | Primary Key | `pk: [id]` |
| `idx` | Index | `idx: [username]` |
| `uk` | Unique Constraint | `uk: [code]` |

---

## 2. View Definition (`views/*.yaml`)
### View Properties
| Property | Description | Common Values |
| :--- | :--- | :--- |
| `name` | Unique view identifier | `user`, `project_list` |
| `table` | Base database table | `tms_user` |
| `api` | Allowed operations | `get, list, save, delete, count` |
| `key` | Primary key field | `id` |
| `filter` | Default list filters | `username ~% :username?` |
| `order` | Default sort order | `id desc`, `full_name` |

### Field Shorthands
| Syntax | Meaning | Description |
| :--- | :--- | :--- |
| `- name` | Simple Field | Maps directly to table column. |
| `- name !` | Required Field | Validation check on input. |
| `- total = a + b` | Calculated | Read-only Tresql expression. |
| `- items *` | Collection | Nested child view (1:N). |
| `- owner_name ->` | Lookup | Fetch value from related table. |

---

## 3. Action Language (`views/*.yaml` > `save/get/delete`)
### Basic Steps
| Step Type | Syntax | Description |
| :--- | :--- | :--- |
| **Assignment** | `var = <expr>` | Evaluate and store in scope. |
| **Set Scope** | `setenv <expr>` | Replace entire data scope. |
| **Return** | `return <expr>` | Stop and return value. |
| **Remove** | `var -=` | Delete variable from scope. |
| **Validation** | `validations:` | List of `cond, "error"` pairs. |

### Control Flow
```yaml
# If/Else
- if (:count > 0):
    - result = 'Found'
- else:
    - result = 'None'

# Foreach
- foreach :items:
    - total = :total + :amount
    - p_val = :'..'.val # Parent scope
```

### Operations
| Operation | Syntax | Description |
| :--- | :--- | :--- |
| **Tresql** | `table[id=:id]{name}` | Execute DB query. |
| **Method** | `com.Pkg.Class.method` | Call Scala/Java static method. |
| **View Call** | `list other_view` | `get/list/save/delete` another view. |
| **HTTP** | `http get 'url'` | Outbound REST call. |
| **File** | `to file (list this)` | Generate/Save file (CSV/PDF/etc). |
| **JSON** | `to json :data` | `to json` or `from json`. |

---

## 4. Tresql Essentials
### Read (Select)
*   `table{col1, col2}` - Simple select.
*   `table[id = :id]{*}` - Filter by ID.
*   `table[name ~% :n?]{name}` - Optional case-insensitive prefix match.
*   `table[a = :a & b = :b]` - AND condition.
*   `table[a = :a | b = :b]` - OR condition.

### Write (CUD)
*   `+table{c1, c2}[:v1, :v2]` - Insert.
*   `=table[id=:id]{c1 = :v1}` - Update.
*   `-table[id=:id]` - Delete.

### Joins & Nesting
*   `t[p_id = p.id] project p{t.name, p.title}` - Join with alias.
*   `project{name, tasks = [task[p_id = _.id]{summary}]}` - Hierarchical select.

---

## 5. Common HTTP Route Handlers (`routes/*.yaml`)
| Alias | Description |
| :--- | :--- |
| `authenticate` | Require valid session/JWT. |
| `audit` | Log request/response to audit table. |
| `doAction $1` | Standard dispatcher for view actions. |
| `checkCsrfToken` | Verify X-XSRF-TOKEN header. |
| `setCsrfCookie` | Initialize CSRF protection. |
