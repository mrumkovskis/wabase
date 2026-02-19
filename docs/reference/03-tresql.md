# Tresql Reference

Tresql is the query language used by Wabase actions and views. It covers read/write queries, filtering, joins, aggregation, and expression evaluation.

## Simple Example

```tresql
person[code = :code] {code, name, surname}
```

```tresql
+person {code, name, surname} ['p100', 'Alice', 'Smith']
```

## Complex Example

```tresql
person p[p.code = :code] {
  p.code,
  p.name,
  main_account = account a[a.id = p.main_account_id] {number},
  accounts = [account[person_code = p.code] {id, number, balance}]
}
```

```tresql
=task[id = :id] {
  status = 'DONE',
  last_modified = now()
}
```

## 1. Select / Read

Basic forms:

```tresql
table_name {col1, col2}
table_name[condition] {col1, col2}
```

Examples:

```tresql
task[project_id = :project_id] {id, summary, status}
tms_user[username ~% :username?] {id, username, full_name}
```

## 2. Insert / Update / Delete

Insert:

```tresql
+table_name {col1, col2} [:v1, :v2]
```

Update:

```tresql
=table_name[id = :id] {col1 = :value}
```

Delete:

```tresql
-table_name[id = :id]
```

## 3. Joins and Aliases

Use aliases for readable join conditions:

```tresql
task t[project_id = p.id] project p[id = :project_id] {
  t.id,
  t.summary,
  p.name
}
```

Lookup pattern used in view fields:

```tresql
account[number = _]{id}
```

## 4. Aggregation

```tresql
task[project_id = :project_id] {count(*)}
task[project_id = :project_id] {sum(coalesce(estimate_hours, 0))}
```

## 5. Variables and Optional Inputs

Bind variables:
1. `:name` required input.
2. `:name?` optional input.
3. `_` current field value in lookup/save mappings.

Examples:

```tresql
tms_user[username = :username] {id}
tms_user[username ~% :username?] {id, username}
```

## 6. Useful Operators and Functions

Common operators/functions in metadata:
1. `coalesce(a, b)`
2. `now()`
3. `length(:value)`
4. `exists(query {1})`
5. `in (...)`

Example:

```tresql
exists(task[id = :id & status = 'OPEN'] {1})
```

## 7. Tresql Inside Actions

Tresql is the default operation language in action steps:

```yaml
save:
- open_tasks = task[project_id = :project_id & status != 'DONE'] {count(*)}
- if (:open_tasks == 0):
    - =project[id = :project_id] {status = 'ARCHIVED'}
- save this
```

## Related Docs

* [Action Language Reference](02-action-language.md)
* [View Definition Reference](01-views.md)
* [Guide: Relationships and Validation](../guide/03-relationships-and-validation.md)
