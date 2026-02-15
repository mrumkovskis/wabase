# Tresql Reference

Tresql is a query language for SQL databases.

## Select

**Basic**: `table_name { col1, col2 }`
**Filter**: `table_name [col1 = 'value'] { col1 }`
**Join**: `table1 t1[t1.id = t2.ref_id] table2 t2 { t1.col1, t2.col2 }`
**Implicit Join**: `table1 { ref_table.col_name }` (uses FK)

## Insert

`+table_name { col1, col2 } [ 'val1', 'val2' ]`

## Update

`=table_name [id = 1] { col1 = 'new_val' }`

## Delete

`-table_name [id = 1]`

## Functions

Tresql supports SQL standard functions: `count(*)`, `sum(col)`, `max(col)`, `now()`, `coalesce(a, b)`.

## Variables

Bind variables are prefixed with colon: `:variable_name`.
Optional variables: `:variable_name?`.
