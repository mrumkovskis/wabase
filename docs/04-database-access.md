# Database Access with Tresql

Wabase uses [Tresql](https://github.com/mrumkovskis/tresql) as its primary query language. Tresql is designed to bridge the gap between SQL and complex, hierarchical application data structures (like JSON).

## What is Tresql?

Tresql allows you to:
*   Select hierarchical data in a single query (JSON-like structure).
*   Perform deep inserts and updates (saving a parent object and its children in one go).
*   Use a concise syntax that is closer to the application domain than raw SQL.

## Using Tresql in Wabase Views

In Wabase, you primarily interact with the database by defining views. Wabase automatically generates the necessary Tresql queries from your view definitions.

### Selection (Read)

When you define fields in a view, Wabase constructs a Tresql query to fetch them.

```yaml
fields:
  - id
  - name
  - accounts * :            # Defines a nested collection
      table: account
      fields:
        - number
```

This roughly translates to a Tresql query like:
`person {id, name, |account {number} accounts}`
Which fetches person data and their associated accounts in a structured format.

### Modification (Write)

When you save data (POST/PUT), Wabase uses the same view definition to perform an "upsert". It handles:
*   Checking if the record exists (based on ID or Key).
*   Inserting or Updating the main record.
*   Inserting, Updating, or Deleting child records (e.g., accounts) to match the incoming JSON.

## Custom Tresql Queries

You can use raw Tresql in your action definitions for complex logic.

```yaml
save:
  # Calculate total balance from another table
  - total_balance = account[person_id = :id] { sum(balance) }
  # Use the calculated value in the save operation or subsequent logic
  - if (:total_balance > 1000) status 'VIP'
  - save this
```

### Accessing Other Databases

Wabase supports multiple databases. You can specify which database to use in your view or action.

```yaml
name: product_view
db:   products_db     # Use a specific database connection pool
table: product
...
```

In Tresql, you can also reference tables from other databases if configured:

`|products_db:product { name, price }`

## Transactions

Wabase handles transactions automatically for actions. A `save` action typically runs within a single transaction. If any step fails, the entire transaction is rolled back.

## Connection Pools

Database connections are configured in `application.conf` under `jdbc.cp`.

```hocon
jdbc.cp {
  main { ... }
  products_db { ... }
}
```

Wabase manages these pools efficiently, using HikariCP by default.
