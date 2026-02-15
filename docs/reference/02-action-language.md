# Action Language Reference

Wabase Actions are defined as a list of steps.

## Operations (Ops)

### Tresql
Execute a Tresql query.
```yaml
- my_var = user[id = :id] { name }
- =user[id = :id] { status = 'ACTIVE' }
```

### If / Else
Conditional execution.
```yaml
- if (:count > 0):
    - result = 'Found'
- else:
    - result = 'None'
```

### Foreach
Iterate over a collection.
```yaml
- foreach :items:
    - total = :total + :amount
```

### View Call
Call another view's action.
```yaml
- other_view_result = list other_view
- save child_view
```

### Invocation
Call a static Scala/Java method.
```yaml
- com.example.Utils.sendEmail(:to, :subject)
```

### Variable Assignment
```yaml
- my_var = 'some value'
- new_var = :my_var || ' suffix'
```

### File Operations
```yaml
- file { :id, :sha_256 }
- file_meta = extract parts
- save_to_file = to file (list report) 'report.csv'
```

### HTTP Operations
```yaml
- res = http get 'https://example.com'
- http post 'https://api.com' { :data }
```

### Response
Return a specific response.
```yaml
- status 200 { 'message' : 'Success' }
- redirect 'https://google.com'
```

### Error Handling
Errors in steps generally stop execution and return 500 (or validation error). You can define an `error_handler` in your route or `application.conf`.
