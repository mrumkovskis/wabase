# Part 5: Custom Actions

Sometimes `save this` isn't enough. We want to do things like "When a task is completed, archive the project if all tasks are done".

## 1. Action Language Syntax

Actions are defined as a list of steps.
```yaml
save:
  - variable = expression
  - if (condition):
      - do something
  - else:
      - do other thing
  - save this
```

### Deeper Integration (`setenv`)

You can modify the "current" action data (scope) using `setenv`.

```yaml
get:
  # Set the current scope to the result of 'get this'
  - setenv get this
  # Now add a new variable to the scope
  - user_status = 'active'
  # Return the modified scope
  - this
```

## 2. Example: Auto-Close Project

Modify `task_view` save action:

```yaml
name: task_view
# ...
save:
  - save this
  # Check parent project
  - project_id = :project_id
  - open_tasks_count = task[project_id = :project_id & status != 'DONE'] { count(*) }
  - if (:open_tasks_count == 0):
      # Update project status
      - =project[id = :project_id] { status = 'ARCHIVED' }
```

## 3. Example: Notify Assignee

Let's log a message when assignee changes.

```yaml
save:
  # Get old assignee from DB before saving
  - old_assignee_id = task[id = :id] { assignee_id }
  - save this
  - if (:assignee_id != :old_assignee_id):
      - assignee_email = tms_user[id = :assignee_id] { email }
      - com.example.tms.TMSFunctions.logNotification(:assignee_email, "You have a new task")
```

## 4. Extending Wabase with Scala

Create `src/main/scala/TMSFunctions.scala`:

```scala
package com.example.tms

object TMSFunctions {
  // Methods referenced in YAML must be static (object methods)
  def logNotification(email: String, message: String): Unit = {
    println(s"SENDING EMAIL TO $email: $message")
  }
}
```

This seamless interoperability is a killer feature. You can inject dependencies (like `HttpRequest`) into your methods automatically if you define an `InjectionParametersProvider`.

**Next Step:** [Background Jobs](06-background-jobs.md)
