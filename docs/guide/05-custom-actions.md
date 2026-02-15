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
      - log_notification(:assignee_email, "You have a new task")
```

Wait, `log_notification` isn't a standard Wabase function. We need to implement it in Scala!

## 4. Extending Wabase with Scala

Create `src/main/scala/TMSFunctions.scala`:

```scala
package com.example.tms

object TMSFunctions {
  def logNotification(email: String, message: String): Unit = {
    println(s"SENDING EMAIL TO $email: $message")
  }
}
```

Now call it in YAML:

```yaml
      - com.example.tms.TMSFunctions.logNotification(:assignee_email, "You have a new task")
```

This seamless interoperability is a killer feature.

**Next Step:** [Background Jobs](06-background-jobs.md)
