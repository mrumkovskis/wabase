# Part 6: Background Jobs

Some tasks, like "Archive old projects" or "Generate Monthly Report", shouldn't happen in a web request. Wabase integrates with `pekko-quartz-scheduler` for this.

## 1. Dependencies

Ensure `build.sbt` has the scheduler dependency:

```scala
libraryDependencies += "io.github.samueleresca" %% "pekko-quartz-scheduler" % "1.1.0-pekko-1.0.x"
```

## 2. Database for Jobs

Wabase needs a table to track job status.

```sql
CREATE TABLE cron_job_status (
    id SERIAL PRIMARY KEY,
    cron_name VARCHAR(50) UNIQUE NOT NULL,
    status VARCHAR(5), -- RUN, ERR, SUCC
    report_time TIMESTAMP,
    up_count INTEGER DEFAULT 0,
    succ_down_count INTEGER DEFAULT 0,
    err_down_count INTEGER DEFAULT 0,
    collision_count INTEGER DEFAULT 0
);
```

## 3. Defining a Job View

A job is just a view with a `job` action.

Create `src/main/resources/jobs.yaml`:

```yaml
name:   archive_old_projects_job
job:
  # Find old projects
  - old_project_ids = project[status = 'DONE' & id < 100] { id }
  - count = 0
  - foreach :old_project_ids:
      - =project[id = :id] { status = 'ARCHIVED' }
      - count = :count + 1
  - return "Archived " || :count || " projects."
```

## 4. Scheduling (Quartz)

In `application.conf`, configure the schedule:

```hocon
pekko.quartz.schedules {
  ArchiveProjects {
    expression = "0 0 1 * * ?" # Every night at 1 AM
    timezone = "UTC"
    description = "Archives completed projects"
  }
}

app.job {
  # Map Quartz schedule name to Wabase job name
  ArchiveProjects = archive_old_projects_job
}
```

## 5. Deferred Requests

If a user request takes too long (e.g. "Generate PDF"), we can defer it.

In `application.conf`:
```hocon
app.deferred-requests {
  enabled = true
}
```

In the view:
```yaml
save:
  - result = startJobAction('generate_pdf_job') { :id }
  # Return '202 Accepted' with location of the result
  - status :result
```

**Next Step:** [Security & Roles](07-security.md)
