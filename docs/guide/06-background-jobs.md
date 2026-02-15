# Part 6: Background Jobs

Some tasks, like "Archive old projects" or "Generate Monthly Report", shouldn't happen in a web request.

## 1. Database for Jobs

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

## 2. Defining a Job View

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

## 3. Scheduling

You can schedule this job using an external cron calling a special endpoint, or configure the internal scheduler (requires Quartz dependency).

For now, let's trigger it manually via HTTP.

Add route:
```yaml
on: /jobs/archive
do:
  - org.wabase.WabaseServer.crudAction: archive_old_projects_job
```

Wait, `crudAction` maps GET to `list` or `get`. We need to map it to `job`.

We can use `startJob` helper in `routes.yaml`:

```yaml
on: /jobs/archive
do:
  - org.wabase.WabaseServer.startJob: archive_old_projects_job
```

Now `POST /jobs/archive` will start the job asynchronously and return `202 Accepted`.

## 4. Deferred Requests

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
