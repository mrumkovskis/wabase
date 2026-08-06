# Background Jobs and Scheduler

## Use This When

You need asynchronous jobs started manually or by scheduler.

## Core Surface

- `src/main/scala/WabaseScheduler.scala`
- `src/main/scala/scheduler/QuartzScheduler.scala`
- `src/it/resources/routes/job.yaml`
- `src/it/resources/views/job.yaml`

## Simple Example

```yaml
name: create_person
job:
- +person{code = 'p1', name = 'John'}
```

```yaml
on: POST /start-job/(?<jobname>.*)
do: startJob $1
```

## Complex Example

```hocon
app.job {
  actor = org.wabase.WabaseJobActor
  actor-name = "wabase-job-actor"
  scheduler-initializer = org.wabase.scheduler.QuartzScheduler.init
  max-time = "'1 hour'::interval"
  job-status-cp = "main"
  on-start-job = "bootstrap_job"
  clean-jobs-on-start = true
}
```

```hocon
pekko.quartz.schedules {
  NightlyReport {
    expression = "0 0 2 * * ?"
    timezone = "UTC"
  }
}
```

## Key Notes

1. `cron_job_status` lock prevents duplicate run.
2. `max-time` allows stale lock takeover.
3. Scheduler and manual `startJob` can coexist.

## Related Docs

- `../reference/09-async-jobs-deferred-events.md`
