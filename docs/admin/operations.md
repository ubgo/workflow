# Admin operations

The `admin` module is the operations side of the library, the calls an operator's screen or command line needs, plus the numbers a queue dashboard shows: list and search runs, show one, list failures, retry a failed run from the step that failed, forget a finished run, cancel one run or every run a filter matches, retry or delete every run a filter matches after a dry run, move a delayed run, withdraw one step, pause and resume a workflow or a queue, list schedules with their next fire time, read each queue's depth and oldest wait, chart run ends over time, and list the online nodes and the client's health. It works over any `workflow.Client`, reports what that client cannot do instead of failing later, and holds no state of its own.

Use it from Go (a CLI, a support tool, a cron job) as on this page, or over HTTP through the [admin JSON API](http-api.md). The same handler serves a [web UI](ui.md) on top of the API.

## Before you start

- `go get github.com/ubgo/workflow/admin@latest`. It is its own module and needs nothing beyond the standard library and the core.
- A client whose driver declares the capabilities of the operations you want (table below). The memory driver declares all of them; check the driver pages ([memory](../drivers/memory.md), [SQL](../drivers/sqlstore.md), [Hatchet](../drivers/hatchet.md)) for the others.
- The runnable program for these pages is [examples/admin](../../examples/admin/).

## Step 1: wrap the client

`admin.New(client, opts...)` never fails. `admin.WithClock(clk)` sets the clock wait times are measured on (the real clock by default) and `admin.WithMaxScan(n)` bounds how many queued runs one queue report reads (`admin.DefaultMaxScan`, 10,000).

<!-- snippet: examples/admin/main.go#mount -->
```go
adm := admin.New(c)
mux := http.NewServeMux()
mux.Handle("/admin/", http.StripPrefix("/admin", admin.NewHandler(adm, auth)))
```

Each operation needs one capability and one optional core interface:

| Operation | Method | Capability | Interface |
|---|---|---|---|
| `runs` | `Runs(ctx, q)` | `CapRunQuery` | `workflow.RunQuerier` |
| `run` | `Run(ctx, ref)` | `CapRunQuery` | `workflow.RunQuerier` |
| `failures` | `Failures(ctx, q)` | `CapRunQuery` | `workflow.RunQuerier` |
| `discarded` | `Discarded(ctx, q)` | `CapRunQuery`, `CapRunStates` | `workflow.RunQuerier` |
| `queues` | `QueueStats(ctx)`, `QueueDepths(ctx)` | `CapRunQuery` | `workflow.RunQuerier` |
| `retry` | `Retry(ctx, ref)` | `CapRetryRun` | `workflow.RunRetrier` |
| `delete` | `Delete(ctx, ref)` | `CapDeleteRun` | `workflow.RunDeleter` |
| `cancel` | `Cancel(ctx, ref)` | `CapCancel` | `Client.Cancel` |
| `cancel_all` | `CancelAll(ctx, q)` | `CapCancelAll` | `workflow.BulkCanceller` |
| `pause` | `Pause`, `Resume`, `Paused` | `CapPauseWorkflow` (workflow or all), `CapPauseQueue` (queue) | `workflow.Pauser` |
| `schedules` | `Schedules(ctx)` | `CapScheduleList` | `workflow.ScheduleLister` |
| `schedule_pause` | `PauseSchedule(ctx, id)`, `ResumeSchedule(ctx, id)` | `CapScheduleList` and `CapSchedulePause` | `workflow.ScheduleLister` and `workflow.SchedulePauser` |
| `retry_all` | `RetryAll(ctx, q, opts)` | `CapRunQuery`, `CapRetryRun` | `workflow.RunQuerier` and `workflow.RunRetrier` |
| `delete_all` | `DeleteAll(ctx, q, opts)` | `CapRunQuery`, `CapDeleteRun` | `workflow.RunQuerier` and `workflow.RunDeleter` |
| `reschedule` | `Reschedule(ctx, ref, at)` | none | `workflow.RunRescheduler` |
| `cancel_step` | `CancelStep(ctx, ref, step)` | none | `workflow.StepCanceller` |
| `nodes` | `Nodes(ctx)` | none | `workflow.NodeLister` |
| `health` | `Health(ctx)` | none | none: every client answers through `workflow.CheckHealth` |

`Admin.Support()` lists every operation with `Supported` and, when it is not, the `Missing` capabilities, so a screen can hide a button that would always fail. An operation that needs only an interface reports `Missing` zero and says why in `Reason`; for `nodes`, `reschedule` and `cancel_step` the answer comes from `workflow.Supports`, so a wrapping client reports what the client underneath does. An unsupported call returns a `*workflow.UnsupportedError` (it matches `workflow.ErrUnsupported`) whose `Field` is `admin.<operation>`. Admin checks the capability as well as the interface, because a wrapping client (`middleware.WithCodec`, `failover`, a plugin client) implements every interface and refuses at the call when the client it wraps cannot do it.

## Step 2: find runs

`Runs(ctx, q)` takes a `workflow.RunQuery`: `Workflow`, `Queue`, `Statuses`, `Labels` (every pair must match), `From` and `To` (enqueue time, `[From, To)`), and `Limit` (default 100, at most 1,000) with `Cursor`. It returns a `RunPage`, newest enqueue first; pass `page.Next` back as `q.Cursor` until it is empty. The cursor is the driver's own and opaque.

`Failures(ctx, q)` is `Runs` with `Statuses` set to `StatusFailed`; it refuses a query that already sets statuses. `Discarded(ctx, q)` is the same for `StatusDiscarded`: runs a step dropped with `workflow.Discard`, kept apart from failures because nobody has to act on them (see [Run states](../jobs/run-states.md#discarded)). A run that is retrying carries `NextAttempt` and `NextAttemptAt`. `Run(ctx, ref)` returns one run, `workflow.ErrNotFound` when there is none. A `RunInfo` carries the ref, workflow, queue, status, the error text and the step that failed, labels, the stored payload, priority, enqueue, start and finish times, the attempt count and, on a driver with `CapErrorHistory`, `Errors`: each failed attempt's step, attempt number, error, time, stack and, with `CapNodeIdentity`, node ([Error history](../jobs/manage-runs.md#error-history)). Newer fields, each filled where the driver has the capability: `Tenant` (`CapTenant`), `RunAt`, when a delayed run is or was due (`CapDueQuery`), `Progress`, the latest `workflow.ReportProgress`, `Output`, the result of a succeeded run (`CapResult`), and `AttemptedBy`, the nodes that ran its attempts (`CapNodeIdentity`). `RunQuery.Tenant` filters by tenant and `RunQuery.DueFrom` and `DueTo` by due time, so "what fires in the next hour" is one query.

`CountRuns(ctx, q)` counts the runs a query matches, reading at most `WithMaxScan` runs, and returns a `RunCount`: `Total`, `Truncated` when the scan stopped first, and `Cursors`, the cursor that starts each page at `q.Limit`, so a screen can show "1–50 of N" and numbered pages over a driver's opaque cursors. `q.Cursor` must be empty. `RunQuery.Labels` matches values exactly and `RunQuery.LabelPrefixes` by prefix ([Search by label](../jobs/manage-runs.md#search-by-label)); a prefix on a client without `CapLabelPrefix` is refused with an `UnsupportedError`.

## Step 3: act on runs

- `Retry(ctx, ref)` re-runs a failed run from the step that failed: steps that succeeded keep their outputs and do not run again. A run that is not failed returns `workflow.ErrWrongState`; a run whose idempotency key another unfinished run now holds returns `workflow.ErrDuplicate`.
- `Delete(ctx, ref)` forgets a finished run; an unfinished one returns `workflow.ErrWrongState`. To forget many, use `DeleteAll` ([below](#act-on-many-runs-at-once)) or the driver's [retention](../operations/retention.md).
- `Reschedule(ctx, ref, at)` moves a delayed run that has not started to `at`, keeping its ref, payload, labels and key; a run in another state returns `workflow.ErrWrongState`. A zero `at` is refused here: an operator who means "now" passes the current time.
- `CancelStep(ctx, ref, step)` withdraws one step. A running attempt has its context cancelled with the cause `workflow.ErrStepCancelled` and its handler decides: returning an output lets the step succeed and the run go on, returning an error fails the step for good (no retry) and the run with it. A step waiting on a backoff, a snooze, an event or a sleep fails for good at once. A step that has not started or has ended returns `workflow.ErrWrongState`.
- `ForceComplete(ctx, ref)` marks an unfinished or failed run succeeded, for work an operator finished by hand: an attempt in progress is cancelled and its result ignored, and the run counts as a success (its idempotency key is freed, a chain continues, run-finished observers see `StatusSucceeded`). `ForceFail(ctx, ref, reason)` marks an unfinished run failed with no retry and without running `OnFailure`; the reason (at most `admin.MaxForceReason`, 1,024 bytes) becomes the run's error, which matches `workflow.ErrForced`. Both need `CapForceRun` ([Force a run to an end](../jobs/manage-runs.md#force-a-run-to-an-end)); a run in another state returns `workflow.ErrWrongState`.
- `Cancel(ctx, ref)` cancels one run. `CancelAll(ctx, q)` cancels every run `q` matches and returns how many. With no statuses it cancels only runs not yet started (`StatusScheduled` and `StatusQueued`); running and waiting runs are cancelled only when listed. A terminal status, a `Limit` or a `Cursor` is refused with `workflow.ErrInvalid`, since a bulk cancel covers every match.

## Act on many runs at once

`RetryAll(ctx, q, opts)` retries every failed run `q` matches, from the step that failed, as `Retry` does one; `q.Statuses` must be empty or exactly `[failed]`. `DeleteAll(ctx, q, opts)` forgets every finished run `q` matches; the filter must name at least one field, so "delete everything" is never an empty request (an operator who means it lists the statuses), and `q.Statuses` may list terminal statuses only. Both take `admin.BulkOptions`: `DryRun` lists what the call would touch and changes nothing, `Max` lowers the bound for this call. `PreviewCancelAll(ctx, q)` is the dry run of `CancelAll`.

<!-- snippet: examples/admin-bulk/main.go#retry -->
```go
failed := workflow.RunQuery{Workflow: "send-email"} // RetryAll adds statuses=[failed]
preview, err := adm.RetryAll(ctx, failed, admin.BulkOptions{DryRun: true})
if err != nil {
	return err
}
fmt.Fprintf(out, "dry run: would retry %d, sample %d, done %d\n", preview.Matched, len(preview.Sample), preview.Done)

res, err := adm.RetryAll(ctx, failed, admin.BulkOptions{})
if err != nil {
	return err
}
fmt.Fprintf(out, "retried %d of %d, skipped %d, more to do: %t\n", res.Done, res.Matched, res.Skipped, res.Truncated)
```

A call returns an `admin.BulkResult`: `Matched` (up to the bound), `Done`, `Skipped` (runs that changed between the listing and the action: no longer in a state the action accepts, gone, or, for a retry, their idempotency key taken by another run), `Truncated` when more runs match than the bound, and `Sample`, the first `admin.BulkSample` (20) refs, newest first. One call acts on at most `WithMaxBulk(n)` runs (`admin.DefaultMaxBulk`, 1,000), so one request does a bounded amount of work; repeat it for the rest, since the runs acted on no longer match. `Limit` and `Cursor` on `q` are refused: a bulk call covers every match.

<!-- snippet: examples/admin-bulk/main.go#delete -->
```go
_, err = adm.DeleteAll(ctx, workflow.RunQuery{}, admin.BulkOptions{DryRun: true})
fmt.Fprintln(out, "delete without a filter refused:", errors.Is(err, workflow.ErrInvalid))

done := workflow.RunQuery{Workflow: "send-email", Statuses: []workflow.Status{workflow.StatusSucceeded}}
preview, err = adm.DeleteAll(ctx, done, admin.BulkOptions{DryRun: true, Max: 3})
if err != nil {
	return err
}
fmt.Fprintf(out, "dry run: would delete %d (Max 3), more match: %t\n", preview.Matched, preview.Truncated)

res, err = adm.DeleteAll(ctx, done, admin.BulkOptions{})
if err != nil {
	return err
}
fmt.Fprintf(out, "deleted %d of %d\n", res.Done, res.Matched)
```

```text
dry run: would retry 5, sample 5, done 0
retried 5 of 5, skipped 0, more to do: false
delete without a filter refused: true
dry run: would delete 3 (Max 3), more match: true
deleted 7 of 7
```

The runs are listed first and acted on one by one, so a bulk call is not atomic: an error other than a skip stops it and returns the result so far with the error. The example is [`examples/admin-bulk`](../../examples/admin-bulk/).

## Step 4: pause and resume

`Pause(ctx, t)` holds the runs a `workflow.PauseTarget` matches without cancelling them: `workflow.PauseWorkflow(name)`, `workflow.PauseQueue(name)` or `workflow.PauseAll()`. New runs are still accepted and wait, attempts already executing finish, and on a durable driver every node obeys the pause. `Resume(ctx, t)` removes exactly that pause; `Resume(ctx, workflow.PauseAll())` removes all of them. `Paused(ctx)` lists the active ones. A driver may support one kind of target and not the other: Admin refuses the other with the capability it lacks.

## Queue depth and oldest wait

`QueueStats(ctx)` reads the queued runs page by page and returns, per queue, `Depth` (runs waiting for a worker, including runs held by a pause; delayed runs are not counted until due), `OldestEnqueuedAt` and `OldestWait` on the Admin's clock. It reads at most the max scan; past it the report says `Truncated` and every depth is a lower bound. On a client with `CapWorkerScale` (the memory, SQL and Redis drivers) each queue also carries `Workers` and `Running`, this client's worker cap and running attempts, and `QueueReport.Workers` is true; every queue the client serves is then listed, even with nothing queued, which is what a [balancing](../operations/balancing.md) dashboard shows. `QueueDepths(ctx)` returns the same as a map of queue to depth, with the signature the [OpenTelemetry depth gauge](../observability/opentelemetry.md#queue-wait-and-queue-depth) takes. Threshold alerts on these numbers are the [Monitor](alerts.md).

## Schedules

`Schedules(ctx)` lists every live schedule, declared crons and run-time `Client.Schedule` ones, sorted by workflow and name, with its cron expression, zone, tenant (`Schedule.Tenant`, set on every run it fires), whether it is paused, its next fire time (zero when paused or never again), its misfire policy, windows and labels. `PauseSchedule(ctx, id)` and `ResumeSchedule(ctx, id)` pause and resume one of them by ID. `Schedule(ctx, id)` returns one schedule (`workflow.ErrNotFound` when there is none), with its overlap policy and, on a driver with `CapScheduleHistory`, `Last`: when it last fired, the run that fire started and that run's status now. `TriggerSchedule(ctx, id)` runs a schedule now with its input and labels and returns the new run; the schedule's [overlap policy](../schedules/overlap.md) still applies, so a skip returns an error matching `workflow.ErrOverlap` and enqueues nothing ([Trigger a schedule now](../schedules/overlap.md#trigger-a-schedule-now)).

## Run-end history for charts

`Metrics(ctx, q)` returns run ends over time for a chart: an `admin.MetricsQuery` (`From`, `To`, `Step` and an optional `Workflow`) answered with an `admin.MetricsSeries`, one `MetricsPoint` per step from `From` to `To`, zero-filled, each counting `Succeeded`, `Failed`, `Cancelled` and `Discarded` (`Total()` sums them). A query asking for more than `admin.MaxMetricsPoints` (1,440) points is refused with `workflow.ErrInvalid`; widen `Step`.

The library keeps no history of its own: `WithMetrics(src)` wires a `MetricsSource`, and without one `Metrics` returns an error matching `workflow.ErrUnsupported`. An app with a metrics store (Prometheus, a warehouse table, an OpenTelemetry backend) adapts it with `admin.MetricsSourceFunc`. `admin.NewMemoryMetrics(cfg)` is the in-process default, fed by the run-finished hook: one-minute buckets (`Bucket`) kept a day (`Keep`), with a repeated delivery of the same end dropped (`Dedupe`, the last 4,096 ends):

<!-- snippet: examples/admin-bulk/main.go#metrics -->
```go
mm := admin.NewMemoryMetrics(admin.MemoryMetricsConfig{Clock: clk}) // one-minute buckets, kept a day
if err := c.OnRunFinished(mm.Observe); err != nil {                 // before Start
	return err
}
adm := admin.New(c, admin.WithMetrics(mm), admin.WithClock(clk))
```

<!-- snippet: examples/admin-bulk/main.go#chart -->
```go
series, err := adm.Metrics(ctx, admin.MetricsQuery{
	From: start,
	To:   start.Add(10 * time.Minute),
	Step: 5 * time.Minute,
})
if err != nil {
	return err
}
for _, p := range series.Points {
	fmt.Fprintf(out, "%s succeeded=%d failed=%d total=%d\n", p.At.Format("15:04"), p.Succeeded, p.Failed, p.Total())
}
```

```text
09:00 succeeded=2 failed=5 total=7
09:05 succeeded=5 failed=0 total=5
```

A retried run ends again and is counted again, as the second point shows. `MetricsSeries.Since` is the earliest time the source has data for: `MemoryMetrics` starts empty at every restart, so points before it read zero because nothing was recorded. It counts what this process's run-finished hook is told: on a durable driver where every end is delivered to one client, each process sees its share.

## Nodes and health

`Nodes(ctx)` lists the online nodes, sorted by ID, from a client implementing `workflow.NodeLister`. `Health(ctx)` is the client's health report through `workflow.CheckHealth` and never fails. Both feed the System screen of the [web UI](ui.md); a liveness or readiness probe should call `workflow.CheckHealth` on the app's own unauthenticated route instead of going through the admin API ([Nodes and health](../operations/nodes.md)).

## Captured output

`WithOutputs(src)` gives the Admin an `OutputSource`, which returns the output each attempt of a run captured with [`middleware.CaptureOutput`](../schedules/output.md); `RunOutput(ctx, ref)` reads it and `HasOutputs()` says whether one is wired. `admin.NewMemoryOutputs(admin.MemoryOutputLimits{...})` is both the capture sink and the source, bounded by default to 1,000 runs, 20 attempts per run and 64 KiB per attempt (longer output is cut and flagged). It lives in one process: a fleet whose workers run elsewhere uses a store over shared storage, such as `sqlstore.OutputStore` wired with `admin.OutputSourceFunc` ([Task output › Keep output in the database](../schedules/output.md#keep-output-in-the-database)).

## Try it

```sh
cd examples
go run ./admin
```

```text
anonymous GET /failures: 401
viewer GET /failures: 200, 1 failed run, step "send": memory: step "send" failed after 1 attempt(s): smtp: connection refused
viewer POST retry: 403 {"error":{"code":"forbidden","message":"not allowed: runs.retry"}}
admin POST retry: 200, run now succeeded
admin POST /pause emails: 200
ALERT queue_depth queue=emails depth=3 (max 2)
workflow.queue.depth{workflow.queue=emails} = 3
admin POST /resume emails: 200
resolved queue_depth queue=emails depth=0 (max 2)
```

## Pitfalls

- **`QueueStats` costs pages, not one count.** It is the only portable way to ask, so a backlog of 100,000 runs with the default max scan reads ten pages of 1,000 and reports `Truncated`. Poll it every 30 seconds or more, not on every request.
- **Retry reruns only the failed part.** A step that succeeded and had a side effect is not run again; a step that failed after a partial side effect runs again from the start, so steps must be idempotent as always.
- **`CancelAll` with no statuses leaves running work alone.** List `StatusRunning` and `StatusWaiting` explicitly to stop them too.
- **Force complete does not undo or finish the work.** It only records an end: steps that did not run never run, and a cancelled attempt may have done part of its side effect. Use it when someone finished the work by hand; use `Retry` to run it again.
- **Trigger-now ignores windows, conditions and a pause.** An operator who presses it means it; only the overlap policy can refuse.
- **`MemoryOutputs` forgets on restart and sees one process.** Fine for a single binary or a demo, not for a fleet.
- **A pause holds, it does not cancel.** Runs pile up while a queue is paused; watch the depth.
- **Preview before a bulk call, and expect repeats.** A dry run is a moment's snapshot; the real call lists again and may match more or fewer runs. A truncated call does the first `WithMaxBulk` runs only: run it again until `Truncated` is false.
- **`MemoryMetrics` forgets on restart.** Wire a `MetricsSource` over your metrics store when the charts must cover deploys.
- **Behind a wrapper, ask what is underneath.** `nodes`, `reschedule` and `cancel_step` carry no capability bit, so the Admin decides them with `workflow.Supports(c, op)` (`OpNodes`, `OpReschedule`, `OpCancelStep`). A wrapper answers through `workflow.OpProber`: `workflow.Forward` (codec and plugin clients) asks its inner client and `failover` asks both backends, so behind a wrapper these operations are offered only when the client underneath does them.

## Related

- [The admin JSON API](http-api.md): the same operations over HTTP, closed by default.
- [Queue alerts](alerts.md): depth and wait thresholds.
- [OpenTelemetry](../observability/opentelemetry.md): wait-time histogram and depth gauge.
- [Retention](../operations/retention.md): deleting finished runs on a schedule.
- [The workflow command](cli.md): these operations from a terminal.
- Examples: [examples/admin](../../examples/admin/), [examples/admin-bulk](../../examples/admin-bulk/)
