# Admin operations

The `admin` module is the operations side of the library, the calls an operator's screen or command line needs, plus the numbers a queue dashboard shows: list and search runs, show one, list failures, retry a failed run from the step that failed, forget a finished run, cancel one run or every run a filter matches, retry or delete every run a filter matches after a dry run, move a delayed run, withdraw one step, pause and resume a workflow or a queue, list schedules with their next fire time, read each queue's depth and oldest wait, chart run ends over time, and list the online nodes and the client's health. It works over any `workflow.Client`, reports what that client cannot do instead of failing later, and holds no state of its own.

Use it from Go (a CLI, a support tool, a cron job) as on this page, or over HTTP through the [admin JSON API](http-api.md). The same handler serves a [web UI](ui.md) on top of the API.

## Before you start

- `go get github.com/ubgo/workflow/admin@latest`. It is its own module and needs nothing beyond the standard library and the core.
- A client whose driver declares the capabilities of the operations you want (table below). The memory driver declares all of them; check the driver pages ([memory](../drivers/memory.md), [SQL](../drivers/sqlstore.md), [Hatchet](../drivers/hatchet.md)) for the others.
- The runnable programs for this page are [examples/admin](../../examples/admin/) (the HTTP side and a Monitor), [examples/admin-inspect](../../examples/admin-inspect/) (steps 2, queues, schedules, nodes and output from Go), [examples/admin-reset-fork](../../examples/admin-reset-fork/) (step 3 and wrappers) and [examples/admin-bulk](../../examples/admin-bulk/) (bulk actions and charts).

## Step 1: wrap the client

`admin.New(client, opts...)` never fails. `admin.WithClock(clk)` sets the clock wait times are measured on (the real clock by default) and `admin.WithMaxScan(n)` bounds how many queued runs one queue report reads (`admin.DefaultMaxScan`, 10,000). [`admin.WithRunQuerier(rq, caps)`](ds:block?id=admin-withrunquerier-g356xuy3) makes the read operations (runs, run, failures, discarded, queue stats, counts) read `rq` instead of the client, typically a run recorder's store with [`recorder.QueryCaps`](ds:block?id=rec-querycaps-x2zk9xju), while every action still goes to the client; a run `rq` does not hold is looked up on the client ([Record run history](../observability/run-recorder.md#read-the-history)).

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
| `schedule_trigger` | `TriggerSchedule(ctx, id)` (`Schedule(ctx, id)` is `schedules`) | `CapScheduleList` and `CapScheduleTrigger` | `workflow.ScheduleLister` and `workflow.ScheduleTriggerer` |
| `force` | `ForceComplete(ctx, ref)`, `ForceFail(ctx, ref, reason)` | `CapForceRun` | `workflow.RunForcer` |
| `retry_all` | `RetryAll(ctx, q, opts)` | `CapRunQuery`, `CapRetryRun` | `workflow.RunQuerier` and `workflow.RunRetrier` |
| `delete_all` | `DeleteAll(ctx, q, opts)` | `CapRunQuery`, `CapDeleteRun` | `workflow.RunQuerier` and `workflow.RunDeleter` |
| `reschedule` | `Reschedule(ctx, ref, at)` | none | `workflow.RunRescheduler` |
| `cancel_step` | `CancelStep(ctx, ref, step)` | none | `workflow.StepCanceller` |
| `edit_run` | `EditRun(ctx, ref, edit)` | none | `workflow.RunEditor` |
| `reset_run` | `ResetRun(ctx, ref, step, workflow.ResetOptions{})` | none | `workflow.RunResetter` |
| `reset_succeeded` | `ResetRun` with `ResetOptions{Succeeded: true}` | none | `workflow.RunResetterWith` |
| `fork_run` | `ForkRun(ctx, ref, step, opts)` | none | `workflow.RunForker` |
| `signal_waiting` | `SignalWaiting(ctx, ev)` | none | `workflow.TransientSignaller` |
| `nodes` | `Nodes(ctx)` | none | `workflow.NodeLister` |
| `health` | `Health(ctx)` | none | none: every client answers through `workflow.CheckHealth` |

`Admin.Support()` lists every operation with `Supported` and, when it is not, the `Missing` capabilities, so a screen can hide a button that would always fail. An operation that needs only an interface reports `Missing` zero and says why in `Reason`; for `nodes`, `reschedule`, `cancel_step`, `edit_run`, `reset_run`, `reset_succeeded`, `fork_run` and `signal_waiting` the answer comes from `workflow.Supports`, so a wrapping client reports what the client underneath does ([below](#behind-a-wrapper-ask-what-is-underneath)). `Admin.Supports(op)` answers for one operation, and `admin.Requirements(op)` lists the capabilities it needs. An unsupported call returns a `*workflow.UnsupportedError` (it matches `workflow.ErrUnsupported`) whose `Field` is `admin.<operation>`. Admin checks the capability as well as the interface, because a wrapping client (`middleware.WithCodec`, `failover`, a plugin client) implements every interface and refuses at the call when the client it wraps cannot do it.

### Behind a wrapper, ask what is underneath

The operations with no capability bit (`workflow.Op`: `OpNodes`, `OpReschedule`, `OpCancelStep`, `OpEditRun`, `OpResetRun`, `OpResetSucceeded`, `OpForkRun`, `OpSignalWaiting`, plus `OpLifecycle` and `OpRunSignals` outside the admin) cannot be judged by a type assertion: a wrapper that embeds `workflow.Forward` has every method whatever it wraps, and refuses at the call. `workflow.Supports(c, op)` asks the client's `workflow.OpProber` when it has one, and `Forward` answers for its `Inner` client (`failover` asks both of its backends). An app's own wrapper gets this by embedding `Forward`:

<!-- snippet: examples/admin-reset-fork/main.go#wrapper -->
```go
// audited is an app's own client wrapper (it would log each enqueue).
// Embedding workflow.Forward beside the wrapped client keeps the optional
// operations of the client underneath reachable; Forward's Supports
// method (workflow.OpProber) answers for that client, not for the
// wrapper, whose methods exist whatever it wraps.
type audited struct {
	workflow.Client
	workflow.Forward
}

// wrap returns c inside audited, with Forward pointing at the same client
// so the optional operations stay reachable.
func wrap(c workflow.Client) *audited {
	return &audited{Client: c, Forward: workflow.Forward{Inner: c}}
}

// plain hides every optional interface of the client it holds, like a
// driver that has none of them.
type plain struct{ workflow.Client }
```

<!-- snippet: examples/admin-reset-fork/main.go#support -->
```go
for _, cl := range []struct {
	name string
	c    workflow.Client
}{
	{"memory client", c},
	{"wrapper over memory", wrap(c)},
	{"wrapper over a plain client", wrap(plain{c})},
} {
	_, hasMethod := cl.c.(workflow.RunResetter) // true for both wrappers
	adm := admin.New(cl.c)
	fmt.Fprintf(w, "%-28s has ResetRun %-5v Supports(OpResetRun) %-5v admin reset_run %v\n",
		cl.name, hasMethod, workflow.Supports(cl.c, workflow.OpResetRun), adm.Supports(admin.OpResetRun))
}
```

```text
memory client                has ResetRun true  Supports(OpResetRun) true  admin reset_run true
wrapper over memory          has ResetRun true  Supports(OpResetRun) true  admin reset_run true
wrapper over a plain client  has ResetRun true  Supports(OpResetRun) false admin reset_run false
wrapper over a plain client: nodes supported=false reason="the client does not implement the interface this operation needs"
```

The last line is `Admin.Support()` for the wrapper over a client with no `NodeLister`: `Missing` is zero because `nodes` needs no capability, so `Reason` says an interface is absent. A screen hides the button; a direct call returns a `*workflow.UnsupportedError` matching `workflow.ErrUnsupported`. A wrapper that cannot embed `Forward` implements `Supports(op workflow.Op) bool` itself and answers for what it wraps.

## Step 2: find runs

`Runs(ctx, q)` takes a `workflow.RunQuery`: `Workflow`, `Queue`, `Statuses`, `Labels` (every pair must match), `From` and `To` (enqueue time, `[From, To)`), and `Limit` (default 100, at most 1,000) with `Cursor`. It returns a `RunPage`, newest enqueue first; pass `page.Next` back as `q.Cursor` until it is empty. The cursor is the driver's own and opaque.

<!-- snippet: examples/admin-inspect/main.go#runs -->
```go
q := workflow.RunQuery{Workflow: wfImport, Labels: map[string]string{"source": "sftp"}, Limit: pageSize}
for page := 1; ; page++ {
	p, err := adm.Runs(ctx, q)
	if err != nil {
		return err
	}
	for _, r := range p.Runs {
		fmt.Fprintf(w, "page %d: %s %s %s\n", page, r.Ref.RunID, r.Status, r.Payload)
	}
	if p.Next == "" {
		break
	}
	q.Cursor = p.Next // the driver's own opaque cursor
}
```

```text
page 1: run-14 succeeded orders-5.csv
page 1: run-11 failed orders-4-bad.csv
page 2: run-8 succeeded orders-3.csv
page 2: run-5 failed orders-2-bad.csv
page 3: run-2 succeeded orders-1.csv
```

`Failures(ctx, q)` is `Runs` with `Statuses` set to `StatusFailed`; it refuses a query that already sets statuses. `Discarded(ctx, q)` is the same for `StatusDiscarded`: runs a step dropped with `workflow.Discard`, kept apart from failures because nobody has to act on them (see [Run states](../jobs/run-states.md#discarded)). A run that is retrying carries `NextAttempt` and `NextAttemptAt`. `Run(ctx, ref)` returns one run, `workflow.ErrNotFound` when there is none. A `RunInfo` carries the ref, workflow, queue, status, the error text and the step that failed, labels, the stored payload, priority, enqueue, start and finish times, the attempt count and, on a driver with `CapErrorHistory`, `Errors`: each failed attempt's step, attempt number, error, time, stack and, with `CapNodeIdentity`, node ([Error history](../jobs/manage-runs.md#error-history)). Newer fields, each filled where the driver has the capability: `Tenant` (`CapTenant`), `RunAt`, when a delayed run is or was due (`CapDueQuery`), `Progress`, the latest `workflow.ReportProgress`, `Output`, the result of a succeeded run (`CapResult`), and `AttemptedBy`, the nodes that ran its attempts (`CapNodeIdentity`). `RunQuery.Tenant` filters by tenant and `RunQuery.DueFrom` and `DueTo` by due time, so "what fires in the next hour" is one query.

<!-- snippet: examples/admin-inspect/main.go#failures -->
```go
failed, err := adm.Failures(ctx, workflow.RunQuery{Workflow: wfImport})
if err != nil {
	return err
}
for _, r := range failed.Runs {
	fmt.Fprintf(w, "failed %s at step %s, %d attempt(s): %s\n", r.Ref.RunID, r.FailedStep, r.Attempts, r.Errors[0].Error)
}
discarded, err := adm.Discarded(ctx, workflow.RunQuery{}) // dropped on purpose: nobody has to act
if err != nil {
	return err
}
fmt.Fprintf(w, "discarded: %d run of %s\n", len(discarded.Runs), discarded.Runs[0].Workflow)
```

```text
failed run-11 at step load, 1 attempt(s): load: header row missing
failed run-5 at step load, 1 attempt(s): load: header row missing
discarded: 1 run of cleanup
Failures with statuses set: true
```

The last line is `Failures` refusing a query that already sets `Statuses`.

`CountRuns(ctx, q)` counts the runs a query matches, reading at most `WithMaxScan` runs, and returns a `RunCount`: `Total`, `Truncated` when the scan stopped first, and `Cursors`, the cursor that starts each page at `q.Limit`, so a screen can show "1–50 of N" and numbered pages over a driver's opaque cursors. `q.Cursor` must be empty. `RunQuery.Labels` matches values exactly and `RunQuery.LabelPrefixes` by prefix ([Search by label](../jobs/manage-runs.md#search-by-label)); a prefix on a client without `CapLabelPrefix` is refused with an `UnsupportedError`.

<!-- snippet: examples/admin-inspect/main.go#count -->
```go
n, err := adm.CountRuns(ctx, workflow.RunQuery{Workflow: wfImport, Limit: pageSize})
if err != nil {
	return err
}
fmt.Fprintf(w, "%d runs, truncated %v, %d pages\n", n.Total, n.Truncated, len(n.Cursors))
// n.Cursors[i] is the Cursor that opens page i+1 ("" for the first).
```

```text
5 runs, truncated false, 3 pages
```

## Step 3: act on runs

- `Retry(ctx, ref)` re-runs a failed run from the step that failed: steps that succeeded keep their outputs and do not run again. A run that is not failed returns `workflow.ErrWrongState`; a run whose idempotency key another unfinished run now holds returns `workflow.ErrDuplicate`.
- `Delete(ctx, ref)` forgets a finished run; an unfinished one returns `workflow.ErrWrongState`. To forget many, use `DeleteAll` ([below](#act-on-many-runs-at-once)) or the driver's [retention](../operations/retention.md).
- `Reschedule(ctx, ref, at)` moves a delayed run that has not started to `at`, keeping its ref, payload, labels and key; a run in another state returns `workflow.ErrWrongState`. A zero `at` is refused here: an operator who means "now" passes the current time.
- `EditRun(ctx, ref, workflow.JobEdit{Payload, Priority, RunAt})` changes a delayed run that has not started; a nil field is not changed, and an edit that changes nothing is refused with `workflow.ErrInvalid`. It returns the run's ref ([Edit a run before it starts](../jobs/edit-runs.md)).
- `CancelStep(ctx, ref, step)` withdraws one step. A running attempt has its context cancelled with the cause `workflow.ErrStepCancelled` and its handler decides: returning an output lets the step succeed and the run go on, returning an error fails the step for good (no retry) and the run with it. A step waiting on a backoff, a snooze, an event or a sleep fails for good at once. A step that has not started or has ended returns `workflow.ErrWrongState`.
- `ResetRun(ctx, ref, step, workflow.ResetOptions{})` re-runs a failed run in place from `step`: it and every step downstream of it run again, the other steps that succeeded keep their outputs, and the run keeps its ref. With `ResetOptions{Succeeded: true}` a run that succeeded is reset too, the explicit opt-in for redoing work that already succeeded (it gets `workflow.LabelRedo`). A run in another state returns `workflow.ErrWrongState`, a step the workflow does not have `workflow.ErrInvalid`. The operations are `reset_run` and `reset_succeeded` ([Manage runs › Re-run a failed run from an earlier step](../jobs/manage-runs.md#re-run-a-failed-run-from-an-earlier-step)).
- `ForkRun(ctx, ref, step, workflow.ForkOptions{IdempotencyKey, Labels})` starts a new run from a finished one at `step`, with the outputs of the steps before it, and returns the new run's ref; the original is not changed. The fork keeps the original's workflow, payload, tenant, queue and priority, never its chain or its idempotency key; a run that has not finished returns `workflow.ErrWrongState`, a reserved label `workflow.ErrInvalid`. The operation is `fork_run` ([Manage runs › Fork a run into a new run](../jobs/manage-runs.md#fork-a-run-into-a-new-run)).
- `SignalWaiting(ctx, workflow.Event{Name, Key, Payload})` delivers an event only to the runs waiting for it now and returns how many it resumed; zero is not an error, and an event without a name is refused with `workflow.ErrInvalid`. The operation is `signal_waiting` ([Signals › Deliver only to runs waiting now](../workflows/signals.md#deliver-only-to-runs-waiting-now)).
- `ForceComplete(ctx, ref)` marks an unfinished or failed run succeeded, for work an operator finished by hand: an attempt in progress is cancelled and its result ignored, and the run counts as a success (its idempotency key is freed, a chain continues, run-finished observers see `StatusSucceeded`). `ForceFail(ctx, ref, reason)` marks an unfinished run failed with no retry and without running `OnFailure`; the reason (at most `admin.MaxForceReason`, 1,024 bytes) becomes the run's error, which matches `workflow.ErrForced`. Both need `CapForceRun` ([Force a run to an end](../jobs/manage-runs.md#force-a-run-to-an-end)); a run in another state returns `workflow.ErrWrongState`.
- `Cancel(ctx, ref)` cancels one run. `CancelAll(ctx, q)` cancels every run `q` matches and returns how many. With no statuses it cancels only runs not yet started (`StatusScheduled` and `StatusQueued`); running and waiting runs are cancelled only when listed. A terminal status, a `Limit` or a `Cursor` is refused with `workflow.ErrInvalid`, since a bulk cancel covers every match.

The program [`examples/admin-reset-fork`](../../examples/admin-reset-fork/) does each of these from Go. Reset a failed run from the step whose input was wrong, once the fix is deployed; the run keeps its ref:

<!-- snippet: examples/admin-reset-fork/main.go#reset -->
```go
ref, err := adm.ResetRun(ctx, failed, "transform", workflow.ResetOptions{})
if err != nil {
	return err
}
```

A run that succeeded is reset only with the explicit opt-in:

<!-- snippet: examples/admin-reset-fork/main.go#reset-succeeded -->
```go
_, err = adm.ResetRun(ctx, failed, "transform", workflow.ResetOptions{})
fmt.Fprintln(w, "reset a succeeded run without the opt-in:", errors.Is(err, workflow.ErrWrongState))
ref, err = adm.ResetRun(ctx, failed, "transform", workflow.ResetOptions{Succeeded: true})
if err != nil {
	return err
}
```

Fork a finished run into a new one that starts at a step, with a key of its own:

<!-- snippet: examples/admin-reset-fork/main.go#fork -->
```go
fork, err := adm.ForkRun(ctx, failed, "publish", workflow.ForkOptions{
	IdempotencyKey: "replay-" + failed.RunID.String(), // the fork's own key, never the original's
	Labels:         map[string]string{"ticket": "OPS-7"},
})
if err != nil {
	return err
}
```

```text
reset run-2 from transform: same run true, now succeeded
reset a succeeded run without the opt-in: true
reset with Succeeded: succeeded, redo label "true"
fork run-20: new run true, succeeded, fork-of run-2 from step publish, ticket OPS-7
fork an unfinished run: true
```

Signal only the runs waiting now; a second identical signal finds nobody and returns zero:

<!-- snippet: examples/admin-reset-fork/main.go#signal -->
```go
n, err := adm.SignalWaiting(ctx, workflow.Event{Name: eventName, Key: "eu", Payload: []byte(`{"reason":"deploy"}`)})
if err != nil {
	return err
}
fmt.Fprintln(w, "resumed:", n)
n, err = adm.SignalWaiting(ctx, workflow.Event{Name: eventName, Key: "eu"}) // nobody waits for eu now
if err != nil {
	return err
}
fmt.Fprintln(w, "resumed again:", n, "(zero is not an error)")
```

```text
resumed: 1
resumed again: 0 (zero is not an error)
an event without a name: true
```

Withdraw a parked step, and end two runs from outside:

<!-- snippet: examples/admin-reset-fork/main.go#cancel-step -->
```go
if err := adm.CancelStep(ctx, waiting["us"], "await"); err != nil {
	return err
}
```

<!-- snippet: examples/admin-reset-fork/main.go#force -->
```go
if err := adm.ForceComplete(ctx, waiting["apac"]); err != nil { // someone refreshed apac by hand
	return err
}
if err := adm.ForceFail(ctx, waiting["latam"], "region retired"); err != nil {
	return err
}
```

```text
cancel step await of run-7: run failed at step await
forced apac: succeeded
forced latam: failed, error workflow: run ended by an operator: region retired
a reason over 1024 bytes: true
```

Change a delayed run before it starts. A delayed run's ref holds only its `ScheduledID` until it starts; pass the ref you got back:

<!-- snippet: examples/admin-reset-fork/main.go#edit -->
```go
payload, priority := []byte(`"dana"`), 5 // higher goes first; zero is the default
delayed, err = adm.EditRun(ctx, delayed, workflow.JobEdit{Payload: &payload, Priority: &priority})
if err != nil {
	return err
}
delayed, err = adm.Reschedule(ctx, delayed, start.Add(2*delayedBy)) // one delayedBy later than first due
if err != nil {
	return err
}
```

```text
edited and moved delayed-26: scheduled, priority 5, due 11:00:00
reschedule to the zero time: true
```

## Act on many runs at once

`RetryAll(ctx, q, opts)` retries every failed run `q` matches, from the step that failed, as `Retry` does one; `q.Statuses` must be empty or exactly `[failed]`. `DeleteAll(ctx, q, opts)` forgets every finished run `q` matches; the filter must name at least one field, so "delete everything" is never an empty request (an operator who means it lists the statuses), and `q.Statuses` may list terminal statuses only. Both take `admin.BulkOptions`: `DryRun` lists what the call would touch and changes nothing, `Max` lowers the bound for this call. `PreviewCancelAll(ctx, q)` is the dry run of `CancelAll`.

<!-- snippet: examples/admin-bulk/main.go#retry -->
```go
failed := workflow.RunQuery{Workflow: "send-email"} // RetryAll adds statuses=[failed]
preview, err := adm.RetryAll(ctx, failed, admin.BulkOptions{DryRun: true})
if err != nil {
	return err
}
fmt.Printf("dry run: would retry %d, sample %d, done %d\n", preview.Matched, len(preview.Sample), preview.Done)

res, err := adm.RetryAll(ctx, failed, admin.BulkOptions{})
if err != nil {
	return err
}
fmt.Printf("retried %d of %d, skipped %d, more to do: %t\n", res.Done, res.Matched, res.Skipped, res.Truncated)
```

A call returns an `admin.BulkResult`: `Matched` (up to the bound), `Done`, `Skipped` (runs that changed between the listing and the action: no longer in a state the action accepts, gone, or, for a retry, their idempotency key taken by another run), `Truncated` when more runs match than the bound, and `Sample`, the first `admin.BulkSample` (20) refs, newest first. One call acts on at most `WithMaxBulk(n)` runs (`admin.DefaultMaxBulk`, 1,000), so one request does a bounded amount of work; repeat it for the rest, since the runs acted on no longer match. `Limit` and `Cursor` on `q` are refused: a bulk call covers every match.

<!-- snippet: examples/admin-bulk/main.go#delete -->
```go
_, err = adm.DeleteAll(ctx, workflow.RunQuery{}, admin.BulkOptions{DryRun: true})
fmt.Println("delete without a filter refused:", errors.Is(err, workflow.ErrInvalid))

done := workflow.RunQuery{Workflow: "send-email", Statuses: []workflow.Status{workflow.StatusSucceeded}}
preview, err = adm.DeleteAll(ctx, done, admin.BulkOptions{DryRun: true, Max: 3})
if err != nil {
	return err
}
fmt.Printf("dry run: would delete %d (Max 3), more match: %t\n", preview.Matched, preview.Truncated)

res, err = adm.DeleteAll(ctx, done, admin.BulkOptions{})
if err != nil {
	return err
}
fmt.Printf("deleted %d of %d\n", res.Done, res.Matched)
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

`QueueStats(ctx)` reads the queued runs page by page and returns, per queue, `Depth` (runs waiting for a worker, including runs held by a pause; delayed runs are not counted until due), `OldestEnqueuedAt` and `OldestWait` on the Admin's clock. It reads at most the max scan; past it the report says `Truncated` and every depth is a lower bound. On a client with `CapWorkerScale` (the memory, SQL and Redis drivers) each queue also carries `Workers` and `Running`, this client's worker cap and running attempts, and `QueueReport.Workers` is true; every queue the client reports loads for is then listed, even with nothing queued (on the memory driver, the queues declared with `memory.WithQueues`), which is what a [balancing](../operations/balancing.md) dashboard shows. `QueueDepths(ctx)` returns the same as a map of queue to depth, with the signature the [OpenTelemetry depth gauge](../observability/opentelemetry.md#queue-wait-and-queue-depth) takes. Threshold alerts on these numbers are the [Monitor](alerts.md).

<!-- snippet: examples/admin-inspect/main.go#queues -->
```go
rep, err := adm.QueueStats(ctx)
if err != nil {
	return err
}
for _, s := range rep.Queues {
	fmt.Fprintf(w, "%-8s depth %d, oldest wait %s, workers %d, running %d\n", s.Queue, s.Depth, s.OldestWait, s.Workers, s.Running)
}
fmt.Fprintf(w, "truncated %v, worker counts %v, at %s\n", rep.Truncated, rep.Workers, rep.At.Format(time.TimeOnly))

small := admin.New(c, admin.WithClock(clk), admin.WithMaxScan(smallScan))
depths, err := small.QueueDepths(ctx) // map[queue]depth, the OpenTelemetry gauge's signature
if err != nil {
	return err
}
fmt.Fprintf(w, "with WithMaxScan(%d): emails depth %d (a lower bound)\n", smallScan, depths[queueEmails])
```

```text
default  depth 0, oldest wait 0s, workers 4, running 0
emails   depth 4, oldest wait 3m0s, workers 2, running 0
truncated false, worker counts true, at 09:03:00
with WithMaxScan(3): emails depth 3 (a lower bound)
```

The emails queue is paused with four runs held for three minutes. The second Admin reads at most three queued runs, so its depth stops at three: a lower bound, which `QueueReport.Truncated` reports on the full report.

## Schedules

`Schedules(ctx)` lists every live schedule, declared crons and run-time `Client.Schedule` ones, sorted by workflow and name, with its cron expression, zone, tenant (`Schedule.Tenant`, set on every run it fires), whether it is paused, its next fire time (zero when paused or never again), its misfire policy, windows and labels. `PauseSchedule(ctx, id)` and `ResumeSchedule(ctx, id)` pause and resume one of them by ID. `Schedule(ctx, id)` returns one schedule (`workflow.ErrNotFound` when there is none), with its overlap policy and, on a driver with `CapScheduleHistory`, `Last`: when it last fired, the run that fire started and that run's status now. `TriggerSchedule(ctx, id)` runs a schedule now with its input and labels and returns the new run; the schedule's [overlap policy](../schedules/overlap.md) still applies, so a skip returns an error matching `workflow.ErrOverlap` and enqueues nothing ([Trigger a schedule now](../schedules/overlap.md#trigger-a-schedule-now)).

<!-- snippet: examples/admin-inspect/main.go#schedules -->
```go
if err := adm.PauseSchedule(ctx, sched); err != nil {
	return err
}
all, err := adm.Schedules(ctx)
if err != nil {
	return err
}
fmt.Fprintf(w, "%s %q paused %v, next fire zero %v\n", all[0].ID, all[0].Cron, all[0].Paused, all[0].Next.IsZero())
if err := adm.ResumeSchedule(ctx, sched); err != nil {
	return err
}
ref, err := adm.TriggerSchedule(ctx, sched) // runs now with the schedule's input and labels
if err != nil {
	return err
}
if err := awaitEnd(ctx, c, ref); err != nil {
	return err
}
one, err := adm.Schedule(ctx, sched)
if err != nil {
	return err
}
fmt.Fprintf(w, "%s next fire %s\n", one.ID, one.Next.Format(time.DateTime))
if !one.Last.Fire.IsZero() { // zero before the first fire, or without CapScheduleHistory
	fmt.Fprintf(w, "last fire %s started %s, now %s (trigger returned %s)\n", one.Last.Fire.Format(time.TimeOnly), one.Last.Run.RunID, one.Last.Status, ref.RunID)
}
```

```text
schedule-31 "0 2 * * *" paused true, next fire zero true
schedule-31 next fire 2026-10-15 02:00:00
last fire 09:03:00 started run-33, now succeeded (trigger returned run-33)
```

A paused schedule has a zero next fire. `Last` is the zero `ScheduleLast` until the schedule has fired, and on a driver without `CapScheduleHistory`.

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
	fmt.Printf("%s succeeded=%d failed=%d total=%d\n", p.At.Format("15:04"), p.Succeeded, p.Failed, p.Total())
}
```

```text
09:00 succeeded=2 failed=5 total=7
09:05 succeeded=5 failed=0 total=5
```

A retried run ends again and is counted again, as the second point shows. `MetricsSeries.Since` is the earliest time the source has data for: `MemoryMetrics` starts empty at every restart, so points before it read zero because nothing was recorded. It counts what this process's run-finished hook is told: on a durable driver where every end is delivered to one client, each process sees its share.

## Nodes and health

`Nodes(ctx)` lists the online nodes, sorted by ID, from a client implementing `workflow.NodeLister`. `Health(ctx)` is the client's health report through `workflow.CheckHealth` and never fails. Both feed the System screen of the [web UI](ui.md); a liveness or readiness probe should call `workflow.CheckHealth` on the app's own unauthenticated route instead of going through the admin API ([Nodes and health](../operations/nodes.md)).

<!-- snippet: examples/admin-inspect/main.go#nodes -->
```go
nodes, err := adm.Nodes(ctx)
if err != nil {
	return err
}
for _, nd := range nodes {
	fmt.Fprintf(w, "node %s (%s) queues %v, %d workers, %d running\n", nd.ID, nd.Mode, nd.Queues, nd.Workers, nd.Running)
}
h := adm.Health(ctx) // never fails: an unreachable backend is a failed check
fmt.Fprintf(w, "live %v, ready %v\n", h.Live, h.Ready)
for _, chk := range h.Checks {
	fmt.Fprintf(w, "  check %s ok=%v %s\n", chk.Name, chk.OK, chk.Detail)
}
```

```text
node worker-1 (full) queues [default emails], 4 workers, 0 running
live true, ready true
  check workers ok=true 0 of 4 busy
```

## Captured output

`WithOutputs(src)` gives the Admin an `OutputSource`, which returns the output each attempt of a run captured with [`middleware.CaptureOutput`](../schedules/output.md); `RunOutput(ctx, ref)` reads it and `HasOutputs()` says whether one is wired. `admin.NewMemoryOutputs(admin.MemoryOutputLimits{...})` is both the capture sink and the source, bounded by default to 1,000 runs, 20 attempts per run and 64 KiB per attempt (longer output is cut and flagged). It lives in one process: a fleet whose workers run elsewhere uses a store over shared storage, such as `sqlstore.OutputStore` wired with `admin.OutputSourceFunc` ([Task output › Keep output in the database](../schedules/output.md#keep-output-in-the-database)).

One `MemoryOutputs` is handed to `middleware.CaptureOutput` as the sink and to `admin.WithOutputs` as the source. `MemoryOutputLimits` sets `Runs`, `PerRun` and `Bytes`; zero keeps each default (`admin.DefaultOutputRuns`, `DefaultOutputPerRun`, `DefaultOutputBytes`):

<!-- snippet: examples/admin-inspect/main.go#outputs -->
```go
outs := admin.NewMemoryOutputs(admin.MemoryOutputLimits{
	Runs:   100,         // the runs whose output is kept (default 1,000)
	PerRun: 5,           // the newest attempts per run (default 20)
	Bytes:  outputBytes, // per attempt; longer text is cut and flagged (default 64 KiB)
})
c := memory.New(memory.WithClock(clk),
	memory.WithMiddleware(middleware.CaptureOutput(outs, middleware.CaptureClock(clk))), // outs is the sink
	memory.WithNodeID("worker-1"), memory.WithWorkers(4), // four step attempts at once on this node
	memory.WithQueues(workflow.QueueConfig{Name: workflow.DefaultQueue}, workflow.QueueConfig{Name: queueEmails, Workers: 2}), // two emails at a time
)
adm := admin.New(c, admin.WithClock(clk), admin.WithOutputs(outs)) // and the source
```

Each `admin.AttemptOutput` carries the `Step`, the `Attempt` number, `Started` and `Finished` on the capturing clock, the `Text` in `middleware.FormatOutput`'s format, `Truncated` when the text was cut, and the attempt's `Error` (empty when it succeeded), oldest attempt first:

<!-- snippet: examples/admin-inspect/main.go#output -->
```go
out, err := adm.RunOutput(ctx, failed.Runs[0].Ref)
if err != nil {
	return err
}
for _, a := range out {
	fmt.Fprintf(w, "step %s attempt %d, error %q, truncated %v\n", a.Step, a.Attempt, a.Error, a.Truncated)
	fmt.Fprintf(w, "%s\n", strings.TrimSpace(a.Text))
}
```

```text
step load attempt 1, error "load: header row missing", truncated true
== 2026-10-14T09:00:00Z import/load attempt 1 [run-11] error: load: header row missing
2026-10-14T09:00:00Z INFO loading file name=orders-4-
without WithOutputs: true
```

The example bounds each attempt to 140 bytes, so the log line is cut and `Truncated` is true. Without `WithOutputs`, `RunOutput` returns an error matching `workflow.ErrUnsupported`, and the JSON API answers 501.

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

The reads and the actions from Go each have a program of their own:

```sh
go run ./admin-inspect
```

```text
-- runs, page by page
page 1: run-14 succeeded orders-5.csv
page 1: run-11 failed orders-4-bad.csv
page 2: run-8 succeeded orders-3.csv
page 2: run-5 failed orders-2-bad.csv
page 3: run-2 succeeded orders-1.csv
-- failed and discarded
failed run-11 at step load, 1 attempt(s): load: header row missing
failed run-5 at step load, 1 attempt(s): load: header row missing
discarded: 1 run of cleanup
Failures with statuses set: true
-- count for numbered pages
5 runs, truncated false, 3 pages
-- queues
default  depth 0, oldest wait 0s, workers 4, running 0
emails   depth 4, oldest wait 3m0s, workers 2, running 0
truncated false, worker counts true, at 09:03:00
with WithMaxScan(3): emails depth 3 (a lower bound)
-- schedules
schedule-31 "0 2 * * *" paused true, next fire zero true
schedule-31 next fire 2026-10-15 02:00:00
last fire 09:03:00 started run-33, now succeeded (trigger returned run-33)
-- nodes and health
node worker-1 (full) queues [default emails], 4 workers, 0 running
live true, ready true
  check workers ok=true 0 of 4 busy
-- captured output
step load attempt 1, error "load: header row missing", truncated true
== 2026-10-14T09:00:00Z import/load attempt 1 [run-11] error: load: header row missing
2026-10-14T09:00:00Z INFO loading file name=orders-4-
without WithOutputs: true
```

```sh
go run ./admin-reset-fork
```

```text
-- what a wrapper supports
memory client                has ResetRun true  Supports(OpResetRun) true  admin reset_run true
wrapper over memory          has ResetRun true  Supports(OpResetRun) true  admin reset_run true
wrapper over a plain client  has ResetRun true  Supports(OpResetRun) false admin reset_run false
wrapper over a plain client: nodes supported=false reason="the client does not implement the interface this operation needs"
-- reset and fork
reset run-2 from transform: same run true, now succeeded
reset a succeeded run without the opt-in: true
reset with Succeeded: succeeded, redo label "true"
fork run-20: new run true, succeeded, fork-of run-2 from step publish, ticket OPS-7
fork an unfinished run: true
-- signal the runs waiting now
resumed: 1
resumed again: 0 (zero is not an error)
an event without a name: true
-- withdraw a step, force an end
cancel step await of run-7: run failed at step await
forced apac: succeeded
forced latam: failed, error workflow: run ended by an operator: region retired
a reason over 1024 bytes: true
-- edit and move a delayed run
edited and moved delayed-26: scheduled, priority 5, due 11:00:00
reschedule to the zero time: true
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
- **Reset and fork do the work again.** Unlike `Retry`, which runs only the failed part, they run the chosen step and everything downstream of it again, side effects included; pick the latest step that fixes the result.
- **A type assertion lies behind a wrapper.** `c.(workflow.RunResetter)` is true for any wrapper that embeds `workflow.Forward`; ask `workflow.Supports(c, op)` or `Admin.Supports(op)` before offering an operation ([Behind a wrapper](#behind-a-wrapper-ask-what-is-underneath)).
- **A step cancel on a parked step ends the run a moment later.** For a step waiting on an event, a sleep or a backoff, the driver ends the run on its own goroutine after `CancelStep` returns; read the run again before showing its new state.
- **A delayed run is addressed by its `ScheduledID`.** Until it starts, the ref `Enqueue` returns for a delayed run carries only a `ScheduledID`; keep the whole ref, and over HTTP add `?scheduled=1`.
- **`Resume(ctx, workflow.PauseAll())` lifts every pause**, the queue and workflow pauses included, not only the one `PauseAll` set.
- **On the memory driver only declared queues report workers.** `QueueStat.Workers` and `Running` come from `memory.WithQueues`; a queue served only by the default pool appears when it has queued runs, without its worker count.

## Related

- [The admin JSON API](http-api.md): the same operations over HTTP, closed by default.
- [Queue alerts](alerts.md): depth and wait thresholds.
- [OpenTelemetry](../observability/opentelemetry.md): wait-time histogram and depth gauge.
- [Retention](../operations/retention.md): deleting finished runs on a schedule.
- [The workflow command](cli.md): these operations from a terminal.
- Examples: [examples/admin](../../examples/admin/), [examples/admin-bulk](../../examples/admin-bulk/), [examples/admin-inspect](../../examples/admin-inspect/) (the reads from Go), [examples/admin-reset-fork](../../examples/admin-reset-fork/) (the actions from Go and `workflow.Supports`)
