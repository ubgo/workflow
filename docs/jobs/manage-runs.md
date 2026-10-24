# Manage runs

When a run fails at 3am, an operator needs to find it, see why, fix the cause and run it again; when a bad deploy floods a queue, they need to stop it and clear it. This page covers the operations the library offers for that: listing runs with filters and pages, re-running a failed run from the step that failed, forgetting a finished run, cancelling every waiting run at once, reading each attempt's error, forcing a run to an end, pausing a workflow, finding and moving the delayed runs due soon, and withdrawing one step of a running run.

## Before you start

- Each operation is an optional interface beside `workflow.Client`, declared by a capability: `RunQuerier` (`CapRunQuery`), `RunRetrier` (`CapRetryRun`), `RunDeleter` (`CapDeleteRun`), `BulkCanceller` (`CapCancelAll`), `RunForcer` (`CapForceRun`) and `Pauser` (`CapPauseWorkflow`, `CapPauseQueue`); error history is `CapErrorHistory`. The memory and SQL drivers have them all; the Redis driver has `RunQuerier`, `RunDeleter`, `RunForcer` and error history only; check `Capabilities()` on others, and see [Pitfalls](#pitfalls).
- Every wrapper in the library (the client `workflow.Use` returns, middleware, the test doubles) forwards these interfaces, so a type assertion on a wrapped client works. A wrapper over a client that lacks one returns an `UnsupportedError` naming the capability.
- The example is [`examples/manage-runs`](../../examples/manage-runs). Run it with `go run ./manage-runs` from `examples/`.
- Filtering by due time needs `workflow.CapDueQuery`. Moving a delayed run (`workflow.RunRescheduler`) and withdrawing one step (`workflow.StepCanceller`) are optional interfaces with no capability bit: check with a type assertion, and a wrapper over a client without them returns an `UnsupportedError` from the call. The memory, Redis and SQL drivers have all three; Hatchet has `CapDueQuery` and `Reschedule` (for a delayed start due more than 15 seconds away) but no `CancelStep`. Their example is [`examples/run-reschedule`](../../examples/run-reschedule), on the memory driver with a fake clock; run it with `go run ./run-reschedule`.

## Step 1: list runs

`Runs` takes a `workflow.RunQuery` and returns one page, newest enqueue first. Every field you set must match: workflow, queue, statuses, labels (every pair exactly) and an enqueue time range `[From, To)`.

<!-- snippet: examples/manage-runs/main.go#list -->
```go
page, err := c.Runs(ctx, workflow.RunQuery{
	Workflow: "fulfil-order",
	Statuses: []workflow.Status{workflow.StatusFailed},
	Labels:   map[string]string{"tenant": "acme"},
	Limit:    50,
})
if err != nil {
	return err
}
for _, r := range page.Runs {
	fmt.Fprintf(out, "failed: %s at step %q: %s\n", r.Ref.RunID, r.FailedStep, r.Error)
}
// page.Next is empty on the last page; pass it as Cursor for the next one.
```

Set `Ref` to the `RunRef` you hold to fetch one run: every field of it that is set must match, so the ref `Enqueue` returned for a delayed run still finds it after it started. Each `workflow.RunInfo` carries the run's ref, workflow, queue, status, `Error` (the message of the failure or cancel) and `FailedStep`, its labels and payload, the enqueue, first-start and finish times, and the number of step attempts. `Limit` is the page size: zero means `workflow.DefaultRunsLimit` (100) and more than `workflow.MaxRunsLimit` (1000) is refused. Pass `page.Next` back as `Cursor` for the next page; it is empty on the last. The cursor is a position, not an offset, so runs enqueued between two calls do not shift the next page.

## Search by label

Labels are how you find runs again: a tenant, an order number, a source. `RunQuery.Labels` matches values exactly; `RunQuery.LabelPrefixes` matches values that start with a prefix, and an empty prefix matches any run that has the key at all. Both may be combined with each other and with every other filter, and every pair must match. They work in `Runs` and `CancelAll`. The program is [`examples/label-search`](../../examples/label-search):

<!-- snippet: examples/label-search/main.go#search -->
```go
acmeA, err := c.Runs(ctx, workflow.RunQuery{
	Labels:        map[string]string{"tenant": "acme"}, // exact value
	LabelPrefixes: map[string]string{"order": "A-"},    // value starts with A-
})
if err != nil {
	return err
}
withCoupon, err := c.Runs(ctx, workflow.RunQuery{
	LabelPrefixes: map[string]string{"coupon": ""}, // the key is present, any value
})
if err != nil {
	return err
}
```

```text
acme, order A-*: A-1001 A-1002
has a coupon: A-1002 A-1003
```

A prefix filter needs `workflow.CapLabelPrefix`, which the in-memory, SQL and Redis drivers declare. A driver without it refuses the query with an `UnsupportedError` for the field `RunQuery.LabelPrefixes` (`workflow.CheckRunQuery` makes that decision) rather than returning a wider page. Hatchet refuses it, because the engine matches metadata by exact value only:

<!-- snippet: examples/label-search/main.go#refused -->
```go
q := workflow.RunQuery{LabelPrefixes: map[string]string{"order": "A-"}}
err = workflow.CheckRunQuery(hatchet.New(hatchet.Config{}).Capabilities(), q)
var unsupported *workflow.UnsupportedError
if errors.As(err, &unsupported) {
	fmt.Fprintf(out, "hatchet refuses: field %s, missing %s\n", unsupported.Field, unsupported.Missing)
}
```

```text
hatchet refuses: field RunQuery.LabelPrefixes, missing LabelPrefix
```

### The label index on the SQL driver

Migration `005_run_labels` adds the table `<prefix>run_labels`, one row per label of a run (each key and value cut to 191 characters), indexed on key, value and run. Enqueue writes the rows in the run's own transaction, and delete and retention remove them, so a label search reads the index instead of decoding every run's labels. Exact values are still checked in Go, so a long value that was cut never matches wrongly. A prefix is matched with `LIKE` on the indexed value, with `%`, `_` and the escape character escaped, so a prefix containing them is taken literally.

Runs written before 005, or by an older client during a rolling deploy, have no index rows until the backfill runs. `Migrate` runs it after the scripts. If you apply the migrations with your own tool (`MigrationScripts`), call `IndexLabels` once after 005 and once more after a rolling deploy in which an older client kept enqueuing; until then a label filter does not see those runs:

<!-- snippet: examples/label-search/main.go#backfill -->
```go
n, err := c.IndexLabels(ctx) // after MigrationScripts applied by your own tool
if err != nil {
	return err
}
again, err := c.IndexLabels(ctx) // safe to repeat: nothing past the watermark
if err != nil {
	return err
}
```

```text
IndexLabels: 4 runs, then 0
```

It works in chunks under a watermark kept in the locks row `labels:indexed`, rewriting the rows of each run past it, so it is safe to run twice and from several processes at once. The first call above rewrote the rows of the four runs enqueued after `Migrate`'s own pass; the second found nothing new.

## Step 2: re-run a failed run from the step that failed

`Retry(ref)` re-opens a failed run. The steps that succeeded keep their outputs and do not run again; the step that failed, and any branch its failure cancelled, start again with fresh attempt counts. A charge that already went through is not charged twice because shipping failed.

<!-- snippet: examples/manage-runs/main.go#retry -->
```go
warehouseDown.Store(false) // the outage is over
ref, err := c.Retry(ctx, failed)
if err != nil {
	return err
}
```

The run keeps its `RunRef` and goes back to `queued`. When it ends again, run-finished observers see a second event for it, which they already tolerate (delivery is at least once). Its idempotency key is taken again; if another unfinished run took it meanwhile, `Retry` returns `workflow.ErrDuplicate`. Only a `failed` run can be retried; any other status returns `workflow.ErrWrongState`.

## Step 3: forget a finished run

`Delete(ref)` forgets a terminal run: afterwards `Status` returns `workflow.ErrNotFound` and `Runs` no longer lists it. An unfinished run returns `ErrWrongState`; cancel it first.

<!-- snippet: examples/manage-runs/main.go#delete -->
```go
err = c.Delete(ctx, ref)
_, statusErr := c.Status(ctx, ref)
fmt.Fprintln(out, "deleted:", err == nil, "then Status is ErrNotFound:", errors.Is(statusErr, workflow.ErrNotFound))
```

To forget many, page through `Runs` and delete each, or use the SQL driver's retention, which prunes finished runs by age.

## Step 4: cancel every waiting run

`CancelAll(query)` cancels every run that matches and returns how many. With no `Statuses` it takes only runs not yet started, `scheduled` and `queued`; running and waiting runs are cancelled only when you list their status. A terminal status, a `Limit` or a `Cursor` is refused, since a bulk cancel covers every match. Each run is cancelled exactly as `Cancel` would: its key released, its run-finished event delivered.

<!-- snippet: examples/manage-runs/main.go#cancel-all -->
```go
n, err := c.CancelAll(ctx, workflow.RunQuery{Workflow: "fulfil-order"}) // runs not yet started
if err != nil {
	return err
}
fmt.Fprintln(out, "cancelled waiting runs:", n)
```

## Error history

On a driver with `CapErrorHistory`, `RunInfo.Errors` keeps every failed attempt of a run, oldest first: the step, the attempt number, the error message, when it failed and, when the error carries one, a stack trace (`workflow.AttemptError`). It records retried and final failures alike, so "it failed three times with 451 and then with something else" is visible without logs. The program is [`examples/run-repair`](../../examples/run-repair):

<!-- snippet: examples/run-repair/main.go#client -->
```go
c := memory.New(
	memory.WithErrorHistory(10), // keep the newest 10 failed attempts per run; 0 turns it off
	memory.WithMiddleware(middleware.Recover(slog.New(slog.DiscardHandler))),
)
```

<!-- snippet: examples/run-repair/main.go#history -->
```go
page, err := c.Runs(ctx, workflow.RunQuery{Ref: ref})
if err != nil {
	return err
}
for _, e := range page.Runs[0].Errors { // oldest first
	fmt.Fprintf(out, "  attempt %d of %q: %s (stack: %t)\n", e.Attempt, e.Step, e.Error, e.Stack != "")
}
```

```text
  attempt 1 of "send": smtp 451: try again later (stack: false)
  attempt 2 of "send": smtp 451: try again later (stack: false)
  attempt 3 of "send": workflow/middleware: step panicked: nil template (stack: true)
```

The history is bounded per run: the driver option (`memory.WithErrorHistory(n)`, and the same name on the other drivers) keeps the newest `n` entries, `workflow.DefaultErrorHistory` (20) when unset; `n <= 0` turns it off. A stack is taken from any error that implements `workflow.StackTracer` (`StackTrace() string`), cut to `workflow.MaxErrorStack` (8 KiB); the error of a panic caught by `middleware.Recover` implements it, and your own error types can too. An attempt put back by a clean shutdown is not a failure and is not recorded ([Deploy workers](../operations/deploy.md#shut-down-cleanly)). A driver that keeps history elsewhere builds entries with `workflow.NewAttemptError` and bounds them with `workflow.AppendAttemptError`.

## Force a run to an end

Sometimes a step cannot finish on its own: the email was sent by hand, or a feed is down for days and the run should stop now. A client that implements `workflow.RunForcer` (`CapForceRun`) ends a run from outside.

`ForceComplete(ref)` records a run as succeeded. It accepts an unfinished run, or a failed one (accepting the failure as done). An attempt in progress has its context cancelled with the cause `workflow.ErrForced` and its result is ignored; remaining steps never run. The run is then treated as a success: its idempotency key is freed, its chain continues, and run-finished observers see `succeeded` (a second event for a run that had failed, which they already tolerate).

<!-- snippet: examples/run-repair/main.go#complete -->
```go
// The email went out by hand: record the run as done.
if err := c.ForceComplete(ctx, ref); err != nil {
	return err
}
```

`ForceFail(ref, reason)` records an unfinished run as failed, with an error that matches `workflow.ErrForced` and ends with your reason. No retry is made and `OnFailure` does not run, because the operator decided, not the workflow; `Retry` can still re-run it later.

<!-- snippet: examples/run-repair/main.go#fail -->
```go
err = c.ForceFail(ctx, stuck, "feed provider is down until Monday")
if err != nil {
	return err
}
```

```text
  import-feed: failed, "workflow: run ended by an operator: feed provider is down until Monday", OnFailure ran 0 times
```

Any other state returns `workflow.ErrWrongState` (a succeeded or cancelled run, or a failed run for `ForceFail`), and an unknown ref `workflow.ErrNotFound`. `workflow.CanForceComplete` and `workflow.CanForceFail` are the rules every driver applies. To move a failed run to another worker, retry it: any worker may pick the retried run up.

## Pause and resume

`Pause` holds runs without cancelling them. The target is one workflow (`workflow.PauseWorkflow`), one queue (`workflow.PauseQueue`) or everything (`workflow.PauseAll`). While a target is paused no step attempt of a run it holds is claimed: new runs and the next step of a running run alike stay `queued`. Attempts already running finish, enqueues are accepted, and timers (delays, sleeps, wait deadlines, backoffs) keep running.

<!-- snippet: examples/manage-runs/main.go#pause -->
```go
if err := c.Pause(ctx, workflow.PauseWorkflow("fulfil-order")); err != nil {
	return err
}
held, err := c.Enqueue(ctx, workflow.Job{Workflow: "fulfil-order", Payload: []byte(`"o-4"`)})
if err != nil {
	return err
}
st, err := c.Status(ctx, held)
if err != nil {
	return err
}
fmt.Fprintln(out, "while paused:", st)
if err := c.Resume(ctx, workflow.PauseWorkflow("fulfil-order")); err != nil {
	return err
}
```

`Resume` lifts exactly the target given, except `Resume(workflow.PauseAll())`, which lifts every pause. `Paused` lists the pauses in force. On the SQL driver a pause is stored, so every node obeys it and it survives a restart.

## List the runs due soon

A run enqueued with `Job.RunAt` waits as `scheduled` until it is due. `RunInfo.RunAt` is the time it is (or was) due, kept after it starts, and `RunQuery.DueFrom` and `RunQuery.DueTo` bound it to `[DueFrom, DueTo)`, which answers "what fires in the next hour?":

<!-- snippet: examples/run-reschedule/main.go#due -->
```go
now := clk.Now()
page, err := c.Runs(ctx, workflow.RunQuery{
	DueFrom: now,
	DueTo:   now.Add(time.Hour), // exclusive
})
if err != nil {
	return err
}
for _, r := range page.Runs {
	fmt.Fprintf(out, "  %s due at +%s, %s\n", r.Labels["name"], r.RunAt.Sub(now), r.Status)
}
```

With either bound set, only runs enqueued with a `RunAt` match; a zero bound is open. `DueFrom` not before `DueTo` fails validation with `ErrInvalid`, and a driver without `CapDueQuery` refuses the query with an `UnsupportedError` rather than ignoring the bounds. The filter combines with the others, so `Statuses: []workflow.Status{workflow.StatusScheduled}` narrows it to runs that have not started yet.

## Reschedule a delayed run

`RunRescheduler.Reschedule(ctx, ref, at)` moves a run that has not started to a new time. It keeps the run's `RunRef`, payload, labels and idempotency key, and `RunInfo.RunAt` reads the new time, so nothing that holds the ref has to change:

<!-- snippet: examples/run-reschedule/main.go#reschedule -->
```go
moved, err := c.Reschedule(ctx, refs["r-2h"], clk.Now().Add(45*time.Minute))
if err != nil {
	return err
}
```

```text
due in the next hour:
  r-30m due at +30m0s, scheduled
rescheduled r-2h to +45m, same ref true
due in the next hour:
  r-2h due at +45m0s, scheduled
  r-30m due at +30m0s, scheduled
```

Only a run in `StatusScheduled` can move. Any other status returns `ErrWrongState`, since a run that has started or ended has no start time left to change, and an unknown ref returns `ErrNotFound`. An `at` in the past, or zero, means now. On a client without the interface, the fallback is `Cancel` plus a new `Enqueue` with the new `RunAt`, which gives the run a new ref.

## Cancel one step

`Cancel` stops a whole run. Sometimes only one step is the problem: a price feed hangs while the rest of the run could carry on with yesterday's prices. `StepCanceller.CancelStep(ctx, ref, step)` withdraws that step and lets the run decide what happens next:

<!-- snippet: examples/run-reschedule/main.go#cancel-step -->
```go
if err := c.CancelStep(ctx, ref, "fetch"); err != nil {
	return err
}
```

For a step with an attempt running, the attempt's `ctx` is cancelled with the cause `workflow.ErrStepCancelled` (read it with `context.Cause`), and the handler's return decides:

- **It returns an output:** the step succeeded, and the run continues with that output. This is how a step offers a fallback when it is interrupted:

<!-- snippet: examples/run-reschedule/main.go#fallback -->
```go
fetchPrices := func(ctx context.Context, _ []byte) ([]byte, error) {
	started <- "fetch"
	<-ctx.Done() // the price feed is hanging
	if errors.Is(context.Cause(ctx), workflow.ErrStepCancelled) {
		return []byte(`"yesterday's prices"`), nil // an operator withdrew the step: fall back
	}
	return nil, context.Cause(ctx)
}
```

- **It returns an error:** the step fails for good, without retries, with an error matching `ErrStepCancelled`; the run fails as any failed run does, and `OnFailure` runs.

A step with no attempt running but not done (waiting out a retry backoff, snoozed, or a wait or sleep step that is suspended) fails for good at once, with an error matching `ErrStepCancelled`. A step that has not started, or has ended, returns `ErrWrongState`; an unknown run or step name `ErrNotFound`.

```text
cancel one step:
  store got "yesterday's prices"
  update-prices succeeded
  sync-feed failed, ErrStepCancelled true
  cancel it again: ErrWrongState true
```

## Try it

```sh
cd examples
go run ./manage-runs
```

```text
failed: run-2 at step "ship": memory: step "ship" failed after 1 attempt(s): warehouse API: 503
retried run-2: succeeded; charge ran 1 time(s), ship 2
deleted: true then Status is ErrNotFound: true
cancelled waiting runs: 2
while paused: queued
after Resume: succeeded
```

## Pitfalls

- **A label search on the SQL driver misses runs that were never indexed.** Runs written before migration 005, or by a client older than it, are found only after `IndexLabels` has run. `Migrate` runs it; a team applying `MigrationScripts` with another tool must call it.
- **Hatchet supports most of it.** It declares `CapRunQuery`, `CapCancelAll` and `CapRetryRun`. Retry is Hatchet's replay of the failed and cancelled step tasks, which keeps the steps that succeeded (checked live). It does not declare `CapDeleteRun`, because the engine has no run delete. Retry does not take the idempotency key again on the engine: it refuses with `ErrDuplicate` when another unfinished run holds the key, but an `Enqueue` with that key while the retried run is unfinished starts a second run. Pause needs `hatchet.Config.API` (see [the Hatchet driver](../drivers/hatchet.md#step-6-pause-a-workflow-or-a-schedule)): with it the driver declares `CapPauseWorkflow`. `PauseAll()` and `PauseQueue` are refused, and `Resume(PauseAll())` clears every paused workflow of the namespace.
- **Retry runs your code again.** The failed step runs a second time with the same input. Make it idempotent, as every retried step must be.
- **`Error` is a string.** An engine stores only the message, so `RunInfo` cannot give you the original error value to match with `errors.Is`; use the run-finished hook on an in-process driver when you need that.
- **The memory driver forgets old finished runs.** By default it keeps finished runs for a day and at most 10,000 of them (`memory.DefaultRunRetention`), so a long-lived process stays bounded; after that `Status`, `Runs` and `Retry` no longer find them. `memory.WithRunRetention` changes the limits, and `RunRetention{}` keeps everything until `Delete`. Unfinished runs are never forgotten.
- **Force complete records an end, it does not do the work.** Steps that did not run never run, and an attempt cancelled midway may have done part of its side effect. Check what happened before you force a run.
- **A step must return when its context is cancelled for a force to stop it.** Go cannot kill a goroutine: a step that ignores `ctx` keeps running after the run is already recorded as ended, and its result is thrown away.
- **Error history grows each run's record.** Twenty entries with 8 KiB stacks are up to about 160 KiB per run on a durable driver; lower `WithErrorHistory` for workflows that retry many times.
- **`CancelStep` only asks.** Like a run cancel, it cancels the attempt's `ctx`; a handler that ignores it runs on, and its output, if it returns one, still counts as the step's.
- **A fallback output looks like success to everything after it.** The run ends `succeeded`; if downstream code must know the data is stale, put that in the output.
- **`DueFrom` and `DueTo` skip runs with no `RunAt`.** A run enqueued to start now is never "due" in this sense, even while it waits for a worker.
- **A paused workflow still accepts enqueues.** A deploy that pauses before a migration and forgets to resume leaves runs piling up silently; alert on `Paused` being non-empty.

## Related

- [Named queues](queues.md): routing runs and pausing one queue.
- [Cancel and status](cancel-and-status.md): one run at a time.
- [React when a run finishes](../observability/run-finished.md): the run-finished hook.
- [Errors](../concepts/errors.md): `ErrWrongState`, `ErrDuplicate`, `ErrNotFound`.
- [Run a job later](delays.md): `Job.RunAt`.
- Example: [examples/run-reschedule](../../examples/run-reschedule/) (due-time filters, reschedule, cancel one step).
