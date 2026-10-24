# Run states

A run's status answers "where is it now?". Two moments need more than queued, running, failed and cancelled: a run whose step failed and is waiting out its backoff before the next attempt, and a run a step dropped on purpose because its work no longer made sense. On a driver with `workflow.CapRunStates` these have their own statuses, `StatusRetrying` and `StatusDiscarded`, so a dashboard, an alert or a batch can tell them apart from work waiting for a worker and from real failures. This page covers both, the whole state machine and the code that exposes it, how to query the states, how batches count discarded jobs, and what changes for code that switches on statuses.

## Before you start

- `workflow.CapRunStates` is declared by the memory, SQL and Redis drivers. Hatchet does not declare it: its engine has no such states, so a run in backoff reads `queued` there, and since Hatchet lacks `CapDiscard` too, `Discard` acts as `Permanent` and the run reads `failed` (see [Pitfalls](#pitfalls)).
- The full status list is in [Cancel and status](cancel-and-status.md#read-a-runs-status).
- The example is [`examples/run-states`](../../examples/run-states). Run it with `go run ./run-states` from `examples/`.

## Retrying

`StatusRetrying` means no step attempt is executing and at least one step attempt failed and is waiting on its retry backoff. It is not terminal: the run goes on when the backoff ends. When a run has several things going on at once, the status follows one order: any attempt executing makes it `running`, else any step in backoff makes it `retrying`, else any attempt waiting for a worker makes it `queued`, else `waiting`.

While a run is retrying, `RunInfo` from the run listing says what comes next: `NextAttempt` is the attempt number the step will run as (2 for the first retry) and `NextAttemptAt` when its backoff ends. With several steps in backoff, they describe the one ready first. Both are zero in every other status. `RunInfo.Error` holds the error of the failed attempt that waits, so a dashboard can show why the run is retrying.

<!-- snippet: examples/run-states/main.go#retrying -->
```go
page, err := c.Runs(ctx, workflow.RunQuery{Statuses: []workflow.Status{workflow.StatusRetrying}})
if err != nil {
	return err
}
for _, info := range page.Runs {
	fmt.Fprintf(out, "%s: %s, attempt %d at %s, last error %q\n",
		info.Workflow, info.Status, info.NextAttempt, info.NextAttemptAt.Sub(start), info.Error)
}
```

```text
sync-invoices: retrying, attempt 2 at 1m0s, last error "upstream 503"
after the backoff: succeeded
```

`RunQuery.Statuses` accepts both new statuses, in `Runs`, in the admin module and in the admin JSON API. A bulk cancel with no statuses still cancels only runs that have not started (`scheduled` and `queued`); a retrying run has started, so `CancelAll` cancels it only when `StatusRetrying` is listed.

## Discarded

`StatusDiscarded` is terminal: a step returned `workflow.Discard(err)` because the work no longer makes sense (the order was deleted, the user unsubscribed). The step is not retried and `OnFailure` does not run. The run's `Err` keeps the reason and matches both `workflow.ErrDiscarded` and `workflow.ErrCancelled`, so code written before this status still recognises it:

<!-- snippet: examples/run-states/main.go#discarded -->
```go
if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "ship-order"}); err != nil {
	return err
}
ev = <-ends
fmt.Fprintf(out, "%s: %s, terminal %v, ErrDiscarded %v, ErrCancelled %v\n", ev.Workflow, ev.Status,
	ev.Status.IsTerminal(), errors.Is(ev.Err, workflow.ErrDiscarded), errors.Is(ev.Err, workflow.ErrCancelled))
```

```text
ship-order: discarded, terminal true, ErrDiscarded true, ErrCancelled true
admin discarded: ship-order, "workflow: run cancelled: workflow: run discarded: step \"ship\": order o-9 no longer exists"
call-partner with MaxErrors 2: failed
```

Two things end a run discarded. An explicit `Discard` does, as above. So does a retry policy that opted in with `RetryPolicy.DiscardOnExhaust`: when it stops retrying a failed attempt (attempts ran out, `MaxErrors` or `Until` stopped it, `NoRetryOn` excluded the error, or the handler returned `Permanent`), the run ends discarded instead of failed, with the same `Err` and no `OnFailure`. Without that opt-in, which is the default, a run that runs out of retries because of `Max`, `MaxErrors` or `Until` ends `failed`: giving up is a failure an operator should see, in the failed-run list, in `RunRetrier.Retry` and in a batch's `Catch`. Pick the opt-in per policy, for work whose giving up is routine; see [Retries](retries.md#quiet-giving-up-discardonexhaust) and [`examples/discard-exhausted`](../../examples/discard-exhausted).

The admin module lists discarded runs with `Discarded` (and the JSON API at `GET /discarded`), apart from `Failures`, so they stay out of the failure view without being lost:

<!-- snippet: examples/run-states/main.go#admin -->
```go
ops := admin.New(c)
dropped, err := ops.Discarded(ctx, workflow.RunQuery{}) // apart from ops.Failures
if err != nil {
	return err
}
for _, info := range dropped.Runs {
	fmt.Fprintf(out, "admin discarded: %s, %q\n", info.Workflow, info.Error)
}
```

Run-finished observers receive `StatusDiscarded` in `RunEvent.Status`. `RunRetrier.Retry` retries only failed runs, so a discarded run returns `workflow.ErrWrongState`; enqueue a new job if the work makes sense again.

`workflow.EndStatus(caps, err)` is the one mapping every driver uses for a step error that ends a run: a discard is `StatusDiscarded` with `CapRunStates`, `StatusCancelled` with `CapDiscard` alone, and `StatusFailed` otherwise; every other error is `StatusFailed`. Write a driver with it so your driver agrees with the built-in ones.

## The state machine

A run is created `scheduled` (a `Job.RunAt` in the future, `CapDelayedStart`) or `queued`, moves among four active statuses while its steps run, and ends in exactly one terminal status. The moves, what causes each and the capability that gates it:

| From | To | What causes it | Needs |
|---|---|---|---|
| `scheduled` | `queued` | The start time arrives | `CapDelayedStart` |
| `queued` | `running` | A worker claims a ready step | any driver |
| `running` | `queued` | An attempt is handed back (graceful stop, dead worker, lost lease), or the next step is ready | any driver |
| `running` | `waiting` | A wait or sleep step parks, or a step returns `Snooze` | `CapWaitForEvent`, `CapDurableSleep`, `CapSnooze` |
| `waiting` | `queued` or `running` | The event arrives or the sleep ends | any driver |
| `running` | `retrying` | An attempt failed and the policy allows another; `RunInfo.NextAttemptAt` says when | `CapRunStates` |
| `retrying` | `running` or `queued` | The backoff elapsed and a worker claimed the step, or is about to | `CapRunStates` |
| `running` | `succeeded` | Every step succeeded | any driver |
| `running` | `failed` | The policy stopped retrying (`Max`, `MaxErrors`, `Until`, `Permanent`, `NoRetryOn`) and `DiscardOnExhaust` is off | any driver |
| `running` | `discarded` | A step returned `Discard` | `CapDiscard` and `CapRunStates` |
| `running` | `discarded` | The same stop as above with `DiscardOnExhaust` on | `CapDiscardExhausted` and `CapRunStates` |
| any non-terminal | `cancelled` | `Cancel` or `CancelAll`; also a `Discard` on a driver with `CapDiscard` but not `CapRunStates` | `CapCancel` |
| any non-terminal | `failed` | The run went past `Workflow.RunTimeout`, or `ForceFail` | `CapRunTimeout`, `CapForceRun` |
| any non-terminal | `succeeded` | `ForceComplete` | `CapForceRun` |
| `failed` | `queued` | `Retry` re-opens the run from its failed step | `CapRetryRun` |

`failed` to `queued` is the only move out of a terminal status. A `discarded` or `cancelled` run is not re-opened: enqueue a new job if the work makes sense again. With parallel steps a run can move between any two active statuses as its steps progress (one step retrying while another waits), so the code also lists every active-to-active pair. The precedence when several things are true at once is the one under [Retrying](#retrying).

Failed and discarded differ in what happens next. Failed is a failure an operator must see, so `OnFailure` runs and a batch's `Catch` fires. Discarded is a run dropped on purpose or given up on by policy: `OnFailure` does not run, the chain does not continue, the idempotency key is released, and `Err` matches `ErrDiscarded` and `ErrCancelled`.

The table is data you can read and check against, not only prose. `workflow.Transitions()` returns every legal move as a `workflow.Transition{From, To, Trigger, Needs}`; `status.TransitionsFrom()` lists the moves that leave one status, and `status.CanTransitionTo(to)` asks whether a move exists at all (it ignores capabilities; read `Needs` for those). A driver can check a status change it is about to store against it, and a test can walk it. The same machine, in prose, sits next to the `Status` type in `status.go`.

## Batches and discarded jobs

A batch counts a discarded job as failed by default: `Catch` runs, `AllowFailures` decides whether the rest are cancelled, and `RetryFailed` picks it up. `Batch.DiscardedAs` changes that per batch to `StatusCancelled` or `StatusSucceeded`, for a batch where a dropped item is expected and fine. `StatusCancelled` is how batches counted a discarded job before this status existed: no `Catch`, but no `Then` either. The choice is saved with the batch, so a durable store keeps it across restarts. `Progress.Discarded` counts discarded jobs on their own as well (they are also in the bucket they were counted as), and `JobState.Status` keeps the raw `discarded`.

<!-- snippet: examples/run-states/main.go#batch -->
```go
orders := []workflow.Job{{Workflow: "pack-order"}, {Workflow: "ship-order"}}
for _, discardedAs := range []workflow.Status{"", workflow.StatusSucceeded} {
	id, err := m.Dispatch(ctx, batch.Batch{
		Jobs:          orders,
		Then:          "batch-then",
		Catch:         "batch-catch",
		AllowFailures: true,        // let pack-order finish after ship-order is dropped
		DiscardedAs:   discardedAs, // zero: a discarded job counts as failed
	})
	if err != nil {
		return err
	}
	ran := <-callbacks
	p, err := finished(ctx, m, id)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "batch with DiscardedAs %q: %s ran; succeeded %d, failed %d, discarded %d\n",
		discardedAs, ran, p.Succeeded, p.Failed, p.Discarded)
}
```

```text
batch with DiscardedAs "": batch-catch ran; succeeded 1, failed 1, discarded 1
batch with DiscardedAs "succeeded": batch-then ran; succeeded 2, failed 0, discarded 1
```

The first batch counts the dropped `ship-order` as failed, so `Catch` runs; the second counts it as succeeded, so `Then` runs. `AllowFailures` lets `pack-order` finish in the first batch instead of being cancelled by the failure.

## Code that switches on statuses

The two new statuses are a breaking change for code that switches on `workflow.Status`. On the memory, SQL and Redis drivers, a discarded run now reads `discarded` (it was `cancelled`) and a run in backoff reads `retrying` (it was `queued`). Keep a default branch, and decide what is finished with `Status.IsTerminal()`, which covers `discarded`:

<!-- snippet: examples/run-states/main.go#switch -->
```go
describe := func(s workflow.Status) string {
	switch s {
	case workflow.StatusRetrying:
		return "waiting to retry"
	case workflow.StatusDiscarded:
		return "dropped on purpose"
	case workflow.StatusFailed:
		return "needs a look"
	default: // keep one: drivers may report statuses added later
		if s.IsTerminal() {
			return "done"
		}
		return "in progress"
	}
}
```

`Status.Valid()` reports whether a string is one of the known statuses, for code that reads a status from a database column or a query string.

## Pitfalls

- **Hatchet reports neither state.** Code that must work on every driver checks `Capabilities().Has(workflow.CapRunStates)` before relying on `retrying` or `discarded`. On Hatchet a `RunQuery` or `CancelAll` filter naming either status is refused with an `UnsupportedError` (field `RunQuery.Statuses`) rather than answered with an empty page. A driver with `CapDiscard` but not `CapRunStates` reports a discard as `cancelled` with an `Err` matching `ErrDiscarded`.
- **A failover client reports them only when both backends do.** Its `CapRunStates` is the intersection of the two.
- **`Retrying` is not stuck.** A run with a long backoff sits in `retrying` for that long. Alert on `NextAttemptAt` being far away, not on the status alone.
- **Discard hides the run from failure views.** That is its purpose, and `DiscardOnExhaust` does the same for every run its policy gives up on. When in doubt return `Permanent(err)` under a policy without the option, which fails the run where an operator sees it.
- **`DiscardOnExhaust` needs both `CapDiscardExhausted` and `CapRunStates`.** Hatchet declares neither and refuses the option at `Register`, so a run out of retries is `failed` there.

## Related

- [Retries](retries.md): backoff, `Discard`, `DiscardOnExhaust`, `MaxErrors` and `Until`.
- [Cancel and status](cancel-and-status.md): every status and how to read it.
- [Manage runs](manage-runs.md): listing and retrying runs.
- [Batch lifecycle](../batches/lifecycle.md): how a batch reacts to its jobs' ends.
