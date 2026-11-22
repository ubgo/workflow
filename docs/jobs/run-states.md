# Run states

A run's status answers "where is it now?". Two moments need more than queued, running, failed and cancelled: a run whose step failed and is waiting out its backoff before the next attempt, and a run a step dropped on purpose because its work no longer made sense. On a driver with `workflow.CapRunStates` these have their own statuses, [`StatusRetrying`](ds:block?id=statusretrying-x8eqka84) and [`StatusDiscarded`](ds:block?id=statusdiscarded-gm3h9fxg), so a dashboard, an alert or a batch can tell them apart from work waiting for a worker and from real failures. This page covers both, the whole state machine and the code that exposes it, how to query the states, how batches count discarded jobs, and what changes for code that switches on statuses.

## Before you start

- `workflow.CapRunStates` is declared by the [memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-batch2caps-9rm4b4ck) and [Redis](ds:block?id=redis-caps-bfgdst9t) drivers. [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z) does not declare it: its engine has no such states, so a run in backoff reads `queued` there, and since Hatchet lacks `CapDiscard` too, `Discard` acts as `Permanent` and the run reads `failed` ([`EndStatus`](ds:block?id=endstatus-d3uc8s8n); see [Pitfalls](#pitfalls)).
- The full status list is in [Cancel and status](cancel-and-status.md#read-a-runs-status).
- The examples are [`examples/run-states`](../../examples/run-states) (retrying, discarded, batches and the admin view, on the memory driver) and [`examples/run-transitions`](../../examples/run-transitions) (the state machine as data). Run them with `go run ./run-states` and `go run ./run-transitions` from `examples/`.

## Retrying

[`StatusRetrying`](ds:block?id=statusretrying-x8eqka84) means no step attempt is executing and at least one step attempt failed and is waiting on its retry backoff. It is not terminal ([`Status.IsTerminal`](ds:block?id=status-isterminal-tfxgce9u)): the run goes on when the backoff ends. When a run has several things going on at once, the status follows one order: any attempt executing makes it `running`, else any step in backoff makes it `retrying`, else any attempt waiting for a worker makes it `queued`, else `waiting` (the memory driver's [`statusLocked`](ds:block?id=memory-run-statuslocked-7dr9vd6u), the SQL driver's [`stepStatus`](ds:block?id=sql-stepstatus-3wp3p5q4)).

While a run is retrying, [`RunInfo`](ds:block?id=runinfo-f6rpt8qe) from the run listing says what comes next: `NextAttempt` is the attempt number the step will run as (2 for the first retry) and `NextAttemptAt` when its backoff ends. With several steps in backoff, they describe the one ready first. Both are zero in every other status. `RunInfo.Error` holds the error of the failed attempt that waits, so a dashboard can show why the run is retrying.

<!-- snippet: examples/run-states/main.go#retrying -->
```go
page, err := c.Runs(ctx, workflow.RunQuery{Statuses: []workflow.Status{workflow.StatusRetrying}})
if err != nil {
	return err
}
for _, info := range page.Runs {
	fmt.Printf("%s: %s, attempt %d at %s, last error %q\n",
		info.Workflow, info.Status, info.NextAttempt, info.NextAttemptAt.Sub(start), info.Error)
}
```

```text
sync-invoices: retrying, attempt 2 at 1m0s, last error "upstream 503"
after the backoff: succeeded
```

`RunQuery.Statuses` accepts both new statuses ([`ValidateRunQuery`](ds:block?id=validaterunquery-bxy4ha24) checks them with [`Status.valid`](ds:block?id=status-valid-hgp8uvf4)), in `Runs`, in the admin module and in the admin JSON API. A bulk cancel with no statuses still cancels only runs that have not started (`scheduled` and `queued`); a retrying run has started, so `CancelAll` cancels it only when `StatusRetrying` is listed ([`CancelStatuses`](ds:block?id=cancelstatuses-8fnq9hyv)).

## Discarded

[`StatusDiscarded`](ds:block?id=statusdiscarded-gm3h9fxg) is terminal: a step returned [`workflow.Discard(err)`](ds:block?id=discard-ttxam8w8) because the work no longer makes sense (the order was deleted, the user unsubscribed). The step is not retried and `OnFailure` does not run. The run's `Err` keeps the reason and matches both `workflow.ErrDiscarded` and `workflow.ErrCancelled` ([`DiscardedError`](ds:block?id=discardederror-ebygkc2v)), so code written before this status still recognises it:

<!-- snippet: examples/run-states/main.go#discarded -->
```go
if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "ship-order"}); err != nil {
	return err
}
ev = <-ends
fmt.Printf("%s: %s, terminal %v, ErrDiscarded %v, ErrCancelled %v\n", ev.Workflow, ev.Status,
	ev.Status.IsTerminal(), errors.Is(ev.Err, workflow.ErrDiscarded), errors.Is(ev.Err, workflow.ErrCancelled))
```

```text
ship-order: discarded, terminal true, ErrDiscarded true, ErrCancelled true
admin discarded: ship-order, "workflow: run cancelled: workflow: run discarded: step \"ship\": order o-9 no longer exists"
call-partner with MaxErrors 2: failed
```

Two things end a run discarded. An explicit `Discard` does, as above. So does a retry policy that opted in with [`RetryPolicy.DiscardOnExhaust`](ds:block?id=retrypolicy-exhaustederror-uwdsjnja): when it stops retrying a failed attempt (attempts ran out, `MaxErrors` or `Until` stopped it, `NoRetryOn` excluded the error, or the handler returned `Permanent`), the run ends discarded instead of failed, with the same `Err` and no `OnFailure`. Without that opt-in, which is the default, a run that runs out of retries because of `Max`, `MaxErrors` or `Until` ends `failed`: giving up is a failure an operator should see, in the failed-run list, in `RunRetrier.Retry` and in a batch's `Catch`. Pick the opt-in per policy, for work whose giving up is routine; see [Retries](retries.md#quiet-giving-up-discardonexhaust) and [`examples/discard-exhausted`](../../examples/discard-exhausted).

The admin module lists discarded runs with [`Discarded`](ds:block?id=admin-admin-discarded-dy868kay) (and the JSON API at `GET /discarded`), apart from `Failures`, so they stay out of the failure view without being lost:

<!-- snippet: examples/run-states/main.go#admin -->
```go
ops := admin.New(c)
dropped, err := ops.Discarded(ctx, workflow.RunQuery{}) // apart from ops.Failures
if err != nil {
	return err
}
for _, info := range dropped.Runs {
	fmt.Printf("admin discarded: %s, %q\n", info.Workflow, info.Error)
}
```

Run-finished observers receive `StatusDiscarded` in `RunEvent.Status`. [`RunRetrier.Retry`](ds:block?id=runretrier-esdpff54) retries only failed runs, so a discarded run returns `workflow.ErrWrongState`; enqueue a new job if the work makes sense again.

[`workflow.EndStatus(caps, err)`](ds:block?id=endstatus-d3uc8s8n) is the one mapping every driver uses for a step error that ends a run: a discard is `StatusDiscarded` with `CapRunStates`, `StatusCancelled` with `CapDiscard` alone, and `StatusFailed` otherwise; every other error is `StatusFailed`. Write a driver with it so your driver agrees with the built-in ones.

## The state machine

A run is created `scheduled` (a `Job.RunAt` in the future, `CapDelayedStart`) or `queued`, moves among four active statuses while its steps run, and ends in exactly one terminal status ([`Status.IsTerminal`](ds:block?id=status-isterminal-tfxgce9u)). The moves, what causes each and the capability that gates it ([`buildTransitions`](ds:block?id=buildtransitions-vreab6mw)):

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

`failed` to `queued` is the only move out of a terminal status ([`TestTransitionTableShape`](ds:block?id=testtransitiontableshape-k65vuntn)). A `discarded` or `cancelled` run is not re-opened: enqueue a new job if the work makes sense again. With parallel steps a run can move between any two active statuses as its steps progress (one step retrying while another waits), so the code also lists every active-to-active pair ([`buildTransitions`](ds:block?id=buildtransitions-vreab6mw), gated by [`reportCaps`](ds:block?id=reportcaps-jxeguskt)). The precedence when several things are true at once is the one under [Retrying](#retrying).

Failed and discarded differ in what happens next. Failed is a failure an operator must see, so `OnFailure` runs and a batch's `Catch` fires. Discarded is a run dropped on purpose or given up on by policy: `OnFailure` does not run, the chain does not continue, the idempotency key is released (the run is terminal, [`Status.IsTerminal`](ds:block?id=status-isterminal-tfxgce9u)), and `Err` matches `ErrDiscarded` and `ErrCancelled` ([`DiscardedError`](ds:block?id=discardederror-ebygkc2v)).

### Read the machine in code

The table is data you can read and check against, not only prose. [`workflow.Transitions()`](ds:block?id=transitions-pxgpmppa) returns every legal move as a [`workflow.Transition{From, To, Trigger, Needs}`](ds:block?id=transition-cymazc4a), and `Needs` is the set of capabilities a driver must declare to report the move (zero means any driver). [`status.TransitionsFrom()`](ds:block?id=status-transitionsfrom-h9u3qtwy) lists the moves that leave one status:

<!-- snippet: examples/run-transitions/main.go#from -->
```go
for _, t := range workflow.StatusRetrying.TransitionsFrom() {
	fmt.Fprintf(w, "%s -> %s: %s (needs %s)\n", t.From, t.To, t.Trigger, t.Needs)
}
```

```text
retrying -> running: the backoff elapsed and a worker claimed the step (needs RunStates)
retrying -> queued: the backoff elapsed (needs RunStates)
retrying -> waiting: parallel steps changed which status describes the run (needs RunStates)
retrying -> cancelled: Cancel or CancelAll (needs Cancel)
retrying -> failed: the run went past Workflow.RunTimeout (needs RunTimeout)
retrying -> failed: ForceFail (needs ForceRun)
retrying -> succeeded: ForceComplete (needs ForceRun)
```

The same pair of statuses can appear more than once with different triggers (`failed` above is reached by a run timeout and by `ForceFail`; [`buildTransitions`](ds:block?id=buildtransitions-vreab6mw)), so look for the move you mean, not only the target. A terminal status other than `failed` has no moves at all ([`TestTransitionTableShape`](ds:block?id=testtransitiontableshape-k65vuntn)).

[`status.CanTransitionTo(to)`](ds:block?id=status-cantransitionto-3we4jpx7) asks whether a move exists at all. It ignores capabilities, so it answers for the machine, not for one driver:

<!-- snippet: examples/run-transitions/main.go#can -->
```go
for _, move := range [][2]workflow.Status{ // each pair is a from -> to move
	{workflow.StatusFailed, workflow.StatusQueued},       // Retry re-opens a failed run
	{workflow.StatusDiscarded, workflow.StatusQueued},    // a discarded run is never re-opened
	{workflow.StatusScheduled, workflow.StatusSucceeded}, // ForceComplete
	{workflow.StatusSucceeded, workflow.StatusFailed},    // terminal: no way out
} {
	fmt.Fprintf(w, "%s -> %s allowed: %v\n", move[0], move[1], move[0].CanTransitionTo(move[1]))
}
```

```text
failed -> queued allowed: true
discarded -> queued allowed: false
scheduled -> succeeded allowed: true
succeeded -> failed allowed: false
```

To ask what one driver can report, keep the moves whose `Needs` its capabilities cover. A driver without `CapRunStates` never reports `retrying` or `discarded`, so eight moves drop out (every move [`buildTransitions`](ds:block?id=buildtransitions-vreab6mw) gates on it):

<!-- snippet: examples/run-transitions/main.go#caps -->
```go
// reportable counts the moves in the machine whose Needs caps covers.
reportable := func(caps workflow.Caps) (n int) {
	for _, t := range workflow.Transitions() {
		if caps.Has(t.Needs) {
			n++
		}
	}
	return n
}
caps := memory.New().Capabilities()
noRunStates := caps.Without(workflow.CapRunStates) // as on a driver without retrying and discarded
fmt.Fprintf(w, "the machine has %d moves; memory reports %d, without CapRunStates %d\n",
	len(workflow.Transitions()), reportable(caps), reportable(noRunStates))
```

```text
the machine has 40 moves; memory reports 40, without CapRunStates 32
```

### Guard a status change in a driver of your own

A driver you write can check every status change against the machine before it stores it, so a bug in its own bookkeeping shows up as an error instead of a run in an impossible state. The guard needs both answers: the move must exist ([`CanTransitionTo`](ds:block?id=status-cantransitionto-3we4jpx7)), and one of its transitions must be covered by the driver's capabilities ([`TransitionsFrom`](ds:block?id=status-transitionsfrom-h9u3qtwy)):

<!-- snippet: examples/run-transitions/main.go#guard -->
```go
// errIllegalMove is what checkMove wraps, so a caller tests for it with
// errors.Is instead of matching the message.
var errIllegalMove = errors.New("illegal status change")

// checkMove is the guard a driver of your own runs before it stores a
// status change: the move must be in the machine, and the driver must
// declare every capability one of the matching transitions needs.
func checkMove(caps workflow.Caps, from, to workflow.Status) error {
	if !from.CanTransitionTo(to) {
		return fmt.Errorf("%w: no move from %s to %s", errIllegalMove, from, to)
	}
	for _, t := range from.TransitionsFrom() {
		if t.To == to && caps.Has(t.Needs) {
			return nil
		}
	}
	return fmt.Errorf("%w: %s to %s needs capabilities this driver lacks", errIllegalMove, from, to)
}
```

<!-- snippet: examples/run-transitions/main.go#check -->
```go
fmt.Fprintln(w, "memory, running -> retrying:", checkMove(caps, workflow.StatusRunning, workflow.StatusRetrying))
fmt.Fprintln(w, "no CapRunStates, running -> retrying:", checkMove(noRunStates, workflow.StatusRunning, workflow.StatusRetrying))
fmt.Fprintln(w, "memory, cancelled -> queued:", checkMove(caps, workflow.StatusCancelled, workflow.StatusQueued))
```

```text
memory, running -> retrying: <nil>
no CapRunStates, running -> retrying: illegal status change: running to retrying needs capabilities this driver lacks
memory, cancelled -> queued: illegal status change: no move from cancelled to queued
```

The library's own tests walk the table the same way (`status_transitions_test.go`): every pair `CanTransitionTo` accepts is in `Transitions()` and no move starts and ends on the same status ([`TestTransitionTableWalk`](ds:block?id=testtransitiontablewalk-kcd7sx3a)), and only `failed` leaves a terminal status ([`TestTransitionTableShape`](ds:block?id=testtransitiontableshape-k65vuntn)). The same machine, in prose, sits next to the `Status` type in `status.go`.

## Batches and discarded jobs

A batch counts a discarded job as failed by default ([`counted`](ds:block?id=batch-state-counted-ezrpnwzb)): `Catch` runs, `AllowFailures` decides whether the rest are cancelled, and [`RetryFailed`](ds:block?id=batch-manager-retryfailed-6h3uccfy) picks it up. [`Batch.DiscardedAs`](ds:block?id=batch-batch-sbryq3mb) changes that per batch to `StatusCancelled` or `StatusSucceeded`, for a batch where a dropped item is expected and fine. `StatusCancelled` is how batches counted a discarded job before this status existed: no `Catch`, but no `Then` either. The choice is saved with the batch, so a durable store keeps it across restarts. [`Progress.Discarded`](ds:block?id=batch-progress-539fkcc8) counts discarded jobs on their own as well (they are also in the bucket they were counted as), and `JobState.Status` keeps the raw `discarded`.

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
	fmt.Printf("batch with DiscardedAs %q: %s ran; succeeded %d, failed %d, discarded %d\n",
		discardedAs, ran, p.Succeeded, p.Failed, p.Discarded)
}
```

```text
batch with DiscardedAs "": batch-catch ran; succeeded 1, failed 1, discarded 1
batch with DiscardedAs "succeeded": batch-then ran; succeeded 2, failed 0, discarded 1
```

The first batch counts the dropped `ship-order` as failed, so `Catch` runs; the second counts it as succeeded, so `Then` runs. `AllowFailures` lets `pack-order` finish in the first batch instead of being cancelled by the failure.

## Code that switches on statuses

The two new statuses are a breaking change for code that switches on `workflow.Status`. On the [memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-batch2caps-9rm4b4ck) and [Redis](ds:block?id=redis-caps-bfgdst9t) drivers, a discarded run now reads `discarded` (it was `cancelled`) and a run in backoff reads `retrying` (it was `queued`). Keep a default branch, and decide what is finished with [`Status.IsTerminal()`](ds:block?id=status-isterminal-tfxgce9u), which covers `discarded`:

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

[`Status.Valid()`](ds:block?id=status-valid-xt5e9vdc) reports whether a string is one of the known statuses, for code that reads a status from a database column or a query string.

## Try it

```sh
cd examples
go run ./run-states
```

```text
memory declares CapRunStates: true
sync-invoices: retrying, attempt 2 at 1m0s, last error "upstream 503"
after the backoff: succeeded
ship-order: discarded, terminal true, ErrDiscarded true, ErrCancelled true
admin discarded: ship-order, "workflow: run cancelled: workflow: run discarded: step \"ship\": order o-9 no longer exists"
call-partner with MaxErrors 2: failed
batch with DiscardedAs "": batch-catch ran; succeeded 1, failed 1, discarded 1
batch with DiscardedAs "succeeded": batch-then ran; succeeded 2, failed 0, discarded 1
  retrying: waiting to retry
  discarded: dropped on purpose
  failed: needs a look
  succeeded: done
```

The first line checks the capability the rest needs. The run in backoff reads `retrying` with its next attempt one minute after the start and then succeeds; `ship-order` ends `discarded` and still matches `ErrCancelled`; the admin lists it apart from failures; `call-partner` runs out of errors under `MaxErrors` and ends `failed` because its policy has no `DiscardOnExhaust`; the two batches count the dropped job as failed and then as succeeded; the last four lines are the status switch with its default branch.

`go run ./run-transitions` prints the state machine lines shown under [Read the machine in code](#read-the-machine-in-code).

## Pitfalls

- **Hatchet reports neither state.** Code that must work on every driver checks `Capabilities().Has(workflow.CapRunStates)` before relying on `retrying` or `discarded`. On Hatchet a `RunQuery` or `CancelAll` filter naming either status is refused with an `UnsupportedError` (field `RunQuery.Statuses`, [`checkRunStates`](ds:block?id=hatchet-checkrunstates-d62jh9cp)) rather than answered with an empty page. A driver with `CapDiscard` but not `CapRunStates` reports a discard as `cancelled` with an `Err` matching `ErrDiscarded` ([`EndStatus`](ds:block?id=endstatus-d3uc8s8n)). See [Hatchet limitations](../drivers/hatchet.md#limitations).
- **A failover client reports them only when both backends do.** Its `CapRunStates` is the intersection of the two ([`Capabilities`](ds:block?id=failover-client-capabilities-cb8n9pqm)).
- **`Retrying` is not stuck.** A run with a long backoff sits in `retrying` for that long. Alert on [`NextAttemptAt`](ds:block?id=runinfo-f6rpt8qe) being far away, not on the status alone.
- **Discard hides the run from failure views.** That is its purpose ([`Discarded`](ds:block?id=admin-admin-discarded-dy868kay) lists them apart), and [`DiscardOnExhaust`](ds:block?id=retrypolicy-exhaustederror-uwdsjnja) does the same for every run its policy gives up on. When in doubt return `Permanent(err)` under a policy without the option, which fails the run where an operator sees it.
- **`CanTransitionTo` ignores capabilities.** It answers for the machine ([`CanTransitionTo`](ds:block?id=status-cantransitionto-3we4jpx7)). A driver that guards its own status changes must also check `Needs` from `TransitionsFrom`, or it would accept `retrying` without `CapRunStates`.
- **`DiscardOnExhaust` needs both `CapDiscardExhausted` and `CapRunStates`** ([`retryCaps`](ds:block?id=retrycaps-vqww8mgj)). [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z) declares neither and refuses the option at `Register` ([`checkRetry`](ds:block?id=hatchet-checkretry-ccamdfbn)), so a run out of retries is `failed` there.

## Related

- [Retries](retries.md): backoff, `Discard`, `DiscardOnExhaust`, `MaxErrors` and `Until`.
- [Cancel and status](cancel-and-status.md): every status and how to read it.
- [Manage runs](manage-runs.md): listing and retrying runs.
- [Writing a driver](../extending/writing-a-driver.md): what a driver of your own must report.
- [Batch lifecycle](../batches/lifecycle.md): how a batch reacts to its jobs' ends.
