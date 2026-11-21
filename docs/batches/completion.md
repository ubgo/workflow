# Batch completion

A batch decides its callbacks from its jobs' ends, so it needs to hear about every one. This page covers where those ends come from (the run-finished hook, a status poller, or Hatchet's run list), how to choose, and what happens when a decided callback fails to enqueue.

## Before you start

- A batch `Manager` ([batches](batches.md)).
- The runnable programs for this page are [examples/batch-completion](../../examples/batch-completion/) (the poller, an app's own source, `RunFinished`; in memory, no service) and [examples/batches](../../examples/batches/) (callback retries; its Hatchet part runs when `HATCHET_CLIENT_TOKEN` is set).

## How a batch learns a job ended

A `batch.Source` tells a `Manager` that batch jobs ended; `Manager.Attach(ctx)` connects the Manager's sources, once, before `client.Start`. A source reports through `Manager.RunFinished(ctx, labels, status)`, which ignores repeats, so several sources may report the same job: the first terminal status recorded wins. Unless `batch.WithCompletionSource(srcs...)` names them, `Attach` picks the sources by what the client can do:

| Client | Default sources | Why |
|---|---|---|
| a `workflow.RunNotifier` declaring `CapRunFinished` (memory, SQL, Redis) | `batch.HookSource()` | every run end is guaranteed to arrive |
| a `RunNotifier` without it (Hatchet) | `HookSource()` plus `batch.PollSource(interval)` | the hook is fast; the poller is the safety net for an event the hook loses |
| any other client | `PollSource(interval)` | `Client.Status` exists on every driver |

- `HookSource()` registers the batch on the client's run-finished hook. Its `Attach` fails with an `UnsupportedError` when the client is not a `RunNotifier`.
- `PollSource(interval)` calls `Client.Status` for every unfinished job of every open batch each interval, on the Manager's clock, and also retries pending callbacks and resumes dead dispatches. A `Status` error for one job is logged and retried next tick, never guessed into a status. Its `Attach` fails with `workflow.ErrInvalid` for an interval that is not positive.
- `batch.WithStatusPoll(interval)` changes the default poller's interval (`batch.DefaultPollInterval`, five seconds). Each tick costs one `Status` call per unfinished job, so it trades engine load against callback latency. It does not add a poller where the defaults would not; name `PollSource` in `WithCompletionSource` for that.

`WithCompletionSource` replaces the defaults entirely, for a native engine feed (below), or `HookSource()` plus `PollSource` on a client whose hook is guaranteed but which the app wants double-checked. `Attach` returns the first source's error, and a `Manager` whose `Attach` failed must not be used.

### Step 1: let the poller find job ends

A client without the run-finished hook gets the poller by default. The example hides the memory driver's hook behind a type that embeds only `workflow.Client`, so `Attach` picks `PollSource`, and sets its interval with `WithStatusPoll`. The poller ticks on the Manager's clock, here a fake one:

<!-- snippet: examples/batch-completion/main.go#status-poll -->
```go
// statusOnly hides the driver's run-finished hook: the Manager sees a
// client that only answers Status, so Attach picks the poller.
store := batch.NewMemoryStore()
polled := batch.New(statusOnly{c}, store,
	batch.WithClock(clk),            // the poller ticks on the Manager's clock
	batch.WithStatusPoll(pollEvery), // the default poller's interval (DefaultPollInterval is 5s)
	batch.WithIDFunc(numbered("p")), // readable IDs for the output
)
if err := polled.Attach(ctx); err != nil { // starts the poller
	return err
}
```

The job runs at once, but the batch hears of it only on the next tick. Moving the fake clock one interval makes the poller call `Status` for the unfinished job, record its end and enqueue `Finally`:

<!-- snippet: examples/batch-completion/main.go#poll-tick -->
```go
p, err := polled.Progress(ctx, id)
if err != nil {
	return err
}
fmt.Fprintf(w, "the job ran; before the poll: %d of %d processed\n", p.Processed(), p.Total)
if err := clk.BlockUntil(ctx, 1); err != nil { // the poller is waiting on its timer
	return err
}
clk.Advance(pollEvery) // one tick: one Status call per unfinished job
```

It prints `before the poll: 0 of 1 processed`, then the `Finally` line and `after one poll: 1 of 1 processed, finished=true`. On a real clock the wait is up to one interval, which is the latency a poller adds to every callback.

### Step 2: feed the batch from your own source

An app with its own feed of run ends (a message bus, an engine webhook) writes a `batch.Source`. Its `Attach(ctx, m)` is called once by `Manager.Attach`, before the client starts; a source that works in the background starts its goroutine there and stops it when `ctx` ends. It reports each end with `Manager.RunFinished(ctx, labels, status)`, passing the finished run's `Job.Labels`, which carry the batch labels:

<!-- snippet: examples/batch-completion/main.go#source -->
```go
// busSource is a batch.Source fed by the app: every run end that arrives
// on events is handed to Manager.RunFinished, which ignores runs that are
// not batch jobs and repeats, so the feed may carry every run.
type busSource struct{ events <-chan workflow.RunEvent }

// Attach starts the reader; it stops when ctx, the context given to
// Manager.Attach, is cancelled.
func (s busSource) Attach(ctx context.Context, m *batch.Manager) error {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-s.events:
				if err := m.RunFinished(ctx, ev.Labels, ev.Status); err != nil {
					fmt.Fprintln(os.Stderr, "batch-completion: record run end:", err) // a real app logs and alerts
				}
			}
		}
	}()
	return nil
}
```

`WithCompletionSource` puts it in place of the defaults, so this Manager has no hook and no poller of its own:

<!-- snippet: examples/batch-completion/main.go#own-source -->
```go
fed := batch.New(c, batch.NewMemoryStore(),
	batch.WithCompletionSource(busSource{events: bus}), // replaces the defaults: no hook, no poller
	batch.WithIDFunc(numbered("f")),
)
if err := fed.Attach(ctx); err != nil { // before Start, as for every Manager
	return err
}
```

The example fills the feed from the driver's run-finished hook, standing for a bus; the batch then runs to `Then` and `Finally` as with the hook. A source that knows a job only by its ref, not its labels, builds them with `batch.JobLabels(id, index, attempt)` (the attempt is `JobState.Attempt`).

### Step 3: report every run, not only batch jobs

`RunFinished` ignores a run that is not a batch job, a non-terminal status, an unknown batch and a repeat for a job already recorded, and returns nil for each, so a feed can carry every run of the app without filtering:

<!-- snippet: examples/batch-completion/main.go#ignored -->
```go
// A run that is not a batch job carries no batch labels: ignored, nil.
err = fed.RunFinished(ctx, map[string]string{"tenant": "acme"}, workflow.StatusSucceeded)
fmt.Fprintln(w, "not a batch job, error:", err)
// A job of a batch this Manager's store does not hold: ignored, nil.
err = fed.RunFinished(ctx, batch.JobLabels("unknown", 0, 0), workflow.StatusFailed)
fmt.Fprintln(w, "unknown batch, error:", err)
```

Both lines print `error: <nil>`. An app can also skip sources and `Attach` altogether and call `RunFinished` from its own handler; it then also owns calling `Manager.RetryCallbacks` and `Manager.ResumeDispatches`, which the poller and the retry loop would otherwise run.

## On Hatchet

The default poller costs one `Status` call per unfinished job per tick. `hatchet.Client.BatchSource(store, interval, logger)` is a source of the Hatchet driver's own that makes one run-list call per unfinished batch each interval instead, filtered by the batch's `workflow.batch.id` label and the terminal statuses. `store` must be the `Store` the Manager uses, because the source reads the open batches from it. A job whose ref holds only a `ScheduledID` (a delayed start that has not fired) has no engine run to list, so the source asks `Status` for that one job. A list error is logged and retried next tick. Each tick also retries pending callbacks, as the poller does; a nil logger means `hatchet.Config.Logger`.

<!-- snippet: examples/batches/hatchet.go#coordinator -->
```go
c := hatchet.New(hatchet.Config{
	WorkerName:       "batches-example",
	BatchCoordinator: true, // Start registers the coordinator workflow on this worker
})
store := batch.NewMemoryStore() // a sqlstore BatchStore when several processes share batches
m := batch.New(c, store, batch.WithCompletionSource(
	batch.HookSource(),                     // fast, best effort on Hatchet
	c.BatchSource(store, time.Second, nil), // the LabelSource a coordinated batch needs
))
if err := m.Attach(ctx); err != nil {
	return err
}
```

`BatchSource` is a `batch.LabelSource`: it finds jobs by label, without their refs. That is what a batch started by the Hatchet batch coordinator needs, whose jobs never get refs, and `Dispatch` refuses a coordinated batch on a Manager with no `LabelSource` attached ([the Hatchet batch coordinator](native-groups.md#the-hatchet-batch-coordinator)).

## Callbacks that fail to enqueue

A decided callback is saved as pending (`State.Pending`, one `batch.PendingCallback` per callback) in the same `Store.Update` as the decision, then enqueued, then removed. One whose `Enqueue` fails stays pending, with its `Attempts`, `LastError` and a `RetryAt` when the current attempt's claim ends, and is retried after that claim on whichever comes first:

- the status poller's next tick (and the Hatchet `BatchSource`'s);
- the next job end recorded for that batch;
- the retry loop `Attach` starts when no source is a poller, which calls `Manager.RetryCallbacks` every claim length;
- a call of `Manager.RetryCallbacks(ctx)` by the app, which retries every due pending callback across every batch in the store.

`batch.WithCallbackRetry(d)` sets the claim length (`batch.DefaultCallbackRetry`, thirty seconds), which must outlast one `Enqueue` so two attempts never overlap. `batch.WithManualCallbackRetry()` stops `Attach` from starting the retry loop, for an app that calls `RetryCallbacks` on its own schedule. The example does that, with a client whose first `Finally` enqueue fails:

<!-- snippet: examples/batches/main.go#retry -->
```go
st, err := store.Get(ctx, id)
if err != nil {
	return err
}
for _, pc := range st.Pending {
	fmt.Printf("pending %s: attempts=%d last error: %s\n", pc.Kind, pc.Attempts, pc.LastError)
}
clk.Advance(batch.DefaultCallbackRetry) // the failed attempt's claim ends
if err := m.RetryCallbacks(ctx); err != nil {
	return err
}
```

A batch stays open (`State.Open`, listed by `Store.Unfinished`) while a callback is pending, even after every job has finished. When the client declares `workflow.CapIdempotencyKey`, every callback carries the key `batch:<id>:<kind>`, so a retry after an enqueue that landed but was not recorded cannot double it.

## Try it

The sources, in memory:

```sh
cd examples
go run ./batch-completion
```

```text
-- a poller, on a client without the run-finished hook
the job ran; before the poll: 0 of 1 processed
finally p1 succeeded=1 of 1
after one poll: 1 of 1 processed, finished=true
-- an app's own feed of run ends
finally f1 succeeded=2 of 2
then f1 succeeded=2 of 2
f1: 2 of 2 processed, finished=true
not a batch job, error: <nil>
unknown batch, error: <nil>
```

`go test ./batch-completion` runs it and checks every line. The callback retry, from the batches program:

```sh
go run ./batches
```

The completion-related lines of its output:

```text
-- a Finally that failed to enqueue
pending finally: attempts=1 last error: batch thumbs-4: enqueue finally callback "finally": engine unavailable
finally thumbs-4 tenant=globex succeeded=1 failed=0 cancelledJobs=0 cancelled=false
```

The Hatchet part (`hatchet.go`) dispatches 50 jobs through the batch coordinator with `BatchSource` as its completion source. Against the shared local stack's engine (gRPC on port 7004, no TLS):

```sh
HATCHET_CLIENT_TOKEN=... HATCHET_CLIENT_HOST_PORT=localhost:7004 HATCHET_CLIENT_TLS_STRATEGY=none go run ./batches
```

```text
hatchet: coordinated=true, one parent run=true
hatchet: finally succeeded=50 of 50
hatchet: coordinated batch without a LabelSource refused: true
```

`go test ./batches` skips the Hatchet test, with the reason, when the token is not set.

## Pitfalls

- **Manual retry means you must retry.** With `WithManualCallbackRetry` and no poller, a callback that fails after the batch's last job end is never retried unless the app calls `RetryCallbacks`.
- **A retry waits for the claim.** `RetryCallbacks` skips a pending callback until its `RetryAt`; calling it right after a failure does nothing.
- **The poller's cost grows with unfinished jobs.** A 10,000-job batch at the default interval is up to 2,000 `Status` calls a second; on Hatchet use `BatchSource`, elsewhere raise the interval.
- **Two sources are fine, a missing one is not.** Repeats are ignored, so adding a poller beside the hook is always safe; a `WithCompletionSource` that names no source able to see the jobs leaves batches open forever.
- **An empty `WithCompletionSource()` keeps the defaults.** The option appends its sources, and a Manager with none falls back to the defaults, so it cannot switch every source off; to feed the batch only by hand, leave `Attach` out instead. Two `WithCompletionSource` options add up rather than the second replacing the first.
- **A source must not block `Attach`.** `Attach` runs before `client.Start`; a source that reads its feed inside `Attach` instead of in a goroutine stops the app from starting.
- **A feed must carry the run's labels.** `RunFinished` finds the job by `workflow.batch.id`, `workflow.batch.index` and, after `RetryFailed`, `workflow.batch.attempt`. A feed that drops labels makes every end look like a run outside any batch, silently ignored.
- **`Status` errors are not guesses.** An engine that pruned a run returns `workflow.ErrNotFound` for it; the poller logs and retries it every tick, so retention on the engine must outlast your batches.

## Related

- [Batches](batches.md): dispatch, progress, cancel.
- [Change a batch after dispatch](lifecycle.md): `RetryFailed`, whose attempt label a source must pass on.
- [Native group runs](native-groups.md): the coordinator that needs a `LabelSource`.
- [React when a run finishes](../observability/run-finished.md): the hook behind `HookSource`.
- [Durability](durability.md): `ResumeDispatches`, which the poller also calls.
- Examples: [examples/batch-completion](../../examples/batch-completion/) and [examples/batches](../../examples/batches/).
