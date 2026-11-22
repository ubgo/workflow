# Batch completion

A batch decides its callbacks from its jobs' ends, so it needs to hear about every one. This page covers where those ends come from (the run-finished hook, a status poller, or Hatchet's run list), how to choose, and what happens when a decided callback fails to enqueue.

## Before you start

- A batch `Manager` ([`batch.New`](ds:block?id=batch-new-geazwpek), [batches](batches.md)).
- The runnable programs for this page are [examples/batch-completion](../../examples/batch-completion/) (the poller, an app's own source, `RunFinished`; in memory, no service) and [examples/batches](../../examples/batches/) (callback retries; its Hatchet part runs when `HATCHET_CLIENT_TOKEN` is set).

## How a batch learns a job ended

A [`batch.Source`](ds:block?id=batch-source-2g3gyra2) tells a `Manager` that batch jobs ended; [`Manager.Attach(ctx)`](ds:block?id=batch-manager-attach-xnchrce9) connects the Manager's sources, once, before `client.Start`. A source reports through [`Manager.RunFinished(ctx, labels, status)`](ds:block?id=batch-manager-runfinished-cfkbj5us), which ignores repeats, so several sources may report the same job: the first terminal status recorded wins. Unless [`batch.WithCompletionSource(srcs...)`](ds:block?id=batch-withcompletionsource-fedeg25x) names them, `Attach` picks the sources by what the client can do ([`defaultSources`](ds:block?id=batch-manager-defaultsources-cgw2pdqt)):

| Client | Default sources | Why |
|---|---|---|
| a `workflow.RunNotifier` declaring `CapRunFinished` ([memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-caps-yu37ef2j), [Redis](ds:block?id=redis-caps-bfgdst9t)) | [`batch.HookSource()`](ds:block?id=batch-hooksource-4vdc7c8r) | every run end is guaranteed to arrive |
| a `RunNotifier` without it ([Hatchet](ds:block?id=hatchet-caps-d6x9fv8z)) | `HookSource()` plus [`batch.PollSource(interval)`](ds:block?id=batch-pollsource-kr85dvdk) | the hook is fast; the poller is the safety net for an event the hook loses |
| any other client | [`PollSource(interval)`](ds:block?id=batch-manager-defaultsources-cgw2pdqt) | `Client.Status` exists on every driver |

- `HookSource()` registers the batch on the client's run-finished hook. Its [`Attach`](ds:block?id=batch-hooksource-attach-3ympu39g) fails with an `UnsupportedError` when the client is not a `RunNotifier`.
- `PollSource(interval)` calls `Client.Status` for every unfinished job of every open batch each interval, on the Manager's clock, and also retries pending callbacks and resumes dead dispatches ([`poll`](ds:block?id=batch-manager-poll-q7znr3wp)). A `Status` error for one job is logged (to the Manager's [`WithLogger`](ds:block?id=batch-withlogger-nd8ayt7j) logger, which discards by default) and retried next tick, never guessed into a status. Its [`Attach`](ds:block?id=batch-pollsource-attach-2eyttg2p) fails with `workflow.ErrInvalid` for an interval that is not positive.
- [`batch.WithStatusPoll(interval)`](ds:block?id=batch-withstatuspoll-yu9gvj3a) changes the default poller's interval ([`batch.DefaultPollInterval`](ds:block?id=batch-defaultpollinterval-a8rdxsaq), five seconds). Each tick costs one `Status` call per unfinished job, so it trades engine load against callback latency. It does not add a poller where the defaults would not; name `PollSource` in `WithCompletionSource` for that.

[`WithCompletionSource`](ds:block?id=batch-withcompletionsource-fedeg25x) replaces the defaults entirely, for a native engine feed (below), or `HookSource()` plus `PollSource` on a client whose hook is guaranteed but which the app wants double-checked. Name every source in one call: a second `WithCompletionSource` replaces the first. An empty `WithCompletionSource()` means no source at all, for an app that reports every run end itself through `Manager.RunFinished`; leave the option out to keep the defaults. [`Attach`](ds:block?id=batch-manager-attach-xnchrce9) returns the first source's error, and a `Manager` whose `Attach` failed must not be used.

### Step 1: let the poller find job ends

A client without the run-finished hook gets the poller by default. The example hides the memory driver's hook behind a type that embeds only `workflow.Client`, so `Attach` picks `PollSource` ([`defaultSources`](ds:block?id=batch-manager-defaultsources-cgw2pdqt)), and sets its interval with `WithStatusPoll`. The poller ticks on the Manager's clock, here a fake one:

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

The job runs at once, but the batch hears of it only on the next tick. Moving the fake clock one interval makes the [poller](ds:block?id=batch-manager-poll-q7znr3wp) call `Status` for the unfinished job, record its end and enqueue `Finally`:

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

An app with its own feed of run ends (a message bus, an engine webhook) writes a [`batch.Source`](ds:block?id=batch-source-2g3gyra2). Its `Attach(ctx, m)` is called once by `Manager.Attach`, before the client starts; a source that works in the background starts its goroutine there and stops it when `ctx` ends. It reports each end with [`Manager.RunFinished(ctx, labels, status)`](ds:block?id=batch-manager-runfinished-cfkbj5us), passing the finished run's `Job.Labels`, which carry the batch labels:

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

[`WithCompletionSource`](ds:block?id=batch-withcompletionsource-fedeg25x) puts it in place of the defaults, so this Manager has no hook and no poller of its own:

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

The example fills the feed from the driver's run-finished hook, standing for a bus; the batch then runs to `Then` and `Finally` as with the hook. A source that knows a job only by its ref, not its labels, builds them with [`batch.JobLabels(id, index, attempt)`](ds:block?id=batch-joblabels-48uy5r4y) (the attempt is [`JobState.Attempt`](ds:block?id=batch-jobstate-pbbfqagq)).

### Step 3: report every run, not only batch jobs

[`RunFinished`](ds:block?id=batch-manager-runfinished-cfkbj5us) ignores a run that is not a batch job, a non-terminal status, an unknown batch and a repeat for a job already recorded ([`record`](ds:block?id=batch-manager-record-b8sf5qgn)), and returns nil for each, so a feed can carry every run of the app without filtering:

<!-- snippet: examples/batch-completion/main.go#ignored -->
```go
// A run that is not a batch job carries no batch labels: ignored, nil.
err = fed.RunFinished(ctx, map[string]string{"tenant": "acme"}, workflow.StatusSucceeded)
fmt.Fprintln(w, "not a batch job, error:", err)
// A job of a batch this Manager's store does not hold: ignored, nil.
err = fed.RunFinished(ctx, batch.JobLabels("unknown", 0, 0), workflow.StatusFailed)
fmt.Fprintln(w, "unknown batch, error:", err)
```

Both lines print `error: <nil>`. An app can also skip sources and `Attach` altogether and call `RunFinished` from its own handler; it then also owns calling [`Manager.RetryCallbacks`](ds:block?id=batch-manager-retrycallbacks-vg9kzc67) and [`Manager.ResumeDispatches`](ds:block?id=batch-manager-resumedispatches-w57mk3c5), which the poller and the retry loop would otherwise run.

## On Hatchet

The default poller costs one `Status` call per unfinished job per tick. [`hatchet.Client.BatchSource(store, interval, logger)`](ds:block?id=hatchet-client-batchsource-prb3q7d4) is a source of the Hatchet driver's own that makes one run-list call per unfinished batch each interval instead, filtered by the batch's `workflow.batch.id` label and the terminal statuses. `store` must be the `Store` the Manager uses, because the source reads the open batches from it. A job whose ref holds only a `ScheduledID` (a delayed start that has not fired) has no engine run to list, so the source asks `Status` for that one job. A list error is logged and retried next tick. Each tick also retries pending callbacks and resumes dead dispatches, as the poller does ([`poll`](ds:block?id=hatchet-batchsource-poll-7fkufb6p)); a nil logger means `hatchet.Config.Logger`.

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

`BatchSource` is a [`batch.LabelSource`](ds:block?id=batch-labelsource-rq5svjm2): it finds jobs by label, without their refs. That is what a batch started by the Hatchet batch coordinator needs, whose jobs never get refs, and [`Dispatch`](ds:block?id=batch-manager-dispatch-6uvrxrj7) refuses a coordinated batch on a Manager with no `LabelSource` attached ([the Hatchet batch coordinator](native-groups.md#the-hatchet-batch-coordinator)).

## Callbacks that fail to enqueue

A decided callback is saved as pending (`State.Pending`, one [`batch.PendingCallback`](ds:block?id=batch-pendingcallback-pyn7sbnr) per callback) in the same `Store.Update` as the decision ([`claimCallbacks`](ds:block?id=batch-manager-claimcallbacks-z2a8hfdx)), then enqueued, then removed ([`enqueueCallbacks`](ds:block?id=batch-manager-enqueuecallbacks-w9992uzd)). One whose `Enqueue` fails stays pending, with its `Attempts`, `LastError` and a `RetryAt` when the current attempt's claim ends, and is retried after that claim on whichever comes first:

- the status poller's next tick ([`poll`](ds:block?id=batch-manager-poll-q7znr3wp); and the Hatchet `BatchSource`'s);
- the next job end recorded for that batch ([`record`](ds:block?id=batch-manager-record-b8sf5qgn));
- the [retry loop](ds:block?id=batch-manager-retryloop-sxmdq5tv) `Attach` starts when no source is a poller, which calls `Manager.RetryCallbacks` every claim length;
- a call of [`Manager.RetryCallbacks(ctx)`](ds:block?id=batch-manager-retrycallbacks-vg9kzc67) by the app, which retries every due pending callback across every batch in the store.

[`batch.WithCallbackRetry(d)`](ds:block?id=batch-withcallbackretry-rctr3u5p) sets the claim length ([`batch.DefaultCallbackRetry`](ds:block?id=batch-defaultcallbackretry-tqjfsrq3), thirty seconds), which must outlast one `Enqueue` so two attempts never overlap. [`batch.WithManualCallbackRetry()`](ds:block?id=batch-withmanualcallbackretry-sxbps7w4) stops `Attach` from starting the retry loop, for an app that calls `RetryCallbacks` on its own schedule. The example does that, with a client whose first `Finally` enqueue fails:

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

A batch stays open ([`State.Open`](ds:block?id=batch-state-open-v97c8cfq), listed by [`Store.Unfinished`](ds:block?id=batch-store-vp7qhnme)) while a callback is pending, even after every job has finished. When the client declares `workflow.CapIdempotencyKey`, every callback carries the key [`batch:<id>:<kind>`](ds:block?id=batch-callbackkey-agsg3nsq) ([`callback`](ds:block?id=batch-manager-callback-9sp5vupc)), so a retry after an enqueue that landed but was not recorded cannot double it.

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

- **Manual retry means you must retry.** With [`WithManualCallbackRetry`](ds:block?id=batch-withmanualcallbackretry-sxbps7w4) and no poller, a callback that fails after the batch's last job end is never retried unless the app calls `RetryCallbacks`.
- **A retry waits for the claim.** [`RetryCallbacks`](ds:block?id=batch-manager-retrycallbacks-vg9kzc67) skips a pending callback until its `RetryAt`; calling it right after a failure does nothing.
- **The poller's cost grows with unfinished jobs.** A 10,000-job batch at the [default interval](ds:block?id=batch-defaultpollinterval-a8rdxsaq) is up to 2,000 `Status` calls a second; on Hatchet use `BatchSource`, elsewhere raise the interval.
- **Two sources are fine, a missing one is not.** Repeats are ignored ([`recordFinish`](ds:block?id=batch-recordfinish-9b55fcjg)), so adding a poller beside the hook is always safe; a `WithCompletionSource` that names no source able to see the jobs leaves batches open forever.
- **An empty `WithCompletionSource()` switches every source off.** Batches then end only when the app calls `Manager.RunFinished` for each job; a slice that turns out empty by mistake leaves them open forever. Two [`WithCompletionSource`](ds:block?id=batch-withcompletionsource-fedeg25x) options do not add up: the last one wins.
- **A source must not block `Attach`.** [`Attach`](ds:block?id=batch-manager-attach-xnchrce9) runs before `client.Start`; a source that reads its feed inside `Attach` instead of in a goroutine stops the app from starting.
- **A feed must carry the run's labels.** `RunFinished` finds the job by [`workflow.batch.id`](ds:block?id=batch-labelbatch-qsurvmc8), [`workflow.batch.index`](ds:block?id=batch-labelindex-jtskuw4z) ([`batchJob`](ds:block?id=batch-batchjob-znu5danh)) and, after `RetryFailed`, [`workflow.batch.attempt`](ds:block?id=batch-labelattempt-ppud3ep6). A feed that drops labels makes every end look like a run outside any batch, silently ignored.
- **`Status` errors are not guesses.** An engine that pruned a run returns `workflow.ErrNotFound` for it; the [poller](ds:block?id=batch-manager-poll-q7znr3wp) logs and retries it every tick, so retention on the engine must outlast your batches.

## Related

- [Batches](batches.md): dispatch, progress, cancel.
- [Change a batch after dispatch](lifecycle.md): `RetryFailed`, whose attempt label a source must pass on.
- [Native group runs](native-groups.md): the coordinator that needs a `LabelSource`.
- [React when a run finishes](../observability/run-finished.md): the hook behind `HookSource`.
- [Durability](durability.md): `ResumeDispatches`, which the poller also calls.
- Examples: [examples/batch-completion](../../examples/batch-completion/) and [examples/batches](../../examples/batches/).
