# Progress and heartbeats

A long job is a black box unless it says how it is doing. An export page wants "4,000 of 10,000 orders"; an operator wants to tell a slow import from a hung one before its one-hour timeout fires. A step does both from inside: [`workflow.ReportProgress`](ds:block?id=reportprogress-qtz35tuj) records how far the run has got, for people and pages to read, and [`workflow.Heartbeat`](ds:block?id=heartbeat-jrq6m57p) says the attempt is alive, so a step that stops beating can be failed and retried early.

## Before you start

- Progress is an optional part of a driver, with no capability bit: [`ReportProgress`](ds:block?id=reportprogress-qtz35tuj) returns an `*workflow.UnsupportedError` (matches `workflow.ErrUnsupported`) where the driver keeps no progress, and outside a step. The memory, Redis and SQL drivers keep it ([`memory`](ds:block?id=memory-client-reportprogress-ntp97att), [`redis`](ds:block?id=redis-progressreporter-reportprogress-gs4bfrq5), [`sqlstore`](ds:block?id=sql-client-progressreporter-3e6yptcc)); Hatchet does not.
- `Step.HeartbeatTimeout` needs [`workflow.CapHeartbeat`](ds:block?id=capheartbeat-92ykxh7q) ([`Requires`](ds:block?id=requires-6tcjbcfj)), and a driver without it refuses a workflow that sets it rather than ignoring the field ([`CheckCaps`](ds:block?id=checkcaps-eg44ezuh)). The memory, Redis and SQL drivers have it ([`memory`](ds:block?id=memory-caps-yxkuq583), [`redis`](ds:block?id=redis-caps-bfgdst9t), [`sqlstore`](ds:block?id=sql-p5caps-wggh6esn)); Hatchet does not ([`hatchet`](ds:block?id=hatchet-caps-d6x9fv8z)). See [Hatchet limitations](../drivers/hatchet.md#limitations).
- The examples are [`examples/results-progress`](../../examples/results-progress) (progress) and [`examples/snooze-heartbeat`](../../examples/snooze-heartbeat) (heartbeats). Both run on the memory driver with a fake clock. Run them with `go run ./results-progress` and `go run ./snooze-heartbeat` from `examples/`.

## Report progress from a step

[`workflow.ReportProgress(ctx, p)`](ds:block?id=reportprogress-qtz35tuj) records `p` as the run's progress. A [`workflow.Progress`](ds:block?id=progress-rtebxj4k) has `Percent` (0 to 100), a `Message` for a person and optional `Data` for the app (a cursor, counts) of at most [`workflow.MaxProgressData`](ds:block?id=maxprogressdata-3mzac5hg) (64 KiB); the driver fills `Step` and `At` (its own clock). The latest report wins ([`workflow.ProgressReporter`](ds:block?id=progressreporter-n64hhjpe)). The export below reports after each thousand orders:

<!-- snippet: examples/results-progress/main.go#report -->
```go
export := workflow.Define(func(ctx context.Context, _ struct{}) (ExportResult, error) {
	const total, batch = 3000, 1000 // orders to export, and how many per round
	for done := batch; done <= total; done += batch {
		// ... export the next 1,000 orders ...
		err := workflow.ReportProgress(ctx, workflow.Progress{
			Percent: 100 * float64(done) / total, // a share from 0 to 100
			Message: fmt.Sprintf("exported %d of %d orders", done, total),
		})
		if err != nil && !errors.Is(err, workflow.ErrUnsupported) {
			return ExportResult{}, err // the run ended under us
		}
	}
	return ExportResult{File: "orders.csv", Rows: total}, nil
})
```

[`ReportProgress`](ds:block?id=reportprogress-qtz35tuj) validates first: a `Percent` outside `[0, 100]` (or NaN) or oversized `Data` is refused with `ErrInvalid` ([`workflow.ValidateProgress`](ds:block?id=validateprogress-gcftj58m) is the check). It then returns an `UnsupportedError` where nothing keeps progress: unlike a heartbeat, the caller asked for data to be stored, so dropping it silently would be a lie. A handler that does not care ignores that error, as the example does, and stays portable. On the memory driver, a report from an attempt whose run has already ended returns an error matching `workflow.ErrCancelled` ([`Client.reportProgress`](ds:block?id=memory-client-reportprogress-ntp97att)).

## Read progress

Progress reaches the outside in two ways. A client with `workflow.RunQuerier` returns the latest report as `RunInfo.Progress` (nil until a step reports; see [`workflow.ProgressReporter`](ds:block?id=progressreporter-n64hhjpe)), which is what an export page polls:

<!-- snippet: examples/results-progress/main.go#latest -->
```go
page, err := c.Runs(ctx, workflow.RunQuery{Ref: ref})
if err != nil {
	return err
}
if p := page.Runs[0].Progress; p != nil { // nil until a step reports
	fmt.Printf("  latest: %.0f%% %s\n", p.Percent, p.Message)
}
```

A client implementing [`workflow.LifecycleNotifier`](ds:block?id=lifecyclenotifier-3vtxxku9) also streams every report as a [`workflow.LifecycleProgress`](ds:block?id=lifecycleprogress-yvqxcvp5) event, with the progress on `LifecycleEvent.Progress`. `OnLifecycle` is registered before `Start`:

<!-- snippet: examples/results-progress/main.go#events -->
```go
progress := make(chan workflow.Progress, 8) // room for every report: the observer must not block
err := c.OnLifecycle(func(_ context.Context, ev workflow.LifecycleEvent) {
	if ev.Kind == workflow.LifecycleProgress {
		progress <- *ev.Progress // a page could push this to the browser
	}
})
```

Lifecycle events are best effort and local to the process that ran the step ([`LifecycleNotifier`](ds:block?id=lifecyclenotifier-3vtxxku9)): good for pushing a bar to a browser connected to that process, not for anything that must not be missed. Use `RunInfo.Progress`, or the [run-finished hook](../observability/run-finished.md) for the end, when it must be reliable. The memory, Redis and SQL drivers implement `OnLifecycle` ([`memory`](ds:block?id=memory-client-onlifecycle-wspq9c6m), [`redis`](ds:block?id=redis-client-onlifecycle-pkz638m3), [`sqlstore`](ds:block?id=sql-client-onlifecycle-yrrp27ek)); Hatchet does not.

## Send heartbeats

[`workflow.Heartbeat(ctx, details)`](ds:block?id=heartbeat-jrq6m57p) tells the driver that the attempt is alive. It is cheap enough to call once per batch or per row:

<!-- snippet: examples/snooze-heartbeat/main.go#heartbeat -->
```go
// load hangs on its first attempt and beats once per batch on the next.
load := func(ctx context.Context, _ []byte) ([]byte, error) {
	info, _ := workflow.StepInfoFrom(ctx)
	attempts <- info.Attempt
	if info.Attempt == 1 {
		<-ctx.Done() // a hung connection: no heartbeat, no progress
		return nil, context.Cause(ctx)
	}
	for batch := 1; batch <= 3; batch++ { // three batches stand in for the catalog
		// ... load one batch ...
		if err := workflow.Heartbeat(ctx, fmt.Appendf(nil, "batch %d", batch)); err != nil {
			return nil, err // the beat was not recorded: the attempt lost its lease
		}
	}
	return nil, nil
}
wf := workflow.Workflow{
	Name:  "import-catalog",
	Retry: workflow.RetryPolicy{Max: 1, Backoff: workflow.Backoff{Initial: time.Second}},
	Steps: []workflow.Step{{
		Name:             "load",
		Run:              load,
		HeartbeatTimeout: 30 * time.Second, // the most it may go without a beat
	}},
}
```

A beat does two things ([`workflow.Heartbeater`](ds:block?id=heartbeater-7xmvzehy)). It restarts the attempt's `HeartbeatTimeout` clock (below), and on a driver that leases attempts it extends the lease, so a long attempt is not taken for a dead one and run elsewhere; the Redis driver renews its lease on every beat ([`Client.attempt`](ds:block?id=redis-client-attempt-46v4982x)). `details` is an optional marker for the driver; neither the memory ([`Client.watchHeartbeat`](ds:block?id=memory-client-watchheartbeat-kbuy9sm6)) nor the Redis driver stores it.

Outside a step, or on a driver without `CapHeartbeat`, [`Heartbeat`](ds:block?id=heartbeat-jrq6m57p) returns nil and does nothing: liveness is a hint, and a portable handler must not fail because one driver has no use for it. An error from [`Heartbeat`](ds:block?id=heartbeater-7xmvzehy) means the beat was not recorded, because the backend is unreachable or the attempt lost its lease and will run elsewhere; a handler that gets an error matching `workflow.ErrCancelled` should stop.

## Catch a hung step

`Step.HeartbeatTimeout` fails an attempt that goes that long without a beat, counted from the attempt's start and then from each beat ([`Client.watchHeartbeat`](ds:block?id=memory-client-watchheartbeat-kbuy9sm6) on the memory driver). It catches a handler stuck on a dead connection after seconds, where `Step.Timeout` has to allow for the longest healthy attempt. The `load` step in [Send heartbeats](#send-heartbeats) sets it to 30 seconds. A step with a heartbeat timeout makes its workflow need [`workflow.CapHeartbeat`](ds:block?id=capheartbeat-92ykxh7q) ([`Requires`](ds:block?id=requires-6tcjbcfj)), so a driver that would never fire it refuses the workflow instead (the memory, SQL and Redis drivers have it; Hatchet refuses such a step at `Register`, in [`checkSupported`](ds:block?id=hatchet-checksupported-733uwgsp)):

<!-- snippet: examples/snooze-heartbeat/main.go#hung -->
```go
// HeartbeatTimeout makes the workflow need CapHeartbeat: Start refuses
// it on a driver without the capability instead of never firing.
fmt.Println("  needs:", workflow.Requires(wf))
```

In the example the first attempt hangs without beating and is failed after 30 seconds; the retry beats and succeeds:

```text
heartbeat:
  needs: Heartbeat
  attempt 1 started
  attempt 2 started at +31s
  attempt 1 failed at +30s: workflow: heartbeat timed out: memory: step "load" attempt 1 went 30s without a heartbeat
  run succeeded
  beats from a direct call: ["batch 1" "batch 2" "batch 3"]
```

The failed attempt's error matches [`workflow.ErrHeartbeatTimeout`](ds:block?id=errheartbeattimeout-t75kucja) and `workflow.ErrTimeout` (see [timeouts](timeouts.md)), and the step's retry policy applies to it like any other failure. A step that sets `HeartbeatTimeout` must beat more often than that, from its first second: an attempt that does a minute of setup before its first beat needs a timeout longer than the setup. `HeartbeatTimeout` is for run steps only; a negative value fails validation ([`validateStep`](ds:block?id=validatestep-x6aebswc)).

The timeout only helps if the handler really beats on every path. [`workflow.Heartbeat`](ds:block?id=heartbeat-jrq6m57p) sends each beat to the `workflow.Heartbeater` on the step's context (drivers set theirs with [`WithHeartbeater`](ds:block?id=withheartbeater-rku3vrw5)), and does nothing when there is none. So a unit test can call the handler directly with a [`workflow.HeartbeatFunc`](ds:block?id=heartbeatfunc-45wm6xnt) that records the beats, which is the last line above:

<!-- snippet: examples/snooze-heartbeat/main.go#heartbeater -->
```go
var beats []string
// beater records each beat the handler sends, in place of a driver.
beater := workflow.HeartbeatFunc(func(_ context.Context, details []byte) error {
	beats = append(beats, string(details))
	return nil
})
testCtx := workflow.WithStepInfo(workflow.WithHeartbeater(ctx, beater), workflow.StepInfo{Attempt: 2}) // past attempt 1, which hangs
if _, err := load(testCtx, nil); err != nil {
	return err
}
fmt.Printf("  beats from a direct call: %q\n", beats)
```

## Try it

```sh
cd examples
go run ./results-progress
```

The progress part of the output:

```text
progress:
   33% exported 1000 of 3000 orders (step export, at 09:00)
   67% exported 2000 of 3000 orders (step export, at 09:00)
  100% exported 3000 of 3000 orders (step export, at 09:00)
  latest: 100% exported 3000 of 3000 orders
  outside a step: unsupported true
```

`go run ./snooze-heartbeat` prints the heartbeat lines above. `go test ./results-progress ./snooze-heartbeat` checks every line.

## Pitfalls

- **`ReportProgress` and `Heartbeat` differ on an unsupported driver.** [`Heartbeat`](ds:block?id=heartbeat-jrq6m57p) is a silent no-op; [`ReportProgress`](ds:block?id=reportprogress-qtz35tuj) returns an `UnsupportedError`. Check the error with `errors.Is(err, workflow.ErrUnsupported)` if the handler must run everywhere.
- **Progress is one value, not a log.** Each report replaces the last ([`workflow.Progress`](ds:block?id=progress-rtebxj4k)); keep a history in your own store if you need one.
- **Lifecycle events are local.** On a fleet, a progress event fires only in the process running the step. An observer runs on a driver goroutine and must return quickly; a panic in it is recovered and never changes the run ([`LifecycleNotifier`](ds:block?id=lifecyclenotifier-3vtxxku9), [`Client.observeLife`](ds:block?id=memory-client-observelife-8cuk4gby) on the memory driver).
- **A heartbeat is not progress.** It says "alive", not "how far": [`Heartbeat`](ds:block?id=heartbeat-jrq6m57p) stores no `Progress`. Call both when a page shows progress and a hang must be caught.
- **A return after the timeout does not save the attempt.** When the watchdog fires, the step's `ctx` is cancelled with a cause matching [`workflow.ErrHeartbeatTimeout`](ds:block?id=errheartbeattimeout-t75kucja) (and `ErrTimeout`; a `Step.Timeout` cut-off matches [`ErrStepTimeout`](ds:block?id=errsteptimeout-6b727264)), so `errors.Is(context.Cause(ctx), workflow.ErrHeartbeatTimeout)` tells a hang from a cancel in the handler. Whatever the handler returns after that, even an output, the attempt fails with the timeout ([`Client.exec`](ds:block?id=memory-client-exec-g88wsrmp) on the memory driver); only a step tolerated by [`WithRules`](../workflows/trigger-rules.md) turns it into a recorded `FailureTimeout`.
- **A handler that ignores `ctx` keeps running after a heartbeat timeout.** Go cannot stop a goroutine: the attempt is failed and retried, and the old goroutine runs on until it returns ([`Client.watchHeartbeat`](ds:block?id=memory-client-watchheartbeat-kbuy9sm6) only cancels the attempt's `ctx`). Pass `ctx` to every call that can block.

## Related

- [Timeouts](timeouts.md): every deadline and its error.
- [Results and awaiting a run](results.md): the value a run returns.
- [Retries](retries.md): what happens after a heartbeat timeout.
- [Workflows and steps › Timeouts](../workflows/steps.md#timeouts): `Step.Timeout`.
- Examples: [examples/results-progress](../../examples/results-progress/), [examples/snooze-heartbeat](../../examples/snooze-heartbeat/).
