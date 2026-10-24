# Progress and heartbeats

A long job is a black box unless it says how it is doing. An export page wants "4,000 of 10,000 orders"; an operator wants to tell a slow import from a hung one before its one-hour timeout fires. A step does both from inside: `workflow.ReportProgress` records how far the run has got, for people and pages to read, and `workflow.Heartbeat` says the attempt is alive, so a step that stops beating can be failed and retried early.

## Before you start

- Progress is an optional part of a driver, with no capability bit: `ReportProgress` returns an `*workflow.UnsupportedError` (matches `workflow.ErrUnsupported`) where the driver keeps no progress, and outside a step. The memory, Redis and SQL drivers keep it; Hatchet does not.
- `Step.HeartbeatTimeout` needs `workflow.CapHeartbeat`, and a driver without it refuses a workflow that sets it rather than ignoring the field. The memory, Redis and SQL drivers have it; Hatchet does not.
- The examples are [`examples/results-progress`](../../examples/results-progress) (progress) and [`examples/snooze-heartbeat`](../../examples/snooze-heartbeat) (heartbeats). Both run on the memory driver with a fake clock. Run them with `go run ./results-progress` and `go run ./snooze-heartbeat` from `examples/`.

## Report progress from a step

`workflow.ReportProgress(ctx, p)` records `p` as the run's progress. A `workflow.Progress` has `Percent` (0 to 100), a `Message` for a person and optional `Data` for the app (a cursor, counts) of at most `workflow.MaxProgressData` (64 KiB); the driver fills `Step` and `At` (its own clock). The latest report wins. The export below reports after each thousand orders:

<!-- snippet: examples/results-progress/main.go#report -->
```go
export := workflow.Define(func(ctx context.Context, _ struct{}) (ExportResult, error) {
	const total = 3000
	for done := 1000; done <= total; done += 1000 {
		// ... export the next 1,000 orders ...
		err := workflow.ReportProgress(ctx, workflow.Progress{
			Percent: 100 * float64(done) / total,
			Message: fmt.Sprintf("exported %d of %d orders", done, total),
		})
		if err != nil && !errors.Is(err, workflow.ErrUnsupported) {
			return ExportResult{}, err // the run ended under us
		}
	}
	return ExportResult{File: "orders.csv", Rows: total}, nil
})
```

`ReportProgress` validates first: a `Percent` outside `[0, 100]` (or NaN) or oversized `Data` is refused with `ErrInvalid` (`workflow.ValidateProgress` is the check). It then returns an `UnsupportedError` where nothing keeps progress: unlike a heartbeat, the caller asked for data to be stored, so dropping it silently would be a lie. A handler that does not care ignores that error, as the example does, and stays portable. On the memory driver, a report from an attempt whose run has already ended returns an error matching `workflow.ErrCancelled`.

## Read progress

Progress reaches the outside in two ways. A client with `workflow.RunQuerier` returns the latest report as `RunInfo.Progress` (nil until a step reports), which is what an export page polls:

<!-- snippet: examples/results-progress/main.go#latest -->
```go
page, err := c.Runs(ctx, workflow.RunQuery{Ref: ref})
if err != nil {
	return err
}
if p := page.Runs[0].Progress; p != nil { // nil until a step reports
	fmt.Fprintf(out, "  latest: %.0f%% %s\n", p.Percent, p.Message)
}
```

A client implementing `workflow.LifecycleNotifier` also streams every report as a `workflow.LifecycleProgress` event, with the progress on `LifecycleEvent.Progress`. `OnLifecycle` is registered before `Start`:

<!-- snippet: examples/results-progress/main.go#events -->
```go
progress := make(chan workflow.Progress, 8)
err := c.OnLifecycle(func(_ context.Context, ev workflow.LifecycleEvent) {
	if ev.Kind == workflow.LifecycleProgress {
		progress <- *ev.Progress // a page could push this to the browser
	}
})
```

Lifecycle events are best effort and local to the process that ran the step: good for pushing a bar to a browser connected to that process, not for anything that must not be missed. Use `RunInfo.Progress`, or the [run-finished hook](../observability/run-finished.md) for the end, when it must be reliable. The memory, Redis and SQL drivers implement `OnLifecycle`; Hatchet does not.

## Send heartbeats

`workflow.Heartbeat(ctx, details)` tells the driver that the attempt is alive. It is cheap enough to call once per batch or per row:

<!-- snippet: examples/snooze-heartbeat/main.go#heartbeat -->
```go
load := func(ctx context.Context, _ []byte) ([]byte, error) {
	info, _ := workflow.StepInfoFrom(ctx)
	attempts <- info.Attempt
	if info.Attempt == 1 {
		<-ctx.Done() // a hung connection: no heartbeat, no progress
		return nil, context.Cause(ctx)
	}
	for batch := 1; batch <= 3; batch++ {
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

A beat does two things. It restarts the attempt's `HeartbeatTimeout` clock (below), and on a driver that leases attempts it extends the lease, so a long attempt is not taken for a dead one and run elsewhere; the Redis driver renews its lease on every beat. `details` is an optional marker for the driver; neither the memory nor the Redis driver stores it.

Outside a step, or on a driver without `CapHeartbeat`, `Heartbeat` returns nil and does nothing: liveness is a hint, and a portable handler must not fail because one driver has no use for it. An error from `Heartbeat` means the beat was not recorded, because the backend is unreachable or the attempt lost its lease and will run elsewhere; a handler that gets an error matching `workflow.ErrCancelled` should stop.

## Catch a hung step

`Step.HeartbeatTimeout` fails an attempt that goes that long without a beat, counted from the attempt's start and then from each beat. It catches a handler stuck on a dead connection after seconds, where `Step.Timeout` has to allow for the longest healthy attempt. In the example the first attempt hangs without beating and is failed after 30 seconds; the retry beats and succeeds:

```text
heartbeat:
  attempt 1 started
  attempt 2 started at +31s
  attempt 1 failed at +30s: workflow: heartbeat timed out: memory: step "load" attempt 1 went 30s without a heartbeat
  run succeeded
```

The failed attempt's error matches `workflow.ErrHeartbeatTimeout` and `workflow.ErrTimeout` (see [timeouts](timeouts.md)), and the step's retry policy applies to it like any other failure. A step that sets `HeartbeatTimeout` must beat more often than that, from its first second: an attempt that does a minute of setup before its first beat needs a timeout longer than the setup. `HeartbeatTimeout` is for run steps only; a negative value fails validation.

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

- **`ReportProgress` and `Heartbeat` differ on an unsupported driver.** `Heartbeat` is a silent no-op; `ReportProgress` returns an `UnsupportedError`. Check the error with `errors.Is(err, workflow.ErrUnsupported)` if the handler must run everywhere.
- **Progress is one value, not a log.** Each report replaces the last; keep a history in your own store if you need one.
- **Lifecycle events are local.** On a fleet, a progress event fires only in the process running the step. An observer runs on a driver goroutine and must return quickly; a panic in it is recovered and never changes the run.
- **A heartbeat is not progress.** It says "alive", not "how far". Call both when a page shows progress and a hang must be caught.
- **A handler that ignores `ctx` keeps running after a heartbeat timeout.** Go cannot stop a goroutine: the attempt is failed and retried, and the old goroutine runs on until it returns. Pass `ctx` to every call that can block.

## Related

- [Timeouts](timeouts.md): every deadline and its error.
- [Results and awaiting a run](results.md): the value a run returns.
- [Retries](retries.md): what happens after a heartbeat timeout.
- [Workflows and steps › Timeouts](../workflows/steps.md#timeouts): `Step.Timeout`.
- Examples: [examples/results-progress](../../examples/results-progress/), [examples/snooze-heartbeat](../../examples/snooze-heartbeat/).
