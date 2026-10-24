# lifecycle-events

Watches runs while they happen on the memory driver, with a fake clock so every time is fixed: the lifecycle feed (`workflow.LifecycleNotifier`) of an export that fails once, reports progress and succeeds, and of a charge that fails for good; the `*workflow.RunError` the failed run carries, read with `workflow.RunErrorOf`; and `middleware.Stuck` reporting an attempt that hangs past its threshold while it keeps running. Nothing needs to be installed.

```sh
cd examples
go run ./lifecycle-events
```

```text
-- lifecycle
  09:00:00 inserted  export status=queued node=worker-1
  09:00:00 started   export step=rows attempt=1 status=running node=worker-1
  09:00:00 retrying  export step=rows attempt=1 status=retrying node=worker-1 err=database busy
  09:00:00 started   export step=rows attempt=2 status=running node=worker-1
  09:00:00 progress  export step=rows attempt=2 status=running node=worker-1 progress=50% "500 of 1000 rows"
  09:00:00 finished  export status=succeeded node=worker-1
  09:00:00 inserted  charge status=queued node=worker-1
  09:00:00 started   charge step=card attempt=1 status=running node=worker-1
  09:00:00 finished  charge status=failed node=worker-1 err=memory: step "card" failed after 1 attempt(s): card declined
-- run error
  failed: workflow "charge" run run-6 step "card" attempt 1: memory: step "card" failed after 1 attempt(s): card declined
  step=card attempt=1 declined=true
  Error(): memory: step "card" failed after 1 attempt(s): card declined
-- stuck
  report step=build attempt=1 node=worker-1 started=09:00 running=6m0s
  the attempt kept running and ended succeeded
```

## The code

The lifecycle feed, registered before Start:

<!-- snippet: examples/lifecycle-events/main.go#observe -->
```go
events := make(chan workflow.LifecycleEvent, 32)
ln, ok := workflow.Client(c).(workflow.LifecycleNotifier)
if !ok {
	return errors.New("this driver has no lifecycle feed")
}
// Register before Start. The observer runs on a driver goroutine:
// return quickly, hand anything slow to a channel.
if err := ln.OnLifecycle(func(_ context.Context, ev workflow.LifecycleEvent) {
	events <- ev
}); err != nil {
	return err
}
```

The failed run's RunError, read in a run-finished observer:

<!-- snippet: examples/lifecycle-events/main.go#runerror -->
```go
failures := make(chan string, 1)
if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) {
	if ev.Status != workflow.StatusFailed {
		return
	}
	re, ok := workflow.RunErrorOf(ev.Err) // which run, workflow, step and attempt failed
	if !ok {
		failures <- "failed: " + ev.Err.Error()
		return
	}
	failures <- fmt.Sprintf("failed: %s\n  step=%s attempt=%d declined=%t\n  Error(): %s",
		re.Describe(), re.Step, re.Attempt, errors.Is(ev.Err, errDeclined), ev.Err)
}); err != nil {
	return err
}
```

Stuck, wrapped around every step of the client:

<!-- snippet: examples/lifecycle-events/main.go#stuck -->
```go
mw := middleware.Stuck(middleware.StuckConfig{
	// After decides for every step; without it, Default applies to all.
	After: func(info workflow.StepInfo) time.Duration {
		if info.Workflow == "nightly-import" {
			return 2 * time.Hour // a step known to be slow
		}
		return 5 * time.Minute // zero or less would mean: never report
	},
	OnStuck: func(_ context.Context, ev middleware.StuckEvent) {
		// Report only: the attempt keeps running. Page someone, log,
		// or count it; a Timeout is what ends an attempt.
		reports <- ev
	},
	Clock: watch,
})
c := memory.New(memory.WithMiddleware(mw), memory.WithNodeID("worker-1"))
```

`main_test.go` runs the program and compares its whole output with the block above.

## Guides

- [Lifecycle events](../../docs/observability/lifecycle.md)
- [Stuck jobs](../../docs/observability/stuck-jobs.md)
- [Errors › Run errors](../../docs/concepts/errors.md#run-errors)
