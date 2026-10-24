# snooze-heartbeat

Three ways a step deals with time, on the in-memory driver with a fake clock: a step that snoozes while a report is not ready (it spends no attempt, so a workflow with no retries still succeeds), a step whose first attempt hangs and is failed by its heartbeat timeout while the retry beats and finishes, and a run that goes past its `RunTimeout` and runs `OnTimeout` instead of `OnFailure`. Minutes pass at once and the output never varies.

```sh
cd examples
go run ./snooze-heartbeat
```

```text
snooze:
  try at +0s: attempt 1
  snoozed, status waiting
  try at +1m0s: attempt 1
  snoozed, status waiting
  try at +2m0s: attempt 1
  run succeeded, attempts 1, errors recorded 0
heartbeat:
  attempt 1 started
  attempt 2 started at +31s
  attempt 1 failed at +30s: workflow: heartbeat timed out: memory: step "load" attempt 1 went 30s without a heartbeat
  run succeeded
run timeout:
  run failed: workflow: run timed out: after 5m0s
  ErrRunTimeout true, IsTimeout true, ErrStepTimeout false
  ran OnTimeout with payload "q3"
```

## The code

A step that snoozes until the report is ready:

<!-- snippet: examples/snooze-heartbeat/main.go#snooze -->
```go
fetch := func(ctx context.Context, _ []byte) ([]byte, error) {
	info, _ := workflow.StepInfoFrom(ctx)
	tries <- info.Attempt
	if !reportReady() {
		// Not a failure: put the step back for a minute, same attempt.
		return nil, workflow.Snooze(time.Minute)
	}
	return []byte(`"report.csv"`), nil
}
wf := workflow.Workflow{
	Name:  "download-report",
	Retry: workflow.RetryPolicy{}, // no retries, and a snooze needs none
	Steps: []workflow.Step{{Name: "fetch", Run: fetch}},
}
```

A step that beats as it works, with a heartbeat timeout that catches a hang:

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

A run timeout with its own handler:

<!-- snippet: examples/snooze-heartbeat/main.go#run-timeout -->
```go
wf := workflow.Workflow{
	Name:       "build-report",
	RunTimeout: 5 * time.Minute, // the whole run, waits and retries included
	Steps: []workflow.Step{{Name: "build", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		started <- struct{}{}
		<-ctx.Done() // the cause is ErrRunTimeout
		return nil, context.Cause(ctx)
	}}},
	OnTimeout: func(_ context.Context, in []byte) ([]byte, error) {
		note("OnTimeout with payload " + string(in)) // tell the user the report is late
		return nil, nil
	},
	OnFailure: func(context.Context, []byte) ([]byte, error) {
		note("OnFailure") // any other failure
		return nil, nil
	},
}
```

Telling the timeouts apart:

<!-- snippet: examples/snooze-heartbeat/main.go#timeout-errors -->
```go
runOut := errors.Is(ev.Err, workflow.ErrRunTimeout) // this deadline in particular
anyDeadline := workflow.IsTimeout(ev.Err)           // any of the four
```

`main_test.go` runs the program and compares its whole output with the block above.

## Guides

- [Retries: snooze a step that is not ready](../../docs/jobs/retries.md#snooze-a-step-that-is-not-ready)
- [Progress and heartbeats: catch a hung step](../../docs/jobs/progress.md#catch-a-hung-step)
- [Timeouts: cap a whole run](../../docs/jobs/timeouts.md#cap-a-whole-run)
- [Timeouts: handle a timeout differently from a failure](../../docs/jobs/timeouts.md#handle-a-timeout-differently-from-a-failure)
