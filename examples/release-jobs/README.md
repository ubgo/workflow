# release-jobs

Two queue features as stock middleware: `middleware.Release` puts a step back for a delay without running it while a condition holds, and `middleware.UniqueUntilStarted` holds a unique job's lock while the run waits and frees it once a step starts. Both run on the in-memory driver with a fake clock, so nothing needs to be installed and the output never varies.

```sh
cd examples
go run ./release-jobs
```

```text
-- release while the API is in maintenance
attempt 1: released for 10m0s
attempt 2: released for 10m0s
maintenance over at 09:10
attempt 3: ran
run: succeeded at 09:20
attempt 1: released for 10m0s
attempt 2: released for 10m0s
attempt 3: released for 10m0s
attempt 4: released for 10m0s
run: failed at 09:50
-- unique until a step starts
first, runs in 5 minutes: accepted
second, while the first waits: refused (ErrDuplicate)
third, while the first runs: accepted
```

## The code

Release a step while the partner API is in maintenance; the policy's `DelayFor` honours the release's delay, and each release counts as an attempt:

<!-- snippet: examples/release-jobs/main.go#release -->
```go
var maintenance atomic.Bool // set by the app while the partner API is down
inMaintenance := func(context.Context, []byte) bool { return maintenance.Load() }

c := memory.New(memory.WithClock(clk), memory.WithMiddleware(
	attempts(out), // outermost, so it sees each release as a failed attempt
	middleware.Release(inMaintenance, 10*time.Minute),
))
err := c.Register(workflow.Workflow{
	Name: "sync",
	Retry: workflow.RetryPolicy{
		Max:      3, // a release is an attempt: allow a few
		Backoff:  workflow.Backoff{Initial: time.Hour},
		DelayFor: workflow.RetryAfterDelay, // honour the release's 10 minutes, not the hour
	},
	Steps: []workflow.Step{{Name: "push", Run: func(context.Context, []byte) ([]byte, error) {
		return nil, nil
	}}},
})
```

A run whose condition never clears spends its retries and fails:

<!-- snippet: examples/release-jobs/main.go#release-runs-out -->
```go
maintenance.Store(true)
ref, err = c.Enqueue(ctx, workflow.Job{Workflow: "sync"})
if err != nil {
	return err
}
for range 3 { // three retries, each released again
	if err := waitTimer(clk); err != nil {
		return err
	}
	clk.Advance(10 * time.Minute)
}
st, err = waitDone(ctx, c, ref)
if err != nil {
	return err
}
fmt.Fprintf(out, "run: %s at %s\n", st, clk.Now().Format("15:04"))
```

Unique until a step starts: the enqueue half wraps the client, the step half goes on the worker:

<!-- snippet: examples/release-jobs/main.go#until-started -->
```go
byReport := func(j workflow.Job) string { return "report:" + j.Labels["account"] }
store := middleware.NewMemoryUnique(clk) // a shared SQL store across processes
uniqueEnqueue, uniqueStep := middleware.UniqueUntilStarted(byReport, store, time.Hour)

mem := memory.New(memory.WithClock(clk), memory.WithMiddleware(uniqueStep)) // the worker's half
c := middleware.WrapClient(mem, uniqueEnqueue)                              // the enqueuer's half
```

<!-- snippet: examples/release-jobs/main.go#until-started-run -->
```go
job := workflow.Job{Workflow: "report", Labels: map[string]string{"account": "a-1"}, RunAt: clk.Now().Add(5 * time.Minute)}
_, err = c.Enqueue(ctx, job)
verdict("first, runs in 5 minutes", err)
_, err = c.Enqueue(ctx, job)
verdict("second, while the first waits", err)

clk.Advance(5 * time.Minute)
<-started // the first run's step is running now
job.RunAt = time.Time{}
_, err = c.Enqueue(ctx, job)
verdict("third, while the first runs", err)
```

`main_test.go` runs the program and compares its whole output with the block above.

## Guides

- [Step middleware: release a step for later](../../docs/middleware/step-middleware.md#release-a-step-for-later)
- [Enqueue middleware: unique until a step starts](../../docs/middleware/enqueue-middleware.md#unique-until-a-step-starts)
