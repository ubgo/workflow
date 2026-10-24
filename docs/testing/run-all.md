# Run everything now in tests

A test that enqueues a job delayed until tomorrow, retried after ten minutes or sleeping for a week has two bad options: wait, or drive a fake clock through every timer in the right order. `memory.Client.RunAll` is a third: it ends delays, backoffs, snoozes and sleeps at once and returns when no run can move any further, with a count of how each run ended. It is for tests on the memory driver; production code never calls it.

## Before you start

- The memory driver, started: `RunAll` returns `workflow.ErrNotStarted` before `Start`.
- The example is [`examples/run-all`](../../examples/run-all). It runs on a fake clock that nobody advances, which proves `RunAll` does not need one. Run it with `go run ./run-all` from `examples/`.
- To check what your code enqueues without running anything, use [the Fake](fake.md) instead; to step through time on purpose, [drive a fake clock](fake-clock.md).

## Run everything now

Enqueue as the code under test does, then call `RunAll(ctx)`:

<!-- snippet: examples/run-all/main.go#runall -->
```go
jobs := []workflow.Job{
	{Workflow: "welcome", RunAt: clk.Now().Add(24 * time.Hour)}, // delayed a day
	{Workflow: "trial-ending"},                                  // sleeps a week
	{Workflow: "approve"},                                       // waits for an event
}
for _, job := range jobs {
	if _, err := c.Enqueue(ctx, job); err != nil {
		return err
	}
}
rep, err := c.RunAll(ctx) // returns when no run can move any further
if err != nil {
	return err
}
fmt.Fprintf(out, "succeeded=%d failed=%d waiting=%d sends=%d clock=%s\n",
	rep.Succeeded, rep.Failed, rep.Waiting, sends.Load(), clk.Now().Format("Jan 2 15:04"))
```

```text
succeeded=2 failed=0 waiting=1 sends=2 clock=Oct 14 09:00
```

The welcome email was delayed a day and its first send failed with a ten-minute backoff; the trial reminder slept a week; both succeeded, and the clock still reads 09:00. The approval run is counted as waiting: it waits for an event, which `RunAll` cannot send.

`RunAll` covers the runs unfinished when it is called and every run enqueued while it runs (chained jobs, children, cron fires). Its `memory.RunAllReport` counts them by how they ended: `Succeeded`, `Failed`, `Cancelled`, `Discarded`, and `Waiting` for runs left on a wait step.

## What it hurries and what it does not

| Hurried: ends at once | Not hurried: follows the client's clock |
|---|---|
| A delayed start (`Job.RunAt`) | A step `Timeout` |
| A retry backoff | A wait step's `Deadline` or `Timeout` |
| A `workflow.Snooze` | `Step.HeartbeatTimeout` |
| A sleep step (`Step.Sleep`) | `Workflow.RunTimeout` |

Timeouts are left alone on purpose: hurrying them would make every step with a timeout fail. A test of a timeout drives the clock itself, with `clock.Fake.Advance` ([Drive time in tests](fake-clock.md)). Rate limits and cron ticks are on the clock too.

## Events, and runs nothing can move

`RunAll` cannot send the event a waiting run needs. The test signals it and calls `RunAll` again, which now carries the woken run to its end:

<!-- snippet: examples/run-all/main.go#signal -->
```go
// RunAll cannot send the event a run waits for; the test does, then
// runs everything again.
if err := c.Signal(ctx, workflow.Event{Name: "approved", Key: "order-7"}); err != nil {
	return err
}
rep, err = c.RunAll(ctx)
if err != nil {
	return err
}
fmt.Fprintf(out, "after the signal: succeeded=%d waiting=%d\n", rep.Succeeded, rep.Waiting)
```

When runs are left that nothing `RunAll` hurries can move, it does not hang: after about half a second without progress it returns its report and an error wrapping `memory.ErrRunAllStalled`. A paused workflow or queue does this, as does a rate limit or timeout waiting on a fake clock, or a concurrency slot held by a run that waits for an event:

<!-- snippet: examples/run-all/main.go#stalled -->
```go
if err := c.Pause(ctx, workflow.PauseWorkflow("report")); err != nil {
	return err
}
if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "report"}); err != nil {
	return err
}
rep, err = c.RunAll(ctx) // a paused run never moves: RunAll gives up
fmt.Fprintf(out, "paused: stalled=%t succeeded=%d\n", errors.Is(err, memory.ErrRunAllStalled), rep.Succeeded)
```

```text
after the signal: succeeded=1 waiting=0
paused: stalled=true succeeded=0
```

If `ctx` ends first, `RunAll` returns `ctx`'s error and an empty report.

## Try it

```sh
cd examples
go run ./run-all
```

```text
succeeded=2 failed=0 waiting=1 sends=2 clock=Oct 14 09:00
after the signal: succeeded=1 waiting=0
paused: stalled=true succeeded=0
```

`go test ./run-all` checks every line.

## Pitfalls

- **A waiting run is a normal end for `RunAll`.** It returns with no error and counts it in `Waiting`; assert on `Waiting` when the test expects every run to finish.
- **A stall costs about half a second.** `RunAll` decides a run cannot move after 500 looks a millisecond apart on the wall clock. A test that expects `ErrRunAllStalled` takes that long.
- **It is memory-only.** There is no `RunAll` on durable drivers; tests against them run real workers and wait for run ends.
- **It runs what your steps do.** Steps execute for real, side effects included; it is not a dry run.

## Related

- [Test app code with the Fake](fake.md): record enqueues without running them.
- [Drive time in tests](fake-clock.md): move a fake clock through timeouts and cron ticks.
- [Test one step](run-step.md).
- [The in-memory driver](../drivers/memory.md).
- Example: [examples/run-all](../../examples/run-all/).
