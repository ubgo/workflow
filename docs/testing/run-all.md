# Run everything now in tests

A test that enqueues a job delayed until tomorrow, retried after ten minutes or sleeping for a week has two bad options: wait, or drive a fake clock through every timer in the right order. [`memory.Client.RunAll`](ds:block?id=memory-client-runall-zdkrjb96) is a third: it ends delays, backoffs, snoozes and sleeps at once and returns when no run can move any further, with a count of how each run ended. It is for tests on the memory driver; production code never calls it.

## Before you start

- The memory driver, started: [`RunAll`](ds:block?id=memory-client-runall-zdkrjb96) returns `workflow.ErrNotStarted` before `Start` ([the client's usable check](ds:block?id=memory-client-usablelocked-ky48yta5), which also refuses a stopped or draining client).
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
refs := map[string]workflow.RunRef{}
for _, job := range jobs {
	ref, err := c.Enqueue(ctx, job)
	if err != nil {
		return err
	}
	refs[job.Workflow] = ref
}
// The counts are exact here: no run can end before RunAll is called,
// since each is delayed, asleep or waiting for an event.
rep, err := c.RunAll(ctx) // returns when no run can move any further
if err != nil {
	return err
}
fmt.Printf("succeeded=%d failed=%d waiting=%d sends=%d clock=%s\n",
	rep.Succeeded, rep.Failed, rep.Waiting, sends.Load(), clk.Now().Format("Jan 2 15:04"))
```

```text
succeeded=2 failed=0 waiting=1 sends=2 clock=Oct 14 09:00
```

The welcome email was delayed a day and its first send failed with a ten-minute backoff; the trial reminder slept a week; both succeeded, and the clock still reads 09:00. The approval run is counted as waiting: it waits for an event, which [`RunAll`](ds:block?id=memory-client-runall-zdkrjb96) cannot send.

[`RunAll`](ds:block?id=memory-client-runall-zdkrjb96) covers the runs unfinished when it is called and every run enqueued while it runs (chained jobs, children, cron fires). Its [`memory.RunAllReport`](ds:block?id=memory-runallreport-jfbkhxx2) counts them by how they ended: `Succeeded`, `Failed`, `Cancelled`, `Discarded`, and `Waiting` for runs left on a wait step.

## Count only what the report covers

The client is started, so its workers do not wait for `RunAll`: a run that can move on its own (not delayed, not in a retry backoff, snooze or sleep, not waiting for an event) may end between your `Enqueue`, `Signal` or `Resume` and the call, and a run that ended before the call is in no count of the report. The rule:

- **Exact:** `Waiting`, and the nil error that says no run can move any further. A run still waiting, or still able to move, was unfinished when `RunAll` was called, so the report covers it.
- **Exact only when nothing could end first:** `Succeeded`, `Failed`, `Cancelled` and `Discarded`, when every run you count was delayed, in a backoff, snoozed, asleep or waiting for an event at the moment of the call, as in the first call above (the welcome email is delayed, the reminder asleep, the approval waiting).
- **Short otherwise:** after a `Signal` or `Resume`, or for a run enqueued to start at once, the counts may miss the runs the workers finished first. Do not assert them. Keep the `RunRef` that `Enqueue` returned and check the run's own `Status` once `RunAll` returns, or count from a [run query](../jobs/manage-runs.md#step-1-list-runs) or from your steps' effects, as the next section does after its signal.

## What it hurries and what it does not

| Hurried: ends at once | Not hurried: follows the client's clock |
|---|---|
| A delayed start (`Job.RunAt`) | A step `Timeout` |
| A retry backoff | A wait step's `Deadline` or `Timeout` |
| A `workflow.Snooze` | `Step.HeartbeatTimeout` |
| A sleep step (`Step.Sleep`) | `Workflow.RunTimeout` |

Timeouts are left alone on purpose: hurrying them would make every step with a timeout fail. [`RunAll`](ds:block?id=memory-client-runall-zdkrjb96) hurries only the four kinds on the left, as [`TestRunAllRunsDelayedRetriedSnoozedAndSleepingWorkNow`](ds:block?id=memory-testrunallrunsdelayedretriedsnoozedandsleepingworknow-rjaujjth) shows. A test of a timeout drives the clock itself, with `clock.Fake.Advance` ([Drive time in tests](fake-clock.md)). Rate limits and cron ticks are on the clock too.

## Events, and runs nothing can move

[`RunAll`](ds:block?id=memory-client-runall-zdkrjb96) cannot send the event a waiting run needs. The test signals it and calls `RunAll` again, which now carries the woken run to its end ([`TestRunAllRightAfterSignal`](ds:block?id=memory-testrunallrightaftersignal-g8fg3vdt)). The woken run can move on its own, so a worker may finish it before `RunAll` is called, and the report would then not count it: the test reads the run's status instead ([Count only what the report covers](#count-only-what-the-report-covers)):

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
// The started client's workers may finish the woken run before RunAll
// is called, and the report leaves out a run already finished then:
// read the run's own status, not rep.Succeeded. Waiting is exact.
st, err := c.Status(ctx, refs["approve"])
if err != nil {
	return err
}
fmt.Printf("after the signal: approve %s, waiting=%d\n", st, rep.Waiting)
```

When runs are left that nothing `RunAll` hurries can move, it does not hang: after about half a second without progress ([500 looks](ds:block?id=memory-runallstall-wwfzs6c2) [a millisecond apart](ds:block?id=memory-runallpoll-pb4sk4m5)) it returns its report and an error wrapping [`memory.ErrRunAllStalled`](ds:block?id=memory-errrunallstalled-gd4qrw79). A paused workflow or queue does this ([`TestRunAllReportsAStall`](ds:block?id=memory-testrunallreportsastall-6b4hvsj9)), as does a rate limit or timeout waiting on a fake clock, or a concurrency slot held by a run that waits for an event:

<!-- snippet: examples/run-all/main.go#stalled -->
```go
if err := c.Pause(ctx, workflow.PauseWorkflow("report")); err != nil {
	return err
}
if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "report"}); err != nil {
	return err
}
rep, err = c.RunAll(ctx) // a paused run never moves: RunAll gives up
fmt.Printf("paused: stalled=%t succeeded=%d\n", errors.Is(err, memory.ErrRunAllStalled), rep.Succeeded)
```

```text
after the signal: approve succeeded, waiting=0
paused: stalled=true succeeded=0
```

If `ctx` ends first, [`RunAll`](ds:block?id=memory-client-runall-zdkrjb96) returns `ctx`'s error and an empty report.

## Try it

```sh
cd examples
go run ./run-all
```

```text
succeeded=2 failed=0 waiting=1 sends=2 clock=Oct 14 09:00
after the signal: approve succeeded, waiting=0
paused: stalled=true succeeded=0
```

`go test ./run-all` checks every line.

## Pitfalls

- **A count from the report can come out short.** On a started client a run that needs nothing `RunAll` hurries can end before the call and is then not counted ([Count only what the report covers](#count-only-what-the-report-covers)). Assert a run's `Status`, not `Succeeded`, after a `Signal`, a `Resume` or an enqueue that starts at once.
- **A waiting run is a normal end for `RunAll`.** It returns with no error and counts it in [`Waiting`](ds:block?id=memory-runallreport-jfbkhxx2); assert on `Waiting` when the test expects every run to finish.
- **A stall costs about half a second.** `RunAll` decides a run cannot move after [500 looks](ds:block?id=memory-runallstall-wwfzs6c2) [a millisecond apart](ds:block?id=memory-runallpoll-pb4sk4m5) on the wall clock. A test that expects `ErrRunAllStalled` takes that long.
- **It is memory-only.** [`RunAll`](ds:block?id=memory-client-runall-zdkrjb96) is a method of the memory client; there is none on durable drivers; tests against them run real workers and wait for run ends.
- **It runs what your steps do.** Steps execute for real, side effects included ([`RunAll`](ds:block?id=memory-client-runall-zdkrjb96) only ends the waits between them); it is not a dry run.

## Related

- [Test app code with the Fake](fake.md): record enqueues without running them.
- [Drive time in tests](fake-clock.md): move a fake clock through timeouts and cron ticks.
- [Test one step](run-step.md).
- [The in-memory driver](../drivers/memory.md).
- Example: [examples/run-all](../../examples/run-all/).
