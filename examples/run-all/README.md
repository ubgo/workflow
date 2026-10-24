# run-all

`memory.Client.RunAll` in a test-style program: a job delayed by a day, a step whose retry waits ten minutes and a step that sleeps a week all finish in one call, on a fake clock nobody advances; a run waiting for an event is counted as waiting until the program signals it; a paused workflow makes `RunAll` stop with `memory.ErrRunAllStalled`. Nothing needs to be installed.

```sh
cd examples
go run ./run-all
```

```text
succeeded=2 failed=0 waiting=1 sends=2 clock=Oct 14 09:00
after the signal: succeeded=1 waiting=0
paused: stalled=true succeeded=0
```

## The code

Run everything now:

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

A run waiting for an event moves only when the test signals it:

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

A paused workflow makes RunAll give up instead of hanging:

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

`main_test.go` runs the program and compares its whole output with the block above.

## Guides

- [Run everything now in tests](../../docs/testing/run-all.md)
