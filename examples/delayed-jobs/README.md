# delayed-jobs

Jobs that start later, a run's status, and cancelling a delayed start and a running run. It runs on the in-memory driver with a fake clock that starts on Monday at 09:00 UTC, so "tomorrow" comes when the program moves the clock by a day.

```sh
cd examples
go run ./delayed-jobs
```

```text
enqueued: scheduled id set true, run id set false, status scheduled
cancelled before it started: cancelled
run-finished: cancelled, cancelled true
reminder sent to ada@example.com at Tue 09:00
after it ran: succeeded, terminal true
past RunAt: run id set true
reminder sent to bob@example.com at Tue 09:00
cancelled while running: cancelled
run-finished: cancelled, cancelled true
unknown run: true
```

## The code

Enqueue a run for tomorrow and read its status:

<!-- snippet: examples/delayed-jobs/main.go#run-at -->
```go
ref, err := c.Enqueue(ctx, workflow.Job{
	Workflow: "remind",
	Payload:  []byte(`{"To":"ada@example.com"}`),
	RunAt:    clk.Now().Add(24 * time.Hour), // tomorrow at this time
})
if err != nil {
	return err
}
// Until it starts, the run has a ScheduledID and no RunID.
st, err := c.Status(ctx, ref)
if err != nil {
	return err
}
fmt.Fprintf(out, "enqueued: scheduled id set %t, run id set %t, status %s\n",
	!ref.ScheduledID.IsZero(), !ref.RunID.IsZero(), st)
```

Cancel a delayed start before it fires (twice: Cancel is idempotent):

<!-- snippet: examples/delayed-jobs/main.go#cancel-delayed -->
```go
later, err := c.Enqueue(ctx, workflow.Job{Workflow: "remind", RunAt: clk.Now().Add(time.Hour)})
if err != nil {
	return err
}
if err := c.Cancel(ctx, later); err != nil {
	return err
}
if err := c.Cancel(ctx, later); err != nil { // cancelling twice is not an error
	return err
}
if st, err = c.Status(ctx, later); err != nil {
	return err
}
fmt.Fprintln(out, "cancelled before it started:", st)
```

Read the status with the RunRef Enqueue returned, after the run fired:

<!-- snippet: examples/delayed-jobs/main.go#status -->
```go
st, err = c.Status(ctx, ref) // the RunRef Enqueue returned still works
if err != nil {
	return err
}
fmt.Fprintf(out, "after it ran: %s, terminal %t\n", st, st.IsTerminal())
```

Cancel a running run; its step sees its ctx cancelled:

<!-- snippet: examples/delayed-jobs/main.go#cancel-running -->
```go
exp, err := c.Enqueue(ctx, workflow.Job{Workflow: "export"})
if err != nil {
	return err
}
<-running // the step is executing
if err := c.Cancel(ctx, exp); err != nil {
	return err
}
if st, err = c.Status(ctx, exp); err != nil {
	return err
}
fmt.Fprintln(out, "cancelled while running:", st)
```

An unknown run:

<!-- snippet: examples/delayed-jobs/main.go#not-found -->
```go
_, err = c.Status(ctx, workflow.RunRef{RunID: "run-404"})
fmt.Fprintln(out, "unknown run:", errors.Is(err, workflow.ErrNotFound))
```

`main_test.go` runs the program and compares its whole output with the block above.

## Guides

- [Run a job later](../../docs/jobs/delays.md)
- [Cancel and status](../../docs/jobs/cancel-and-status.md)
