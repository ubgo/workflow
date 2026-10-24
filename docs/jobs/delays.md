# Run a job later

Some work belongs to a time, not to the moment you know about it: a reminder the day before an appointment, a trial that ends in fourteen days, a follow-up email an hour after sign-up. Set `Job.RunAt` and the run waits, as a scheduled run, until that time comes. For work that repeats, use a [cron schedule](../schedules/cron.md) instead.

## Before you start

- A driver with `workflow.CapDelayedStart`. The memory, SQL, Redis and Hatchet drivers all have it; a driver without it refuses a job with `RunAt` set rather than starting it at once.
- The example is [`examples/delayed-jobs`](../../examples/delayed-jobs). It runs on the memory driver with a fake clock, so "tomorrow" comes when the program moves the clock. Run it with `go run ./delayed-jobs` from `examples/`.

## Run a job later

Set `RunAt` to the instant the run should start. Read the time from your clock (the example uses a fake one), never from `time.Now` in code you want to test:

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

Until it starts, the `RunRef` has a `ScheduledID` and no `RunID`, and `Status` reports `StatusScheduled`. Engines such as Hatchet hand out the run's ID only when it fires, which is why the two IDs are separate fields. You never need to swap one for the other: keep passing the `RunRef` you were given, and the driver finds the run whether it has started or not.

<!-- snippet: examples/delayed-jobs/main.go#status -->
```go
st, err = c.Status(ctx, ref) // the RunRef Enqueue returned still works
if err != nil {
	return err
}
fmt.Fprintf(out, "after it ran: %s, terminal %t\n", st, st.IsTerminal())
```

A delayed start can be cancelled before it fires, which is how you call off a reminder when the appointment is cancelled:

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

A `RunAt` that is not in the future starts the run now, with a `RunID` straight away.

## Try it

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

The reminder was enqueued on Monday at 09:00 for 24 hours later, and ran when the program moved the fake clock by a day.

## Pitfalls

- **On the memory driver a delayed run lives in the process.** It is lost when the process exits, like everything else on that driver. Use the SQL driver or Hatchet for a reminder due next week.
- **Hatchet refuses `IdempotencyKey` together with `RunAt`.** The Hatchet driver returns an error matching `workflow.ErrUnsupported` for a job that sets both. To make a delayed job idempotent there, dedup before you enqueue (for example with [unique jobs](../reliability/unique-jobs.md)).
- **Hatchet stores the trigger time without a zone.** The driver sends `RunAt` in UTC so a local time does not fire hours off; pass any `time.Time` and let it convert.
- **A run held back by concurrency or a rate limit starts later than `RunAt`.** `RunAt` is the earliest start, not a promise; the run then waits its turn like any other ([concurrency](concurrency.md)).
- **Do not compute `RunAt` from `time.Now` in tested code.** Take a `clock.Clock` and call `Now()` on it, so a test with `clock.NewFake` can move time ([fake clock](../testing/fake-clock.md)).

## Related

- [Cancel and status](cancel-and-status.md)
- [Cron schedules](../schedules/cron.md) for recurring work
- Example: [`examples/delayed-jobs`](../../examples/delayed-jobs)
