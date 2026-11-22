# Run a job later

Some work belongs to a time, not to the moment you know about it: a reminder the day before an appointment, a trial that ends in fourteen days, a follow-up email an hour after sign-up. Set [`Job.RunAt`](ds:block?id=job-j5g728nc) and the run waits, as a scheduled run, until that time comes. For work that repeats, use a [cron schedule](../schedules/cron.md) instead.

## Before you start

- A driver with [`workflow.CapDelayedStart`](ds:block?id=capdelayedstart-qbxsx6hb). The [memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-caps-yu37ef2j), [Redis](ds:block?id=redis-caps-bfgdst9t) and [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z) drivers all have it; a driver without it refuses a job with `RunAt` set rather than starting it at once ([`RequiresJob`](ds:block?id=requiresjob-mrfs7vqd)).
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
fmt.Printf("enqueued: scheduled id set %t, run id set %t, status %s\n",
	!ref.ScheduledID.IsZero(), !ref.RunID.IsZero(), st)
```

Until it starts, the `RunRef` has a `ScheduledID` and no `RunID`, and `Status` reports `StatusScheduled` ([memory `startRunLocked`](ds:block?id=memory-client-startrunlocked-5w6zs86f), conformance [`delayedCases`](ds:block?id=workflowtest-delayedcases-m54xtw3z)). Engines such as Hatchet hand out the run's ID only when it fires, which is why the two IDs are separate fields. You never need to swap one for the other: keep passing the `RunRef` you were given, and the driver finds the run whether it has started or not.

<!-- snippet: examples/delayed-jobs/main.go#status -->
```go
st, err = c.Status(ctx, ref) // the RunRef Enqueue returned still works
if err != nil {
	return err
}
fmt.Printf("after it ran: %s, terminal %t\n", st, st.IsTerminal())
```

A delayed start can be cancelled before it fires ([`cancelCases`](ds:block?id=workflowtest-cancelcases-67m5qgnv)), which is how you call off a reminder when the appointment is cancelled:

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
fmt.Println("cancelled before it started:", st)
```

A `RunAt` that is not in the future starts the run now, with a `RunID` straight away ([`startRunLocked`](ds:block?id=memory-client-startrunlocked-5w6zs86f)).

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

- **On the memory driver a delayed run lives in the process.** It is lost when the process exits, like everything else on that driver ([`startRunLocked`](ds:block?id=memory-client-startrunlocked-5w6zs86f) arms an in-process timer). Use the SQL or Redis driver or Hatchet for a reminder due next week.
- **On Hatchet a delayed job with an `IdempotencyKey` starts at once and waits in a start gate.** Hatchet's scheduled runs take no key, so the driver starts the run at once on a hidden copy of the workflow (the engine holds the key from the `Enqueue` until `RunAt`) whose start gate ([`startGateFunc`](ds:block?id=hatchet-startgatefunc-u8ju3mwr)) sleeps until `RunAt`, about a second late at most. It reads `StatusScheduled` meanwhile and its `RunRef` holds the `RunID` ([`Enqueue`](ds:block?id=hatchet-client-enqueue-ym8nfcz9)). It is refused on a workflow with a `Concurrency`, with a `RunAt` further ahead than `Config.IdempotencyMaxHold`, and on a client that did not register the workflow ([`checkDelayedKey`](ds:block?id=hatchet-checkdelayedkey-8s6en9y6)), and it cannot be rescheduled (cancel it and enqueue again: [`Reschedule`](ds:block?id=hatchet-errreschedulegated-9akd6fmr)). See [Hatchet limitations](../drivers/hatchet.md#limitations).
- **Hatchet stores the trigger time without a zone.** The driver sends `RunAt` in UTC so a local time does not fire hours off ([`enqueue`](ds:block?id=hatchet-client-enqueue-537mqp9q)); pass any `time.Time` and let it convert.
- **A run held back by concurrency or a rate limit starts later than `RunAt`.** `RunAt` is the earliest start, not a promise; the run then waits its turn like any other (the memory driver's [`admitLocked`](ds:block?id=memory-client-admitlocked-g322u8vq)) ([concurrency](concurrency.md)).
- **Do not compute `RunAt` from `time.Now` in tested code.** Take a `clock.Clock` and call `Now()` on it, so a test with `clock.NewFake` can move time ([fake clock](../testing/fake-clock.md)).

## Related

- [Cancel and status](cancel-and-status.md)
- [Cron schedules](../schedules/cron.md) for recurring work
- Example: [`examples/delayed-jobs`](../../examples/delayed-jobs)
