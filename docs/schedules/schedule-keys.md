# Idempotency keys on schedules

A schedule's fired runs, a manual `TriggerSchedule` of it and a job enqueued by hand for the same work are, without a key, three runs that know nothing of each other. `Schedule.IdempotencyKey` gives every run the schedule starts, by tick and by trigger, a `Job.IdempotencyKey`, with exactly the semantics of [idempotency keys](../jobs/idempotency.md): while a run of the workflow holding the key is unfinished, a fire starts nothing and answers with that run.

## Before you start

- A driver with `workflow.CapScheduleKey` (and `CapIdempotencyKey`): memory and sqlstore. Redis and Hatchet refuse a schedule with a key at `Schedule` with an `UnsupportedError` naming `Schedule.IdempotencyKey` (`CheckScheduleCaps`).
- The example is [`examples/schedule-keys`](../../examples/schedule-keys). Run it with `go run ./schedule-keys` from `examples/`.

## A fixed key

A template without placeholders is one key for every fire: at most one run of the schedule's work at a time, shared with jobs enqueued by hand under the same key.

<!-- snippet: examples/schedule-keys/main.go#fixed -->
```go
id, err := c.Schedule(ctx, workflow.Schedule{
	Name: "sync-acme", Workflow: "sync", Cron: "0 * * * *", Location: time.UTC,
	IdempotencyKey: "sync:acme", // every fire and every trigger carries this key
})
if err != nil {
	return err
}
first, err := c.TriggerSchedule(ctx, id)
if err != nil {
	return err
}
octx, outcome := workflow.WithEnqueueOutcome(ctx)
again, err := c.TriggerSchedule(octx, id) // the first run is still running
if err != nil {
	return err
}
fmt.Println("second trigger returned the running run:", again == first, "duplicate:", outcome.Duplicate)
byHand, err := c.Enqueue(ctx, workflow.Job{Workflow: "sync", IdempotencyKey: "sync:acme"})
if err != nil {
	return err
}
fmt.Println("a hand enqueue under the same key joined it:", byHand == first)
```

A tick whose key is held is reported as fired (`TickFired`) with the holder as its run, and the schedule's last fire stays on the run that really started. The workflow's `Idempotency` policy applies as for any key: under `IdempotencyOff` the key is ignored.

## What a trigger does while the key is held

`Schedule.OnHeld` decides, with the choice [`UniqueConfig.ReturnExisting`](../reliability/unique-jobs.md#return-the-existing-run) gives enqueue middleware:

| `OnHeld` | `TriggerSchedule` while the key is held |
|---|---|
| `HeldReturnExisting` (the default, also empty) | Returns the holder's `RunRef` and a nil error; a `WithEnqueueOutcome` outcome reports a duplicate |
| `HeldRefuse` | Returns an error wrapping `ErrDuplicate` and starts nothing |

<!-- snippet: examples/schedule-keys/main.go#refuse -->
```go
strict, err := c.Schedule(ctx, workflow.Schedule{
	Name: "sync-acme-strict", Workflow: "sync", Cron: "0 * * * *", Location: time.UTC,
	IdempotencyKey: "sync:acme", OnHeld: workflow.HeldRefuse,
})
if err != nil {
	return err
}
_, err = c.TriggerSchedule(ctx, strict)
fmt.Println("a refusing trigger while the key is held:", errors.Is(err, workflow.ErrDuplicate))
```

Ticks always behave as `HeldReturnExisting`: a tick has no caller to refuse.

## A key per fire or per period

`workflow.ExpandScheduleKey(template, fire, loc)` replaces each placeholder with a value of the fire time (the tick's time, or the driver's clock for a trigger):

| Placeholder | Value | Example |
|---|---|---|
| `{fire}` (`KeyFire`) | The fire time in UTC, RFC 3339 to the second | `2026-11-28T09:00:00Z` |
| `{day}` (`KeyDay`) | `PeriodKey` of the fire time on the schedule's `Location` | `2026-11-28` |
| `{week}` (`KeyWeek`) | The ISO week | `2026-W48` |
| `{month}`, `{quarter}`, `{year}` | `KeyMonth`, `KeyQuarter`, `KeyYear` | `2026-11`, `2026-Q4`, `2026` |

A period key makes a manual trigger on the same day join the day's tick, and lets the app enqueue the same work by hand under the key the tick carries:

<!-- snippet: examples/schedule-keys/main.go#period -->
```go
key, err := workflow.ExpandScheduleKey("digest:"+workflow.KeyDay, time.Date(2026, 11, 28, 9, 0, 0, 0, time.UTC), time.UTC)
if err != nil {
	return err
}
fmt.Println("the key of a daily digest fired at 09:00:", key)
```

An unknown placeholder, an unclosed brace or `OnHeld` without a key is refused by `ValidateSchedule` with `ErrInvalid`.

## Pitfalls

- The key dedups only while the holder is unfinished, as every idempotency key does. A daily key does not stop a second run after the first one finished; combine it with [OverlapSkip](overlap.md) or [once per period](../jobs/idempotency.md#once-per-period) logic when that matters.
- Declared crons (`Workflow.Cron`) carry no key; use a named `Schedule` for keyed work.
- With [`Workflow.Coalesce`](../reliability/coalesce.md), a fire while the holder runs keeps one run pending behind it, as an enqueue would.
- On a workflow with [concurrency keys or `ConcurrencyLimits`](../jobs/concurrency.md), a fired run that waits for a slot already holds its key: a trigger or a hand enqueue under the key names that waiting run, which runs once, when it is admitted.

## Related

- [Idempotency keys](../jobs/idempotency.md)
- [Overlap, trigger now and last fire](overlap.md)
- [Coalescing runs](../reliability/coalesce.md)
