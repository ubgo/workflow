# Backfill and schedule lifetimes

The nightly report did not run for three days while the database was being migrated, and finance wants those three days now: each run must know which day it is for, and running the backfill twice must not produce six reports. Around that sit the other questions about a schedule's life: run this export poll three times and stop, send one reminder at 10:20 and forget the schedule, warm a cache when the app boots but only once per day across ten replicas, skip public holidays, and do not start a thousand tenants' 09:00 jobs in the same second. These helpers answer them on top of the ordinary [cron schedules](cron.md), without new storage.

## Before you start

- `Backfill` needs `workflow.CapIdempotencyKey` (memory, SQL, Redis, Hatchet) and refuses without it unless `BackfillAllowDuplicates()`. With `workflow.CapRunQuery` it also finds runs that already finished.
- `LimitRuns` and `OneShot` set `Schedule.Active`, which needs `workflow.CapScheduleActive` (memory, SQL, Redis). `PruneEndedSchedules` needs `workflow.CapScheduleList` and a client implementing `workflow.ScheduleLister` (memory, SQL, Redis, Hatchet).
- `FireAtStartup` runs on every driver; `StartupOncePer` needs `workflow.CapIdempotencyKey`.
- `SkipExcluded` is a `Workflow.ScheduleWhen`, which needs `workflow.CapScheduleWhen` (memory, SQL, Redis). See [schedule conditions](conditions.md).
- `middleware.FireJitter` needs `workflow.CapSnooze` and `workflow.CapTickTime` (memory, SQL, Redis); `middleware.CheckFireJitter(c)` checks a client.
- The example is [`examples/schedule-tools`](../../examples/schedule-tools). It runs on the memory driver with a fake clock. Run it with `go run ./schedule-tools` from `examples/`.

## Backfill missed ticks

`workflow.BackfillPlan(spec)` lists the ticks of a schedule in `[From, To)`, in the schedule's zone with the usual [DST rules](time-zones.md#daylight-saving-time), minus those outside its `Active` period or `Windows`. `workflow.Backfill(ctx, c, spec)` starts one run per tick, oldest first, and returns their refs:

<!-- snippet: examples/schedule-tools/main.go#backfill -->
```go
spec := workflow.BackfillSpec{
	Schedule: &report, // the schedule as declared: workflow, cron, zone, input, labels
	From:     time.Date(2026, 10, 1, 0, 0, 0, 0, berlin),
	To:       time.Date(2026, 10, 4, 0, 0, 0, 0, berlin), // [From, To): Oct 1, 2 and 3
}
ticks, err := workflow.BackfillPlan(spec) // what would run, nothing enqueued
if err != nil {
	return err
}
refs, err := workflow.Backfill(ctx, c, spec) // one run per tick, keyed by schedule and tick
if err != nil {
	return err
}
```

Set `BackfillSpec.Schedule` to the schedule as declared, or `Workflow`, `Cron`, `Location` (and `Seconds`, `Input`, `Labels`) for a declared `Workflow.Cron` entry or a schedule that is gone. `ScheduleID`, when the schedule is live, goes into each run's `LabelSchedule`. `Parser` reads a [custom dialect](expressions.md). A range holding more than `Max` ticks (default `DefaultBackfillMax`, 1000) is refused whole with `ErrInvalid`, never cut short.

Each run starts now, with the schedule's input, tenant and labels, plus the labels of its tick: `LabelScheduleName`, `LabelTickAt`, `LabelTrigger` set to `LabelTriggerBackfill`, and `LabelRunKey`. The step reads the tick, and so the interval it covers, with `StepInfo.TickAt()`:

<!-- snippet: examples/schedule-tools/main.go#tickat -->
```go
func (r *recorder) step(ctx context.Context, _ []byte) ([]byte, error) {
	info, _ := workflow.StepInfoFrom(ctx)
	day, _ := info.TickAt() // the tick this run is for, not the time it started
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ticks = append(r.ticks, day)
```

A repeated or overlapping backfill starts nothing twice. Each run's idempotency key is derived from the schedule (its ID, else its workflow and name, else its workflow, expression, zone and seconds flag) and the tick. The key folds a second enqueue into a run that is still unfinished; once runs have finished, `Backfill` finds them by `LabelRunKey` on a client with `CapRunQuery`, among the runs the driver still keeps. On an enqueue error it stops and returns the refs so far, so the same call again resumes where it stopped.

## Run a schedule n times, or once

`workflow.LimitRuns(s, n, from, parser)` returns the schedule with an `Active` period holding exactly its first n ticks after `from`: `Active.From` is the first and `Active.Until` lands just after the n-th. `workflow.OneShot(s, at)` returns a schedule that fires once, at `at`; with an empty `Cron` it writes one from `at` on the schedule's wall clock.

<!-- snippet: examples/schedule-tools/main.go#limit -->
```go
s, err := workflow.LimitRuns(workflow.Schedule{Workflow: "poll-export", Cron: "*/15 * * * *", Location: time.UTC},
	3, fc.Now(), nil) // the next three ticks from now, then the Active period ends
if err != nil {
	return err
}
if _, err := c.Schedule(ctx, s); err != nil {
	return err
}
reminder, err := workflow.OneShot(workflow.Schedule{Workflow: "send-reminder", Location: time.UTC},
	start.Add(20*time.Minute)) // fires once, at 10:20
if err != nil {
	return err
}
if _, err := c.Schedule(ctx, reminder); err != nil {
	return err
}
```

The limit is data on the schedule, stored with it, so it is the same on every node after a restart, with no counter. It counts ticks, not runs: a tick skipped by `ScheduleWhen`, a window, a pause, the overlap policy or a misfire still uses one of the n. `OneShot` refuses an `at` its expression does not fire at, including a wall time that DST skips or repeats.

An ended schedule stays listed until it is unscheduled. `workflow.PruneEndedSchedules(ctx, c, now)` unschedules every schedule whose `Active.Until` plus a grace (`MisfireGrace` by default, `PruneGrace(d)` to change it) is at or before `now`, and returns their IDs; declared crons and schedules without an end are left alone. Call it from a daily job or at boot:

<!-- snippet: examples/schedule-tools/main.go#prune -->
```go
pruned, err := workflow.PruneEndedSchedules(ctx, c, fc.Now()) // Active ended + MisfireGrace
if err != nil {
	return err
}
```

## Run at startup, once per day across nodes

`workflow.FireAtStartup(ctx, c, s, opts...)` enqueues one run of the schedule's workflow now, with its input, tenant and labels, `LabelTickAt` set to now and `LabelTrigger` set to `LabelTriggerStartup`. By default every node that calls it starts a run. `StartupOncePer(period, loc)` keys the run by the schedule and `PeriodKey`, so a rollout of ten replicas starts one run per period:

<!-- snippet: examples/schedule-tools/main.go#startup -->
```go
ref, err := workflow.FireAtStartup(ctx, c, warm,
	workflow.StartupOncePer(workflow.PeriodDay, berlin), // one run per Berlin day across the fleet
	workflow.StartupClock(fc))
if err != nil {
	return err
}
```

## Skip holidays and weekends

An `ExclusionCalendar` answers one question, `Excluded(t)`. `ExcludeDates(loc, "2026-12-25", ...)` excludes whole days on a zone's wall clock, `ExcludeWeekdays(loc, days...)` excludes weekdays, `ExcludeFunc` adapts a function (a holiday API you cache, a table you keep), and `AnyExcluded` combines them. `SkipExcluded(cal)` turns a calendar into a `Workflow.ScheduleWhen` that skips every tick whose time (`Tick.At`) is excluded:

<!-- snippet: examples/schedule-tools/main.go#calendar -->
```go
holidays, err := workflow.ExcludeDates(berlin, "2026-12-24", "2026-12-25", "2026-12-26")
if err != nil {
	return err
}
weekends, err := workflow.ExcludeWeekdays(berlin, time.Saturday, time.Sunday)
if err != nil {
	return err
}
wf := workflow.Workflow{
	Name:         "standup-digest",
	Steps:        []workflow.Step{{Name: "send", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}},
	Cron:         []string{"0 9 * * *"},
	CronLocation: berlin,
	ScheduleWhen: workflow.SkipExcluded(workflow.AnyExcluded(holidays, weekends)),
}
```

A skipped tick is reported to [tick observers](hooks.md) as `TickSkipped` with `TickReasonWhen`; an excluded day is not a missed one, so no misfire policy brings it back. Combine a calendar with other conditions through `AllWhen`.

## Spread fires with jitter

`middleware.FireJitter(window)` holds each run a cron tick fired until tick + a jitter in `[0, window)`, drawn from the run ID (`RunIDJitter`), so a thousand tenants' `0 9 * * *` schedules start across the window instead of in one second:

<!-- snippet: examples/schedule-tools/main.go#jitter -->
```go
c := memory.New(memory.WithClock(fc), memory.WithMiddleware(
	middleware.FireJitter(5*time.Minute, middleware.WithFireJitterClock(fc)), // each fired run waits tick + [0, 5m)
))
if err := middleware.CheckFireJitter(c); err != nil { // needs CapSnooze and CapTickTime
	return err
}
```

It keeps no state: a step of a fired run returns `workflow.Snooze` for the time left while the clock is before its deadline. A retry, another node and every step of the run reach the same deadline, later steps pass straight through, and a run that already starts late is not delayed further. Plain enqueues and runs started by hand (`TriggerSchedule`, `Backfill`, `FireAtStartup`) are never held. `WithJitterFunc` replaces the draw; it must be deterministic. `WithFireJitterClock` sets the clock.

## Try it

```sh
cd examples
go run ./schedule-tools
```

The sections this page covers:

```text
calendars:
  Tue Dec 22 fired
  Wed Dec 23 fired
  Thu Dec 24 skipped (when)
  Fri Dec 25 skipped (when)
  Sat Dec 26 skipped (when)
  Sun Dec 27 skipped (when)
  Mon Dec 28 fired
backfill:
  plan: 3 ticks, first Oct 1 02:00 CEST
  run for Oct 1 02:00
  run for Oct 2 02:00
  run for Oct 3 02:00
  started 3; a repeat returns the same runs: true
limited:
  limited: */15 * * * *, active 10:15:00 to 10:45:01
  one-shot: 20 10 12 10 *
  poll-export ran 3 times (last 10:45), send-reminder 1 time
  pruned 2 schedules, 0 left
startup:
  three boots, once per day: 1 run
jitter:
  tick 09:00:00, step started 09:04:36
```

## Pitfalls

- **A repeat finds finished runs only while the driver keeps them.** The idempotency key covers unfinished runs; the `LabelRunKey` lookup covers finished ones until retention removes them (the memory driver keeps finished runs 24 hours by default, the SQL driver until `Prune`). A backfill repeated after that starts the ticks again.
- **Backfill does not see the scheduler's own fires.** A tick the scheduler fired carries no run key; pass a range the scheduler missed.
- **Backfill ignores `ScheduleWhen`.** It honours the schedule's `Active` period and `Windows`, which are data; a condition in code runs only at tick time. Filter `BackfillPlan`'s ticks yourself when it matters.
- **`LimitRuns` counts skipped ticks.** Three ticks of which one fell on an excluded day are three, and two runs.
- **A tick exactly at `from` is not counted.** A driver fires only ticks after a schedule exists, so `LimitRuns(s, n, now, nil)` starts at the next tick.
- **Prune after the grace.** Pruning at `Active.Until` itself could remove a schedule before a late scheduler fired its last tick; the default grace is `MisfireGrace`.
- **Once per period is per key, not per success.** With `StartupOncePer`, a node booting the same day after the run finished and was pruned starts it again.
- **FireJitter on a driver without `CapSnooze`** turns each wait into a failed attempt; check with `CheckFireJitter`.

## Related

- [Cron expression tools](expressions.md) (`L`, `W`, `#`, `H`, `Describe`)
- [Cron schedules](cron.md#which-tick-a-run-is-for) (the tick label), [misfire policies](misfire.md), [schedule conditions](conditions.md) (an active period, `ScheduleWhen`)
- [Idempotency keys](../jobs/idempotency.md)
- Example: [`examples/schedule-tools`](../../examples/schedule-tools)
