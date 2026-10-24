# Schedule conditions

A schedule's cron says when a tick is due; a condition says whether a due tick should run. Office-hours-only syncs, a quiet hour at lunch, nothing while a deploy runs, a job that only makes sense when a feature flag is on: each is a rule about whether one tick runs. Here they are time-of-day windows, which are data and stored with the schedule, and one `ScheduleWhen` function on the workflow, which is code. A tick a condition stops is not lost silently: it is reported as a skipped tick, with the reason, to [tick observers and schedule hooks](hooks.md).

## Before you start

- Windows need `workflow.CapScheduleWindow` and `ScheduleWhen` needs `workflow.CapScheduleWhen`. The memory, SQL and Redis drivers have both; check `c.Capabilities()` on another driver. A driver without them refuses the field (at `Register` for a workflow, at `Schedule` for a schedule) with an `*workflow.UnsupportedError`, rather than firing ticks the condition should have stopped. Hatchet fires crons on its engine, where no condition of ours can run, so it declares neither `CapScheduleWhen` nor `CapScheduleWindow` and refuses both fields rather than ignoring them.
- The example is [`examples/schedule-hooks`](../../examples/schedule-hooks). It runs an hourly schedule on the memory driver with a fake clock, so a working day passes in a moment. Run it with `go run ./schedule-hooks` from `examples/`.
- Cron basics are on [cron schedules](cron.md).

## Limit ticks to time windows

`Schedule.Windows` limits a schedule's ticks by time of day on the wall clock of its `Location`. A tick fires only inside at least one `Between` window (no `Between` windows: at any time) and outside every `Unless` window. Each `Window` is `From` and `To` as `"HH:MM"` on a 24-hour clock, `From` inclusive and `To` exclusive, so `09:00`-`17:00` lets the 09:00 tick through and stops the 17:00 one. A `To` before `From` wraps midnight: `{From: "22:00", To: "06:00"}` is the night. A window with `From` equal to `To` is refused, since it would mean either nothing or the whole day.

<!-- snippet: examples/schedule-hooks/main.go#windows -->
```go
id, err := c.Schedule(ctx, workflow.Schedule{
	Name:     "office-hours",
	Workflow: "sync-orders",
	Cron:     "0 * * * *", // hourly
	Location: time.UTC,
	Misfire:  workflow.Misfire{Policy: workflow.MisfireSkip}, // after an outage, fire no missed tick
	Windows: workflow.Windows{
		Between: []workflow.Window{{From: "09:00", To: "17:00"}}, // office hours only
		Unless:  []workflow.Window{{From: "12:00", To: "13:00"}}, // not over lunch
	},
})
```

Declared crons take the same windows through `Workflow.CronWindows`, evaluated in `Workflow.CronLocation`. Windows are read on the schedule's own wall clock, so an `09:00`-`17:00` window on a Berlin schedule is Berlin office hours all year, across daylight saving changes. `workflow.Windows.Allows(t, loc)` is the pure function every driver applies, for showing a user whether a time passes.

## Decide in code with ScheduleWhen

`Workflow.ScheduleWhen` is asked before each tick of the workflow's schedules fires, the declared `Cron` and every `Client.Schedule` of the workflow alike. Returning false skips the tick. It gets the `workflow.Tick`, so a rule can differ per schedule by `tick.Name`, by the schedule's labels (a tenant ID) or by `tick.At`.

<!-- snippet: examples/schedule-hooks/main.go#when -->
```go
var deploying atomic.Bool // set by the app while a deploy runs
err := c.Register(workflow.Workflow{
	Name: "sync-orders",
	// Asked before every tick of every schedule of this workflow.
	ScheduleWhen: func(_ context.Context, t workflow.Tick) bool {
		return !deploying.Load()
	},
	Steps: []workflow.Step{{Name: "sync", Run: syncOrders(clk)}},
})
```

The condition is code on the workflow, not a field of the schedule, for a reason: a schedule made with `Client.Schedule` is data a durable driver stores and may fire on any node, and a function cannot be stored. Every node registers the workflow, so every node has the function. It runs on the firing node, on the driver's scheduler goroutine, so it must return quickly; read a flag or a cached setting, do not call a slow service.

## Run only in some environments

`workflow.InEnvironment(current, allowed...)` returns a `ScheduleWhen` that fires ticks only when `current` is one of the allowed names, for schedules that must not run in staging or on a laptop. The names are the app's own: the library has no notion of an environment and reads nothing from the process, so the app passes the value it already has (a config field, an environment variable it reads). With no allowed names it never fires. `workflow.AllWhen(conds...)` combines conditions, asking them in order and stopping at the first false:

<!-- snippet: examples/schedule-specs/main.go#environment -->
```go
when := workflow.AllWhen(
	workflow.InEnvironment(env, "production"), // env is the app's own setting
	func(_ context.Context, t workflow.Tick) bool { return t.At.Weekday() != time.Sunday },
)
```

The ticks it stops are reported as skipped with `TickReasonWhen`, like any `ScheduleWhen`. Not registering the schedule at all outside production works too, and is the only choice on a driver without `CapScheduleWhen`.

## An active period

`Schedule.Active` (or `Workflow.CronActive` for a declared cron) limits a schedule to a period: a campaign that runs for a week, a trial reminder that stops when the trial ends. Ticks `t` with `From <= t < Until` fire; a zero `From` means no start and a zero `Until` no end, and an `Until` at or before `From` is refused. It needs `workflow.CapScheduleActive` (the memory, Redis and SQL drivers declare it; Hatchet refuses).

<!-- snippet: examples/schedule-specs/main.go#active -->
```go
campaign := workflow.Active{
	From:  time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC),
	Until: time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC), // exclusive
}
runs := workflow.NextRuns(workflow.NextRunsQuery{Schedule: s, Location: time.UTC, After: monday, Count: 10, Active: campaign})
```

Ticks outside the period are not ticks at all, unlike a window: there is no skipped-tick event for them and no misfire catch-up across the time before `From`, so a schedule created today with `From` next Monday does not fire the days in between when Monday comes. `workflow.NextActive(schedule, after, loc, active)` is the shared next-tick rule every driver uses, and `NextRuns` applies the period too, so a dashboard lists only the fires that will happen. Once `Until` has passed the schedule stays listed and never fires; `Unschedule` it to remove it.

## Try it

```sh
cd examples
go run ./schedule-hooks
```

The ticks of the office-hours schedule from 08:00 to 13:00:

```text
-- ticks
  08:00 skipped (window)
  09:00 fired, run succeeded
  10:00 skipped (when)
  11:00 fired, run failed
  12:00 skipped (window)
  13:00 fired, run succeeded
```

08:00 is outside the `Between` window, 10:00 is stopped by `ScheduleWhen` while the deploy flag is set, and 12:00 falls in the `Unless` window. `go test ./schedule-hooks` checks every line.

## Pitfalls

- **A skipped tick does not come back.** A condition decides about one tick; when the window opens or the flag clears, the next tick fires, not the ones skipped. The misfire policy is about ticks nobody was awake to fire, not ticks a condition said no to.
- **Windows use the schedule's zone, not the server's.** A schedule with `Location: time.UTC` and a `09:00`-`17:00` window runs 09:00-17:00 UTC wherever the server is.
- **`ScheduleWhen` runs once per tick on one node.** It is not a lock and not a filter on runs enqueued by hand; a run enqueued with `Client.Enqueue` never asks it.
- **A slow `ScheduleWhen` delays the scheduler.** It runs on the driver's scheduler goroutine.
- **An active period is not a window.** Ticks before `From` or from `Until` on are not reported as skipped and are never caught up by a misfire policy; a schedule past its `Until` stays listed until it is removed.
- **`InEnvironment` decides once.** The environment is read when the condition is built, not per tick; build it from the value the process started with.
- **A panicking `ScheduleWhen` skips the tick.** The memory, SQL and Redis drivers recover the panic, log it at Error and report the tick skipped with `TickReasonWhen`, so a bug in the condition never fires work it should have stopped.

## Related

- [Schedule hooks and pings](hooks.md): see each skipped tick and why.
- [Manage schedules](manage.md): list schedules with their next fire, pause one.
- [Cron schedules](cron.md), [Misfire policies](misfire.md), [Time zones](time-zones.md).
- Example: [examples/schedule-hooks](../../examples/schedule-hooks/).
- Example: [examples/schedule-specs](../../examples/schedule-specs/) (active period, environment gating).
