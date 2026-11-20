# schedule-tools

The helpers around a schedule, on the in-memory driver with a fake clock: the extended `L`, `W` and `#` day fields (`cron.ParseExtended`, `cron.Extended` on a driver), hashed spread with `cron.Hash`, English descriptions with `cron.Describe`, holidays and weekends skipped with `ExcludeDates`, `ExcludeWeekdays` and `SkipExcluded`, three missed days replayed with `Backfill` (twice, starting nothing new), a schedule limited to three ticks with `LimitRuns` and a one-shot with `OneShot`, both removed by `PruneEndedSchedules`, a startup run once per day with `FireAtStartup` and `StartupOncePer`, and a fired run held by `middleware.FireJitter`.

```sh
cd examples
go run ./schedule-tools
```

```text
extended:
  0 9 L * *      [Sat Oct 31 09:00 Mon Nov 30 09:00 Thu Dec 31 09:00]
  0 9 15W * *    [Thu Oct 15 09:00 Mon Nov 16 09:00 Tue Dec 15 09:00]
  0 18 * * FRIL  [Fri Oct 30 18:00 Fri Nov 27 18:00 Fri Dec 25 18:00]
  0 9 * * MON#1  [Mon Nov 2 09:00 Mon Dec 7 09:00 Mon Jan 4 09:00]
  memory with cron.Extended fired for Oct 31 09:00; the default parser refuses L: true
hashed:
  acme     42 3 * * *
  globex   7 4 * * *
  initech  16 1 * * *
describe:
  0 9 * * MON-FRI  At 09:00, Monday through Friday
  */15 * * * *     Every 15 minutes
  5 * * * *        At minute 5 past every hour
  0 0 L * *        At 00:00, on the last day of the month
  30 8 * * MON#1   At 08:30, on the first Monday of the month
  @every 1h30m     Every 1 hour and 30 minutes
  */10 * * * * *   Every 10 seconds
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

## The code

The extended day fields, and a driver that reads them:

<!-- snippet: examples/schedule-tools/main.go#extended -->
```go
s, err := cron.ParseExtended(expr) // L, L-n, nW, LW, d#n and dL on top of the five fields
if err != nil {
	return err
}
next := s.Between(monday, monday.AddDate(0, 3, 0), time.UTC, 3)
```

<!-- snippet: examples/schedule-tools/main.go#extended-driver -->
```go
c, fc, err := fakeClient(ctx, monday, []workflow.Workflow{rec.workflow("month-end")},
	memory.WithCronParser(cron.Extended)) // opt-in: this driver now reads L, W and #
if err != nil {
	return err
}
defer c.Stop()
_, err = c.Schedule(ctx, workflow.Schedule{Name: "month-end", Workflow: "month-end", Cron: "0 9 L * *", Location: time.UTC})
```

One expression spread per tenant, and its description:

<!-- snippet: examples/schedule-tools/main.go#hash -->
```go
expr, err := cron.Hash("H H(0-5) * * *", tenant+"/nightly-export") // once a night, 00:00-05:59
if err != nil {
	return err
}
```

<!-- snippet: examples/schedule-tools/main.go#describe -->
```go
text, err := cron.Describe(expr) // "0 9 * * MON-FRI" is "At 09:00, Monday through Friday"
if err != nil {
	return err
}
```

A daily schedule that skips holidays and weekends:

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

Three missed days replayed; each run reads the day it is for:

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

<!-- snippet: examples/schedule-tools/main.go#tickat -->
```go
func (r *recorder) step(ctx context.Context, _ []byte) ([]byte, error) {
	info, _ := workflow.StepInfoFrom(ctx)
	day, _ := info.TickAt() // the tick this run is for, not the time it started
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ticks = append(r.ticks, day)
```

A schedule limited to three ticks, a one-shot, and both pruned once ended:

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

<!-- snippet: examples/schedule-tools/main.go#prune -->
```go
pruned, err := workflow.PruneEndedSchedules(ctx, c, fc.Now()) // Active ended + MisfireGrace
if err != nil {
	return err
}
```

A run at boot, once per day across nodes, and jitter on fired runs:

<!-- snippet: examples/schedule-tools/main.go#startup -->
```go
ref, err := workflow.FireAtStartup(ctx, c, warm,
	workflow.StartupOncePer(workflow.PeriodDay, berlin), // one run per Berlin day across the fleet
	workflow.StartupClock(fc))
if err != nil {
	return err
}
```

<!-- snippet: examples/schedule-tools/main.go#jitter -->
```go
c := memory.New(memory.WithClock(fc), memory.WithMiddleware(
	middleware.FireJitter(5*time.Minute, middleware.WithFireJitterClock(fc)), // each fired run waits tick + [0, 5m)
))
if err := middleware.CheckFireJitter(c); err != nil { // needs CapSnooze and CapTickTime
	return err
}
```

The guides that copy this program are [cron expression tools](../../docs/schedules/expressions.md) and [backfill and schedule lifetimes](../../docs/schedules/backfill.md). `go test ./schedule-tools` checks every line.
