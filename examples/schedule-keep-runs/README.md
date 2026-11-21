# schedule-keep-runs

The parts of a schedule that live between its runs: the tick each run stands for, a cursor that hands each run the rows after the previous run's, and a cap on how many finished runs a schedule keeps. An hourly import in Europe/Berlin runs on time at 09:00 and 10:00; then the process is down from 10:30 to 13:30, and `MisfireFireAll` fires the three missed ticks at once. Each catch-up run still imports its own hour (it reads `StepInfo.TickAt`, not the clock) and the next three rows (it moves a cursor in the schedule's `DataStore` folder with `UpdateData`). At the end `Schedule.KeepRuns` and `Workflow.CronKeepRuns` have left only the newest two finished runs of each schedule. The binary embeds the zone database with `cron/tzdata`. It runs on the in-memory driver with a fake clock and one worker, so the output never varies.

```sh
cd examples
go run ./schedule-keep-runs
```

```text
-- driver
  memory has ScheduleKeep|TickTime: true
-- hourly import, three rows a run
  tick 09:00, run at 09:00: orders 08:00-09:00, rows 1-3
  tick 10:00, run at 10:00: orders 09:00-10:00, rows 4-6
-- after a three-hour outage
  tick 11:00, run at 13:30: orders 10:00-11:00, rows 7-9
  tick 12:00, run at 13:30: orders 11:00-12:00, rows 10-12
  tick 13:00, run at 13:30: orders 12:00-13:00, rows 13-15
-- run history
  import-orders (KeepRuns 2): 2 runs kept, ticks 12:00 13:00
  rollup (CronKeepRuns 2): 2 runs kept, ticks 12:00 13:00
```

## The code

A run-time schedule that keeps its newest two finished runs:

<!-- snippet: examples/schedule-keep-runs/main.go#keep -->
```go
id, err := c.Schedule(ctx, workflow.Schedule{
	Name:     "hourly-import",
	Workflow: "import-orders",
	Cron:     "0 * * * *",
	Location: berlin,
	KeepRuns: keep, // only the newest 2 finished runs stay in the run store
	Misfire:  workflow.Misfire{Policy: workflow.MisfireFireAll, Max: catchUp},
})
```

The step's period, from the tick the run stands for:

<!-- snippet: examples/schedule-keep-runs/main.go#period -->
```go
func period(ctx context.Context) (from, to time.Time, ok bool) {
	info, _ := workflow.StepInfoFrom(ctx)
	tick, ok := info.TickAt() // the tick the schedule fired this run for, in UTC
	if !ok {
		return time.Time{}, time.Time{}, false // enqueued by hand: no tick
	}
	return tick.Add(-time.Hour), tick, true
}
```

The cursor in the schedule's own folder:

<!-- snippet: examples/schedule-keep-runs/main.go#cursor -->
```go
info, _ := workflow.StepInfoFrom(ctx)
folder := workflow.Folder("schedule/" + info.Labels[workflow.LabelSchedule]) // this schedule's own folder
var last int64
next, err := workflow.UpdateData(ctx, folder, cursorName, func(cur int64, _ bool) (int64, error) {
	last = cur // set again on every attempt: UpdateData retries on a lost race
	return min(cur+batchSize, newestRow), nil
})
if err != nil {
	return nil, err
}
// Rows last+1..next are this run's alone: the compare-and-set gave them
// to no other run. Import them here, outside the function UpdateData
// may call more than once.
```

A unit test of the step, with the labels a schedule puts on a fired run:

<!-- snippet: examples/schedule-keep-runs/main_test.go#test -->
```go
tick := time.Date(2026, 1, 5, 12, 0, 0, 0, time.UTC)
labels := workflow.FiredLabelsAt(nil, "sched-1", "hourly-import", tick) // what the driver sets
ctx := workflow.WithStepInfo(context.Background(), workflow.StepInfo{Labels: labels})
from, to, ok := period(ctx)
if !ok || !from.Equal(tick.Add(-time.Hour)) || !to.Equal(tick) {
	t.Fatalf("period = %v, %v, %v; want the hour ending at %v", from, to, ok, tick)
}
```

`go test ./schedule-keep-runs` checks every line of the output. The guides are [Manage schedules](../../docs/schedules/manage.md), [Cron schedules › Which tick a run is for](../../docs/schedules/cron.md#which-tick-a-run-is-for) and [Time zones](../../docs/schedules/time-zones.md#embed-the-zone-database).
