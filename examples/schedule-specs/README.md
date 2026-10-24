# schedule-specs

Ways to write and store a schedule beyond a five-field cron string: a structured calendar spec, the opt-in seconds dialect, zones loaded by name, the next runs shown in the schedule's zone and in UTC, an active period, environment gating, a schedule stored as JSON, and a per-node local cron on a fake clock. Everything runs in-process, so nothing needs to be installed and the output never varies.

```sh
cd examples
go run ./schedule-specs
```

```text
calendar:
  0,30 9 * * 1-5
  ErrCalendar=true
seconds:
  00:00:20
  00:00:40
  00:01:00
zones:
  loaded Europe/Berlin
  "" ErrZone=true
  "Local" ErrZone=true
  "Europe/Berln" ErrZone=true
next runs:
  2026-10-24 09:00:00 CEST (07:00:00 UTC)
  2026-10-25 09:00:00 CET (08:00:00 UTC)
  2026-10-26 09:00:00 CET (08:00:00 UTC)
active:
  Wed 14 Oct 09:00
  Thu 15 Oct 09:00
  Fri 16 Oct 09:00
environment:
  production fires=true
  staging fires=false
json:
  {"v":1,"name":"nightly-report","workflow":"report","cron":"0 2 * * *","location":"Europe/Berlin","misfire":{"policy":"skip"}}
  back: nightly-report 0 2 * * * Europe/Berlin
  v2 ErrScheduleJSON=true
local cron:
  ran for 00:01
  ran for 00:02
  ran for 00:03
```

## The code

A structured calendar spec, compiled to the expression a person would write:

<!-- snippet: examples/schedule-specs/main.go#calendar -->
```go
spec := cron.Calendar{
	Hour:      []int{9},
	Minute:    []int{0, 30},
	DayOfWeek: []time.Weekday{time.Monday, time.Tuesday, time.Wednesday, time.Thursday, time.Friday},
}
expr, err := spec.Expr() // "0,30 9 * * 1-5", a plain cron string every driver takes
if err != nil {
	return err
}
```

The six-field dialect with seconds first:

<!-- snippet: examples/schedule-specs/main.go#seconds -->
```go
s, err := cron.ParseSeconds("*/20 * * * * *") // second minute hour day month weekday
if err != nil {
	return err
}
```

A zone loaded by name, so a typo fails at boot:

<!-- snippet: examples/schedule-specs/main.go#zones -->
```go
berlin, err := cron.LoadZone("Europe/Berlin")
if err != nil {
	return err // fails at boot, not as a schedule firing at the wrong hour
}
```

The next runs in the schedule's zone and in UTC, across the end of summer time:

<!-- snippet: examples/schedule-specs/main.go#nextruns -->
```go
s, err := cron.Parse("0 9 * * *")
if err != nil {
	return err
}
runs := workflow.NextRuns(workflow.NextRunsQuery{
	Schedule: s,
	Location: berlin,
	After:    time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC), // the caller's clock; Berlin leaves summer time on the 25th
	Count:    3,
})
for _, r := range runs {
	fmt.Fprintf(out, "  %s\n", r) // "2026-10-24 09:00:00 CEST (07:00:00 UTC)"
}
```

An active period:

<!-- snippet: examples/schedule-specs/main.go#active -->
```go
campaign := workflow.Active{
	From:  time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC),
	Until: time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC), // exclusive
}
runs := workflow.NextRuns(workflow.NextRunsQuery{Schedule: s, Location: time.UTC, After: monday, Count: 10, Active: campaign})
```

Environment gating combined with another condition:

<!-- snippet: examples/schedule-specs/main.go#environment -->
```go
when := workflow.AllWhen(
	workflow.InEnvironment(env, "production"), // env is the app's own setting
	func(_ context.Context, t workflow.Tick) bool { return t.At.Weekday() != time.Sunday },
)
```

A schedule stored as JSON and read back:

<!-- snippet: examples/schedule-specs/main.go#json -->
```go
data, err := json.Marshal(workflow.Schedule{
	Name:     "nightly-report",
	Workflow: "report",
	Cron:     "0 2 * * *",
	Location: berlin, // stored by its IANA name
	Misfire:  workflow.Misfire{Policy: workflow.MisfireSkip},
})
if err != nil {
	return err
}
var back workflow.Schedule
if err := json.Unmarshal(data, &back); err != nil { // refuses another version, unknown fields and zones
	return err
}
```

A local cron, which runs on every node that starts one:

<!-- snippet: examples/schedule-specs/main.go#localcron -->
```go
lc := workflow.NewLocalCron(
	workflow.WithLocalCronClock(clk),
	workflow.WithLocalCronErrors(func(name string, at time.Time, err error) {
		fmt.Fprintf(os.Stderr, "local cron %s at %s: %v\n", name, at, err)
	}),
)
err := lc.Add("warm-cache", "* * * * *", time.UTC, func(ctx context.Context, at time.Time) error {
	ran <- at // at is the tick, not the wall time the run started
	return nil
})
if err != nil {
	return err
}
```

`main_test.go` runs the program and compares its whole output with the block above.

## Guides

- [Cron schedules: a seconds field](../../docs/schedules/cron.md#a-seconds-field)
- [Cron schedules: calendar specs](../../docs/schedules/cron.md#calendar-specs)
- [Time zones: load a zone by name](../../docs/schedules/time-zones.md#load-a-zone-by-name)
- [Time zones: show the next runs in two zones](../../docs/schedules/time-zones.md#show-the-next-runs-in-two-zones)
- [Schedule conditions: an active period](../../docs/schedules/conditions.md#an-active-period)
- [Schedule conditions: run only in some environments](../../docs/schedules/conditions.md#run-only-in-some-environments)
- [Manage schedules: store a schedule as JSON](../../docs/schedules/manage.md#store-a-schedule-as-json)
- [Local cron](../../docs/schedules/local-cron.md)
