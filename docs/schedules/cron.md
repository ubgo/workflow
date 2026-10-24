# Cron schedules

Recurring work runs on a cron schedule: a nightly cleanup, a digest at nine every morning, a weekly report per customer. There are two ways to declare one. A cron on the workflow is part of your code and fires for as long as the workflow is registered. A schedule made at run time belongs to your data (one per tenant, per user setting) and is created and removed while the app runs. Both use the same five-field expressions, parsed by the library's own parser, and both fire in an explicit time zone.

## Before you start

- A driver with `workflow.CapCron`. The memory, SQL, Redis and Hatchet drivers have it; Hatchet only in UTC and without macros (see [Pitfalls](#pitfalls)).
- The example is [`examples/cron-schedules`](../../examples/cron-schedules). Its memory parts run on a fake clock, so a day passes when the program moves the clock; its SQL part uses SQLite in a temporary directory. Run it with `go run ./cron-schedules` from `examples/`.
- Time zones and daylight saving time have their own page: [time zones](time-zones.md). What happens to ticks missed while nothing ran: [misfire](misfire.md).

## Cron on a workflow

`Workflow.Cron` lists expressions declared with the workflow. Each fires a run with an empty payload, evaluated in `Workflow.CronLocation` (UTC when nil):

<!-- snippet: examples/cron-schedules/main.go#declared -->
```go
err = c.Register(workflow.Workflow{
	Name:         "digest",
	Cron:         []string{"0 9 * * *"}, // 09:00 every day
	CronLocation: kolkata,               // in Kolkata, not UTC
	Steps: []workflow.Step{{Name: "send", Run: func(context.Context, []byte) ([]byte, error) {
		fired <- clk.Now()
		return nil, nil
	}}},
})
```

Registering the workflow again replaces its crons; it never duplicates them. Nothing fires before `Start`.

## Schedules at run time

`Client.Schedule` creates a recurring trigger for a registered workflow and returns a `workflow.ScheduleID`. Store the ID with the thing it belongs to (a column on the tenant row) and pass it to `Unschedule` when the tenant turns the report off:

<!-- snippet: examples/cron-schedules/main.go#schedule -->
```go
id, err := c.Schedule(ctx, workflow.Schedule{
	Name:     "acme-weekly",
	Workflow: "report",
	Cron:     "0 8 * * MON", // Mondays at 08:00
	Location: berlin,        // required: time.UTC for UTC
	Input:    []byte(`"weekly report"`),
	Labels:   map[string]string{"tenant": "acme"},
})
if err != nil {
	return err
}
// The app stores id (a column on the tenant row) to remove it later.
```

A `Schedule` carries its own `Input` (the payload of each fired run) and `Labels` (copied onto each run). `Location` is required; use `time.UTC` for UTC, so no schedule is ever evaluated in an implicit zone.

<!-- snippet: examples/cron-schedules/main.go#unschedule -->
```go
if err := c.Unschedule(ctx, id); err != nil {
	return err
}
if err := c.Unschedule(ctx, ""); err != nil { // an empty id is a no-op
	return err
}
```

`Unschedule` with an empty ID is a no-op, so you can pass a stored column without checking it, and removing a schedule twice is not an error. An ID the client never issued returns `workflow.ErrNotFound`.

**A named schedule is updated in place.** On every driver, `Schedule` with a `Name` is an upsert by workflow and name: calling it again, on any node or after a reboot, returns the same `ScheduleID` and updates that one row (cron, zone, input, labels, misfire policy) instead of adding a second. An app can therefore declare its schedules at every boot. When the cron and zone are unchanged, the next fire time is kept, so a reboot neither skips nor repeats a tick:

<!-- snippet: examples/cron-schedules/main.go#upsert -->
```go
nightly := workflow.Schedule{Name: "nightly", Workflow: "report", Cron: "0 3 * * *", Location: time.UTC}
first, err := sq.Schedule(ctx, nightly)
if err != nil {
	return err
}
nightly.Cron = "30 3 * * *" // the next boot declares it again, changed
second, err := sq.Schedule(ctx, nightly)
if err != nil {
	return err
}
fmt.Fprintln(out, "  sqlstore, same name twice, same schedule:", first == second)
```

The memory driver upserts the same way (the example prints `true` for it too). On Hatchet a named schedule is one trigger per workflow and name with a stable ID; a changed definition replaces the trigger on the engine behind that same ID. Without a `Name`, every call adds a schedule on every driver.

## Cron syntax

An expression is five fields: minute, hour, day of month, month, day of week. Each field is a comma-separated list of items, and an item is `*`, a value (`5`), or a range (`1-10`), optionally followed by a step (`*/15`, `1-10/2`, `5/20` meaning from 5 to the maximum every 20). Months accept `JAN`-`DEC` and weekdays `SUN`-`SAT`, in any case; Sunday is 0 or 7.

| Expression | Fires |
|---|---|
| `0 9 * * MON-FRI` | 09:00 on weekdays |
| `*/15 * * * *` | every 15 minutes |
| `0 3 1 * *` | 03:00 on the 1st of each month |
| `30 8 * JAN,JUL MON` | 08:30 on Mondays in January and July |

Macros: `@yearly` and `@annually` (`0 0 1 1 *`), `@monthly` (`0 0 1 * *`), `@weekly` (`0 0 * * 0`), `@daily` and `@midnight` (`0 0 * * *`), `@hourly` (`0 * * * *`), and `@every <duration>` for a fixed interval such as `@every 90s` (at least one second; it ignores the wall clock, so DST never moves it).

`cron.Parse` turns an expression into a `cron.Schedule`. It reads no clock: `Next(after, loc)` returns the first fire strictly after `after` in zone `loc`, and `Between(from, to, loc, limit)` lists the fires in `(from, to]`. That makes it the tool for showing a user when their schedule will run:

<!-- snippet: examples/cron-schedules/main.go#parse -->
```go
s, err := cron.Parse("0 9 * * MON-FRI") // 09:00 on weekdays
if err != nil {
	return err
}
ny, err := time.LoadLocation("America/New_York")
if err != nil {
	return err
}
t := monday
for range 3 {
	t = s.Next(t, ny) // strictly after t, evaluated in New York
	fmt.Fprintln(out, " ", t.Format(layout))
}
```

## Day of month and day of week

When both the day of month and the day of week are restricted (neither field starts with `*`), a day matches if **either** matches: `0 0 1 * MON` fires on the 1st of every month and on every Monday. When either field starts with `*` (including `*/2`), both must match. This is the Vixie cron rule, the one most crontabs follow:

<!-- snippet: examples/cron-schedules/main.go#vixie -->
```go
both, _ := cron.Parse("0 0 1 * MON") // day of month AND day of week restricted
fires := both.Between(monday, monday.AddDate(0, 1, 0), time.UTC, 10)
var days []string
for _, f := range fires {
	days = append(days, f.Format("Mon 2 Jan"))
}
fmt.Fprintln(out, "  1st of the month or a Monday:", strings.Join(days, ", "))
```

## Build an expression from a frequency

The `cron` package also writes expressions from named frequencies (daily, every five minutes, on weekdays, at 13:00). A `cron.Builder` emits the same text a person would write, never a new format, so every driver keeps parsing one syntax and the result can be stored, logged and read back. It is pure and imports only the standard library. The program is [examples/cron-builder](../../examples/cron-builder/):

<!-- snippet: examples/cron-builder/main.go#frequencies -->
```go
frequencies := []cron.Builder{
	cron.EverySeconds(30),
	cron.EveryMinutes(15).Weekdays(),
	cron.HourlyAt(17),
	cron.EveryHours(6),
	cron.DailyAt("9:00").Weekdays(),
	cron.TwiceDaily(1, 13),
	cron.WeeklyOn(time.Friday, "17:30"),
	cron.MonthlyOn(15, "8:00"),
	cron.Quarterly(),
	cron.YearlyOn(time.June, 1, "12:00"),
}
for _, f := range frequencies {
	s, err := f.Schedule() // the expression, parsed
	if err != nil {
		return err
	}
	next := s.Next(monday, time.UTC)
	fmt.Fprintf(out, "  %-18s next %s\n", s.String(), next.Format("Mon 2 Jan 15:04:05"))
}
```

`Expr()` returns the expression, checked by `cron.Parse`; `Schedule()` parses it for `Next` and `Between`; `MustExpr()` panics on an error and is meant for frequencies written as constants in the program, such as a `Workflow.Cron` entry:

<!-- snippet: examples/cron-builder/main.go#workflow -->
```go
err := c.Register(workflow.Workflow{
	Name:         "standup-reminder",
	Cron:         []string{cron.DailyAt("9:30").Weekdays().MustExpr()}, // "30 9 * * 1-5"
	CronLocation: time.UTC,
	Steps: []workflow.Step{{Name: "remind", Run: func(context.Context, []byte) ([]byte, error) {
		fired <- clk.Now()
		return nil, nil
	}}},
})
```

Every frequency and constraint, and the expression it writes:

| Builder | Expression | Note |
|---|---|---|
| `EverySeconds(30)` | `@every 30s` | n divides 60 |
| `EveryMinute()` | `* * * * *` | |
| `EveryMinutes(5)` | `*/5 * * * *` | n divides 60 |
| `Hourly()` | `0 * * * *` | |
| `HourlyAt(17)` | `17 * * * *` | |
| `EveryHours(2)` | `0 */2 * * *` | n divides 24 |
| `Daily()` | `0 0 * * *` | |
| `DailyAt("13:00")` | `0 13 * * *` | |
| `TwiceDaily(1, 13)` | `0 1,13 * * *` | |
| `TwiceDailyAt(1, 13, 15)` | `15 1,13 * * *` | |
| `Weekly()` | `0 0 * * 0` | |
| `WeeklyOn(time.Monday, "8:00")` | `0 8 * * 1` | |
| `Monthly()` | `0 0 1 * *` | |
| `MonthlyOn(4, "15:00")` | `0 15 4 * *` | |
| `TwiceMonthly(1, 16, "13:00")` | `0 13 1,16 * *` | |
| `Quarterly()` | `0 0 1 1,4,7,10 *` | |
| `QuarterlyOn(4, "14:00")` | `0 14 4 1,4,7,10 *` | |
| `Yearly()` | `0 0 1 1 *` | |
| `YearlyOn(time.June, 1, "17:00")` | `0 17 1 6 *` | |
| `.At("13:00")` | sets minute and hour | |
| `.Weekdays()` | day of week `1-5` | |
| `.Weekends()` | day of week `0,6` | |
| `.Days(time.Monday, time.Wednesday)` | day of week `1,3` | |

A `Builder` is an immutable value: each method returns a new one, so a base frequency can be shared and refined. Invalid input never panics in a builder call and never writes a wrong expression: the first error is kept, and `Expr` returns it wrapping `cron.ErrBuilder`. The builder refuses input that would quietly write a wrong schedule:

<!-- snippet: examples/cron-builder/main.go#refusals -->
```go
for _, f := range []cron.Builder{
	cron.EveryMinutes(7),                     // */7 would fire at :56 and :00
	cron.Hourly().At("13:00"),                // At would drop the repetition
	cron.Monthly().Days(time.Monday),         // cron fires when EITHER day matches
	cron.YearlyOn(time.February, 30, "9:00"), // never fires
	cron.DailyAt("9:5"),                      // minutes take two digits
} {
	_, err := f.Expr()
	fmt.Fprintf(out, "  ErrBuilder=%t %v\n", errors.Is(err, cron.ErrBuilder), err)
}
```

- A step that does not divide the period: `*/7` fires at :56 and again at :00, four minutes later. Write such an expression by hand, or use `@every 7m`.
- `At` on an hourly, minute or second frequency, which would replace the repetition with one time of day.
- `Days`, `Weekdays` or `Weekends` on a frequency that also fixes the day of the month (`Monthly`, `Quarterly`, `Yearly` and their `On` forms): under the [Vixie rule](#day-of-month-and-day-of-week) that fires when either day matches, not on Mondays that are the 1st.
- A date the month never has (30 February), which would parse and never fire.
- Constraints on `EverySeconds`, since an `@every` interval ignores the calendar.

The last day of the month has no five-field form and is not offered. Time windows and `ScheduleWhen` decide at tick time whether a fire runs; they are conditions on a schedule, not part of the expression.

## A seconds field

Five fields fire at most once a minute. For work that must run more often on a wall-clock grid (every 10 seconds, at 09:00:30), `cron.ParseSeconds` reads a six-field dialect with a seconds field first: `second minute hour day-of-month month day-of-week`. The other five fields, the macros and `@every` mean exactly what they mean to `cron.Parse`; a macro fires at second 0.

<!-- snippet: examples/schedule-specs/main.go#seconds -->
```go
s, err := cron.ParseSeconds("*/20 * * * * *") // second minute hour day month weekday
if err != nil {
	return err
}
```

A schedule opts in with `Schedule.Seconds` (or `Workflow.CronSeconds` for a declared cron) on a driver with `workflow.CapCronSeconds`, which the memory, SQL, Redis and Hatchet drivers all declare. `workflow.ParseCron(expr, seconds, custom)` is the one rule every driver uses: the seconds flag wins over a driver's custom dialect, because it is part of what the stored schedule means, while a custom parser is one process's choice.

The dialect is opt-in and never guessed from the number of fields: six fields could also mean a year last, which is what some other crontabs use. Windows stay minute-resolution: a `09:00`-`17:00` window lets through every second of 09:00 to 16:59.

## Calendar specs

A `cron.Calendar` writes the fire times as lists of field values instead of a cron string: "hour 9, minutes 0 and 30, Monday to Friday". It compiles to the five-field expression a person would write, never a new format, so every driver takes it and a stored schedule stays plain text:

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

An empty list means every value of that field, except `Minute`, where empty means minute 0, so `cron.Calendar{Hour: []int{9}}` fires once at 09:00 rather than sixty times. Consecutive values are written as ranges. `Calendar.Schedule()` parses the result for `Next` and `Between`. Day matching is AND, the way a calendar reads, so setting both `DayOfMonth` and `DayOfWeek` is refused with an error wrapping `cron.ErrCalendar`: cron would fire on either day (the [Vixie rule](#day-of-month-and-day-of-week)), not on Fridays that are the 13th. Use one and check the other in the job. For named frequencies ("daily at 9:00 on weekdays") the [builder](#build-an-expression-from-a-frequency) reads better; a calendar suits specs that come from data, such as a form with a checkbox per weekday.

## Parse errors

Every parse error is a `*cron.ParseError` naming the field (`minute`, `hour`, `day-of-month`, `month`, `day-of-week`, `macro` or `expression`), the offending text and the reason, so a typo fails when the workflow is registered or the schedule created, not silently at the first fire. Drivers wrap it in an error matching `workflow.ErrInvalid`, and `errors.As` still reaches it:

<!-- snippet: examples/cron-schedules/main.go#parse-error -->
```go
_, err = cron.Parse("0 25 * * *")
if pe, ok := errors.AsType[*cron.ParseError](err); ok {
	fmt.Fprintf(out, "  field %s, value %q: %v\n", pe.Field, pe.Value, err)
}
```

## Use another cron dialect

`cron.Parse` is what every driver uses by default, which is what makes one expression mean the same on every driver. If you need another dialect (the extended `L`, `W` and `#` fields, `H` hashing; seconds have [their own flag](#a-seconds-field)), write a `cron.Parser` and pass it to the memory driver (`memory.WithCronParser`), the SQL driver (`sqlstore.WithCronParser`) and the test fake (`workflowtest.FakeCronParser`). The example adds one macro and hands everything else to the core parser:

<!-- snippet: examples/cron-schedules/main.go#dialect -->
```go
withQuarter := func(expr string) (cron.Schedule, error) {
	if expr == "@quarter-hour" {
		expr = "*/15 * * * *"
	}
	return cron.Parse(expr)
}
c := memory.New(memory.WithCronParser(withQuarter))
```

A parser must return a `cron.Schedule` whose `Next` never returns a time at or before its argument and treats a nil zone as UTC, and must be safe for concurrent use. A parser that returns neither a schedule nor an error is refused. On the SQL driver every client of one database must use the same parser, since each one fires stored schedules. Hatchet parses expressions on the engine and has no such option.

## One fire per tick across nodes

On the SQL driver, schedules are rows with their next fire time, and every client registered for the workflow competes to fire them. A tick is fired by moving the schedule's next fire time on from the value that client read and inserting a (schedule, fire time) row, in one transaction: the client that wins enqueues the run, any other fails and enqueues nothing. A tick therefore runs once across any number of processes sharing the database, with no leader election. The memory driver is one process, so the question does not arise; on Hatchet the engine fires crons.

## Which tick a run is for

A run that a schedule fired knows the tick it stands for. Drivers with `workflow.CapTickTime` (the memory, SQL, Redis and Hatchet drivers) put it on every fired run as the reserved label `workflow.LabelTickAt`, in UTC, and a step reads it with `StepInfo.TickAt()` (or `workflow.TickTime(labels)` from a run's labels). Use it instead of the clock when the job is about a period: a daily report that starts late, or a misfire catch-up firing three missed days in a row, still knows which day each run is for. A manual trigger (`ScheduleTriggerer`) carries the driver's clock at the trigger. A run enqueued by hand has no tick, and `TickAt` reports false.

## Try it

```sh
cd examples
go run ./cron-schedules
```

The parts this page covers:

```text
syntax:
  Mon 5 Jan 09:00 EST
  Tue 6 Jan 09:00 EST
  Wed 7 Jan 09:00 EST
  1st of the month or a Monday: Mon 12 Jan, Mon 19 Jan, Mon 26 Jan, Sun 1 Feb, Mon 2 Feb
  field hour, value "25": cron: invalid hour "25" in "0 25 * * *": value 25 out of range 0-23
declared cron:
  fired Mon 5 Jan 09:00 IST (03:30 UTC)
  fired Tue 6 Jan 09:00 IST (03:30 UTC)
schedule at run time:
  "weekly report" for acme at Mon 5 Jan 08:00 CET
  after Unschedule: two Mondays passed, no run
upsert by name:
  sqlstore, same name twice, same schedule: true
  memory, same name twice, same schedule: true
another dialect:
  memory with the dialect: <nil>
  memory with cron.Parse: workflow: invalid spec: workflow "poll": Cron[0]: cron: invalid macro "@quarter-hour" in "@quarter-hour": unknown macro
```

The builder's program:

```sh
cd examples
go run ./cron-builder
```

```text
frequencies:
  @every 30s         next Mon 5 Jan 00:00:30
  */15 * * * 1-5     next Mon 5 Jan 00:15:00
  17 * * * *         next Mon 5 Jan 00:17:00
  0 */6 * * *        next Mon 5 Jan 06:00:00
  0 9 * * 1-5        next Mon 5 Jan 09:00:00
  0 1,13 * * *       next Mon 5 Jan 01:00:00
  30 17 * * 5        next Fri 9 Jan 17:30:00
  0 8 15 * *         next Thu 15 Jan 08:00:00
  0 0 1 1,4,7,10 *   next Wed 1 Apr 00:00:00
  0 12 1 6 *         next Mon 1 Jun 12:00:00
refusals:
  ErrBuilder=true cron: invalid builder: EveryMinutes(7): n must divide 60 (1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30)
  ErrBuilder=true cron: invalid builder: At("13:00"): only a daily or coarser frequency has a time of day to set
  ErrBuilder=true cron: invalid builder: Days(): the frequency also fixes the day of the month, and cron would fire when either day matches
  ErrBuilder=true cron: invalid builder: YearlyOn(February, 30): day must be 1-29
  ErrBuilder=true cron: invalid builder: At("9:5"): want "HH:MM" on a 24-hour clock, e.g. "13:00" or "9:05"
on a workflow:
  Mon 5 Jan 09:30, Tue 6 Jan 09:30, Wed 7 Jan 09:30, Thu 8 Jan 09:30, Fri 9 Jan 09:30, Mon 12 Jan 09:30
```

## Pitfalls

- **Hatchet takes plain five-field expressions in UTC.** The Hatchet driver refuses macros and `@every`, any `CronLocation` or `Location` other than UTC, and any misfire policy, at `Register` (declared crons) or `Schedule`. The example prints those refusals; see [time zones](time-zones.md#pitfalls) and [misfire](misfire.md#pitfalls).
- **Hatchet does not dedupe schedules by name.** Calling `Schedule` at every boot on Hatchet adds a schedule each time; store the ID and `Unschedule` the old one first.
- **`Schedule` needs `Start` first** and a registered workflow; before `Start` it returns `workflow.ErrNotStarted`, for an unknown workflow `workflow.ErrNotFound`.
- **An expression that never fires is valid.** `0 0 30 2 *` (30 February) parses, and `Next` returns the zero time; the memory driver logs a warning and stops that schedule.
- **`EverySeconds` does not run on Hatchet.** It writes `@every <n>s`, which the memory, SQL and Redis drivers take and Hatchet refuses; every other builder frequency writes five plain fields, which every driver takes.
- **Six fields are never guessed.** A six-field expression without `Seconds` (or `CronSeconds`) is a parse error, not a seconds schedule; a driver without `CapCronSeconds` refuses the flag rather than firing once a minute.
- **A calendar with both day fields is refused.** Cron cannot say "the 13th and a Friday"; pick one field and check the other in the job.
- **Use the tick, not the clock, for the period a run covers.** A catch-up run starts now but is for an earlier tick; read `StepInfo.TickAt()`.
- **A declared cron's run has an empty payload.** Put what the run needs in the workflow itself, or use `Client.Schedule` with `Input`.

## Related

- [Time zones and DST](time-zones.md), [misfire policies](misfire.md)
- [Schedule conditions](conditions.md) (windows, `ScheduleWhen`), [schedule hooks and pings](hooks.md), [manage schedules](manage.md) (list, pause)
- Example: [examples/cron-builder](../../examples/cron-builder/) (the frequency builder)
- Example: [examples/schedule-specs](../../examples/schedule-specs/) (calendar specs, seconds, zones, next runs, JSON, local cron)
- [Local cron](local-cron.md) for work that runs on every node
- [Run a job later](../jobs/delays.md) for a one-off delayed run
- [The test fake](../testing/fake.md) (`AssertScheduled`)
- Example: [`examples/cron-schedules`](../../examples/cron-schedules)
