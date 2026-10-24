# cron-schedules

Recurring work: the cron syntax and its parse errors, daylight saving time, a cron declared on a workflow in a time zone, a schedule created and removed at run time, a schedule upserted by name on the SQL and memory drivers, misfire policies after a pause, another cron dialect, and what a UTC-only engine refuses. The memory parts run on a fake clock; the SQL part uses SQLite in a temporary directory, so nothing needs to be installed; the Hatchet refusals need no server, because the Hatchet client connects only at `Start`.

```sh
cd examples
go run ./cron-schedules
```

```text
syntax:
  Mon 5 Jan 09:00 EST
  Tue 6 Jan 09:00 EST
  Wed 7 Jan 09:00 EST
  1st of the month or a Monday: Mon 12 Jan, Mon 19 Jan, Mon 26 Jan, Sun 1 Feb, Mon 2 Feb
  field hour, value "25": cron: invalid hour "25" in "0 25 * * *": value 25 out of range 0-23
daylight saving time:
  "30 2 * * *" Mar 8 03:00 EDT, Mar 9 02:30 EDT
  "30 * * * *" Mar 8 01:30 EST, Mar 8 03:30 EDT, Mar 8 04:30 EDT
  "30 1 * * *" Nov 1 01:30 EDT, Nov 2 01:30 EST
  "30 * * * *" Nov 1 00:30 EDT, Nov 1 01:30 EDT, Nov 1 01:30 EST
declared cron:
  fired Mon 5 Jan 09:00 IST (03:30 UTC)
  fired Tue 6 Jan 09:00 IST (03:30 UTC)
schedule at run time:
  "weekly report" for acme at Mon 5 Jan 08:00 CET
  after Unschedule: two Mondays passed, no run
upsert by name:
  sqlstore, same name twice, same schedule: true
  memory, same name twice, same schedule: true
misfire after a five-hour pause:
  skip      0 run(s)
  fire-once 1 run(s)
  fire-all  3 run(s)
  Due skip      []
  Due fire-once [05:00]
  Due fire-all  [03:00 04:00 05:00]
another dialect:
  memory with the dialect: <nil>
  memory with cron.Parse: workflow: invalid spec: workflow "poll": Cron[0]: cron: invalid macro "@quarter-hour" in "@quarter-hour": unknown macro
refusals:
  no Location: workflow: invalid spec: schedule for "report" has no Location; use time.UTC for UTC
  time.Local: true
  hatchet zoned: workflow: Workflow.CronLocation unsupported by this driver (missing CronZone): workflow zoned: Hatchet evaluates crons in UTC only, not Asia/Kolkata; shifting the expression would break across DST
  hatchet catch-up: workflow: Workflow.Misfire unsupported by this driver (missing MisfireFireAll): workflow catch-up: Hatchet never catches up a missed cron tick (no backfill) and has no setting for how late a tick may still fire, so only the driver default is honoured: a tick missed while the engine was down is dropped
  hatchet macro: workflow "macro": Cron[0]: workflow: Cron unsupported by this driver: Hatchet takes plain five-field expressions, not "@daily"
  memory needs for a zoned cron: Cron|CronZone | has them: true
```

## The code

Parse an expression and list its next fires in a zone:

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

Day of month and day of week together (the Vixie rule):

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

A parse error names the field:

<!-- snippet: examples/cron-schedules/main.go#parse-error -->
```go
_, err = cron.Parse("0 25 * * *")
if pe, ok := errors.AsType[*cron.ParseError](err); ok {
	fmt.Fprintf(out, "  field %s, value %q: %v\n", pe.Field, pe.Value, err)
}
```

Fixed-time and periodic schedules on New York's DST days:

<!-- snippet: examples/cron-schedules/main.go#dst -->
```go
show := func(expr string, from time.Time, n int) {
	s, _ := cron.Parse(expr)
	var got []string
	for _, f := range s.Between(from, from.Add(48*time.Hour), ny, n) {
		got = append(got, f.In(ny).Format("Jan 2 15:04 MST"))
	}
	fmt.Fprintf(out, "  %-12q %s\n", expr, strings.Join(got, ", "))
}
spring := time.Date(2026, 3, 7, 12, 0, 0, 0, ny)
fall := time.Date(2026, 10, 31, 12, 0, 0, 0, ny)
show("30 2 * * *", spring, 2) // 02:30 does not exist on 8 March
show("30 * * * *", spring.Add(13*time.Hour), 3)
show("30 1 * * *", fall, 2) // 01:30 happens twice on 1 November
show("30 * * * *", fall.Add(12*time.Hour), 3)
```

A cron declared on a workflow, in Kolkata:

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

A schedule per tenant, created at run time:

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

Removing it:

<!-- snippet: examples/cron-schedules/main.go#unschedule -->
```go
if err := c.Unschedule(ctx, id); err != nil {
	return err
}
if err := c.Unschedule(ctx, ""); err != nil { // an empty id is a no-op
	return err
}
```

The same named schedule declared twice on the SQL driver:

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

Three misfire policies:

<!-- snippet: examples/cron-schedules/main.go#misfire -->
```go
policies := map[string]workflow.Misfire{
	"skip":      {Policy: workflow.MisfireSkip},                            // fire nothing late
	"fire-once": {Policy: workflow.MisfireFireOnce, Within: 6 * time.Hour}, // the newest missed tick, if under 6h old
	"fire-all":  {Policy: workflow.MisfireFireAll, Max: 3},                 // up to 3 missed ticks, oldest first
}
```

The rule every driver applies to missed ticks:

<!-- snippet: examples/cron-schedules/main.go#due -->
```go
hourly, _ := cron.Parse("0 * * * *")
last, now := monday, monday.Add(5*time.Hour+30*time.Minute)
for _, name := range names {
	var ticks []string
	for _, t := range policies[name].Due(hourly, last, now, time.UTC) {
		ticks = append(ticks, t.Format("15:04"))
	}
	fmt.Fprintf(out, "  Due %-9s [%s]\n", name, strings.Join(ticks, " "))
}
```

Another cron dialect on the memory driver:

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

Refusals: an implicit zone, and Hatchet's UTC-only crons:

<!-- snippet: examples/cron-schedules/main.go#refusals -->
```go
_, err = c.Schedule(ctx, workflow.Schedule{Workflow: "report", Cron: "0 8 * * *"}) // no Location
fmt.Fprintln(out, "  no Location:", err)
err = c.Register(workflow.Workflow{Name: "local", Cron: []string{"0 8 * * *"}, CronLocation: time.Local,
	Steps: []workflow.Step{{Name: "s", Run: noop}}})
fmt.Fprintln(out, "  time.Local:", errors.Is(err, workflow.ErrInvalid))

h := hatchet.New(hatchet.Config{})
for _, wf := range []workflow.Workflow{
	{Name: "zoned", Cron: []string{"0 9 * * *"}, CronLocation: kolkata},
	{Name: "catch-up", Cron: []string{"0 9 * * *"}, Misfire: workflow.Misfire{Policy: workflow.MisfireFireAll, Max: 3}},
	{Name: "macro", Cron: []string{"@daily"}},
} {
	wf.Steps = []workflow.Step{{Name: "s", Run: noop}}
	fmt.Fprintf(out, "  hatchet %s: %v\n", wf.Name, h.Register(wf))
}
```

`main_test.go` runs the program and compares its whole output with the block above.

## Guides

- [Cron schedules](../../docs/schedules/cron.md)
- [Misfire policies](../../docs/schedules/misfire.md)
- [Time zones](../../docs/schedules/time-zones.md)
