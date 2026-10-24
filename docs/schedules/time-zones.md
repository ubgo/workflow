# Time zones

"Every day at nine" means nine where the user is. A schedule evaluated in UTC drifts by an hour twice a year for anyone in a zone with daylight saving time, and by the zone's whole offset for everyone else. Every schedule in this library is evaluated in an explicit zone: a declared cron in `Workflow.CronLocation`, a run-time schedule in `Schedule.Location`. This page covers setting the zone and what happens on the days the clocks change.

## Before you start

- A zone other than UTC needs `workflow.CapCronZone`. The memory, SQL and Redis drivers have it; Hatchet does not, because it evaluates crons in UTC only.
- The example is [`examples/cron-schedules`](../../examples/cron-schedules). Run it with `go run ./cron-schedules` from `examples/`.
- Load zones with `cron.LoadZone("Europe/Berlin")` (see [load a zone by name](#load-a-zone-by-name)). The binary needs the zone database: the host's, or `import _ "github.com/ubgo/workflow/cron/tzdata"` to embed it.

## Schedules in a time zone

`Workflow.CronLocation` is the zone of a workflow's declared crons. Nil means UTC. The example's digest fires at 09:00 in Kolkata, which is 03:30 UTC:

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

`Schedule.Location` is the zone of a run-time schedule, and it is required: a schedule without one fails with an error matching `workflow.ErrInvalid`, so none is ever evaluated in an implicit zone. Pass `time.UTC` for UTC:

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

`time.Local` is refused as a `CronLocation`, because it differs from machine to machine and a cron must fire at the same instant on all of them. `workflow.IsUTC(loc)` reports whether a zone evaluates as UTC: nil, `time.UTC`, or a zone named `UTC`. Those need no `CapCronZone`; anything else does, and a driver without it refuses the workflow or schedule rather than shifting the expression, since a shifted expression would be wrong for half the year:

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

## Daylight saving time

On the day the clocks jump forward, some wall times do not exist; on the day they fall back, some happen twice. The parser picks one of two rules by the hour field, so each kind of schedule does what its author meant:

- **Fixed-time schedules** (the hour field does not match all 24 hours, such as `30 2 * * *`) run once per day. A wall time inside the spring-forward gap fires once, at the first valid instant after the gap; a wall time inside the fall-back overlap fires once, on its first occurrence.
- **Periodic schedules** (the hour field matches every hour, such as `30 * * * *`) follow real time. A wall time inside the gap is not a real instant and does not fire; a wall time inside the overlap fires on both occurrences. No real instant is skipped or fired twice.
- **`@every` intervals** ignore the wall clock entirely, so DST never moves them.

The example prints both kinds around New York's two changes in 2026 (8 March, 02:00 jumps to 03:00; 1 November, 02:00 falls back to 01:00):

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

```text
daylight saving time:
  "30 2 * * *" Mar 8 03:00 EDT, Mar 9 02:30 EDT
  "30 * * * *" Mar 8 01:30 EST, Mar 8 03:30 EDT, Mar 8 04:30 EDT
  "30 1 * * *" Nov 1 01:30 EDT, Nov 2 01:30 EST
  "30 * * * *" Nov 1 00:30 EDT, Nov 1 01:30 EDT, Nov 1 01:30 EST
```

The daily 02:30 job runs at 03:00 on 8 March (02:30 did not exist) and at 02:30 the next day; the hourly job has no 02:30 run that night. On 1 November the daily 01:30 job runs once, and the hourly job runs at both 01:30s, an hour apart.

## Load a zone by name

`cron.LoadZone(name)` loads an IANA zone for a schedule and fails at boot instead of firing at the wrong hour. It refuses `""` and `"Local"`, which `time.LoadLocation` silently turns into UTC and the host's zone, and any name the zone database does not have; the error wraps `cron.ErrZone` and names the zone. Use it wherever a zone comes from config or user data:

<!-- snippet: examples/schedule-specs/main.go#zones -->
```go
berlin, err := cron.LoadZone("Europe/Berlin")
if err != nil {
	return err // fails at boot, not as a schedule firing at the wrong hour
}
```

A container image without a zone database (scratch, distroless and many slim images) finds no zone but UTC. Import `github.com/ubgo/workflow/cron/tzdata` once in the main package to embed one; it is the standard library's `time/tzdata` (about 450 KB) under a name that says why a scheduler imports it, and Go uses the embedded copy only when the host has none, so a host's newer rules still win:

```go
import _ "github.com/ubgo/workflow/cron/tzdata"
```

## Show the next runs in two zones

`workflow.NextRuns` lists a schedule's next fire times, each in the schedule's zone and in UTC, for a dashboard's "next run" column or a confirmation screen. It drops the ticks the schedule's `Windows` and `Active` period would skip, so the list shows runs that will really start, and it reads no clock: the reference time is the query's `After`. `Count` is capped at `workflow.MaxNextRuns` (100).

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

`NextRun.String()` prints the schedule's wall clock with its zone abbreviation and the UTC time in parentheses (with the UTC date when it differs), so a reader in another zone sees both. Across the end of summer time the UTC column moves while the local one stays at 09:00:

```text
  2026-10-24 09:00:00 CEST (07:00:00 UTC)
  2026-10-25 09:00:00 CET (08:00:00 UTC)
  2026-10-26 09:00:00 CET (08:00:00 UTC)
```

## Try it

```sh
cd examples
go run ./cron-schedules
```

Besides the DST lines above, the zone-related output is:

```text
declared cron:
  fired Mon 5 Jan 09:00 IST (03:30 UTC)
  fired Tue 6 Jan 09:00 IST (03:30 UTC)
schedule at run time:
  "weekly report" for acme at Mon 5 Jan 08:00 CET
```

and, under `refusals:`, a schedule without a `Location`, a `time.Local` cron, and Hatchet refusing the Kolkata zone.

## Pitfalls

- **Hatchet is UTC only.** The Hatchet driver refuses any `CronLocation` or `Location` other than UTC (field `Workflow.CronLocation` or `Schedule.Location`, missing `CronZone`). Translating "09:00 Berlin" to a UTC expression yourself is wrong for half the year; run such schedules on the memory or SQL driver, or accept UTC.
- **On the SQL driver a zone must load by name on every node.** The zone is stored by name, so every process must be able to `time.LoadLocation` it (or it must be a fixed offset). Ship the zone database with the binary (`cron/tzdata`) if your containers have none, and load zones with `cron.LoadZone` so a missing one fails at boot.
- **Do not pass `time.Local` as a `Schedule.Location` either.** Validation refuses it only as a `CronLocation`. For a run-time schedule the SQL driver refuses it with an error matching `workflow.ErrUnsupported`, but the memory driver accepts it and evaluates in the host's zone, so code tested on memory can fail in production. Name the zone.
- **A fixed offset is not a zone.** `time.FixedZone("CET", 3600)` never observes DST; use the IANA name (`Europe/Berlin`) when the schedule should follow local time.
- **Print fire times with the zone.** Users read "09:00" in their own zone; store and show the zone name alongside the expression.

## Related

- [Cron schedules](cron.md), [misfire policies](misfire.md)
- [Time and clocks](../concepts/time.md)
- Example: [`examples/schedule-specs`](../../examples/schedule-specs) (`LoadZone`, `NextRuns`)
- Example: [`examples/cron-schedules`](../../examples/cron-schedules)
