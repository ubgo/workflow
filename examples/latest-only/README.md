# latest-only

Skip the work of scheduled runs that are no longer the newest with `middleware.LatestOnly`. An hourly cache refresh runs on time at 09:00; then the process is down from 09:30 to 12:30, and the `MisfireFireAll` policy fires the three missed ticks at once. Only the newest (12:00) refreshes the cache: the 10:00 and 11:00 runs see that a newer tick is already due and return their input without calling the handler. A run enqueued by hand has no tick and always works. It runs on the in-memory driver with a fake clock, so nothing needs to be installed and the output never varies.

```sh
cd examples
go run ./latest-only
```

```text
-- on time
  tick 09:00, run at 09:00: refreshed the cache
-- after a three-hour outage
  tick 10:00, run at 12:30: skipped, the 11:00 tick is due
  tick 11:00, run at 12:30: skipped, the 12:00 tick is due
  tick 12:00, run at 12:30: refreshed the cache
-- not fired by a schedule
  tick none, run at 12:30: refreshed the cache
```

## The code

The middleware, wrapped around the one step whose work is skipped:

<!-- snippet: examples/latest-only/main.go#latest -->
```go
latest := middleware.LatestOnly(middleware.LatestOnlyConfig{
	Schedules: c,   // the client lists its schedules (CapScheduleList)
	Clock:     clk, // the driver's clock, so a fake clock decides in tests
	OnSkip: func(ctx context.Context, info workflow.StepInfo, tick, newer time.Time) {
		j.add(tick, fmt.Sprintf("tick %s, run at %s: skipped, the %s tick is due", tick.Format(hourFormat), clk.Now().Format(hourFormat), newer.Format(hourFormat)))
	},
})
err := c.Register(workflow.Workflow{Name: "refresh-cache", Steps: []workflow.Step{
	{Name: "refresh", Run: latest(refresh)}, // only this step's work is skipped
}})
```

The schedule, with a misfire policy that fires every missed tick:

<!-- snippet: examples/latest-only/main.go#schedule -->
```go
_, err = c.Schedule(ctx, workflow.Schedule{
	Name:     "hourly",
	Workflow: "refresh-cache",
	Cron:     "0 * * * *",
	Location: time.UTC,
	// After an outage, fire every missed tick (at most three), oldest first.
	Misfire: workflow.Misfire{Policy: workflow.MisfireFireAll, Max: catchUp},
})
```

`go test ./latest-only` checks every line of the output. The guide is [docs/schedules/latest-only.md](../../docs/schedules/latest-only.md).
