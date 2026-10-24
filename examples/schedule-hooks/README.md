# schedule-hooks

Control when a schedule's ticks fire and react to each one: time-of-day windows, a `ScheduleWhen` condition, a tick observer that sees fired and skipped ticks, per-schedule hooks with heartbeat pings, the schedule list with next fire times, and pausing one schedule. It runs on the in-memory driver with a fake clock and a local HTTP server as the heartbeat service, so nothing needs to be installed and the output never varies.

```sh
cd examples
go run ./schedule-hooks
```

```text
-- schedules
  sync-orders  office-hours "0 * * * *" next Mon 08:00 UTC
-- ticks
  08:00 skipped (window)
  09:00 fired, run succeeded
  10:00 skipped (when)
  11:00 fired, run failed
  12:00 skipped (window)
  13:00 fired, run succeeded
-- pause
  sync-orders  office-hours "0 * * * *" next never (paused)
  14:00 skipped (paused)
  15:00 fired, run succeeded
-- heartbeat pings
  /fail x1, /ok x3, /start x4
```

## The code

A condition in code, asked before every tick of the workflow's schedules:

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

An hourly schedule limited to office hours, minus lunch:

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

Every tick outcome, from every schedule:

<!-- snippet: examples/schedule-hooks/main.go#observer -->
```go
err = c.(workflow.TickNotifier).OnTick(func(_ context.Context, t workflow.Tick) {
	ticks <- t // every fired, skipped and misfired tick, from every schedule
})
```

Hooks and heartbeat pings for one schedule:

<!-- snippet: examples/schedule-hooks/main.go#hooks -->
```go
ping := middleware.NewPinger(middleware.HTTPPing(hb.Client()))
err = workflow.AttachScheduleHooks(c, workflow.ScheduleHooks{
	Workflow:  "sync-orders",
	Name:      "office-hours",               // one schedule; empty means all of the workflow's
	Fired:     ping.Tick(hb.URL + "/start"), // a run started
	OnSuccess: ping.Run(hb.URL + "/ok"),     // it succeeded
	OnFailure: ping.Run(hb.URL + "/fail"),   // it failed
	After: func(_ context.Context, ev workflow.RunEvent) { // after
		ends <- ev
	},
})
```

The schedule list:

<!-- snippet: examples/schedule-hooks/main.go#list -->
```go
infos, err := c.(workflow.ScheduleLister).Schedules(ctx)
if err != nil {
	return err
}
for _, s := range infos {
	next := "never (paused)"
	if !s.Next.IsZero() {
		next = s.Next.In(s.Location).Format("Mon 15:04 MST")
	}
	fmt.Fprintf(out, "  %-12s %-11s %-9q next %s\n", s.Workflow, s.Name, s.Cron, next)
}
```

Pause and resume:

<!-- snippet: examples/schedule-hooks/main.go#pause -->
```go
pauser := c.(workflow.SchedulePauser)
if err := pauser.PauseSchedule(ctx, workflow.ScheduleTarget{ID: id}); err != nil {
	return err
}
```

<!-- snippet: examples/schedule-hooks/main.go#resume -->
```go
if err := pauser.ResumeSchedule(ctx, workflow.ScheduleTarget{ID: id}); err != nil {
	return err
}
```

`main_test.go` runs the program and compares its whole output with the block above.

## Guides

- [Schedule conditions](../../docs/schedules/conditions.md)
- [Schedule hooks and pings](../../docs/schedules/hooks.md)
- [Manage schedules](../../docs/schedules/manage.md)
