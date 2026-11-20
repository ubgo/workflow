# Latest-only runs

Some scheduled work only matters for the newest tick: refreshing a cache, rebuilding a dashboard, pushing the current stock level. When a misfire catch-up fires three missed hours at once, or a backlog delays the 09:00 run until after 10:00, running the older ticks is wasted work. `middleware.LatestOnly` skips the work of a scheduled run whose next tick is already due, so only the newest run does it.

## Before you start

- The run must carry its tick time: drivers with `workflow.CapTickTime` (memory, SQL, Redis, Hatchet) set `workflow.LabelTickAt` on every fired run.
- The middleware reads the schedule's expression through `workflow.ScheduleLister` (`workflow.CapScheduleList`), usually the client itself, or from a `Next` function you give it.
- Misfire catch-up is on [misfire](misfire.md); the tick time on [cron](cron.md).

## Skip stale runs

Apply `middleware.LatestOnly` to the steps whose work should be skipped:

```go
latest := middleware.LatestOnly(middleware.LatestOnlyConfig{
	Schedules: c.(workflow.ScheduleLister),
	Clock:     clk, // the driver's clock
	OnSkip: func(ctx context.Context, info workflow.StepInfo, tick, newer time.Time) {
		slog.InfoContext(ctx, "stale tick skipped", "tick", tick, "newer", newer)
	},
})
wf := workflow.Workflow{Name: "refresh-cache", Cron: []string{"0 * * * *"}, Steps: []workflow.Step{
	{Name: "refresh", Run: latest(refresh)},
}}
```

For each attempt it reads the run's tick (`StepInfo.TickAt`), looks up the schedule that fired the run (`workflow.LabelSchedule`), computes the tick after the run's own in the schedule's zone, and compares it with the clock. When that next tick is already due, a newer run exists or is about to, so the step returns its input unchanged without calling the handler and `OnSkip` is told. Otherwise the handler runs.

Runs not fired by a schedule, and runs whose schedule is gone, always run. `Next` replaces the lookup for a schedule kept elsewhere, and `Parser` names the cron dialect when the drivers use a custom one.

## Pitfalls

- A skipped step still succeeds, and the steps after it run with its input. Put `LatestOnly` on the step that does the expensive work, or on every step of the run.
- The comparison uses the clock you pass. Give it the driver's clock, or a test with a fake clock decides on the wall time.
