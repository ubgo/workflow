# schedule-overlap

Keep a slow hourly job from piling up, run a schedule by hand, and read back its last fire: a schedule with the skip overlap policy, a tick held back while the previous run is unfinished, a manual trigger refused for the same reason and accepted once the run ended, and the schedule list with its last fire, last run and that run's status. It runs on the in-memory driver with a fake clock, so nothing needs to be installed and the output never varies.

```sh
cd examples
go run ./schedule-overlap
```

```text
-- ticks
  09:00 fired, run run-3
  10:00 skipped (overlap)
-- run now while the 09:00 run is unfinished
  refused: workflow: schedule schedule-1: previous run run-3 is running: workflow: the schedule's previous run is unfinished
-- run now after it finished
  run-6 succeeded, labels team=sales ubgo.workflow/trigger=manual
-- schedules
  crm-hourly overlap=skip last fire 10:05 run run-6 (succeeded), next 11:00
```

## The code

A schedule that skips a tick while its previous run is unfinished:

<!-- snippet: examples/schedule-overlap/main.go#schedule -->
```go
id, err := c.Schedule(ctx, workflow.Schedule{
	Name:     "crm-hourly",
	Workflow: "sync-crm",
	Cron:     "0 * * * *",
	Location: time.UTC,
	Input:    []byte(`{"full":false}`),
	Labels:   map[string]string{"team": "sales"},
	// Hold a tick back while the run the previous fire started is
	// unfinished. The default, OverlapAllow, starts a run every tick.
	Overlap: workflow.OverlapSkip,
})
```

Run it now; refused while the previous run is unfinished:

<!-- snippet: examples/schedule-overlap/main.go#trigger-refused -->
```go
trig := c.(workflow.ScheduleTriggerer)
_, err = trig.TriggerSchedule(ctx, id)
if errors.Is(err, workflow.ErrOverlap) {
	fmt.Fprintln(out, "  refused:", err)
}
```

<!-- snippet: examples/schedule-overlap/main.go#trigger -->
```go
ref, err := trig.TriggerSchedule(ctx, id)
if err != nil {
	return err
}
```

The last fire, its run and that run's status:

<!-- snippet: examples/schedule-overlap/main.go#last -->
```go
infos, err := c.(workflow.ScheduleLister).Schedules(ctx)
if err != nil {
	return err
}
for _, s := range infos {
	fmt.Fprintf(out, "  %s overlap=%s last fire %s run %s (%s), next %s\n",
		s.Name, s.Overlap, s.Last.Fire.Format("15:04"), s.Last.Run.RunID, s.Last.Status,
		s.Next.Format("15:04"))
}
```

`main_test.go` runs the program and compares its whole output with the block above.

## Guides

- [Overlap, trigger now and last fire](../../docs/schedules/overlap.md)
