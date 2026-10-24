# Overlap, trigger now and last fire

An hourly sync that sometimes takes ninety minutes starts a second copy while the first is still writing, and the two fight over the same rows. This page covers the overlap policy that skips a tick while the previous run is unfinished, running a schedule by hand with its normal input, and reading back when a schedule last fired, which run that started and how it ended.

## Before you start

- A driver that declares the capability for each part: `CapOverlapSkip` for the skip policy, `CapScheduleTrigger` for trigger-now (the `workflow.ScheduleTriggerer` interface), and `CapScheduleHistory` for `ScheduleInfo.Last`. Which drivers have which is in [Driver support](#driver-support).
- A schedule, declared with `Workflow.Cron` or created with `Client.Schedule` ([Cron schedules](cron.md)).
- The example is [`examples/schedule-overlap`](../../examples/schedule-overlap). Run it with `go run ./schedule-overlap` from `examples/`.

## Skip a tick while the previous run is unfinished

Set `Schedule.Overlap` to `workflow.OverlapSkip`, or `Workflow.CronOverlap` for every entry of a workflow's `Cron`. The default, `OverlapAllow` (also the zero value), starts a run on every tick whatever the previous one is doing.

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

The "previous run" is the run the schedule's last fire started, a tick or a manual trigger. At each tick the driver reads that run's status from its own run state and skips the tick while the status is not terminal (`OverlapPolicy.Blocks` is the one rule every driver uses). The check is not a lock held by one node, so it holds across a fleet of workers and, on a durable driver, across a restart. A skipped tick is reported to tick observers as `TickSkipped` with the reason `workflow.TickReasonOverlap` ([Schedule hooks](hooks.md)), and it does not count as a fire.

A previous run the driver no longer knows, because it was deleted or pruned by retention, does not block. Each entry of `Workflow.Cron` is its own schedule, so one entry's long run does not hold back another entry's tick. `Workflow.CronOverlap` without `Cron`, or an unknown policy, is refused by `Validate` with `ErrInvalid`; a driver without `CapOverlapSkip` refuses `OverlapSkip` at `Register` or `Schedule` with an `UnsupportedError` that names the missing capability (for a schedule, the field `Schedule.Overlap`), rather than starting overlapping runs.

## Trigger a schedule now

`TriggerSchedule(ctx, id)` on a client that implements `workflow.ScheduleTriggerer` enqueues the schedule's workflow now, with its `Input` and its labels, and returns the new run. It obeys the overlap policy: while the previous run is unfinished a skipping schedule refuses with an error that matches `workflow.ErrOverlap`, and nothing is enqueued.

<!-- snippet: examples/schedule-overlap/main.go#trigger-refused -->
```go
trig := c.(workflow.ScheduleTriggerer)
_, err = trig.TriggerSchedule(ctx, id)
if errors.Is(err, workflow.ErrOverlap) {
	fmt.Fprintln(out, "  refused:", err)
}
```

Once that run has ended, the trigger is accepted:

<!-- snippet: examples/schedule-overlap/main.go#trigger -->
```go
ref, err := trig.TriggerSchedule(ctx, id)
if err != nil {
	return err
}
```

The triggered run carries the labels a tick would give it (`ubgo.workflow/schedule` and `ubgo.workflow/schedule-name`) plus `ubgo.workflow/trigger=manual` (`workflow.LabelTrigger`), so a run list or a dashboard can tell it from a cron fire. It becomes the schedule's last run, so the next tick's overlap check sees it. Tick observers get a `TickFired` with `Tick.Manual` set. The schedule's next cron fire does not move.

A trigger ignores the schedule's `Windows` and `ScheduleWhen`, and it fires a paused schedule too: a pause stops the cron, not an operator. An unknown ID returns `workflow.ErrNotFound`. Declared crons can be triggered by the stable ID that `ScheduleLister.Schedules` reports for them.

## Read the last fire

On a driver with `CapScheduleHistory`, each `ScheduleInfo` from `ScheduleLister.Schedules` carries `Last`, a `workflow.ScheduleLast`: `Fire` (when the schedule last fired, by tick or by trigger), `Run` (the run that fire started) and `Status` (that run's status when you listed, empty when the driver no longer knows the run). `Next` is the next cron fire, as before, and `Overlap` the policy.

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

A durable driver stores the last fire and run with the schedule, so they survive a restart. A skipped tick does not change them. The admin UI shows the same fields and a Run now button ([Admin web UI](../admin/ui.md)).

## Driver support

| Driver | Overlap skip | Trigger now | Last fire |
|---|---|---|---|
| Memory | yes | yes | yes (lost on restart) |
| SQL (`sqlstore`) | yes (a fire moves the last run with a compare-and-set, so a tick and a trigger on two nodes cannot both start a run) | yes | yes (stored, migration `006_schedule_history`) |
| Redis | yes (decided in the firing script) | yes | yes (stored) |
| Hatchet | refused by field name (the engine fires ticks without asking a worker) | yes, a declared or `Schedule` trigger, a disabled one too | yes, read from the engine's run list within `Config.ScheduleHistoryWindow` (a week by default), not stored |
| `failover` | the primary's | the primary's | the primary's |

## Try it

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

## Pitfalls

- **A skipped tick is not caught up.** When the long run ends, the schedule waits for its next tick; nothing fires for the tick it skipped. Trigger it by hand if the missed run matters.
- **Overlap is per schedule, not per workflow.** Two schedules of one workflow, or a schedule and a job you enqueue yourself, can still run at the same time. To allow one run of a workflow at a time whatever started it, use a concurrency key ([Concurrency and rate limits](../jobs/concurrency.md)).
- **A run stuck in `waiting` blocks the schedule.** A wait step that never gets its signal keeps the run unfinished, so every tick is skipped until it times out or someone cancels it. Give wait steps a timeout.
- **Deleting the previous run unblocks the schedule.** The policy reads the run's status; a run the driver no longer knows does not block.
- **Trigger-now bypasses windows, conditions and a pause on purpose.** Only the overlap policy can refuse it.

## Related

- [Cron schedules](cron.md): declaring and creating schedules.
- [Manage schedules](manage.md): list, pause and group schedules.
- [Schedule hooks and pings](hooks.md): the tick observer that sees skipped ticks.
- [Admin operations](../admin/operations.md#schedules): the same from the admin API.
