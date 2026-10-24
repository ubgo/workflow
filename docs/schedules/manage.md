# Manage schedules

An app with schedules needs to answer "what runs when?" and to stop a schedule for a while without losing it. Here a client that implements `workflow.ScheduleLister` lists every live schedule with its next fire time in its own zone, and one that implements `workflow.SchedulePauser` pauses and resumes one schedule or all of them. On a durable driver both read and write the stored schedules, so every node agrees.

## Before you start

- Listing needs `workflow.CapScheduleList` and pausing `workflow.CapSchedulePause`; the memory, SQL and Redis drivers have both. Hatchet declares `CapScheduleList`, and `CapSchedulePause` only when `hatchet.Config.API` is set (see [the Hatchet driver](../drivers/hatchet.md)); there `PauseSchedule` of `All` is refused, while `ResumeSchedule` of `All` re-enables every disabled trigger. Find the interface with a type assertion after checking the capability: wrappers implement every optional interface and refuse when the client inside lacks it, so the capability is the reliable test.
- The example is [`examples/schedule-hooks`](../../examples/schedule-hooks), on the memory driver with a fake clock. Run it with `go run ./schedule-hooks` from `examples/`.

## List schedules

`ScheduleLister.Schedules(ctx)` returns a `workflow.ScheduleInfo` per live schedule, declared crons and `Client.Schedule` ones alike, sorted by workflow and then name. Each has its `ID`, `Name`, `Workflow`, `Cron`, `Location`, `Declared`, `Paused`, `Misfire`, `Windows`, `Labels` and `Next`, the next fire time, zero when the schedule is paused or never fires again. An unscheduled schedule is not listed.

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

`Next` is the cron's next fire; a [window or `ScheduleWhen`](conditions.md) can still skip that tick when it comes. The ID of a declared cron is the driver's stable id for that declaration, the same one its ticks carry, so it can be paused like any other.

## Pause a schedule

`SchedulePauser.PauseSchedule(ctx, target)` stops a schedule firing without removing it; `ResumeSchedule` starts it again. A `workflow.ScheduleTarget` names one schedule by `ID` or every schedule with `All: true` (exactly one of the two). `ResumeSchedule` with `All` clears every pause.

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

While a schedule is paused its ticks are reported as skipped with `Reason` `workflow.TickReasonPaused` to [tick observers](hooks.md), and the list shows it `Paused` with no next fire. A tick that falls due during the pause never fires, not even on resume and whatever the schedule's [misfire policy](misfire.md): the schedule resumes at its next tick. The misfire policy still governs ticks missed while nothing was running. Pausing a schedule does not touch runs it already fired; to hold those, pause the workflow or its queue (`workflow.Pauser`).

## Group schedules

Several schedules often share options. There is no group type, because a Go function does it: build each `workflow.Schedule` from a helper that sets the shared fields (zone, windows, misfire policy, labels), and give the group a label so it can be found in the list and its runs filtered.

<!-- snippet: examples/schedule-groups/main.go#helper -->
```go
// officeHours builds a schedule of the office-hours group: every schedule
// it returns shares the zone, the window, the misfire policy and the group
// label, so changing one of them here changes the whole group.
func officeHours(berlin *time.Location, name, workflowName, expr string) workflow.Schedule {
	return workflow.Schedule{
		Name:     name,
		Workflow: workflowName,
		Cron:     expr,
		Location: berlin,
		Windows:  workflow.Windows{Between: []workflow.Window{{From: "09:00", To: "18:00"}}},
		Misfire:  workflow.Misfire{Policy: workflow.MisfireSkip},
		Labels:   map[string]string{groupLabel: officeGroup},
	}
}
```

The label then finds the group: in the schedule list, in the runs its ticks started (a fired run carries its schedule's labels), and when pausing every schedule of the group at once:

<!-- snippet: examples/schedule-groups/main.go#pause -->
```go
infos, err := c.Schedules(ctx)
if err != nil {
	return err
}
for _, s := range infos {
	if s.Labels[groupLabel] != officeGroup {
		continue
	}
	if err := c.PauseSchedule(ctx, workflow.ScheduleTarget{ID: s.ID}); err != nil {
		return err
	}
}
```

The program [schedule-groups](../../examples/schedule-groups/) builds two schedules with the helper and one without, lists them with their group, finds the group's runs by its label and pauses the group.

## Keep the newest runs

`Schedule.KeepRuns` (or `Workflow.CronKeepRuns`) keeps only the newest N finished runs a schedule fired, so a schedule that fires every minute does not fill the run store with a year of history. It needs `workflow.CapScheduleKeep` (the memory, Redis and SQL drivers declare it; Hatchet refuses); zero keeps everything, as before. Only finished runs carrying the schedule's label are deleted, never a run still queued or running, and never a run of the same workflow enqueued by hand. Retention by age ([retention](../operations/retention.md)) still applies on top.

## Store a schedule as JSON

`workflow.Schedule` implements `json.Marshaler` and `json.Unmarshaler` with a versioned form (`workflow.ScheduleJSONVersion`, 1), for schedules kept as data: a settings table, a config file, an admin form. The zone is written by its IANA name and durations and instants as text:

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

```text
{"v":1,"name":"nightly-report","workflow":"report","cron":"0 2 * * *","location":"Europe/Berlin","misfire":{"policy":"skip"}}
```

Reading refuses another version, unknown fields (so a document from a newer release never loses meaning silently) and zones that do not load; every error wraps `workflow.ErrScheduleJSON`. `json.Unmarshal` loads zones with `cron.LoadZone`; `workflow.UnmarshalSchedule(data, loadZone)` takes your own loader, for a zone alias table or a test. Writing refuses `time.Local` and a fixed offset, since neither has a name another process can load. The JSON form does not validate the rest of the schedule; `Client.Schedule` does that when it is created.

## Task output

A scheduled run is ordinary Go code, so its output is its logs: log through `log/slog` with [`middleware.Logging`](../observability/logging.md) for every step, and ship the logs where the team reads them. For a summary per run, an `After` [schedule hook](hooks.md) can send it.

## Try it

```sh
cd examples
go run ./schedule-hooks
```

The list before and after the pause, and the ticks around it:

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
```

## Pitfalls

- **A pause is per schedule, not per run.** Runs a schedule already fired keep running; cancel or pause those separately.
- **A paused tick is gone.** Resuming does not catch up on the ticks of the pause, on any driver and under any misfire policy, so a daily report paused over its hour skips that day. Enqueue the run by hand if it must still happen.
- **A named schedule is upserted.** Calling `Schedule` again with the same `Name` and workflow returns the same ID and replaces the schedule, so declaring schedules at every boot is safe; without a `Name`, every call adds one.
- **`time.Local` is refused.** A schedule's `Location` must name a zone; the host's local zone differs from machine to machine, so every driver refuses it with `ErrInvalid`.
- **`KeepRuns` deletes only runs the schedule fired.** Runs of the workflow enqueued by hand, and runs not yet finished, are never counted or deleted.
- **`json.Marshal(Schedule)` changed form.** Earlier releases wrote the zone as `{}`; it is now the versioned form above, and a document from before cannot be read back.
- **The list is of live schedules.** After `Unschedule` a schedule is gone from it; a paused one stays.

## Related

- [Schedule hooks and pings](hooks.md): paused ticks are reported there.
- [Schedule conditions](conditions.md), [Cron schedules](cron.md), [Misfire policies](misfire.md).
- Example: [examples/schedule-hooks](../../examples/schedule-hooks/).
- Example: [examples/schedule-groups](../../examples/schedule-groups/) (a group of schedules from one helper).
- Example: [examples/schedule-specs](../../examples/schedule-specs/) (a schedule stored as JSON).
