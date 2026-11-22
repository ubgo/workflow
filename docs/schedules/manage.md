# Manage schedules

An app with schedules needs to answer "what runs when?" and to stop a schedule for a while without losing it. Here a client that implements [`workflow.ScheduleLister`](ds:block?id=schedulelister-2vgjk9bh) lists every live schedule with its next fire time in its own zone, and one that implements [`workflow.SchedulePauser`](ds:block?id=schedulepauser-ed8jzah5) pauses and resumes one schedule or all of them. On a durable driver both read and write the stored schedules, so every node agrees.

## Before you start

- Listing needs [`workflow.CapScheduleList`](ds:block?id=capschedulelist-3hbmmd6n) and pausing [`workflow.CapSchedulePause`](ds:block?id=capschedulepause-h24u69mw); the [memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-paritycaps-e5y5vqhk) and [Redis](ds:block?id=redis-caps-bfgdst9t) drivers have both. Hatchet [declares `CapScheduleList`](ds:block?id=hatchet-caps-d6x9fv8z), and `CapSchedulePause` [only when `hatchet.Config.API` is set](ds:block?id=hatchet-client-capabilities-3ms5edtr) (see [the Hatchet driver](../drivers/hatchet.md)); there [`PauseSchedule` of `All` is refused](ds:block?id=hatchet-client-pauseschedule-m3skypt5), while `ResumeSchedule` of `All` re-enables every disabled trigger. Find the interface with a type assertion after checking the capability: wrappers implement every optional interface and refuse when the client inside lacks it, so the capability is the reliable test. It refuses [`KeepRuns`](ds:block?id=hatchet-keeprunsreason-ethbbz58) and [`Active`](ds:block?id=hatchet-client-schedule-ug2ype7f) (no `CapScheduleKeep`, `CapScheduleActive`); see [Hatchet limitations](../drivers/hatchet.md#limitations).
- The example is [`examples/schedule-hooks`](../../examples/schedule-hooks), on the memory driver with a fake clock. Run it with `go run ./schedule-hooks` from `examples/`.
- Keeping runs, the cursor and the tick time are in [`examples/schedule-keep-runs`](../../examples/schedule-keep-runs), also on the memory driver with a fake clock; the JSON form is in [`examples/schedule-specs`](../../examples/schedule-specs).

## List schedules

[`ScheduleLister.Schedules(ctx)`](ds:block?id=memory-client-schedules-mbv79g96) returns a [`workflow.ScheduleInfo`](ds:block?id=scheduleinfo-hudmtgvr) per live schedule, declared crons and `Client.Schedule` ones alike, sorted by workflow and then name. Each has its `ID`, `Name`, `Workflow`, `Cron`, `Location`, `Declared`, `Paused`, `Misfire`, `Windows`, `Labels` and `Next`, the next fire time, zero when the schedule is paused or never fires again. An unscheduled schedule is not listed.

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
	fmt.Printf("  %-12s %-11s %-9q next %s\n", s.Workflow, s.Name, s.Cron, next)
}
```

`Next` is the cron's next fire; a [window or `ScheduleWhen`](conditions.md) can still skip that tick when it comes. The ID of a declared cron is the driver's stable id for that declaration, the same one its ticks carry, so it can be paused like any other.

## Pause a schedule

[`SchedulePauser.PauseSchedule(ctx, target)`](ds:block?id=schedulepauser-ed8jzah5) stops a schedule firing without removing it; `ResumeSchedule` starts it again. A [`workflow.ScheduleTarget`](ds:block?id=scheduletarget-hnk6efmn) names one schedule by `ID` or every schedule with `All: true` ([exactly one of the two](ds:block?id=validatescheduletarget-tap9kqc5)). `ResumeSchedule` with `All` clears every pause.

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

While a schedule is paused its ticks are reported as skipped with `Reason` [`workflow.TickReasonPaused`](ds:block?id=tickreasonpaused-mxj3utjy) to [tick observers](hooks.md), and the list shows it `Paused` with no next fire. A tick that falls due during the pause [never fires](ds:block?id=memory-client-wakeschedulelocked-sx48wwqs), not even on resume and whatever the schedule's [misfire policy](misfire.md) (the same on the [SQL driver](ds:block?id=sql-client-fireschedules-9tkj9xrk)): the schedule resumes at its next tick. The misfire policy still governs ticks missed while nothing was running. Pausing a schedule does not touch runs it already fired; to hold those, pause the workflow or its queue (`workflow.Pauser`).

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

The label then finds the group: in the schedule list (each [`ScheduleInfo`](ds:block?id=scheduleinfo-hudmtgvr) carries `Labels`), in the runs its ticks started (a fired run [carries its schedule's labels](ds:block?id=firedlabels-jhhxe6ux)), and when pausing every schedule of the group at once:

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

A schedule that fires every minute fills the run store with a year of history nobody reads. `Schedule.KeepRuns` keeps only the newest N finished runs a schedule fired and deletes the older ones ([memory](ds:block?id=memory-client-keeplocked-w39asqby), [SQL](ds:block?id=sql-client-keepruns-g7nyqccy)). It needs [`workflow.CapScheduleKeep`](ds:block?id=capschedulekeep-rx5bjfyu) (the [memory](ds:block?id=memory-caps-yxkuq583), [Redis](ds:block?id=redis-caps-bfgdst9t) and [SQL](ds:block?id=sql-p5caps-wggh6esn) drivers declare it; [Hatchet refuses the field](ds:block?id=hatchet-keeprunsreason-ethbbz58) with an `*workflow.UnsupportedError` naming `Schedule.KeepRuns`); zero keeps everything. Check the capability where the driver is chosen at run time:

<!-- snippet: examples/schedule-keep-runs/main.go#caps -->
```go
need := workflow.CapsOf(workflow.CapScheduleKeep, workflow.CapTickTime)
fmt.Fprintf(w, "  memory has %s: %v\n", need, mem.Capabilities().Has(need))
```

Set the field on the schedule:

<!-- snippet: examples/schedule-keep-runs/main.go#keep -->
```go
id, err := c.Schedule(ctx, workflow.Schedule{
	Name:     "hourly-import",
	Workflow: "import-orders",
	Cron:     "0 * * * *",
	Location: berlin,
	KeepRuns: keep, // only the newest 2 finished runs stay in the run store
	Misfire:  workflow.Misfire{Policy: workflow.MisfireFireAll, Max: catchUp},
})
```

For a cron declared on the workflow, `Workflow.CronKeepRuns` does the same for each of its expressions; it is [refused on a workflow without `Cron`](ds:block?id=validatepolicies-x88m4hwu), and both refuse a negative count with `workflow.ErrInvalid` ([`ValidateSchedule`](ds:block?id=validateschedule-knj437ed)):

<!-- snippet: examples/schedule-keep-runs/main.go#cron-keep -->
```go
err = c.Register(workflow.Workflow{
	Name:         "rollup",
	Cron:         []string{"0 * * * *"},
	CronLocation: berlin,
	CronKeepRuns: keep, // each declared cron keeps its newest 2 finished runs
	Misfire:      workflow.Misfire{Policy: workflow.MisfireFireAll, Max: catchUp},
	Steps:        []workflow.Step{{Name: "sum", Run: rollup(rollups)}},
})
```

Only [finished runs carrying the schedule's label](ds:block?id=sql-client-keepruns-g7nyqccy) count and are deleted: never a run still queued or running, and never a run of the same workflow enqueued by hand. The example fires five ticks of each schedule and ends with two runs of each, the 12:00 and 13:00 ticks. Retention by age ([retention](../operations/retention.md)) still applies on top.

## Store a schedule as JSON

`workflow.Schedule` implements [`json.Marshaler`](ds:block?id=schedule-marshaljson-r7xwj8x5) and [`json.Unmarshaler`](ds:block?id=schedule-unmarshaljson-rvdt6ut7) with a versioned form ([`workflow.ScheduleJSONVersion`](ds:block?id=schedulejsonversion-f9pnaa4s), 1), for schedules kept as data: a settings table, a config file, an admin form. The zone is written by its IANA name and durations and instants as text:

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

[Reading refuses](ds:block?id=unmarshalschedule-7ytw9duj) another version, unknown fields (so a document from a newer release never loses meaning silently) and zones that do not load; every error wraps [`workflow.ErrScheduleJSON`](ds:block?id=errschedulejson-v6hk7ay2). `json.Unmarshal` loads zones with `cron.LoadZone`; [`workflow.UnmarshalSchedule(data, loadZone)`](ds:block?id=unmarshalschedule-7ytw9duj) takes your own loader, for a zone alias table or a test, and nil means `cron.LoadZone`:

<!-- snippet: examples/schedule-specs/main.go#json-read -->
```go
stored := []byte(`{"v":2,"name":"x"}`)           // written by a newer release
_, err = workflow.UnmarshalSchedule(stored, nil) // nil loads zones with cron.LoadZone
fmt.Printf("  v2 ErrScheduleJSON=%t\n", errors.Is(err, workflow.ErrScheduleJSON))
```

```text
  v2 ErrScheduleJSON=true
```

Match `ErrScheduleJSON` with `errors.Is` to tell a bad stored document (show it to an operator, keep the old schedule) from a failure of the store itself. [Writing refuses](ds:block?id=schedule-marshaljson-r7xwj8x5) `time.Local` and a fixed offset, since neither has a name another process can load. The JSON form does not validate the rest of the schedule; `Client.Schedule` does that when it is created.

## Keep a cursor between runs

A schedule that pulls new rows each tick needs to remember where the last tick stopped. Keep that cursor in the schedule's own folder of a [`DataStore`](../workflows/memoisation.md#share-data-across-runs) and move it with `UpdateData`, a compare-and-set, so two runs of the schedule never both take the same rows.

First give the client a data store with the [`workflow.Data`](ds:block?id=data-26rctv2b) plugin. [`workflow.NewMemoryData()`](ds:block?id=newmemorydata-4h6jkcnw) lives in one process; `sqlstore.DataStore` and `redis.DataStore` survive restarts and are shared by every node:

<!-- snippet: examples/schedule-keep-runs/main.go#data -->
```go
c, err := workflow.Use(mem, workflow.Data(workflow.NewMemoryData())) // sqlstore.DataStore or redis.DataStore across processes
if err != nil {
	return err
}
```

Then, in the step, key the folder by the schedule's ID and claim the next rows:

<!-- snippet: examples/schedule-keep-runs/main.go#cursor -->
```go
info, _ := workflow.StepInfoFrom(ctx)
folder := workflow.Folder("schedule/" + info.Labels[workflow.LabelSchedule]) // this schedule's own folder
var last int64
next, err := workflow.UpdateData(ctx, folder, cursorName, func(cur int64, _ bool) (int64, error) {
	last = cur // set again on every attempt: UpdateData retries on a lost race
	return min(cur+batchSize, newestRow), nil
})
if err != nil {
	return nil, err
}
// Rows last+1..next are this run's alone: the compare-and-set gave them
// to no other run. Import them here, outside the function UpdateData
// may call more than once.
```

[`UpdateData`](ds:block?id=updatedata-zc9hugjv) reads the cursor, calls the function and writes the result only if nobody wrote the cursor in between; otherwise it reads again and calls the function again. So the function only computes the new cursor, and the import of rows `last+1` to `next` happens after it, once. After [`workflow.UpdateDataAttempts`](ds:block?id=updatedataattempts-3544vz5p) (32) lost races it returns [`workflow.ErrDataConflict`](ds:block?id=errdataconflict-vv8g67fu), which is not permanent, so the step's retry policy tries again later.

[`LabelSchedule`](ds:block?id=labelschedule-89dq6u92) is the schedule's ID, set on every run it fires; a run started by hand has none, so key the folder by `Schedule.Name` instead when manual runs should move the same cursor. In the example each of the five runs takes the next three rows, the three catch-up runs included, and no row is taken twice.

## Task output

A scheduled run is ordinary Go code, so its output is its logs: log through `log/slog` with [`middleware.Logging`](../observability/logging.md) for every step, and ship the logs where the team reads them. To keep each run's own lines and result where someone finds them in the morning (a file, a mail, a table the admin reads), wrap the steps in [`middleware.CaptureOutput`](ds:block?id=mw-captureoutput-fk79ecqu); [Task output](output.md) shows the sinks. For a summary per run, an `After` [schedule hook](hooks.md) can send it.

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

The keep, cursor and tick part, from [schedule-keep-runs](../../examples/schedule-keep-runs/):

```sh
cd examples
go run ./schedule-keep-runs
```

```text
-- driver
  memory has ScheduleKeep|TickTime: true
-- hourly import, three rows a run
  tick 09:00, run at 09:00: orders 08:00-09:00, rows 1-3
  tick 10:00, run at 10:00: orders 09:00-10:00, rows 4-6
-- after a three-hour outage
  tick 11:00, run at 13:30: orders 10:00-11:00, rows 7-9
  tick 12:00, run at 13:30: orders 11:00-12:00, rows 10-12
  tick 13:00, run at 13:30: orders 12:00-13:00, rows 13-15
-- run history
  import-orders (KeepRuns 2): 2 runs kept, ticks 12:00 13:00
  rollup (CronKeepRuns 2): 2 runs kept, ticks 12:00 13:00
```

Each run takes the next three rows, also the three catch-up runs that start together at 13:30, and each schedule ends with its newest two runs. `go test ./schedule-hooks ./schedule-keep-runs` checks every line.

## Pitfalls

- **A pause is per schedule, not per run.** Runs a schedule already fired keep running; cancel or pause those separately.
- **A paused tick is gone.** Resuming [does not catch up](ds:block?id=memory-client-wakeschedulelocked-sx48wwqs) on the ticks of the pause, on any driver and under any misfire policy, so a daily report paused over its hour skips that day. Enqueue the run by hand if it must still happen.
- **A named schedule is upserted.** Calling `Schedule` again with the same `Name` and workflow [returns the same ID and replaces the schedule](ds:block?id=sql-client-schedule-tjaxged6), so declaring schedules at every boot is safe; without a `Name`, every call adds one.
- **`time.Local` is refused.** A schedule's `Location` must name a zone; the host's local zone differs from machine to machine, so every driver [refuses it](ds:block?id=validateschedule-knj437ed) with `ErrInvalid`.
- **`KeepRuns` deletes only runs the schedule fired.** Runs of the workflow enqueued by hand, and runs not yet finished, are never counted or deleted.
- **A cursor function runs more than once.** [`UpdateData`](ds:block?id=updatedata-zc9hugjv) calls it again after a lost race, so a side effect inside it (an import, a message) can happen twice. Compute the new cursor inside, do the work after.
- **A cursor keyed by the schedule ID starts over with a new schedule.** `Schedule` with the same `Name` keeps the ID, but `Unschedule` followed by a new `Schedule` gives a new ID and so a new, empty folder. Key the folder by `Schedule.Name` when the schedule may be recreated, or copy the cursor over.
- **`json.Marshal(Schedule)` changed form.** Earlier releases wrote the zone as `{}`; it is now the versioned form above, and a document from before cannot be read back.
- **The list is of live schedules.** After `Unschedule` a schedule [is gone from it](ds:block?id=memory-client-schedules-mbv79g96); a paused one stays.

## Related

- [Schedule hooks and pings](hooks.md): paused ticks are reported there.
- [Schedule conditions](conditions.md), [Cron schedules](cron.md), [Misfire policies](misfire.md).
- Example: [examples/schedule-hooks](../../examples/schedule-hooks/).
- Example: [examples/schedule-groups](../../examples/schedule-groups/) (a group of schedules from one helper).
- Example: [examples/schedule-specs](../../examples/schedule-specs/) (a schedule stored as JSON).
- Example: [examples/schedule-keep-runs](../../examples/schedule-keep-runs/) (`KeepRuns`, `CronKeepRuns`, a cursor between runs).
