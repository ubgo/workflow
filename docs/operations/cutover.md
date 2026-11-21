# Backend cutover

Moving an app from one backend to another without a stop: from SQLite to Postgres when one server becomes three, from the SQL driver to Hatchet when the queue outgrows the database. The old backend still holds runs in flight (an export half done, a run waiting for a reply, delayed jobs due next week) and schedules the app made at run time; the app holds refs to those runs in its own tables. Stopping the old backend loses them, and draining it first means a window where no new work is accepted.

Package `cutover` joins the old and the new client into one: new work goes to the new backend, the old one keeps running what it holds, and every ref says which backend it belongs to, so `Status`, `Cancel` and the run queries reach the right one. `Remaining` tells the app when the old backend is empty and can be dropped.

## Before you start

- Two clients, both unstarted and owned by the cutover client (it registers and starts them): any drivers, for example `sqlstore` on SQLite as the old one and `sqlstore` on Postgres as the new one.
- `Remaining` needs `CapRunQuery` on the old backend; `MoveSchedules` needs `CapScheduleList` there. The memory, SQL and Redis drivers have both.
- The runnable program for this page is [examples/backend-cutover](../../examples/backend-cutover/).

## Join the two backends

`cutover.New(old, new, opts...)` returns a `workflow.Client`. `Register` and `Start` go to both, so the old backend keeps the handlers of the runs it holds:

<!-- snippet: examples/backend-cutover/main.go#client -->
```go
oldBackend, newBackend := memory.New(), memory.New()
c := cutover.New(oldBackend, newBackend)   // schedules go to the new backend
if err := c.Register(export); err != nil { // on both: the old one finishes what it holds
	return err
}
if err := c.Start(ctx); err != nil {
	return err
}
```

A workflow the old backend cannot run (its `CheckCaps` fails, for a feature only the new backend has) is registered on the new backend only; `NewOnly(name)` reports it.

## New work and old refs

`Enqueue` goes to the new backend only, and its ref carries the prefix `new:` in front of the driver's own ID:

<!-- snippet: examples/backend-cutover/main.go#enqueue -->
```go
ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "export", Payload: []byte(`"april"`)})
if err != nil {
	return err
}
fmt.Println("enqueued:", ref.RunID) // new:<the new backend's own ID>
```

A ref the app stored before the cutover has no prefix. `Status`, `Cancel`, `Result`, `Retry`, `Delete`, `Reschedule`, `EditRun`, `ResetRun` and `CancelStep` try it on the old backend first and on the new one when the old one answers `ErrNotFound`. Refs with a prefix (`old:` or `new:`) go straight to their backend; `Runs` merges both backends' feeds, newest first, with prefixed refs:

<!-- snippet: examples/backend-cutover/main.go#status -->
```go
for _, r := range []workflow.RunRef{stored, ref} {
	st, err := c.Status(ctx, r) // an unprefixed ref stored before the cutover is looked up on the old backend first
	if err != nil {
		return err
	}
	fmt.Printf("status of %s: %s\n", r.RunID, st)
}
page, err := c.Runs(ctx, workflow.RunQuery{Workflow: "export"}) // both backends, one feed
if err != nil {
	return err
}
```

Two backends can hand out the same bare ID (two memory drivers both count from `run-1`), which is why every ref this client returns names its backend. `Signal` and `SignalWaiting` go to both backends, since a waiting run may sit on either (`SignalWaiting` only when both implement `workflow.TransientSignaller`, and it returns the total resumed); `CancelAll`, `Pause` and `Resume` apply to both; `Drain` drains both, `Nodes` lists both, `Health` is ready while the new backend is, and `OnRunFinished` observes both with prefixed refs. Step handlers see the prefixed `RunID` in `StepInfo`.

## Move the schedules

Schedules live on one backend, the new one by default, since a schedule on both would fire every tick twice. `Workflow.Cron` is registered on that backend only, so a driver that keeps declared crons in its store drops the old copy at `Start`. `cutover.SchedulesOn(cutover.SideOld)` keeps them on the old backend instead, until the app is ready to move them.

Schedules the app made with `Client.Schedule` on the old backend stay there until it removes them. `MoveSchedules` copies each one the old backend lists to the new backend, pauses the copy when the original was paused, and unschedules the original; a `ScheduleInfo` carries no `Input`, so the app supplies it:

<!-- snippet: examples/backend-cutover/main.go#move -->
```go
inputs := map[string][]byte{"nightly": []byte(`"nightly"`)} // a ScheduleInfo carries no Input: the app supplies it
moved, err := c.MoveSchedules(ctx, func(info workflow.ScheduleInfo) ([]byte, error) {
	return inputs[info.Name], nil
})
if err != nil {
	return err
}
for _, m := range moved { // a cutover.Moved: Name, Workflow, From (old:...), To (new:...)
	fmt.Printf("moved %q from %s to %s\n", m.Name, m.From, m.To)
}
```

`MoveSchedules` returns one `cutover.Moved` per schedule moved: its `Name` and `Workflow`, `From` (the old, `old:`-prefixed ID) and `To` (the new, `new:`-prefixed ID). Store the new IDs wherever the app kept the old ones. It stops at the first failure and returns what it moved so far; a schedule is never left firing on both, because when the old copy cannot be removed the new copy is unscheduled again before the error is returned. Run it once, from one process.

The two backends are the two values of `cutover.Side`: `cutover.SideOld` and `cutover.SideNew`, which are also the prefixes of every ID the client hands out. An app not ready to move its schedules yet builds the client with `cutover.SchedulesOn(cutover.SideOld)`:

<!-- snippet: examples/backend-cutover/main.go#keep-old -->
```go
c := cutover.New(memory.New(), memory.New(),
	cutover.SchedulesOn(cutover.SideOld), // Workflow.Cron and Schedule stay on the old backend
)
```

Its schedules then land on the old backend, and `MoveSchedules` refuses, since this client is meant to keep them there:

<!-- snippet: examples/backend-cutover/main.go#sides -->
```go
id, err := c.Schedule(ctx, workflow.Schedule{Name: "weekly", Workflow: "report", Cron: "0 6 * * 1", Location: time.UTC})
if err != nil {
	return err
}
side, _, _ := strings.Cut(string(id), ":") // every ID this client hands out starts with its Side
fmt.Printf("schedule on the old side: %t (%s)\n", cutover.Side(side) == cutover.SideOld, id)
_, err = c.MoveSchedules(ctx, func(workflow.ScheduleInfo) ([]byte, error) { return nil, nil })
fmt.Println("MoveSchedules refused under SchedulesOn(SideOld):", errors.Is(err, workflow.ErrInvalid))
```

When the app is ready, it builds the client again with the default (`SideNew`) and calls `MoveSchedules`.

`Schedules` lists both backends, so what is still on `old:` is visible. `Schedule` returns an ID with its backend's prefix; `Unschedule`, `PauseSchedule` and `ResumeSchedule` decode it, and an unprefixed ID (stored before the cutover) is tried on the old backend first.

## Retire the old backend

`Remaining` counts the old backend's runs that have not finished (scheduled, queued, running, waiting or retrying):

<!-- snippet: examples/backend-cutover/main.go#remaining -->
```go
n, err := c.Remaining(ctx)
if err != nil {
	return err
}
fmt.Println("unfinished runs on the old backend:", n)
```

At zero, with the schedules moved, nothing more will run there: deploy the app with the new client alone. Keep the cutover client while you may still `Retry` a failed run of the old backend, since a retry re-opens it there.

## Capabilities

`Capabilities` answers for the calls the client routes:

| Family | Whose | Capabilities |
|---|---|---|
| Enqueue path | the new backend's | every capability not named below: what `Enqueue`, `Register` and `Start` check |
| Runs | both backends' | `CapCancel`, `CapRunQuery`, `CapRetryRun`, `CapDeleteRun`, `CapCancelAll`, `CapRunStates`, `CapErrorHistory`, `CapLabelPrefix`, `CapDueQuery`, `CapResult`, `CapPauseWorkflow`, `CapPauseQueue`, `CapDrain`, `CapNodeIdentity`, `CapRunFinished` (which also needs both to implement `workflow.RunNotifier`) |
| Schedules | the schedule backend's | `CapCron`, `CapCronZone`, `CapCronSeconds`, `CapScheduleWhen`, `CapScheduleWindow`, `CapSchedulePause`, `CapScheduleList`, `CapOverlapSkip`, `CapScheduleHistory`, `CapScheduleActive`, `CapScheduleKeep`, `CapTickTime` and the misfire policies |
| Not offered | none | `CapScheduleTrigger`, `CapTickEvents`, `CapWorkerScale`, `CapClosure`, `CapForceRun`, `CapDrainReport`: their interfaces are not routed; call `c.Old()` or `c.New()` for those |

## Try it

```sh
cd examples
go run ./backend-cutover
```

```text
-- New work goes to the new backend
enqueued: new:run-2
status of run-2: running
status of new:run-2: running
runs: new:run-2, old:run-2
-- Move the schedules
schedule "nightly" on old:schedule-4
moved "nightly" from old:schedule-4 to new:schedule-4
schedule "nightly" on new:schedule-4
-- Retire the old backend
unfinished runs on the old backend: 1
after the export finished: 0 - the app can drop the old backend
-- Keep the schedules on the old backend for now
schedule on the old side: true (old:schedule-1)
MoveSchedules refused under SchedulesOn(SideOld): true
```

## Pitfalls

- Nothing is enqueued on the old backend, not even when the new one is down: the cutover client does not fail over. For that, see [failover](../reliability/failover.md).
- An unprefixed ref is looked up on the old backend first. If the app also stores bare IDs it got from the new backend directly (not through this client), and both backends could hand out the same ID, store the prefixed ref instead.
- `Signal` goes to both backends; when one fails the other has already taken the event, and the error names the failing one, so a retried `Signal` may reach that one twice.
- `MoveSchedules` skips declared crons (they follow `Register`), needs the input func (`ErrInvalid` without it), refuses under `SchedulesOn(SideOld)`, and needs `SchedulePauser` on the new backend for a paused schedule. It stops at the first failure and never leaves a schedule on both: when the old copy cannot be removed, the new copy is removed again. Run it once, from one process.
- A schedule's `ScheduleWhen` condition is not in its `ScheduleInfo` and does not move; set it again on the new backend.
- An optional interface whose capability is missing on a backend it needs is refused with an `UnsupportedError`, never answered from one backend alone. The calls without a capability (`Reschedule`, `EditRun`, `ResetRun`, `CancelStep`, `SignalWaiting`) are refused the same way when the backend they reach does not implement them, and `workflow.Supports(c, op)` is true only when both backends do.

## Related

- [Failover](../reliability/failover.md): a second backend that takes enqueues while the first is down.
- [Migrations](migrations.md): the SQL driver's schema on the new database.
- [Deploy](deploy.md): rolling the app out with the new client alone once `Remaining` is zero.
- Example: [examples/backend-cutover](../../examples/backend-cutover/).
