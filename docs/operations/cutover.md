# Backend cutover

Moving an app from one backend to another without a stop: from SQLite to Postgres when one server becomes three, from the SQL driver to Hatchet when the queue outgrows the database. The old backend still holds runs in flight (an export half done, a run waiting for a reply, delayed jobs due next week) and schedules the app made at run time; the app holds refs to those runs in its own tables. Stopping the old backend loses them, and draining it first means a window where no new work is accepted.

Package `cutover` joins the old and the new client into one ([`cutover.New`](ds:block?id=cutover-new-wkttdfys)): new work goes to the new backend, the old one keeps running what it holds, and every ref says which backend it belongs to, so `Status`, `Cancel` and the run queries reach the right one. `Remaining` tells the app when the old backend is empty and can be dropped.

## Before you start

- Two clients, both unstarted and owned by the cutover client (it registers and starts them, [`cutover.New`](ds:block?id=cutover-new-wkttdfys)): any drivers, for example `sqlstore` on SQLite as the old one and `sqlstore` on Postgres as the new one.
- [`Remaining`](ds:block?id=cutover-client-remaining-qasz4mwf) needs `CapRunQuery` on the old backend; [`MoveSchedules`](ds:block?id=cutover-client-moveschedules-yrmb5kjs) needs `CapScheduleList` there. The [memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-paritycaps-e5y5vqhk), [Redis](ds:block?id=redis-caps-bfgdst9t) and [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z) drivers have both.
- The runnable program for this page is [examples/backend-cutover](../../examples/backend-cutover/).

## Join the two backends

[`cutover.New(old, new, opts...)`](ds:block?id=cutover-new-wkttdfys) returns a `workflow.Client`. [`Register`](ds:block?id=cutover-client-register-dxfjnaqq) and `Start` go to both, so the old backend keeps the handlers of the runs it holds:

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

A workflow the old backend cannot run (its [`CheckCaps`](ds:block?id=checkcaps-eg44ezuh) fails, for a feature only the new backend has) is registered on the new backend only; [`NewOnly(name)`](ds:block?id=cutover-client-newonly-hp38vd62) reports it.

## New work and old refs

[`Enqueue`](ds:block?id=cutover-client-enqueue-mdktyder) goes to the new backend only, and its ref carries the prefix `new:` in front of the driver's own ID ([`encodeRef`](ds:block?id=cutover-encoderef-4er3m4bu)):

<!-- snippet: examples/backend-cutover/main.go#enqueue -->
```go
ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "export", Payload: []byte(`"april"`)})
if err != nil {
	return err
}
fmt.Println("enqueued:", ref.RunID) // new:<the new backend's own ID>
```

A ref the app stored before the cutover has no prefix. `Status`, `Cancel`, `Result`, `Retry`, `Delete`, `Reschedule`, `EditRun`, `ResetRun` and `CancelStep` try it on the old backend first and on the new one when the old one answers `ErrNotFound` ([`onRef`](ds:block?id=cutover-client-onref-c9jkt4up)). Refs with a prefix (`old:` or `new:`) go straight to their backend; [`Runs`](ds:block?id=cutover-client-runs-tsymaquu) merges both backends' feeds, newest first, with prefixed refs:

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

Two backends can hand out the same bare ID (two memory drivers both count from `run-1`), which is why every ref this client returns names its backend. [`Signal`](ds:block?id=cutover-client-signal-4xrqzvqc) and [`SignalWaiting`](ds:block?id=cutover-client-signalwaiting-79gpg9qc) go to both backends, since a waiting run may sit on either (`SignalWaiting` only when both implement `workflow.TransientSignaller`, and it returns the total resumed); `CancelAll`, `Pause` and `Resume` apply to both; [`Drain`](ds:block?id=cutover-client-drain-zqjmfrsf) drains both, [`Nodes`](ds:block?id=cutover-client-nodes-awkvzshn) lists both, [`Health`](ds:block?id=cutover-client-health-j9hxbjx3) is ready while the new backend is, and [`OnRunFinished`](ds:block?id=cutover-client-onrunfinished-tvwmvejt) observes both with prefixed refs. Step handlers see the prefixed `RunID` in `StepInfo`.

## Move the schedules

Schedules live on one backend, the new one by default, since a schedule on both would fire every tick twice. `Workflow.Cron` is registered on that backend only ([`Register`](ds:block?id=cutover-client-register-dxfjnaqq) strips it from the other copy), so a driver that keeps declared crons in its store drops the old copy at `Start`. [`cutover.SchedulesOn(cutover.SideOld)`](ds:block?id=cutover-scheduleson-ya9de99v) keeps them on the old backend instead, until the app is ready to move them.

Schedules the app made with `Client.Schedule` on the old backend stay there until it removes them. [`MoveSchedules`](ds:block?id=cutover-client-moveschedules-yrmb5kjs) copies each one the old backend lists to the new backend, pauses the copy when the original was paused, and unschedules the original ([`moveOne`](ds:block?id=cutover-client-moveone-69cewga5)); a `ScheduleInfo` carries no `Input`, so the app supplies it:

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

`MoveSchedules` returns one [`cutover.Moved`](ds:block?id=cutover-moved-hcz4wvs2) per schedule moved: its `Name` and `Workflow`, `From` (the old, `old:`-prefixed ID) and `To` (the new, `new:`-prefixed ID). Store the new IDs wherever the app kept the old ones. It stops at the first failure and returns what it moved so far; a schedule is never left firing on both, because when the old copy cannot be removed the new copy is unscheduled again before the error is returned. Run it once, from one process.

The two backends are the two values of [`cutover.Side`](ds:block?id=cutover-side-a33cayts): `cutover.SideOld` and `cutover.SideNew`, which are also the prefixes of every ID the client hands out. An app not ready to move its schedules yet builds the client with `cutover.SchedulesOn(cutover.SideOld)`:

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

[`Schedules`](ds:block?id=cutover-client-schedules-kcsh3twn) lists both backends, so what is still on `old:` is visible. [`Schedule`](ds:block?id=cutover-client-schedule-3ktbbxq5) returns an ID with its backend's prefix; [`Unschedule`](ds:block?id=cutover-client-unschedule-n84rqfj5), `PauseSchedule` and `ResumeSchedule` ([`onSchedule`](ds:block?id=cutover-client-onschedule-uhfgu7ju)) decode it, and an unprefixed ID (stored before the cutover) is tried on the old backend first.

## Retire the old backend

[`Remaining`](ds:block?id=cutover-client-remaining-qasz4mwf) counts the old backend's runs that have not finished (scheduled, queued, running, waiting or retrying):

<!-- snippet: examples/backend-cutover/main.go#remaining -->
```go
n, err := c.Remaining(ctx)
if err != nil {
	return err
}
fmt.Println("unfinished runs on the old backend:", n)
```

At zero, with the schedules moved, nothing more will run there: deploy the app with the new client alone. Keep the cutover client while you may still `Retry` a failed run of the old backend, since a retry re-opens it there ([`Remaining`](ds:block?id=cutover-client-remaining-qasz4mwf)).

## Capabilities

[`Capabilities`](ds:block?id=cutover-client-capabilities-sq86kg5y) answers for the calls the client routes:

| Family | Whose | Capabilities |
|---|---|---|
| Enqueue path | the new backend's | every capability not named below: what `Enqueue`, `Register` and `Start` check |
| Runs | both backends' ([`sharedCaps`](ds:block?id=cutover-sharedcaps-848dhjab)) | `CapCancel`, `CapRunQuery`, `CapRetryRun`, `CapDeleteRun`, `CapCancelAll`, `CapRunStates`, `CapErrorHistory`, `CapLabelPrefix`, `CapDueQuery`, `CapResult`, `CapPauseWorkflow`, `CapPauseQueue`, `CapDrain`, `CapNodeIdentity`, `CapRunFinished` (which also needs both to implement `workflow.RunNotifier`) |
| Schedules | the schedule backend's ([`scheduleCaps`](ds:block?id=cutover-schedulecaps-u6qn43gw)) | `CapCron`, `CapCronZone`, `CapCronSeconds`, `CapScheduleWhen`, `CapScheduleWindow`, `CapSchedulePause`, `CapScheduleList`, `CapOverlapSkip`, `CapScheduleHistory`, `CapScheduleActive`, `CapScheduleKeep`, `CapTickTime` and the misfire policies |
| Not offered | none ([`unroutedCaps`](ds:block?id=cutover-unroutedcaps-a996f4gq)) | `CapScheduleTrigger`, `CapTickEvents`, `CapWorkerScale`, `CapClosure`, `CapForceRun`, `CapDrainReport`: their interfaces are not routed; call `c.Old()` or `c.New()` for those |

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

- Nothing is enqueued on the old backend, not even when the new one is down: the cutover client does not fail over ([`Enqueue`](ds:block?id=cutover-client-enqueue-mdktyder)). For that, see [failover](../reliability/failover.md).
- An unprefixed ref is looked up on the old backend first ([`onRef`](ds:block?id=cutover-client-onref-c9jkt4up)). If the app also stores bare IDs it got from the new backend directly (not through this client), and both backends could hand out the same ID, store the prefixed ref instead.
- [`Signal`](ds:block?id=cutover-client-signal-4xrqzvqc) goes to both backends; when one fails the other has already taken the event, and the error names the failing one, so a retried `Signal` reaches the backend that took it the first time twice.
- [`MoveSchedules`](ds:block?id=cutover-client-moveschedules-yrmb5kjs) skips declared crons (they follow `Register`), needs the input func (`ErrInvalid` without it), refuses under `SchedulesOn(SideOld)`, and needs `SchedulePauser` on the new backend for a paused schedule. It stops at the first failure and never leaves a schedule on both: when the old copy cannot be removed, the new copy is removed again. Run it once, from one process.
- A schedule's `ScheduleWhen` condition is not in its `ScheduleInfo` and does not move ([`moveOne`](ds:block?id=cutover-client-moveone-69cewga5)); set it again on the new backend.
- An optional interface whose capability is missing on a backend it needs is refused with an `UnsupportedError`, never answered from one backend alone ([`need`](ds:block?id=cutover-client-need-zy5t2vf3), [`both`](ds:block?id=cutover-both-wgpnkv98)). The calls without a capability (`Reschedule`, `EditRun`, `ResetRun`, `CancelStep`, `SignalWaiting`) are refused the same way when the backend they reach does not implement them, and `workflow.Supports(c, op)` is true only when both backends do ([`Supports`](ds:block?id=cutover-client-supports-k9xeak5n)).

## Related

- [Failover](../reliability/failover.md): a second backend that takes enqueues while the first is down.
- [Migrations](migrations.md): the SQL driver's schema on the new database.
- [Deploy](deploy.md): rolling the app out with the new client alone once `Remaining` is zero.
- Example: [examples/backend-cutover](../../examples/backend-cutover/).
