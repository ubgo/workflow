// Package cutover moves an app from one backend to another without
// stopping it (BK-13): from the memory driver to sqlstore, from SQLite to
// Postgres, from sqlstore to Redis or Hatchet. It joins an old and a new
// workflow client into one: new work goes to the new backend, the old one
// keeps running the runs it already holds, and every RunRef and schedule
// ID says which backend it belongs to, so Status, Cancel and the run
// queries reach the right one. When Remaining reports that the old
// backend holds no unfinished run, the app drops it and uses the new
// client alone.
//
//	c := cutover.New(oldClient, newClient)
//	c.Register(wf) // on both: old still runs what it holds
//	c.Start(ctx)   // starts both
//	ref, err := c.Enqueue(ctx, job)       // to the new backend: "new:<id>"
//	st, err := c.Status(ctx, storedRef)   // an old ref still works
//	n, err := c.Remaining(ctx)            // 0: retire the old backend
//
// # Where calls go
//
// Register and Start go to both. A workflow the old backend cannot run
// (workflow.CheckCaps against its Capabilities fails, say a feature only
// the new backend has) is registered on the new backend only: the old one
// cannot hold a run of it. Enqueue goes to the new backend only; there is
// no failover to the old one (see package failover for that). Signal goes
// to both, since a waiting run may sit on either; when one fails, the
// other has already taken the event and the error names the failing one.
//
// # Run references
//
// Every RunRef this client returns, and every RunEvent, RunInfo and
// StepInfo it passes on, carries "old:" or "new:" in front of the
// driver's own IDs, which follow unchanged. Status, Cancel, Result, Retry
// and Delete go to the backend the prefix names. A ref without a prefix is
// one the app stored before the cutover, so it is tried on the old
// backend first and on the new one when the old one does not know it
// (ErrNotFound). Runs merges both backends' feeds, newest first, with a
// cursor that records each feed's position; CancelAll, Pause and Resume
// apply to both; Drain drains both and Done closes when both stopped;
// Nodes lists both; Health is live while both are and ready while the new
// one is.
//
// # Schedules
//
// Schedules live on one backend, the new one by default (SchedulesOn
// moves them to the old one until the app is ready): a schedule on both
// would fire every tick twice. Workflow.Cron and the other schedule
// fields of a workflow are registered on that backend only; the other
// backend's copy has none, so a driver that keeps declared crons in its
// store (sqlstore, Redis) drops them there at Start. Schedules the app
// made with Client.Schedule on the old backend stay there until it
// removes them: MoveSchedules copies each one the old backend lists to
// the new backend and unschedules it on the old one. Schedule returns an
// ID with the backend's prefix; Unschedule, PauseSchedule and
// ResumeSchedule decode it, and an unprefixed ID (stored before the
// cutover) is tried on the old backend, then the new one. Schedules lists
// both backends' schedules, so the ones left to move are visible.
//
// # Capabilities
//
// Capabilities answers for the calls this client routes:
//
//   - New-backend capabilities: everything Enqueue, Register and Start
//     check (every capability not named below) is the new backend's,
//     because new work runs there.
//   - Shared capabilities: CapCancel, CapRunQuery, CapRetryRun,
//     CapDeleteRun, CapCancelAll, CapRunStates, CapErrorHistory,
//     CapLabelPrefix, CapDueQuery, CapResult, CapPauseWorkflow,
//     CapPauseQueue, CapDrain, CapNodeIdentity and CapRunFinished need
//     both backends, because the runs and workers they reach may sit on
//     either (CapRunFinished also needs both to implement
//     workflow.RunNotifier).
//   - Schedule capabilities: CapCron, CapCronZone, CapCronSeconds,
//     CapScheduleWhen, CapScheduleWindow, CapSchedulePause,
//     CapScheduleList, CapOverlapSkip, CapScheduleHistory,
//     CapScheduleActive, CapScheduleKeep, CapTickTime and the misfire
//     policies are the schedule backend's.
//   - Not offered: CapScheduleTrigger, CapTickEvents, CapWorkerScale,
//     CapClosure, CapForceRun and CapDrainReport, whose interfaces this
//     client does not route; call the backend itself for those.
//
// Each optional interface the client implements refuses with an
// UnsupportedError when Capabilities lacks its bit or a backend it needs
// does not implement it, never answering from one backend alone.
package cutover
