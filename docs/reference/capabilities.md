# Capabilities

A driver declares what it can really do as a `workflow.Caps` set, returned by `Client.Capabilities()`. `Start` compares it with what each registered workflow needs and refuses a workflow the driver cannot honour with an `UnsupportedError` naming the missing capabilities, and `Enqueue` and `Schedule` do the same for a job or schedule. A missing feature is an error at start-up, never a field silently dropped at run time. A capability is claimed only when the conformance suite proves it against the real backend.

## Capability matrix

Y means the driver declares the capability. The Fake column is `workflowtest.NewFake()` with no options; `FakeCaps` and `FakeLike` change it. A failover client declares the intersection of its two backends, with the exceptions in the last column.

| Capability | What it promises | Needed by | memory | sqlstore | hatchet | redis | Fake | failover |
|---|---|---|---|---|---|---|---|---|
| `CapDurable` | Runs, waits and sleeps survive a process restart | Nothing asks for it: durability is the app's choice of driver | — | Y | Y | Y | — | both |
| `CapDelayedStart` | `Job.RunAt` is honoured | A job with `RunAt`; `middleware.Debounce` | Y | Y | Y | Y | Y | both |
| `CapWaitForEvent` | Wait steps suspend without holding a worker until a `Signal` or their deadline | A workflow with a `Wait` step | Y | Y | Y | Y | Y | both |
| `CapDurableSleep` | Sleep steps suspend without holding a worker | A workflow with a `Sleep` step | Y | Y | Y | Y | Y | both |
| `CapConcurrencyKey` | `Workflow.Concurrency` is enforced per key | A workflow with `Concurrency` | Y | Y | Y | Y | Y | both |
| `CapRateLimit` | `Workflow.RateLimits` are enforced | A workflow with `RateLimits` | Y | Y | Y | Y | Y | both |
| `CapIdempotencyKey` | `Job.IdempotencyKey` dedups enqueues | A job with a key, a workflow with `IdempotencyRequired`; an outbox relay target unless `AllowDuplicates` | Y | Y | Y | Y | Y | both |
| `CapCron` | `Workflow.Cron` and `Client.Schedule` fire | A workflow with `Cron`; every `Schedule` call | Y | Y | Y | Y | Y | the primary's alone, or both under `WithSchedulesOnBoth` |
| `CapCancel` | `Client.Cancel` stops a run | `middleware.Debounce` | Y | Y | Y | Y | Y | both |
| `CapRunFinished` | `OnRunFinished` delivers every terminal run at least once | Code that must see every run end; `UniqueUntilFinished` and `CleanupOnFinish` need only the hook (`RunNotifier`), and fall back to their TTL or the store's expiry without this guarantee | Y | Y | — (best effort) | Y | — (never) | both, and both must implement `RunNotifier` |
| `CapRetryJitter` | `Backoff.Jitter` spreads retry delays | A step whose effective retry policy has `Jitter` | Y | Y | — | Y | Y | both |
| `CapRetryDelay` | `RetryPolicy.DelayFor` sets a retry's delay from the error (`RetryAfter`) | A step whose effective retry policy has `DelayFor` | Y | Y | — | Y | Y | both |
| `CapMisfireSkip` | `Misfire{Policy: MisfireSkip}` is honoured | A cron or schedule with that policy | Y | Y | — | Y | Y | both |
| `CapMisfireFireOnce` | `Misfire{Policy: MisfireFireOnce}`, with `Within`, is honoured | A cron or schedule with that policy | Y | Y | — | Y | Y | both |
| `CapMisfireFireAll` | `Misfire{Policy: MisfireFireAll}`, with `Within` and `Max`, is honoured | A cron or schedule with that policy | Y | Y | — | Y | Y | both |
| `CapCronZone` | `CronLocation` and `Schedule.Location` may be a zone other than UTC, evaluated in that zone across DST | A cron or schedule outside UTC | Y | Y | — (UTC only) | Y | Y | both |
| `CapQueue` | `Workflow.Queue` and `Job.Queue` route runs to named queues served by their own workers | A workflow naming a queue other than `default`; a job naming any queue | Y | Y | Y (one Hatchet worker per queue, no queue priority) | Y | Y | both |
| `CapQueueOverride` | `Job.Queue` may move a run off its workflow's queue | A job whose queue differs from its workflow's | Y | Y | — | Y | Y | both |
| `CapQueueLimitGlobal` | `QueueConfig.Limit` caps a queue's in-flight step attempts across every client sharing the store (`Workers` stays per client) | A `QueueConfig` with a non-zero `Limit` (refused at `Start` without it) | Y (one process is the whole cluster) | Y (lock row per queue, lease-aware count) | — (refused at `Start`) | — (refused at `Start`) | — (default) | both |
| `CapJobRetry` | `Job.Retry` replaces the workflow's retry policy for one run | A job with `Retry` | Y | Y | — | Y | Y | both |
| `CapRetryUntil` | `RetryPolicy.Until` stops retries at a deadline | A policy with `Until` | Y | Y | — | Y | Y | both |
| `CapRetryMaxErrors` | `RetryPolicy.MaxErrors` counts errors apart from releases | A policy with `MaxErrors` | Y | Y | — | Y | Y | both |
| `CapDiscard` | `workflow.Discard(err)` ends the run without failing it: `discarded` with `CapRunStates`, else `cancelled` | Nothing: without it `Discard` acts as `Permanent` | Y | Y | — | Y | Y | both |
| `CapDiscardExhausted` | `RetryPolicy.DiscardOnExhaust` ends a run whose retries ran out `discarded` instead of `failed`, with no `OnFailure` | A policy with `DiscardOnExhaust` (also needs `CapRunStates` to report the status) | Y | Y | — (refused by field name) | Y | — (default) | both |
| `CapRunQuery` | `RunQuerier.Runs` lists runs by filter, with a cursor | `admin` run listing | Y | Y | Y | Y | — (never) | both |
| `CapRetryRun` | `RunRetrier.Retry` re-runs a failed run | `admin` retry | Y | Y | Y (Hatchet replay; the key is not taken again on the engine) | Y | Y | both |
| `CapDeleteRun` | `RunDeleter.Delete` forgets a finished run | `admin` delete | Y | Y | — | Y | Y | both |
| `CapCancelAll` | `BulkCanceller.CancelAll` cancels every run a filter matches | `admin` cancel by filter | Y | Y | Y | Y | — (never) | both |
| `CapPauseWorkflow` | `Pauser` holds a workflow, or everything, without losing runs | `Pause(PauseWorkflow(...))`, `Pause(PauseAll())` | Y | Y | with `Config.API` (`PauseAll` refused) | Y | Y | both |
| `CapPauseQueue` | `Pauser` holds one queue | `Pause(PauseQueue(...))` | Y | Y | — | Y | Y | both |
| `CapSchedulePause` | `SchedulePauser` pauses and resumes schedules; a paused tick never fires | `PauseSchedule`, `ResumeSchedule` | Y | Y | with `Config.API` (`PauseSchedule` of `All` refused) | Y | — (never) | the primary's alone, or both under `WithSchedulesOnBoth` |
| `CapScheduleWhen` | `Workflow.ScheduleWhen` is asked before each tick | A workflow with `ScheduleWhen` | Y | Y | — | Y | Y | the primary's alone, or both under `WithSchedulesOnBoth` |
| `CapScheduleWindow` | `Schedule.Windows` and `Workflow.CronWindows` limit ticks by time of day | A schedule with `Windows`, a workflow with `CronWindows` | Y | Y | — | Y | Y | the primary's alone, or both under `WithSchedulesOnBoth` |
| `CapTickEvents` | `TickNotifier.OnTick` reports every fired, skipped and misfired tick; fired runs carry `LabelSchedule` | `AttachScheduleHooks` tick hooks | Y | Y | — | Y | — (never) | the primary's alone, or both under `WithSchedulesOnBoth` |
| `CapScheduleList` | `ScheduleLister.Schedules` lists live schedules with their next fire | `admin` schedule list | Y | Y | Y | Y | — (never) | the primary's alone, or both under `WithSchedulesOnBoth` |
| `CapDrain` | `Drainer.Drain` stops taking work and `Done` closes when in-flight steps end | Graceful shutdown | Y | Y | Y | Y | — (never) | both |
| `CapDrainReport` | `ReportingDrainer.DrainWithReport` returns a `DrainReport`: steps and runs finished, and every attempt cut off with what became of it | `workflow.DrainTimeoutReport`, `ShutdownHookReport` (both degrade to an unknown report without it) | Y | Y | Y (cut attempts are `engine`-owned; runs finished stays 0) | Y | — (never) | both (see the note below) |
| `CapRunStates` | Runs report `StatusRetrying` while a step waits on its backoff and `StatusDiscarded` after `Discard`; `RunInfo.NextAttempt` and `NextAttemptAt` are filled | A `RunQuery` or `CancelAll` filter naming either status (refused without it on Hatchet) | Y | Y | — | Y | Y | both |
| `CapChain` | `Job.Chain` runs each job after the one before it succeeds, and steps can edit the rest with `PrependToChain` and `AppendToChain` | A job with `Chain`; `PrependToChain` and `AppendToChain` | Y | Y | — | Y | Y | both |
| `CapWorkerScale` | The client implements `WorkerScaler`: queue loads and worker counts set while running | `BalanceLoop` | Y | Y | — | Y | — (never) | the primary's alone |
| `CapClosure` | The client implements `ClosureEnqueuer`: in-process Go functions run as jobs | `EnqueueClosure` | Y | — | — | — | Y | the primary's alone |
| `CapLabelPrefix` | `RunQuery.LabelPrefixes` matches a label value by prefix (an empty prefix: the key is present) | A `RunQuery` or `CancelAll` filter with `LabelPrefixes` (`CheckRunQuery` refuses it without) | Y | Y (on the label index) | — (exact metadata match only) | Y | — (never) | both |
| `CapOverlapSkip` | `OverlapSkip` on `Schedule.Overlap` or `Workflow.CronOverlap` skips a tick while the schedule's previous run is unfinished, decided from stored run state | A schedule or cron with `OverlapSkip` | Y | Y | — (refused by field name) | Y | Y | the primary's alone |
| `CapScheduleTrigger` | `ScheduleTriggerer.TriggerSchedule` runs a schedule now with its input and labels, obeying the overlap policy | `admin` trigger-now | Y | Y | Y | Y | Y | the primary's alone |
| `CapScheduleHistory` | `ScheduleInfo.Last` holds the last fire, its run and that run's status | `admin` last fire | Y | Y (stored) | Y (read from the engine's run list within `Config.ScheduleHistoryWindow`) | Y | — | the primary's alone |
| `CapEnqueueOnly` | The driver has an enqueue-only mode: `Enqueue`, `Status`, `Cancel` and `Signal` work without `Start`, and nothing runs | Web nodes that never run work | — (in-process) | Y (`WithEnqueueOnly`) | Y (`Config.EnqueueOnly`) | Y (`WithEnqueueOnly`) | — | both |
| `CapErrorHistory` | `RunInfo.Errors` keeps each failed attempt's step, error, time and stack, bounded per run | `admin` run detail | Y | Y | — | Y | — | both |
| `CapShutdownRequeue` | An attempt cut short by a clean stop runs again as the same attempt, not counted toward retries or the error history | A deploy that must not use up retries | — (stop cancels runs) | Y | — | Y | — | both |
| `CapForceRun` | `RunForcer` force-completes or force-fails a run from outside | `admin` force actions | Y | Y | — | Y | Y | both |
| `CapSnooze` | A step returning `Snooze(d)` is put back for `d` without spending an attempt | `workflow.Snooze`, `AwaitChild` | Y | Y | — | Y | — | both |
| `CapHeartbeat` | `Step.HeartbeatTimeout` is honoured and `Heartbeat` reaches the driver | A step with `HeartbeatTimeout` | Y | Y | — | Y | Y | both |
| `CapTenant` | `Job.Tenant` and `Schedule.Tenant` are stored and reported; `RunQuery.Tenant` filters | A job or schedule with `Tenant`, a query with `Tenant` | Y | Y | Y | Y | Y | both |
| `CapRunTimeout` | `Workflow.RunTimeout` ends a run that runs too long and `Workflow.OnTimeout` handles timeouts | A workflow with `RunTimeout` or `OnTimeout` | Y | Y | — | Y | Y | both |
| `CapNodeIdentity` | `StepInfo.Node`, `AttemptError.Node` and `RunInfo.AttemptedBy` name the node that ran each attempt | `RunInfo.AttemptedBy`, the admin node views | Y | Y | — | Y | — | both |
| `CapKeyFIFO` | `Concurrency.FIFO` starts the queued runs of one key in enqueue order | A workflow with `Concurrency.FIFO` | Y | Y | Y | Y | Y | both |
| `CapDueQuery` | `RunQuery.DueFrom` and `DueTo` select delayed runs; `RunInfo.RunAt` is filled | A query with a due-time range | Y | Y | Y | Y | — | both |
| `CapResult` | The client implements `ResultReader`: `RunInfo.Output` holds a succeeded run's result | `Await`, `AwaitResult`, `AwaitChild` | Y | Y | Y | Y | — | both |
| `CapCronSeconds` | `Schedule.Seconds` and `Workflow.CronSeconds` (six-field expressions, seconds first) are honoured | A schedule or cron with the seconds flag | Y | Y | Y | Y | Y | the primary's |
| `CapScheduleActive` | `Schedule.Active` and `Workflow.CronActive` limit a schedule to a start and end instant | A schedule or cron with `Active` | Y | Y | — | Y | Y | the primary's |
| `CapScheduleKeep` | `Schedule.KeepRuns` and `Workflow.CronKeepRuns` keep only the newest N finished runs of a schedule | A schedule or cron with `KeepRuns` | Y | Y | — | Y | Y | the primary's |
| `CapTickTime` | Every run a schedule fires carries the tick it stands for in `LabelTickAt` | `StepInfo.TickAt`, `workflow.TickTime` | Y | Y | Y | Y | — | the primary's |

Notes on the columns:

- **memory** keeps everything in the process, so it never claims `CapDurable`; it refuses expression keys (`workflow.KeyExpr`) at `Start`, which no capability names.
- **hatchet** implements `OnRunFinished` by polling the engine and does not claim `CapRunFinished`: an event can be missed. Hatchet evaluates crons in UTC only and never runs a missed tick, so a zone or a misfire policy is refused. Its other refusals are listed in the `hatchet` package documentation.
- **hatchet** declares `CapPauseWorkflow` and `CapSchedulePause` only when `Config.API` (the engine's REST client, `hatchet.NewAPI`) is set; there `PauseAll()`, `PauseQueue` and `PauseSchedule` of `All` are refused.
- **Fake** claims neither `CapQueueLimitGlobal` nor `CapDiscardExhausted` unless `FakeCaps` or `FakeLike` adds them (it validates the fields, it enforces no cap and runs no retries), and never claims `CapDrainReport`.
- **failover** and the `middleware.WrapClient` and `WithCodec` wrappers do not forward `DrainWithReport`: `workflow.DrainTimeoutReport` on them still drains, and returns `detailed` false, meaning the report is unknown (not that nothing was cut off). Drain the backend clients directly when you need the report.
- **redis** keeps runs, schedules, signals, queues and leases in Redis and declares every capability but `CapClosure` (a Go function cannot be stored) and `CapQueueLimitGlobal` (a run is claimed whole, so a queue's in-flight attempts are not counted across clients). Its `CapDurable` is as strong as the Redis deployment's persistence settings ([The Redis driver](../drivers/redis.md)).
- **Fake** claims the memory driver's set minus the capabilities it cannot serve by itself: `CapRunFinished` (it runs nothing, so no run ever finishes), `CapRunQuery`, `CapLabelPrefix`, `CapCancelAll`, `CapSchedulePause`, `CapScheduleList`, `CapTickEvents`, `CapDrain`, `CapWorkerScale`, and the bookkeeping it cannot do (`CapScheduleHistory`, `CapErrorHistory`, `CapEnqueueOnly`, `CapShutdownRequeue`). `FakeCaps` and `FakeLike` drop the same set. It records `Retry`, `Delete` and `Pause` itself.
- **failover** treats the schedule family (`CapCron`, `CapScheduleWhen`, `CapScheduleWindow`, `CapSchedulePause`, `CapScheduleList`, `CapTickEvents`) as the primary's alone unless `WithSchedulesOnBoth`.

## Reading and checking a set

| API | What it does |
|---|---|
| `caps.Has(want)` | True when `caps` holds every capability in `want` |
| `caps.Missing(required)` | The capabilities in `required` that `caps` lacks; zero means none |
| `caps.String()` | The names joined by `\|`, such as `Durable\|Cron`; the empty set is `none` |
| `workflow.Requires(wf)` | What a driver needs to run `wf` as written (the "Needed by" column) |
| `workflow.RequiresJob(job)` | What a job needs: `CapDelayedStart` for `RunAt`, `CapIdempotencyKey` for a key, even under `IdempotencyOff`, so a key is never dropped without an error; `CapQueue` for a `Queue`, `CapJobRetry` (and the retry capabilities of its policy) for a `Retry` |
| `workflow.CheckJob(have, job)`, `CheckJobQueue(have, wf, job)` | The `UnsupportedError` a driver returns from `Enqueue`: the job's own needs, then `CapQueueOverride` when the job leaves its workflow's queue |
| `workflow.RequiresPause(t)`, `CheckPause(have, t)` | What a `PauseTarget` needs (`CapPauseWorkflow` for a workflow or all, `CapPauseQueue` for a queue) and the refusal |
| `workflow.RequiresSchedule(s)` | What a schedule needs: `CapCron`, `CapCronZone` outside UTC, and its misfire policy's capability |
| `workflow.CheckCaps(have, wf)`, `CheckScheduleCaps(have, s)` | The `UnsupportedError` a driver returns from `Start` and `Schedule`, or nil |

The names in `String()` and in an `UnsupportedError` are the constant names without `Cap`: `Durable`, `DelayedStart`, `WaitForEvent`, `DurableSleep`, `ConcurrencyKey`, `RateLimit`, `IdempotencyKey`, `Cron`, `Cancel`, `RunFinished`, `RetryJitter`, `RetryDelay`, `MisfireSkip`, `MisfireFireOnce`, `MisfireFireAll`, `CronZone`, `Queue`, `QueueOverride`, `QueueLimitGlobal`, `JobRetry`, `RetryUntil`, `RetryMaxErrors`, `Discard`, `DiscardExhausted`, `RunQuery`, `RetryRun`, `DeleteRun`, `CancelAll`, `PauseWorkflow`, `PauseQueue`, `SchedulePause`, `ScheduleWhen`, `ScheduleWindow`, `TickEvents`, `ScheduleList`, `Drain`, `DrainReport`, `RunStates`, `Chain`, `WorkerScale`, `Closure`, `LabelPrefix`, `OverlapSkip`, `ScheduleTrigger`, `ScheduleHistory`, `EnqueueOnly`, `ErrorHistory`, `ShutdownRequeue`, `ForceRun`, `Snooze`, `Heartbeat`, `Tenant`, `RunTimeout`, `NodeIdentity`, `KeyFIFO`, `DueQuery`, `Result`, `CronSeconds`, `ScheduleActive`, `ScheduleKeep`, `TickTime`.

## Related

- [Capabilities in the core contract](../concepts/capabilities.md): why refusing beats emulating.
- [Choosing a driver](../choosing/drivers.md).
- [Errors](errors.md): `UnsupportedError` and `ErrUnsupported`.
- [Behave like your production driver](../testing/fake.md#behave-like-your-production-driver): make the Fake claim your driver's set.
