# Choose a method

For the jobs every app has, the library often offers two or three ways to do it. They differ in what they guarantee when a process dies, a caller retries or a burst arrives, and in which drivers support them. This page picks between them: how to enqueue so the job matches your data, how to declare recurring work, when a batch is worth it, which of the three duplicate guards you want, how to delay work, when to stop retrying and where to react to a run's end.

## Enqueue, EnqueueTx or the outbox

The question: when your handler writes rows and enqueues a job, what happens if the process dies between the two, or the transaction rolls back?

| Method | Guarantee | Drivers | Enqueue middleware | Guide | Example |
|---|---|---|---|---|---|
| `Client.Enqueue` after your commit | The job runs if the process survives until the call returns. A crash between commit and enqueue loses the job; an enqueue before the commit runs for data a rollback then removes | all | runs | [Enqueue a job](../jobs/enqueue.md#enqueue-a-job) | [quickstart](../../examples/quickstart/) |
| `sqlstore.Client.EnqueueTx(ctx, tx, job)` | The run is a row in your transaction: it exists if and only if your writes commit. Admitted within the poll interval after the commit (one second by default), or at once after `Client.Wake()` | sqlstore, with the queue in the same database as your data | **does not run** (its side effects would not roll back with your transaction) | [Enqueue inside your transaction](../reliability/transactional-enqueue.md#enqueue-inside-your-transaction) | [transactional-enqueue](../../examples/transactional-enqueue/) |
| `outbox.Outbox.Write(ctx, tx, job)` plus a `Relay` | An outbox row in your transaction; a relay enqueues it on any client after the commit, at least once, with the job's key or the entry ID as idempotency key | any target client (Hatchet, memory, a wrapped client, sqlstore in another database); the outbox table needs SQLite, Postgres or MySQL | runs, at relay time on the target | [Write jobs inside your transaction](../reliability/outbox.md#write-jobs-inside-your-transaction) | [outbox](../../examples/outbox/) |

- If your data and your queue share one SQL database and you need no enqueue middleware, use `EnqueueTx`: no relay to run, nothing extra to monitor.
- If the queue is elsewhere (Hatchet, another database), or you need unique jobs, debounce or tracing on these enqueues, use the outbox.
- If the job does not depend on rows you just wrote (a cron fire, a manual retry button), plain `Enqueue` is enough.
- Pitfall: the outbox relays at least once. A relay that dies after the target accepted an entry but before deleting it enqueues it again; the target drops the second one only while the first run is unfinished. Handlers must be idempotent.
- Pitfall: `NewRelay` refuses a target without `CapIdempotencyKey`; `AllowDuplicates` accepts one and means a redelivered entry always runs twice.
- Pitfall: on Postgres, call `EnqueueTx` under READ COMMITTED (the default). A REPEATABLE READ transaction cannot see an idempotency key committed after its snapshot and fails instead of returning the holder.

## Workflow.Cron or Schedule

| Method | Use it for | Lifetime | Drivers | Guide | Example |
|---|---|---|---|---|---|
| `Workflow.Cron` (plus `CronLocation`, `Misfire`) | Recurring work that is part of the app: the nightly cleanup, the hourly sync | Declared with the workflow at `Register`; registering again replaces it, never duplicates | every driver with `CapCron` (all three) | [Cron on a workflow](../schedules/cron.md#cron-on-a-workflow) | [cron-schedules](../../examples/cron-schedules/) |
| `Client.Schedule` / `Unschedule` | Recurring work created at run time: per tenant, per user, configured in your UI | Until `Unschedule` with the stored `ScheduleID` | every driver with `CapCron` | [Schedules at run time](../schedules/cron.md#schedules-at-run-time) | [cron-schedules](../../examples/cron-schedules/) |

- If every deployment runs it on the same expression, declare it on the workflow.
- If it differs per tenant or a user can change it, use `Schedule`, store the returned ID next to the tenant, and `Unschedule` it when the tenant changes or leaves. `Unschedule("")` is a no-op, so pass the stored ID unchecked.
- Pitfall: calling `Schedule` again with the same `Name` updates the schedule in place on sqlstore (an upsert by workflow and name), but creates a second schedule on the memory and Hatchet drivers. On those, unschedule the old ID before scheduling a replacement, and do not call `Schedule` at every boot.
- Pitfall: `Schedule.Location` is required (`time.UTC` for UTC). Hatchet refuses any other zone, and refuses misfire policies: crons there run in UTC and a tick missed while the engine was down is dropped.
- Pitfall: a declared cron runs on every client that registers the workflow. On sqlstore a tick fires once across all of them; on the memory driver each process fires its own.

## A batch or plain enqueues

| Method | Use it for | Guide | Example |
|---|---|---|---|
| A loop of `Enqueue` calls | Independent jobs nobody waits on as a group | [Enqueue a job](../jobs/enqueue.md#enqueue-a-job) | [quickstart](../../examples/quickstart/) |
| `batch.New(client, store).Dispatch(ctx, batch.Batch{...})` | A group with an outcome: run `Then` when every job succeeded, `Catch` on the first failure, `Finally` when all finished; read `Progress`; `Cancel` the rest | [Dispatch a batch](../batches/batches.md#dispatch-a-batch) | [batches](../../examples/batches/) |
| One workflow with parallel steps | A fixed fan-out known when you write the code (charge, then receipt and shipping in parallel, then notify) | [Steps that depend on other steps](../workflows/steps.md#steps-that-depend-on-other-steps) | [first-workflow](../../examples/first-workflow/) |

- If the number of jobs depends on data (import 10 000 rows, email every member), use a batch. If the shape is fixed, a workflow of steps is simpler and carries outputs from step to step.
- If the batch must survive a restart of the process that dispatched it, give it a durable store (`sqlstore.Client.BatchStore()`) and a durable driver ([A durable store](../batches/durability.md#a-durable-store)).
- If you are on Hatchet, a batch starts as bulk triggers (one call per up to 1000 jobs) instead of one call per job ([Native group runs](../batches/native-groups.md#native-group-runs)).
- Pitfall: a batch learns that a job ended through completion sources, connected by `Manager.Attach`: the run-finished hook where it is guaranteed, a `Status` poller otherwise. Without `Attach`, callbacks never fire ([How a batch learns a job ended](../batches/completion.md#how-a-batch-learns-a-job-ended)).
- Pitfall: without `AllowFailures`, the first failure cancels the batch's running jobs.

## Idempotency key, unique or debounce

Three guards against duplicate work, answering three different questions:

| Guard | Question it answers | Second job while the first is unfinished | After the first finished | Drivers | Guide | Example |
|---|---|---|---|---|---|---|
| `Job.IdempotencyKey` | "Is this the same request again?" (a retried webhook, a double-clicked button) | Starts nothing; `Enqueue` returns the first run's `RunRef` and a nil error | The key is released: the same key runs again | the driver must have `CapIdempotencyKey` (all three; Hatchet refuses a key with `RunAt`) | [Idempotency keys](../jobs/idempotency.md#idempotency-keys) | [idempotency](../../examples/idempotency/) |
| `middleware.Unique` / `UniqueUntilFinished` | "Is a job for this thing already queued?" (one sync per account at a time) | Refused: `Enqueue` returns an error wrapping `ErrDuplicate` | `Unique`: still refused until the TTL ends. `UniqueUntilFinished`: freed when the run ends (the TTL stays as a safety net) | any client; `UniqueUntilFinished` needs `RunNotifier`; a shared store for several processes | [Unique for a time](../reliability/unique-jobs.md#unique-for-a-time) | [unique-jobs](../../examples/unique-jobs/) |
| `middleware.Debounce` | "Should a burst collapse into one run, after it goes quiet?" (reindex after the last of many edits) | The pending run is cancelled and replaced; the last job of the burst wins and runs once the key has been quiet for the window (`maxWait` caps the delay) | A new burst starts | needs `CapDelayedStart` and `CapCancel` (refused otherwise); a shared store for several processes | [Debounce a burst](../reliability/debounce.md#debounce-a-burst) | [debounce](../../examples/debounce/) |

- If a caller may send the same request twice and you want one run with the same answer, use an idempotency key. It is enforced by the driver, so it holds across every process on a durable driver, with no extra store.
- If you want at most one queued or running job per entity, and the caller should be told, use `UniqueUntilFinished`.
- If you want the latest state processed once after changes stop, use debounce.
- If a workflow must never run without a key, set `Workflow.Idempotency` to `IdempotencyRequired`; an enqueue without one then fails ([Require or ignore keys](../jobs/idempotency.md#require-or-ignore-keys)).
- Pitfall: do not set `IdempotencyKey` on debounced jobs. The pending run holds the key, so the replacement would be folded into it instead of replacing it.
- Pitfall: `Unique` and `Debounce` keep their state in a store. The default in-memory stores hold only inside one process; with several processes enqueueing, pass `sqlstore.UniqueStore` / `sqlstore.DebounceStore` or your own ([Bring your own store](../extending/custom-stores.md#bring-your-own-store)).
- Pitfall: a failover client requires an idempotency key on every job, because an ambiguous primary failure can send the job to both backends ([Why a key is required](../reliability/failover.md#why-a-key-is-required)).

## Concurrency limit or rate limit

| Policy | Caps | The run over the limit | Guide | Example |
|---|---|---|---|---|
| `Workflow.Concurrency` | Simultaneous runs per key (`MaxRuns`) | Waits, is dropped or cancelled, or cancels the running one, by `Strategy` | [Limit runs per key](../jobs/concurrency.md#limit-runs-per-key) | [concurrency](../../examples/concurrency/) |
| `Workflow.RateLimits` | Run starts per key per window (`Limit` per `Per`) | Stays queued until the window allows it; never dropped | [Rate limits](../jobs/concurrency.md#rate-limits) | [concurrency](../../examples/concurrency/) |
| `middleware.ThrottleErrors` | Calls to a failing dependency (a circuit breaker per key) | Fails fast with `ErrThrottled` until the cooldown passes | [Throttle a failing dependency](../middleware/step-middleware.md#throttle-a-failing-dependency) | [middleware-stack](../../examples/middleware-stack/) |

- If a downstream allows N calls at once, use a concurrency limit; if it allows N calls per minute, a rate limit; if it is down, the circuit breaker stops you hammering it.
- Pitfall: keys are Go functions on memory, sqlstore and Redis and CEL expressions on Hatchet; each refuses the other form.

## RunAt, a sleep step or a schedule

Three ways to make work happen later, for three different shapes of "later":

| Method | Use it for | Needs | Guide | Example |
|---|---|---|---|---|
| `Job.RunAt` | One job that must not start before a point in time: the reminder at 09:00 tomorrow, the trial-end email | `CapDelayedStart` (all three drivers) | [Run a job later](../jobs/delays.md#run-a-job-later) | [delayed-jobs](../../examples/delayed-jobs/) |
| `Step.Sleep` | A pause between two steps of one run: charge now, send the survey three days later | `CapDurableSleep` (every driver) | [Sleep inside a workflow](../workflows/sleep.md#sleep-inside-a-workflow) | [signals-and-sleep](../../examples/signals-and-sleep/) |
| `Workflow.Cron` or `Client.Schedule` | Work that repeats | `CapCron` | [Cron on a workflow](../schedules/cron.md#cron-on-a-workflow) | [cron-schedules](../../examples/cron-schedules/) |

- If the delay is measured from an absolute time, use `RunAt`. If it is measured from an earlier step finishing, use a sleep step, so the first step's output carries over and the run stays one run with one status.
- If the job may be called off before it starts (the user replied, the order shipped), keep its `RunRef` and `Cancel` it ([Cancel a run](../jobs/cancel-and-status.md#cancel-a-run)). For a run that waits on a person, a [wait step](../workflows/signals.md#wait-for-an-event) with a deadline is usually better than a sleep, since a `Signal` ends the wait early.
- Pitfall: on the memory driver, a delayed job and a sleeping run are lost when the process exits. Neither is refused, because durability is never inferred from a workflow.
- Pitfall: never `time.Sleep` inside a `Run` handler. It holds a worker for the whole delay, ignores the fake clock and is lost on a crash.
- Pitfall: Hatchet refuses `IdempotencyKey` together with `RunAt` (`ErrUnsupported`); dedup a delayed job there with unique jobs instead.

## Max, Permanent, NoRetryOn or FailOn

When a step fails, four tools decide whether it is tried again:

| Tool | Where the decision lives | Use it for | Guide | Example |
|---|---|---|---|---|
| `RetryPolicy{Max, Backoff}` on `Workflow.Retry`, or `Step.Retry` to override one step | The workflow | How many retries a transient error gets, and how far apart. The zero value means no retries | [Retry policy](../jobs/retries.md#retry-policy) | [retries](../../examples/retries/) |
| `return workflow.Permanent(err)` | The handler | The handler knows this input can never succeed (a validation error, a 404 for a deleted record) | [Errors that must not retry](../jobs/retries.md#errors-that-must-not-retry) | [retries](../../examples/retries/), [first-workflow](../../examples/first-workflow/) |
| `RetryPolicy.NoRetryOn` | The workflow | A class of error that never recovers for this workflow, without changing handler code | [Retry policy](../jobs/retries.md#retry-policy) | [retries](../../examples/retries/) |
| `middleware.FailOn(match)` | The client, through step middleware | The same rule for every workflow on the client | [Fail on chosen errors](../middleware/step-middleware.md#fail-on-chosen-errors) | [middleware-stack](../../examples/middleware-stack/) |

- If a dependency returns a delay to wait (an HTTP 429 with `Retry-After`), wrap the error with `workflow.RetryAfter` and set `DelayFor: workflow.RetryAfterDelay` ([Retry after a delay the error names](../jobs/retries.md#retry-after-a-delay-the-error-names)); this needs `CapRetryDelay`, which Hatchet lacks.
- If one step must never retry while the workflow default does, set `Step.Retry: workflow.RetriesNone()`.
- Pitfall: `Max` counts retries after the first attempt, so `Max: 3` means up to four attempts.
- Pitfall: a cancelled run (`ErrCancelled`) is never retried, whatever the policy says.
- Pitfall: Hatchet refuses `Backoff.Jitter` (no `CapRetryJitter`) and a `Backoff` whose `Initial` is not `Factor` seconds, because the engine waits `Factor`^n seconds between attempts.

## OnFailure, OnRunFinished or a batch callback

| Hook | When it runs | Scope | Guide | Example |
|---|---|---|---|---|
| `Workflow.OnFailure` | Once when a run fails for good, with the failing step's input | One workflow, as part of its definition | [Run a handler when a run fails](../workflows/steps.md#run-a-handler-when-a-run-fails) | [first-workflow](../../examples/first-workflow/) |
| `OnRunFinished` (the `RunNotifier` a driver implements) | After every run ends: succeeded, failed or cancelled | Every run on the client, registered before `Start` | [React when a run finishes](../observability/run-finished.md#react-when-a-run-finishes) | [run-finished](../../examples/run-finished/) |
| Batch `Then`, `Catch`, `Finally` | When a group of jobs succeeded, first failed, or all finished | One batch | [Dispatch a batch](../batches/batches.md#dispatch-a-batch) | [batches](../../examples/batches/) |

- If the reaction is part of the business process (refund the charge, mark the order failed), use `OnFailure`, so it travels with the workflow to every driver.
- If it is cross-cutting (metrics, an audit log, freeing a lock), use `OnRunFinished`.
- Pitfall: `OnRunFinished` is guaranteed only where the driver declares `CapRunFinished` (memory, sqlstore and Redis). On Hatchet it is best-effort, so anything that relies on it needs a fallback, such as a TTL or a poller.

## Related

- [Choose a driver](drivers.md)
- [Choose by kind of app](app-types.md)
- [Feature index](../reference/features.md)
