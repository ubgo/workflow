# Choose a driver

Which backend runs your workflows: the in-process memory driver, the SQL driver on SQLite, Postgres or MySQL 8, the Redis driver, or the Hatchet engine. Your workflow code is the same on all four; the choice decides what survives a restart, how many machines can share the work, what you have to operate, and which features are available. The core refuses at start-up what a driver cannot do, so a wrong choice shows up as an error naming the missing capability, not as silently lost work.

## Memory, SQL, Redis or Hatchet

| | memory | sqlstore | redis | hatchet |
|---|---|---|---|---|
| Package | `github.com/ubgo/workflow/memory` (core) | `github.com/ubgo/workflow/sqlstore` (own module) | `github.com/ubgo/workflow/redis` (own module) | `github.com/ubgo/workflow/hatchet` (own module) |
| Survives a restart (`CapDurable`) | no: everything is lost on exit | yes: every run, step result, wait, sleep and schedule is a row | yes: runs, schedules and leases are Redis keys (with AOF, up to a second can be lost if Redis itself crashes) | yes: the engine holds it |
| Machines that can share the work | one process | SQLite: processes on one machine; Postgres or MySQL: any number of machines | any number of machines on one Redis | any number of workers |
| You operate | nothing | the database you already run | a Redis you already run, set to `noeviction` | a Hatchet engine, which itself runs on Postgres |
| Capabilities | all but `CapDurable` | all but `CapClosure` | all but `CapClosure` and `CapQueueLimitGlobal`: wait steps, concurrency and rate keys, named queues with weights, pauses, run-finished events, chains, retry and bulk cancel, schedule conditions and tick events, tenants, results, snooze, heartbeats, run timeouts, node identity, worker scaling | `Durable`, `DelayedStart`, `WaitForEvent`, `DurableSleep`, `ConcurrencyKey`, `RateLimit`, `IdempotencyKey`, `Cron`, `Cancel`, `Queue`, `RunQuery`, `RetryRun`, `CancelAll`, `ScheduleList`, `Drain`, `ScheduleTrigger`, `ScheduleHistory`, `EnqueueOnly`, `Tenant`, `TickTime`, `DueQuery`, `Result`, `CronSeconds`, `KeyFIFO`, `DrainReport` (pauses with `Config.API`) |
| Missing there | durability; closures are in-process only | `CapClosure` (a Go function cannot be stored; a [function by name](../jobs/functions.md) runs everywhere) | `CapClosure` (a Go function cannot be stored; a [function by name](../jobs/functions.md) runs everywhere), `CapQueueLimitGlobal` (no cluster-wide queue cap; `Workers` is per client) | `CapRunFinished` (run ends are delivered best-effort), `CapSnooze`, `CapHeartbeat`, `CapRunTimeout`, `CapNodeIdentity` (no per-attempt worker history), `CapScheduleActive`, `CapScheduleKeep`, queue weights, `CapRetryJitter`, `CapRetryDelay`, misfire policies, `CapCronZone` (crons in UTC only), `CapRunStates` (no retrying or discarded status), `CapDiscardExhausted`, `CapQueueLimitGlobal` (no count of a queue across workers), `CapChain` (a run's shape is fixed at registration), `CapWorkerScale` (slots fixed per worker process), `CapClosure`, `CapLabelPrefix` (metadata matched by exact value) |
| Keys for concurrency and rate limits | Go functions (`workflow.KeyFunc`) | Go functions | Go functions | CEL expressions (`workflow.KeyExpr`) |
| Enqueue inside your own SQL transaction | no | `Client.EnqueueTx` | through the outbox | through the outbox |
| Durable memo journal and shared leaky bucket for [`Memo`](../workflows/memoisation.md) and [`UpstreamLimit`](../jobs/upstream-limits.md) | in-process only | `JournalStore`, `BucketStore` (tables) | `JournalStore`, `BucketStore` (keys) | in-process only; bring your own store |
| Batches | per job | per job; durable batch store (`Client.BatchStore`) | per job, completion by status polling | native bulk triggers (`batch.GroupRunner`), an opt-in coordinator run |
| Guide | [memory](../drivers/memory.md#create-the-client) | [sqlstore](../drivers/sqlstore.md#create-the-client) | [redis](../drivers/redis.md#create-the-client) | [hatchet](../drivers/hatchet.md#create-the-client) |
| Example | [quickstart](../../examples/quickstart/) | [sqlite-worker](../../examples/sqlite-worker/), [postgres-cluster](../../examples/postgres-cluster/) | [redis-worker](../../examples/redis-worker/) | [hatchet-worker](../../examples/hatchet-worker/) |

The capability rows are printed by the [capabilities](../../examples/capabilities/) example from the drivers themselves; the [capability reference](../reference/capabilities.md#capability-matrix) has the full matrix.

### If X, then Y

- **If it is a test of your app code**, use `workflowtest.NewFake(workflowtest.FakeLike(prod))`, which runs nothing and refuses what production would ([Test app code with the Fake](../testing/fake.md#test-app-code-with-the-fake)). Use the memory driver with a fake clock when the test must run the steps.
- **If losing queued work on a crash or deploy is acceptable** (a cache warm-up, a best-effort notification, a CLI tool, local development), use memory.
- **If you have one server and want durability with nothing new to run**, use sqlstore on a SQLite file. Several processes on that machine can share it.
- **If you run several machines and already have Postgres or MySQL 8**, use sqlstore on it. Workers claim steps with `FOR UPDATE SKIP LOCKED`, a cron tick fires once across all of them, and a crashed worker's step is taken over when its lease expires.
- **If a job must be enqueued if and only if your own transaction commits**, use sqlstore with `EnqueueTx` when the queue is the same database, or the outbox in front of any driver otherwise ([Enqueue, EnqueueTx or the outbox](methods.md#enqueue-enqueuetx-or-the-outbox)).
- **If you already run Redis and need durable jobs, cron and workflows**, use the redis driver: runs are claimed whole under a lease, writes are fenced, a tick fires once across clients, and it has everything the SQL driver has except closures and transactional enqueue ([The Redis driver](../drivers/redis.md)).
- **If you already run Hatchet, or want its dashboard and engine-side scheduling**, use the hatchet driver. Write concurrency and rate-limit keys as CEL (`workflow.KeyExpr`), keep crons in UTC, and do not rely on misfire policies or retry jitter, which it refuses.
- **If one backend must not be a single point of failure for enqueues**, put `failover.New(primary, secondary)` in front of two drivers; every job then needs an idempotency key ([Fail over to a second backend](../reliability/failover.md#fail-over-to-a-second-backend)).
- **If you start on one and expect to move**, write against `workflow.Client` only, keep keys as Go functions unless you are on Hatchet, and run your workflows through a `FakeLike` test for each target driver.

## Pitfalls

- The memory driver accepts a workflow that sleeps for a week: durability is never inferred from a workflow, so nothing refuses it. Choosing memory is choosing to lose that sleep on exit.
- The Redis driver refuses closures (`EnqueueClosure`) and expression keys (`KeyExpr`), and cannot enqueue inside your SQL transaction; use functions by name, Go key functions and the outbox there.
- SQLite is durable on one machine, not shared across machines. A network file system does not change that.
- `sqlstore.New` never creates tables, and `Start` never migrates: run `Client.Migrate` from your deploy first ([Run the migrations](../operations/migrations.md#run-the-migrations)).
- Every sqlstore client sharing a database must use the same cron parser; a different table prefix is a different, separate queue.
- On Hatchet, `OnRunFinished` is best-effort: a restarted client replays only ends of runs created within `Config.RunFinishedCatchUp`. Features that rely on run ends (`UniqueUntilFinished`, batch callbacks through the hook) keep their fallback there: a TTL, a poller or Hatchet's own `BatchSource`.
- Hatchet crons run in UTC and a tick missed while the engine was down is dropped. A schedule that must run in a local zone or catch up belongs on sqlstore.
- A key written as a Go function is refused by Hatchet at `Register`, and a CEL expression is refused by memory, sqlstore and Redis at `Start`. Moving between them means rewriting keys.

## Related

- [Choose by kind of app](app-types.md)
- [Choose a method](methods.md)
- [Capabilities](../concepts/capabilities.md#what-a-driver-declares)
- [Going to production](../start/production.md#checklist)
