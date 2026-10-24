# The SQL driver (sqlstore)

`sqlstore` is the durable driver on `database/sql`: every run, step result, wait, sleep, schedule, signal and lease is a row, so a restart or a dead worker loses nothing. It runs on SQLite (durable on one machine), Postgres and MySQL 8 (many workers on many machines over one database), through [`github.com/ubgo/sqlb`](https://github.com/ubgo/sqlb). Reach for it when work must survive a restart and you do not want to run a workflow engine; it declares every capability but `CapClosure` (a Go function cannot be stored in a row), each proven by the conformance suite on all three databases.

## Before you start

- `go get github.com/ubgo/workflow/sqlstore@latest` plus one database driver from sqlb's contrib modules (see [Pick a database](#pick-a-database)).
- A workflow to run: [First workflow](../start/first-workflow.md).
- The schema is applied by an explicit `Migrate` call; read [Migrations](../operations/migrations.md) before your first deploy.

## Create the client

`sqlstore.New(db, dialect, opts...)` takes the app's pool and the sqlb dialect its SQL is written for. It opens nothing and starts nothing. Then the order is always: `Migrate` (explicit, versioned; `Start` never migrates), `Register`, `Start`.

Step 1: open the database. A SQLite file survives restarts; `:memory:` does not.

<!-- snippet: examples/sqlite-worker/main.go#open -->
```go
db, err := driversqlite.Open(path) // a file; ":memory:" would be lost on exit
if err != nil {
	return err
}
defer db.Close()
```

Step 2: build the client and migrate. The options here are the ones a production worker usually sets; every one is optional.

<!-- snippet: examples/sqlite-worker/main.go#new -->
```go
c, err := sqlstore.New(db, sqlb.SQLiteDialect{},
	sqlstore.WithWorkers(4),                         // step attempts at once (default 8)
	sqlstore.WithPollInterval(200*time.Millisecond), // find other clients' work (default 1s)
	sqlstore.WithLease(30*time.Second),              // a dead worker's step is retaken after this
	sqlstore.WithMaxReclaims(3),                     // then the attempt fails with ErrAbandoned
	sqlstore.WithLogger(logger),
	sqlstore.WithIDGenerator(orderedIDs),
	sqlstore.WithErrorClassifier(func(err error) sqlstore.ErrorClass {
		class := sqlstore.DefaultErrorClassifier(err)
		if class.Retryable {
			lostRaces.Add(1) // e.g. a metric of lock conflicts
		}
		return class
	}),
)
if err != nil {
	return err
}
if err := c.Migrate(ctx); err != nil { // explicit; Start never migrates
	return err
}
```

Step 3: register, start and enqueue. `Start` checks every workflow against the capabilities, refuses expression keys, checks that the tables exist and that no newer release migrated them, records declared crons, then starts the dispatcher and the workers. It does not block.

<!-- snippet: examples/sqlite-worker/main.go#start -->
```go
if err := c.Register(receiptWorkflow()); err != nil {
	return err
}
ctx, cancel := context.WithCancel(ctx)
defer cancel()
if err := c.Start(ctx); err != nil {
	return err
}
defer c.Stop() // hands running steps back and waits for the workers

ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "send-receipt", Payload: []byte(`{"Order":"A-1"}`)})
if err != nil {
	return err
}
```

## Pick a database

| Database | Dialect | Driver module and open call | Fits |
|---|---|---|---|
| SQLite | `sqlb.SQLiteDialect{}` | `github.com/ubgo/sqlb/contrib/driver-sqlite`, `driversqlite.Open(path)` | One machine: a CLI, a desktop app, a single server. Several processes may share the file; they are serialised by SQLite's single writer. |
| Postgres | `sqlb.PostgresDialect{}` | `github.com/ubgo/sqlb/contrib/driver-pgx`, `driverpgx.Open(dsn)` | Many workers on many machines. Steps are claimed with `FOR UPDATE SKIP LOCKED`. The only engine that takes `WithSchema`. |
| MySQL 8 | `sqlb.MySQLDialect{}` | `github.com/ubgo/sqlb/contrib/driver-mysql`, `drivermysql.Open(dsn)` | Many workers, with `FOR UPDATE SKIP LOCKED`. The schema is the database: point the pool at it. |

On SQLite a step is claimed with one conditional `UPDATE`. Every engine stores times as `BIGINT` Unix nanoseconds. Another `database/sql` driver works when its errors read like these; otherwise give it an [error classifier](#database-errors).

## Workers and polling

`WithWorkers(n)` caps how many step attempts one client executes at once (default 8). Waits and sleeps never hold a worker.

A client wakes at once for work it made due itself (its own `Enqueue`, a step finishing). Work another client made due (a step another node queued, a signal for a run this client did not start) is found by polling every `WithPollInterval` (default one second), unless a notifier wakes it first ([Wake workers at once](#wake-workers-at-once)).

`Client.Wake()` makes this client look for work now instead of at its next poll. Call it after committing a transaction that used `EnqueueTx`, so the run is admitted without waiting up to a poll interval ([Transactional enqueue](../reliability/transactional-enqueue.md#enqueue-inside-your-transaction)). It never blocks and is a no-op on a client that is not running; other clients still find the run at their next poll.

## Wake workers at once

`WithNotifier(n)` wakes idle clients of the same tables on other machines when this client makes work due (an enqueue, a finished step, a retry, a resume), and wakes this client when another one does. A `sqlstore.Notifier` is two methods, `Notify(ctx, channel)` and `Listen(ctx, channel, wake)`; the channel is derived from the schema and table prefix, so two clusters on one database never wake each other. On Postgres, `sqlstore/pgnotify` implements it with LISTEN/NOTIFY over the pool the client uses:

```go
db, err := driverpgx.Open(dsn)
c, err := sqlstore.New(db, sqlb.PostgresDialect{}, sqlstore.WithNotifier(pgnotify.New(db)))
```

The program is [`examples/worker-wakeup`](../../examples/worker-wakeup). With `PG_DSN` it uses `pgnotify`; without it, it runs on SQLite with an in-process notifier, the same seam a Redis pub/sub or NATS notifier would fill:

<!-- snippet: examples/worker-wakeup/main.go#notifier -->
```go
func (b *localBus) Notify(_ context.Context, channel string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, wake := range b.subs[channel] {
		wake()
	}
	return nil
}

func (b *localBus) Listen(ctx context.Context, channel string, wake func()) error {
	b.mu.Lock()
	b.subs[channel] = append(b.subs[channel], wake)
	b.mu.Unlock()
	wake() // cover anything sent before the subscription
	<-ctx.Done()
	return nil
}
```

<!-- snippet: examples/worker-wakeup/main.go#worker -->
```go
worker, err := sqlstore.New(db, dialect,
	sqlstore.WithTablePrefix(prefix),
	sqlstore.WithPollInterval(pollInterval), // the fallback; a lost wake-up only delays work
	sqlstore.WithNotifier(notifier),         // pgnotify.New(db) on Postgres
)
```

Polling stays on as the fallback, so a lost, late or duplicated notification changes when work starts, never whether it runs. Measured on the host Postgres with a one-second poll, enqueue on one client to step start on another, 15 samples each: polling p50 591 ms, p95 897 ms; with `pgnotify` p50 38 ms, p95 99 ms (a shared development machine under load).

- `pgnotify`'s `Listen` needs the pgx stdlib driver (`driverpgx.Open` uses it), because `database/sql` cannot wait for a notification; on another driver it returns `pgnotify.ErrDriver` and the client keeps polling. `Notify` works through any Postgres driver.
- `Listen` holds one pool connection for as long as the client runs: size the pool one connection above the workers.
- LISTEN does not work through a transaction-mode pooler (PgBouncer in transaction or statement mode); point the pool at Postgres itself or leave the notifier off.
- An enqueue-only client notifies before `Enqueue` returns, since it has no background sender.

## Leases and crashed workers

A claimed step is a lease. A live worker renews it every third of `WithLease` (default 30 seconds); a step whose lease expires is claimed again by any client, so a step runs at least once. A lease expires when its worker died, and also when a live worker is starved long enough that it cannot renew, for example by a database busy with lock waits; the heartbeat starts before the claimed step's first database write, so only a stall longer than the lease lets another worker take the step. Delivery stays at least once either way, so choose a `WithLease` longer than the worst lock wait you expect. A client that stops cleanly hands its running steps back at once, so they do not wait for the lease.

Leases are measured on wall time, set apart by `WithLeaseClock` (default `clock.Real`). `WithClock` drives due times only: a test that moves a fake scheduling clock by an hour must not make every running step look abandoned.

A step that kills every worker it lands on (an out-of-memory payload, a segfaulting library) would otherwise travel the cluster forever. After more than `WithMaxReclaims` takeovers (default 5; zero fails at the first takeover) the attempt is not run again: it fails with an error wrapping `sqlstore.ErrAbandoned` that names how many workers abandoned it. The retry policy then applies as for any failure, and a retry starts with a fresh count. To fail the run at once instead, give the policy a `NoRetryOn` that returns `errors.Is(err, sqlstore.ErrAbandoned)`.

## Clocks, skew and one fire node

Nodes whose clocks disagree compare due times and lease ends against different "now"s: a holder whose clock runs behind writes lease ends that look expired early to a node whose clock runs ahead. Three options close the gap, each off by default.

- `WithDatabaseClock(resync)` makes the database server's clock the one source of "now" for due times and leases, in place of `WithClock` and `WithLeaseClock`. `Start` reads it first (and fails when it cannot), then again every `resync` (a minute when zero or less); between reads it advances on the local monotonic clock, so a wall-clock jump on a node moves nothing. `sqlstore.DatabaseClock` is the same clock as a value, with `Offset` (how far the database is ahead) and `Uncertainty` (half the last round trip).
- `WithLeaseSkew(d)` makes the other nodes wait `d` longer past a lease's end before taking its step over; it applies to steps, `OnFailure` handlers, run-end delivery and the prune lock. Set it to the largest clock difference you expect between nodes (with the database clock, a little above its `Uncertainty`).
- A lease holder fences itself: when a renewal has not landed a whole lease after the last one began, measured on its own monotonic lease clock, the attempt is cancelled with `sqlstore.ErrLeaseLost` and its outcome is not written, because another node may already hold the step.

By default every client fires cron ticks and the unique tick row lets one fire per tick win: no election, and it survives a split brain. Where the fire must happen on one known node (a job that talks to a system that allows one caller), `WithLeaderElection(node, opts...)` limits cron firing and housekeeping to the leader of the role `scheduler`. The elector is a lease row (`<prefix>leaders`, migration `007_leader_archive`): the leader renews it, a crash hands the role on after `LeaderLease` (15 seconds by default), and `LeaderElector.Term` grows on every handover as a fencing token. `WithElector(e)` takes any `sqlstore.Elector` instead (`IsLeader() bool`, answered from memory): a Redis lock or a Kubernetes Lease fits the same way. Steps run on every client either way, and the tick row stays as the second guard. `Client.IsLeader` reports the answer. See also [nodes](../operations/nodes.md) for scheduler-only and worker-only processes (`WithSchedulerOnly`, `WithoutScheduler`).

`WithFetchBatch(n)` sets how many rows one dispatcher query reads (100 by default: pending runs to admit, delayed runs to start, due sleeps, waits and signals, schedules to fire, run ends). A larger batch drains a backlog in fewer round trips; a smaller one keeps each pass short. Workers still claim one step per query on purpose: a claim leases its step, and a lease held for a busy worker would keep the step from every other node.

## Keep the hot tables small

The queue's tables are hot: claims and admissions rewrite their rows, so on Postgres every finished run kept there adds index entries and every update a dead row for `VACUUM`. [Retention](../operations/retention.md) keeps them small. To keep the history anyway, add `WithArchiveTable()`: `Prune` then moves each finished run, with the outputs of its succeeded steps, into the cold tables `<prefix>runs_archive` and `<prefix>steps_archive` in the transaction that deletes it, so a run is in exactly one of the two places and the queue never reads the cold tables. `Client.ArchivedRuns(ctx, sqlstore.ArchiveQuery{Workflow, RunID, FinishedBefore, Limit})` reads them back, newest finish first. The cold tables have no retention of their own: trim or partition them as the history's owner decides.

## Per-run facts, journal and bucket state

Migration `008_run_info_nodes` adds one `run_info` row per run: its tenant, the result written when it succeeds, the latest progress, and the nodes that claimed its attempts. That is what makes the SQL driver declare `CapTenant`, `CapResult`, `CapNodeIdentity`, `CapSnooze`, `CapHeartbeat`, `CapRunTimeout`, `CapKeyFIFO` and `CapDueQuery`, and what lets `Reschedule`, `CancelStep`, `ReportProgress` and `OnLifecycle` work across clients (see [results](../jobs/results.md), [progress](../jobs/progress.md), [tenants](../jobs/tenants.md) and [nodes](../operations/nodes.md)). A cancel of one step reaches a running attempt on another client at that client's next lease renewal. Migration `009_schedule_tenant` stores `Schedule.Tenant` in a side table (an `ALTER TABLE` cannot be written to run twice on MySQL) and sets it as the tenant of every run the schedule fires or is triggered to run; `ScheduleInfo.Tenant` reports it.

Two stores keep workflow state in the same database. `Client.JournalStore()` is a durable `workflow.JournalStore`, so the results `workflow.Memo` stored survive a crash and a retry on any node ([memoisation](../workflows/memoisation.md)). `Client.BucketStore()` is a durable `workflow.BucketStore` whose `Take` locks the bucket's row before reading its level, so every worker calling one upstream draws from one leaky bucket ([upstream limits](../jobs/upstream-limits.md)). Both can also be built over any `*sql.DB` with `sqlstore.NewJournalStore` and `sqlstore.NewBucketStore`, each with its own `Migrate` and its own migration list in the shared ledger, so an app on another driver gets durable state without running the SQL queue.

## Enqueue inside a pgx transaction

`Client.EnqueueTx` takes the app's `*sql.Tx`. An app whose transactions are pgx transactions passes them through `sqlstore/pgxtx`: `pgxtx.Run(ctx, tx, func(stx *sql.Tx) error { ... })` hands `EnqueueTx` (or `outbox.Write`) a `*sql.Tx` whose statements run on the app's `pgx.Tx`, so the run commits or rolls back with the app's own pgx writes. The `*sql.Tx` is a view: its `Commit` and `Rollback` do nothing and it is valid only inside the function. Postgres only. See [Transactional enqueue](../reliability/transactional-enqueue.md).

## Database errors

The driver's correctness guards are unique indexes (an idempotency key held twice, a cron tick fired twice), and some transactions lose lock races. So it must sort each database error, which an `ErrorClassifier` does. It returns an `ErrorClass`:

| Field | Meaning | What the driver does |
|---|---|---|
| `UniqueViolation` | A unique constraint refused the write | Treats it as "someone else won the race". Say true only when sure. |
| `Retryable` | A deadlock, a serialization failure, SQLite busy | Retries the whole transaction, up to `WithTxAttempts` (default 5; 1 turns retrying off) |
| `Unavailable` | Refused, reset or dropped connection, server shutting down | Marks the error with `workflow.Unavailable`, so `errors.Is(err, workflow.ErrUnavailable)` holds and a [failover client](../reliability/failover.md#decide-what-counts-as-an-outage) can tell an outage from a bad request |

The zero value, "neither", is the safe answer: the error reaches the caller. `DefaultErrorClassifier` knows the drivers sqlb's contrib modules open: Postgres by SQLSTATE (23505 unique; 40P01 and 40001 retryable; class 08 and 57P01 to 57P03 unavailable), MySQL by "Error 1062", "Error 1213"/"Error 1205" and "invalid connection"/"Error 1040"/"Error 1053", SQLite by "UNIQUE constraint failed" and "SQLITE_BUSY"/"database is locked"; on any engine a `*net.OpError`, `driver.ErrBadConn`, `sql.ErrConnDone` or a pgx "failed to connect to" error is unavailable. A driver, proxy or wrapper whose errors read differently needs its own classifier, usually checking its own forms and falling back to the default, as Step 2 above does. Without one, a lost idempotency race comes back as an error and a deadlock is not retried.

## Run IDs

`WithIDGenerator` decides how run IDs, delayed-start IDs, step IDs and unnamed schedule IDs are made. It is called with an `IDKind`: `sqlstore.IDRun` ("run"), `IDScheduled` ("sched"), `IDStep` ("step") or `IDSchedule` ("sch"). The default, `DefaultIDGenerator`, returns the kind, an underscore and 32 hex characters. Use your own for time-sortable IDs (UUIDv7, ULID) or IDs that line up with the app's:

<!-- snippet: examples/sqlite-worker/main.go#ids -->
```go
// orderedIDs makes IDs that sort by creation time (like a UUIDv7 or ULID):
// the kind, the Unix milliseconds in fixed-width hex, then 8 random bytes.
// An ID must be 1 to 64 bytes and unique per kind across every client.
func orderedIDs(kind sqlstore.IDKind) (string, error) {
	var rnd [8]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		return "", err // the driver refuses the operation; no bad row is written
	}
	return fmt.Sprintf("%s_%012x%s", kind, time.Now().UnixMilli(), hex.EncodeToString(rnd[:])), nil
}
```

A named schedule's ID is derived from its workflow and name, so the same name is the same row on every node and boot; the generator is not asked for it. Lease and failure tokens are internal and always 128 random bits.

## Queues, pauses and run admin

Every client of one database shares the same rows, so the queue and run controls work across a cluster: run separate worker processes per queue, pause a queue from any process, and list, retry and forget runs from whichever client an operator uses. The driver declares `CapQueue`, `CapQueueOverride`, `CapJobRetry`, `CapRetryUntil`, `CapRetryMaxErrors`, `CapDiscard`, `CapRunQuery`, `CapRetryRun`, `CapDeleteRun`, `CapCancelAll`, `CapPauseWorkflow`, `CapPauseQueue`, `CapSchedulePause`, `CapScheduleWhen`, `CapScheduleWindow`, `CapTickEvents`, `CapScheduleList` and `CapDrain`. The tables behind them come from migration `003_queues_admin`, so run `Migrate` first. It also declares `CapOverlapSkip`, `CapScheduleTrigger` and `CapScheduleHistory` ([Overlap, trigger now and last fire](../schedules/overlap.md); migration `006_schedule_history`), `CapEnqueueOnly`, `CapErrorHistory`, `CapShutdownRequeue` and `CapForceRun` ([Manage runs](../jobs/manage-runs.md#force-a-run-to-an-end)), and `CapLabelPrefix`: label searches, exact and by prefix, read the label index of migration `005_run_labels` ([Search by label](../jobs/manage-runs.md#search-by-label)).

Step 1: give each workflow a queue and each worker process the queues it serves. A client built with `WithQueues` claims only those queues; runs of other queues wait in the table for a client that serves them. `Workers` caps one queue's attempts at once on that client, and a higher `Priority` queue is always drained first. `Limit` is the queue's ceiling across every client on the database (`CapQueueLimitGlobal`): `Workers: 4` on three nodes allows twelve attempts, `Limit: 5` keeps them to five ([named queues](../jobs/queues.md#cap-a-queue-across-every-worker)). A discard policy and the drain report work here too: `RetryPolicy.DiscardOnExhaust` ends a run out of retries `discarded`, and `DrainWithReport` lists the attempts it handed back as `requeued` ([deploy](../operations/deploy.md#give-running-steps-a-grace-period-and-see-what-was-cut-off)).

<!-- snippet: examples/sql-queue-workers/main.go#workflows -->
```go
email := workflow.Workflow{Name: "send-email", Queue: "mail", Steps: []workflow.Step{{Name: "send",
	Run: func(ctx context.Context, in []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		ranOn <- info.Queue
		return in, nil
	}}}}
report := workflow.Workflow{Name: "build-report", Steps: []workflow.Step{ // the default queue
	{Name: "gather", Run: func(_ context.Context, in []byte) ([]byte, error) { gathered.Add(1); return in, nil }},
	{Name: "render", After: []string{"gather"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
		rendered.Add(1)
		if broken.Load() {
			return nil, workflow.Permanent(errors.New("template missing"))
		}
		return in, nil
	}},
}}
```

<!-- snippet: examples/sql-queue-workers/main.go#clients -->
```go
mail, err := sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithPollInterval(poll),
	sqlstore.WithQueues(workflow.QueueConfig{Name: "mail", Workers: 1})) // one email at a time
if err != nil {
	return err
}
reports, err := sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithPollInterval(poll),
	sqlstore.WithQueues(workflow.QueueConfig{Name: workflow.DefaultQueue}))
if err != nil {
	return err
}
```

Step 2: pause and resume. A pause is a row the claim statement reads, so it holds every client at once and survives restarts; running attempts finish, new attempts wait as queued, and `Enqueue` keeps working. `Pause` works on a client that was never started.

<!-- snippet: examples/sql-queue-workers/main.go#pause -->
```go
// Paused from the reports client; the mail client obeys, because the
// pause is a row both read when they claim.
if err := reports.Pause(ctx, workflow.PauseQueue("mail")); err != nil {
	return err
}
ref, err := reports.Enqueue(ctx, workflow.Job{Workflow: "send-email", Payload: []byte(`"hi"`)})
if err != nil {
	return err
}
```

Step 3: find failed runs, retry them from the step that failed, and forget them. `Runs` lists runs of every workflow in the database, newest first, with an opaque cursor; `Retry` keeps the outputs of steps that succeeded and takes the run's idempotency key again (`ErrDuplicate` when another unfinished run holds it); `Delete` and `CancelAll` act on finished and not-yet-started runs.

<!-- snippet: examples/sql-queue-workers/main.go#admin -->
```go
page, err := mail.Runs(ctx, workflow.RunQuery{Statuses: []workflow.Status{workflow.StatusFailed}, Labels: map[string]string{"tenant": "acme"}})
if err != nil {
	return err
}
for _, r := range page.Runs {
	fmt.Fprintf(out, "failed: %s at step %s on queue %s\n", r.Workflow, r.FailedStep, r.Queue)
}
// The fix is deployed.
broken.Store(false)
again, err := mail.Retry(ctx, page.Runs[0].Ref) // from the failed step; gather keeps its output
if err != nil {
	return err
}
```

Step 4: stop a worker gracefully. `Drain` stops claiming, waits for running attempts to finish and their outcomes to be written, then stops; an attempt cut off by its ctx is handed back as ready for another client. `WithStopWhenIdle(grace)` drains on its own once no unfinished run of the client's workflows (on its queues) has existed for `grace`.

<!-- snippet: examples/sql-queue-workers/main.go#drain -->
```go
dctx, dcancel := context.WithTimeout(ctx, 5*time.Second)
defer dcancel()
err = mail.Drain(dctx) // stop claiming, let running attempts finish, stop
fmt.Fprintln(out, "mail worker drained:", err == nil)
<-mail.Done()
```

`EnqueueMany(ctx, jobs)` writes several runs in one transaction, all or none, and is what `batch.EnqueueMany` uses on this driver; `BatchStore` implements `batch.Pruner`, so `Manager.Prune` deletes old batch state.

Schedules get the same treatment: `Schedules` lists declared and run-time schedules with their next fire, `PauseSchedule` and `ResumeSchedule` stop and restart them (stored, so every node obeys), `Workflow.ScheduleWhen` and `Schedule.Windows` (or `Workflow.CronWindows`) decide each tick on the firing node, and `OnTick` reports every fired, skipped and misfired tick once, from the node that handled it. Runs a schedule fires carry the `workflow.LabelSchedule` label.

## Options

The options not covered above:

| Option | What it sets | Default |
|---|---|---|
| `WithClock(c)` | Every due time: delayed starts, sleeps, wait deadlines, retry backoff, step timeouts, rate-limit windows, cron fires | `clock.Real` |
| `WithLogger(l)` | Where the driver reports what it cannot return to a caller (a failed poll, an `OnFailure` error, a cron fire that could not enqueue) | discard |
| `WithMiddleware(mw...)` | Step middleware around every step and `OnFailure` handler, first outermost | none |
| `WithJitterSource(rnd)` | Where `Backoff.Jitter` draws values in [0, 1), so tests can pin them; must be safe for concurrent use | `math/rand/v2` |
| `WithCronParser(p)` | Another cron dialect; every client of one database must use the same parser, because stored expressions are parsed again when they fire | `cron.Parse` |
| `WithTablePrefix(p)`, `WithSchema(name)` | Table names and a Postgres schema: [Migrations › Table names and schemas](../operations/migrations.md#table-names-and-schemas) | `wf_`, none |
| `WithRetention`, `WithArchive`, `WithPruneInterval`, `WithPruneChunk`, `WithoutAutoPrune` | Deleting finished runs: [Retention](../operations/retention.md) | keep forever |
| `WithQueues(qs...)` | The queues this client serves, each with a `Workers` cap, a cluster-wide `Limit` and a `Priority`; New refuses an invalid list | every queue, one pool |
| `WithStopWhenIdle(grace)` | Drain once no unfinished run of this client's workflows (on its queues) existed for `grace` on the scheduling clock | off |
| `WithRecycle(r)` | Drain and stop at a `workflow.Recycle` limit (jobs, time, memory) so a supervisor starts a fresh process; unclaimed runs stay queued; `StopCause` and `workflow.Serve` report it ([Worker recycling](../operations/recycling.md)) | off |
| `WithNotifier(n)` | Wake-ups between clients of one database ([Wake workers at once](#wake-workers-at-once)); `pgnotify.New(db)` on Postgres | none (polling only) |
| `WithEnqueueOnly()` | A client that enqueues and reads status without `Start` and runs nothing ([Deploy workers › Web nodes that only enqueue](../operations/deploy.md#web-nodes-that-only-enqueue)) | off |
| `WithDatabaseClock(resync)`, `WithLeaseSkew(d)` | The database's clock as the one "now", and the margin other nodes wait past a lease: [Clocks, skew and one fire node](#clocks-skew-and-one-fire-node) | off, 0 |
| `WithLeaderElection(node, opts...)`, `WithElector(e)` | Cron firing and housekeeping on one elected node | every client fires |
| `WithFetchBatch(n)` | Rows one dispatcher query reads | 100 |
| `WithArchiveTable()` | Prune moves finished runs to the cold tables: [Keep the hot tables small](#keep-the-hot-tables-small) | drop them |
| `WithNodeID(id)`, `WithSchedulerOnly()`, `WithoutScheduler()` | The node's name, and a process that only fires schedules or only runs steps: [nodes](../operations/nodes.md) | `host:pid`, full node |
| `WithPayloadLimit(n)` | Refuse a payload over `n` bytes at `Enqueue`, `EnqueueTx` and `EnqueueMany` | none |
| `WithErrorHistory(n)` | How many failed attempts each run keeps in `RunInfo.Errors`, newest kept; `n <= 0` turns it off ([Error history](../jobs/manage-runs.md#error-history)) | 20 |
| `WithTickRetention`, `WithHousekeepingEvery` | Clean-up cadence: [Tuning › Housekeeping](../operations/tuning.md#housekeeping) | 24h, 50 passes |

`Client.DB()` returns the pool the client was built with: the escape hatch for reading the tables directly (a dashboard, a report) or running `Migrate` inside a wider schema step.

## Try it

```sh
cd examples
go run ./sqlite-worker
```

```text
memory durable: false
memory cron: true
memory run: succeeded
sqlite durable: true
run id kind: run
sqlite run: succeeded
status after the end: succeeded
```

The last line is the point of the driver: the run's outcome is a row, readable after the run ended and after a restart.

## Pitfalls

- **One more pool connection with `pgnotify`.** Its listener holds a connection for as long as the client runs; a pool sized exactly to the workers starves a worker.
- `Start` never migrates. A database that has not been migrated (or that lacks a table a newer release added, such as `002_schedule_misfire`'s) fails `Start` with "table ... is not readable (run Migrate, or apply MigrationScripts, first)".
- Concurrency and rate-limit keys must be Go functions (`workflow.KeyFunc`); a `workflow.KeyExpr` is refused at `Start`, because SQL has no engine expression language.
- A client only claims, admits, fires and delivers for the workflows it registered. A run of a workflow no running client registered waits in the table.
- Cancel reaches a step running on another client at that client's next lease renewal (a third of the lease), not at once.
- A custom classifier that says `UniqueViolation` for an error that is not one makes the driver treat a failure as "someone else won".
- `Until` and `MaxErrors` are judged when an attempt's failure is recorded, on the scheduling clock; an attempt cut off by a stop or a dead worker counts as neither.
- A tick that falls due while its schedule is paused never fires, not even on resume, whatever the misfire policy: the schedule resumes at its next tick. The misfire policy still governs ticks missed while no client was running.
- `Runs` filters labels and the exact status of an admitted run (queued, running, waiting) in Go after the SQL read, so a narrow label filter over a large table reads more rows than it returns.
- Every client of one database must use the same cron parser and the same table prefix to be workers of one queue; two prefixes share nothing.

## Related

- [Run several workers](../operations/deploy.md#run-several-workers) and [shut down cleanly](../operations/deploy.md#shut-down-cleanly)
- [Migrations](../operations/migrations.md), [Retention](../operations/retention.md), [Tuning](../operations/tuning.md)
- [The in-memory driver](memory.md) and [the Hatchet driver](hatchet.md)
- [Transactional enqueue](../reliability/transactional-enqueue.md), [Tenants](../jobs/tenants.md), [Nodes and health](../operations/nodes.md)
- Examples: [sqlite-worker](../../examples/sqlite-worker/), [postgres-cluster](../../examples/postgres-cluster/), [sql-queue-workers](../../examples/sql-queue-workers/)
