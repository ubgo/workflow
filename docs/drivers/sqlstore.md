# The SQL driver (sqlstore)

`sqlstore` is the durable driver on `database/sql`: every run, step result, wait, sleep, schedule, signal and lease is a row, so a restart or a dead worker loses nothing. It runs on SQLite (durable on one machine), Postgres and MySQL 8 (many workers on many machines over one database), through [`github.com/ubgo/sqlb`](https://github.com/ubgo/sqlb). Reach for it when work must survive a restart and you do not want to run a workflow engine; it declares every capability but `CapClosure` (a Go function cannot be stored in a row), each proven by the conformance suite on all three databases.

## Before you start

- `go get github.com/ubgo/workflow/sqlstore@latest` plus one database driver from sqlb's contrib modules (see [Pick a database](#pick-a-database)). The module itself requires no database driver: it takes your `*sql.DB` and a dialect, so your app downloads only the driver it opens the pool with.
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
defer func() { err = errors.Join(err, db.Close()) }()
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

In code the choice is two values, the pool and its dialect, and the rest of the program does not change. [`examples/sql-options`](../../examples/sql-options) opens each engine this way and runs the SQLite branch:

<!-- snippet: examples/sql-options/main.go#pick -->
```go
// openDB opens the pool and returns the sqlb dialect its SQL is written
// for. The driver module is the app's choice; sqlstore itself requires
// none.
func openDB(e engine, dsn string) (*sql.DB, sqlb.Dialect, error) {
	switch e {
	case engineSQLite: // one machine; a file path, ":memory:" is lost on exit
		db, err := driversqlite.Open(dsn)
		return db, sqlb.SQLiteDialect{}, err
	case enginePostgres: // many machines; the only engine with WithSchema
		db, err := driverpgx.Open(dsn)
		return db, sqlb.PostgresDialect{}, err
	case engineMySQL: // many machines; the schema is the database in the DSN
		db, err := drivermysql.Open(dsn)
		return db, sqlb.MySQLDialect{}, err
	}
	return nil, nil, fmt.Errorf("unknown engine %q", e)
}
```

On SQLite a step is claimed with one conditional `UPDATE`. Every engine stores times as `BIGINT` Unix nanoseconds. Another `database/sql` driver works when its errors read like these; otherwise give it an [error classifier](#database-errors).

## Workers and polling

`WithWorkers(n)` caps how many step attempts one client executes at once (default 8). Waits and sleeps never hold a worker.

<!-- snippet: examples/sql-options/main.go#workers -->
```go
c, err := sqlstore.New(db, dialect,
	sqlstore.WithWorkers(workers),       // step attempts at once (default 8)
	sqlstore.WithPollInterval(idlePoll), // look for other clients' work (default 1s)
)
```

A client wakes at once for work it made due itself (its own `Enqueue`, a step finishing). Work another client made due (a step another node queued, a signal for a run this client did not start) is found by polling every `WithPollInterval` (default one second), unless a notifier wakes it first ([Wake workers at once](#wake-workers-at-once)).

`Client.Wake()` makes this client look for work now instead of at its next poll. Call it after committing a transaction that used `EnqueueTx`, so the run is admitted without waiting up to a poll interval ([Transactional enqueue](../reliability/transactional-enqueue.md#enqueue-inside-your-transaction)). It never blocks and is a no-op on a client that is not running; other clients still find the run at their next poll. In `examples/sql-options` the client above polls once an hour, and the run still starts at once:

<!-- snippet: examples/sql-options/main.go#wake -->
```go
tx, err := db.BeginTx(ctx, nil)
if err != nil {
	return err
}
ref, err := c.EnqueueTx(ctx, tx, workflow.Job{Workflow: receipt.Name, Payload: []byte(`"A-1"`)})
if err != nil {
	return errors.Join(err, tx.Rollback())
}
// ... the app's own writes in tx ...
if err := tx.Commit(); err != nil {
	return err
}
c.Wake() // look now, not at the next poll an hour away
```

```text
run enqueued in a transaction ran before the next poll: true
```

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

- `pgnotify` is its own module, so only an app that uses it downloads pgx: `go get github.com/ubgo/workflow/sqlstore/pgnotify@latest`. Upgrade `sqlstore` to v0.1.1 or later with it: v0.1.0 still carried the package, and the two together are an ambiguous import.
- `pgnotify`'s `Listen` needs the pgx stdlib driver (`driverpgx.Open` uses it), because `database/sql` cannot wait for a notification; on another driver it returns `pgnotify.ErrDriver` and the client keeps polling. `Notify` works through any Postgres driver.
- `Listen` holds one pool connection for as long as the client runs: size the pool one connection above the workers.
- LISTEN does not work through a transaction-mode pooler (PgBouncer in transaction or statement mode); point the pool at Postgres itself or leave the notifier off.
- An enqueue-only client notifies before `Enqueue` returns, since it has no background sender.

## Leases and crashed workers

A claimed step is a lease. A live worker renews it every third of `WithLease` (default 30 seconds); a step whose lease expires is claimed again by any client, so a step runs at least once. A lease expires when its worker died, and also when a live worker is starved long enough that it cannot renew, for example by a database busy with lock waits; the heartbeat starts before the claimed step's first database write, so only a stall longer than the lease lets another worker take the step. Delivery stays at least once either way, so choose a `WithLease` longer than the worst lock wait you expect. A client that stops cleanly hands its running steps back at once, so they do not wait for the lease.

Leases are measured on wall time, set apart by `WithLeaseClock` (default `clock.Real`). `WithClock` drives due times only: a test that moves a fake scheduling clock by an hour must not make every running step look abandoned.

A step that kills every worker it lands on (an out-of-memory payload, a segfaulting library) would otherwise travel the cluster forever. After more than `WithMaxReclaims` takeovers (default 5; zero fails at the first takeover) the attempt is not run again: it fails with an error wrapping `sqlstore.ErrAbandoned` that names how many workers abandoned it. The retry policy then applies as for any failure, and a retry starts with a fresh count. To fail the run at once instead, give the policy a `NoRetryOn` that returns `errors.Is(err, sqlstore.ErrAbandoned)`.

[`examples/sql-leader-election`](../../examples/sql-leader-election) plays the whole story on one SQLite file with two clients. node-a claims a step, then stalls: its fake lease clock jumps a whole lease with no renewal landing, so the driver cancels the attempt with `ErrLeaseLost` (see [the next section](#clocks-skew-and-one-fire-node)) and writes nothing. Its `WithLeaseSkew` of an hour keeps it from taking its own step straight back:

<!-- snippet: examples/sql-leader-election/main.go#lease-holder -->
```go
// charge is node-a's step: it starts, then hangs until its lease is lost.
charge := func(ctx context.Context, _ []byte) ([]byte, error) {
	close(started)
	<-ctx.Done() // the database stalls: no renewal lands
	// The holder fenced itself: its outcome is not written, because
	// another node may already hold the step.
	fenced <- errors.Is(context.Cause(ctx), sqlstore.ErrLeaseLost)
	return nil, ctx.Err()
}
a, err := sqlstore.New(db, sqlb.SQLiteDialect{},
	sqlstore.WithNodeID("node-a"),
	sqlstore.WithLease(stepLease),      // renewed every third; taken over after it lapses
	sqlstore.WithLeaseClock(clkA),      // wall time for leases, apart from WithClock
	sqlstore.WithLeaseSkew(holderSkew), // this node waits an hour past a lease's end
)
```

node-b's lease clock reads two hours ahead, past node-a's lease end, so it takes the step over and runs it. Run again with `WithMaxReclaims(0)`, the same takeover fails the attempt instead:

<!-- snippet: examples/sql-leader-election/main.go#lease-taker -->
```go
reclaims := 5 // the default: takeovers of one attempt before ErrAbandoned
if poison {
	reclaims = 0 // fail at the first takeover
}
b, err := sqlstore.New(db, sqlb.SQLiteDialect{},
	sqlstore.WithNodeID("node-b"),
	sqlstore.WithLease(stepLease),
	sqlstore.WithLeaseClock(clock.NewFake(base.Add(takerAhead))), // past node-a's lease end
	sqlstore.WithMaxReclaims(reclaims),
)
```

```text
node-a stopped its attempt with ErrLeaseLost: true
node-b took the step over and ran it: run succeeded
WithMaxReclaims(0): run failed, abandoned: true
error: sqlstore: step "charge" failed after 1 attempt(s): sqlstore: step "charge" abandoned by 1 workers (its lease expired 1 times, over WithMaxReclaims 0): sqlstore: step abandoned by its workers
```

The run's stored error is text, so read it as text (`RunInfo.Error`, `RunEvent.Err.Error()`); `errors.Is(err, sqlstore.ErrAbandoned)` holds for the attempt's error inside the process, which is what a retry policy's `NoRetryOn` sees.

## Clocks, skew and one fire node

Nodes whose clocks disagree compare due times and lease ends against different "now"s: a holder whose clock runs behind writes lease ends that look expired early to a node whose clock runs ahead. Three options close the gap, each off by default.

- `WithDatabaseClock(resync)` makes the database server's clock the one source of "now" for due times and leases, in place of `WithClock` and `WithLeaseClock`. `Start` reads it first (and fails when it cannot), then again every `resync` (a minute when zero or less); between reads it advances on the local monotonic clock, so a wall-clock jump on a node moves nothing. `sqlstore.DatabaseClock` is the same clock as a value, with `Offset` (how far the database is ahead) and `Uncertainty` (half the last round trip).
- `WithLeaseSkew(d)` makes the other nodes wait `d` longer past a lease's end before taking its step over; it applies to steps, `OnFailure` handlers, run-end delivery and the prune lock. Set it to the largest clock difference you expect between nodes (with the database clock, a little above its `Uncertainty`).
- A lease holder fences itself: when a renewal has not landed a whole lease after the last one began, measured on its own monotonic lease clock, the attempt is cancelled with `sqlstore.ErrLeaseLost` and its outcome is not written, because another node may already hold the step. A handler tells it apart with `errors.Is(context.Cause(ctx), sqlstore.ErrLeaseLost)`, as [the lease example above](#leases-and-crashed-workers) does.

### Use the database's clock

`NewDatabaseClock(db, dialect)` reads nothing until `Sync`; after one read its `Now` runs on the local monotonic clock. `WithDatabaseClock` builds the same clock inside the client, and `Client.DatabaseClock()` returns it:

<!-- snippet: examples/sql-leader-election/main.go#dbclock -->
```go
dbc, err := sqlstore.NewDatabaseClock(db, sqlb.SQLiteDialect{})
if err != nil {
	return err
}
if err := dbc.Sync(ctx); err != nil { // one read; Now then runs on the local monotonic clock
	return err
}
// Offset: how far the database is ahead of this machine. Uncertainty:
// half the read's round trip. A SQLite file shares this machine's clock,
// so both are tiny here; on Postgres or MySQL they are what the nodes'
// clocks disagree by.
fmt.Fprintf(w, "database clock: offset under %s: %v, uncertainty under %s: %v\n",
	trustBound, dbc.Offset().Abs() < trustBound, trustBound, dbc.Uncertainty() < trustBound)

c, err := sqlstore.New(db, sqlb.SQLiteDialect{},
	sqlstore.WithDatabaseClock(clockResync), // due times and leases on the database's clock
	sqlstore.WithLeaseSkew(clusterSkew),     // others wait this long past a lease's end
)
```

```text
database clock: offset under 1s: true, uncertainty under 1s: true
client runs on the database clock: true
```

Postgres reads `clock_timestamp()`, MySQL `UTC_TIMESTAMP(6)` and SQLite `julianday('now')`. A SQLite file lives on one machine, so there the database clock only makes the processes of that machine agree, which they already do; it earns its keep on Postgres and MySQL, where the nodes are different machines.

### Elect one fire node

By default every client fires cron ticks and the unique tick row lets one fire per tick win: no election, and it survives a split brain. Where the fire must happen on one known node (a job that talks to a system that allows one caller), `WithLeaderElection(node, opts...)` limits cron firing and housekeeping to the leader of the role `scheduler`. The elector is a lease row (`<prefix>leaders`, migration `007_leader_archive`): the leader renews it, a crash hands the role on after `LeaderLease` (15 seconds by default), and `LeaderElector.Term` grows on every handover as a fencing token. Steps run on every client either way, and the tick row stays as the second guard. `Client.IsLeader` reports the answer. See also [nodes](../operations/nodes.md) for scheduler-only and worker-only processes (`WithSchedulerOnly`, `WithoutScheduler`).

Give every node the same option with its own node ID (empty means `workflow.DefaultNodeID()`, `host:pid`). The elector campaigns from `Start`, on the client's lease clock and `WithLeaseSkew`, over the client's own tables:

<!-- snippet: examples/sql-leader-election/main.go#election -->
```go
// newNode builds one node's client over the shared database.
newNode := func(node string) (*sqlstore.Client, error) {
	return sqlstore.New(db, sqlb.SQLiteDialect{},
		// Cron fires and housekeeping only while this node leads the role
		// "scheduler"; every node still runs steps.
		sqlstore.WithLeaderElection(node, sqlstore.LeaderLease(leaderLease)),
	)
}
```

A client that stops cleanly resigns, so the next node takes the role at its next campaign (every third of its `LeaderLease`) instead of after the lease:

<!-- snippet: examples/sql-leader-election/main.go#handover -->
```go
fmt.Fprintf(w, "node-a leads: %v, node-b leads: %v\n", a.IsLeader(), b.IsLeader())
a.Stop() // resigns: node-b takes the role at its next campaign, not after the lease
if err := waitFor("node-b to take over", b.IsLeader); err != nil {
	return err
}
fmt.Fprintf(w, "after node-a stopped: node-a leads: %v, node-b leads: %v\n", a.IsLeader(), b.IsLeader())
```

```text
node-a leads: true, node-b leads: false
after node-a stopped: node-a leads: false, node-b leads: true
```

`NewLeaderElector(db, dialect, role, node, opts...)` elects a leader for a role of your own (a nightly export, a single consumer of a partner API) over the leader table a client's `Migrate` created. `Campaign` makes one attempt and reports whether this node leads; `Run(ctx)` campaigns every third of the lease until ctx ends, then resigns. The options are `LeaderLease`, `LeaderSkew` (the margin a taker waits past the lease end), `LeaderClock` (a `DatabaseClock` makes every node compare against the database's time) and `LeaderTablePrefix` and `LeaderSchema`, which must match the client that migrated:

<!-- snippet: examples/sql-leader-election/main.go#elector -->
```go
// mk builds one node's elector for the app's role "nightly-export".
mk := func(node string) (*sqlstore.LeaderElector, error) {
	return sqlstore.NewLeaderElector(db, sqlb.SQLiteDialect{}, "nightly-export", node,
		sqlstore.LeaderLease(leaderLease),
		sqlstore.LeaderTablePrefix("wf_"), // the prefix of the client that migrated
	)
}
n1, err := mk("node-1")
if err != nil {
	return err
}
n2, err := mk("node-2")
if err != nil {
	return err
}
won1, err := n1.Campaign(ctx) // one attempt; Run(ctx) campaigns every third of the lease
if err != nil {
	return err
}
won2, err := n2.Campaign(ctx)
if err != nil {
	return err
}
fmt.Fprintf(w, "node-1 won: %v (term %d), node-2 won: %v\n", won1, n1.Term(), won2)
if err := n1.Resign(ctx); err != nil { // hand the role on now, not after the lease
	return err
}
won2, err = n2.Campaign(ctx)
if err != nil {
	return err
}
fmt.Fprintf(w, "after node-1 resigned: node-2 won: %v (term %d)\n", won2, n2.Term())
```

```text
node-1 won: true (term 1), node-2 won: false
after node-1 resigned: node-2 won: true (term 2)
```

`WithElector(e)` takes any `sqlstore.Elector` instead: one method, `IsLeader() bool`, called on every dispatcher pass, so it answers from memory and campaigns elsewhere. A Redis lock or a Kubernetes Lease fits the same way:

<!-- snippet: examples/sql-leader-election/main.go#custom-elector -->
```go
// flagElector is an Elector of the app's own: a Redis lock, a Kubernetes
// Lease or, here, a flag. IsLeader is called on every dispatcher pass, so
// it answers from memory.
type flagElector struct{ leads atomic.Bool }

// IsLeader implements sqlstore.Elector.
func (f *flagElector) IsLeader() bool { return f.leads.Load() }
```

<!-- snippet: examples/sql-leader-election/main.go#with-elector -->
```go
flag := &flagElector{}
gated, err := sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithElector(flag))
if err != nil {
	return err
}
fmt.Fprintf(w, "client with a follower elector may fire cron: %v\n", gated.IsLeader())
flag.leads.Store(true)
fmt.Fprintf(w, "and once it leads: %v\n", gated.IsLeader())
```

```text
client with a follower elector may fire cron: false
and once it leads: true
```

### Rows per dispatcher query

`WithFetchBatch(n)` sets how many rows one dispatcher query reads (100 by default; values below 1 are ignored): pending runs to admit, delayed runs to start, due sleeps, waits and signals, chains to hand on, schedules to fire, failing runs and run ends. A larger batch drains a backlog in fewer round trips; a smaller one keeps each pass and its transactions short. Workers still claim one step per query on purpose: a claim leases its step, and a lease held for a busy worker would keep the step from every other node. It is one more option on `New`; [`examples/sql-archive`](../../examples/sql-archive) sets it to 50 in [Keep the hot tables small](#keep-the-hot-tables-small).

## Keep the hot tables small

The queue's tables are hot: claims and admissions rewrite their rows, so on Postgres every finished run kept there adds index entries and every update a dead row for `VACUUM`. [Retention](../operations/retention.md) keeps them small. To keep the history anyway, add `WithArchiveTable()`: `Prune` then moves each finished run, with the outputs of its succeeded steps, into the cold tables `<prefix>runs_archive` and `<prefix>steps_archive` in the transaction that deletes it, so a run is in exactly one of the two places and the queue never reads the cold tables. The cold tables have no retention of their own: trim or partition them as the history's owner decides.

Step 1: give the client a retention and the archive table. `WithArchiveTable` needs `WithRetention`, which decides what is moved and when; a `WithArchive` hook, if set, still sees each chunk first. [`examples/sql-archive`](../../examples/sql-archive) keeps succeeded runs an hour and prunes by hand:

<!-- snippet: examples/sql-archive/main.go#archive -->
```go
c, err := sqlstore.New(db, sqlb.SQLiteDialect{},
	sqlstore.WithRetention(sqlstore.Retention{Succeeded: keepSucceeded, Undelivered: true}),
	sqlstore.WithArchiveTable(),         // Prune moves runs to wf_runs_archive and wf_steps_archive
	sqlstore.WithFetchBatch(fetchBatch), // rows per dispatcher query (default 100)
	sqlstore.WithIDGenerator(ids.next),  // run, step and delayed-start IDs
	sqlstore.WithoutAutoPrune(),         // this program calls Prune itself
	sqlstore.WithClock(clk),
)
```

Step 2: prune. Each chunk is moved and deleted in one transaction; the hot tables then no longer know the run:

<!-- snippet: examples/sql-archive/main.go#prune -->
```go
res, err := c.Prune(ctx) // moves, then deletes, in one transaction per chunk
if err != nil {
	return err
}
fmt.Fprintf(w, "pruned %d runs from the hot tables\n", res.Deleted)
if _, err := c.Status(ctx, first); errors.Is(err, workflow.ErrNotFound) {
	fmt.Fprintf(w, "status of %s: not found (it left the queue)\n", first.RunID)
}
```

Step 3: read the history back. `Client.ArchivedRuns(ctx, sqlstore.ArchiveQuery{Workflow, RunID, FinishedBefore, Limit})` returns `FinishedRun` values, newest finish first, each with its step outputs; `Limit` is 100 when zero. Page by passing the last `FinishedAt` of one page as the next page's `FinishedBefore`. It reads the tables directly, so it needs `Migrate`, not `Start`:

<!-- snippet: examples/sql-archive/main.go#read -->
```go
var before time.Time // zero: start from the newest finish
for page := 1; ; page++ {
	runs, err := c.ArchivedRuns(ctx, sqlstore.ArchiveQuery{Workflow: invoice.Name, FinishedBefore: before, Limit: pageSize})
	if err != nil {
		return err
	}
	if len(runs) == 0 {
		break
	}
	for _, r := range runs {
		fmt.Fprintf(w, "page %d: %s %s %s outputs %s\n", page, r.Ref.RunID, r.Status, r.Payload, outputs(r.Outputs))
	}
	before = runs[len(runs)-1].FinishedAt // the next page finished before the last one here
}
```

```text
pruned 3 runs from the hot tables
status of run_000001: not found (it left the queue)
page 1: run_000003 succeeded "INV-3" outputs render="pdf:INV-3" send="sent"
page 1: run_000002 succeeded "INV-2" outputs render="pdf:INV-2" send="sent"
page 2: run_000001 succeeded "INV-1" outputs render="pdf:INV-1" send="sent"
```

## Per-run facts, journal and bucket state

Migration `008_run_info_nodes` adds one `run_info` row per run: its tenant, the result written when it succeeds, the latest progress, and the nodes that claimed its attempts. That is what makes the SQL driver declare `CapTenant`, `CapResult`, `CapNodeIdentity`, `CapSnooze`, `CapHeartbeat`, `CapRunTimeout`, `CapKeyFIFO` and `CapDueQuery`, and what lets `Reschedule`, `CancelStep`, `ReportProgress` and `OnLifecycle` work across clients (see [results](../jobs/results.md), [progress](../jobs/progress.md), [tenants](../jobs/tenants.md) and [nodes](../operations/nodes.md)). A cancel of one step reaches a running attempt on another client at that client's next lease renewal. Migration `009_schedule_tenant` stores `Schedule.Tenant` in a side table (an `ALTER TABLE` cannot be written to run twice on MySQL) and sets it as the tenant of every run the schedule fires or is triggered to run; `ScheduleInfo.Tenant` reports it.

Two stores keep workflow state in the same database. `Client.JournalStore()` is a durable `workflow.JournalStore`, so the results `workflow.Memo` stored survive a crash and a retry on any node ([memoisation](../workflows/memoisation.md)). `Client.BucketStore()` is a durable `workflow.BucketStore` whose `Take` locks the bucket's row before reading its level, so every worker calling one upstream draws from one leaky bucket ([upstream limits](../jobs/upstream-limits.md)). Both can also be built over any `*sql.DB` with `sqlstore.NewJournalStore` and `sqlstore.NewBucketStore`, each with its own `Migrate` and its own migration list in the shared ledger, so an app on another driver gets durable state without running the SQL queue.

The same holds for every shared store: `sqlstore.NewDataStore` ([shared data](../workflows/memoisation.md#share-data-across-runs)), `NewPromiseStore`, `NewInbox`, `NewJournalStore`, `NewBucketStore` and `NewAggregateStore` each take the pool, the dialect and the client options. Of those options only `WithTablePrefix`, `WithSchema`, `WithTxAttempts` and `WithErrorClassifier` apply; the rest configure a client and are ignored. Each store's `Migrate` creates its own table and nothing else. The Redis driver has the same six constructors ([Redis shared stores](redis.md#shared-stores)). From [`examples/sql-options`](../../examples/sql-options), on a database no driver client runs on:

<!-- snippet: examples/sql-options/main.go#stores -->
```go
data, err := sqlstore.NewDataStore(db, sqlb.SQLiteDialect{}, sqlstore.WithTablePrefix("app_"))
if err != nil {
	return err
}
if err := data.Migrate(ctx); err != nil { // the data table only, in the shared ledger
	return err
}
journal, err := sqlstore.NewJournalStore(db, sqlb.SQLiteDialect{}, sqlstore.WithTablePrefix("app_"))
if err != nil {
	return err
}
if err := journal.Migrate(ctx); err != nil {
	return err
}
ver, err := data.Put(ctx, workflow.Folder("import"), "cursor", []byte(`42`), 0) // 0: no version check
if err != nil {
	return err
}
if err := journal.Save(ctx, "run_1", "fetch", []byte(`"page-1"`)); err != nil {
	return err
}
```

```text
data store without a client: cursor 42 (found true, version 1)
journal store without a client: fetch "page-1" (found true)
```

## Fair turns between keys

Migration `011_fair_turns` adds `fair_keys` (each round-robin key's place in its workflow's turns) and `run_fair` (each admitted round-robin run's turn). They make the SQL driver declare `CapRoundRobin` and `CapKeyWeights`: under `ConcurrencyRoundRobin` the keys of a workflow take turns for the workers, in proportion to `Concurrency.Weight` ([Share the workers fairly between keys](../jobs/concurrency.md#share-the-workers-fairly-between-keys)). Turns are written by the admission transaction, which already holds the workflow's lock row, so they add no lock; a claim that picks a step of a round-robin workflow makes one more read to hand that place to the waiting step with the earliest turn, and a workflow without round robin claims as before. The admission read also passes over runs whose bucket is full, so one tenant's backlog never fills the fetch window (`WithFetchBatch`).

## Enqueue inside a pgx transaction

`Client.EnqueueTx` takes the app's `*sql.Tx`. An app whose transactions are pgx transactions passes them through `sqlstore/pgxtx`, its own module (`go get github.com/ubgo/workflow/sqlstore/pgxtx@latest`, with `sqlstore` v0.1.1 or later): `pgxtx.Run(ctx, tx, func(stx *sql.Tx) error { ... })` hands `EnqueueTx` (or `outbox.Write`) a `*sql.Tx` whose statements run on the app's `pgx.Tx`, so the run commits or rolls back with the app's own pgx writes. The `*sql.Tx` is a view: its `Commit` and `Rollback` do nothing and it is valid only inside the function. Postgres only. See [Transactional enqueue](../reliability/transactional-enqueue.md).

## Database errors

The driver's correctness guards are unique indexes (an idempotency key held twice, a cron tick fired twice), and some transactions lose lock races. So it must sort each database error, which an `ErrorClassifier` does. It returns an `ErrorClass`:

| Field | Meaning | What the driver does |
|---|---|---|
| `UniqueViolation` | A unique constraint refused the write | Treats it as "someone else won the race". Say true only when sure. |
| `Retryable` | A deadlock, a serialization failure, SQLite busy | Retries the whole transaction, up to `WithTxAttempts` (default 5; 1 turns retrying off) |
| `Unavailable` | Refused, reset or dropped connection, server shutting down | Marks the error with `workflow.Unavailable`, so `errors.Is(err, workflow.ErrUnavailable)` holds and a [failover client](../reliability/failover.md#decide-what-counts-as-an-outage) can tell an outage from a bad request |

The zero value, "neither", is the safe answer: the error reaches the caller. `DefaultErrorClassifier` knows the drivers sqlb's contrib modules open: Postgres by SQLSTATE (23505 unique; 40P01 and 40001 retryable; class 08 and 57P01 to 57P03 unavailable), MySQL by "Error 1062", "Error 1213"/"Error 1205" and "invalid connection"/"Error 1040"/"Error 1053", SQLite by "UNIQUE constraint failed" and "SQLITE_BUSY"/"database is locked"; on any engine a `*net.OpError`, `driver.ErrBadConn`, `sql.ErrConnDone` or a pgx "failed to connect to" error is unavailable. A driver, proxy or wrapper whose errors read differently needs its own classifier, usually checking its own forms and falling back to the default. Without one, a lost idempotency race comes back as an error and a deadlock is not retried.

Step 1: write the classifier. It is called only with a non-nil error, which may be wrapped (use `errors.As`), and must be safe for concurrent use. [`examples/sql-options`](../../examples/sql-options) sorts a proxy's own codes and leaves the rest to the default:

<!-- snippet: examples/sql-options/main.go#classifier -->
```go
// proxyError is how a database proxy in front of the pool reports errors:
// a code of its own, which DefaultErrorClassifier cannot read.
type proxyError struct{ code string }

// Error implements error.
func (e *proxyError) Error() string { return "proxy: " + e.code }

// Codes the proxy sends.
const (
	proxyDuplicate = "DUP"  // a unique index refused the write
	proxyBusy      = "BUSY" // a lock conflict; try the transaction again
	proxyDown      = "DOWN" // the database behind the proxy is unreachable
)

// classifyProxy sorts the proxy's errors and leaves every other error to
// the default, which knows the drivers sqlb's contrib modules open.
func classifyProxy(err error) sqlstore.ErrorClass {
	var pe *proxyError
	if errors.As(err, &pe) {
		return sqlstore.ErrorClass{
			UniqueViolation: pe.code == proxyDuplicate,
			Retryable:       pe.code == proxyBusy,
			Unavailable:     pe.code == proxyDown,
		}
	}
	return sqlstore.DefaultErrorClassifier(err)
}
```

Step 2: give it to the client, with `WithTxAttempts` when lock races are common (a SQLite file many processes write):

<!-- snippet: examples/sql-options/main.go#classify -->
```go
c, err := sqlstore.New(db, dialect,
	sqlstore.WithErrorClassifier(classifyProxy),
	sqlstore.WithTxAttempts(txAttempts), // tries in all for a lost lock race (default 5; 1 turns it off)
)
```

Classifying a real SQLite duplicate-key error, two proxy errors (one wrapped) and an unknown error prints:

```text
sqlite duplicate: {UniqueViolation:true Retryable:false Unavailable:false}
proxy BUSY:      {UniqueViolation:false Retryable:true Unavailable:false}
proxy DOWN:      {UniqueViolation:false Retryable:false Unavailable:true}
other:           {UniqueViolation:false Retryable:false Unavailable:false}
```

The classifier also counts what the driver sees: Step 2 of [Create the client](#create-the-client) wraps the default to count lock conflicts for a metric.

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

A generator can shape each kind on its own. [`examples/sql-archive`](../../examples/sql-archive) numbers runs and steps apart, so the IDs it prints repeat on every run (a counter in one process is fine for a demo; production needs IDs unique across every client):

<!-- snippet: examples/sql-archive/main.go#ids -->
```go
// sequentialIDs numbers every ID per kind: "run_000001", "step_000001".
// An ID must be 1 to 64 bytes and unique per kind across every client of
// the database, so a real app uses a UUIDv7 or ULID; a counter in one
// process is enough for this demo and keeps its output stable.
type sequentialIDs struct{ runs, steps, others atomic.Int64 }

// next is the sqlstore.IDGenerator: it is told which kind of ID to make.
func (s *sequentialIDs) next(kind sqlstore.IDKind) (string, error) {
	n := &s.others // IDScheduled (a delayed run) and IDSchedule (an unnamed schedule)
	switch kind {
	case sqlstore.IDRun:
		n = &s.runs
	case sqlstore.IDStep:
		n = &s.steps
	case sqlstore.IDScheduled, sqlstore.IDSchedule:
	}
	return fmt.Sprintf("%s_%06d", kind, n.Add(1)), nil
}
```

An ID must be 1 to 64 bytes (the columns are `VARCHAR(64)`). A generator that returns an error, or an ID that is empty or too long, makes the driver refuse the operation that needed it; no bad row is written.

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
	fmt.Printf("failed: %s at step %s on queue %s\n", r.Workflow, r.FailedStep, r.Queue)
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
fmt.Println("mail worker drained:", err == nil)
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

The four options every app may want to set look like this in [`examples/sql-options`](../../examples/sql-options); the middleware prints each step it wraps:

<!-- snippet: examples/sql-options/main.go#options -->
```go
logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
// logSteps is a step middleware that prints each attempt it wraps.
logSteps := func(next workflow.Handler) workflow.Handler {
	return func(ctx context.Context, in []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		fmt.Fprintf(w, "middleware: %s/%s attempt %d\n", info.Workflow, info.Step, info.Attempt)
		return next(ctx, in)
	}
}
c, err := sqlstore.New(db, dialect,
	sqlstore.WithClock(clock.Real{}),                                  // every due time; tests pass clock.NewFake
	sqlstore.WithLogger(logger),                                       // what the driver cannot return to a caller
	sqlstore.WithMiddleware(logSteps),                                 // around every step and OnFailure handler
	sqlstore.WithJitterSource(func() float64 { return pinnedJitter }), // Backoff.Jitter's draws, pinned
)
```

`Client.DB()` returns the pool the client was built with: the escape hatch for reading the tables directly (a dashboard, a report) or running `Migrate` inside a wider schema step.

<!-- snippet: examples/sql-options/main.go#db -->
```go
var n int
// Client.DB is the pool the client was built with: read the tables
// directly for a report, or run Migrate inside a wider schema step.
if err := c.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM wf_runs WHERE status = 'succeeded'").Scan(&n); err != nil {
	return err
}
fmt.Fprintf(w, "succeeded runs in wf_runs: %d\n", n)
```

```text
middleware: send-receipt/send attempt 1
succeeded runs in wf_runs: 3
```

The tables are the driver's: read them, never write them, except for the [insert contract](sqlstore-insert-contract.md) that enqueues from another language.

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

Every section above has its program, each on SQLite files in a temporary directory, so none needs anything installed. Their output is shown in the section that uses it:

```sh
cd examples
go run ./sql-options          # pick a database, workers and Wake, database errors, options, stores on their own
go run ./sql-leader-election  # leases and takeovers, the database clock, leader election
go run ./sql-archive          # archive tables, fetch batch, your own IDs
go run ./insert-contract      # runs inserted by plain SQL, as another language would
```

## Record run history

A [run recorder](../observability/run-recorder.md) applied with `workflow.Use(client, rec.Plugin())` records every run of this driver, and its SQL store, `recorder/sqlrecorder`, can live in the same database: its tables carry their own prefix (`wf_rec_` by default), so they never collide with the driver's.

- **Every end is recorded.** The driver declares `CapRunFinished`: each run end is claimed from the table under a lease and delivered at least once, by one client with observers, including ends that happened while no client was running. Every worker wrapped with [`rec.Plugin()`](ds:block?id=rec-recorder-plugin-u7hfc2yc) over the same store is such a client, and the recorder's writes are upserts, so a repeat is harmless.
- **Records outlive the driver's retention.** [Retention](../operations/retention.md) deletes or archives finished runs from the queue tables; the record, with its steps and events, stays until the recorder's own two-stage [retention](../observability/run-recorder.md#retention) removes it.
- **Web nodes take `EnqueuePlugin`.** A client built with `WithEnqueueOnly()` delivers no run ends, so `rec.Plugin()` would record nothing but the enqueue there; apply [`rec.EnqueuePlugin()`](ds:block?id=rec-recorder-enqueueplugin-rn5k8npv) on the web node and `rec.Plugin()` on the workers over the same store ([Web nodes that only enqueue](../observability/run-recorder.md#web-nodes-that-only-enqueue), [examples/recorder-split](../../examples/recorder-split/)).
- **`EnqueueTx` and `EnqueueMany` pass no enqueue middleware.** They are methods of the driver's client, not of the client `Use` returns, so a run they create is recorded from its first step, with the source its labels carry, rather than at enqueue.
- **The Reconciler can backfill.** The driver declares `CapRunQuery`, so a Reconciler pass records runs created past the recorder (the ones above, an outbox relay through the bare client) before they start.

The recorder's end-to-end suite runs its driver cases on this driver on SQLite, Postgres and MySQL, recording into `sqlrecorder` in the driver's own database, and splits it into an enqueue-only web node and a worker on each engine.

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
- `WithArchiveTable` moves only what `Prune` would delete: without `WithRetention`, `Prune` deletes nothing, so nothing reaches the cold tables either. The guards still apply: a run that holds an idempotency key, whose end was never delivered (unless `Retention.Undelivered`), or whose batch is open stays in the hot tables.
- `WithDatabaseClock` makes `Start` read the database first and fail when it cannot. It overrides `WithClock` and `WithLeaseClock` whatever the order of options, so a test that passes a fake clock and the database clock gets the database clock.
- With `WithLeaderElection`, a crashed leader's cron fires pause until its `LeaderLease` (15 seconds by default) has passed, plus `WithLeaseSkew`; a clean `Stop` resigns and hands over at once. Where a few seconds without fires matter less than a split brain, keep the default (every client fires, the tick row picks one).
- An `Elector`'s `IsLeader` is called on every dispatcher pass: it must answer from memory, never by asking a server.
- A takeover after `ErrLeaseLost` counts toward `WithMaxReclaims`, so a database that stalls often can fail healthy steps with `ErrAbandoned`. Choose a `WithLease` longer than the worst stall, and a reclaim count above the takeovers a stall may cause.
- A run's stored error is text: `errors.Is(err, sqlstore.ErrAbandoned)` holds inside the attempt (and for `NoRetryOn`), not on the error a status read or a `RunEvent` returns later.

## Related

- [Run several workers](../operations/deploy.md#run-several-workers) and [shut down cleanly](../operations/deploy.md#shut-down-cleanly)
- [Migrations](../operations/migrations.md), [Retention](../operations/retention.md), [Tuning](../operations/tuning.md)
- [Record run history](../observability/run-recorder.md): a run history the app owns, in the same database
- [The in-memory driver](memory.md) and [the Hatchet driver](hatchet.md)
- [Transactional enqueue](../reliability/transactional-enqueue.md), [Tenants](../jobs/tenants.md), [Nodes and health](../operations/nodes.md)
- [Enqueue from other languages](sqlstore-insert-contract.md): the rows another program writes to start a run
- Examples: [sqlite-worker](../../examples/sqlite-worker/), [postgres-cluster](../../examples/postgres-cluster/), [sql-queue-workers](../../examples/sql-queue-workers/), [sql-options](../../examples/sql-options/), [sql-leader-election](../../examples/sql-leader-election/), [sql-archive](../../examples/sql-archive/), [insert-contract](../../examples/insert-contract/)
