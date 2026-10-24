# Deploy workers

This guide covers running your app as several worker processes over one database, web nodes that only enqueue, and stopping workers without losing work or retries. It applies to the [SQL driver](../drivers/sqlstore.md); the [Hatchet driver](../drivers/hatchet.md) is spread across workers by the engine, and the [memory driver](../drivers/memory.md) lives and dies with its process.

## Before you start

- A database every process can reach: Postgres or MySQL 8 for several machines, SQLite for several processes on one machine.
- The schema migrated before the new code starts: [Migrations](migrations.md).

## Run several workers

Every process runs the same start-up: `Migrate`, `Register`, `Start`. Clients over one database and one table prefix are workers of one queue; there is no leader to elect and nothing to configure per node.

Step 1: declare the workflows, the same in every process.

<!-- snippet: examples/postgres-cluster/main.go#workflows -->
```go
workflows := []workflow.Workflow{
	{Name: "resize", Steps: []workflow.Step{{Name: "only", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		ran.hit(ctx)
		return in, nil
	}}}},
	{Name: "hourly-report", Cron: []string{"0 * * * *"}, Steps: []workflow.Step{{Name: "only", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		ticks <- clk.Now()
		return nil, nil
	}}}},
}
```

Step 2: start a worker. The example starts three in one process so it can count what they did; in production each is its own process running this code.

<!-- snippet: examples/postgres-cluster/main.go#workers -->
```go
// Every process runs the same start-up: Migrate (safe to run from all of
// them at once; the engine's lock serialises it), Register, Start.
startCtx, stopAll := context.WithCancel(ctx)
defer stopAll()
clients := make([]*sqlstore.Client, workers)
for i := range clients {
	c, err := sqlstore.New(db, dialect,
		sqlstore.WithTablePrefix(prefix),
		sqlstore.WithClock(clk),
		sqlstore.WithPollInterval(pollInterval), // 20ms so the demo is quick; the default is 1s
	)
	if err != nil {
		return err
	}
	if err := c.Migrate(ctx); err != nil {
		return err
	}
	for _, wf := range workflows {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	if err := c.Start(startCtx); err != nil {
		return err
	}
	clients[i] = c
}
```

The example shares one fake clock so it can make the cron tick due on demand; leave `WithClock` out in production. It also polls every 20ms so the demo is quick; the default is one second.

What the cluster guarantees, from `sqlstore/doc.go` and the driver's cluster tests:

- **A step is claimed by one worker.** Postgres and MySQL claim with `FOR UPDATE SKIP LOCKED`; SQLite with one conditional `UPDATE`. Any worker can run any step of a workflow it registered.
- **A cron tick fires once across all workers.** Firing moves the schedule's next fire time and inserts a (schedule, fire time) row in one transaction; the worker that wins enqueues the run, every other one fails that insert and enqueues nothing.
- **A dead worker's steps are taken over** when their lease expires (30 seconds by default), so a step runs at least once: [Leases and crashed workers](../drivers/sqlstore.md#leases-and-crashed-workers).
- **Run ends reach an observer at least once**, including ends that happened while no client was running.
- **A worker only touches what it registered.** Services with different workflows can share the tables; a run of a workflow no running worker registered waits.

Work a worker makes due itself starts at once; work another worker made due is found at the next poll (`WithPollInterval`, one second by default).

## Web nodes that only enqueue

Web servers usually enqueue and never run jobs: they should not poll the database, fire crons or hold worker goroutines. A client built with `sqlstore.WithEnqueueOnly()` (`workflow.CapEnqueueOnly`) is that client: `Enqueue`, `EnqueueMany`, `EnqueueTx`, `Status`, `Cancel` and `Signal` work without `Start`, and `Start` returns nil and starts nothing. The program is [`examples/enqueue-only`](../../examples/enqueue-only):

<!-- snippet: examples/enqueue-only/main.go#web -->
```go
// A web node: it enqueues and reads status, and never runs a step.
// No Start, no worker goroutines, no cron.
web, err := sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithEnqueueOnly())
if err != nil {
	return err
}
if err := web.Migrate(ctx); err != nil { // usually your deploy step, not the web node
	return err
}
ref, err := web.Enqueue(ctx, workflow.Job{Workflow: "thumbnail", Payload: []byte(`"img-1"`)})
if err != nil {
	return err
}
```

Worker nodes are ordinary clients over the same tables:

<!-- snippet: examples/enqueue-only/main.go#worker -->
```go
c, err := sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithPollInterval(pollInterval))
if err != nil {
	return nil, nil, err
}
if err := c.Register(wf); err != nil {
	return nil, nil, err
}
wctx, cancel := context.WithCancel(ctx)
if err := c.Start(wctx); err != nil {
	cancel()
	return nil, nil, err
}
```

`Register` is optional on an enqueue-only client. A workflow registered there has its jobs checked and keyed as on any client; a job of a workflow not registered there passes only the checks that need no definition (`workflow.CheckJob`), and its workflow's concurrency key, rate keys and default queue are evaluated by the first worker that admits it. With a notifier ([The SQL driver › Wake workers at once](../drivers/sqlstore.md#wake-workers-at-once)), each enqueue wakes the workers before it returns.

| Driver | Enqueue-only mode |
|---|---|
| sqlstore | `sqlstore.WithEnqueueOnly()` |
| Redis | `redis.WithEnqueueOnly()` |
| Hatchet | `hatchet.Config{EnqueueOnly: true}`: connects on first use; the engine refuses a workflow no worker has registered yet, so start a worker once first |
| memory | Not declared: an in-process client has nobody else to run its work, so `Enqueue` still needs `Start` |

## Shut down cleanly

On SIGTERM, cancel the context you passed to `Start` and call `Stop`. `Stop` waits until every goroutine the client started has exited. Steps that were still running are handed back at once, so another worker takes them without waiting for their lease to expire; the runs stay in the database for the next client.

<!-- snippet: examples/postgres-cluster/main.go#shutdown -->
```go
// Shut down: cancel the Start context (what SIGTERM handling does) and
// call Stop, which waits until every goroutine has exited. Steps still
// running are handed back, so another worker takes them at once.
stopAll()
for _, c := range clients {
	c.Stop()
}
```

A step handed back this way is not a failure. On a driver with `workflow.CapShutdownRequeue` the next worker runs it with the same `StepInfo.Attempt`, and it is counted neither in `RunInfo.Attempts`, nor against the retry limit, nor in the [error history](../jobs/manage-runs.md#error-history), so a deploy never uses up a job's retries. In the example the step has no retries at all, and the run still succeeds after worker A is stopped mid-step:

```text
web enqueued true status queued
worker A runs attempt 1
worker A stopped
worker B runs attempt 1
run succeeded, attempts counted 1, errors recorded 0
```

After that, calls that need workers fail with `workflow.ErrNotStarted`. `Stop` is safe to call more than once and before `Start`.

The other drivers stop the same way, with different consequences:

| Driver | On `Stop` or a cancelled `Start` context |
|---|---|
| sqlstore | Running steps handed back to the cluster without spending an attempt (`CapShutdownRequeue`); every run stays in the database |
| Redis | Runs it holds handed back at once without spending an attempt (`CapShutdownRequeue`); runs stay in Redis |
| memory | Every unfinished run becomes `StatusCancelled` (observers see it) and is gone |
| Hatchet | The worker stops (cancel `Start`'s context; the client has no `Stop`); runs stay on the engine. A step whose worker died is reclaimed when its execution timeout expires (the crash case the driver's live tests prove) |

A step handler that ignores its context delays shutdown: pass the ctx you are given to every call that can block.

### Give running steps a grace period, and see what was cut off

Cancelling the `Start` context cuts running steps off at once. A deploy usually wants the opposite order: stop taking new work, give the running steps a fixed grace period to finish, then cut off whatever is left and say what it was. That is `Drain` (see [Stop gracefully](../concepts/client.md#stop-gracefully)); three helpers wrap it for a deploy script:

- `workflow.DrainTimeout(ctx, client, grace)` drains within `grace`. A grace of zero or less means no waiting at all, never "wait forever": a shutdown that can hang is the bug a grace period exists to prevent. A tighter deadline already on `ctx` wins.
- `workflow.DrainTimeoutReport(ctx, client, grace)` does the same and also returns a `workflow.DrainReport`. It returns `detailed` false when the client cannot report (no `CapDrainReport`), meaning the report is unknown, not that nothing was cut off.
- `workflow.ShutdownHook(client, grace)` and `workflow.ShutdownHookReport(client, grace, onReport)` return a plain `func(context.Context) error`. That is the shape every phased shutdown manager registers in a phase, so the library works with ubgo/shutdown, an `errgroup`-style stopper or your own signal loop without importing any of them. Register the hook in the phase that runs before the database and the HTTP listener close, because running steps still use both.

<!-- snippet: examples/graceful-drain-report/main.go#hook -->
```go
// Register this in the shutdown phase that runs before the database
// and HTTP listener close. It drains within the grace, or sooner if
// the phase's own ctx ends first, and reports what it cut off.
hook := workflow.ShutdownHookReport(c, grace, printReport(out, "memory"))
err = hook(ctx)
```

A `DrainReport` has three parts. `FinishedSteps` counts attempts that returned after the drain began and before its deadline. `FinishedRuns` counts runs that reached a terminal status from the drain's start until the client stopped (a driver that cannot see runs end leaves it 0). `Cut` lists each attempt still running at the deadline with its run, workflow, step and attempt number, and a `Disposition` saying what became of it:

| Disposition | Meaning | Drivers |
|---|---|---|
| `requeued` | Handed back to the store as ready, not charged an attempt (`CapShutdownRequeue`), so another worker runs it at once as the same attempt number | sqlstore, Redis |
| `cancelled` | Nothing outlives the process, so the run became `cancelled` | memory |
| `engine` | The engine, not this client, decides what happens to the attempt; the driver cannot say more without lying | Hatchet |

`report.Clean()` is true when nothing was cut off, `report.CutRuns()` counts the distinct runs cut off, and the error `Drain` returns is the context's error only when something was cut (`workflow.DrainOutcome` is that one rule, so drivers cannot disagree). A second `DrainWithReport` call returns the first call's report.

The example drains twice, with a step that never finishes inside the grace. On memory the cut run is cancelled; on SQLite the attempt is requeued and a second worker runs it as attempt 1 again, then the run succeeds:

```text
memory: detailed=true finished steps=0 runs=0, cut off 1
  cut: thumbnail/resize attempt 1 -> cancelled
memory: drain error is a deadline: true
memory: cut run status: cancelled
sql: worker A runs attempt 1
sql: detailed=true finished steps=0 runs=0, cut off 1
  cut: thumbnail/resize attempt 1 -> requeued
sql: worker B runs attempt 1
sql: run finished: succeeded
```

The program is [`examples/graceful-drain-report`](../../examples/graceful-drain-report).

## Try it

```sh
cd examples
go run ./postgres-cluster
```

```text
30 jobs, 30 ran exactly once
hourly-report fired at 10:00
fired once across 3 workers
stopped; Enqueue now fails with workflow: client not started
```

Without `PG_DSN` it runs on a SQLite file. To run the same three workers on Postgres, point `PG_DSN` at a database you can create tables in; the program uses a fresh table prefix and drops its tables at the end:

```sh
PG_DSN="postgres://$USER@localhost:5432/ubgo_workflow_test?sslmode=disable" go run ./postgres-cluster
```

## Pitfalls

- Run `Migrate` before the new release's `Start`. A database missing a table the release needs fails `Start`; see [Migrations › Run the migrations](migrations.md#run-the-migrations).
- Every worker of one queue needs the same table prefix and the same cron parser. Two prefixes are two queues that share nothing.
- SQLite serialises writers. Several processes on one file work, but heavy write load belongs on Postgres or MySQL; raise `WithTxAttempts` for a file many processes write.
- A crashed worker's steps wait up to one lease. Shorten `WithLease` for faster takeover, at the cost of more renewals.
- **A requeue without penalty needs a clean stop.** Cancelling `Start`'s context and calling `Stop` hands steps back at once; a killed process (SIGKILL, OOM, a lost machine) leaves them until the lease expires, and that attempt is counted, as a crash should be.
- **An enqueue-only client never delivers run ends.** Register run-finished observers (alerts, reporters, unique-lock release) on the workers.
- Cancel reaches a step on another worker at that worker's next lease renewal, a third of the lease later.

## Related

- [The SQL driver](../drivers/sqlstore.md)
- [Migrations](migrations.md), [Retention](retention.md), [Tuning](tuning.md)
- [Nodes and health](nodes.md): which node ran each attempt, liveness and readiness probes, scheduler-only and worker-only processes
- Example: [postgres-cluster](../../examples/postgres-cluster/)
- Example: [graceful-drain-report](../../examples/graceful-drain-report/) (a drain with a grace period and its report)
