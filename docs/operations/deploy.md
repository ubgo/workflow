# Deploy workers

This guide covers running your app as several worker processes over one database, web nodes that only enqueue, and stopping workers without losing work or retries. It applies to the [SQL driver](../drivers/sqlstore.md); the [Hatchet driver](../drivers/hatchet.md) is spread across workers by the engine, and the [memory driver](../drivers/memory.md) lives and dies with its process.

## Before you start

- A database every process can reach: Postgres or MySQL 8 for several machines, SQLite for several processes on one machine.
- The schema migrated before the new code starts: [Migrations](migrations.md).

## Run several workers

Every process runs the same start-up: `Migrate`, `Register`, [`Start`](ds:block?id=sql-client-start-ab8ayzsy). Clients over one database and one table prefix are workers of one queue; there is no leader to elect and nothing to configure per node.

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

The example shares one fake clock so it can make the cron tick due on demand; leave `WithClock` out in production. It also polls every 20ms so the demo is quick; the default is one second ([`defaultPoll`](ds:block?id=sql-defaultpoll-nn9t9vqu)).

What the cluster guarantees, from `sqlstore/doc.go` and the driver's cluster tests:

- **A step is claimed by one worker.** Postgres and MySQL claim with `FOR UPDATE SKIP LOCKED`; SQLite with one conditional `UPDATE` ([`claimFrom`](ds:block?id=sql-client-claimfrom-ucz7r6ys)). Any worker can run any step of a workflow it registered.
- **A cron tick fires once across all workers.** Firing moves the schedule's next fire time and inserts a (schedule, fire time) row in one transaction ([`fireOne`](ds:block?id=sql-client-fireone-mfjemgus)); the worker that wins enqueues the run, every other one fails that insert and enqueues nothing.
- **A dead worker's steps are taken over** when their lease expires (30 seconds by default, [`defaultLease`](ds:block?id=sql-defaultlease-qygq5sbe)), so a step runs at least once: [Leases and crashed workers](../drivers/sqlstore.md#leases-and-crashed-workers).
- **Run ends reach an observer at least once**, including ends that happened while no client was running (the driver declares `CapRunFinished`: [`caps`](ds:block?id=sql-caps-yu37ef2j)).
- **A worker only touches what it registered.** Services with different workflows can share the tables; a run of a workflow no running worker registered waits ([`claimFrom`](ds:block?id=sql-client-claimfrom-ucz7r6ys) claims only registered workflows).

Work a worker makes due itself starts at once; work another worker made due is found at the next poll ([`WithPollInterval`](ds:block?id=sql-withpollinterval-4mg6xgyu), one second by default).

## Web nodes that only enqueue

Web servers usually enqueue and never run jobs: they should not poll the database, fire crons or hold worker goroutines. A client built with [`sqlstore.WithEnqueueOnly()`](ds:block?id=sql-withenqueueonly-z8xgtkdr) ([`workflow.CapEnqueueOnly`](ds:block?id=capenqueueonly-9mhemhe8)) is that client: `Enqueue`, `EnqueueMany`, `EnqueueTx`, `Status`, `Cancel` and `Signal` work without `Start`, and `Start` returns nil and starts nothing. The program is [`examples/enqueue-only`](../../examples/enqueue-only):

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

`Register` is optional on an enqueue-only client ([`WithEnqueueOnly`](ds:block?id=sql-withenqueueonly-z8xgtkdr)). A workflow registered there has its jobs checked and keyed as on any client; a job of a workflow not registered there passes only the checks that need no definition (`workflow.CheckJob`), and its workflow's concurrency key, rate keys and default queue are evaluated by the first worker that admits it. With a notifier ([The SQL driver › Wake workers at once](../drivers/sqlstore.md#wake-workers-at-once)), each enqueue wakes the workers before it returns.

| Driver | Enqueue-only mode |
|---|---|
| sqlstore | [`sqlstore.WithEnqueueOnly()`](ds:block?id=sql-withenqueueonly-z8xgtkdr) |
| Redis | [`redis.WithEnqueueOnly()`](ds:block?id=redis-withenqueueonly-wt5y2y49) |
| Hatchet | `hatchet.Config{EnqueueOnly: true}` ([`startEnqueueOnly`](ds:block?id=hatchet-client-startenqueueonly-ddszquvp)): connects on first use; the engine refuses a workflow no worker has registered yet, so start a worker once first |
| memory | Not declared ([`caps`](ds:block?id=memory-caps-yxkuq583)): an in-process client has nobody else to run its work, so `Enqueue` still needs `Start` |

A [run recorder](../observability/run-recorder.md) on a web node takes `rec.EnqueuePlugin()`, not `rec.Plugin()`: an enqueue-only client delivers no run ends (Hatchet's refuses `OnRunFinished`), so the web node records each accepted run with its source and input, and the workers, wrapped with `rec.Plugin()` over the same store, record the steps and the end ([Record run history › Web nodes that only enqueue](../observability/run-recorder.md#web-nodes-that-only-enqueue), [examples/recorder-split](../../examples/recorder-split/)).

## Deploy a new version while old workers run

During a rolling deploy old and new processes run side by side. When the new code changes a workflow so that a run started by it would break on the old code (a step added, removed or renamed, a payload shape changed), give the definition a version: `Workflow.Version`. Every run records the `Version` of the client that enqueued it, and a worker admits, runs and advances only runs whose version lies in its definition's `[MinVersion, Version]` ([`Workflow.RunsVersion`](ds:block?id=workflow-runsversion-r83ptyyz), [`CapWorkflowVersion`](ds:block?id=capworkflowversion-6gd7k448)). A run started by the new code never reaches an old worker; it waits, queued, until a worker that runs its version takes it, as a run of a workflow no worker has registered does. `MinVersion` zero (the default) lets the new definition drain the runs its predecessors started, so a deploy needs no pause; set `MinVersion` to `Version` to pin each run to the code that started it and keep old workers until their runs are done. An unversioned definition (`Version` 0) runs only unversioned runs ([`Workflow.RunsVersion`](ds:block?id=workflow-runsversion-r83ptyyz)).

A definition that runs older versions runs them with its own steps, so a step whose behaviour changed branches on `StepInfo.Version`, the run's version:

<!-- snippet: examples/rolling-deploy/main.go#v2 -->
```go
return workflow.Workflow{Name: "checkout", Version: 2, Steps: []workflow.Step{
	{Name: "fraud", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		if info, _ := workflow.StepInfoFrom(ctx); info.Version < 2 {
			j.add("new: no fraud check for a run the old code started (run v%d)", info.Version)
			return in, nil // the old checkout already took the order
		}
		j.add("new: fraud check (run v%d)", version(ctx))
		return in, nil
	}},
	{Name: "charge", After: []string{"fraud"}, Run: func(ctx context.Context, in []byte) ([]byte, error) {
		j.add("new: charge (run v%d)", version(ctx))
		return in, nil
	}},
	{Name: "email", After: []string{"charge"}, Run: func(ctx context.Context, in []byte) ([]byte, error) {
		if info, _ := workflow.StepInfoFrom(ctx); info.Version < 2 {
			j.add("new: plain email for a run the old code started (run v%d)", info.Version)
			return in, nil
		}
		j.add("new: email with the fraud score (run v%d)", version(ctx))
		return in, nil
	}},
}}
```

Until a new worker is up, the new runs wait:

<!-- snippet: examples/rolling-deploy/main.go#waiting -->
```go
st, err := old.Status(ctx, second)
if err != nil {
	return err
}
fmt.Println("  the v2 run on a fleet of v1 workers:", st) // queued until a v2 worker takes it
```

`RunInfo.Version` shows each run's version in the admin API and `Runs`. The [memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-p5caps-wggh6esn) and [Redis](ds:block?id=redis-caps-bfgdst9t) drivers declare `CapWorkflowVersion`; the SQL driver keeps the version in `<prefix>run_version` (migration `010_run_version`, [`runVersionDefs`](ds:block?id=sql-runversiondefs-nmhgsrjq)), the Redis driver in the run and in per-version keys. Hatchet refuses a versioned workflow at [`Register`](ds:block?id=hatchet-client-register-zg59b2j4) ([`workflow.Requires`](ds:block?id=requires-6tcjbcfj) asks for `CapWorkflowVersion`, which [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z) lacks): it hands a run to any worker that registered the workflow's name.

## Shut down cleanly

On SIGTERM, cancel the context you passed to `Start` and call [`Stop`](ds:block?id=sql-client-stop-p9pcyebp). `Stop` waits until every goroutine the client started has exited. Steps that were still running are handed back at once ([`release`](ds:block?id=sql-client-release-psd7yyay)), so another worker takes them without waiting for their lease to expire; the runs stay in the database for the next client.

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

A step handed back this way is not a failure. Ask the client (here `a`, a SQL worker from [`examples/graceful-drain-report`](../../examples/graceful-drain-report)) whether it does this before relying on it:

<!-- snippet: examples/graceful-drain-report/main.go#caps -->
```go
caps := a.Capabilities()
fmt.Printf("sql: drain report %t, requeue on a clean stop %t\n",
	caps.Has(workflow.CapDrainReport), caps.Has(workflow.CapShutdownRequeue))
```

On a driver with [`workflow.CapShutdownRequeue`](ds:block?id=capshutdownrequeue-q5p8zrkz) ([sqlstore](ds:block?id=sql-p4caps-ns9ehh5e) and [Redis](ds:block?id=redis-caps-bfgdst9t)) the next worker runs it with the same `StepInfo.Attempt`, and it is counted neither in `RunInfo.Attempts`, nor against the retry limit, nor in the [error history](../jobs/manage-runs.md#error-history), so a deploy never uses up a job's retries. In the example the step has no retries at all, and the run still succeeds after worker A is stopped mid-step:

```text
web enqueued true status queued
worker A runs attempt 1
worker A stopped
worker B runs attempt 1
run succeeded, attempts counted 1, errors recorded 0
```

After that, calls that need workers fail with `workflow.ErrNotStarted`. [`Stop`](ds:block?id=sql-client-stop-p9pcyebp) is safe to call more than once and before `Start`.

The other drivers stop the same way, with different consequences:

| Driver | On `Stop` or a cancelled `Start` context |
|---|---|
| sqlstore | Running steps handed back to the cluster without spending an attempt (`CapShutdownRequeue`, [`release`](ds:block?id=sql-client-release-psd7yyay)); every run stays in the database |
| Redis | Runs it holds handed back at once without spending an attempt (`CapShutdownRequeue`, [`Stop`](ds:block?id=redis-client-stop-az3e3p93)); runs stay in Redis |
| memory | Every unfinished run becomes `StatusCancelled` (observers see it) and is gone ([`shutdown`](ds:block?id=memory-client-shutdown-kwcx9htd); [`Drain`](ds:block?id=memory-client-drain-d2bhfshw) ends the same way) |
| Hatchet | The worker stops (cancel `Start`'s context; the client has no `Stop`); runs stay on the engine ([`DrainWithReport`](ds:block?id=hatchet-client-drainwithreport-aze8rd7c)). A step whose worker died is reclaimed when its execution timeout expires (the crash case the driver's live tests prove) |

A step handler that ignores its context delays shutdown: pass the ctx you are given to every call that can block.

### Give running steps a grace period, and see what was cut off

Cancelling the `Start` context cuts running steps off at once. A deploy usually wants the opposite order: stop taking new work, give the running steps a fixed grace period to finish, then cut off whatever is left and say what it was. That is `Drain` (see [Stop gracefully](../concepts/client.md#stop-gracefully)); three helpers wrap it for a deploy script:

- [`workflow.DrainTimeout(ctx, client, grace)`](ds:block?id=draintimeout-esadwb5f) drains within `grace`. A grace of zero or less means no waiting at all ([`DrainContext`](ds:block?id=draincontext-b6n9r79e)), never "wait forever": a shutdown that can hang is the bug a grace period exists to prevent. A tighter deadline already on `ctx` wins.
- [`workflow.DrainTimeoutReport(ctx, client, grace)`](ds:block?id=draintimeoutreport-febd5nuj) does the same and also returns a `workflow.DrainReport`. It returns `detailed` false when the client cannot report (no `CapDrainReport`), meaning the report is unknown, not that nothing was cut off.
- [`workflow.ShutdownHook(client, grace)`](ds:block?id=shutdownhook-nwhvw4d6) and [`workflow.ShutdownHookReport(client, grace, onReport)`](ds:block?id=shutdownhookreport-7as6cre3) return a plain `func(context.Context) error`. That is the shape every phased shutdown manager registers in a phase, so the library works with ubgo/shutdown, an `errgroup`-style stopper or your own signal loop without importing any of them. Register the hook in the phase that runs before the database and the HTTP listener close, because running steps still use both.

<!-- snippet: examples/graceful-drain-report/main.go#hook -->
```go
// Register this in the shutdown phase that runs before the database
// and HTTP listener close. It drains within the grace, or sooner if
// the phase's own ctx ends first, and reports what it cut off.
hook := workflow.ShutdownHookReport(c, grace, printReport("memory"))
err = hook(ctx)
```

The callback receives the report and `detailed`, which is false when the client has no `CapDrainReport` ([`ShutdownHookReport`](ds:block?id=shutdownhookreport-7as6cre3)): the report is then unknown, not clean. A deploy script's callback logs one line and each cut-off attempt:

<!-- snippet: examples/graceful-drain-report/main.go#report -->
```go
func printReport(label string) func(workflow.DrainReport, bool) {
	// next says what becomes of a cut-off attempt; DrainDisposition is a
	// closed set, so every value has an entry.
	next := map[workflow.DrainDisposition]string{
		workflow.DrainRequeued:    "another worker runs it as the same attempt",
		workflow.DrainCancelled:   "the run is cancelled",
		workflow.DrainEngineOwned: "the engine decides",
	}
	return func(r workflow.DrainReport, detailed bool) {
		if !detailed { // no CapDrainReport: the report is unknown, not clean
			fmt.Printf("%s: the driver cannot say what a drain cut off\n", label)
			return
		}
		fmt.Printf("%s: finished steps=%d runs=%d, clean=%t, cut off %d attempt(s) in %d run(s)\n",
			label, r.FinishedSteps, r.FinishedRuns, r.Clean(), len(r.Cut), r.CutRuns())
		for _, c := range r.Cut {
			fmt.Printf("  cut: %s/%s attempt %d -> %s: %s\n", c.Workflow, c.Step, c.Attempt, c.Disposition, next[c.Disposition])
		}
	}
}
```

A [`DrainReport`](ds:block?id=drainreport-6gp3974h) has three parts. `FinishedSteps` counts attempts that returned after the drain began and before its deadline. `FinishedRuns` counts runs that reached a terminal status from the drain's start until the client stopped (a driver that cannot see runs end leaves it 0). `Cut` lists each attempt still running at the deadline with its run, workflow, step and attempt number, and a [`Disposition`](ds:block?id=draindisposition-xjkm645x) saying what became of it:

| Disposition | Meaning | Drivers |
|---|---|---|
| `requeued` | Handed back to the store as ready, not charged an attempt (`CapShutdownRequeue`), so another worker runs it at once as the same attempt number | [sqlstore](ds:block?id=sql-client-drainwithreport-dvhyw82q), [Redis](ds:block?id=redis-client-drainwithreport-zxumh4bq) |
| `cancelled` | Nothing outlives the process, so the run became `cancelled` | [memory](ds:block?id=memory-client-drainwithreport-752nb52t) |
| `engine` | The engine, not this client, decides what happens to the attempt; the driver cannot say more without lying | [Hatchet](ds:block?id=hatchet-client-drainwithreport-aze8rd7c) |

[`report.Clean()`](ds:block?id=drainreport-clean-ax7h22gu) is true when nothing was cut off, [`report.CutRuns()`](ds:block?id=drainreport-cutruns-mzxd4et9) counts the distinct runs cut off (one run can have several attempts running at once), and the error `Drain` returns is the context's error only when something was cut ([`workflow.DrainOutcome`](ds:block?id=drainoutcome-vausxe9q) is that one rule, so drivers cannot disagree). A second `DrainWithReport` call returns the first call's report ([`ReportingDrainer`](ds:block?id=reportingdrainer-najup2m2)).

Without the hook, the same drain takes three calls. A client that declares `CapDrainReport` implements [`workflow.ReportingDrainer`](ds:block?id=reportingdrainer-najup2m2), whose `DrainWithReport` behaves exactly as `Drain` and also returns the report; [`workflow.DrainContext`](ds:block?id=draincontext-b6n9r79e) puts the grace period on the ctx:

<!-- snippet: examples/graceful-drain-report/main.go#drain-with-report -->
```go
// What ShutdownHookReport does, by hand: a grace period on the ctx,
// then DrainWithReport when the client is a ReportingDrainer.
dctx, cancel := workflow.DrainContext(ctx, grace) // zero or less: no waiting at all
var report workflow.DrainReport
rd, detailed := any(a).(workflow.ReportingDrainer)
if detailed {
	report, err = rd.DrainWithReport(dctx)
} else {
	err = a.Drain(dctx) // it still drains; the report stays unknown
}
cancel()
printReport("sql")(report, detailed)
// Drain's error is DrainOutcome: nil when nothing was cut off, else the ctx's error.
fmt.Println("sql: drain error is the deadline:", errors.Is(err, context.DeadlineExceeded),
	"same as DrainOutcome:", err == workflow.DrainOutcome(report, context.DeadlineExceeded))
```

A driver of your own builds its report with [`workflow.DrainTracker`](ds:block?id=draintracker-368xeckq), the bookkeeping every built-in driver embeds (see [Writing a driver](../extending/writing-a-driver.md)).

The example drains twice, with a step that never finishes inside the grace. On memory the cut run is cancelled; on SQLite the attempt is requeued and a second worker runs it as attempt 1 again, then the run succeeds. Run it with `go run ./graceful-drain-report` from `examples/`:

```text
memory: finished steps=0 runs=0, clean=false, cut off 1 attempt(s) in 1 run(s)
  cut: thumbnail/resize attempt 1 -> cancelled: the run is cancelled
memory: drain error is a deadline: true
memory: cut run status: cancelled
sql: worker A runs attempt 1
sql: drain report true, requeue on a clean stop true
sql: finished steps=0 runs=0, clean=false, cut off 1 attempt(s) in 1 run(s)
  cut: thumbnail/resize attempt 1 -> requeued: another worker runs it as the same attempt
sql: drain error is the deadline: true same as DrainOutcome: true
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

- Run `Migrate` before the new release's `Start`. A database missing a table the release needs fails `Start` ([`checkSchema`](ds:block?id=sql-client-checkschema-5y2gj6z7)); see [Migrations › Run the migrations](migrations.md#run-the-migrations).
- Every worker of one queue needs the same table prefix and the same cron parser. Two prefixes are two queues that share nothing.
- SQLite serialises writers. Several processes on one file work, but heavy write load belongs on Postgres or MySQL; raise [`WithTxAttempts`](ds:block?id=sql-withtxattempts-4aey9mx7) for a file many processes write.
- A crashed worker's steps wait up to one lease. Shorten [`WithLease`](ds:block?id=sql-withlease-gtcuhgcm) for faster takeover, at the cost of more renewals.
- **A requeue without penalty needs a clean stop.** Cancelling `Start`'s context and calling `Stop` hands steps back at once; a killed process (SIGKILL, OOM, a lost machine) leaves them until the lease expires, and that attempt is counted, as a crash should be ([`release`](ds:block?id=sql-client-release-psd7yyay) runs only on a clean stop).
- **An enqueue-only client never delivers run ends** ([`WithEnqueueOnly`](ds:block?id=sql-withenqueueonly-z8xgtkdr)). Register run-finished observers (alerts, reporters, unique-lock release) on the workers.
- Cancel reaches a step on another worker at that worker's next lease renewal, a third of the lease later ([`heartbeat`](ds:block?id=sql-client-heartbeat-uu2x8xhn)).
- **Versions only work between releases that have them.** A worker built before workflow versions existed runs every run of the workflow's name. Ship a release with the workflow at `Version` 0 or 1 first, then bump it in a later one. A web node that enqueues without registering the workflow stamps version 0 ([`WithEnqueueOnly`](ds:block?id=sql-withenqueueonly-z8xgtkdr): an unregistered workflow is a definition-free stand-in), so register the workflow on web nodes too.
- **A definition with `MinVersion` below `Version` runs old runs with its own steps.** On the SQL driver a run an old worker already started keeps its step rows, so keep the old step names in the new definition, or pin versions and let old workers drain.
- **On Hatchet a stopped worker does not hand its steps back, and versions are refused.** A worker stopped by its `Start` context cancels the steps running on it and the engine counts the attempt (no `CapShutdownRequeue` in [`caps`](ds:block?id=hatchet-caps-d6x9fv8z)), so `Drain` before a deploy; `Workflow.Version` and `MinVersion` are refused at [`Register`](ds:block?id=hatchet-client-register-zg59b2j4), because the engine hands a run to any worker that registered the name. See [Hatchet limitations](../drivers/hatchet.md#limitations).

## Related

- [The SQL driver](../drivers/sqlstore.md)
- [Migrations](migrations.md), [Retention](retention.md), [Tuning](tuning.md)
- [Nodes and health](nodes.md): which node ran each attempt, liveness and readiness probes, scheduler-only and worker-only processes
- Example: [postgres-cluster](../../examples/postgres-cluster/)
- Example: [rolling-deploy](../../examples/rolling-deploy/) (workflow versions while old and new workers share a database)
- Example: [graceful-drain-report](../../examples/graceful-drain-report/) (a drain with a grace period and its report)
