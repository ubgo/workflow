# Choose an operations tool

Two operational choices that look alike from a distance: where finished runs go when the queue no longer needs them (deleted, copied out by a hook, moved to archive tables in the same database, or recorded by the run recorder), and how a process avoids doing work too early (a start gate that holds `Start`, a readiness probe that holds traffic, a clock that can be trusted). Every option links to its guide and to an example that runs it.

## Archive tables, the WithArchive hook or the run recorder

The SQL driver keeps every finished run until you turn on retention, and the hot tables slow down as they grow. Each option below keeps a different amount of history in a different place.

| Option | What is kept, and where | Written when | Read back with | Drivers | Guide | Example |
|---|---|---|---|---|---|---|
| `sqlstore.WithRetention(r)` alone | Nothing: finished runs past their age or beyond `KeepLast` are deleted, with their steps | The background prune (hourly by default, one client per interval) or your own `Prune` | nothing | sqlstore | [Keep finished runs for a while](../operations/retention.md#keep-finished-runs-for-a-while) | [retention](../../examples/retention/) |
| `sqlstore.WithArchive(fn)` | Whatever your hook writes (an object store, another database, a data lake), from `FinishedRun`: workflow, status, labels, payload, error text, times and each succeeded step's output | Before each chunk is deleted; an error keeps the chunk and stops the prune | your own store | sqlstore, with `WithRetention` | [Archive before deleting](../operations/retention.md#archive-before-deleting) | [retention](../../examples/retention/) |
| `sqlstore.WithArchiveTable()` | The same fields, in `<prefix>runs_archive` and `<prefix>steps_archive` in the same database, append-only | In the transaction that deletes the run from the hot tables, so a run is in exactly one place | `Client.ArchivedRuns(ctx, ArchiveQuery{...})`, newest finish first; needs `Migrate`, not `Start` | sqlstore (migration `007_leader_archive`), with `WithRetention` | [Keep the hot tables small](../drivers/sqlstore.md#keep-the-hot-tables-small) | [sql-archive](../../examples/sql-archive/) |
| The run recorder (`recorder` module) with `sqlrecorder` or `recorder.MemoryStore` | A record per run: who started it (`Source`), every attempt of every step with input, output, error, node and events, and the outcome; kept apart from the driver | As the run happens, through step and enqueue middleware and the run-finished hook | `Store.Run`, `Store.Records`, `Store.Runs` (a `workflow.RunQuerier`) | every driver (Hatchet: plus the `Reconciler`) | [Record run history](../observability/run-recorder.md#wire-it-in) | [run-recorder](../../examples/run-recorder/), [recorder-sql](../../examples/recorder-sql/) |
| The recorder's own retention and `Retention.Archive` | Step rows dropped after `Steps`, the run record handed to `Archive` and deleted after `Runs` | When you call the store's `Prune` | your own store | `sqlrecorder`, `MemoryStore` | [Retention](../observability/run-recorder.md#retention) | [recorder-sql](../../examples/recorder-sql/) |

- **If you only need the queue to stay fast**, turn on `WithRetention` and nothing else.
- **If history must be kept for audit or analytics outside the database**, add `WithArchive` and write each chunk to cold storage; a failed copy keeps the chunk for the next prune.
- **If history should stay queryable next to your data without slowing the queue**, add `WithArchiveTable`. The cold tables have no retention of their own: trim or partition them yourself.
- **If people need to see who started a run and what each step did**, on any driver and past the driver's own retention, use the run recorder. It answers a different question from the archive: the archive keeps the driver's final row, the recorder keeps the timeline.
- **If you are on Redis, memory or Hatchet**, finished runs expire by the driver's or engine's retention and there is no archive option: use the run recorder for history.
- **If you want both**, they combine: `WithArchiveTable` and `WithArchive` on the same client (the hook sees each chunk first), and the recorder beside either.

## StartWhen or a readiness probe

| Option | What it holds back | Until | Meanwhile | Drivers | Guide | Example |
|---|---|---|---|---|---|---|
| `workflow.StartWhen(ctx, c, ready, opts)` | `Start` itself: no schedule fires, no step runs, no tick is computed from a wrong clock | `ready` reports true (asked every `Poll`, one second by default); an error from `ready` or the end of `ctx` stops the wait | `Enqueue` and `Signal` return `workflow.ErrNotStarted` | every driver | [Start when ready](../operations/start-gate.md#step-1--hold-start-until-the-clock-is-synchronised) | [start-gate](../../examples/start-gate/) |
| `workflow.ClockAfter(clk, floor)` | Used as the `ReadyFunc` above | The clock reads `floor` or later (the binary's build time, in UTC) | as above | every driver | [Start when ready](../operations/start-gate.md#step-1--hold-start-until-the-clock-is-synchronised) | [start-gate](../../examples/start-gate/) |
| A readiness probe from `workflow.CheckHealth(ctx, c)` | Traffic from the load balancer or orchestrator; the client itself runs as usual | `Ready` is true: started, not draining, and the database, Redis or engine answers | the process stays up (`Live` is true), so an outage does not cause a restart storm | memory, sqlstore, Redis, Hatchet implement `HealthChecker`; any other client is reported ready with a check saying it cannot tell | [Health and readiness probes](../operations/nodes.md#health-and-readiness-probes) | [node-health](../../examples/node-health/) |
| `clock.WakeChecked(clock.Real{}, every)` | Nothing: it makes timers fire by the wall clock after a laptop or device sleeps | every timer re-checks at most every `every` (30 seconds by default) | timers that fell due during the sleep fire within one check of the resume | every driver's `WithClock`; the memory driver is the one that needs it | [Laptops that sleep](../operations/sleep-and-wake.md#wrap-the-clock) | [sleep-wake](../../examples/sleep-wake/) |
| `sqlstore.WithDatabaseClock(resync)` | Nothing: due times and leases use the database's clock, so nodes with skewed clocks agree | re-read every `resync` (a minute by default) | `Start` fails when the first read fails | sqlstore | [Use the database's clock](../drivers/sqlstore.md#use-the-databases-clock) | [sql-leader-election](../../examples/sql-leader-election/) |

- **If starting too early does harm** (an edge device that boots at 1970, a scheduler that must not fire before a migration ran or a VPN is up), use `StartWhen` in place of `Start`.
- **If the process may start but should get no requests until its backend answers**, serve `/readyz` from `CheckHealth` and let the orchestrator hold traffic; restart only on `/livez`.
- **If both apply**, use both: before `Start` a client reports live but not ready, so a pod behind a `StartWhen` gate stays out of rotation until the gate opens.
- **If the clock is right but the machine sleeps**, wrap it with `clock.WakeChecked`; if the clock may be wrong after a power cycle, add `StartWhen` with `ClockAfter` too.
- **If several servers' clocks disagree**, the cure is `WithDatabaseClock` and `WithLeaseSkew` on the SQL driver, not a start gate.

## Pitfalls

- `WithArchiveTable` without `WithRetention` moves nothing: retention decides what is moved and when. Nothing refuses the combination.
- Page `ArchivedRuns` with both halves of the cursor: the last run's `FinishedAt` as `FinishedBefore` and its `Ref.RunID` as `AfterRunID`. `FinishedBefore` alone is a strict "before", so of two runs with the same finish time on either side of a page boundary the second would be skipped.
- The `WithArchive` hook sees a chunk at least once: it runs before the delete transaction, so a delete that fails hands the same runs to the next prune. A run that a guard keeps (it was retried meanwhile) is copied but not deleted. Key what the hook writes by run ID.
- A hook that keeps failing stops every prune, and the hot tables grow again; log its error and alert on it.
- Retention in a cluster where no client registers an `OnRunFinished` observer deletes nothing unless `Retention.Undelivered` is true; watch `PruneResult.HeldUndelivered`.
- The recorder's store prunes only when you call `Prune`; schedule it yourself. Its `Archive` sees a run at least once on `sqlrecorder` (a cancelled prune no longer repeats a batch; a crash or a failed delete right after `Archive` does) and exactly once on `MemoryStore`; key what it writes by run ID.
- `StartWhen` blocks, and its `ctx` becomes the client's lifetime once `Start` runs: a `context.WithTimeout` given to it stops the workers at the deadline as well. Put a deadline inside the `ReadyFunc` instead.
- A `ReadyFunc` that returns an error ends the wait for good. A dependency that is still booting should return `false, nil`.
- A readiness probe does not stop a client from scheduling: a node with a wrong clock but a healthy database reports ready and fires wrong ticks. That is what `StartWhen` is for.
- The admin API's `GET /health` is not a probe: it needs the admin login and always answers 200.

## Related

- [Retention](../operations/retention.md), [The SQL driver › Keep the hot tables small](../drivers/sqlstore.md#keep-the-hot-tables-small), [Record run history](../observability/run-recorder.md)
- [Start when ready](../operations/start-gate.md), [Nodes and health](../operations/nodes.md), [Laptops that sleep](../operations/sleep-and-wake.md)
- [Choose a schedule tool](schedules.md), [Choose by kind of app](app-types.md), [Going to production](../start/production.md#checklist)
