# Retention

The SQL driver keeps every finished run forever unless you say otherwise: deleting history is a decision only the app can make. `WithRetention` turns deletion on with an age per final status and a count per workflow; `Prune` deletes what the rules select, in chunks, each passed first to an archive hook if you set one. A few guards keep runs that something still needs.

## Before you start

- A [SQL driver](../drivers/sqlstore.md) client. Retention is a `sqlstore` option; the memory driver holds nothing past the process and Hatchet keeps runs on its engine.
- Decide whether any client of the cluster calls `OnRunFinished`. It changes one setting, `Undelivered`, below.

## Keep finished runs for a while

`sqlstore.Retention` has a keep-for duration per final status (zero keeps forever) and a `KeepLast` count per workflow (only the newest N finished runs stay, by finish time). Either rule deletes: a run goes when either says so. `PerWorkflow` replaces the durations and `KeepLast` for the named workflows, whole: a workflow listed there uses only its own entry.

<!-- snippet: examples/retention/main.go#retention -->
```go
c, err := sqlstore.New(db, sqlb.SQLiteDialect{},
	sqlstore.WithRetention(sqlstore.Retention{
		Succeeded: 24 * time.Hour,     // keep successes a day
		Failed:    7 * 24 * time.Hour, // failures a week; Cancelled: zero, forever
		PerWorkflow: map[string]sqlstore.Retention{
			"report": {KeepLast: 2}, // replaces the rules above for "report", whole
		},
		Undelivered: true, // no client of this cluster calls OnRunFinished
	}),
	sqlstore.WithArchive(func(ctx context.Context, runs []sqlstore.FinishedRun) error {
		for _, r := range runs {
			coldStorage = append(coldStorage, r.Workflow+" "+string(r.Status)+" "+string(r.Payload))
		}
		return nil // an error here keeps the chunk and stops the prune
	}),
	sqlstore.WithPruneChunk(2),  // runs per delete transaction and per archive call (default 100)
	sqlstore.WithoutAutoPrune(), // the app calls Prune itself
	sqlstore.WithClock(clk),
)
```

`Retention.Cancelled` also governs discarded runs (`StatusDiscarded`), which are cancelled work by nature. A run's steps and consumed-signal markers are deleted with it. `Prune` never deletes a run that:

- **still holds an idempotency key**;
- **belongs to an open batch** in `Client.BatchStore()`;
- **has an end not yet delivered to an `OnRunFinished` observer**, unless `Retention.Undelivered` is set.

That last guard needs a choice. When any client of the cluster registers an observer, leave `Undelivered` false, or an end it has not seen yet is lost. When none does, set it to true: an end nobody observes is never marked delivered, so without it nothing would ever be deleted. `Prune` reports such runs in `PruneResult.HeldUndelivered`, and the background prune logs a warning naming both fixes.

A batch kept in another store cannot be seen by the batch guard, and its status poller reads each job's `Status`. With retention on, keep batches in `Client.BatchStore()`, or a job's run may be deleted before its batch finishes.

## Prune on your own schedule

A started client with `WithRetention` prunes in the background every `WithPruneInterval` (default an hour), when it wins the prune lock's lease, so one client of a cluster prunes per interval. `WithoutAutoPrune()` turns that off; the app calls `Client.Prune` from its own scheduler (a cron workflow, an admin endpoint):

<!-- snippet: examples/retention/main.go#prune -->
```go
res, err := c.Prune(ctx)
if err != nil {
	return err
}
fmt.Fprintf(out, "now: deleted %d, held undelivered %d, idempotency %d, batch %d\n",
	res.Deleted, res.HeldUndelivered, res.HeldIdempotency, res.HeldBatch)
```

`PruneResult.Deleted` is how many runs went (with their steps). The `Held` counts are runs old enough to delete that a guard kept, so a setup that can never delete anything shows up as a number rather than as silence; a run that both an age rule and `KeepLast` select is counted once per rule in `HeldIdempotency`. Without `WithRetention`, `Prune` deletes nothing.

`WithPruneChunk(n)` sets how many runs one delete transaction removes and the archive hook receives at once (default 100). Smaller chunks hold row locks for less time; larger ones clear a backlog in fewer round trips.

## Archive before deleting

`WithArchive(fn)` passes each chunk to an `ArchiveFunc` before it is deleted. Returning an error keeps the chunk and stops the prune, so history copied to cold storage is never lost to a failed copy. Each `FinishedRun` carries `Ref`, `Workflow`, `Status`, `Labels`, `Payload`, `Err` (the stored error text, empty when it succeeded), `CreatedAt`, `FinishedAt`, and `Outputs`, each step's stored output by step name (a step that never succeeded is absent). The example above appends one line per run to a slice standing in for an object store or an archive table.

## Try it

```sh
cd examples
go run ./retention
```

```text
now: deleted 3, held undelivered 0, idempotency 0, batch 0
a day later: deleted 2
a week later: deleted 1
archived: email failed "fail", email succeeded "a", email succeeded "b", report succeeded "r1", report succeeded "r2", report succeeded "r3"
no observer, Undelivered false: deleted 0, held undelivered 1
Undelivered true: deleted 1
```

Reading it: right away, `KeepLast: 2` deletes the three oldest of five reports. A day later the two successful emails pass their 24 hours. A week later the failed email passes its seven days. The last two lines are a second database with no observer: with `Undelivered` false the run is held and reported, with it true the run goes.

## Pitfalls

- Turning retention on in a cluster where no client registers an observer, without `Undelivered: true`, deletes nothing; watch `HeldUndelivered`.
- Setting `Undelivered: true` while an observer is registered can delete a run before its end is delivered.
- `PerWorkflow` replaces the whole rule set for that workflow: an entry with only `KeepLast` keeps that workflow's runs forever by age.
- An archive hook that keeps failing stops every prune; it is the safe default, but log its error.
- Retention covers finished runs only. Old cron tick rows, consumed signals, expired unique locks and finished debounce bursts are cleaned by the dispatcher whatever the retention: [Tuning › Housekeeping](tuning.md#housekeeping).

## Related

- [The SQL driver](../drivers/sqlstore.md)
- [Run-finished hook](../observability/run-finished.md)
- [Batch durability](../batches/durability.md)
- Example: [retention](../../examples/retention/)
