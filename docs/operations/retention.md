# Retention

The SQL driver keeps every finished run forever unless you say otherwise: deleting history is a decision only the app can make. [`WithRetention`](ds:block?id=sql-withretention-dk86xzbt) turns deletion on with an age per final status and a count per workflow; [`Prune`](ds:block?id=sql-client-prune-ybwksw5e) deletes what the rules select, in chunks, each passed first to an archive hook if you set one. A few guards keep runs that something still needs.

## Before you start

- A [SQL driver](../drivers/sqlstore.md) client. Retention is a `sqlstore` option ([`sqlstore.Retention`](ds:block?id=sql-retention-eh72esa7)); the memory driver holds nothing past the process and Hatchet keeps runs on its engine.
- Decide whether any client of the cluster calls `OnRunFinished`. It changes one setting, `Undelivered`, below ([`guards`](ds:block?id=sql-client-guards-raaht9rz)).

## Keep finished runs for a while

[`sqlstore.Retention`](ds:block?id=sql-retention-eh72esa7) has a keep-for duration per final status (zero keeps forever) and a `KeepLast` count per workflow (only the newest N finished runs stay, by finish time). Either rule deletes: a run goes when either says so. `PerWorkflow` replaces the durations and `KeepLast` for the named workflows, whole: a workflow listed there uses only its own entry ([`pruneRules`](ds:block?id=sql-client-prunerules-dtx6z9su)).

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
	sqlstore.WithArchive(archive), // each chunk goes through archive before it is deleted
	sqlstore.WithPruneChunk(2),    // runs per delete transaction and per archive call (default 100)
	sqlstore.WithoutAutoPrune(),   // the app calls Prune itself
	sqlstore.WithClock(clk),
)
```

`Retention.Cancelled` also governs discarded runs (`StatusDiscarded`), which are cancelled work by nature ([`pruneRules`](ds:block?id=sql-client-prunerules-dtx6z9su)). A run's steps and consumed-signal markers are deleted with it ([`deleteChunk`](ds:block?id=sql-client-deletechunk-hw8hk3br)). [`Prune`](ds:block?id=sql-client-prune-ybwksw5e) never deletes a run that:

- **still holds an idempotency key** ([`guards`](ds:block?id=sql-client-guards-raaht9rz));
- **belongs to an open batch** in `Client.BatchStore()` ([`batchOpen`](ds:block?id=sql-client-batchopen-tspv3wev));
- **has an end not yet delivered to an `OnRunFinished` observer**, unless `Retention.Undelivered` is set ([`guards`](ds:block?id=sql-client-guards-raaht9rz)).

That last guard needs a choice. When any client of the cluster registers an observer, leave `Undelivered` false, or an end it has not seen yet is lost. When none does, set it to true: an end nobody observes is never marked delivered, so without it nothing would ever be deleted. `Prune` reports such runs in `PruneResult.HeldUndelivered` ([`countHeld`](ds:block?id=sql-client-countheld-fh99t2nc)), and the background prune logs a warning naming both fixes ([`pruneLoop`](ds:block?id=sql-client-pruneloop-ehzb5kct)).

A batch kept in another store cannot be seen by the batch guard ([`batchOpen`](ds:block?id=sql-client-batchopen-tspv3wev)), and its status poller reads each job's `Status`. With retention on, keep batches in `Client.BatchStore()`, or a job's run may be deleted before its batch finishes.

## Prune on your own schedule

A started client with `WithRetention` prunes in the background every [`WithPruneInterval`](ds:block?id=sql-withpruneinterval-t4z7sbmu) (default [`an hour`](ds:block?id=sql-defaultpruneinterval-vheuphq3)), when it wins the prune lock's lease, so one client of a cluster prunes per interval ([`pruneLoop`](ds:block?id=sql-client-pruneloop-ehzb5kct)). [`WithoutAutoPrune()`](ds:block?id=sql-withoutautoprune-84z54ert) turns that off; the app calls `Client.Prune` from its own scheduler (a cron workflow, an admin endpoint):

<!-- snippet: examples/retention/main.go#prune -->
```go
res, err := c.Prune(ctx)
if err != nil {
	return err
}
fmt.Printf("now: deleted %d, held undelivered %d, idempotency %d, batch %d\n",
	res.Deleted, res.HeldUndelivered, res.HeldIdempotency, res.HeldBatch)
```

[`PruneResult.Deleted`](ds:block?id=sql-pruneresult-9hg2h6bm) is how many runs went (with their steps). The `Held` counts are runs old enough to delete that a guard kept, so a setup that can never delete anything shows up as a number rather than as silence; a run that both an age rule and `KeepLast` select is counted once per rule in `HeldIdempotency` ([`countHeld`](ds:block?id=sql-client-countheld-fh99t2nc)). Without `WithRetention`, [`Prune`](ds:block?id=sql-client-prune-ybwksw5e) deletes nothing.

[`WithPruneChunk(n)`](ds:block?id=sql-withprunechunk-sp8smwfb) sets how many runs one delete transaction removes and the archive hook receives at once (default [`100`](ds:block?id=sql-defaultprunechunk-7c3jdqyv); values below 1 are ignored). Smaller chunks hold row locks for less time; larger ones clear a backlog in fewer round trips.

## Archive before deleting

[`WithArchive(fn)`](ds:block?id=sql-witharchive-wk4xpfe4) passes each chunk to a [`sqlstore.ArchiveFunc`](ds:block?id=sql-archivefunc-xpbwfzf2) before it is deleted, in the same chunks `WithPruneChunk` sets. Each [`sqlstore.FinishedRun`](ds:block?id=sql-finishedrun-exkhvx6z) carries `Ref`, `Workflow`, `Status`, `Labels`, `Payload`, `Err` (the stored error text, empty when it succeeded), `CreatedAt`, `FinishedAt`, and `Outputs`, each step's stored output by step name (a step that never succeeded is absent). The example's hook writes one line per run to a slice standing in for an object store or an archive table:

<!-- snippet: examples/retention/main.go#archive -->
```go
// archive copies each chunk to cold storage before Prune deletes it.
var archive sqlstore.ArchiveFunc = func(ctx context.Context, runs []sqlstore.FinishedRun) error {
	for _, r := range runs {
		line := r.Workflow + " " + string(r.Status) + " " + string(r.Payload)
		if r.Err != "" { // the stored error text; empty when it succeeded
			line += " err=" + r.Err
		}
		for step, out := range r.Outputs { // each succeeded step's output
			line += " " + step + "=" + string(out)
		}
		coldStorage = append(coldStorage, line)
	}
	return nil // an error here keeps the chunk and stops the prune
}
```

The client above takes it with `sqlstore.WithArchive(archive)`. Returning an error keeps the chunk and stops the prune, so history copied to cold storage is never lost to a failed copy:

<!-- snippet: examples/retention/main.go#archive-fails -->
```go
res, err := c.Prune(ctx)
fmt.Printf("archive failed: deleted %d, error is the hook's: %t\n", res.Deleted, errors.Is(err, errBucket))
```

`Prune` wraps the hook's error (`sqlstore: archive hook refused a chunk of N runs (kept): ...`, in [`deleteChunk`](ds:block?id=sql-client-deletechunk-hw8hk3br)), so `errors.Is` finds it. Chunks archived and deleted before the failing one stay deleted; the failing chunk and everything after it wait for the next prune.

To keep finished runs in the same database instead, in archive tables you can still query, see [The SQL driver › Keep the hot tables small](../drivers/sqlstore.md#keep-the-hot-tables-small). The [run recorder](../observability/run-recorder.md) keeps a history of its own that outlives the driver's tables.

## Try it

```sh
cd examples
go run ./retention
```

```text
now: deleted 3, held undelivered 0, idempotency 0, batch 0
a day later: deleted 2
a week later: deleted 1
archived: email failed "fail" err=sqlstore: step "send" failed after 1 attempt(s): mailbox does not exist, email succeeded "a" send="sent", email succeeded "b" send="sent", report succeeded "r1" build="r1", report succeeded "r2" build="r2", report succeeded "r3" build="r3"
no observer, Undelivered false: deleted 0, held undelivered 1
Undelivered true: deleted 1
archive failed: deleted 0, error is the hook's: true
the run is kept: succeeded
```

Reading it: right away, `KeepLast: 2` deletes the three oldest of five reports. A day later the two successful emails pass their 24 hours. A week later the failed email passes its seven days. The `archived` line is what the hook copied: the failed email with its error text, the others with their step output. The next two lines are a second database with no observer: with `Undelivered` false the run is held and reported, with it true the run goes. The last two are a third database whose archive hook fails: nothing is deleted and the run is still there.

## Pitfalls

- Turning retention on in a cluster where no client registers an observer, without `Undelivered: true`, deletes nothing; watch `HeldUndelivered` ([`guards`](ds:block?id=sql-client-guards-raaht9rz)).
- Setting `Undelivered: true` while an observer is registered can delete a run before its end is delivered ([`Retention.Undelivered`](ds:block?id=sql-retention-eh72esa7)).
- `PerWorkflow` replaces the whole rule set for that workflow ([`pruneRules`](ds:block?id=sql-client-prunerules-dtx6z9su)): an entry with only `KeepLast` keeps that workflow's runs forever by age.
- An archive hook that keeps failing stops every prune ([`ArchiveFunc`](ds:block?id=sql-archivefunc-xpbwfzf2)); it is the safe default, but log its error.
- Retention covers finished runs only. Old cron tick rows, consumed signals, expired unique locks and finished debounce bursts are cleaned by the dispatcher whatever the retention: [Tuning › Housekeeping](tuning.md#housekeeping).

## Related

- [The SQL driver](../drivers/sqlstore.md)
- [Run-finished hook](../observability/run-finished.md)
- [Batch durability](../batches/durability.md)
- Example: [retention](../../examples/retention/)
