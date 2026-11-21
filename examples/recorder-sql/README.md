# recorder-sql

The run history in SQL with the recorder's own store, `sqlrecorder`, on SQLite in memory. The program reads the dialect from configuration and checks it against `sqlrecorder.Dialects()`, gives the tables a prefix of its own, adds a proxy's contention error to `DefaultRetryable` with `WithRetryable`, and sets two-stage retention with an archive hook: step rows go after a week, run rows after a month once the archive accepted them, and a `billing` workflow keeps its runs a year. An archive that is down stops the prune with nothing deleted; `recorder.PruneEvery` prunes again once it is back. Time moves on a fake clock, so a month passes at once and the output never varies.

```sh
cd examples
go run ./recorder-sql
```

```text
dialects: [postgres mysql sqlite]
refused: history database "oracle": want one of [postgres mysql sqlite]
retried: sqlite busy true | proxy busy true | syntax error false
migration statements: 15, tables: hist_events hist_labels hist_runs hist_schema hist_steps
day 0: report succeeded steps=1, billing succeeded steps=1
prune after a week: steps cleared=1 runs deleted=0 archived=0
day 7: report succeeded steps=0, billing succeeded steps=1
prune after a month, archive down: deleted=0 error=sqlrecorder: archive: cold storage unreachable
day 31: report succeeded steps=0, billing succeeded steps=1
archived: report (steps kept: 0)
after PruneEvery: report gone, billing succeeded steps=1
```

- `steps cleared=1` after a week: `report` lost its step rows and kept its run row; `billing` has a `PerWorkflow` rule with no step age, so it keeps its steps.
- After a month the archive was down: `Prune` returned its error and deleted nothing.
- `PruneEvery` pruned at once when started, handed `report` to the archive, then deleted it. `billing` stays until its year is up.

## The code

<!-- snippet: examples/recorder-sql/main.go#store -->
```go
dialect, err := dialectFor(historyDialect)
if err != nil {
	return err
}
store, err := sqlrecorder.New(db, dialect,
	sqlrecorder.WithTablePrefix(tablePrefix),
	sqlrecorder.WithTxAttempts(txAttempts),
	sqlrecorder.WithRetryable(retryable),
	sqlrecorder.WithClock(clk), // retention ages are measured on this clock
	sqlrecorder.WithRetention(recorder.Retention{
		RetentionRule: recorder.RetentionRule{Steps: stepsAge, Runs: runsAge},
		// A PerWorkflow rule replaces both ages: billing keeps its
		// steps (zero) and its runs for a year.
		PerWorkflow: map[string]recorder.RetentionRule{"billing": {Runs: billingRunsAge}},
		// Every run is handed here before stage two deletes it.
		Archive: cold.Put,
	}),
)
if err != nil {
	return err
}
if err := store.Migrate(ctx); err != nil { // idempotent, on every start
	return err
}
```

<!-- snippet: examples/recorder-sql/main.go#prune-every -->
```go
go func() {
	// Prunes at once, then every pruneInterval, until pctx ends.
	done <- recorder.PruneEvery(pctx, store, pruneInterval, func(err error) {
		fmt.Fprintln(w, "prune failed:", err) // a log line in an app
	})
}()
```

`main_test.go` runs the program and compares its whole output with the block above, and checks that `dialectFor` accepts every dialect by name.

## Guides

- [Record run history › The SQL store](../../docs/observability/run-recorder.md#the-sql-store) and [Retention](../../docs/observability/run-recorder.md#retention)
