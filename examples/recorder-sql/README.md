# recorder-sql

The run history in SQL with the recorder's own store, `sqlrecorder`, on SQLite in memory. The program reads the dialect from configuration and checks it against `sqlrecorder.Dialects()`, gives the tables a prefix of its own, adds a proxy's contention error to `DefaultRetryable` with `WithRetryable`, and sets two-stage retention with an archive hook: step rows go after a week, run rows after a month once the archive accepted them, and a `billing` workflow keeps its runs a year. An archive that is down stops the prune with nothing deleted; `recorder.PruneEvery` prunes again once it is back. A second store in the same database, under the prefix `arch_`, takes `sqlrecorder.WithArchiveTable()`: its prune moves each expired run into `arch_archive` in the same transaction as the delete, exactly once, and the program reads the archive back a page at a time with `ArchivedRuns` and one run with `ArchivedRun`. Time moves on a fake clock, so a month passes at once and the output never varies.

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
archive store: migration statements: 19, tables: arch_archive arch_events arch_labels arch_runs arch_schema arch_steps
archive prune: runs deleted=3 moved=3
archived runs, pages of 2: 2+1 runs: exp-1 exp-2 exp-3
archived run exp-1: succeeded steps=1 events=1
still in the runs table: false
```

- `steps cleared=1` after a week: `report` lost its step rows and kept its run row; `billing` has a `PerWorkflow` rule with no step age, so it keeps its steps.
- After a month the archive was down: `Prune` returned its error and deleted nothing.
- `PruneEvery` pruned at once when started, handed `report` to the archive, then deleted it. `billing` stays until its year is up.
- The archive store has one table more and 19 migration statements: the 15 of the default schema, then the archive table and its three indexes.
- Its prune moved the three expired runs (`moved=3`: with no `Archive` callback, `PruneResult.Archived` counts the runs moved to the table). The three ended at the same instant, and reading the archive two at a time still returns each once; `ArchivedRun` returns a run with its step and event, and the runs table no longer holds it.

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

<!-- snippet: examples/recorder-sql/main.go#archive-store -->
```go
store, err := sqlrecorder.New(db, sqlrecorder.SQLite,
	sqlrecorder.WithTablePrefix(archivePrefix),
	sqlrecorder.WithClock(clk),
	// Retention decides what moves and when; the table is where it goes.
	sqlrecorder.WithRetention(recorder.Retention{RetentionRule: recorder.RetentionRule{Runs: runsAge}}),
	sqlrecorder.WithArchiveTable(), // Migrate also creates arch_archive
)
if err != nil {
	return err
}
if err := store.Migrate(ctx); err != nil {
	return err
}
```

<!-- snippet: examples/recorder-sql/main.go#archive-read -->
```go
res, err := store.Prune(ctx) // stage two moves each expired run, exactly once
if err != nil {
	return err
}
fmt.Fprintf(w, "archive prune: runs deleted=%d moved=%d\n", res.RunsDeleted, res.Archived)

// Read the archive a page at a time, oldest finish first.
var sizes, sources []string
q := sqlrecorder.ArchiveQuery{Workflow: exportWorkflow, Limit: archivePageSize}
for {
	page, err := store.ArchivedRuns(ctx, q)
	if err != nil {
		return err
	}
	sizes = append(sizes, fmt.Sprint(len(page.Runs)))
	for _, r := range page.Runs {
		sources = append(sources, r.SourceRef)
	}
	if page.Next == "" { // the last page
		break
	}
	q.Cursor = page.Next
}
slices.Sort(sources)
fmt.Fprintf(w, "archived runs, pages of %d: %s runs: %s\n", archivePageSize, strings.Join(sizes, "+"), strings.Join(sources, " "))

// One run, as Run returned it before the move.
r, err := store.ArchivedRun(ctx, first)
if err != nil {
	return err
}
events := 0
for _, st := range r.Steps {
	events += len(st.Events)
}
fmt.Fprintf(w, "archived run %s: %s steps=%d events=%d\n", r.SourceRef, r.Status, len(r.Steps), events)
_, err = store.Run(ctx, first) // the runs table no longer holds it
fmt.Fprintln(w, "still in the runs table:", !errors.Is(err, workflow.ErrNotFound))
```

`main_test.go` runs the program and compares its whole output with the block above, and checks that `dialectFor` accepts every dialect by name.

## Guides

- [Record run history › The SQL store](../../docs/observability/run-recorder.md#the-sql-store), [Retention](../../docs/observability/run-recorder.md#retention) and [Archive into a table, exactly once](../../docs/observability/run-recorder.md#archive-into-a-table-exactly-once)
