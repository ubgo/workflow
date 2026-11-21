// Command recorder-sql keeps the run history in SQL with the recorder's
// own store, sqlrecorder, on SQLite in memory: the dialect read from
// configuration and checked against sqlrecorder.Dialects, a table prefix of
// the app's own, the contention retries (WithTxAttempts, WithRetryable on
// top of DefaultRetryable), and two-stage retention with an archive hook:
// step rows go after a week, run rows after a month once the archive took
// them, a billing workflow keeps its runs for a year, and an archive that
// is down stops the prune with nothing deleted. Time moves on a fake clock,
// so a month passes in a millisecond and the output never varies.
//
// It is the program behind the SQL store and retention sections of
// docs/observability/run-recorder.md: the guide copies its regions (marked
// "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/recorder"
	"github.com/ubgo/workflow/recorder/sqlrecorder"
)

// The history's configuration, as an app would read it from its settings.
const (
	// historyDialect names the database engine; in an app it comes from
	// the same setting that picked the database driver.
	historyDialect = "sqlite"

	// tablePrefix keeps the history's tables apart from the app's own
	// (hist_runs, hist_steps, hist_events, hist_labels, hist_schema).
	tablePrefix = "hist_"

	// txAttempts is how often a write transaction runs on contention
	// before the error reaches the recorder's error hook.
	txAttempts = 4

	// proxyBusy is the error text of a connection proxy in front of the
	// database that DefaultRetryable does not know: a pool momentarily
	// full is contention too, worth retrying.
	proxyBusy = "proxy: pool exhausted"
)

// Retention ages. A runs page lists a month of runs; their step detail is
// kept for a week; billing runs stay a year for the auditors.
const (
	// stepsAge is when a run's step rows go: a week of step detail.
	stepsAge = 7 * 24 * time.Hour
	// runsAge is when a run's row goes, after the archive took it: the month
	// a runs page lists.
	runsAge = 30 * 24 * time.Hour
	// billingRunsAge keeps billing runs a year for the auditors.
	billingRunsAge = 365 * 24 * time.Hour
)

// Timings of the demo itself, not of the store.
const (
	// pruneInterval is how often PruneEvery prunes. Its first prune runs
	// at once, which is the one the demo waits for.
	pruneInterval = time.Hour
	// finishWait bounds the wait for the recorder to record a run's end.
	finishWait = 5 * time.Second
	// finishPoll is how often that wait reads the store.
	finishPoll = 5 * time.Millisecond
)

// workflows are the two workflows the demo runs: report follows the default
// retention, billing its own PerWorkflow rule.
var workflows = []string{"report", "billing"}

// start is the fake clock's first instant; any fixed time works.
var start = time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "recorder-sql:", err)
		os.Exit(1)
	}
}

// snippet:dialect
// dialectFor turns a configuration value into a sqlrecorder.Dialect,
// refusing an engine the store does not support before anything is opened.
func dialectFor(name string) (sqlrecorder.Dialect, error) {
	for _, d := range sqlrecorder.Dialects() {
		if string(d) == name {
			return d, nil
		}
	}
	return "", fmt.Errorf("history database %q: want one of %v", name, sqlrecorder.Dialects())
}

// snippet:end

// snippet:retryable
// retryable is DefaultRetryable plus the proxy's own contention error, for
// sqlrecorder.WithRetryable.
func retryable(err error) bool {
	return sqlrecorder.DefaultRetryable(err) || (err != nil && strings.Contains(err.Error(), proxyBusy))
}

// snippet:end

// coldStorage stands in for the archive the retention hands runs to before
// deleting them (an object store, a data warehouse). While down it refuses
// every batch, as a real archive does during an outage.
type coldStorage struct {
	mu   sync.Mutex
	down bool
	// archived receives the workflow names of each batch it accepted.
	archived chan []string
}

// Put is the Retention.Archive hook.
func (c *coldStorage) Put(_ context.Context, runs []recorder.RunRecord) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.down {
		return errors.New("cold storage unreachable")
	}
	var names []string
	for _, r := range runs {
		names = append(names, fmt.Sprintf("%s (steps kept: %d)", r.Workflow, len(r.Steps)))
	}
	c.archived <- names
	return nil
}

// setDown takes the archive down or brings it back.
func (c *coldStorage) setDown(down bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.down = down
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, w io.Writer) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	clk := clock.NewFake(start)

	fmt.Fprintln(w, "dialects:", sqlrecorder.Dialects())
	_, bad := dialectFor("oracle")
	fmt.Fprintln(w, "refused:", bad)
	fmt.Fprintln(w, "retried: sqlite busy", retryable(errors.New("database is locked (5) (SQLITE_BUSY)")),
		"| proxy busy", retryable(errors.New(proxyBusy)), "| syntax error", retryable(errors.New(`near "SELEC": syntax error`)))

	db, err := driversqlite.Memory() // one connection: an in-memory database lives on it
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	cold := &coldStorage{archived: make(chan []string, 1)}

	// snippet:store
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
	// snippet:end
	tables, err := tableNames(ctx, db)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "migration statements: %d, tables: %s\n", len(store.MigrationScripts()), strings.Join(tables, " "))

	rec, err := recorder.New(store, recorder.WithClock(clk))
	if err != nil {
		return err
	}
	c, err := workflow.Use(memory.New(memory.WithClock(clk)), rec.Plugin())
	if err != nil {
		return err
	}
	for _, name := range workflows {
		if err := c.Register(workflow.Workflow{Name: name, Steps: []workflow.Step{{Name: "build", Run: func(context.Context, []byte) ([]byte, error) {
			return []byte(`"done"`), nil
		}}}}); err != nil {
			return err
		}
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	refs := map[string]workflow.RunRef{}
	for _, name := range workflows {
		ref, err := c.Enqueue(ctx, workflow.WithSource(workflow.Job{Workflow: name}, "api", "req-"+name))
		if err != nil {
			return err
		}
		if err := waitFinished(ctx, store, ref); err != nil {
			return err
		}
		refs[name] = ref
	}
	// show prints what the store holds of each run at the moment when.
	show := func(when string) error {
		var parts []string
		for _, name := range workflows {
			r, err := store.Run(ctx, refs[name])
			if errors.Is(err, workflow.ErrNotFound) {
				parts = append(parts, name+" gone")
				continue
			}
			if err != nil {
				return err
			}
			parts = append(parts, fmt.Sprintf("%s %s steps=%d", name, r.Status, len(r.Steps)))
		}
		fmt.Fprintf(w, "%s: %s\n", when, strings.Join(parts, ", "))
		return nil
	}
	if err := show("day 0"); err != nil {
		return err
	}

	// snippet:prune
	clk.Advance(stepsAge + time.Hour) // an hour past the step age
	res, err := store.Prune(ctx)      // or a scheduled job of your own
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "prune after a week: steps cleared=%d runs deleted=%d archived=%d\n", res.StepsCleared, res.RunsDeleted, res.Archived)
	// snippet:end
	if err := show("day 7"); err != nil {
		return err
	}

	clk.Advance(runsAge - stepsAge)
	cold.setDown(true)
	res, err = store.Prune(ctx)
	fmt.Fprintf(w, "prune after a month, archive down: deleted=%d error=%v\n", res.RunsDeleted, err)
	if err := show("day 31"); err != nil {
		return err
	}

	cold.setDown(false)
	pctx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	// snippet:prune-every
	go func() {
		// Prunes at once, then every pruneInterval, until pctx ends.
		done <- recorder.PruneEvery(pctx, store, pruneInterval, func(err error) {
			fmt.Fprintln(w, "prune failed:", err) // a log line in an app
		})
	}()
	// snippet:end
	fmt.Fprintln(w, "archived:", strings.Join(<-cold.archived, ", "))
	// The delete follows the archive's return; stop only after it, since
	// a cancelled prune leaves the batch for the next one to archive again.
	if err := waitGone(ctx, store, refs["report"]); err != nil {
		return err
	}
	stop()
	if err := <-done; err != nil {
		return err
	}
	return show("after PruneEvery")
}

// tableNames lists the history's tables, to show the prefix at work.
func tableNames(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE ?`, tablePrefix+"%")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		names = append(names, n)
	}
	slices.Sort(names)
	return names, rows.Err()
}

// waitGone waits until the store no longer holds the run.
func waitGone(ctx context.Context, store recorder.Store, ref workflow.RunRef) error {
	deadline := time.Now().Add(finishWait)
	for time.Now().Before(deadline) {
		_, err := store.Run(ctx, ref)
		if errors.Is(err, workflow.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		time.Sleep(finishPoll)
	}
	return fmt.Errorf("run %s: still stored after %s", ref.RunID, finishWait)
}

// waitFinished waits until the store holds the run's end: the recorder
// writes it from the driver's run-finished goroutine, after the step.
func waitFinished(ctx context.Context, store recorder.Store, ref workflow.RunRef) error {
	deadline := time.Now().Add(finishWait)
	for time.Now().Before(deadline) {
		r, err := store.Run(ctx, ref)
		if err == nil && r.Status.IsTerminal() {
			return nil
		}
		if err != nil && !errors.Is(err, workflow.ErrNotFound) {
			return err
		}
		time.Sleep(finishPoll)
	}
	return fmt.Errorf("run %s: no end recorded within %s", ref.RunID, finishWait)
}
