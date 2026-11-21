// Command insert-contract enqueues SQL driver runs the way a program in
// another language would: with plain SQL in its own transaction, no
// sqlstore client, following docs/drivers/sqlstore-insert-contract.md. A Go
// worker that registers the workflow then runs them: a run due now (with a
// label, a tenant and its queue left for the worker to fill in), a delayed
// run, and a keyed run whose key a second insert and a Go Enqueue both see
// held.
//
// It runs on a SQLite file in a temporary directory and needs nothing
// installed. The statements are the page's, with SQLite's "?"
// placeholders; Postgres takes $1, $2, ... in the same places.
//
// It is the program behind docs/drivers/sqlstore-insert-contract.md: the
// guide copies its regions (marked "snippet:") and task docs:snippets fails
// when they drift. sqlstore/enginetest/foreign_insert_test.go proves the
// same statements on SQLite, Postgres and MySQL.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ubgo/sqlb"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/sqlstore"
)

const (
	// pollInterval is how often the worker looks for runs it did not
	// enqueue itself; short so the demo is quick (the default is 1s).
	pollInterval = 20 * time.Millisecond
	// delay is how far ahead the delayed run starts.
	delay = 300 * time.Millisecond
	// holdFor keeps the keyed run waiting, so it holds its key for the
	// whole program.
	holdFor = time.Hour
	// runTimeout bounds the wait for one run, generous for a slow CI host.
	runTimeout = 10 * time.Second
	// statusPoll is how often waitStatus re-reads a run's status.
	statusPoll = 5 * time.Millisecond
)

// runStatus is the status column a foreign writer inserts.
type runStatus string

// Run statuses a foreign writer may insert, and nothing else.
const (
	statusPending   runStatus = "pending"   // starts now
	statusScheduled runStatus = "scheduled" // starts at run_at
)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "insert-contract:", err)
		os.Exit(1)
	}
}

// snippet:statements
// The contract's statements for the default table prefix "wf_". Every "?"
// is a parameter; the literals are part of the contract.
const (
	insertRun = `INSERT INTO wf_runs (id, scheduled_id, workflow, payload, labels, idem_active, priority, bucket, rate_keys,
  status, run_at, sig_floor, created_at, finished_at, fail_until, notified, notify_until, ver)
VALUES (?, ?, ?, ?, ?, ?, ?, '', '', ?, ?, (SELECT COALESCE(MAX(id), 0) FROM wf_signals), ?, 0, 0, 0, 0, 0)`
	insertRunExt = `INSERT INTO wf_run_ext (run_id, queue, idem_key, retry, started_at, discarded)
VALUES (?, ?, ?, NULL, 0, 0)`
	insertRunInfo = `INSERT INTO wf_run_info (run_id, tenant, output, has_output, progress, nodes, deadline, timeout)
VALUES (?, ?, NULL, 0, NULL, '[]', 0, '')`
	insertRunLabel = `INSERT INTO wf_run_labels (run_id, lkey, lval) VALUES (?, ?, ?)`
)

// snippet:end

// snippet:job
// foreignJob is one run as the other program describes it.
type foreignJob struct {
	ID          string // unique, 1 to 64 bytes: the RunRef.RunID Go code uses
	ScheduledID string // a second unique ID for a delayed run, else empty
	Workflow    string // exactly as registered in Go
	Payload     []byte // the first step's input, stored as given
	Labels      map[string]string
	Key         string    // idempotency key, empty for none
	Queue       string    // empty: the admitting worker fills in the workflow's queue
	Tenant      string    // empty for none
	Priority    int       // higher goes first
	RunAt       time.Time // zero: now
}

// snippet:end

// snippet:insert
// insert writes job's rows in one transaction. A held idempotency key
// fails the runs insert with a unique violation, and the rollback leaves
// nothing behind.
func insert(ctx context.Context, db *sql.DB, job foreignJob) (err error) {
	labels := []byte(`{}`) // a JSON object of string values, never NULL
	if len(job.Labels) > 0 {
		if labels, err = json.Marshal(job.Labels); err != nil {
			return err
		}
	}
	now := time.Now().UnixNano() // Unix nanoseconds: UTC by definition
	status, runAt := statusPending, now
	if !job.RunAt.IsZero() {
		status, runAt = statusScheduled, job.RunAt.UnixNano()
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, tx.Rollback())
		}
	}()
	if _, err := tx.ExecContext(ctx, insertRun, job.ID, nullable(job.ScheduledID), job.Workflow, job.Payload, string(labels),
		nullable(job.Key), job.Priority, status, runAt, now); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, insertRunExt, job.ID, job.Queue, nullable(job.Key)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, insertRunInfo, job.ID, job.Tenant); err != nil {
		return err
	}
	for k, v := range job.Labels { // only for label searches; the labels column is what runs read
		if _, err := tx.ExecContext(ctx, insertRunLabel, job.ID, k, v); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// nullable maps "" to SQL NULL, as the contract wants for absent values.
func nullable(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}

// snippet:end

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, w io.Writer) (err error) {
	dir, err := os.MkdirTemp("", "insert-contract-*")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(dir)) }()
	db, err := driversqlite.Open(filepath.Join(dir, "app.db"))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close()) }()

	var mu sync.Mutex
	seen := map[workflow.RunID]string{} // run ID -> the payload its step saw

	// snippet:worker
	worker, err := sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithPollInterval(pollInterval))
	if err != nil {
		return err
	}
	if err := worker.Migrate(ctx); err != nil { // or apply sqlstore.MigrationScripts with your own tool
		return err
	}
	report := workflow.Workflow{Name: "report", Queue: "reports", Steps: []workflow.Step{{Name: "build",
		Run: func(ctx context.Context, in []byte) ([]byte, error) {
			info, _ := workflow.StepInfoFrom(ctx)
			mu.Lock()
			seen[info.RunID] = string(in)
			mu.Unlock()
			return nil, nil
		}}}}
	if err := worker.Register(report); err != nil { // register before runs are inserted for it
		return err
	}
	// snippet:end
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := worker.Start(runCtx); err != nil {
		return err
	}
	defer worker.Stop()

	// snippet:write
	now := foreignJob{ID: "run_billing_0001", Workflow: "report", Payload: []byte(`{"month":"2026-11"}`),
		Labels: map[string]string{"source": "billing"}, Tenant: "acme"}
	later := foreignJob{ID: "run_billing_0002", ScheduledID: "sched_billing_0002", Workflow: "report",
		Payload: []byte(`{"month":"2026-12"}`), RunAt: time.Now().Add(delay)}
	for _, job := range []foreignJob{now, later} {
		if err := insert(ctx, db, job); err != nil {
			return err
		}
	}
	// snippet:end

	if err := waitStatus(ctx, worker, workflow.RunRef{RunID: workflow.RunID(now.ID)}, workflow.StatusSucceeded); err != nil {
		return err
	}
	mu.Lock()
	fmt.Fprintf(w, "%s succeeded; its step saw %s\n", now.ID, seen[workflow.RunID(now.ID)])
	mu.Unlock()
	page, err := worker.Runs(ctx, workflow.RunQuery{Labels: map[string]string{"source": "billing"}})
	if err != nil {
		return err
	}
	for _, r := range page.Runs {
		fmt.Fprintf(w, "label source=billing finds %s: queue %s (filled in at admission), tenant %s\n", r.Ref.RunID, r.Queue, r.Tenant)
	}
	if err := waitStatus(ctx, worker, workflow.RunRef{RunID: workflow.RunID(later.ID)}, workflow.StatusSucceeded); err != nil {
		return err
	}
	fmt.Fprintf(w, "%s (scheduled) succeeded after its run_at\n", later.ID)

	// snippet:key
	keyed := foreignJob{ID: "run_billing_0003", ScheduledID: "sched_billing_0003", Workflow: "report",
		Key: "report-2027-01", RunAt: time.Now().Add(holdFor)}
	if err := insert(ctx, db, keyed); err != nil {
		return err
	}
	dup := keyed
	dup.ID, dup.ScheduledID = "run_billing_0004", "sched_billing_0004"
	err = insert(ctx, db, dup)
	// A unique violation means "already enqueued"; the transaction wrote nothing.
	fmt.Fprintf(w, "second insert of key %s: unique violation %v\n", keyed.Key, sqlstore.DefaultErrorClassifier(err).UniqueViolation)
	held, err := worker.Enqueue(ctx, workflow.Job{Workflow: "report", IdempotencyKey: keyed.Key})
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "Go Enqueue with the same key returns the inserted run: %v\n", held.ScheduledID == workflow.ScheduledID(keyed.ScheduledID))
	// snippet:end
	return nil
}

// waitStatus waits until the run behind ref reaches want.
func waitStatus(ctx context.Context, c *sqlstore.Client, ref workflow.RunRef, want workflow.Status) error {
	deadline := time.Now().Add(runTimeout)
	for {
		st, err := c.Status(ctx, ref)
		if err != nil {
			return err
		}
		if st == want {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("run %s is %s after %s, want %s", ref.RunID, st, runTimeout, want)
		}
		time.Sleep(statusPoll)
	}
}
