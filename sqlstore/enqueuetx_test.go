package sqlstore_test

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/sqlstore"
)

// txSetup starts one client with a one-step workflow that counts its
// executions, plus an app table (<prefix>orders) the tests write beside the
// run, dropped after the test.
func txSetup(t *testing.T, e engine, policy workflow.IdempotencyPolicy) (*sqlstore.Client, *sql.DB, string, *atomic.Int64) {
	t.Helper()
	db, prefix := e.open(t)
	orders := e.dialect.QuoteIdent(prefix + "orders")
	if _, err := db.Exec("CREATE TABLE " + orders + " (id VARCHAR(64) NOT NULL PRIMARY KEY)"); err != nil {
		t.Fatalf("create app table: %v", err)
	}
	t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS " + orders) })
	c := newClient(t, e, db, prefix)
	var runs atomic.Int64
	if err := c.Register(workflow.Workflow{
		Name:        "ship",
		Idempotency: policy,
		Steps: []workflow.Step{{Name: "a", Run: func(context.Context, []byte) ([]byte, error) {
			runs.Add(1)
			return nil, nil
		}}},
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return c, db, orders, &runs
}

func TestEnqueueTxCommitRuns(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()
			c, db, orders, runs := txSetup(t, e, workflow.IdempotencyOptional)
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = tx.Rollback() }) // a no-op after Commit; frees locks when a check fails first
			if _, err := tx.Exec("INSERT INTO " + orders + " (id) VALUES ('o1')"); err != nil {
				t.Fatal(err)
			}
			ref, err := c.EnqueueTx(ctx, tx, workflow.Job{Workflow: "ship"})
			if err != nil {
				t.Fatalf("EnqueueTx: %v", err)
			}
			if ref.RunID.IsZero() {
				t.Fatalf("EnqueueTx returned no RunID: %+v", ref)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			c.Wake()
			waitFor(t, 5*time.Second, "the committed run to succeed", func() bool {
				st, err := c.Status(ctx, ref)
				return err == nil && st == workflow.StatusSucceeded
			})
			if n := runs.Load(); n != 1 {
				t.Fatalf("step ran %d times, want 1", n)
			}
		})
	}
}

func TestEnqueueTxRollbackLeavesNoRun(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()
			c, db, orders, runs := txSetup(t, e, workflow.IdempotencyOptional)
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = tx.Rollback() }) // a no-op after Commit; frees locks when a check fails first
			if _, err := tx.Exec("INSERT INTO " + orders + " (id) VALUES ('o1')"); err != nil {
				t.Fatal(err)
			}
			ref, err := c.EnqueueTx(ctx, tx, workflow.Job{Workflow: "ship", IdempotencyKey: "o1"})
			if err != nil {
				t.Fatalf("EnqueueTx: %v", err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			c.Wake()
			// Several polls: a run that leaked out of the rollback would be
			// admitted and executed in this time.
			time.Sleep(20 * testPoll)
			if _, err := c.Status(ctx, ref); !errors.Is(err, workflow.ErrNotFound) {
				t.Fatalf("Status after rollback = %v, want ErrNotFound", err)
			}
			if n := runs.Load(); n != 0 {
				t.Fatalf("step ran %d times after a rollback, want 0", n)
			}
			// The key was never held, so a later enqueue with it starts a run.
			ref2, err := c.Enqueue(ctx, workflow.Job{Workflow: "ship", IdempotencyKey: "o1"})
			if err != nil {
				t.Fatalf("Enqueue after rollback: %v", err)
			}
			if ref2 == ref {
				t.Fatalf("the rolled-back run's ref came back: %+v", ref2)
			}
		})
	}
}

// TestEnqueueTxIdempotency proves a key held earlier in the same tx, or by a
// committed run, returns the holder, and that the duplicate does not abort
// the caller's transaction (on Postgres a unique violation would, without
// the savepoint): the app's later write still commits.
func TestEnqueueTxIdempotency(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()
			c, db, orders, runs := txSetup(t, e, workflow.IdempotencyOptional)
			job := workflow.Job{Workflow: "ship", IdempotencyKey: "order-1"}

			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = tx.Rollback() }) // a no-op after Commit; frees locks when a check fails first
			first, err := c.EnqueueTx(ctx, tx, job)
			if err != nil {
				t.Fatalf("EnqueueTx: %v", err)
			}
			again, err := c.EnqueueTx(ctx, tx, job)
			if err != nil {
				t.Fatalf("EnqueueTx with the key held in tx: %v", err)
			}
			if again != first {
				t.Fatalf("same key in one tx: %+v, want %+v", again, first)
			}
			if _, err := tx.Exec("INSERT INTO " + orders + " (id) VALUES ('after-duplicate')"); err != nil {
				t.Fatalf("the duplicate aborted the caller's tx: %v", err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("commit after a duplicate: %v", err)
			}
			var n int
			if err := db.QueryRow("SELECT COUNT(*) FROM " + orders).Scan(&n); err != nil || n != 1 {
				t.Fatalf("app rows after commit = %d (%v), want 1", n, err)
			}

			// A committed holder: a second transaction gets the same run. The
			// job starts in an hour, so the run cannot finish (and release
			// the key) while the test runs.
			tx2, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = tx2.Rollback() }) // a no-op after Commit; frees locks when a check fails first
			later := workflow.Job{Workflow: "ship", IdempotencyKey: "order-2", RunAt: time.Now().Add(time.Hour)}
			held, err := c.Enqueue(ctx, later)
			if err != nil {
				t.Fatal(err)
			}
			second, err := c.EnqueueTx(ctx, tx2, later)
			if err != nil {
				t.Fatalf("EnqueueTx with a committed holder: %v", err)
			}
			if err := tx2.Commit(); err != nil {
				t.Fatal(err)
			}
			if second != held || second.ScheduledID.IsZero() {
				t.Fatalf("key held by a committed run: %+v, want %+v", second, held)
			}
			c.Wake()
			waitFor(t, 5*time.Second, "the in-tx keyed run to succeed", func() bool {
				st, err := c.Status(ctx, first)
				return err == nil && st == workflow.StatusSucceeded
			})
			if n := runs.Load(); n != 1 {
				t.Fatalf("one keyed run executed %d times", n)
			}
		})
	}
}

func TestEnqueueTxRefusals(t *testing.T) {
	e := engines(t)[0]
	ctx := context.Background()
	c, db, _, _ := txSetup(t, e, workflow.IdempotencyRequired)
	if _, err := c.EnqueueTx(ctx, nil, workflow.Job{Workflow: "ship"}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("nil tx: %v, want ErrInvalid", err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() }) // a no-op after Commit; frees locks when a check fails first
	if _, err := c.EnqueueTx(ctx, tx, workflow.Job{Workflow: "ship"}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("missing required key: %v, want ErrInvalid", err)
	}
	if _, err := c.EnqueueTx(ctx, tx, workflow.Job{Workflow: "nope"}); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("unregistered workflow: %v, want ErrNotFound", err)
	}
}
