package enginetest

import (
	"context"
	"testing"
	"time"

	"github.com/ubgo/workflow"
)

// enqueueResult is what a concurrent EnqueueTx returned.
type enqueueResult struct {
	ref workflow.RunRef
	err error
}

// blockedFor is how long the second transaction's EnqueueTx must stay
// blocked behind the first's uncommitted key before the first ends. Long
// enough that an insert that did not wait would have returned, short
// enough to keep the suite quick.
const blockedFor = 300 * time.Millisecond

// TestEnqueueTxConcurrentKey proves the uniqueness of an idempotency key
// across two transactions that are both open (TX-06): the second
// transaction's EnqueueTx with the key the first wrote but has not
// committed blocks on the key, and once the first ends:
//
//   - first commits: the second returns the first's run, so exactly one run
//     exists, whether the second then commits or rolls back;
//   - first rolls back: the second's insert goes through and its own run is
//     the one that exists.
//
// Either way the one run runs once. On Postgres and MySQL the wait is the
// unique index's row lock; on SQLite it is the single writer lock (the
// insert is the second transaction's first statement, so it waits out
// busy_timeout instead of failing). The "after a read" cases give the second
// transaction a snapshot before the first commits: on MySQL (REPEATABLE
// READ) the holder is still found because it is read with FOR SHARE; on
// Postgres READ COMMITTED every statement sees the latest commit. They skip
// on SQLite, where a transaction that has read and then writes after another
// writer committed fails with SQLITE_BUSY_SNAPSHOT by design.
func TestEnqueueTxConcurrentKey(t *testing.T) {
	cases := []struct {
		name        string
		firstCommit bool // the first transaction commits; otherwise it rolls back
		lastCommit  bool // the second transaction commits; otherwise it rolls back
		readFirst   bool // the second transaction reads the app table before its EnqueueTx
	}{
		{name: "commit then commit", firstCommit: true, lastCommit: true},
		{name: "commit then rollback", firstCommit: true, lastCommit: false},
		{name: "rollback then commit", firstCommit: false, lastCommit: true},
		{name: "commit then commit after a read", firstCommit: true, lastCommit: true, readFirst: true},
		{name: "rollback then commit after a read", firstCommit: false, lastCommit: true, readFirst: true},
	}
	for _, e := range engines(t) {
		for _, tc := range cases {
			t.Run(e.name+"/"+tc.name, func(t *testing.T) {
				if tc.readFirst && e.name == "sqlite" {
					t.Skip("SQLite: a transaction that read before another writer committed cannot write (SQLITE_BUSY_SNAPSHOT); EnqueueTx's insert must be its first statement")
				}
				ctx := context.Background()
				c, db, orders, runs := txSetup(t, e, workflow.IdempotencyOptional)
				job := workflow.Job{Workflow: "ship", IdempotencyKey: "order-7"}

				tx1, err := db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = tx1.Rollback() }) // a no-op after Commit; frees locks when a check fails first
				first, err := c.EnqueueTx(ctx, tx1, job)
				if err != nil {
					t.Fatalf("first EnqueueTx: %v", err)
				}

				tx2, err := db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = tx2.Rollback() })
				if tc.readFirst {
					var n int
					if err := tx2.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+orders).Scan(&n); err != nil {
						t.Fatalf("second tx read: %v", err)
					}
				}
				done := make(chan enqueueResult, 1)
				go func() {
					ref, err := c.EnqueueTx(ctx, tx2, job)
					done <- enqueueResult{ref, err}
				}()
				select {
				case r := <-done:
					t.Fatalf("second EnqueueTx returned %+v, %v while the first transaction held the key uncommitted; want it to wait", r.ref, r.err)
				case <-time.After(blockedFor):
				}

				if tc.firstCommit {
					err = tx1.Commit()
				} else {
					err = tx1.Rollback()
				}
				if err != nil {
					t.Fatalf("end the first transaction: %v", err)
				}
				var second enqueueResult
				select {
				case second = <-done:
				case <-time.After(10 * time.Second):
					t.Fatal("second EnqueueTx still blocked 10s after the first transaction ended")
				}
				if second.err != nil {
					t.Fatalf("second EnqueueTx: %v", second.err)
				}
				switch {
				case tc.firstCommit && second.ref != first:
					t.Fatalf("after the holder committed, the second EnqueueTx returned %+v, want the holder %+v", second.ref, first)
				case !tc.firstCommit && second.ref == first:
					t.Fatalf("after the holder rolled back, the second EnqueueTx returned its rolled-back run %+v", first)
				}
				if tc.lastCommit {
					err = tx2.Commit()
				} else {
					err = tx2.Rollback()
				}
				if err != nil {
					t.Fatalf("end the second transaction: %v", err)
				}

				want := first
				if !tc.firstCommit {
					want = second.ref
				}
				c.Wake()
				waitFor(t, 10*time.Second, "the one run to succeed", func() bool {
					st, err := c.Status(ctx, want)
					return err == nil && st == workflow.StatusSucceeded
				})
				if n := countRuns(t, c); n != 1 {
					t.Fatalf("%d runs exist for the key, want exactly 1", n)
				}
				// A few polls more: a duplicate run would be admitted and run here.
				time.Sleep(10 * testPoll)
				if n := runs.Load(); n != 1 {
					t.Fatalf("the step ran %d times, want 1", n)
				}
			})
		}
	}
}

// countRuns counts every run of the "ship" workflow txSetup registers.
func countRuns(t *testing.T, c interface {
	Runs(context.Context, workflow.RunQuery) (workflow.RunPage, error)
}) int {
	t.Helper()
	page, err := c.Runs(context.Background(), workflow.RunQuery{Workflow: "ship"})
	if err != nil {
		t.Fatalf("Runs: %v", err)
	}
	return len(page.Runs)
}
