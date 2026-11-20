package enginetest

import (
	"context"
	"database/sql"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/sqlstore"
	"github.com/ubgo/workflow/workflowtest"
)

func newInboxes(t *testing.T, e engine) (*sql.DB, string, *sqlstore.Inbox, *sqlstore.Inbox) {
	t.Helper()
	db, prefix := e.open(t)
	a, err := sqlstore.NewInbox(db, e.dialect, sqlstore.WithTablePrefix(prefix))
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	b, err := sqlstore.NewInbox(db, e.dialect, sqlstore.WithTablePrefix(prefix))
	if err != nil {
		t.Fatal(err)
	}
	return db, prefix, a, b
}

// TestInbox is the workflow.Inbox contract (MQ-15) on every engine,
// through two inboxes over one table, as two processes would hold.
func TestInbox(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			_, _, a, b := newInboxes(t, e)
			workflowtest.InboxContract(t, a, b)
		})
	}
}

// TestInboxMarkTx: the mark commits and rolls back with the app's
// transaction; a duplicate leaves the transaction usable; of concurrent
// transactions marking one message exactly one does the work; Claim sees
// a marked message as done; PruneExpired removes ended rows only.
func TestInboxMarkTx(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()
			db, prefix, in, _ := newInboxes(t, e)
			now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
			q := func(s string) string { return "CREATE TABLE " + e.dialect.QuoteIdent(prefix+"orders") + s }
			if _, err := db.ExecContext(ctx, q(" (id VARCHAR(64) PRIMARY KEY)")); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _, _ = db.Exec("DROP TABLE " + e.dialect.QuoteIdent(prefix+"orders")) })
			insertOrder := func(tx *sql.Tx, id string) error {
				_, err := tx.ExecContext(ctx, "INSERT INTO "+e.dialect.QuoteIdent(prefix+"orders")+" (id) VALUES ("+e.dialect.Placeholder(1)+")", id)
				return err
			}
			key := workflow.InboxKey{Consumer: "orders", ID: "msg-1"}

			// A rolled-back mark is gone.
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			if fresh, err := in.MarkTx(ctx, tx, key, time.Hour, now); !fresh || err != nil {
				t.Fatalf("first mark = %v, %v", fresh, err)
			}
			_ = tx.Rollback()

			// Concurrent deliveries: exactly one transaction does the work.
			var did atomic.Int32
			var wg sync.WaitGroup
			for i := range 4 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					err := withTx(ctx, db, func(tx *sql.Tx) error {
						fresh, err := in.MarkTx(ctx, tx, key, time.Hour, now)
						if err != nil || !fresh {
							return err
						}
						did.Add(1)
						return insertOrder(tx, "order-"+string(rune('a'+i)))
					})
					if err != nil {
						t.Errorf("delivery %d: %v", i, err)
					}
				}()
			}
			wg.Wait()
			if n := did.Load(); n != 1 {
				t.Fatalf("%d transactions did the work; want 1", n)
			}
			var orders int
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+e.dialect.QuoteIdent(prefix+"orders")).Scan(&orders); err != nil || orders != 1 {
				t.Fatalf("orders = %d, %v; want 1", orders, err)
			}

			// A duplicate leaves the caller's transaction usable.
			err = withTx(ctx, db, func(tx *sql.Tx) error {
				if fresh, err := in.MarkTx(ctx, tx, key, time.Hour, now.Add(time.Minute)); fresh || err != nil {
					t.Fatalf("duplicate mark = %v, %v", fresh, err)
				}
				return insertOrder(tx, "after-duplicate")
			})
			if err != nil {
				t.Fatalf("the transaction after a duplicate mark: %v", err)
			}
			if st, _, err := in.Claim(ctx, key, time.Minute, now.Add(time.Minute)); st != workflow.InboxDone || err != nil {
				t.Fatalf("Claim of a marked message = %v, %v; want done", st, err)
			}
			// After the keep ends the message is new again.
			err = withTx(ctx, db, func(tx *sql.Tx) error {
				fresh, err := in.MarkTx(ctx, tx, key, time.Hour, now.Add(2*time.Hour))
				if !fresh || err != nil {
					t.Fatalf("mark after keep = %v, %v; want fresh", fresh, err)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			other := workflow.InboxKey{Consumer: "orders", ID: "msg-2"}
			if st, _, _ := in.Claim(ctx, other, time.Minute, now); st != workflow.InboxClaimed {
				t.Fatal(st)
			}
			if n, err := in.PruneExpired(ctx, now.Add(30*time.Minute)); n != 1 || err != nil {
				t.Fatalf("PruneExpired = %d, %v; want the expired claim only", n, err)
			}
			if st, _, _ := in.Claim(ctx, key, time.Minute, now.Add(2*time.Hour)); st != workflow.InboxDone {
				t.Fatalf("PruneExpired removed a live entry: %v", st)
			}
		})
	}
}

// withTx runs fn in a transaction, retrying a whole transaction that lost
// a lock race (SQLite busy, a MySQL deadlock) as an app would.
func withTx(ctx context.Context, db *sql.DB, fn func(tx *sql.Tx) error) error {
	var err error
	for range 20 {
		var tx *sql.Tx
		tx, err = db.BeginTx(ctx, nil)
		if err != nil {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		if err = fn(tx); err != nil {
			_ = tx.Rollback()
			time.Sleep(5 * time.Millisecond)
			continue
		}
		if err = tx.Commit(); err == nil {
			return nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return err
}
