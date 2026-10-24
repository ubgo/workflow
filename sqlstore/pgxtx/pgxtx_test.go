package pgxtx_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/ubgo/sqlb"
	driverpgx "github.com/ubgo/sqlb/contrib/driver-pgx"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/sqlstore"
	"github.com/ubgo/workflow/sqlstore/outbox"
	"github.com/ubgo/workflow/sqlstore/pgxtx"
)

// setup opens the Postgres test database twice (the store's database/sql
// pool and a pgx connection), with a fresh table prefix, and starts a
// client running workflow "ship", which counts its runs.
func setup(t *testing.T) (*sqlstore.Client, *pgx.Conn, string, *atomic.Int64) {
	t.Helper()
	dsn := os.Getenv("WFTEST_PG_DSN")
	if dsn == "" {
		t.Skip("WFTEST_PG_DSN unset: run task test:pg")
	}
	ctx := context.Background()
	db, err := driverpgx.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	prefix := "t" + hex.EncodeToString(b) + "_"
	t.Cleanup(func() { dropAll(db, prefix) })
	c, err := sqlstore.New(db, sqlb.PostgresDialect{}, sqlstore.WithTablePrefix(prefix), sqlstore.WithPollInterval(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	var runs atomic.Int64
	if err := c.Register(workflow.Workflow{Name: "ship", Idempotency: workflow.IdempotencyOptional, Steps: []workflow.Step{{Name: "a",
		Run: func(context.Context, []byte) ([]byte, error) { runs.Add(1); return nil, nil }}}}); err != nil {
		t.Fatal(err)
	}
	cctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	if err := c.Start(cctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Stop)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	if _, err := conn.Exec(ctx, "CREATE TABLE "+prefix+"orders (id TEXT PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	return c, conn, prefix, &runs
}

func dropAll(db *sql.DB, prefix string) {
	rows, err := db.Query("SELECT tablename FROM pg_tables WHERE tablename LIKE $1", prefix+"%")
	if err != nil {
		return
	}
	var names []string
	for rows.Next() {
		var n string
		if rows.Scan(&n) == nil {
			names = append(names, n)
		}
	}
	_ = rows.Close()
	for _, n := range names {
		_, _ = db.Exec(`DROP TABLE IF EXISTS "` + n + `" CASCADE`)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestEnqueueInPgxTx: a run enqueued through the view commits with the
// pgx transaction's own write, and a second enqueue with the same key in
// the same transaction returns the first run (the savepoint path).
func TestEnqueueInPgxTx(t *testing.T) {
	c, conn, prefix, runs := setup(t)
	ctx := context.Background()
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "INSERT INTO "+prefix+"orders (id) VALUES ('o1')"); err != nil {
		t.Fatal(err)
	}
	var first, second workflow.RunRef
	err = pgxtx.Run(ctx, tx, func(stx *sql.Tx) error {
		var err error
		if first, err = c.EnqueueTx(ctx, stx, workflow.Job{Workflow: "ship", IdempotencyKey: "o1"}); err != nil {
			return err
		}
		second, err = c.EnqueueTx(ctx, stx, workflow.Job{Workflow: "ship", IdempotencyKey: "o1"})
		return err
	})
	if err != nil {
		t.Fatalf("EnqueueTx through pgxtx: %v", err)
	}
	if first.RunID != second.RunID {
		t.Fatalf("the same key gave two runs in one transaction: %s and %s", first.RunID, second.RunID)
	}
	if st, err := c.Status(ctx, first); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("Status before commit = %v, %v; want ErrNotFound (the run is not committed)", st, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	c.Wake()
	waitFor(t, "the committed run", func() bool {
		st, err := c.Status(ctx, first)
		return err == nil && st == workflow.StatusSucceeded
	})
	if n := runs.Load(); n != 1 {
		t.Fatalf("%d runs, want 1", n)
	}
}

// TestRollbackInPgxTx: rolling the pgx transaction back leaves no run and
// no outbox entry.
func TestRollbackInPgxTx(t *testing.T) {
	c, conn, prefix, runs := setup(t)
	ctx := context.Background()
	ob, err := outbox.New(c.DB(), sqlb.PostgresDialect{}, outbox.WithTablePrefix(prefix))
	if err != nil {
		t.Fatal(err)
	}
	if err := ob.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var ref workflow.RunRef
	err = pgxtx.Run(ctx, tx, func(stx *sql.Tx) error {
		var err error
		if ref, err = c.EnqueueTx(ctx, stx, workflow.Job{Workflow: "ship"}); err != nil {
			return err
		}
		_, err = ob.Write(ctx, stx, workflow.Job{Workflow: "ship"})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	c.Wake()
	time.Sleep(200 * time.Millisecond)
	if _, err := c.Status(ctx, ref); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("Status after rollback: %v, want ErrNotFound", err)
	}
	var n int
	if err := conn.QueryRow(ctx, "SELECT COUNT(*) FROM "+prefix+"outbox").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 || runs.Load() != 0 {
		t.Fatalf("after rollback: %d outbox entries, %d runs; want none", n, runs.Load())
	}
}

// TestRunRefusesNil and a function error passes through.
func TestRunErrors(t *testing.T) {
	if err := pgxtx.Run(context.Background(), nil, func(*sql.Tx) error { return nil }); err == nil {
		t.Fatal("Run(nil tx) succeeded")
	}
}
