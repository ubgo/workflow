package sqlstore_test

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	driverpgx "github.com/ubgo/sqlb/contrib/driver-pgx"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/sqlstore"
	"github.com/ubgo/workflow/workflowtest"
)

// smallPool opens a database for e whose pool holds only n connections,
// with its own table prefix (dropped after).
func smallPool(t *testing.T, e engine, n int) (*sql.DB, string) {
	t.Helper()
	if e.name != "postgres" {
		db, prefix := e.open(t)
		db.SetMaxOpenConns(n)
		return db, prefix
	}
	db, err := driverpgx.Open(os.Getenv(envPostgresDSN), driverpgx.WithMaxOpenConns(n))
	if err != nil {
		t.Fatal(err)
	}
	prefix := "t" + randHex(6) + "_"
	t.Cleanup(func() {
		dropTables(db, prefix, `"`)
		_ = db.Close()
	})
	return db, prefix
}

// TestSmallPoolDoesNotDeadlock runs more workers than the pool has
// connections over many short jobs. A query on the pool made while the
// same call path holds a transaction needs a second connection; once every
// connection is held by such a transaction, nothing finishes. The jobs
// must all succeed within the limit.
func TestSmallPoolDoesNotDeadlock(t *testing.T) {
	const (
		conns   = 2
		workers = 10
		jobs    = 50
	)
	for _, e := range engines(t) {
		if e.name == "mysql" {
			continue // its shared opener has no pool option; Postgres and SQLite cover the path
		}
		t.Run(e.name, func(t *testing.T) {
			db, prefix := smallPool(t, e, conns)
			wf := workflow.Workflow{Name: "short", Steps: []workflow.Step{
				{Name: "a", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }},
				{Name: "b", After: []string{"a"}, Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }},
			}}
			c := newClient(t, e, db, prefix, sqlstore.WithWorkers(workers))
			if err := c.Register(wf); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			if err := c.Start(ctx); err != nil {
				t.Fatal(err)
			}
			// Every call is bounded: in a deadlock an enqueue or a read would
			// wait for a connection forever instead of letting the test fail.
			const limit = 60 * time.Second
			rctx, rcancel := context.WithTimeout(ctx, limit)
			defer rcancel()
			refs := make([]workflow.RunRef, jobs)
			for i := range refs {
				ref, err := c.Enqueue(rctx, workflow.Job{Workflow: wf.Name})
				if err != nil {
					t.Fatalf("Enqueue %d (a pool deadlock fails here): %v", i, err)
				}
				refs[i] = ref
			}
			waitFor(t, limit+5*time.Second, "every run to succeed on a small pool (a pool deadlock fails here)", func() bool {
				for _, ref := range refs {
					if st, err := c.Status(rctx, ref); err != nil || st != workflow.StatusSucceeded {
						return false
					}
				}
				return true
			})
		})
	}
}

// TestConformanceOnOneConnection runs the shared driver suite on SQLite
// with a pool of a single connection: the guard against the next nested
// pool use. Any query the driver makes on the pool while the same call
// path holds a transaction (or an open result set) waits for a connection
// that only it holds, so the suite hangs at that call and go test's
// -timeout dump names it, instead of the deadlock reaching a production
// pool under load.
func TestConformanceOnOneConnection(t *testing.T) {
	e := engines(t)[0]
	workflowtest.Run(t, workflowtest.Harness{New: func(t *testing.T) workflowtest.Instance {
		db, prefix := e.open(t)
		db.SetMaxOpenConns(1)
		clk := clock.NewFake(start)
		current := newClient(t, e, db, prefix, sqlstore.WithClock(clk))
		return workflowtest.Instance{
			Client: current,
			Clock:  clk,
			Restart: func(t *testing.T) workflow.Client {
				current = newClient(t, e, db, prefix, sqlstore.WithClock(clk))
				return current
			},
			EnqueueOnly: func(t *testing.T) workflow.Client {
				return newClient(t, e, db, prefix, sqlstore.WithClock(clk), sqlstore.WithEnqueueOnly())
			},
		}
	}})
}
