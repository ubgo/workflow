package sqlstore_test

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/sqlstore"
)

// TestDatabaseClockReadsTheServer: on every engine a synced DatabaseClock
// reads the server's time (the test databases run on this machine, so it
// is within a second of the local clock), and before its first Sync it is
// the local clock.
func TestDatabaseClockReadsTheServer(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, _ := e.open(t)
			k, err := sqlstore.NewDatabaseClock(db, e.dialect)
			if err != nil {
				t.Fatal(err)
			}
			if k.Offset() != 0 {
				t.Fatalf("Offset before Sync = %s, want 0", k.Offset())
			}
			if err := k.Sync(context.Background()); err != nil {
				t.Fatalf("Sync: %v", err)
			}
			if d := k.Now().Sub(time.Now()); d < -time.Second || d > time.Second {
				t.Fatalf("database now is %s from the local clock, want within a second", d)
			}
		})
	}
}

// TestDatabaseClockAdvancesLocally: between readings the clock advances by
// exactly the local clock's elapsed time from the database's reading, and a
// new Sync replaces the reference.
func TestDatabaseClockAdvancesLocally(t *testing.T) {
	db, _ := openSQLite(t)
	k, err := sqlstore.NewDatabaseClock(db, sqlb.SQLiteDialect{})
	if err != nil {
		t.Fatal(err)
	}
	local := clock.NewFake(time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC))
	sqlstore.SetDatabaseClockLocal(k, local)
	if got := k.Now(); !got.Equal(local.Now()) {
		t.Fatalf("Now before Sync = %s, want the local %s", got, local.Now())
	}
	if err := k.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if off := k.Offset(); off < 20*365*24*time.Hour {
		t.Fatalf("Offset = %s, want the database's present far ahead of the fake 2001", off)
	}
	before := k.Now()
	local.Advance(10 * time.Second)
	if d := k.Now().Sub(before); d != 10*time.Second {
		t.Fatalf("Now moved %s after the local clock moved 10s, want 10s", d)
	}
	if d := before.Sub(time.Now()); d < -time.Second || d > time.Second {
		t.Fatalf("database now is %s from the real clock, want within a second", d)
	}
}

// TestWithDatabaseClockRunsWork: a client on the database clock runs a
// delayed job and exposes the clock; an enqueue-only client syncs at Start.
func TestWithDatabaseClockRunsWork(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			var ran atomic.Int64
			wf := workflow.Workflow{Name: "dbclock", Steps: []workflow.Step{{Name: "a", Run: func(context.Context, []byte) ([]byte, error) {
				ran.Add(1)
				return nil, nil
			}}}}
			c := newClient(t, e, db, prefix, sqlstore.WithDatabaseClock(50*time.Millisecond))
			ctx := startWith(t, c, wf)
			k := c.DatabaseClock()
			if k == nil {
				t.Fatal("DatabaseClock() = nil with WithDatabaseClock")
			}
			if off := k.Offset(); off < -time.Second || off > time.Second {
				t.Fatalf("Offset = %s after Start, want a synced reading within a second", off)
			}
			if _, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, RunAt: k.Now().Add(200 * time.Millisecond)}); err != nil {
				t.Fatal(err)
			}
			waitFor(t, 10*time.Second, "the delayed run", func() bool { return ran.Load() == 1 })

			eo := newClient(t, e, db, prefix, sqlstore.WithDatabaseClock(0), sqlstore.WithEnqueueOnly())
			if err := eo.Start(ctx); err != nil {
				t.Fatal(err)
			}
			if eo.DatabaseClock().Offset() == 0 && eo.DatabaseClock().Uncertainty() == 0 {
				t.Fatal("the enqueue-only client did not read the database clock at Start")
			}
			if plain := newClient(t, e, db, prefix); plain.DatabaseClock() != nil {
				t.Fatal("DatabaseClock() is set without WithDatabaseClock")
			}
		})
	}
}

// TestLeaseSkewDelaysTakeover: a crashed worker's step is not taken over
// by a client whose lease skew margin has not passed, and is taken over by
// one without a margin.
func TestLeaseSkewDelaysTakeover(t *testing.T) {
	const lease = 300 * time.Millisecond
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			var execs atomic.Int64
			wf := workflow.Workflow{Name: "skew", Steps: []workflow.Step{{Name: "a", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
				if execs.Add(1) == 1 {
					<-ctx.Done() // the first worker dies here
					return nil, ctx.Err()
				}
				return nil, nil
			}}}}
			mk := func(opts ...sqlstore.Option) *sqlstore.Client {
				c := newClient(t, e, db, prefix, append([]sqlstore.Option{sqlstore.WithLease(lease)}, opts...)...)
				startWith(t, c, wf)
				return c
			}
			a := mk()
			ref, err := a.Enqueue(context.Background(), workflow.Job{Workflow: wf.Name})
			if err != nil {
				t.Fatal(err)
			}
			waitFor(t, 10*time.Second, "the first execution", func() bool { return execs.Load() == 1 })
			sqlstore.Crash(a)
			b := mk(sqlstore.WithLeaseSkew(time.Hour))
			time.Sleep(5 * lease)
			if got := execs.Load(); got != 1 {
				t.Fatalf("a client with an hour of lease skew took the step over %s after its lease ended (%d executions)", 4*lease, got)
			}
			b.Stop()
			c := mk()
			waitFor(t, 10*time.Second, "the takeover", func() bool {
				st, err := c.Status(context.Background(), ref)
				return err == nil && st == workflow.StatusSucceeded
			})
		})
	}
}

// TestLeaseFenceOnLostRenewal: while another connection holds a lock that
// stops every lease renewal, the holder cancels its own attempt with
// ErrLeaseLost once a whole lease has passed, writes no outcome, and the
// step runs again after the lock is released.
func TestLeaseFenceOnLostRenewal(t *testing.T) {
	const lease = 600 * time.Millisecond
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			var execs atomic.Int64
			causes := make(chan error, 1)
			wf := workflow.Workflow{Name: "fence", Steps: []workflow.Step{{Name: "a", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
				if execs.Add(1) == 1 {
					<-ctx.Done()
					causes <- context.Cause(ctx)
					return nil, ctx.Err()
				}
				return nil, nil
			}}}}
			c := newClient(t, e, db, prefix, sqlstore.WithLease(lease))
			ctx := startWith(t, c, wf)
			ref, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name})
			if err != nil {
				t.Fatal(err)
			}
			waitFor(t, 10*time.Second, "the first execution", func() bool { return execs.Load() == 1 })
			unlock := lockSteps(t, e, db, prefix)
			select {
			case cause := <-causes:
				if !errors.Is(cause, sqlstore.ErrLeaseLost) {
					unlock()
					t.Fatalf("attempt cancelled with %v, want ErrLeaseLost", cause)
				}
			case <-time.After(5 * lease):
				unlock()
				t.Fatalf("no fence %s after renewals stopped (lease %s)", 5*lease, lease)
			}
			unlock()
			waitFor(t, 10*time.Second, "the run to succeed after the lock", func() bool {
				st, err := c.Status(ctx, ref)
				return err == nil && st == workflow.StatusSucceeded
			})
			if got := execs.Load(); got != 2 {
				t.Fatalf("%d executions, want 2 (the fenced one and the retake)", got)
			}
		})
	}
}

// lockSteps holds a lock on the steps table from another connection until
// the returned function is called: SQLite takes the database's write lock,
// Postgres and MySQL lock the table, so every renewal waits.
func lockSteps(t *testing.T, e engine, db *sql.DB, prefix string) func() {
	t.Helper()
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var stmts []string
	var end string
	switch e.name {
	case "sqlite":
		stmts, end = []string{"BEGIN IMMEDIATE"}, "ROLLBACK"
	case "postgres":
		stmts, end = []string{"BEGIN", `LOCK TABLE "` + prefix + `steps" IN EXCLUSIVE MODE`}, "ROLLBACK"
	case "mysql":
		stmts, end = []string{"LOCK TABLES `" + prefix + "steps` WRITE"}, "UNLOCK TABLES"
	}
	for _, s := range stmts {
		if _, err := conn.ExecContext(ctx, s); err != nil {
			_ = conn.Close()
			t.Fatalf("%s: %v", s, err)
		}
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			_, _ = conn.ExecContext(ctx, end)
			_ = conn.Close()
		})
	}
}
