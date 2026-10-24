// Command outbox writes jobs into an outbox table inside the app's own
// transaction and relays them to another client after the commit: here the
// in-memory driver, in production Hatchet or any other workflow.Client. It
// also shows an entry going dead, being listed and requeued, and a relay
// to a target that has no idempotency keys.
//
// It is the program behind docs/reliability/outbox.md: the guide copies its
// regions (marked "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/ubgo/sqlb"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
	"github.com/ubgo/workflow/sqlstore/outbox"
	"github.com/ubgo/workflow/workflowtest"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "outbox:", err)
		os.Exit(1)
	}
}

var ship = workflow.Workflow{Name: "ship", Steps: []workflow.Step{{
	Name: "ship",
	Run:  func(ctx context.Context, in []byte) ([]byte, error) { return nil, nil },
}}}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	dir, err := os.MkdirTemp("", "outbox-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	db, err := driversqlite.Open(filepath.Join(dir, "app.db"))
	if err != nil {
		return err
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "CREATE TABLE orders (id INTEGER PRIMARY KEY, item TEXT NOT NULL)"); err != nil {
		return err
	}
	clk := clock.NewFake(time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC))

	// The target: the memory driver behind a switch that makes it refuse
	// every enqueue while down, standing in for an engine outage.
	shipped := make(chan string, 4)
	mc := memory.New()
	if err := mc.Register(workflow.Workflow{Name: "ship", Steps: []workflow.Step{{
		Name: "ship",
		Run: func(ctx context.Context, in []byte) ([]byte, error) {
			shipped <- string(in)
			return nil, nil
		},
	}}}); err != nil {
		return err
	}
	if err := mc.Start(ctx); err != nil {
		return err
	}
	var down atomic.Bool
	target := middleware.WrapClient(mc, func(next workflow.EnqueueFunc) workflow.EnqueueFunc {
		return func(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
			if down.Load() {
				return workflow.RunRef{}, workflow.Unavailable(errors.New("engine unreachable"))
			}
			return next(ctx, job)
		}
	})

	fmt.Fprintln(out, "-- Write jobs inside your transaction")
	// snippet:write
	ob, err := outbox.New(db, sqlb.SQLiteDialect{}, outbox.WithClock(clk))
	if err != nil {
		return err
	}
	if err := ob.Migrate(ctx); err != nil { // creates wf_outbox; explicit, versioned
		return err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() // a no-op once committed
	if _, err := tx.ExecContext(ctx, "INSERT INTO orders (id, item) VALUES (1, 'book')"); err != nil {
		return err
	}
	if _, err := ob.Write(ctx, tx, workflow.Job{Workflow: "ship", Payload: []byte(`{"order":1}`)}); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	// snippet:end

	// A rolled-back transaction leaves no entry behind.
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err := ob.Write(ctx, tx, workflow.Job{Workflow: "ship", Payload: []byte(`{"order":2}`)}); err != nil {
		return err
	}
	if err := tx.Rollback(); err != nil {
		return err
	}

	fmt.Fprintln(out, "-- Relay entries to any client")
	// snippet:relay
	relay, err := ob.NewRelay(target)
	if err != nil {
		return err
	}
	if err := relay.Start(ctx); err != nil { // nothing is relayed before Start
		return err
	}
	relay.Notify() // relay now rather than at the next poll
	// snippet:end
	fmt.Fprintln(out, "shipped", <-shipped)
	relay.Stop()
	left, err := relay.Flush(ctx) // the rolled-back write left nothing to relay
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "entries left after the rollback:", left)

	fmt.Fprintln(out, "-- Failed and dead entries")
	if err := deadEntries(ctx, out, db, ob, target, &down, clk, shipped); err != nil {
		return err
	}

	fmt.Fprintln(out, "-- Targets without idempotency keys")
	return withoutKeys(ctx, out, db, ob)
}

// deadEntries sends one entry while the target is down until it goes dead,
// lists it, then requeues it once the target is back.
func deadEntries(ctx context.Context, out io.Writer, db *sql.DB, ob *outbox.Outbox, target workflow.Client,
	down *atomic.Bool, clk *clock.Fake, shipped <-chan string) error {
	// snippet:dead
	relay, err := ob.NewRelay(target,
		outbox.WithBackoff(workflow.Backoff{Initial: time.Minute, Factor: 2, Max: time.Hour}),
		outbox.WithMaxAttempts(2), // dead after two failed enqueues; the default retries forever
		outbox.WithOnDead(func(ctx context.Context, e outbox.DeadEntry, cause error) {
			fmt.Fprintln(out, "dead:", e.Job.Workflow, "after", e.Attempts, "attempts:", cause)
		}))
	if err != nil {
		return err
	}
	// snippet:end

	down.Store(true)
	if err := writeOne(ctx, db, ob, `{"order":4}`); err != nil {
		return err
	}
	// Flush relays once without starting the loop: handy in a test or
	// from the app's own scheduler.
	n, err := relay.Flush(ctx) // attempt 1 fails; retried after the backoff
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "accepted while down:", n)
	clk.Advance(time.Minute)
	if _, err := relay.Flush(ctx); err != nil { // attempt 2 fails: dead
		return err
	}

	// snippet:list-requeue
	dead, err := ob.ListDead(ctx, outbox.DeadQuery{Workflow: "ship"})
	if err != nil {
		return err
	}
	for _, e := range dead {
		fmt.Fprintf(out, "listed: %s attempts=%d last error=%q\n", e.Job.Payload, e.Attempts, e.LastErr)
	}
	down.Store(false) // the target is back
	for _, e := range dead {
		if err := ob.Requeue(ctx, e.ID); err != nil { // same ID, so same idempotency key
			return err
		}
	}
	n, err = relay.Flush(ctx)
	// snippet:end
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "accepted after requeue:", n)
	fmt.Fprintln(out, "shipped", <-shipped)
	return nil
}

// withoutKeys relays to a target that cannot dedup by idempotency key.
func withoutKeys(ctx context.Context, out io.Writer, db *sql.DB, ob *outbox.Outbox) error {
	// A Fake claiming every capability of the memory driver except
	// CapIdempotencyKey stands in for such a target.
	noKeys := workflowtest.NewFake(workflowtest.FakeCaps(memory.New().Capabilities() &^ workflow.CapIdempotencyKey))
	if err := noKeys.Register(ship); err != nil {
		return err
	}
	if err := noKeys.Start(ctx); err != nil {
		return err
	}

	// snippet:allow-duplicates
	_, err := ob.NewRelay(noKeys)
	fmt.Fprintln(out, "refused without AllowDuplicates:", errors.Is(err, workflow.ErrUnsupported))
	relay, err := ob.NewRelay(noKeys, outbox.AllowDuplicates()) // a redelivered entry runs twice
	if err != nil {
		return err
	}
	// snippet:end
	if err := writeOne(ctx, db, ob, `{"order":5}`); err != nil {
		return err
	}
	n, err := relay.Flush(ctx)
	if err != nil {
		return err
	}
	jobs := noKeys.Jobs()
	fmt.Fprintf(out, "accepted: %d, idempotency key sent: %q\n", n, jobs[len(jobs)-1].IdempotencyKey)
	return nil
}

// writeOne writes one ship job in its own committed transaction.
func writeOne(ctx context.Context, db *sql.DB, ob *outbox.Outbox, payload string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := ob.Write(ctx, tx, workflow.Job{Workflow: "ship", Payload: []byte(payload)}); err != nil {
		return err
	}
	return tx.Commit()
}
