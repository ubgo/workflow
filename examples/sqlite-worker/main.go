// Command sqlite-worker runs one workflow on the two drivers that need no
// server: first on the in-memory driver (what an app's tests use), then on
// the durable SQL driver over a SQLite file, with the options a production
// worker sets (workers, polling, leases, an error classifier, its own run
// IDs).
//
// It is the program behind docs/drivers/memory.md and
// docs/drivers/sqlstore.md: the guides copy its regions (marked
// "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ubgo/sqlb"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/sqlstore"
)

// runTimeout bounds the wait for the demo run, generous for a slow CI host.
const runTimeout = 10 * time.Second

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "sqlite-worker:", err)
		os.Exit(1)
	}
}

// receipt is the job's payload; workflow.Define decodes it from JSON.
type receipt struct{ Order string }

// receiptWorkflow is the one workflow both drivers run: render a receipt,
// then send it. Nothing in it names a driver.
func receiptWorkflow() workflow.Workflow {
	return workflow.Workflow{
		Name: "send-receipt",
		Steps: []workflow.Step{
			{Name: "render", Run: workflow.Define(func(ctx context.Context, in receipt) (string, error) {
				return "receipt for " + in.Order, nil
			})},
			{Name: "send", After: []string{"render"}, Run: workflow.Define(func(ctx context.Context, body string) (string, error) {
				return "sent: " + body, nil
			})},
		},
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	if err := runMemory(ctx, out); err != nil {
		return fmt.Errorf("memory: %w", err)
	}
	dir, err := os.MkdirTemp("", "sqlite-worker-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	if err := runSQLite(ctx, out, filepath.Join(dir, "app.db")); err != nil {
		return fmt.Errorf("sqlite: %w", err)
	}
	return nil
}

// runMemory runs the workflow on the in-memory driver and waits for its end.
func runMemory(ctx context.Context, out io.Writer) error {
	// snippet:memory
	logger := slog.New(slog.NewTextHandler(io.Discard, nil)) // your app's logger
	c := memory.New(
		memory.WithWorkers(4),                                  // step attempts at once (default 8)
		memory.WithLogger(logger),                              // errors no caller can see
		memory.WithJitterSource(func() float64 { return 0.5 }), // pin retry jitter in tests
	)
	defer c.Stop() // cancels unfinished runs and waits for every goroutine
	// snippet:end

	ended := make(chan workflow.RunEvent, 1)
	if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { ended <- ev }); err != nil {
		return err
	}
	if err := c.Register(receiptWorkflow()); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}

	// snippet:memory-caps
	caps := c.Capabilities()
	fmt.Fprintln(out, "memory durable:", caps.Has(workflow.CapDurable))
	fmt.Fprintln(out, "memory cron:", caps.Has(workflow.CapCron))
	// snippet:end

	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "send-receipt", Payload: []byte(`{"Order":"A-1"}`)}); err != nil {
		return err
	}
	ev := <-ended
	fmt.Fprintln(out, "memory run:", ev.Status)
	return nil
}

// runSQLite runs the workflow on the SQL driver over a SQLite file.
func runSQLite(ctx context.Context, out io.Writer, path string) error {
	// snippet:open
	db, err := driversqlite.Open(path) // a file; ":memory:" would be lost on exit
	if err != nil {
		return err
	}
	defer db.Close()
	// snippet:end

	var lostRaces atomic.Int64
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// snippet:new
	c, err := sqlstore.New(db, sqlb.SQLiteDialect{},
		sqlstore.WithWorkers(4),                         // step attempts at once (default 8)
		sqlstore.WithPollInterval(200*time.Millisecond), // find other clients' work (default 1s)
		sqlstore.WithLease(30*time.Second),              // a dead worker's step is retaken after this
		sqlstore.WithMaxReclaims(3),                     // then the attempt fails with ErrAbandoned
		sqlstore.WithLogger(logger),
		sqlstore.WithIDGenerator(orderedIDs),
		sqlstore.WithErrorClassifier(func(err error) sqlstore.ErrorClass {
			class := sqlstore.DefaultErrorClassifier(err)
			if class.Retryable {
				lostRaces.Add(1) // e.g. a metric of lock conflicts
			}
			return class
		}),
	)
	if err != nil {
		return err
	}
	if err := c.Migrate(ctx); err != nil { // explicit; Start never migrates
		return err
	}
	// snippet:end

	ended := make(chan workflow.RunEvent, 1)
	if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { ended <- ev }); err != nil {
		return err
	}

	// snippet:start
	if err := c.Register(receiptWorkflow()); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := c.Start(ctx); err != nil {
		return err
	}
	defer c.Stop() // hands running steps back and waits for the workers

	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "send-receipt", Payload: []byte(`{"Order":"A-1"}`)})
	if err != nil {
		return err
	}
	// snippet:end

	fmt.Fprintln(out, "sqlite durable:", c.Capabilities().Has(workflow.CapDurable))
	fmt.Fprintln(out, "run id kind:", strings.SplitN(string(ref.RunID), "_", 2)[0])
	select {
	case ev := <-ended:
		fmt.Fprintln(out, "sqlite run:", ev.Status)
	case <-time.After(runTimeout):
		return fmt.Errorf("the run did not finish within %s", runTimeout)
	}
	st, err := c.Status(ctx, ref)
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "status after the end:", st)
	return nil
}

// snippet:ids
// orderedIDs makes IDs that sort by creation time (like a UUIDv7 or ULID):
// the kind, the Unix milliseconds in fixed-width hex, then 8 random bytes.
// An ID must be 1 to 64 bytes and unique per kind across every client.
func orderedIDs(kind sqlstore.IDKind) (string, error) {
	var rnd [8]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		return "", err // the driver refuses the operation; no bad row is written
	}
	return fmt.Sprintf("%s_%012x%s", kind, time.Now().UnixMilli(), hex.EncodeToString(rnd[:])), nil
}

// snippet:end
