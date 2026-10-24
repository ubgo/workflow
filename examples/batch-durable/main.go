// Command batch-durable keeps batch state in SQLite through the SQL
// driver's BatchStore, so a batch outlives the process that dispatched it.
// Process A starts dispatching a five-job batch and stalls on its third
// job, as a process that hangs or dies part-way would; process B, sharing the
// database, takes the dispatch over once A's lease has ended, starts the
// jobs A never started and runs the batch to its Finally.
//
// It is the program behind docs/batches/durability.md: the guide copies
// its regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/ubgo/sqlb"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/sqlstore"
)

const (
	// pollInterval is short so the demo finishes quickly; production keeps
	// the driver's default.
	pollInterval = 20 * time.Millisecond
	// statePoll is how often the program re-reads the batch store while it
	// waits for the dispatch to reach the state it stalls in.
	statePoll = 5 * time.Millisecond
)

// lease is how long a dispatch may go without writing before another
// process takes it over (batch.DefaultDispatchLease is five minutes).
const lease = time.Minute

func main() {
	dir, err := os.MkdirTemp("", "batch-durable")
	if err != nil {
		fmt.Fprintln(os.Stderr, "batch-durable:", err)
		os.Exit(1)
	}
	defer os.RemoveAll(dir)
	if err := run(context.Background(), os.Stdout, filepath.Join(dir, "wf.db")); err != nil {
		fmt.Fprintln(os.Stderr, "batch-durable:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer, path string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	db, err := driversqlite.Open(path)
	if err != nil {
		return err
	}
	defer db.Close()
	// Both managers read time from this clock, so the lease can be ended
	// with Advance instead of waiting a minute.
	clk := clock.NewFake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	// Process A dispatches and stalls on job 2.
	ctxA, killA := context.WithCancel(ctx)
	defer killA()
	a, err := newClient(ctx, db)
	if err != nil {
		return err
	}
	finally := make(chan batch.Progress, 1)      // the Finally may run on either process's workers
	if err := register(a, finally); err != nil { // the same code runs in every process
		return err
	}
	if err := a.Start(ctxA); err != nil {
		return err
	}
	stalled := stalledAt{Client: a, index: "2", ctx: ctxA}
	// snippet:dispatcher
	ma := batch.New(stalled, a.BatchStore(), batch.WithClock(clk),
		batch.WithDispatchLease(lease),
		batch.WithResumableDispatch(), // keep the job specs so a resumer can start the unstarted ones
	)
	// snippet:end
	dispatched := make(chan error, 1)
	go func() {
		_, err := ma.Dispatch(ctxA, batch.Batch{Name: "nightly", Jobs: jobs(5), Finally: "report"})
		dispatched <- err
	}()
	id, err := waitForBatch(ctx, a.BatchStore(), dispatched)
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "process A started jobs 0 and 1, then stalled on job 2")

	// Process B: another client on the same database.
	b, err := newClient(ctx, db)
	if err != nil {
		return err
	}
	if err := register(b, finally); err != nil {
		return err
	}
	// snippet:store
	store := b.BatchStore() // durable: every process with this database sees the same batches
	mb := batch.New(b, store, batch.WithClock(clk), batch.WithDispatchLease(lease))
	if err := mb.Attach(ctx); err != nil {
		return err
	}
	if err := b.Start(ctx); err != nil {
		return err
	}
	// snippet:end

	// snippet:resume
	clk.Advance(lease + time.Second) // A has not written for longer than its lease
	if err := mb.ResumeDispatches(ctx); err != nil {
		return err
	}
	// snippet:end

	p := <-finally
	fmt.Fprintf(out, "finally: %d jobs, %d succeeded, %d cancelled\n", p.Total, p.Succeeded, p.CancelledJobs)
	st, err := store.Get(ctx, id)
	if err != nil {
		return err
	}
	for i, j := range st.Jobs {
		fmt.Fprintf(out, "job %d: %s\n", i, j.Status)
	}

	killA() // A's stalled call returns: its lease is gone, so it starts nothing more
	fmt.Fprintln(out, "process A's dispatch ended with an error:", <-dispatched != nil)
	return nil
}

// newClient opens a SQL driver client on db and migrates its tables.
func newClient(ctx context.Context, db *sql.DB) (*sqlstore.Client, error) {
	c, err := sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithPollInterval(pollInterval))
	if err != nil {
		return nil, err
	}
	return c, c.Migrate(ctx)
}

// jobs returns n export jobs whose payload is their index.
func jobs(n int) []workflow.Job {
	out := make([]workflow.Job, n)
	for i := range out {
		out[i] = workflow.Job{Workflow: "export", Payload: fmt.Appendf(nil, "%d", i)}
	}
	return out
}

// register registers the batch's job workflow and its Finally on c.
func register(c *sqlstore.Client, finally chan<- batch.Progress) error {
	if err := c.Register(workflow.Workflow{
		Name:  "export",
		Steps: []workflow.Step{{Name: "run", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}},
	}); err != nil {
		return err
	}
	return c.Register(workflow.Workflow{
		Name: "report",
		Steps: []workflow.Step{{Name: "run", Run: workflow.Define(func(_ context.Context, p batch.Progress) (string, error) {
			finally <- p
			return "", nil
		})}},
	})
}

// waitForBatch waits until a batch exists whose jobs 0 and 1 have refs,
// and returns its ID, or fails when the dispatch ended first.
func waitForBatch(ctx context.Context, store batch.Store, dispatched <-chan error) (batch.ID, error) {
	for {
		open, err := store.Unfinished(ctx)
		if err != nil {
			return "", err
		}
		if len(open) == 1 && !open[0].Jobs[1].Ref.IsZero() {
			return open[0].ID, nil
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case err := <-dispatched:
			return "", fmt.Errorf("dispatch ended before stalling: %w", err)
		case <-time.After(statePoll):
		}
	}
}

// stalledAt is a client whose Enqueue of the batch job with the given
// index does not return until ctx ends, the way a process that hangs or
// dies mid-dispatch stops writing. The job never reaches the driver.
type stalledAt struct {
	workflow.Client
	index string
	ctx   context.Context
}

// Enqueue blocks the job at f.index until f.ctx ends, then fails it the way
// a killed process would: the dispatch stops part-way, with the earlier jobs
// already started.
func (f stalledAt) Enqueue(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
	if job.Labels[batch.LabelIndex] == f.index {
		<-f.ctx.Done()
		return workflow.RunRef{}, errors.New("process killed")
	}
	return f.Client.Enqueue(ctx, job)
}
