// Command enqueue-only splits web nodes from worker nodes on the SQL
// driver, on a SQLite file: a web client built with WithEnqueueOnly
// enqueues and reads status without Start and runs nothing, two worker
// clients over the same tables run the work, and a step cut short by a
// clean stop of one worker is finished by the other on the same attempt,
// without spending a retry.
//
// It is the program behind the "Web nodes that only enqueue" and "Shut
// down cleanly" sections of docs/operations/deploy.md: the guide copies its
// regions (marked "snippet:") and task docs:snippets fails when they
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
	"github.com/ubgo/workflow/sqlstore"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "enqueue-only:", err)
		os.Exit(1)
	}
}

// pollInterval is short so the demo is quick; the default is one second.
const pollInterval = 20 * time.Millisecond

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	dir, err := os.MkdirTemp("", "enqueue-only-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	db, err := driversqlite.Open(filepath.Join(dir, "jobs.db"))
	if err != nil {
		return err
	}
	defer db.Close()

	// A step that blocks on its first run until its worker stops, then
	// succeeds on whichever worker runs it next.
	attempts := make(chan int, 4)
	firstRun := true
	thumbnail := workflow.Workflow{
		Name:  "thumbnail",
		Retry: workflow.RetryPolicy{}, // no retries: a spent attempt would fail the run
		Steps: []workflow.Step{{Name: "resize", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			info, _ := workflow.StepInfoFrom(ctx)
			attempts <- info.Attempt
			if firstRun {
				firstRun = false
				<-ctx.Done() // the worker is shutting down
				return nil, ctx.Err()
			}
			return nil, nil
		}}},
	}

	// snippet:web
	// A web node: it enqueues and reads status, and never runs a step.
	// No Start, no worker goroutines, no cron.
	web, err := sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithEnqueueOnly())
	if err != nil {
		return err
	}
	if err := web.Migrate(ctx); err != nil { // usually your deploy step, not the web node
		return err
	}
	ref, err := web.Enqueue(ctx, workflow.Job{Workflow: "thumbnail", Payload: []byte(`"img-1"`)})
	if err != nil {
		return err
	}
	// snippet:end
	st, err := web.Status(ctx, ref)
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "web enqueued", ref.RunID != "", "status", st)

	workerA, stopA, err := startWorker(ctx, db, thumbnail)
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "worker A runs attempt", <-attempts)

	// snippet:shutdown
	// SIGTERM on worker A: cancel its Start context and wait for Stop.
	// The step it was running is put back without spending an attempt.
	stopA()
	workerA.Stop()
	// snippet:end
	fmt.Fprintln(out, "worker A stopped")

	workerB, stopB, err := startWorker(ctx, db, thumbnail)
	if err != nil {
		return err
	}
	defer func() { stopB(); workerB.Stop() }()
	fmt.Fprintln(out, "worker B runs attempt", <-attempts)

	deadline := time.Now().Add(5 * time.Second)
	for {
		st, err := web.Status(ctx, ref)
		if err != nil {
			return err
		}
		if st.IsTerminal() {
			break
		}
		if time.Now().After(deadline) {
			return errors.New("the run did not finish in 5s")
		}
		time.Sleep(pollInterval)
	}
	page, err := web.Runs(ctx, workflow.RunQuery{Ref: ref})
	if err != nil {
		return err
	}
	r := page.Runs[0]
	fmt.Fprintf(out, "run %s, attempts counted %d, errors recorded %d\n", r.Status, r.Attempts, len(r.Errors))
	return nil
}

// startWorker starts a worker client over db: the same code every worker
// process runs.
func startWorker(ctx context.Context, db *sql.DB, wf workflow.Workflow) (*sqlstore.Client, context.CancelFunc, error) {
	// snippet:worker
	c, err := sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithPollInterval(pollInterval))
	if err != nil {
		return nil, nil, err
	}
	if err := c.Register(wf); err != nil {
		return nil, nil, err
	}
	wctx, cancel := context.WithCancel(ctx)
	if err := c.Start(wctx); err != nil {
		cancel()
		return nil, nil, err
	}
	// snippet:end
	return c, cancel, nil
}
