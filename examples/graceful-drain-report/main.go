// Command graceful-drain-report shows what a deploy-time Drain tells you
// when a step does not finish inside the grace period: workflow.ShutdownHookReport
// drains within a grace and hands the report to a callback, which lists
// every attempt that was cut off and what became of it. On the in-memory
// driver the cut run is cancelled; on the SQL driver (a SQLite file here)
// it is requeued without spending an attempt, and a second worker runs it
// as the same attempt number.
//
// ShutdownHookReport returns a plain func(context.Context) error, so any
// phased shutdown manager can register it; this program just calls it.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/ubgo/sqlb"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/sqlstore"
)

// grace is how long a drain lets running steps finish. Short here so the
// demo is quick; a deploy would use the platform's termination window
// minus a margin.
const grace = 50 * time.Millisecond

// pollInterval is short so the demo is quick; the default is one second.
const pollInterval = 20 * time.Millisecond

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "graceful-drain-report:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	if err := inMemory(ctx, out); err != nil {
		return err
	}
	return durable(ctx, out)
}

// stuckWorkflow's step reports its attempt number, then holds until its
// ctx ends (the cut-off) or, when release is non-nil and closed, finishes.
func stuckWorkflow(attempts chan<- int, release <-chan struct{}) workflow.Workflow {
	return workflow.Workflow{
		Name:  "thumbnail",
		Retry: workflow.RetryPolicy{}, // no retries: a spent attempt would fail the run
		Steps: []workflow.Step{{Name: "resize", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			info, _ := workflow.StepInfoFrom(ctx)
			attempts <- info.Attempt
			select {
			case <-release:
				return nil, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}}},
	}
}

// printReport is the log line a deploy script writes.
func printReport(out io.Writer, label string) func(workflow.DrainReport, bool) {
	return func(r workflow.DrainReport, detailed bool) {
		fmt.Fprintf(out, "%s: detailed=%v finished steps=%d runs=%d, cut off %d\n", label, detailed, r.FinishedSteps, r.FinishedRuns, len(r.Cut))
		for _, c := range r.Cut {
			fmt.Fprintf(out, "  cut: %s/%s attempt %d -> %s\n", c.Workflow, c.Step, c.Attempt, c.Disposition)
		}
	}
}

func inMemory(ctx context.Context, out io.Writer) error {
	attempts := make(chan int, 4)
	c := memory.New()
	if err := c.Register(stuckWorkflow(attempts, nil)); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "thumbnail"})
	if err != nil {
		return err
	}
	<-attempts

	// snippet:hook
	// Register this in the shutdown phase that runs before the database
	// and HTTP listener close. It drains within the grace, or sooner if
	// the phase's own ctx ends first, and reports what it cut off.
	hook := workflow.ShutdownHookReport(c, grace, printReport(out, "memory"))
	err = hook(ctx)
	// snippet:end
	fmt.Fprintln(out, "memory: drain error is a deadline:", err == context.DeadlineExceeded)
	st, err := c.Status(ctx, ref)
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "memory: cut run status:", st)
	return nil
}

func durable(ctx context.Context, out io.Writer) error {
	dir, err := os.MkdirTemp("", "graceful-drain-report-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	db, err := driversqlite.Open(filepath.Join(dir, "jobs.db"))
	if err != nil {
		return err
	}
	defer db.Close()

	attempts := make(chan int, 4)
	a, err := worker(ctx, db, stuckWorkflow(attempts, nil))
	if err != nil {
		return err
	}
	ref, err := a.Enqueue(ctx, workflow.Job{Workflow: "thumbnail"})
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "sql: worker A runs attempt", <-attempts)
	hook := workflow.ShutdownHookReport(a, grace, printReport(out, "sql"))
	_ = hook(ctx)

	release := make(chan struct{})
	close(release)
	b, err := worker(ctx, db, stuckWorkflow(attempts, release))
	if err != nil {
		return err
	}
	defer b.Stop()
	fmt.Fprintln(out, "sql: worker B runs attempt", <-attempts)
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(pollInterval) {
		st, err := b.Status(ctx, ref)
		if err != nil {
			return err
		}
		if st.IsTerminal() {
			fmt.Fprintln(out, "sql: run finished:", st)
			return nil
		}
	}
	return fmt.Errorf("the requeued run did not finish")
}

// worker builds, migrates and starts one SQL client with wf.
func worker(ctx context.Context, db *sql.DB, wf workflow.Workflow) (*sqlstore.Client, error) {
	c, err := sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithPollInterval(pollInterval))
	if err != nil {
		return nil, err
	}
	if err := c.Migrate(ctx); err != nil {
		return nil, err
	}
	if err := c.Register(wf); err != nil {
		return nil, err
	}
	if err := c.Start(ctx); err != nil {
		return nil, err
	}
	return c, nil
}
