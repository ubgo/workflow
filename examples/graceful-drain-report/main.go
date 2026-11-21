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
	"errors"
	"fmt"
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

const (
	// attemptsBuffer is the room on the attempts channel: each run sends one
	// value per attempt and the program sees at most two, so four never
	// blocks a step on a slow reader.
	attemptsBuffer = 4
	// finishTimeout bounds the wait for the requeued run to finish on worker
	// B; it finishes in a few polls, so a longer wait means it is stuck.
	finishTimeout = 5 * time.Second
)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "graceful-drain-report:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	if err := inMemory(ctx); err != nil {
		return err
	}
	return durable(ctx)
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
//
// snippet:report
func printReport(label string) func(workflow.DrainReport, bool) {
	// next says what becomes of a cut-off attempt; DrainDisposition is a
	// closed set, so every value has an entry.
	next := map[workflow.DrainDisposition]string{
		workflow.DrainRequeued:    "another worker runs it as the same attempt",
		workflow.DrainCancelled:   "the run is cancelled",
		workflow.DrainEngineOwned: "the engine decides",
	}
	return func(r workflow.DrainReport, detailed bool) {
		if !detailed { // no CapDrainReport: the report is unknown, not clean
			fmt.Printf("%s: the driver cannot say what a drain cut off\n", label)
			return
		}
		fmt.Printf("%s: finished steps=%d runs=%d, clean=%t, cut off %d attempt(s) in %d run(s)\n",
			label, r.FinishedSteps, r.FinishedRuns, r.Clean(), len(r.Cut), r.CutRuns())
		for _, c := range r.Cut {
			fmt.Printf("  cut: %s/%s attempt %d -> %s: %s\n", c.Workflow, c.Step, c.Attempt, c.Disposition, next[c.Disposition])
		}
	}
}

// snippet:end

// inMemory drains a memory client through ShutdownHookReport while a step
// is stuck, and shows the cut run ends cancelled.
func inMemory(ctx context.Context) error {
	attempts := make(chan int, attemptsBuffer)
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
	hook := workflow.ShutdownHookReport(c, grace, printReport("memory"))
	err = hook(ctx)
	// snippet:end
	fmt.Println("memory: drain error is a deadline:", err == context.DeadlineExceeded)
	st, err := c.Status(ctx, ref)
	if err != nil {
		return err
	}
	fmt.Println("memory: cut run status:", st)
	return nil
}

// durable drains a SQL worker by hand while a step is stuck, then shows a
// second worker running the requeued run as the same attempt number.
func durable(ctx context.Context) (err error) {
	dir, err := os.MkdirTemp("", "graceful-drain-report-")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(dir)) }()
	db, err := driversqlite.Open(filepath.Join(dir, "jobs.db"))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close()) }()

	attempts := make(chan int, attemptsBuffer)
	a, err := worker(ctx, db, stuckWorkflow(attempts, nil))
	if err != nil {
		return err
	}
	ref, err := a.Enqueue(ctx, workflow.Job{Workflow: "thumbnail"})
	if err != nil {
		return err
	}
	fmt.Println("sql: worker A runs attempt", <-attempts)

	// snippet:caps
	caps := a.Capabilities()
	fmt.Printf("sql: drain report %t, requeue on a clean stop %t\n",
		caps.Has(workflow.CapDrainReport), caps.Has(workflow.CapShutdownRequeue))
	// snippet:end

	// snippet:drain-with-report
	// What ShutdownHookReport does, by hand: a grace period on the ctx,
	// then DrainWithReport when the client is a ReportingDrainer.
	dctx, cancel := workflow.DrainContext(ctx, grace) // zero or less: no waiting at all
	var report workflow.DrainReport
	rd, detailed := any(a).(workflow.ReportingDrainer)
	if detailed {
		report, err = rd.DrainWithReport(dctx)
	} else {
		err = a.Drain(dctx) // it still drains; the report stays unknown
	}
	cancel()
	printReport("sql")(report, detailed)
	// Drain's error is DrainOutcome: nil when nothing was cut off, else the ctx's error.
	fmt.Println("sql: drain error is the deadline:", errors.Is(err, context.DeadlineExceeded),
		"same as DrainOutcome:", err == workflow.DrainOutcome(report, context.DeadlineExceeded))
	// snippet:end

	release := make(chan struct{})
	close(release)
	b, err := worker(ctx, db, stuckWorkflow(attempts, release))
	if err != nil {
		return err
	}
	defer b.Stop()
	fmt.Println("sql: worker B runs attempt", <-attempts)
	for deadline := time.Now().Add(finishTimeout); time.Now().Before(deadline); time.Sleep(pollInterval) {
		st, err := b.Status(ctx, ref)
		if err != nil {
			return err
		}
		if st.IsTerminal() {
			fmt.Println("sql: run finished:", st)
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
