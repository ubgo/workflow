// Command rolling-deploy shows workflow versions during a rolling deploy
// (LO-11): old and new workers share one database while the new code
// changes a workflow. A run started by the new code never reaches an old
// worker, which would run it with the old steps; it waits until a new
// worker takes it, and the new worker also drains the runs the old code
// started, branching on StepInfo.Version where the two differ. Two SQL
// clients over one SQLite file stand for the old and the new process.
//
// It is the program behind the "Deploy a new version while old workers
// run" section of docs/operations/deploy.md: the guide copies its regions
// (marked "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/ubgo/sqlb"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/sqlstore"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "rolling-deploy:", err)
		os.Exit(1)
	}
}

// journal records which process ran which step for which run version.
type journal struct {
	mu    sync.Mutex
	lines []string
}

func (j *journal) add(format string, args ...any) {
	j.mu.Lock()
	j.lines = append(j.lines, fmt.Sprintf(format, args...))
	j.mu.Unlock()
}

// v1 is the old definition: charge, then email.
func v1(j *journal) workflow.Workflow {
	return workflow.Workflow{Name: "checkout", Version: 1, Steps: []workflow.Step{
		{Name: "charge", Run: func(ctx context.Context, in []byte) ([]byte, error) {
			j.add("old: charge (run v%d)", version(ctx))
			return in, nil
		}},
		{Name: "email", After: []string{"charge"}, Run: func(ctx context.Context, in []byte) ([]byte, error) {
			j.add("old: email (run v%d)", version(ctx))
			return in, nil
		}},
	}}
}

// v2 is the new definition: a fraud check before the charge. It still
// runs version 1 runs (MinVersion 0) with its own steps, so the steps
// whose behaviour changed branch on the run's version.
func v2(j *journal) workflow.Workflow {
	// snippet:v2
	return workflow.Workflow{Name: "checkout", Version: 2, Steps: []workflow.Step{
		{Name: "fraud", Run: func(ctx context.Context, in []byte) ([]byte, error) {
			if info, _ := workflow.StepInfoFrom(ctx); info.Version < 2 {
				j.add("new: no fraud check for a run the old code started (run v%d)", info.Version)
				return in, nil // the old checkout already took the order
			}
			j.add("new: fraud check (run v%d)", version(ctx))
			return in, nil
		}},
		{Name: "charge", After: []string{"fraud"}, Run: func(ctx context.Context, in []byte) ([]byte, error) {
			j.add("new: charge (run v%d)", version(ctx))
			return in, nil
		}},
		{Name: "email", After: []string{"charge"}, Run: func(ctx context.Context, in []byte) ([]byte, error) {
			if info, _ := workflow.StepInfoFrom(ctx); info.Version < 2 {
				j.add("new: plain email for a run the old code started (run v%d)", info.Version)
				return in, nil
			}
			j.add("new: email with the fraud score (run v%d)", version(ctx))
			return in, nil
		}},
	}}
	// snippet:end
}

func version(ctx context.Context) int {
	info, _ := workflow.StepInfoFrom(ctx)
	return info.Version
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	dir, err := os.MkdirTemp("", "rolling-deploy-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	db, err := driversqlite.Open(filepath.Join(dir, "jobs.db"))
	if err != nil {
		return err
	}
	defer db.Close()
	j := &journal{}
	start := func(wf workflow.Workflow, opts ...sqlstore.Option) (*sqlstore.Client, error) {
		c, err := sqlstore.New(db, sqlb.SQLiteDialect{}, append([]sqlstore.Option{sqlstore.WithPollInterval(5 * time.Millisecond)}, opts...)...)
		if err != nil {
			return nil, err
		}
		if err := c.Migrate(ctx); err != nil {
			return nil, err
		}
		if err := c.Register(wf); err != nil {
			return nil, err
		}
		return c, c.Start(ctx)
	}
	wait := func(c *sqlstore.Client, ref workflow.RunRef) error {
		_, err := workflow.AwaitResult(ctx, c, ref, workflow.AwaitOptions{Poll: 5 * time.Millisecond})
		return err
	}

	// The old worker is running and has a run in flight... (enqueued by
	// an old web node that does not run work).
	old, err := start(v1(j))
	if err != nil {
		return err
	}
	oldWeb, err := start(v1(j), sqlstore.WithEnqueueOnly())
	if err != nil {
		return err
	}
	fmt.Println("old worker up; old web node enqueues one run")
	first, err := oldWeb.Enqueue(ctx, workflow.Job{Workflow: "checkout"})
	if err != nil {
		return err
	}
	if err := wait(old, first); err != nil {
		return err
	}

	// ...then the new web node ships first and enqueues with v2.
	newWeb, err := start(v2(j), sqlstore.WithEnqueueOnly())
	if err != nil {
		return err
	}
	fmt.Println("new web node enqueues one run; the old worker leaves it alone")
	second, err := newWeb.Enqueue(ctx, workflow.Job{Workflow: "checkout"})
	if err != nil {
		return err
	}
	time.Sleep(100 * time.Millisecond)
	// snippet:waiting
	st, err := old.Status(ctx, second)
	if err != nil {
		return err
	}
	fmt.Println("  the v2 run on a fleet of v1 workers:", st) // queued until a v2 worker takes it
	// snippet:end

	// An old web node still enqueues a v1 run during the deploy.
	third, err := oldWeb.Enqueue(ctx, workflow.Job{Workflow: "checkout"})
	if err != nil {
		return err
	}
	old.Stop() // the old worker is replaced before it takes the v1 run
	_ = third

	fmt.Println("new worker up: runs the v2 run and drains the v1 run")
	newer, err := start(v2(j))
	if err != nil {
		return err
	}
	defer newer.Stop()
	for _, ref := range []workflow.RunRef{second, third} {
		if err := wait(newer, ref); err != nil {
			return err
		}
	}
	page, err := newer.Runs(ctx, workflow.RunQuery{Workflow: "checkout"})
	if err != nil {
		return err
	}
	var versions []int
	for _, r := range page.Runs {
		versions = append(versions, r.Version)
	}
	sort.Ints(versions)
	fmt.Println("run versions:", versions)
	fmt.Println("journal (sorted; the two runs interleave):")
	sort.Strings(j.lines)
	for _, line := range j.lines {
		fmt.Println(" ", line)
	}
	return nil
}
