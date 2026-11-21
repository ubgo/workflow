// Command queue-guards shows the counting functions behind
// middleware.Bounded and middleware.Quota written by the app: a DepthFunc
// that bounds each workflow's waiting runs instead of each queue's, and a
// CountFunc that reads a tenant's open uploads from the app's own table.
// It runs on the in-memory driver.
//
// It is the program behind the "Bound a queue and cap a tenant" section
// of docs/jobs/guards.md: the guide copies its regions (marked
// "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

// Workflow names: exportWorkflow is bounded, reportWorkflow is not.
const (
	exportWorkflow = "export"
	reportWorkflow = "report"
)

// maxWaitingExports is how many export runs may wait at once, across
// every queue: each one holds a large file in memory when it starts.
const maxWaitingExports = 2

// maxOpenUploads is how many open uploads one tenant may have.
const maxOpenUploads = 5

// startLater keeps the example's runs waiting (StatusScheduled), so they
// count as waiting for as long as the program runs.
const startLater = time.Hour

// errTableDown is what the uploads table returns while it is unreachable.
var errTableDown = errors.New("uploads table unreachable")

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "queue-guards:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code
// and reads what it wrote to w.
func run(ctx context.Context, w io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	mc := memory.New()
	// noop is every workflow's one step: the runs only need to wait.
	noop := func(context.Context, []byte) ([]byte, error) { return nil, nil }
	for _, name := range []string{exportWorkflow, reportWorkflow} {
		if err := mc.Register(workflow.Workflow{Name: name, Steps: []workflow.Step{{Name: "run", Run: noop}}}); err != nil {
			return err
		}
	}
	if err := mc.Start(ctx); err != nil {
		return err
	}

	// snippet:depth
	// Waiting runs of the job's workflow, whatever queue they are on: the
	// stock DepthFromRuns counts per queue instead.
	perWorkflow := middleware.DepthFunc(func(ctx context.Context, job workflow.Job) (int, error) {
		page, err := mc.Runs(ctx, workflow.RunQuery{
			Workflow: job.Workflow,
			Statuses: []workflow.Status{workflow.StatusQueued, workflow.StatusScheduled},
			Limit:    workflow.MaxRunsLimit,
		})
		if err != nil {
			return 0, err // the enqueue fails: a bound that cannot be checked is not let through
		}
		return len(page.Runs), nil
	})
	// limit is the bound per job: only export is bounded.
	limit := func(job workflow.Job) int {
		if job.Workflow == exportWorkflow {
			return maxWaitingExports
		}
		return 0 // no bound: the job passes without a count
	}
	// snippet:end

	// snippet:count
	// Open uploads per tenant, from a table the app keeps (a SQL count in
	// production); Quota never asks the driver.
	openUploads := map[string]int{"acme": maxOpenUploads, "globex": 1}
	var tableUp atomic.Bool
	tableUp.Store(true)
	fromTable := middleware.CountFunc(func(_ context.Context, job workflow.Job) (int, error) {
		if !tableUp.Load() {
			return 0, errTableDown
		}
		return openUploads[job.Tenant], nil
	})

	c := middleware.WrapClient(mc,
		middleware.Quota(fromTable, maxOpenUploads),
		middleware.Bounded(perWorkflow, limit),
	)
	// snippet:end

	// enqueue enqueues job to start later, so it stays waiting, and prints
	// whether a guard refused it.
	enqueue := func(what string, job workflow.Job) {
		job.RunAt = time.Now().Add(startLater)
		_, err := c.Enqueue(ctx, job)
		switch {
		case err == nil:
			fmt.Fprintf(w, "%s: accepted\n", what)
		case errors.Is(err, middleware.ErrQueueFull):
			fmt.Fprintf(w, "%s: refused, queue full (%v)\n", what, err)
		case errors.Is(err, middleware.ErrQuotaExceeded):
			fmt.Fprintf(w, "%s: refused, quota (%v)\n", what, err)
		default:
			fmt.Fprintf(w, "%s: failed (%v)\n", what, err)
		}
	}

	for i := 1; i <= maxWaitingExports+1; i++ {
		enqueue(fmt.Sprintf("globex export %d", i), workflow.Job{Workflow: exportWorkflow, Tenant: "globex"})
	}
	enqueue("globex report", workflow.Job{Workflow: reportWorkflow, Tenant: "globex"})
	enqueue("acme report", workflow.Job{Workflow: reportWorkflow, Tenant: "acme"})
	enqueue("report without a tenant", workflow.Job{Workflow: reportWorkflow})
	tableUp.Store(false)
	enqueue("globex report, table down", workflow.Job{Workflow: reportWorkflow, Tenant: "globex"})
	return nil
}
