// Command route-jobs shows middleware.Route, an app-side routing table in
// enqueue middleware: bulk invoices go to a workflow of their own, every
// job gets a cost-center label, and a label the enqueue site chose wins
// over the table. It runs on the in-memory driver and reads each run back
// (RunQuerier) to print the workflow and labels it was enqueued with.
//
// It is the program behind the Route jobs section of
// docs/middleware/enqueue-middleware.md: the guide copies its regions
// (marked "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

// Workflow names: the code that enqueues knows only invoiceWorkflow; the
// routing table sends bulk jobs to bulkInvoiceWorkflow.
const (
	invoiceWorkflow     = "invoice"
	bulkInvoiceWorkflow = "invoice-bulk"
)

// costCenterLabel is the label the table adds to every job, so the admin
// and the label search can split runs by who pays for them.
const costCenterLabel = "cost-center"

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "route-jobs:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code
// and reads what it wrote to w.
func run(ctx context.Context, w io.Writer) error {
	mc := memory.New()
	// noop is the step body of both workflows: the routing is the point here.
	noop := func(context.Context, []byte) ([]byte, error) { return nil, nil }
	for _, name := range []string{invoiceWorkflow, bulkInvoiceWorkflow} {
		if err := mc.Register(workflow.Workflow{Name: name, Steps: []workflow.Step{{Name: "render", Run: noop}}}); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := mc.Start(ctx); err != nil {
		return err
	}

	// snippet:route
	// isBulk matches the plain invoice jobs the enqueue site marked as bulk.
	isBulk := func(j workflow.Job) bool {
		return j.Workflow == invoiceWorkflow && j.Labels["tier"] == "bulk"
	}
	c := middleware.WrapClient(mc, middleware.Route(
		// The first rule whose Match accepts the job applies; the rest
		// are skipped.
		middleware.RouteRule{Match: isBulk, Workflow: bulkInvoiceWorkflow, Labels: map[string]string{costCenterLabel: "batch"}},
		// A nil Match accepts every job: the default for the rest.
		middleware.RouteRule{Labels: map[string]string{costCenterLabel: "web"}},
	))
	// snippet:end

	// snippet:enqueue
	jobs := []workflow.Job{
		{Workflow: invoiceWorkflow, Labels: map[string]string{"tier": "bulk"}},
		{Workflow: invoiceWorkflow},
		{Workflow: invoiceWorkflow, Labels: map[string]string{costCenterLabel: "sales"}}, // chosen here: wins
	}
	for _, job := range jobs {
		ref, err := c.Enqueue(ctx, job)
		if err != nil {
			return err
		}
		page, err := mc.Runs(ctx, workflow.RunQuery{Ref: ref})
		if err != nil {
			return err
		}
		got := page.Runs[0]
		fmt.Fprintf(w, "job %s %v -> run of %s, %s=%s\n", job.Workflow, job.Labels, got.Workflow, costCenterLabel, got.Labels[costCenterLabel])
	}
	// snippet:end
	fmt.Fprintf(w, "the caller's labels are not changed: %v\n", jobs[0].Labels)
	return nil
}
