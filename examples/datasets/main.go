// Command datasets shows data-aware scheduling and asset lineage: two
// loader workflows announce new dataset versions from their steps, an
// audit starts on every new orders version (any of), a revenue report
// starts only once both orders and exchange rates have changed since its
// last run (all of), a retried loader announcing the same version again
// starts nothing, and the lineage store answers which step produced a
// version and which versions a run read. It runs on the in-memory driver,
// so nothing needs to be installed.
//
// It is the program behind docs/events/datasets.md: the guide copies its
// regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/events"
	"github.com/ubgo/workflow/memory"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "datasets:", err)
		os.Exit(1)
	}
}

// await is how the program waits for a run.
var await = workflow.AwaitOptions{Poll: 2 * time.Millisecond}

// The dataset names.
const (
	orders = "warehouse.orders"
	fx     = "warehouse.fx_rates"
)

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	c := memory.New()
	bus, err := events.New(c)
	if err != nil {
		return err
	}
	ds, err := events.NewDatasets(bus)
	if err != nil {
		return err
	}
	noop := func(context.Context, []byte) ([]byte, error) { return nil, nil }
	wfs := []workflow.Workflow{
		// The orders loader's first attempt fails after announcing its
		// version, so its retry announces the same version again.
		{Name: "load-orders", Retry: workflow.RetryPolicy{Max: 1}, Steps: []workflow.Step{{Name: "write", Run: loader(ds, orders, true)}}},
		{Name: "load-fx", Steps: []workflow.Step{{Name: "write", Run: loader(ds, fx, false)}}},
		{Name: "orders-audit", Steps: []workflow.Step{{Name: "audit", Run: noop}}},
		{Name: "revenue-report", Steps: []workflow.Step{{Name: "report", Run: noop}}},
	}
	for _, wf := range wfs {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	// snippet:triggers
	err = errors.Join(
		ds.Trigger(events.DatasetTrigger{Name: "audit", Datasets: []string{orders}, Workflow: "orders-audit"}),
		ds.Trigger(events.DatasetTrigger{Name: "revenue", Datasets: []string{orders, fx}, All: true, Workflow: "revenue-report"}),
	)
	if err != nil {
		return err
	}
	// snippet:end
	if err := c.Start(ctx); err != nil {
		return err
	}

	loads := []struct{ workflow, version string }{
		{"load-orders", "2026-11-20T09"},
		{"load-fx", "2026-11-20T09"},
		{"load-orders", "2026-11-20T10"},
	}
	for _, l := range loads {
		fmt.Printf("%s %s:\n", l.workflow, l.version)
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: l.workflow, Payload: []byte(l.version)})
		if err != nil {
			return err
		}
		if _, err := workflow.Await(ctx, c, ref, await); err != nil {
			return err
		}
	}
	return lineage(ctx, c, ds)
}

// loader is a step that writes dataset (here: does nothing) and announces
// the version in its payload. With flaky, the first attempt of every run
// fails after the announcement, as a step whose later work failed would.
func loader(ds *events.Datasets, dataset string, flaky bool) workflow.Handler {
	return func(ctx context.Context, in []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		// snippet:updated
		res, err := ds.Updated(ctx, dataset, string(in), map[string]string{"rows": "1200"})
		if err != nil {
			return nil, err // retried: announcing the same version again is safe
		}
		// snippet:end
		var started []string
		for _, t := range res.Triggered {
			started = append(started, t.Trigger)
		}
		fmt.Printf("  attempt %d started: %s\n", info.Attempt, orNothing(started))
		if flaky && info.Attempt == 1 {
			return nil, errors.New("manifest upload failed")
		}
		return nil, nil
	}
}

// lineage prints who produced the orders versions and what the revenue
// report read.
func lineage(ctx context.Context, c workflow.Client, ds *events.Datasets) error {
	fmt.Println("lineage:")
	// snippet:lineage
	producers, err := events.Producers(ctx, ds.Lineage(), orders)
	if err != nil {
		return err
	}
	for _, p := range producers {
		fmt.Printf("  %s %s produced by %s/%s\n", p.Dataset, p.Version, p.Workflow, p.Step)
	}
	reads, err := events.Consumers(ctx, ds.Lineage(), fx)
	if err != nil {
		return err
	}
	for _, r := range reads {
		inputs, err := events.Inputs(ctx, ds.Lineage(), r.Run)
		if err != nil {
			return err
		}
		var read []string
		for _, in := range inputs {
			read = append(read, in.Dataset+"@"+in.Version)
		}
		fmt.Printf("  %s run read %s\n", r.Workflow, strings.Join(read, ", "))
	}
	// snippet:end
	return nil
}

func orNothing(names []string) string {
	if len(names) == 0 {
		return "nothing"
	}
	return strings.Join(names, ", ")
}
