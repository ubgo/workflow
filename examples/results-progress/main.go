// Command results-progress shows a long export that reports how far it
// has got and returns a result the caller waits for: ReportProgress from
// inside the step, progress events and RunInfo.Progress outside it,
// AwaitResult and DecodePayload for the result, the result of a workflow
// with two final steps, a run that fails while someone waits on it, and
// runs filtered by tenant.
//
// It runs on the in-memory driver with a fake clock, so every timestamp is
// fixed and the output is the same on every run. It is the program behind
// docs/jobs/progress.md, docs/jobs/results.md and docs/jobs/tenants.md:
// the guides copy its regions (marked "snippet:") and task docs:snippets
// fails when they drift.
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
)

// start is the fake clock's first instant; any fixed time works.
var start = time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC)

const (
	// awaitPoll is how often the program looks at a run it waits for: the
	// in-memory driver ends a run in microseconds.
	awaitPoll = 10 * time.Millisecond
	// progressReports is how many progress events the export sends: its
	// total over its batch size.
	progressReports = 3
)

// await is how the program waits for a run outside the guide's regions.
var await = workflow.AwaitOptions{Poll: awaitPoll}

// ExportResult is what the export step returns: the run's result.
type ExportResult struct {
	File string `json:"file"`
	Rows int    `json:"rows"`
}

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "results-progress:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	clk := clock.NewFake(start)

	// snippet:report
	export := workflow.Define(func(ctx context.Context, _ struct{}) (ExportResult, error) {
		const total, batch = 3000, 1000 // orders to export, and how many per round
		for done := batch; done <= total; done += batch {
			// ... export the next 1,000 orders ...
			err := workflow.ReportProgress(ctx, workflow.Progress{
				Percent: 100 * float64(done) / total, // a share from 0 to 100
				Message: fmt.Sprintf("exported %d of %d orders", done, total),
			})
			if err != nil && !errors.Is(err, workflow.ErrUnsupported) {
				return ExportResult{}, err // the run ended under us
			}
		}
		return ExportResult{File: "orders.csv", Rows: total}, nil
	})
	// snippet:end

	// ok returns a step that answers with the JSON s.
	ok := func(s string) workflow.Handler {
		return func(context.Context, []byte) ([]byte, error) { return []byte(s), nil }
	}
	// snippet:sinks
	checkout := workflow.Workflow{
		Name: "checkout",
		Steps: []workflow.Step{
			{Name: "charge", Run: ok(`"ch_1"`)},
			{Name: "email", After: []string{"charge"}, Run: ok(`"sent"`)},    // a final step
			{Name: "invoice", After: []string{"charge"}, Run: ok(`"inv_7"`)}, // a final step
		},
	}
	// snippet:end
	// refuse is a step that always fails for good.
	refuse := func(context.Context, []byte) ([]byte, error) {
		return nil, workflow.Permanent(errors.New("card declined"))
	}

	c := memory.New(memory.WithClock(clk))
	// snippet:events
	progress := make(chan workflow.Progress, 8) // room for every report: the observer must not block
	err := c.OnLifecycle(func(_ context.Context, ev workflow.LifecycleEvent) {
		if ev.Kind == workflow.LifecycleProgress {
			progress <- *ev.Progress // a page could push this to the browser
		}
	})
	// snippet:end
	if err != nil {
		return err
	}
	for _, wf := range []workflow.Workflow{
		{Name: "export-orders", Steps: []workflow.Step{{Name: "export", Run: export}}},
		checkout,
		{Name: "pay", Steps: []workflow.Step{{Name: "charge", Run: refuse}}},
	} {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	defer c.Stop()

	fmt.Println("progress:")
	// snippet:await
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "export-orders", Tenant: "acme"})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute) // how long the caller will wait
	defer cancel()
	raw, err := workflow.AwaitResult(ctx, c, ref, workflow.AwaitOptions{Poll: 10 * time.Millisecond}) // how often to look
	if err != nil {
		return err
	}
	res, err := workflow.DecodePayload[ExportResult](raw)
	if err != nil {
		return err
	}
	// snippet:end
	for range progressReports {
		p := <-progress
		fmt.Printf("  %3.0f%% %s (step %s, at %s)\n", p.Percent, p.Message, p.Step, p.At.Format("15:04"))
	}
	// snippet:latest
	page, err := c.Runs(ctx, workflow.RunQuery{Ref: ref})
	if err != nil {
		return err
	}
	if p := page.Runs[0].Progress; p != nil { // nil until a step reports
		fmt.Printf("  latest: %.0f%% %s\n", p.Percent, p.Message)
	}
	// snippet:end
	fmt.Printf("  outside a step: unsupported %t\n", errors.Is(workflow.ReportProgress(ctx, workflow.Progress{Percent: 1}), workflow.ErrUnsupported))

	fmt.Println("result:")
	fmt.Printf("  %s\n  decoded: %s, %d rows\n", raw, res.File, res.Rows)

	ref2, err := c.Enqueue(ctx, workflow.Job{Workflow: "checkout"})
	if err != nil {
		return err
	}
	if _, err := workflow.Await(ctx, c, ref2, await); err != nil {
		return err
	}
	// snippet:read-result
	reader, isReader := workflow.Client(c).(workflow.ResultReader) // CapResult
	if !isReader {
		return errors.New("this driver keeps no results")
	}
	raw2, err := reader.Result(ctx, ref2) // ErrWrongState unless the run succeeded
	if err != nil {
		return err
	}
	fmt.Printf("  checkout, final steps %v: %s\n", workflow.Sinks(checkout), raw2)

	listed, err := c.Runs(ctx, workflow.RunQuery{Ref: ref2})
	if err != nil {
		return err
	}
	fmt.Printf("  RunInfo.Output is the same: %t\n", bytes.Equal(listed.Runs[0].Output, raw2))

	// RunOutput is the rule the driver applied to the step outputs.
	rule := workflow.RunOutput(checkout, map[string][]byte{
		"charge": []byte(`"ch_1"`), "email": []byte(`"sent"`), "invoice": []byte(`"inv_7"`),
	})
	fmt.Printf("  RunOutput gives the same: %t\n", bytes.Equal(rule, raw2))
	// snippet:end

	ref3, err := c.Enqueue(ctx, workflow.Job{Workflow: "pay"})
	if err != nil {
		return err
	}
	// snippet:failed
	_, err = workflow.AwaitResult(ctx, c, ref3, workflow.AwaitOptions{Poll: 10 * time.Millisecond}) // how often to look
	var failed *workflow.RunFailedError
	if errors.As(err, &failed) {
		fmt.Printf("  pay: ended %s: %s\n", failed.Status, failed.Err)
	}
	// snippet:end
	_, err = c.Result(ctx, ref3)
	fmt.Printf("  Result of a failed run: ErrWrongState %t\n", errors.Is(err, workflow.ErrWrongState))

	return tenants(ctx, c)
}

// tenants enqueues runs for two tenants and lists one tenant's.
func tenants(ctx context.Context, c *memory.Client) error {
	fmt.Println("tenants:")
	for _, tenant := range []string{"globex", "acme"} {
		// snippet:tenant
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "checkout", Tenant: tenant})
		if err != nil {
			return err
		}
		// snippet:end
		if _, err := workflow.Await(ctx, c, ref, await); err != nil {
			return err
		}
	}
	// snippet:tenant-schedule
	id, err := c.Schedule(ctx, workflow.Schedule{
		Name:     "acme-nightly",
		Workflow: "checkout",
		Cron:     "0 2 * * *",
		Location: time.UTC,
		Tenant:   "acme", // every run it fires belongs to acme
		Labels:   map[string]string{"source": "schedule"},
	})
	if err != nil {
		return err
	}
	// snippet:end
	fired, err := c.TriggerSchedule(ctx, id) // fire it now instead of waiting for 02:00
	if err != nil {
		return err
	}
	if _, err := workflow.Await(ctx, c, fired, await); err != nil {
		return err
	}
	// snippet:tenant-query
	page, err := c.Runs(ctx, workflow.RunQuery{Tenant: "acme"})
	if err != nil {
		return err
	}
	for _, r := range page.Runs {
		fmt.Printf("  %s %s %s", r.Tenant, r.Workflow, r.Status)
		if r.Labels["source"] == "schedule" {
			fmt.Print(" (fired by acme-nightly)")
		}
		fmt.Println()
	}
	// snippet:end
	_, err = c.Enqueue(ctx, workflow.Job{Workflow: "checkout", Tenant: "acme corp"})
	fmt.Printf("  tenant with a space: ErrInvalid %t\n", errors.Is(err, workflow.ErrInvalid))
	return nil
}
