// Command run-finished reacts to the end of every run through the
// run-finished hook (workflow.RunNotifier): a run that succeeded, one that
// failed, and a delayed run cancelled before it ever started, which a
// driver with CapRunFinished still reports. It also shows the refusal of
// an observer registered after Start.
//
// hatchet.go holds the same observer on the Hatchet driver, whose delivery
// is best effort; it runs only when HATCHET_CLIENT_TOKEN is set.
//
// It is the program behind docs/observability/run-finished.md: the guide
// copies its regions (marked "snippet:") and task docs:snippets fails when
// they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

var errDeclined = errors.New("card declined")

func main() {
	ctx := context.Background()
	if err := run(ctx, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "run-finished:", err)
		os.Exit(1)
	}
	if os.Getenv(envToken) != "" {
		if err := runHatchet(ctx, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "run-finished on Hatchet:", err)
			os.Exit(1)
		}
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var c workflow.Client = memory.New()
	if err := c.Register(workflow.Workflow{
		Name: "charge",
		Steps: []workflow.Step{{Name: "run", Run: func(_ context.Context, in []byte) ([]byte, error) {
			if string(in) == `"declined"` {
				return nil, workflow.Permanent(errDeclined)
			}
			return nil, nil
		}}},
	}); err != nil {
		return err
	}

	// snippet:observe
	n, ok := c.(workflow.RunNotifier)
	if !ok {
		return errors.New("this driver cannot report run ends; poll Status instead")
	}
	guaranteed := c.Capabilities().Has(workflow.CapRunFinished)
	fmt.Fprintln(out, "every run end is delivered:", guaranteed)

	ends := make(chan workflow.RunEvent, 8)
	err := n.OnRunFinished(func(ctx context.Context, ev workflow.RunEvent) {
		// Runs on a driver goroutine, at least once per run: keep it
		// short and idempotent, and enqueue anything long.
		ends <- ev
	})
	if err != nil {
		return err
	}
	// snippet:end

	if err := c.Start(ctx); err != nil {
		return err
	}
	// snippet:late
	err = n.OnRunFinished(func(context.Context, workflow.RunEvent) {})
	fmt.Fprintln(out, "observer after Start refused:", errors.Is(err, workflow.ErrAlreadyStarted))
	// snippet:end

	print := func(ev workflow.RunEvent) {
		fmt.Fprintf(out, "%s %s order=%s err=%v cancelled=%v\n",
			ev.Workflow, ev.Status, ev.Labels["order"], ev.Err, errors.Is(ev.Err, workflow.ErrCancelled))
	}
	for _, job := range []workflow.Job{
		{Workflow: "charge", Labels: map[string]string{"order": "o-1"}},
		{Workflow: "charge", Payload: []byte(`"declined"`), Labels: map[string]string{"order": "o-2"}},
	} {
		if _, err := c.Enqueue(ctx, job); err != nil {
			return err
		}
		print(<-ends)
	}

	// A delayed run cancelled before it fired never started, and is still
	// reported.
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "charge", RunAt: time.Now().Add(time.Hour), Labels: map[string]string{"order": "o-3"}})
	if err != nil {
		return err
	}
	if err := c.Cancel(ctx, ref); err != nil {
		return err
	}
	print(<-ends)
	return nil
}
