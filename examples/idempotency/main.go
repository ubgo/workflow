// Command idempotency enqueues the same job twice with one idempotency key
// and gets one run, shows that the key is free again once the run has
// finished, and shows the three per-workflow policies: optional, required
// and off. It ends with a client that cannot honour keys, which refuses
// them instead of dropping them.
//
// It is the program behind docs/jobs/idempotency.md: the guide copies its
// regions (marked "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/workflowtest"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "idempotency:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	release := make(chan struct{})
	finished := make(chan workflow.RunEvent, 8)
	noop := func(context.Context, []byte) ([]byte, error) { return nil, nil }

	c := memory.New()
	if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { finished <- ev }); err != nil {
		return err
	}
	// snippet:policies
	workflows := []workflow.Workflow{
		{
			Name: "invoice", // Idempotency empty: IdempotencyOptional, a key is honoured when given
			Steps: []workflow.Step{{Name: "send", Run: func(context.Context, []byte) ([]byte, error) {
				<-release // keep the run unfinished until the program says so
				return nil, nil
			}}},
		},
		{
			Name:        "payout",
			Idempotency: workflow.IdempotencyRequired, // an enqueue without a key fails
			Steps:       []workflow.Step{{Name: "pay", Run: noop}},
		},
		{
			Name:        "ping",
			Idempotency: workflow.IdempotencyOff, // keys are ignored: every enqueue is a run
			Steps:       []workflow.Step{{Name: "ping", Run: noop}},
		},
	}
	// snippet:end
	for _, wf := range workflows {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer c.Stop()
	defer cancel()
	if err := c.Start(ctx); err != nil {
		return err
	}

	// snippet:key
	job := workflow.Job{Workflow: "invoice", IdempotencyKey: "invoice:order-42"}
	first, err := c.Enqueue(ctx, job)
	if err != nil {
		return err
	}
	second, err := c.Enqueue(ctx, job) // a retried HTTP request, a redelivered message
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "second enqueue returned the first run:", second == first)
	// snippet:end

	close(release)
	<-finished

	// snippet:released
	third, err := c.Enqueue(ctx, job) // the first run has finished: the key is free
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "after the run finished, a new run:", third != first)
	// snippet:end
	<-finished

	// snippet:required
	_, err = c.Enqueue(ctx, workflow.Job{Workflow: "payout"})
	fmt.Fprintln(out, "payout without a key:", errors.Is(err, workflow.ErrInvalid))
	// snippet:end

	// snippet:off
	a, err := c.Enqueue(ctx, workflow.Job{Workflow: "ping", IdempotencyKey: "same"})
	if err != nil {
		return err
	}
	b, err := c.Enqueue(ctx, workflow.Job{Workflow: "ping", IdempotencyKey: "same"})
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "ping with the same key twice, two runs:", a != b)
	// snippet:end
	<-finished
	<-finished

	return refusals(ctx, out)
}

// refusals uses the workflowtest Fake, told to claim every capability but
// idempotency keys, to stand for a driver without them.
func refusals(ctx context.Context, out io.Writer) error {
	// snippet:refusal
	all := memory.New().Capabilities()
	f := workflowtest.NewFake(workflowtest.FakeCaps(all &^ workflow.CapIdempotencyKey))
	noop := func(context.Context, []byte) ([]byte, error) { return nil, nil }
	if err := f.Register(workflow.Workflow{Name: "invoice", Steps: []workflow.Step{{Name: "send", Run: noop}}}); err != nil {
		return err
	}
	if err := f.Start(ctx); err != nil {
		return err
	}
	_, err := f.Enqueue(ctx, workflow.Job{Workflow: "invoice", IdempotencyKey: "invoice:order-42"})
	fmt.Fprintln(out, "no CapIdempotencyKey:", err)
	// snippet:end
	return nil
}
