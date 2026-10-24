// Command discard-exhausted shows two things about retries on the in-memory
// driver. First, which policy a step gets: Job.Retry beats Step.Retry beats
// Workflow.Retry beats the default of no retries (workflow.DefaultRetryMax).
// Second, what happens to a run that runs out of retries: it ends failed (an
// operator must look), unless the policy sets RetryPolicy.DiscardOnExhaust,
// in which case it ends discarded (giving up is routine) with no OnFailure.
//
// It needs a driver with CapDiscardExhausted; Hatchet refuses the option.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	c := memory.New()

	errDown := errors.New("smtp: connection refused")
	var attempts, alerts atomic.Int64
	failing := func(err error) workflow.Handler {
		return func(context.Context, []byte) ([]byte, error) {
			attempts.Add(1)
			return nil, err
		}
	}
	onFailure := func(context.Context, []byte) ([]byte, error) { alerts.Add(1); return nil, nil }

	// snippet:policies
	// Workflow.Retry is the default for steps that set none: 3 retries.
	// "welcome-mail" gives up quietly; "invoice-mail" keeps the default
	// ending (failed, OnFailure pages someone).
	welcome := workflow.Workflow{
		Name:      "welcome-mail",
		Retry:     workflow.RetryPolicy{Max: 3, DiscardOnExhaust: true},
		Steps:     []workflow.Step{{Name: "send", Run: failing(errDown)}},
		OnFailure: onFailure,
	}
	invoice := workflow.Workflow{
		Name:      "invoice-mail",
		Retry:     workflow.RetryPolicy{Max: 3},
		Steps:     []workflow.Step{{Name: "send", Run: failing(errDown)}},
		OnFailure: onFailure,
	}
	// A step's own Retry beats the workflow's, and RetriesNone() is "never".
	stepOwn := workflow.Workflow{
		Name:  "step-own",
		Retry: workflow.RetryPolicy{Max: 5},
		Steps: []workflow.Step{{Name: "send", Run: failing(errDown),
			Retry: &workflow.RetryPolicy{Max: 1, DiscardOnExhaust: true}}},
	}
	// A Permanent error stops at once; with the option on it is discarded
	// instead of failed.
	permanent := workflow.Workflow{
		Name:  "permanent",
		Retry: workflow.RetryPolicy{Max: 5, DiscardOnExhaust: true},
		Steps: []workflow.Step{{Name: "send", Run: failing(workflow.Permanent(errors.New("mailbox does not exist")))}},
	}
	// snippet:end

	for _, wf := range []workflow.Workflow{welcome, invoice, stepOwn, permanent} {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	ends := make(chan workflow.RunEvent, 8)
	if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { ends <- ev }); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	defer c.Stop()

	report := func(label string, job workflow.Job) error {
		attempts.Store(0)
		alerts.Store(0)
		if _, err := c.Enqueue(ctx, job); err != nil {
			return err
		}
		ev := <-ends
		fmt.Fprintf(out, "%-34s %-9s after %d attempt(s), OnFailure ran %d, ErrDiscarded %v\n",
			label+":", ev.Status, attempts.Load(), alerts.Load(), errors.Is(ev.Err, workflow.ErrDiscarded))
		return nil
	}
	if err := report("workflow policy, DiscardOnExhaust", workflow.Job{Workflow: "welcome-mail"}); err != nil {
		return err
	}
	if err := report("workflow policy, default ending", workflow.Job{Workflow: "invoice-mail"}); err != nil {
		return err
	}
	if err := report("step policy beats workflow", workflow.Job{Workflow: "step-own"}); err != nil {
		return err
	}
	// snippet:job
	// Job.Retry beats both: this dispatch gets no retries and the default ending.
	if err := report("Job.Retry beats step policy", workflow.Job{Workflow: "step-own", Retry: &workflow.RetryPolicy{Max: 0}}); err != nil {
		return err
	}
	// snippet:end
	return report("Permanent with DiscardOnExhaust", workflow.Job{Workflow: "permanent"})
}
