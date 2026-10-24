// Command quickstart registers a one-step workflow on the in-memory driver,
// starts the workers, enqueues a job and prints what the step returned.
//
// It is the program behind docs/start/quickstart.md: the guide copies its
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
)

// greet is the job's payload; workflow.Define decodes it from JSON.
type greet struct{ Name string }

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "quickstart:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	done := make(chan string, 1)

	// snippet:register
	c := memory.New()
	err := c.Register(workflow.Workflow{
		Name: "greet",
		Steps: []workflow.Step{{
			Name: "say",
			Run: workflow.Define(func(ctx context.Context, in greet) (string, error) {
				done <- "hello " + in.Name
				return "ok", nil
			}),
		}},
	})
	if err != nil {
		return err
	}
	// snippet:end

	// snippet:start
	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // cancelling the Start context stops the workers
	if err := c.Start(ctx); err != nil {
		return err
	}
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "greet", Payload: []byte(`{"Name":"Ada"}`)})
	if err != nil {
		return err
	}
	// snippet:end

	if ref.IsZero() {
		return errors.New("enqueue returned no run reference")
	}
	fmt.Fprintln(out, <-done)
	return nil
}
