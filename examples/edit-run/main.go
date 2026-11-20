// Command edit-run shows workflow.RunEditor: a reminder scheduled for
// tomorrow is fixed before it runs (a new address, a higher priority) and
// moved to now, on the in-memory driver.
//
// It is the program behind docs/jobs/edit-runs.md: the guide copies its
// regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "edit-run:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	c := memory.New()
	sent := make(chan string, 1)
	if err := c.Register(workflow.Workflow{Name: "remind", Steps: []workflow.Step{{Name: "mail", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		sent <- string(in)
		return nil, nil
	}}}}); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "remind", Payload: []byte(`{"to":"old@example.com"}`), RunAt: time.Now().Add(24 * time.Hour)})
	if err != nil {
		return err
	}

	// snippet:edit
	editor, ok := workflow.Client(c).(workflow.RunEditor)
	if !ok {
		return errors.New("this driver cannot edit runs")
	}
	payload := []byte(`{"to":"new@example.com"}`)
	priority := 10
	now := time.Now()
	ref, err = editor.EditRun(ctx, ref, workflow.JobEdit{Payload: &payload, Priority: &priority, RunAt: &now})
	if err != nil {
		return err // ErrWrongState once the run has started
	}
	// snippet:end
	fmt.Println("sent to", <-sent)

	if _, err := workflow.Await(ctx, c, ref, workflow.AwaitOptions{Poll: 2 * time.Millisecond}); err != nil {
		return err
	}
	_, err = editor.EditRun(ctx, ref, workflow.JobEdit{Payload: &payload})
	fmt.Println("editing it after it ran:", errors.Is(err, workflow.ErrWrongState))
	return nil
}
