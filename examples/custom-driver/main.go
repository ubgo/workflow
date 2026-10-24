// Command custom-driver runs a workflow on Tiny, a minimal driver written
// in driver.go: under 300 lines that implement workflow.Client
// with no capability beyond the core contract and pass the conformance
// suite (main_test.go runs it).
//
// It is the program behind docs/extending/writing-a-driver.md: the guide
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
	"github.com/ubgo/workflow/clock"
)

// statusPoll is how often waitTerminal re-reads a run's status: short, so
// the demo finishes quickly; a real app waits on the run-finished hook.
const statusPoll = 5 * time.Millisecond

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "custom-driver:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// snippet:use
	c := NewTiny(clock.Real{})
	err := c.Register(workflow.Workflow{
		Name: "report",
		Steps: []workflow.Step{
			{Name: "fetch", Run: func(ctx context.Context, in []byte) ([]byte, error) {
				return []byte(`"42 rows"`), nil
			}},
			{Name: "render", After: []string{"fetch"}, Run: func(ctx context.Context, in []byte) ([]byte, error) {
				fmt.Fprintf(out, "render got %s\n", in)
				return nil, nil
			}},
		},
	})
	if err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "report"})
	if err != nil {
		return err
	}
	// snippet:end

	st, err := waitTerminal(ctx, c, ref)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "%s: %s\n", ref.RunID, st)

	// snippet:refused
	_, err = c.Enqueue(ctx, workflow.Job{Workflow: "report", RunAt: time.Now().Add(time.Hour)})
	fmt.Fprintln(out, "delayed start refused:", errors.Is(err, workflow.ErrUnsupported))
	fmt.Fprintln(out, err)
	// snippet:end
	return nil
}

// waitTerminal polls Status until the run has finished.
func waitTerminal(ctx context.Context, c workflow.Client, ref workflow.RunRef) (workflow.Status, error) {
	for {
		st, err := c.Status(ctx, ref)
		if err != nil || st.IsTerminal() {
			return st, err
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(statusPoll):
		}
	}
}
