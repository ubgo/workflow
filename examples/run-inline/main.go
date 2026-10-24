// Command run-inline runs a workflow in the caller's goroutine with
// workflow.RunInline: no client, no Start, no
// worker, for a command-line tool, a one-off script or a unit test of a
// whole workflow. The same definition runs on any driver unchanged.
//
// It is the program behind docs/jobs/run-inline.md: the guide copies its
// regions (marked "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ubgo/workflow"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// importUsers is an ordinary workflow: the same value an app registers on
// its driver.
func importUsers() workflow.Workflow {
	attempts := 0
	return workflow.Workflow{
		Name:  "import-users",
		Retry: workflow.RetryPolicy{Max: 2, Backoff: workflow.Backoff{Initial: time.Second}},
		Steps: []workflow.Step{
			{Name: "fetch", Run: func(_ context.Context, in []byte) ([]byte, error) {
				attempts++
				if attempts == 1 {
					return nil, errors.New("CSV host timed out")
				}
				return []byte(`["ada","grace"]`), nil
			}},
			{Name: "store", After: []string{"fetch"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
				return []byte(`"stored ` + string(in) + `"`), nil
			}},
		},
	}
}

// logSteps is a step middleware, the kind an app gives its driver.
func logSteps(out io.Writer) workflow.Middleware {
	return func(next workflow.Handler) workflow.Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			info, _ := workflow.StepInfoFrom(ctx)
			res, err := next(ctx, in)
			fmt.Fprintf(out, "step %s attempt %d: err=%v\n", info.Step, info.Attempt, err)
			return res, err
		}
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	var waited []time.Duration
	// snippet:inline
	res, err := workflow.RunInline(ctx, importUsers(), workflow.Job{Payload: []byte(`"users.csv"`)},
		workflow.InlineMiddleware(logSteps(out)), // the same middleware the driver gets
		workflow.InlineSleep(func(_ context.Context, d time.Duration) error {
			waited = append(waited, d) // a test skips the backoff; the default really waits
			return nil
		}),
	)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "store output: %s\n", res.Outputs["store"])
	// snippet:end
	fmt.Fprintf(out, "attempts: fetch %d, store %d; backoff waits %v\n", res.Attempts["fetch"], res.Attempts["store"], waited)

	// snippet:wait
	approval := workflow.Workflow{Name: "approve", Steps: []workflow.Step{
		{Name: "wait", Wait: &workflow.WaitSpec{Event: "approved", Key: "doc-7"}},
	}}
	_, err = workflow.RunInline(ctx, approval, workflow.Job{})
	fmt.Fprintln(out, "a wait step without InlineEvents is refused:", errors.Is(err, workflow.ErrUnsupported))
	res, err = workflow.RunInline(ctx, approval, workflow.Job{},
		workflow.InlineEvents(workflow.Event{Name: "approved", Key: "doc-7", Payload: []byte(`"by ada"`)}))
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "wait output: %s\n", res.Outputs["wait"])
	// snippet:end
	return nil
}
