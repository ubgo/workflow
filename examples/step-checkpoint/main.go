// Command step-checkpoint pages through an upstream API in one step and
// saves the next page's cursor after every page, so the retry after a
// failure on page 3 resumes at page 3 instead of fetching pages 1 and 2
// again. The checkpoint is typed (DefineCheckpoint) and is dropped when the
// run ends.
//
// It is the program behind docs/jobs/checkpoints.md: the guide copies its
// regions (marked "snippet:") and task docs:snippets fails when they drift.
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
		fmt.Fprintln(os.Stderr, "step-checkpoint:", err)
		os.Exit(1)
	}
}

// errUpstream is the transient failure the fake upstream returns once.
var errUpstream = errors.New("upstream: 503")

// snippet:cursor
// cursor is where the export stands: the next page to fetch.
type cursor struct {
	Page int `json:"page"`
}

// progress is the step's checkpoint, typed.
var progress = workflow.DefineCheckpoint[cursor]()

// snippet:end

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	const lastPage = 4
	failed := false
	// snippet:step
	export := func(ctx context.Context, _ []byte) ([]byte, error) {
		at, ok, err := progress.Load(ctx)
		if err != nil {
			return nil, err
		}
		if !ok {
			at = cursor{Page: 1} // the first attempt starts at the beginning
		}
		info, _ := workflow.StepInfoFrom(ctx)
		fmt.Printf("attempt %d starts at page %d\n", info.Attempt, at.Page)
		for ; at.Page <= lastPage; at.Page++ {
			if at.Page == 3 && !failed {
				failed = true
				return nil, errUpstream // retried; the cursor says page 3
			}
			fmt.Println("fetched page", at.Page)
			if err := progress.Save(ctx, cursor{Page: at.Page + 1}); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}
	// snippet:end

	c := memory.New(memory.WithMaxCheckpoint(1024))
	if err := c.Register(workflow.Workflow{
		Name:  "export",
		Steps: []workflow.Step{{Name: "pages", Run: export}},
		Retry: workflow.RetryPolicy{Max: 3, Backoff: workflow.Backoff{Initial: 10 * time.Millisecond}},
	}); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := c.Start(ctx); err != nil {
		return err
	}
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "export"})
	if err != nil {
		return err
	}
	st, err := workflow.Await(ctx, c, ref, workflow.AwaitOptions{Poll: 5 * time.Millisecond})
	if err != nil {
		return err
	}
	fmt.Println("the run:", st)
	return nil
}
