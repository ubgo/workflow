// Command flow-children shows child runs started from inside a step:
// Spawn, which starts jobs as runs of their own and moves on, and
// AwaitChildren, which starts them and puts the parent step back until
// every child has ended, then returns their results in order. It runs on
// the in-memory driver, so nothing needs to be installed.
//
// It is the program behind docs/workflows/children.md: the guide copies
// its regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "flow-children:", err)
		os.Exit(1)
	}
}

// await is how the program waits for a run.
var await = workflow.AwaitOptions{Poll: 2 * time.Millisecond}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	c, err := workflow.Use(memory.New(), workflow.Journal(workflow.NewMemoryJournal()))
	if err != nil {
		return err
	}
	var notified atomic.Int32
	wfs := []workflow.Workflow{
		{Name: "notify", Steps: []workflow.Step{{Name: "send", Run: func(context.Context, []byte) ([]byte, error) {
			notified.Add(1)
			return nil, nil
		}}}},
		{Name: "resize", Steps: []workflow.Step{{Name: "resize", Run: resize}}},
		signupWorkflow(c),
		albumWorkflow(c),
	}
	for _, wf := range wfs {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	if err := spawn(ctx, c, out, &notified); err != nil {
		return err
	}
	return awaitChildren(ctx, c, out)
}

// resize is the child workflow's step: it fails for a file named bad.png.
func resize(_ context.Context, in []byte) ([]byte, error) {
	var name string
	if err := json.Unmarshal(in, &name); err != nil {
		return nil, workflow.Permanent(err)
	}
	if name == "bad.png" {
		return nil, workflow.Permanent(errors.New("not an image"))
	}
	return json.Marshal(strings.TrimSuffix(name, ".png") + "@2x.png")
}

// signupWorkflow spawns two notification runs and does not wait for them.
// The step reaches the client through its closure: a step has no client
// of its own.
func signupWorkflow(c workflow.Client) workflow.Workflow {
	return workflow.Workflow{Name: "signup", Steps: []workflow.Step{{Name: "welcome", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		// snippet:spawn
		refs, err := workflow.Spawn(ctx, c, "notify", []workflow.Job{
			{Workflow: "notify", Payload: []byte(`"email"`)},
			{Workflow: "notify", Payload: []byte(`"sms"`)},
		})
		if err != nil {
			return nil, err
		}
		// snippet:end
		return json.Marshal(len(refs))
	}}}}
}

func spawn(ctx context.Context, c workflow.Client, out io.Writer, notified *atomic.Int32) error {
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "signup"})
	if err != nil {
		return err
	}
	res, err := workflow.AwaitResult(ctx, c, ref, await)
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "spawn:")
	fmt.Fprintf(out, "  parent finished, children started: %s\n", res)
	// snippet:children
	page, err := c.(workflow.RunQuerier).Runs(ctx, workflow.RunQuery{Workflow: "notify"})
	if err != nil {
		return err
	}
	for _, r := range page.Runs {
		fmt.Fprintf(out, "  child of %s, parent run matches: %v\n",
			r.Labels[workflow.LabelParentStep], r.Labels[workflow.LabelParentRun] == ref.RunID.String())
	}
	// snippet:end
	for _, r := range page.Runs {
		if _, err := workflow.Await(ctx, c, r.Ref, await); err != nil {
			return err
		}
	}
	fmt.Fprintf(out, "  notifications sent: %d\n", notified.Load())
	return nil
}

// albumWorkflow resizes every image of an album as child runs and joins
// their results.
func albumWorkflow(c workflow.Client) workflow.Workflow {
	return workflow.Workflow{Name: "album", Steps: []workflow.Step{{Name: "resize-all", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		var images []string
		if err := json.Unmarshal(in, &images); err != nil {
			return nil, workflow.Permanent(err)
		}
		// snippet:await
		jobs := make([]workflow.Job, len(images))
		for i, img := range images {
			jobs[i] = workflow.Job{Workflow: "resize", Payload: []byte(`"` + img + `"`)}
		}
		outs, err := workflow.AwaitChildren(ctx, c, "resize", jobs, workflow.ChildOptions{Poll: 5 * time.Millisecond})
		if err != nil {
			return nil, err // a Snooze while children run; a failed child fails this step
		}
		names := make([]string, len(outs))
		for i, o := range outs {
			if err := json.Unmarshal(o, &names[i]); err != nil {
				return nil, workflow.Permanent(err)
			}
		}
		return json.Marshal(names)
		// snippet:end
	}}}}
}

func awaitChildren(ctx context.Context, c workflow.Client, out io.Writer) error {
	fmt.Fprintln(out, "await children:")
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "album", Payload: []byte(`["a.png","b.png","c.png"]`)})
	if err != nil {
		return err
	}
	res, err := workflow.AwaitResult(ctx, c, ref, await)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "  results in job order: %s\n", res)
	page, err := c.(workflow.RunQuerier).Runs(ctx, workflow.RunQuery{Ref: ref, Limit: 1})
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "  parent step attempts spent: %d\n", page.Runs[0].Attempts)

	// snippet:failed
	ref, err = c.Enqueue(ctx, workflow.Job{Workflow: "album", Payload: []byte(`["a.png","bad.png"]`)})
	if err != nil {
		return err
	}
	_, err = workflow.AwaitResult(ctx, c, ref, await)
	var failed *workflow.RunFailedError
	if errors.As(err, &failed) {
		fmt.Fprintf(out, "  a failed child fails the parent: %s\n", failed.Status)
		fmt.Fprintf(out, "  parent error names the child: %v\n", strings.Contains(failed.Err, `await child "resize" #1`))
	}
	// snippet:end
	if failed == nil {
		return fmt.Errorf("parent ended with %v, want a RunFailedError", err)
	}
	return nil
}
