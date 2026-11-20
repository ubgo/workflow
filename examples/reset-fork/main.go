// Command reset-fork redoes work that already succeeded, two ways, on the
// in-memory driver: reset a succeeded run in place from a step
// (workflow.RunResetterWith with ResetOptions.Succeeded), and fork a
// finished run into a new run that starts from a step with the outputs of
// the steps before it (workflow.RunForker). The chain the run handed on
// and the Then rule its success started do not run a second time.
//
// It is the program behind the reset and fork sections of
// docs/jobs/manage-runs.md: the guide copies its regions (marked
// "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var fixed atomic.Bool // the transform bug is fixed and deployed
	var extracts, notifies atomic.Int32
	report := workflow.Workflow{Name: "report", Steps: []workflow.Step{
		{Name: "extract", Run: func(context.Context, []byte) ([]byte, error) {
			extracts.Add(1)
			return json.Marshal(41)
		}},
		{Name: "transform", After: []string{"extract"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
			var n int
			if err := json.Unmarshal(in, &n); err != nil {
				return nil, workflow.Permanent(err)
			}
			if fixed.Load() {
				n++ // the fix: the source counts from zero
			}
			return json.Marshal(n)
		}},
		{Name: "publish", After: []string{"transform"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
			return in, nil
		}},
	}}
	notify := workflow.Workflow{Name: "notify", Steps: []workflow.Step{{Name: "send",
		Run: func(context.Context, []byte) ([]byte, error) { notifies.Add(1); return nil, nil }}}}

	// The Then rule tells the team once a report is published.
	then, err := workflow.Then([]workflow.ThenRule{{When: "report", Start: func(workflow.RunEvent) (workflow.Job, bool, error) {
		return workflow.Job{Workflow: "notify"}, true, nil
	}}})
	if err != nil {
		return err
	}
	c, err := workflow.Use(memory.New(), then)
	if err != nil {
		return err
	}
	for _, wf := range []workflow.Workflow{report, notify} {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	await := func(ref workflow.RunRef) (string, error) {
		out, err := workflow.AwaitResult(ctx, c, ref, workflow.AwaitOptions{Poll: time.Millisecond})
		return string(out), err
	}

	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "report"})
	if err != nil {
		return err
	}
	out, err := await(ref)
	if err != nil {
		return err
	}
	fmt.Println("first run:", out)

	// snippet:reset-succeeded
	fixed.Store(true)
	rs := c.(workflow.RunResetterWith)
	again, err := rs.ResetRunWith(ctx, ref, "transform", workflow.ResetOptions{Succeeded: true})
	if err != nil {
		return err
	}
	// snippet:end
	out, err = await(again)
	if err != nil {
		return err
	}
	fmt.Println("reset in place:", again.RunID == ref.RunID, out)

	// snippet:fork
	fk := c.(workflow.RunForker)
	fork, err := fk.ForkRun(ctx, ref, "publish", workflow.ForkOptions{Labels: map[string]string{"ticket": "OPS-7"}})
	if err != nil {
		return err
	}
	// snippet:end
	out, err = await(fork)
	if err != nil {
		return err
	}
	info, err := c.(workflow.RunQuerier).Runs(ctx, workflow.RunQuery{Ref: fork})
	if err != nil {
		return err
	}
	l := info.Runs[0].Labels
	fmt.Println("fork:", fork.RunID != ref.RunID, out, "fork-of", l[workflow.LabelForkOf] == ref.RunID.String(), "redo", l[workflow.LabelRedo])
	time.Sleep(20 * time.Millisecond) // a wrong second notify would start by now
	fmt.Printf("extract ran %d time(s), notify %d\n", extracts.Load(), notifies.Load())
	return nil
}
