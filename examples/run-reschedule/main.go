// Command run-reschedule shows operations on runs that have not finished:
// listing the delayed runs due in the next hour (RunQuery.DueFrom and
// DueTo), moving one to another time (Reschedule), and withdrawing one
// running step while the rest of the run decides what happens next
// (CancelStep): a step that falls back to cached data lets its run
// continue, a step that gives up fails its run.
//
// It runs on the in-memory driver with a fake clock, so hours pass at once
// and the output is the same on every run. It is the program behind
// docs/jobs/manage-runs.md (reschedule, due-time filters, cancel one
// step): the guide copies its regions (marked "snippet:") and task
// docs:snippets fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
)

// start is the fake clock's first instant; any fixed time works.
var start = time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "run-reschedule:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	clk := clock.NewFake(start)
	started := make(chan string, 4)
	stored := make(chan string, 1)

	// snippet:fallback
	fetchPrices := func(ctx context.Context, _ []byte) ([]byte, error) {
		started <- "fetch"
		<-ctx.Done() // the price feed is hanging
		if errors.Is(context.Cause(ctx), workflow.ErrStepCancelled) {
			return []byte(`"yesterday's prices"`), nil // an operator withdrew the step: fall back
		}
		return nil, context.Cause(ctx)
	}
	// snippet:end
	pull := func(ctx context.Context, _ []byte) ([]byte, error) {
		started <- "pull"
		<-ctx.Done()
		return nil, context.Cause(ctx) // no fallback: the step fails, and so does the run
	}
	store := func(_ context.Context, in []byte) ([]byte, error) {
		stored <- string(in)
		return nil, nil
	}

	c := memory.New(memory.WithClock(clk))
	finished := make(chan workflow.RunEvent, 8)
	if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { finished <- ev }); err != nil {
		return err
	}
	for _, wf := range []workflow.Workflow{
		{Name: "send-reminder", Steps: []workflow.Step{{Name: "send", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}},
		{Name: "update-prices", Steps: []workflow.Step{
			{Name: "fetch", Run: fetchPrices},
			{Name: "store", After: []string{"fetch"}, Run: store},
		}},
		{Name: "sync-feed", Steps: []workflow.Step{{Name: "pull", Run: pull}}},
	} {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	defer c.Stop()

	refs := map[string]workflow.RunRef{}
	for _, r := range []struct {
		name string
		in   time.Duration
	}{{"r-30m", 30 * time.Minute}, {"r-2h", 2 * time.Hour}, {"r-5h", 5 * time.Hour}} {
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "send-reminder", RunAt: start.Add(r.in), Labels: map[string]string{"name": r.name}})
		if err != nil {
			return err
		}
		refs[r.name] = ref
	}

	fmt.Fprintln(out, "due in the next hour:")
	if err := dueNextHour(ctx, c, clk, out); err != nil {
		return err
	}

	// snippet:reschedule
	moved, err := c.Reschedule(ctx, refs["r-2h"], clk.Now().Add(45*time.Minute))
	if err != nil {
		return err
	}
	// snippet:end
	fmt.Fprintf(out, "rescheduled r-2h to +45m, same ref %t\n", moved == refs["r-2h"])
	fmt.Fprintln(out, "due in the next hour:")
	if err := dueNextHour(ctx, c, clk, out); err != nil {
		return err
	}

	if err := clk.BlockUntil(ctx, 3); err != nil { // three delayed starts are armed
		return err
	}
	clk.Advance(time.Hour)
	var ran []string
	for range 2 {
		ev := <-finished
		ran = append(ran, ev.Labels["name"]+" "+string(ev.Status))
	}
	slices.Sort(ran) // the two runs finish in either order
	fmt.Fprintf(out, "after an hour: %s\n", strings.Join(ran, ", "))
	_, err = c.Reschedule(ctx, refs["r-30m"], clk.Now().Add(time.Hour))
	fmt.Fprintf(out, "reschedule a finished run: ErrWrongState %t\n", errors.Is(err, workflow.ErrWrongState))

	fmt.Fprintln(out, "cancel one step:")
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "update-prices"})
	if err != nil {
		return err
	}
	<-started
	// snippet:cancel-step
	if err := c.CancelStep(ctx, ref, "fetch"); err != nil {
		return err
	}
	// snippet:end
	fmt.Fprintf(out, "  store got %s\n", <-stored)
	fmt.Fprintf(out, "  update-prices %s\n", (<-finished).Status)

	ref, err = c.Enqueue(ctx, workflow.Job{Workflow: "sync-feed"})
	if err != nil {
		return err
	}
	<-started
	if err := c.CancelStep(ctx, ref, "pull"); err != nil {
		return err
	}
	ev := <-finished
	fmt.Fprintf(out, "  sync-feed %s, ErrStepCancelled %t\n", ev.Status, errors.Is(ev.Err, workflow.ErrStepCancelled))
	err = c.CancelStep(ctx, ref, "pull")
	fmt.Fprintf(out, "  cancel it again: ErrWrongState %t\n", errors.Is(err, workflow.ErrWrongState))
	return nil
}

// dueNextHour prints the delayed runs due within the next hour.
func dueNextHour(ctx context.Context, c *memory.Client, clk *clock.Fake, out io.Writer) error {
	// snippet:due
	now := clk.Now()
	page, err := c.Runs(ctx, workflow.RunQuery{
		DueFrom: now,
		DueTo:   now.Add(time.Hour), // exclusive
	})
	if err != nil {
		return err
	}
	for _, r := range page.Runs {
		fmt.Fprintf(out, "  %s due at +%s, %s\n", r.Labels["name"], r.RunAt.Sub(now), r.Status)
	}
	// snippet:end
	return nil
}
