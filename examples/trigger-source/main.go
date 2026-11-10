// Command trigger-source shows where a run's trigger source comes from. An
// app names its own sources with workflow.WithSource; the library derives
// the others from the labels it already stamps (a cron fire, a manual
// trigger of a schedule); a step reads either with workflow.SourceOf; and
// middleware.RequireSource refuses a job with no source, or with one that
// is not on its allow-list. It runs on the in-memory driver with a fake
// clock, so the 02:00 cron fires at once.
//
// It is the program behind docs/concepts/trigger-source.md and the
// "Require a trigger source" section of docs/middleware/enqueue-middleware.md:
// the guides copy its regions (marked "snippet:") and task docs:snippets
// fails when they drift.
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
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

// start is the fake clock's first instant: an hour before the 02:00 cron.
var start = time.Date(2026, 1, 5, 1, 0, 0, 0, time.UTC)

// runTimeout bounds each wait for a step, generous for a slow CI host.
const runTimeout = 10 * time.Second

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "trigger-source:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	clk := clock.NewFake(start)
	mem := memory.New(memory.WithClock(clk))
	defer mem.Stop()

	seen := make(chan string, 4)
	// snippet:step
	report := func(ctx context.Context, _ []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		src, ref := workflow.SourceOf(info.Labels) // set by the app, or derived from the library's labels
		seen <- fmt.Sprintf("source=%s ref=%s", src, ref)
		return nil, nil
	}
	// snippet:end
	if err := mem.Register(workflow.Workflow{
		Name: "nightly-report", Cron: []string{"0 2 * * *"}, CronLocation: time.UTC,
		Steps: []workflow.Step{{Name: "build", Run: report}},
	}); err != nil {
		return err
	}

	// snippet:require
	// Every enqueue through c must name a source, and one of these three.
	c := middleware.WrapClient(mem, middleware.RequireSource("api", "webhook", "admin"))
	// snippet:end
	if err := c.Start(ctx); err != nil {
		return err
	}
	wait := func(what string) error {
		select {
		case s := <-seen:
			fmt.Fprintf(out, "%s: %s\n", what, s)
			return nil
		case <-time.After(runTimeout):
			return fmt.Errorf("%s: no run within %s", what, runTimeout)
		}
	}

	// snippet:enqueue
	_, err := c.Enqueue(ctx, workflow.Job{Workflow: "nightly-report"})
	fmt.Fprintln(out, "no source:", err)

	_, err = c.Enqueue(ctx, workflow.WithSource(workflow.Job{Workflow: "nightly-report"}, "webook", ""))
	fmt.Fprintln(out, "typo:", err)

	job := workflow.WithSource(workflow.Job{Workflow: "nightly-report"}, "webhook", "delivery-42")
	if _, err := c.Enqueue(ctx, job); err != nil {
		return err
	}
	// snippet:end
	if err := wait("webhook"); err != nil {
		return err
	}

	// The cron fires past the enqueue middleware; its run carries the
	// schedule label, from which SourceOf derives "schedule".
	clk.Advance(time.Hour)
	if err := wait("cron fire"); err != nil {
		return err
	}
	// Run now: the manual trigger label makes it "schedule-manual".
	if _, err := mem.TriggerSchedule(ctx, "cron:nightly-report:0"); err != nil {
		return err
	}
	if err := wait("run now"); err != nil {
		return err
	}

	// snippet:validate
	fmt.Fprintln(out, "validate:", workflow.ValidateSource("Slack"))
	fmt.Fprintln(out, "library sources:", workflow.LibrarySources())
	// snippet:end
	if !errors.Is(workflow.ValidateSource("Slack"), workflow.ErrInvalid) {
		return errors.New("ValidateSource does not wrap ErrInvalid")
	}
	return nil
}
