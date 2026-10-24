// Command hybrid-placement splits one workflow.Client across two backends
// with package hybrid: the schedules fire on a scheduler client, the runs
// live on a jobs client. Both are memory clients here, so the program
// needs nothing installed; in an app the jobs client is a durable driver
// (sqlstore, redis) and the scheduler an in-process memory client.
//
// It is the program behind docs/drivers/hybrid.md: the guide copies its
// regions (marked "snippet:") and task docs:snippets fails when they
// drift.
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
	"github.com/ubgo/workflow/hybrid"
	"github.com/ubgo/workflow/memory"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "hybrid-placement:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	clk := clock.NewFake(time.Date(2026, 10, 14, 1, 59, 30, 0, time.UTC)) // the scheduler's clock

	ran := make(chan workflow.StepInfo, 4)
	// snippet:new
	scheduler := memory.New(memory.WithClock(clk), memory.WithNodeID("sched")) // in process: fires the crons
	jobs := memory.New(memory.WithNodeID("jobs"))                              // in an app: sqlstore or redis
	c := hybrid.New(scheduler, jobs)

	err := c.Register(workflow.Workflow{
		Name:         "nightly-report",
		Cron:         []string{"0 2 * * *"},
		CronLocation: time.UTC,
		Steps: []workflow.Step{{Name: "build", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			info, _ := workflow.StepInfoFrom(ctx)
			ran <- info
			return nil, nil
		}}},
	})
	if err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil { // starts jobs, then the scheduler
		return err
	}
	// snippet:end

	caps := c.Capabilities()
	fmt.Fprintf(out, "cron: %t (scheduler)  run query: %t (jobs)  trigger: %t\n",
		caps.Has(workflow.CapCron), caps.Has(workflow.CapRunQuery), caps.Has(workflow.CapScheduleTrigger))

	if err := clk.BlockUntil(ctx, 1); err != nil { // the 02:00 tick is armed
		return err
	}
	clk.Advance(30 * time.Second)
	info := <-ran
	at, _ := info.TickAt()
	fmt.Fprintf(out, "tick %s ran on node %q, schedule %s\n", at.Format("15:04"), info.Node, info.Labels[workflow.LabelSchedule])

	// snippet:runs
	page, err := c.Runs(ctx, workflow.RunQuery{Workflow: "nightly-report"}) // run queries go to the jobs client
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "runs on the jobs client:", len(page.Runs))
	// snippet:end

	// snippet:refused
	err = c.Register(workflow.Workflow{
		Name: "sync", Cron: []string{"*/5 * * * *"}, CronLocation: time.UTC,
		CronOverlap: workflow.OverlapSkip, // the scheduler would see only its forwarder runs
		Steps:       []workflow.Step{{Name: "run", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}},
	})
	fmt.Fprintln(out, "CronOverlap refused:", errors.Is(err, workflow.ErrUnsupported))
	_, err = c.TriggerSchedule(ctx, "cron:nightly-report:0")
	fmt.Fprintln(out, "TriggerSchedule refused:", errors.Is(err, workflow.ErrUnsupported))
	// snippet:end
	return nil
}
