// Command delayed-jobs enqueues jobs that start later, reads where each run
// is with Status, and cancels a delayed start and a running run.
//
// It runs on the in-memory driver with a fake clock, so "tomorrow at nine"
// arrives when the program moves the clock, not after a day. It is the
// program behind docs/jobs/delays.md and docs/jobs/cancel-and-status.md:
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
)

// start is the fake clock's first instant: a Monday at 09:00 UTC.
var start = time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC)

// reminder is the payload of the "remind" workflow.
type reminder struct{ To string }

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "delayed-jobs:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	clk := clock.NewFake(start)
	sent := make(chan string, 4)
	running := make(chan struct{}, 1)
	finished := make(chan workflow.RunEvent, 8)

	c := memory.New(memory.WithClock(clk))
	if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { finished <- ev }); err != nil {
		return err
	}
	err := c.Register(workflow.Workflow{
		Name: "remind",
		Steps: []workflow.Step{{
			Name: "send",
			Run: workflow.Define(func(ctx context.Context, in reminder) (string, error) {
				sent <- fmt.Sprintf("reminder sent to %s at %s", in.To, clk.Now().Format("Mon 15:04"))
				return "sent", nil
			}),
		}},
	})
	if err != nil {
		return err
	}
	// export runs until it is cancelled: it stands for a long step.
	err = c.Register(workflow.Workflow{
		Name: "export",
		Steps: []workflow.Step{{
			Name: "write",
			Run: func(ctx context.Context, _ []byte) ([]byte, error) {
				running <- struct{}{}
				<-ctx.Done() // a long step watches its ctx
				return nil, ctx.Err()
			},
		}},
	})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer c.Stop()
	defer cancel()
	if err := c.Start(ctx); err != nil {
		return err
	}

	// snippet:run-at
	ref, err := c.Enqueue(ctx, workflow.Job{
		Workflow: "remind",
		Payload:  []byte(`{"To":"ada@example.com"}`),
		RunAt:    clk.Now().Add(24 * time.Hour), // tomorrow at this time
	})
	if err != nil {
		return err
	}
	// Until it starts, the run has a ScheduledID and no RunID.
	st, err := c.Status(ctx, ref)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "enqueued: scheduled id set %t, run id set %t, status %s\n",
		!ref.ScheduledID.IsZero(), !ref.RunID.IsZero(), st)
	// snippet:end

	// snippet:cancel-delayed
	later, err := c.Enqueue(ctx, workflow.Job{Workflow: "remind", RunAt: clk.Now().Add(time.Hour)})
	if err != nil {
		return err
	}
	if err := c.Cancel(ctx, later); err != nil {
		return err
	}
	if err := c.Cancel(ctx, later); err != nil { // cancelling twice is not an error
		return err
	}
	if st, err = c.Status(ctx, later); err != nil {
		return err
	}
	fmt.Fprintln(out, "cancelled before it started:", st)
	// snippet:end
	ev := <-finished
	fmt.Fprintf(out, "run-finished: %s, cancelled %t\n", ev.Status, errors.Is(ev.Err, workflow.ErrCancelled))

	clk.Advance(24 * time.Hour)
	fmt.Fprintln(out, <-sent)
	<-finished

	// snippet:status
	st, err = c.Status(ctx, ref) // the RunRef Enqueue returned still works
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "after it ran: %s, terminal %t\n", st, st.IsTerminal())
	// snippet:end

	// A RunAt that is not in the future starts the run now.
	now, err := c.Enqueue(ctx, workflow.Job{Workflow: "remind", Payload: []byte(`{"To":"bob@example.com"}`), RunAt: clk.Now().Add(-time.Minute)})
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "past RunAt: run id set %t\n", !now.RunID.IsZero())
	fmt.Fprintln(out, <-sent)
	<-finished

	// snippet:cancel-running
	exp, err := c.Enqueue(ctx, workflow.Job{Workflow: "export"})
	if err != nil {
		return err
	}
	<-running // the step is executing
	if err := c.Cancel(ctx, exp); err != nil {
		return err
	}
	if st, err = c.Status(ctx, exp); err != nil {
		return err
	}
	fmt.Fprintln(out, "cancelled while running:", st)
	// snippet:end
	ev = <-finished
	fmt.Fprintf(out, "run-finished: %s, cancelled %t\n", ev.Status, errors.Is(ev.Err, workflow.ErrCancelled))

	// snippet:not-found
	_, err = c.Status(ctx, workflow.RunRef{RunID: "run-404"})
	fmt.Fprintln(out, "unknown run:", errors.Is(err, workflow.ErrNotFound))
	// snippet:end
	return nil
}
