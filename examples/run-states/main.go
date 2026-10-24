// Command run-states shows the two run statuses for runs between and
// after attempts on the in-memory driver: StatusRetrying, with the next
// attempt's number and time, while a failed step waits on its backoff;
// and StatusDiscarded for a run a step dropped with workflow.Discard. It
// also shows that a run that runs out of retries still ends failed, and
// how a batch counts a discarded job, by default and with DiscardedAs.
//
// It is the program behind docs/jobs/run-states.md.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/admin"
	"github.com/ubgo/workflow/batch"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
)

// start is the fake clock's first instant; any fixed time works.
var start = time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC)

// statusPoll is how often the program re-reads a run's status while a
// worker goroutine moves it on.
const statusPoll = time.Millisecond

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	clk := clock.NewFake(start)
	c := memory.New(memory.WithClock(clk))

	var syncs atomic.Int64
	callbacks := make(chan string, 4)
	errBoom := errors.New("upstream 503")
	wfs := []workflow.Workflow{
		{
			Name:  "sync-invoices",
			Retry: workflow.RetryPolicy{Max: 3, Backoff: workflow.Backoff{Initial: time.Minute}},
			Steps: []workflow.Step{{Name: "push", Run: func(context.Context, []byte) ([]byte, error) {
				if syncs.Add(1) == 1 {
					return nil, errBoom
				}
				return nil, nil
			}}},
		},
		{
			Name:  "ship-order",
			Retry: workflow.RetryPolicy{Max: 5},
			Steps: []workflow.Step{{Name: "ship", Run: func(context.Context, []byte) ([]byte, error) {
				return nil, workflow.Discard(errors.New("order o-9 no longer exists"))
			}}},
		},
		{
			Name:  "pack-order",
			Steps: []workflow.Step{{Name: "pack", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}},
		},
		callback("batch-then", callbacks),
		callback("batch-catch", callbacks),
		{
			Name:  "call-partner",
			Retry: workflow.RetryPolicy{Max: 10, MaxErrors: 2},
			Steps: []workflow.Step{{Name: "call", Run: func(context.Context, []byte) ([]byte, error) {
				return nil, errBoom
			}}},
		},
	}
	for _, wf := range wfs {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	ends := make(chan workflow.RunEvent, 8)
	if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { ends <- ev }); err != nil {
		return err
	}
	m := batch.New(c, batch.NewMemoryStore())
	if err := m.Attach(ctx); err != nil { // before Start: it registers the run-finished hook
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	defer c.Stop()
	fmt.Fprintf(out, "memory declares CapRunStates: %v\n", c.Capabilities().Has(workflow.CapRunStates))

	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "sync-invoices"})
	if err != nil {
		return err
	}
	if err := waitFor(ctx, c, ref, workflow.StatusRetrying); err != nil {
		return err
	}
	// snippet:retrying
	page, err := c.Runs(ctx, workflow.RunQuery{Statuses: []workflow.Status{workflow.StatusRetrying}})
	if err != nil {
		return err
	}
	for _, info := range page.Runs {
		fmt.Fprintf(out, "%s: %s, attempt %d at %s, last error %q\n",
			info.Workflow, info.Status, info.NextAttempt, info.NextAttemptAt.Sub(start), info.Error)
	}
	// snippet:end
	clk.Advance(time.Minute) // the backoff ends
	ev := <-ends
	fmt.Fprintf(out, "after the backoff: %s\n", ev.Status)

	// snippet:discarded
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "ship-order"}); err != nil {
		return err
	}
	ev = <-ends
	fmt.Fprintf(out, "%s: %s, terminal %v, ErrDiscarded %v, ErrCancelled %v\n", ev.Workflow, ev.Status,
		ev.Status.IsTerminal(), errors.Is(ev.Err, workflow.ErrDiscarded), errors.Is(ev.Err, workflow.ErrCancelled))
	// snippet:end

	// snippet:admin
	ops := admin.New(c)
	dropped, err := ops.Discarded(ctx, workflow.RunQuery{}) // apart from ops.Failures
	if err != nil {
		return err
	}
	for _, info := range dropped.Runs {
		fmt.Fprintf(out, "admin discarded: %s, %q\n", info.Workflow, info.Error)
	}
	// snippet:end

	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "call-partner"}); err != nil {
		return err
	}
	ev = <-ends
	fmt.Fprintf(out, "%s with MaxErrors 2: %s\n", ev.Workflow, ev.Status)

	// snippet:batch
	orders := []workflow.Job{{Workflow: "pack-order"}, {Workflow: "ship-order"}}
	for _, discardedAs := range []workflow.Status{"", workflow.StatusSucceeded} {
		id, err := m.Dispatch(ctx, batch.Batch{
			Jobs:          orders,
			Then:          "batch-then",
			Catch:         "batch-catch",
			AllowFailures: true,        // let pack-order finish after ship-order is dropped
			DiscardedAs:   discardedAs, // zero: a discarded job counts as failed
		})
		if err != nil {
			return err
		}
		ran := <-callbacks
		p, err := finished(ctx, m, id)
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "batch with DiscardedAs %q: %s ran; succeeded %d, failed %d, discarded %d\n",
			discardedAs, ran, p.Succeeded, p.Failed, p.Discarded)
	}
	// snippet:end
	waitN(ends, 6) // the batch jobs and callbacks

	// snippet:switch
	describe := func(s workflow.Status) string {
		switch s {
		case workflow.StatusRetrying:
			return "waiting to retry"
		case workflow.StatusDiscarded:
			return "dropped on purpose"
		case workflow.StatusFailed:
			return "needs a look"
		default: // keep one: drivers may report statuses added later
			if s.IsTerminal() {
				return "done"
			}
			return "in progress"
		}
	}
	// snippet:end
	for _, s := range []workflow.Status{workflow.StatusRetrying, workflow.StatusDiscarded, workflow.StatusFailed, workflow.StatusSucceeded} {
		fmt.Fprintf(out, "  %s: %s\n", s, describe(s))
	}
	return nil
}

// callback is a one-step workflow that reports its name when a batch
// calls it.
func callback(name string, ran chan<- string) workflow.Workflow {
	return workflow.Workflow{Name: name, Steps: []workflow.Step{{Name: "run", Run: func(context.Context, []byte) ([]byte, error) {
		ran <- name
		return nil, nil
	}}}}
}

// finished polls a batch's progress until every job has ended: Catch
// runs on the first failure, possibly before the other jobs end.
func finished(ctx context.Context, m *batch.Manager, id batch.ID) (batch.Progress, error) {
	for {
		p, err := m.Progress(ctx, id)
		if err != nil || p.Finished {
			return p, err
		}
		select {
		case <-ctx.Done():
			return p, ctx.Err()
		case <-time.After(statusPoll):
		}
	}
}

// waitN takes n run ends.
func waitN(ends <-chan workflow.RunEvent, n int) {
	for range n {
		<-ends
	}
}

// waitFor polls Status until the run reaches want, since the worker moves
// it on in its own goroutine.
func waitFor(ctx context.Context, c workflow.Client, ref workflow.RunRef, want workflow.Status) error {
	for {
		st, err := c.Status(ctx, ref)
		if err != nil {
			return err
		}
		if st == want {
			return nil
		}
		if st.IsTerminal() {
			return fmt.Errorf("run ended %s, want %s", st, want)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(statusPoll):
		}
	}
}
