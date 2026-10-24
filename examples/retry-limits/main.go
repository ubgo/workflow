// Command retry-limits shows the retry limits beyond Max on the in-memory
// driver: a per-dispatch policy (Job.Retry), a deadline (RetryPolicy.Until),
// a cap on real errors that lets releases through (RetryPolicy.MaxErrors),
// and dropping a run whose work no longer makes sense (workflow.Discard).
//
// Each attempt of the deadline demo moves the fake clock 50 seconds, so the
// deadline is reached at the same attempt on every run. It is the program
// behind the "Limits beyond Max" sections of docs/jobs/retries.md.
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
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
)

// start is the fake clock's first instant; any fixed time works.
var start = time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC)

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

	var sms, pushes, rate, ship, onFailure atomic.Int64
	errBoom := errors.New("upstream 503")

	// snippet:job-retry
	sendSMS := workflow.Workflow{
		Name:  "send-sms",
		Retry: workflow.RetryPolicy{Max: 1}, // the default: one retry
		Steps: []workflow.Step{{Name: "send", Run: func(context.Context, []byte) ([]byte, error) {
			sms.Add(1)
			return nil, errBoom
		}}},
	}
	// snippet:end

	// snippet:until
	syncCRM := workflow.Workflow{
		Name: "sync-crm",
		// Retry as often as needed, but give up two minutes after the
		// first attempt started: the data is stale after that.
		Retry: workflow.RetryPolicy{Max: 100, Until: 2 * time.Minute},
		Steps: []workflow.Step{{Name: "push", Run: func(context.Context, []byte) ([]byte, error) {
			pushes.Add(1)
			clk.Advance(50 * time.Second) // each attempt takes 50 seconds
			return nil, errBoom
		}}},
	}
	// snippet:end

	// snippet:max-errors
	callAPI := workflow.Workflow{
		Name: "call-partner-api",
		// Up to 10 attempts, but only 2 real errors: being rate limited
		// (a release, marked by RetryAfter) is not the API's fault.
		Retry: workflow.RetryPolicy{Max: 10, MaxErrors: 2, DelayFor: workflow.RetryAfterDelay},
		Steps: []workflow.Step{{Name: "call", Run: func(context.Context, []byte) ([]byte, error) {
			if rate.Add(1) <= 3 {
				return nil, workflow.RetryAfter(errors.New("429 too many requests"), 0)
			}
			return nil, errBoom
		}}},
	}
	// snippet:end

	// snippet:discard
	shipOrder := workflow.Workflow{
		Name:  "ship-order",
		Retry: workflow.RetryPolicy{Max: 5},
		Steps: []workflow.Step{{Name: "ship", Run: func(context.Context, []byte) ([]byte, error) {
			ship.Add(1)
			// The order was deleted after the job was queued: nothing to
			// ship, nothing to retry, nothing an operator must look at.
			return nil, workflow.Discard(errors.New("order o-9 no longer exists"))
		}}},
		OnFailure: func(context.Context, []byte) ([]byte, error) { onFailure.Add(1); return nil, nil },
	}
	// snippet:end

	for _, wf := range []workflow.Workflow{sendSMS, syncCRM, callAPI, shipOrder} {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	ends := make(chan workflow.RunEvent, 8)
	if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { ends <- ev }); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	defer c.Stop()

	// snippet:dispatch
	vip := &workflow.RetryPolicy{Max: 4} // this customer gets more tries
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "send-sms", Retry: vip}); err != nil {
		return err
	}
	// snippet:end
	ev := <-ends
	fmt.Fprintf(out, "send-sms with Job.Retry{Max: 4}: %s after %d attempts\n", ev.Status, sms.Load())

	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "sync-crm"}); err != nil {
		return err
	}
	ev = <-ends
	fmt.Fprintf(out, "sync-crm with Until 2m: %s after %d attempts, %s after the first\n", ev.Status, pushes.Load(), clk.Now().Sub(start))

	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "call-partner-api"}); err != nil {
		return err
	}
	ev = <-ends
	fmt.Fprintf(out, "call-partner-api with MaxErrors 2: %s after %d attempts (3 releases, 2 errors)\n", ev.Status, rate.Load())

	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "ship-order"}); err != nil {
		return err
	}
	ev = <-ends
	fmt.Fprintf(out, "ship-order discarded: %s after %d attempt, OnFailure ran %d times, ErrDiscarded: %v\n",
		ev.Status, ship.Load(), onFailure.Load(), errors.Is(ev.Err, workflow.ErrDiscarded))
	return nil
}
