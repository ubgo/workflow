// Command snooze-heartbeat shows three ways a step deals with time: a
// step that snoozes ("not yet", without spending an attempt), a step whose
// handler hangs and is caught by its heartbeat timeout while a healthy
// attempt beats and finishes, and a run that goes past its RunTimeout and
// runs OnTimeout instead of OnFailure.
//
// It runs on the in-memory driver with a fake clock, so minutes pass at
// once and the output is the same on every run. It is the program behind
// docs/jobs/retries.md (snooze), docs/jobs/progress.md (heartbeats) and
// docs/jobs/timeouts.md: the guides copy its regions (marked "snippet:")
// and task docs:snippets fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
)

// start is the fake clock's first instant; any fixed time works.
var start = time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "snooze-heartbeat:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	for _, part := range []func(context.Context, io.Writer) error{snooze, heartbeat, runTimeout} {
		if err := part(ctx, out); err != nil {
			return err
		}
	}
	return nil
}

// newClient starts a memory client on clk with wfs registered and every
// run end sent to the returned channel.
func newClient(ctx context.Context, clk *clock.Fake, wfs ...workflow.Workflow) (*memory.Client, <-chan workflow.RunEvent, error) {
	c := memory.New(memory.WithClock(clk))
	finished := make(chan workflow.RunEvent, 4)
	if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { finished <- ev }); err != nil {
		return nil, nil, err
	}
	for _, wf := range wfs {
		if err := c.Register(wf); err != nil {
			return nil, nil, err
		}
	}
	if err := c.Start(ctx); err != nil {
		return nil, nil, err
	}
	return c, finished, nil
}

// snooze runs a step that asks to be tried again later while the report it
// needs is not ready. The workflow allows no retries at all, yet the step
// snoozes twice and succeeds: a snooze is not a failure.
func snooze(ctx context.Context, out io.Writer) error {
	fmt.Fprintln(out, "snooze:")
	clk := clock.NewFake(start)
	tries := make(chan int, 4)
	checks := 0
	reportReady := func() bool { checks++; return checks == 3 } // ready on the third look

	// snippet:snooze
	fetch := func(ctx context.Context, _ []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		tries <- info.Attempt
		if !reportReady() {
			// Not a failure: put the step back for a minute, same attempt.
			return nil, workflow.Snooze(time.Minute)
		}
		return []byte(`"report.csv"`), nil
	}
	wf := workflow.Workflow{
		Name:  "download-report",
		Retry: workflow.RetryPolicy{}, // no retries, and a snooze needs none
		Steps: []workflow.Step{{Name: "fetch", Run: fetch}},
	}
	// snippet:end

	c, finished, err := newClient(ctx, clk, wf)
	if err != nil {
		return err
	}
	defer c.Stop()
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "download-report"})
	if err != nil {
		return err
	}
	for i := range 3 {
		attempt := <-tries
		fmt.Fprintf(out, "  try at +%s: attempt %d\n", clk.Now().Sub(start), attempt)
		if i == 2 { // the third look finds the report
			break
		}
		if err := clk.BlockUntil(ctx, 1); err != nil { // the snooze timer is armed
			return err
		}
		st, err := c.Status(ctx, ref)
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "  snoozed, status %s\n", st)
		clk.Advance(time.Minute)
	}
	ev := <-finished
	page, err := c.Runs(ctx, workflow.RunQuery{Ref: ref})
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "  run %s, attempts %d, errors recorded %d\n", ev.Status, page.Runs[0].Attempts, len(page.Runs[0].Errors))
	return nil
}

// heartbeat runs a step with a heartbeat timeout: its first attempt hangs
// without beating and is failed after 30 seconds, long before any step
// timeout would; the retry beats as it works and succeeds.
func heartbeat(ctx context.Context, out io.Writer) error {
	fmt.Fprintln(out, "heartbeat:")
	clk := clock.NewFake(start)
	attempts := make(chan int, 4)

	// snippet:heartbeat
	load := func(ctx context.Context, _ []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		attempts <- info.Attempt
		if info.Attempt == 1 {
			<-ctx.Done() // a hung connection: no heartbeat, no progress
			return nil, context.Cause(ctx)
		}
		for batch := 1; batch <= 3; batch++ {
			// ... load one batch ...
			if err := workflow.Heartbeat(ctx, fmt.Appendf(nil, "batch %d", batch)); err != nil {
				return nil, err // the beat was not recorded: the attempt lost its lease
			}
		}
		return nil, nil
	}
	wf := workflow.Workflow{
		Name:  "import-catalog",
		Retry: workflow.RetryPolicy{Max: 1, Backoff: workflow.Backoff{Initial: time.Second}},
		Steps: []workflow.Step{{
			Name:             "load",
			Run:              load,
			HeartbeatTimeout: 30 * time.Second, // the most it may go without a beat
		}},
	}
	// snippet:end

	c, finished, err := newClient(ctx, clk, wf)
	if err != nil {
		return err
	}
	defer c.Stop()
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "import-catalog"})
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "  attempt %d started\n", <-attempts)
	if err := clk.BlockUntil(ctx, 1); err != nil { // the heartbeat watchdog
		return err
	}
	clk.Advance(30 * time.Second)
	if err := clk.BlockUntil(ctx, 1); err != nil { // the retry's backoff
		return err
	}
	clk.Advance(time.Second)
	fmt.Fprintf(out, "  attempt %d started at +%s\n", <-attempts, clk.Now().Sub(start))
	ev := <-finished
	page, err := c.Runs(ctx, workflow.RunQuery{Ref: ref})
	if err != nil {
		return err
	}
	first := page.Runs[0].Errors[0]
	fmt.Fprintf(out, "  attempt %d failed at +%s: %s\n", first.Attempt, first.At.Sub(start), first.Error)
	fmt.Fprintf(out, "  run %s\n", ev.Status)
	return nil
}

// runTimeout runs a workflow whose whole run may take five minutes. Its
// step hangs, the run times out, and OnTimeout runs instead of OnFailure.
func runTimeout(ctx context.Context, out io.Writer) error {
	fmt.Fprintln(out, "run timeout:")
	clk := clock.NewFake(start)
	started := make(chan struct{}, 1)
	var mu sync.Mutex
	var ran []string
	note := func(s string) { mu.Lock(); ran = append(ran, s); mu.Unlock() }

	// snippet:run-timeout
	wf := workflow.Workflow{
		Name:       "build-report",
		RunTimeout: 5 * time.Minute, // the whole run, waits and retries included
		Steps: []workflow.Step{{Name: "build", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			started <- struct{}{}
			<-ctx.Done() // the cause is ErrRunTimeout
			return nil, context.Cause(ctx)
		}}},
		OnTimeout: func(_ context.Context, in []byte) ([]byte, error) {
			note("OnTimeout with payload " + string(in)) // tell the user the report is late
			return nil, nil
		},
		OnFailure: func(context.Context, []byte) ([]byte, error) {
			note("OnFailure") // any other failure
			return nil, nil
		},
	}
	// snippet:end

	c, finished, err := newClient(ctx, clk, wf)
	if err != nil {
		return err
	}
	defer c.Stop()
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "build-report", Payload: []byte(`"q3"`)}); err != nil {
		return err
	}
	<-started
	if err := clk.BlockUntil(ctx, 1); err != nil { // the run timeout, armed at admission
		return err
	}
	clk.Advance(5 * time.Minute)
	ev := <-finished

	// snippet:timeout-errors
	runOut := errors.Is(ev.Err, workflow.ErrRunTimeout) // this deadline in particular
	anyDeadline := workflow.IsTimeout(ev.Err)           // any of the four
	// snippet:end
	fmt.Fprintf(out, "  run %s: %v\n", ev.Status, ev.Err)
	fmt.Fprintf(out, "  ErrRunTimeout %t, IsTimeout %t, ErrStepTimeout %t\n", runOut, anyDeadline, errors.Is(ev.Err, workflow.ErrStepTimeout))
	mu.Lock()
	defer mu.Unlock()
	for _, s := range ran {
		fmt.Fprintf(out, "  ran %s\n", s)
	}
	return nil
}
