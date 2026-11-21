// Command memory-options shows every option of the in-memory driver at
// work on a fake clock: the worker pool, the clock, the logger, step
// middleware and the retry jitter source; named queues; how many finished
// runs the client remembers; a client that stops by itself once idle; and
// one scheduler goroutine and one timer for every schedule.
//
// It is the program behind docs/drivers/memory.md: the guide copies its
// regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
)

const (
	// runTimeout bounds the real-time wait for one run, generous for a
	// slow CI host; the fake clock moves only when the program says so.
	runTimeout = 10 * time.Second
	// statusPoll is how often waitStatus re-reads a run's status.
	statusPoll = 2 * time.Millisecond
	// retryInitial is the first retry's full delay; with Jitter 0.5 and
	// the pinned draw 0.5 the client waits 7.5s of it (10s - 10s*0.5*0.5).
	retryInitial = 10 * time.Second
	// retryMax is the flaky step's retry budget; it needs only the first.
	retryMax = 2
	// beforeRetry is just short of the 7.5s jittered delay, so the run is
	// still retrying; afterRetry is the rest of the delay.
	beforeRetry = 7 * time.Second
	afterRetry  = 500 * time.Millisecond
	// retainedRuns is how many runs the retention demo finishes: one more
	// than its MaxRuns, so the oldest is forgotten.
	retainedRuns = 3
	// retryJitter shortens a delay d to a value in [d*(1-Jitter), d].
	retryJitter = 0.5
	// pinnedDraw is what the jitter source returns: a test pins it so the
	// delay is the same on every run.
	pinnedDraw = 0.5
	// idleGrace is how long the client waits with nothing to do before it
	// stops by itself.
	idleGrace = 30 * time.Second
	// schedules is how many schedules the heap demo declares.
	schedules = 3
	// everyFiveMinutes is the cron spec of the heap demo's schedules.
	everyFiveMinutes = "*/5 * * * *"
	// cronGap is how far the clock moves so every schedule fires once.
	cronGap = 5 * time.Minute
)

// start is the fake clock's first reading, a Monday morning in UTC.
var start = time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "memory-options:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, w io.Writer) error {
	for _, part := range []func(context.Context, io.Writer) error{
		basics, queues, retention, stopWhenIdle, scheduleHeap,
	} {
		if err := part(ctx, w); err != nil {
			return err
		}
	}
	return nil
}

// basics shows WithWorkers, WithClock, WithLogger, WithMiddleware and
// WithJitterSource: a step that fails once and is retried after a jittered
// backoff the program can predict, and an OnFailure error that only the
// logger sees.
func basics(ctx context.Context, w io.Writer) error {
	// snippet:options
	clk := clock.NewFake(start)
	var calls atomic.Int32
	countCalls := func(next workflow.Handler) workflow.Handler { // step middleware
		return func(ctx context.Context, in []byte) ([]byte, error) {
			calls.Add(1)
			return next(ctx, in)
		}
	}
	logger := slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{ReplaceAttr: dropTime}))
	c := memory.New(
		memory.WithWorkers(2),                                         // step attempts at once (default 8)
		memory.WithClock(clk),                                         // every timer: backoff, sleeps, cron, timeouts
		memory.WithLogger(logger),                                     // errors no caller can see
		memory.WithMiddleware(countCalls),                             // around every step and OnFailure
		memory.WithJitterSource(func() float64 { return pinnedDraw }), // pin Backoff.Jitter's draw
	)
	defer c.Stop()
	// snippet:end

	var attempts atomic.Int32
	flaky := workflow.Workflow{
		Name: "sync",
		Retry: workflow.RetryPolicy{Max: retryMax, Backoff: workflow.Backoff{
			Initial: retryInitial, Jitter: retryJitter,
		}},
		Steps: []workflow.Step{{Name: "pull", Run: func(context.Context, []byte) ([]byte, error) {
			if attempts.Add(1) == 1 {
				return nil, errors.New("upstream 503")
			}
			return nil, nil
		}}},
	}
	broken := workflow.Workflow{
		Name: "import",
		Steps: []workflow.Step{{Name: "read", Run: func(context.Context, []byte) ([]byte, error) {
			return nil, workflow.Permanent(errors.New("file is empty"))
		}}},
		// Nobody waits on OnFailure, so its error goes to the logger.
		OnFailure: func(context.Context, []byte) ([]byte, error) { return nil, errors.New("pager is down") },
	}
	for _, wf := range []workflow.Workflow{flaky, broken} {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	if err := c.Start(ctx); err != nil {
		return err
	}

	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "sync"})
	if err != nil {
		return err
	}
	if err := waitStatus(ctx, c, ref, workflow.StatusRetrying); err != nil {
		return err
	}
	if err := clk.BlockUntil(ctx, 1); err != nil { // the backoff timer is armed
		return err
	}
	clk.Advance(beforeRetry)
	st, err := c.Status(ctx, ref)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "sync after 7s: %s\n", st)
	clk.Advance(afterRetry)
	if err := waitStatus(ctx, c, ref, workflow.StatusSucceeded); err != nil {
		return err
	}
	fmt.Fprintf(w, "sync after 7.5s: %s, attempts %d\n", workflow.StatusSucceeded, attempts.Load())

	bad, err := c.Enqueue(ctx, workflow.Job{Workflow: "import"})
	if err != nil {
		return err
	}
	if err := waitStatus(ctx, c, bad, workflow.StatusFailed); err != nil {
		return err
	}
	c.Stop() // waits for every goroutine, so the OnFailure log line is out
	fmt.Fprintf(w, "middleware calls: %d\n", calls.Load())
	return nil
}

// queues shows WithQueues: the client serves only the listed queues, so a
// job or a workflow for another queue is refused instead of waiting for
// ever.
func queues(ctx context.Context, w io.Writer) error {
	// snippet:queues
	c := memory.New(memory.WithQueues(
		workflow.QueueConfig{Name: "emails", Workers: 4},               // four email steps at once
		workflow.QueueConfig{Name: "reports", Workers: 1, Priority: 1}, // one report, served first
	))
	defer c.Stop()
	if err := c.Register(workflow.Workflow{Name: "welcome", Queue: "emails", Steps: []workflow.Step{{Name: "send", Run: noop}}}); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	_, err := c.Enqueue(ctx, workflow.Job{Workflow: "welcome", Queue: "video"}) // not served here
	fmt.Fprintln(w, "job on queue video: ErrInvalid =", errors.Is(err, workflow.ErrInvalid))
	// snippet:end

	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "welcome"})
	if err != nil {
		return err
	}
	if err := waitStatus(ctx, c, ref, workflow.StatusSucceeded); err != nil {
		return err
	}
	fmt.Fprintln(w, "welcome on queue emails:", workflow.StatusSucceeded)

	other := memory.New(memory.WithQueues(workflow.QueueConfig{Name: "emails"}))
	defer other.Stop()
	if err := other.Register(workflow.Workflow{Name: "render", Queue: "reports", Steps: []workflow.Step{{Name: "draw", Run: noop}}}); err != nil {
		return err
	}
	err = other.Start(ctx)
	fmt.Fprintln(w, "workflow on unserved queue at Start: ErrInvalid =", errors.Is(err, workflow.ErrInvalid))
	return nil
}

// retention shows WithRunRetention and DefaultRunRetention: past the
// limit, the oldest finished run is forgotten and its Status is
// ErrNotFound.
func retention(ctx context.Context, w io.Writer) error {
	fmt.Fprintf(w, "default retention: %s, at most %d runs\n", memory.DefaultRunRetention.MaxAge, memory.DefaultRunRetention.MaxRuns)

	// snippet:retention
	c := memory.New(memory.WithRunRetention(memory.RunRetention{
		MaxAge:  time.Hour, // forget a finished run an hour after it ended
		MaxRuns: 2,         // and keep at most the newest two
	}))
	// snippet:end
	defer c.Stop()
	if err := c.Register(workflow.Workflow{Name: "ping", Steps: []workflow.Step{{Name: "pong", Run: noop}}}); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	var refs []workflow.RunRef
	for range retainedRuns {
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "ping"})
		if err != nil {
			return err
		}
		if err := waitStatus(ctx, c, ref, workflow.StatusSucceeded); err != nil {
			return err
		}
		refs = append(refs, ref)
	}
	for _, ref := range refs {
		st, err := c.Status(ctx, ref)
		switch {
		case errors.Is(err, workflow.ErrNotFound):
			fmt.Fprintf(w, "%s: forgotten\n", ref.RunID)
		case err != nil:
			return err
		default:
			fmt.Fprintf(w, "%s: %s\n", ref.RunID, st)
		}
	}
	return nil
}

// stopWhenIdle shows WithStopWhenIdle: a batch-style process that enqueues
// its work and exits once nothing is left.
func stopWhenIdle(ctx context.Context, w io.Writer) error {
	clk := clock.NewFake(start)
	// snippet:stop-when-idle
	c := memory.New(memory.WithClock(clk), memory.WithStopWhenIdle(idleGrace))
	if err := c.Register(workflow.Workflow{Name: "resize", Steps: []workflow.Step{{Name: "image", Run: noop}}}); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	var refs []workflow.RunRef
	for range 3 { // three images to resize
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "resize"})
		if err != nil {
			return err
		}
		refs = append(refs, ref)
	}
	// The last run ends, nothing is enqueued for idleGrace: Done closes.
	// snippet:end
	for _, ref := range refs {
		// The run that ends last arms the idle timer as it ends.
		if err := waitStatus(ctx, c, ref, workflow.StatusSucceeded); err != nil {
			return err
		}
	}
	clk.Advance(idleGrace)
	select {
	case <-c.Done():
		fmt.Fprintf(w, "stopped by itself after %s idle\n", idleGrace)
	case <-time.After(runTimeout):
		return errors.New("the idle client did not stop")
	}
	_, err := c.Enqueue(ctx, workflow.Job{Workflow: "resize"})
	fmt.Fprintln(w, "enqueue after the stop: ErrNotStarted =", errors.Is(err, workflow.ErrNotStarted))
	return nil
}

// scheduleHeap compares the default (a goroutine and a timer per
// schedule) with WithScheduleHeap (one of each for all of them): the same
// ticks fire either way.
func scheduleHeap(ctx context.Context, w io.Writer) error {
	for _, heap := range []bool{false, true} {
		clk := clock.NewFake(start)
		var fired atomic.Int32
		opts := []memory.Option{memory.WithClock(clk)}
		// snippet:schedule-heap
		if heap {
			opts = append(opts, memory.WithScheduleHeap()) // one goroutine, one timer
		}
		c := memory.New(opts...)
		// snippet:end
		if err := c.Register(workflow.Workflow{Name: "digest", Steps: []workflow.Step{{Name: "send", Run: func(context.Context, []byte) ([]byte, error) {
			fired.Add(1)
			return nil, nil
		}}}}); err != nil {
			return err
		}
		if err := c.Start(ctx); err != nil {
			return err
		}
		for i := range schedules {
			if _, err := c.Schedule(ctx, workflow.Schedule{Workflow: "digest", Name: fmt.Sprint("tenant-", i), Cron: everyFiveMinutes, Location: time.UTC}); err != nil {
				return err
			}
		}
		want := schedules
		if heap {
			want = 1
		}
		if err := clk.BlockUntil(ctx, want); err != nil {
			return err
		}
		pending := clk.Pending()
		clk.Advance(cronGap)
		deadline := time.Now().Add(runTimeout)
		for fired.Load() < schedules && time.Now().Before(deadline) {
			time.Sleep(statusPoll)
		}
		c.Stop()
		fmt.Fprintf(w, "schedule heap %v: %d schedules, %d pending timer(s), %d fired at 09:05\n", heap, schedules, pending, fired.Load())
	}
	return nil
}

// noop is a step that returns its input.
func noop(_ context.Context, in []byte) ([]byte, error) { return in, nil }

// dropTime removes the time from each log line, so the output is the same
// on every run.
func dropTime(groups []string, a slog.Attr) slog.Attr {
	if len(groups) == 0 && a.Key == slog.TimeKey {
		return slog.Attr{}
	}
	return a
}

// waitStatus polls until the run reads want, or fails after runTimeout or
// when it ends in another state.
func waitStatus(ctx context.Context, c workflow.Client, ref workflow.RunRef, want workflow.Status) error {
	deadline := time.Now().Add(runTimeout)
	for {
		st, err := c.Status(ctx, ref)
		if err != nil {
			return err
		}
		if st == want {
			return nil
		}
		if st.IsTerminal() || time.Now().After(deadline) {
			return fmt.Errorf("run %s is %s, want %s", ref.RunID, st, want)
		}
		time.Sleep(statusPoll)
	}
}
