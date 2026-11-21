// Command admin-alerts watches queues with admin.Monitor on the in-memory
// driver, with fake clocks so every time it prints is fixed: a paused
// queue grows too deep (AlertQueueDepth), its oldest run waits too long
// (AlertQueueWait), the depth alert is sent again while it stays over
// (WithRepeat), a poll fails while the run store is down and reaches the
// error handler (WithErrorHandler) without stopping the Monitor, and both
// alerts resolve once the queue drains.
//
// It is the program behind docs/admin/alerts.md: the guide copies its
// regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/admin"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "admin-alerts:", err)
		os.Exit(1)
	}
}

// start is the fake clocks' first instant, so every time printed is fixed.
var start = time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)

// The example's queue, its backlog and the Monitor's timings.
const (
	queueEmails = "emails"
	backlog     = 3                // runs held in the paused queue
	maxDepth    = 2                // the emails queue alerts above this depth
	maxWait     = 10 * time.Minute // or when its oldest run waits longer
	anyDepth    = 1000             // every other queue alerts above this depth
	interval    = 30 * time.Second // how often Run polls
	repeat      = 15 * time.Minute // an open alert is sent again this often
)

// errStoreDown is what the run store answers while it is unreachable.
var errStoreDown = errors.New("run store: connection refused")

// flaky is the run store the Admin reads, which can be switched off to
// show a failed poll.
type flaky struct {
	workflow.RunQuerier
	down atomic.Bool
}

// Runs fails while the store is down.
func (f *flaky) Runs(ctx context.Context, q workflow.RunQuery) (workflow.RunPage, error) {
	if f.down.Load() {
		return workflow.RunPage{}, errStoreDown
	}
	return f.RunQuerier.Runs(ctx, q)
}

// outbox collects what the Monitor sends from its own goroutine, so run
// prints one poll's lines in a fixed order: the alerts that resolve on
// one poll come in map order.
type outbox struct {
	mu    sync.Mutex
	lines []string
}

// add records one line; the Monitor calls it from its own goroutine.
func (o *outbox) add(s string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.lines = append(o.lines, s)
}

// flush prints and forgets the lines collected so far, sorted.
func (o *outbox) flush(w io.Writer) {
	o.mu.Lock()
	defer o.mu.Unlock()
	slices.Sort(o.lines)
	for _, l := range o.lines {
		fmt.Fprintln(w, l)
	}
	o.lines = nil
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, w io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// The driver and the Monitor each get a fake clock, moved together, so
	// the Monitor's poll timer is the only timer on its clock.
	runClk, monClk := clock.NewFake(start), clock.NewFake(start)
	// advance moves both fake clocks by d, keeping them in step.
	advance := func(d time.Duration) { runClk.Advance(d); monClk.Advance(d) }

	c := memory.New(memory.WithClock(runClk))
	if err := c.Register(workflow.Workflow{Name: "send-email", Queue: queueEmails, Steps: []workflow.Step{{Name: "send",
		Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}}); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	defer c.Stop()
	store := &flaky{RunQuerier: c}
	adm := admin.New(c, admin.WithClock(monClk), admin.WithRunQuerier(store, c.Capabilities()))
	sent := &outbox{}

	// snippet:monitor
	// alerter turns each alert, raised or resolved, into one line.
	alerter := admin.AlerterFunc(func(_ context.Context, a admin.Alert) {
		state, over := "ALERT", ">"
		if a.Resolved {
			state, over = "resolved", "<="
		}
		switch a.Kind {
		case admin.AlertQueueDepth:
			sent.add(fmt.Sprintf("%s %s %s %s: depth %d %s %d", a.At.Format(time.TimeOnly), state, a.Kind, a.Queue, a.Depth, over, a.Threshold.MaxDepth))
		case admin.AlertQueueWait:
			sent.add(fmt.Sprintf("%s %s %s %s: oldest wait %s %s %s", a.At.Format(time.TimeOnly), state, a.Kind, a.Queue, a.Wait, over, a.Threshold.MaxWait))
		}
	})
	m, err := admin.NewMonitor(adm, alerter,
		[]admin.Threshold{
			{Queue: queueEmails, MaxDepth: maxDepth, MaxWait: maxWait}, // this queue's own threshold
			{MaxDepth: anyDepth}, // the default for every queue without its own
		},
		admin.WithInterval(interval),
		admin.WithRepeat(repeat),
		admin.WithErrorHandler(func(err error) { // failed polls and alerter panics
			sent.add(fmt.Sprintf("%s monitor error: %v", monClk.Now().Format(time.TimeOnly), err))
		}),
	)
	if err != nil {
		return err
	}
	// snippet:end

	// The emails queue is paused, so its runs pile up.
	if err := c.Pause(ctx, workflow.PauseQueue(queueEmails)); err != nil {
		return err
	}
	for range backlog {
		if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "send-email"}); err != nil {
			return err
		}
	}

	fmt.Fprintln(w, "-- Check polls once")
	if err := m.Check(ctx); err != nil {
		return err
	}
	sent.flush(w)
	advance(maxWait + time.Minute)
	if err := m.Check(ctx); err != nil {
		return err
	}
	sent.flush(w)
	_, err = admin.NewMonitor(adm, alerter, []admin.Threshold{{Queue: queueEmails}})
	fmt.Fprintln(w, "a threshold that checks nothing:", errors.Is(err, workflow.ErrInvalid))
	_, err = admin.NewMonitor(adm, alerter, []admin.Threshold{{Queue: queueEmails, MaxDepth: 1}, {Queue: queueEmails, MaxWait: time.Minute}})
	fmt.Fprintln(w, "two thresholds for one queue:", errors.Is(err, workflow.ErrInvalid))

	fmt.Fprintln(w, "-- Run polls every interval")
	// snippet:run
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- m.Run(runCtx) }() // returns when runCtx ends
	// snippet:end
	defer stop()
	// poll moves the clocks one interval and waits until Run has polled
	// and armed its next timer.
	poll := func() error {
		if err := monClk.BlockUntil(ctx, 1); err != nil {
			return err
		}
		advance(interval)
		if err := monClk.BlockUntil(ctx, 1); err != nil {
			return err
		}
		sent.flush(w)
		return nil
	}
	store.down.Store(true)
	if err := poll(); err != nil { // the store is down: the error handler hears of it
		return err
	}
	store.down.Store(false)
	// Run's timer fires during this move and polls at 09:15:00, fifteen
	// minutes after the depth alert was first sent: WithRepeat sends it
	// again. The wait alert, sent at 09:11:00, is not due yet.
	advance(repeat - maxWait - time.Minute - interval)
	if err := poll(); err != nil {
		return err
	}
	if err := c.Resume(ctx, workflow.PauseQueue(queueEmails)); err != nil {
		return err
	}
	if _, err := c.RunAll(ctx); err != nil {
		return err
	}
	if err := poll(); err != nil { // the queue drained: both alerts resolve
		return err
	}
	stop()
	fmt.Fprintln(w, "Run returned:", <-done)
	return nil
}
