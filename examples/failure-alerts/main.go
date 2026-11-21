// Command failure-alerts tells people when runs fail, on the in-memory
// driver with a fake clock: an alert sink (a function standing in for
// Slack, email or a pager) called on final failures and deduplicated per
// workflow and step, with a count of the alerts held back, and an error
// reporter (standing in for an error tracker) that receives each final
// failure and, on opt-in, each failed attempt that will be retried,
// without the payload. A second part keys alerts per tenant, filters out a
// test tenant and caps alerts across every key with a MemoryAlertLimiter
// of its own, on the bare Alerts observer.
//
// It is the program behind docs/observability/failure-alerts.md: the guide copies
// its regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

// The fake clocks' first instants: fixed, so the output never varies.
var (
	// start is where the first part's clock begins.
	start = time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC)
	// limitsStart is where the second part's clock begins.
	limitsStart = time.Date(2026, 1, 5, 12, 0, 0, 0, time.UTC)
)

const (
	// endsBuffer is the room of the run-end channel: more than the runs
	// one fail call waits for, so the observer never blocks the driver.
	endsBuffer = 8
	// heldBack is how many failures the first part sends inside the dedup
	// window: reported but not alerted, they become the next alert's
	// "(and 2 more)".
	heldBack = 2
	// defaultDedup is AlertPlugin's default dedup window, the time the
	// first part moves the clock to let the next alert out.
	defaultDedup = 10 * time.Minute
	// settlePolls and settlePoll bound waitLines: up to a second for the
	// observers to finish, checked often so the demo stays quick.
	settlePolls = 200
	settlePoll  = 5 * time.Millisecond
)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "failure-alerts:", err)
		os.Exit(1)
	}
}

// lines collects what the sinks print, since they run on driver
// goroutines.
type lines struct {
	mu  sync.Mutex
	out []string
}

// add records one formatted line; safe from any goroutine.
func (l *lines) add(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.out = append(l.out, fmt.Sprintf(format, args...))
}

// flush prints the lines collected so far, indented, and clears them.
func (l *lines) flush() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, s := range l.out {
		fmt.Println(" ", s)
	}
	l.out = nil
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	clk := clock.NewFake(start)
	mem := memory.New(memory.WithClock(clk))
	var got lines

	// snippet:sinks
	// The app's own senders: here they print, in production they post to
	// Slack or call an error tracker's SDK.
	slack := middleware.AlerterFunc(func(_ context.Context, a middleware.Alert) error {
		got.add("ALERT %s: %v (and %d more)", a.Key, a.Err, a.Suppressed)
		return nil
	})
	tracker := workflow.ReporterFunc(func(_ context.Context, f workflow.Failure) {
		got.add("report %s step=%s final=%t attempt=%d tenant=%s: %v", f.Workflow, f.Step, f.Final, f.Attempt, f.Labels["tenant"], f.Err)
	})
	// snippet:end

	// snippet:plugins
	c, err := workflow.Use(mem,
		// One alert per workflow and step every 10 minutes (the default
		// dedup), on the fake clock so the demo can move time.
		middleware.AlertPlugin(slack, middleware.AlertConfig{Clock: clk}),
		// Every final failure, plus every failed attempt that will be
		// retried.
		middleware.Report(tracker, middleware.ReportConfig{Attempts: true, Clock: clk}),
	)
	// snippet:end
	if err != nil {
		return err
	}

	err = c.Register(workflow.Workflow{
		Name:  "charge",
		Retry: workflow.RetryPolicy{Max: 1}, // one retry, at once
		Steps: []workflow.Step{{Name: "capture", Run: func(context.Context, []byte) ([]byte, error) {
			return nil, errors.New("card declined")
		}}},
	})
	if err != nil {
		return err
	}
	ends := make(chan workflow.RunEvent, endsBuffer)
	if err := mem.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { ends <- ev }); err != nil {
		return err
	}
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := c.Start(cctx); err != nil {
		return err
	}
	defer mem.Stop()

	// fail enqueues n charges for acme, waits for each to end and for the
	// sinks to finish.
	fail := func(n int) error {
		for range n {
			_, err := c.Enqueue(ctx, workflow.Job{Workflow: "charge", Payload: []byte(`{"card":"4242"}`), Labels: map[string]string{"tenant": "acme"}})
			if err != nil {
				return err
			}
			<-ends
		}
		// The alert and report observers run beside the one above; give
		// them a moment to land.
		return waitLines(&got)
	}

	fmt.Println("-- first failure")
	if err := fail(1); err != nil {
		return err
	}
	got.flush()

	fmt.Println("-- two more within 10 minutes: reported, not alerted")
	if err := fail(heldBack); err != nil {
		return err
	}
	got.flush()

	fmt.Println("-- 10 minutes later")
	clk.Advance(defaultDedup)
	if err := fail(1); err != nil {
		return err
	}
	got.flush()
	return limits(ctx)
}

// Rules of the second part, small so the demo shows the cap: an alert per
// tenant every 10 minutes, at most two alerts an hour across all tenants.
const (
	tenantDedup = 10 * time.Minute
	alertCap    = 2
	capWindow   = time.Hour
)

// limits shows the dedup key, the filter and the rate cap: three tenants
// fail at once and a test tenant fails too; the cap lets two alerts out,
// the test tenant never alerts, and the held-back tenant alerts an hour
// later with the count of what was held back.
func limits(ctx context.Context) error {
	clk := clock.NewFake(limitsStart)
	mem := memory.New(memory.WithClock(clk))
	var got lines
	// slack records each alert with its key and held-back count.
	slack := middleware.AlerterFunc(func(_ context.Context, a middleware.Alert) error {
		got.add("ALERT %s (and %d more)", a.Key, a.Suppressed)
		return nil
	})

	// snippet:limiter
	limiter := middleware.NewMemoryAlertLimiter(clk, middleware.AlertLimits{
		Dedup: tenantDedup, // one alert per key in this window
		Max:   alertCap,    // and at most this many alerts, all keys together,
		Per:   capWindow,   // in any sliding window this long
	})
	alerts := middleware.Alerts(slack, middleware.AlertConfig{
		// One alert per workflow and tenant, not per workflow and step.
		Key: func(f workflow.Failure) string { return f.Workflow + "/" + f.Labels["tenant"] },
		// The load-test tenant fails on purpose: never page for it.
		When:    func(f workflow.Failure) bool { return f.Labels["tenant"] != "loadtest" },
		Limiter: limiter,
		Clock:   clk,
	})
	if err := mem.OnRunFinished(alerts); err != nil { // the bare observer; AlertPlugin wraps it for workflow.Use
		return err
	}
	// snippet:end

	err := mem.Register(workflow.Workflow{
		Name: "charge",
		Steps: []workflow.Step{{Name: "capture", Run: func(context.Context, []byte) ([]byte, error) {
			return nil, errors.New("card declined")
		}}},
	})
	if err != nil {
		return err
	}
	ends := make(chan workflow.RunEvent, endsBuffer)
	if err := mem.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { ends <- ev }); err != nil {
		return err
	}
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := mem.Start(cctx); err != nil {
		return err
	}
	defer mem.Stop()

	// fail enqueues one charge per tenant, waits for each to end and for
	// the sink to finish.
	fail := func(tenants ...string) error {
		for _, t := range tenants {
			if _, err := mem.Enqueue(ctx, workflow.Job{Workflow: "charge", Labels: map[string]string{"tenant": t}}); err != nil {
				return err
			}
			<-ends
		}
		return waitLines(&got)
	}

	fmt.Println("-- per tenant, at most 2 alerts an hour: acme, globex, initech and loadtest fail")
	if err := fail("acme", "globex", "initech", "loadtest"); err != nil {
		return err
	}
	got.flush()

	fmt.Println("-- an hour later initech fails again")
	clk.Advance(capWindow)
	if err := fail("initech"); err != nil {
		return err
	}
	got.flush()
	return nil
}

// waitLines waits until the collected lines stop changing.
func waitLines(l *lines) error {
	prev := -1
	for range settlePolls {
		time.Sleep(settlePoll)
		l.mu.Lock()
		n := len(l.out)
		l.mu.Unlock()
		if n == prev && n > 0 {
			sortLines(l)
			return nil
		}
		prev = n
	}
	return errors.New("the sinks were not called")
}

// sortLines orders the lines so the output does not depend on which
// observer ran first.
func sortLines(l *lines) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i := 1; i < len(l.out); i++ {
		for j := i; j > 0 && strings.Compare(l.out[j-1], l.out[j]) > 0; j-- {
			l.out[j-1], l.out[j] = l.out[j], l.out[j-1]
		}
	}
}
