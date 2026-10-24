// Command failure-alerts tells people when runs fail, on the in-memory
// driver with a fake clock: an alert sink (a function standing in for
// Slack, email or a pager) called on final failures and deduplicated per
// workflow and step, with a count of the alerts held back, and an error
// reporter (standing in for an error tracker) that receives each final
// failure and, on opt-in, each failed attempt that will be retried,
// without the payload.
//
// It is the program behind docs/observability/failure-alerts.md: the guide copies
// its regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
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

func (l *lines) add(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.out = append(l.out, fmt.Sprintf(format, args...))
}

func (l *lines) flush(out io.Writer) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, s := range l.out {
		fmt.Fprintln(out, " ", s)
	}
	l.out = nil
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	clk := clock.NewFake(time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC))
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
	ends := make(chan workflow.RunEvent, 8)
	if err := mem.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { ends <- ev }); err != nil {
		return err
	}
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := c.Start(cctx); err != nil {
		return err
	}
	defer mem.Stop()

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

	fmt.Fprintln(out, "-- first failure")
	if err := fail(1); err != nil {
		return err
	}
	got.flush(out)

	fmt.Fprintln(out, "-- two more within 10 minutes: reported, not alerted")
	if err := fail(2); err != nil {
		return err
	}
	got.flush(out)

	fmt.Fprintln(out, "-- 10 minutes later")
	clk.Advance(10 * time.Minute)
	if err := fail(1); err != nil {
		return err
	}
	got.flush(out)
	return nil
}

// waitLines waits until the collected lines stop changing.
func waitLines(l *lines) error {
	prev := -1
	for range 200 {
		time.Sleep(5 * time.Millisecond)
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
