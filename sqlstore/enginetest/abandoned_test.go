package enginetest

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/sqlstore"
)

// TestAbandonedRunErrorMatchesAfterTheEnd: a step whose only worker dies
// mid-step, with WithMaxReclaims(0), fails at the first takeover with an
// error that still matches sqlstore.ErrAbandoned once the run has ended,
// as an OnRunFinished observer gets it (rebuilt from the stored text), and
// whose text counts one worker and one lease expiry in the singular.
// OnFailure runs for it, not OnTimeout: abandonment is not a deadline.
func TestAbandonedRunErrorMatchesAfterTheEnd(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			var execs, onFailure, onTimeout atomic.Int64
			wf := workflow.Workflow{Name: "abandon", Retry: workflow.RetryPolicy{}, Steps: []workflow.Step{{Name: "only",
				Run: func(ctx context.Context, _ []byte) ([]byte, error) {
					execs.Add(1)
					<-ctx.Done() // the worker "dies" here; a crashed client writes nothing
					return nil, ctx.Err()
				}}},
				OnFailure: func(context.Context, []byte) ([]byte, error) { onFailure.Add(1); return nil, nil },
				OnTimeout: func(context.Context, []byte) ([]byte, error) { onTimeout.Add(1); return nil, nil },
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var mu sync.Mutex
			var ends []workflow.RunEvent
			mk := func() *sqlstore.Client {
				c := newClient(t, e, db, prefix, sqlstore.WithLease(crashLease), sqlstore.WithMaxReclaims(0))
				if err := c.Register(wf); err != nil {
					t.Fatal(err)
				}
				if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) {
					mu.Lock()
					ends = append(ends, ev)
					mu.Unlock()
				}); err != nil {
					t.Fatal(err)
				}
				if err := c.Start(ctx); err != nil {
					t.Fatal(err)
				}
				return c
			}
			c := mk()
			ref, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name})
			if err != nil {
				t.Fatal(err)
			}
			waitFor(t, 10*time.Second, "the first execution", func() bool { return execs.Load() == 1 })
			crash(c)
			c = mk()
			waitFor(t, 10*time.Second, "the run end", func() bool {
				mu.Lock()
				defer mu.Unlock()
				return len(ends) > 0
			})
			mu.Lock()
			ev := ends[0]
			mu.Unlock()
			if ev.Status != workflow.StatusFailed {
				t.Fatalf("run ended %q, want failed", ev.Status)
			}
			if !errors.Is(ev.Err, sqlstore.ErrAbandoned) {
				t.Errorf("errors.Is(RunEvent.Err, sqlstore.ErrAbandoned) = false after the run ended; Err = %v", ev.Err)
			}
			if workflow.IsTimeout(ev.Err) {
				t.Errorf("RunEvent.Err = %v matches ErrTimeout; abandonment is not a deadline", ev.Err)
			}
			msg := ev.Err.Error()
			if !strings.Contains(msg, "abandoned by 1 worker (its lease expired 1 time,") {
				t.Errorf("run error = %q, want it to say \"abandoned by 1 worker (its lease expired 1 time, ...\"", msg)
			}
			if got := execs.Load(); got != 1 {
				t.Errorf("the step executed %d times, want 1 (WithMaxReclaims(0) abandons at the first takeover)", got)
			}
			page, err := c.Runs(ctx, workflow.RunQuery{Ref: ref})
			if err != nil || len(page.Runs) != 1 {
				t.Fatalf("Runs(%v) = %d runs, %v; want 1", ref, len(page.Runs), err)
			}
			if info := page.Runs[0]; !strings.Contains(info.Error, sqlstore.ErrAbandoned.Error()) {
				t.Errorf("RunInfo.Error = %q, want it to name %q", info.Error, sqlstore.ErrAbandoned)
			}
			waitFor(t, 5*time.Second, "OnFailure", func() bool { return onFailure.Load() == 1 })
			if n := onTimeout.Load(); n != 0 {
				t.Errorf("OnTimeout ran %d times for an abandoned run, want 0", n)
			}
		})
	}
}
