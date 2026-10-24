package sqlstore_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/sqlstore"
)

// TestQueueLimitIsClusterWideAndSurvivesCrash: two clients with Limit 2
// never run more than 2 attempts of the queue at once between them, and
// when a client dies holding slots they are freed by lease expiry (not
// leaked): every run still finishes.
func TestQueueLimitIsClusterWideAndSurvivesCrash(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			var now, peak atomic.Int64
			var execs atomic.Int64
			wf := workflow.Workflow{Name: "lim", Queue: "q", Steps: []workflow.Step{{Name: "a", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
				n := now.Add(1)
				defer now.Add(-1)
				for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
				}
				if execs.Add(1) <= 2 {
					<-ctx.Done() // the first two attempts hold their slots until their client crashes
					return nil, ctx.Err()
				}
				time.Sleep(30 * time.Millisecond)
				return nil, nil
			}}}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var finished atomic.Int64
			mk := func() *sqlstore.Client {
				c := newClient(t, e, db, prefix, sqlstore.WithWorkers(4),
					sqlstore.WithQueues(workflow.QueueConfig{Name: "q", Limit: 2}))
				if err := c.Register(wf); err != nil {
					t.Fatal(err)
				}
				if err := c.OnRunFinished(func(context.Context, workflow.RunEvent) { finished.Add(1) }); err != nil {
					t.Fatal(err)
				}
				if err := c.Start(ctx); err != nil {
					t.Fatal(err)
				}
				return c
			}
			a := mk()
			for range 6 {
				if _, err := a.Enqueue(ctx, workflow.Job{Workflow: wf.Name}); err != nil {
					t.Fatal(err)
				}
			}
			waitFor(t, 10*time.Second, "two attempts holding the two slots", func() bool { return execs.Load() >= 2 })
			time.Sleep(200 * time.Millisecond)
			if got := execs.Load(); got != 2 {
				t.Fatalf("%d attempts started with Limit 2 and both slots held, want 2", got)
			}
			sqlstore.Crash(a)
			b := mk() // a second node; the dead one's slots free when their leases expire
			_ = b
			waitFor(t, 20*time.Second, "all runs to finish after the crash", func() bool { return finished.Load() >= 6 })
			if got := peak.Load(); got > 2 {
				t.Fatalf("peak concurrent attempts = %d, want at most the Limit 2", got)
			}
		})
	}
}
