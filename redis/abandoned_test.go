package redis_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	wfredis "github.com/ubgo/workflow/redis"
)

// TestAbandonedRunErrorMatchesAfterTheEnd: a run whose only worker dies
// mid-step, with WithMaxReclaims(0), fails at the first takeover with an
// error that still matches redis.ErrAbandoned once the run has ended, as
// an OnRunFinished observer gets it (rebuilt from the stored text), and
// RunInfo.Error keeps the text. It does not match ErrTimeout, and
// OnTimeout does not run: abandonment is not a deadline.
func TestAbandonedRunErrorMatchesAfterTheEnd(t *testing.T) {
	rdb := redisClient(t)
	prefix := testPrefix(t, rdb)
	var execs, onTimeout atomic.Int64
	blocked := make(chan struct{})
	wf := workflow.Workflow{Name: "abandon", Steps: []workflow.Step{{Name: "only",
		Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			execs.Add(1)
			<-blocked // the process is gone: this attempt never returns in time
			return nil, ctx.Err()
		}}},
		OnTimeout: func(context.Context, []byte) ([]byte, error) { onTimeout.Add(1); return nil, nil },
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	var mu sync.Mutex
	var ends []workflow.RunEvent
	mk := func() *wfredis.Client {
		c := newClient(t, rdb, prefix, wfredis.WithLease(time.Second), wfredis.WithMaxReclaims(0))
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
	victim := mk()
	// Registered after the client, so it runs before the client's Stop
	// (cleanups run last first) and frees the victim's blocked attempt.
	t.Cleanup(func() { close(blocked) })
	ref, err := victim.Enqueue(ctx, workflow.Job{Workflow: wf.Name})
	if err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 10*time.Second, "the first execution", func() bool { return execs.Load() == 1 })
	wfredis.Crash(victim)
	c := mk()
	waitUntil(t, 20*time.Second, "the run end", func() bool {
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
	if !errors.Is(ev.Err, wfredis.ErrAbandoned) {
		t.Errorf("errors.Is(RunEvent.Err, redis.ErrAbandoned) = false after the run ended; Err = %v", ev.Err)
	}
	if workflow.IsTimeout(ev.Err) {
		t.Errorf("RunEvent.Err = %v matches ErrTimeout; abandonment is not a deadline", ev.Err)
	}
	if msg := ev.Err.Error(); !strings.Contains(msg, "its lease expired 1 time in a row") {
		t.Errorf("run error = %q, want it to say \"its lease expired 1 time in a row\"", msg)
	}
	if got := execs.Load(); got != 1 {
		t.Errorf("the step executed %d times, want 1 (WithMaxReclaims(0) abandons at the first takeover)", got)
	}
	page, err := c.Runs(ctx, workflow.RunQuery{Ref: ref})
	if err != nil || len(page.Runs) != 1 {
		t.Fatalf("Runs(%v) = %d runs, %v; want 1", ref, len(page.Runs), err)
	}
	if info := page.Runs[0]; !strings.HasPrefix(info.Error, wfredis.ErrAbandoned.Error()) {
		t.Errorf("RunInfo.Error = %q, want it to start with %q", info.Error, wfredis.ErrAbandoned)
	}
	// The driver ends an abandoned run without its failure handler (it
	// fails before any step runs); OnTimeout must not run either.
	if n := onTimeout.Load(); n != 0 {
		t.Errorf("OnTimeout ran %d times for an abandoned run, want 0", n)
	}
}

// waitUntil polls cond every 10ms until it holds, or fails t after limit.
func waitUntil(t *testing.T, limit time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", limit, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
