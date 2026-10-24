package middleware_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

func TestRelease_ReleasesWithoutRunning(t *testing.T) {
	calls := 0
	h := middleware.Release(func(_ context.Context, in []byte) bool { return string(in) == "busy" }, 30*time.Second)(
		func(_ context.Context, in []byte) ([]byte, error) { calls++; return in, nil })
	ctx := context.Background()

	out, err := h(ctx, []byte("busy"))
	if !errors.Is(err, middleware.ErrReleased) || out != nil || calls != 0 {
		t.Fatalf("released: out=%q err=%v calls=%d", out, err, calls)
	}
	if d, ok := workflow.RetryAfterDelay(err, 1); !ok || d != 30*time.Second {
		t.Fatalf("RetryAfterDelay = %v, %v; want 30s", d, ok)
	}
	if workflow.IsPermanent(err) {
		t.Fatal("a release must be retryable")
	}
	if out, err := h(ctx, []byte("free")); err != nil || string(out) != "free" || calls != 1 {
		t.Fatalf("not released: out=%q err=%v calls=%d", out, err, calls)
	}
}

func TestRelease_NilConditionAndNegativeDelay(t *testing.T) {
	if _, err := middleware.Release(nil, time.Second)(echo)(context.Background(), nil); err != nil {
		t.Fatalf("nil condition: %v", err)
	}
	_, err := middleware.Release(func(context.Context, []byte) bool { return true }, -time.Second)(echo)(context.Background(), nil)
	if d, ok := workflow.RetryAfterDelay(err, 1); !ok || d != 0 {
		t.Fatalf("negative delay: %v, %v", d, ok)
	}
}

// TestRelease_OnMemoryDriver proves a release waits its delay under a
// policy that reads RetryAfter, counts as an attempt, and that the step
// runs once the condition clears.
func TestRelease_OnMemoryDriver(t *testing.T) {
	clk := clock.NewFake(time.Unix(0, 0))
	var busy atomic.Bool
	busy.Store(true)
	ran := make(chan struct{}, 1)
	c := memory.New(memory.WithClock(clk), memory.WithMiddleware(
		middleware.Release(func(context.Context, []byte) bool { return busy.Load() }, time.Minute)))
	err := c.Register(workflow.Workflow{
		Name:  "w",
		Retry: workflow.RetryPolicy{Max: 2, Backoff: workflow.Backoff{Initial: time.Hour}, DelayFor: workflow.RetryAfterDelay},
		Steps: []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) { ran <- struct{}{}; return nil, nil }}},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Stop()
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "w"})
	if err != nil {
		t.Fatal(err)
	}
	// The first attempt is released and the retry timer waits a minute,
	// not the hour of the policy's Backoff.
	waitFor(t, "retry timer armed", func() bool { return clk.Pending() > 0 })
	busy.Store(false)
	clk.Advance(time.Minute)
	select {
	case <-ran:
	case <-time.After(5 * time.Second):
		t.Fatal("step did not run a minute after the release")
	}
	waitFor(t, "run succeeded", func() bool { st, _ := c.Status(ctx, ref); return st == workflow.StatusSucceeded })
}

func TestUniqueUntilStarted_FreesTheLockWhenAStepStarts(t *testing.T) {
	store := middleware.NewMemoryUnique(clock.NewFake(time.Unix(0, 0)))
	stub := &stubClient{}
	by := func(j workflow.Job) string { return string(j.Payload) }
	enq, step := middleware.UniqueUntilStarted(by, store, time.Hour)
	c := middleware.WrapClient(stub, enq)
	ctx := context.Background()
	job := workflow.Job{Workflow: "w", Payload: []byte("a")}

	if _, err := c.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Enqueue(ctx, job); !errors.Is(err, workflow.ErrDuplicate) {
		t.Fatalf("duplicate passed before the run started: %v", err)
	}
	first := stub.jobs[0].Labels
	run := func(labels map[string]string) {
		t.Helper()
		sctx := workflow.WithStepInfo(ctx, workflow.StepInfo{Workflow: "w", Step: "s", Attempt: 1, Labels: labels})
		if _, err := step(echo)(sctx, nil); err != nil {
			t.Fatal(err)
		}
	}
	run(first)
	if _, err := c.Enqueue(ctx, job); err != nil {
		t.Fatalf("lock kept after the step started: %v", err)
	}
	// A later step (or a retry) of the first run must not free the lock
	// the second job now holds: the owner token differs.
	run(first)
	if _, err := c.Enqueue(ctx, job); !errors.Is(err, workflow.ErrDuplicate) {
		t.Fatalf("the first run's later step freed a newer holder's lock: %v", err)
	}
	// No StepInfo, or a run without unique labels, passes through.
	if _, err := step(echo)(ctx, nil); err != nil {
		t.Fatal(err)
	}
	run(map[string]string{"tenant": "t1"})
	if _, err := c.Enqueue(ctx, job); !errors.Is(err, workflow.ErrDuplicate) {
		t.Fatalf("a step without the labels freed the lock: %v", err)
	}
}

// TestUniqueUntilStarted_OnMemoryDriver runs both halves on the real
// driver: the second enqueue is refused while the run waits for its delay,
// and accepted while the first run's step is still running.
func TestUniqueUntilStarted_OnMemoryDriver(t *testing.T) {
	clk := clock.NewFake(time.Unix(0, 0))
	by := func(j workflow.Job) string { return j.Labels["order"] }
	enq, step := middleware.UniqueUntilStarted(by, middleware.NewMemoryUnique(clk), time.Hour)
	mem := memory.New(memory.WithClock(clk), memory.WithMiddleware(step))
	c := middleware.WrapClient(mem, enq)

	started, finish := make(chan struct{}, 2), make(chan struct{})
	err := c.Register(workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) {
		started <- struct{}{}
		<-finish
		return nil, nil
	}}}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer mem.Stop()
	defer close(finish)

	job := workflow.Job{Workflow: "w", Labels: map[string]string{"order": "o1"}, RunAt: clk.Now().Add(time.Minute)}
	if _, err := c.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Enqueue(ctx, job); !errors.Is(err, workflow.ErrDuplicate) {
		t.Fatalf("duplicate passed while the run waits: %v", err)
	}
	clk.Advance(time.Minute)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("run did not start")
	}
	job.RunAt = time.Time{}
	if _, err := c.Enqueue(ctx, job); err != nil {
		t.Fatalf("lock kept while the first run's step runs: %v", err)
	}
}
