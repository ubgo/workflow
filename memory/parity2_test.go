package memory_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

func TestSetWorkersRaisesAQueueCapWhileRunning(t *testing.T) {
	release := make(chan struct{})
	started := make(chan string, 4)
	a := one("a", block(started, release))
	a.Queue = "a"
	c, fc := newClient(t, []workflow.Workflow{a}, memory.WithWorkers(4),
		memory.WithQueues(workflow.QueueConfig{Name: "a", Workers: 1}, workflow.QueueConfig{Name: "b"}))
	for _, p := range []string{"a1", "a2", "a3"} {
		enqueue(t, c, workflow.Job{Workflow: "a", Payload: []byte(p)})
	}
	<-started
	fc.Advance(time.Minute)
	ctx := context.Background()
	loads, err := c.QueueLoads(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(loads) != 2 || loads[0].Queue != "a" || loads[1].Queue != "b" {
		t.Fatalf("loads = %+v, want a then b", loads)
	}
	if l := loads[0]; l.Depth != 2 || l.Running != 1 || l.Workers != 1 || l.OldestWait != time.Minute {
		t.Fatalf("queue a load = %+v, want depth 2, running 1, workers 1, oldest wait 1m", l)
	}
	if l := loads[1]; l.Workers != 4 || l.Depth != 0 || l.OldestWait != 0 {
		t.Fatalf("uncapped queue b load = %+v, want the pool size 4 and no backlog", l)
	}
	select {
	case s := <-started:
		t.Fatalf("%s started over the cap", s)
	case <-time.After(30 * time.Millisecond):
	}
	if err := c.SetWorkers(ctx, "a", 3); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("raising the cap did not start the waiting attempts")
		}
	}
	if err := c.SetWorkers(ctx, "a", 0); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("SetWorkers(0) = %v", err)
	}
	if err := c.SetWorkers(ctx, "nope", 2); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("SetWorkers(unknown) = %v", err)
	}
	close(release)
}

func TestBalanceLoopMovesWorkersToTheBusyQueue(t *testing.T) {
	release := make(chan struct{})
	started := make(chan string, 16)
	a, b := one("a", block(started, release)), one("b", block(started, release))
	a.Queue, b.Queue = "a", "b"
	c, _ := newClient(t, []workflow.Workflow{a, b}, memory.WithWorkers(8),
		memory.WithQueues(workflow.QueueConfig{Name: "a", Workers: 1}, workflow.QueueConfig{Name: "b", Workers: 3}))
	for range 6 {
		enqueue(t, c, workflow.Job{Workflow: "a"})
	}
	loop := workflow.BalanceLoop{Scaler: c, Balancer: workflow.ByDepth(workflow.BalanceConfig{Total: 4})}
	if err := loop.Once(context.Background()); err != nil {
		t.Fatal(err)
	}
	loads, _ := c.QueueLoads(context.Background())
	if loads[0].Workers != 3 || loads[1].Workers != 1 {
		t.Fatalf("after balancing a=%d b=%d, want a 3 (its backlog) and b at its minimum 1", loads[0].Workers, loads[1].Workers)
	}
	close(release)
}

func TestRetryingAndDiscardedOnMemory(t *testing.T) {
	var calls int
	var mu sync.Mutex
	wf := one("flaky", func(context.Context, []byte) ([]byte, error) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 1 {
			return nil, errors.New("transient")
		}
		return nil, workflow.Discard(errors.New("order gone"))
	})
	wf.Retry = workflow.RetryPolicy{Max: 3, Backoff: workflow.Backoff{Initial: time.Minute, Factor: 1}}
	c, fc := newClient(t, []workflow.Workflow{wf})
	ref := enqueue(t, c, workflow.Job{Workflow: "flaky"})
	blockUntil(t, fc, 1)
	waitStatus(t, c, ref, workflow.StatusRetrying)
	page, err := c.Runs(context.Background(), workflow.RunQuery{Ref: ref})
	if err != nil || len(page.Runs) != 1 {
		t.Fatal(page, err)
	}
	if info := page.Runs[0]; info.NextAttempt != 2 || !info.NextAttemptAt.Equal(start.Add(time.Minute)) || info.Error != "transient" {
		t.Fatalf("NextAttempt %d at %s, Error %q; want 2 at %s and the failed attempt's error", info.NextAttempt, info.NextAttemptAt, info.Error, start.Add(time.Minute))
	}
	fc.Advance(time.Minute)
	waitStatus(t, c, ref, workflow.StatusDiscarded)
	page, _ = c.Runs(context.Background(), workflow.RunQuery{Ref: ref})
	if info := page.Runs[0]; info.NextAttempt != 0 || !info.NextAttemptAt.IsZero() || !strings.Contains(info.Error, "order gone") {
		t.Fatalf("discarded run info = %+v; want no next attempt and the reason kept", info)
	}
	if _, err := c.Retry(context.Background(), ref); !errors.Is(err, workflow.ErrWrongState) {
		t.Fatalf("Retry of a discarded run = %v, want ErrWrongState", err)
	}
	if err := c.Delete(context.Background(), ref); err != nil {
		t.Fatalf("Delete of a discarded run: %v", err)
	}
}

func TestChainSurvivesRetryAndLogsAMissingWorkflow(t *testing.T) {
	var buf bytes.Buffer
	var mu sync.Mutex
	var ran []string
	rec := func(name string) workflow.Handler {
		return func(context.Context, []byte) ([]byte, error) {
			mu.Lock()
			ran = append(ran, name)
			mu.Unlock()
			return nil, nil
		}
	}
	fail := true
	twoSteps := workflow.Workflow{Name: "two", Steps: []workflow.Step{
		{Name: "first", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			return nil, workflow.AppendToChain(ctx, workflow.Job{Workflow: "c"})
		}},
		{Name: "second", After: []string{"first"}, Run: func(context.Context, []byte) ([]byte, error) {
			mu.Lock()
			defer mu.Unlock()
			if fail {
				return nil, workflow.Permanent(errors.New("down"))
			}
			return nil, nil
		}},
	}}
	c, _ := newClient(t, []workflow.Workflow{twoSteps, one("b", rec("b")), one("c", rec("c"))},
		memory.WithLogger(slog.New(slog.NewTextHandler(&lockedWriter{w: &buf}, nil))))
	ref := enqueue(t, c, workflow.Job{Workflow: "two", Chain: []workflow.Job{{Workflow: "b"}, {Workflow: "missing"}}})
	waitStatus(t, c, ref, workflow.StatusFailed)
	mu.Lock()
	fail = false
	mu.Unlock()
	if _, err := c.Retry(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, ref, workflow.StatusSucceeded)
	eventually(t, "b to run", func() bool { mu.Lock(); defer mu.Unlock(); return len(ran) == 1 })
	eventually(t, "the missing workflow to be logged", func() bool {
		return strings.Contains((&lockedWriter{w: &buf}).String(), "chained job not enqueued")
	})
	time.Sleep(30 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(ran, ",") != "b" {
		t.Fatalf("ran %v; want b only: the chain ends at the missing workflow, before c (appended by a step that succeeded before the retry)", ran)
	}
}

// lockedWriter serialises writes to a buffer the test also reads.
type lockedWriter struct {
	w *bytes.Buffer
}

var logMu sync.Mutex

func (l *lockedWriter) Write(p []byte) (int, error) {
	logMu.Lock()
	defer logMu.Unlock()
	return l.w.Write(p)
}

func (l *lockedWriter) String() string {
	logMu.Lock()
	defer logMu.Unlock()
	return l.w.String()
}

func TestClosureOnMemory(t *testing.T) {
	c, _ := newClient(t, nil, memory.WithQueues(workflow.QueueConfig{Name: "default"}))
	ctx := context.Background()
	done := make(chan struct{})
	ref, err := c.EnqueueClosure(ctx, workflow.ClosureJob{Name: "ping", Labels: map[string]string{"tenant": "t1"},
		Fn: func(context.Context) error { close(done); return nil }})
	if err != nil {
		t.Fatal(err)
	}
	<-done
	waitStatus(t, c, ref, workflow.StatusSucceeded)
	page, err := c.Runs(ctx, workflow.RunQuery{Workflow: workflow.ClosureWorkflow, Labels: map[string]string{workflow.LabelClosure: "ping"}})
	if err != nil || len(page.Runs) != 1 || page.Runs[0].Labels["tenant"] != "t1" {
		t.Fatalf("closure run listing = %+v, %v", page.Runs, err)
	}
	if _, err := c.EnqueueClosure(ctx, workflow.ClosureJob{Name: "x", Queue: "other", Fn: func(context.Context) error { return nil }}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("closure on an unserved queue = %v", err)
	}
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: workflow.ClosureWorkflow}); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("Enqueue by the closure workflow name = %v, want ErrNotFound", err)
	}

	// A cancelled closure stops: its ctx ends.
	stopped := make(chan struct{})
	ref, err = c.EnqueueClosure(ctx, workflow.ClosureJob{Name: "long", Fn: func(ctx context.Context) error {
		<-ctx.Done()
		close(stopped)
		return ctx.Err()
	}})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, ref, workflow.StatusRunning)
	if err := c.Cancel(ctx, ref); err != nil {
		t.Fatal(err)
	}
	<-stopped
	waitStatus(t, c, ref, workflow.StatusCancelled)
}
