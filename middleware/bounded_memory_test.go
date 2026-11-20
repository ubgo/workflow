package middleware_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

// TestBoundedMemoryQueue proves IO-04: the memory driver behind Bounded
// holds at most the limit of waiting runs, so a burst cannot grow the
// process's memory without end. Past the limit an enqueue is refused with
// ErrQueueFull; with BoundedWait the producer blocks until a worker takes
// a run, and then goes through.
func TestBoundedMemoryQueue(t *testing.T) {
	const limit = 3
	m := memory.New(memory.WithWorkers(1))
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) } // frees the worker, also when an assertion fails
	if err := m.Register(workflow.Workflow{Name: "upload", Steps: []workflow.Step{{Name: "send", Run: func(context.Context, []byte) ([]byte, error) {
		<-gate
		return nil, nil
	}}}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { release(); cancel(); m.Stop() }()
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	depth, err := middleware.DepthFromRuns(m)
	if err != nil {
		t.Fatal(err)
	}
	limits := middleware.QueueLimits(map[string]int{workflow.DefaultQueue: limit})
	refuse := middleware.WrapClient(m, middleware.Bounded(depth, limits))
	block := middleware.WrapClient(m, middleware.Bounded(depth, limits, middleware.BoundedWait(2*time.Millisecond, clock.Real{})))

	first, err := refuse.Enqueue(ctx, workflow.Job{Workflow: "upload"})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, m, first, workflow.StatusRunning) // the one worker holds it; it is not waiting
	for i := range limit {
		if _, err := refuse.Enqueue(ctx, workflow.Job{Workflow: "upload"}); err != nil {
			t.Fatalf("waiting run %d of %d refused: %v", i+1, limit, err)
		}
	}
	if _, err := refuse.Enqueue(ctx, workflow.Job{Workflow: "upload"}); !errors.Is(err, middleware.ErrQueueFull) {
		t.Fatalf("enqueue past the bound: %v, want ErrQueueFull", err)
	}
	page, err := m.Runs(ctx, workflow.RunQuery{Workflow: "upload"})
	if err != nil || len(page.Runs) != limit+1 {
		t.Fatalf("the driver holds %d runs (%v), want %d", len(page.Runs), err, limit+1)
	}

	done := make(chan error, 1)
	go func() { _, err := block.Enqueue(ctx, workflow.Job{Workflow: "upload"}); done <- err }()
	select {
	case err := <-done:
		t.Fatalf("BoundedWait went through a full queue: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	gate <- struct{}{} // the worker finishes one run and takes the next
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("BoundedWait after room was made: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("BoundedWait still blocked after a worker took a run")
	}
	release()
}

func waitStatus(t *testing.T, c workflow.Client, ref workflow.RunRef, want workflow.Status) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, err := c.Status(context.Background(), ref)
		if err == nil && st == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s is %s (%v), want %s", ref.RunID, st, err, want)
		}
		time.Sleep(time.Millisecond)
	}
}
