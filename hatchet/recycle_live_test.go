package hatchet_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/hatchet"
)

// TestRecycleLive proves Config.Recycle on the engine: workflow.Serve
// returns a max_jobs RecycleError once the worker has finished MaxJobs
// attempts, the worker takes no further run (the rest stay queued on the
// engine), and a fresh client, the restarted process, finishes every
// remaining run, each exactly once.
func TestRecycleLive(t *testing.T) {
	skipWithoutEngine(t)
	prefix := "wfrecycle_" + uuid.NewString()[:8]
	const runs, maxJobs = 4, 2
	var mu sync.Mutex
	ran := map[string]int{}
	wf := workflow.Workflow{Name: prefix, Steps: []workflow.Step{{Name: "a", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		// Long enough that the pause lands before the engine would hand
		// this one-slot worker the next run.
		time.Sleep(500 * time.Millisecond)
		mu.Lock()
		ran[string(in)]++
		mu.Unlock()
		return nil, nil
	}}}}
	newClient := func(recycle workflow.Recycle) *hatchet.Client {
		c := hatchet.New(hatchet.Config{WorkerName: prefix, Slots: 1, Recycle: recycle})
		if err := c.Register(wf); err != nil {
			t.Fatal(err)
		}
		return c
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c := newClient(workflow.Recycle{MaxJobs: maxJobs})
	served := make(chan error, 1)
	go func() { served <- workflow.Serve(ctx, c) }()
	// Serve starts the client; enqueue once it is up.
	var refs []workflow.RunRef
	deadline := time.Now().Add(liveWait)
	for len(refs) < runs {
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, Payload: []byte(`"` + uuid.NewString() + `"`)})
		if errors.Is(err, workflow.ErrNotStarted) && time.Now().Before(deadline) {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, ref)
	}
	var err error
	select {
	case err = <-served:
	case <-time.After(liveWait):
		t.Fatal("Serve did not return after MaxJobs attempts")
	}
	var re *workflow.RecycleError
	if !errors.Is(err, workflow.ErrRecycle) || !errors.As(err, &re) || re.Reason != workflow.RecycleMaxJobs || re.Jobs != maxJobs {
		t.Fatalf("Serve = %v, want a max_jobs RecycleError after %d jobs", err, maxJobs)
	}
	mu.Lock()
	before := len(ran)
	mu.Unlock()
	if before >= runs {
		t.Fatalf("the recycled worker ran %d of %d runs, want some left for the next process", before, runs)
	}
	// The restarted process takes the rest.
	next := newClient(workflow.Recycle{})
	if err := next.Start(ctx); err != nil {
		t.Fatal(err)
	}
	for _, ref := range refs {
		waitStatus(t, next, ref, workflow.StatusSucceeded)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(ran) != runs {
		t.Fatalf("%d distinct runs ran, want %d", len(ran), runs)
	}
	for in, n := range ran {
		if n != 1 {
			t.Fatalf("run %s ran %d times, want once", in, n)
		}
	}
	t.Logf("recycled after %d jobs; %d runs ran before the restart", re.Jobs, before)
}
