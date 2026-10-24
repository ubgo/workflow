package sqlstore_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/sqlstore"
)

// echoWorkflow is a one-step workflow that returns its input.
func echoWorkflow(name string) workflow.Workflow {
	return workflow.Workflow{Name: name, Steps: []workflow.Step{{Name: "a",
		Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}}
}

// waitDone waits for c to stop.
func waitDone(t *testing.T, c *sqlstore.Client) {
	t.Helper()
	select {
	case <-c.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("the client did not stop")
	}
}

// recycleReason is err's RecycleError reason, failing t when err is not
// one.
func recycleReason(t *testing.T, err error) workflow.RecycleReason {
	t.Helper()
	var re *workflow.RecycleError
	if !errors.Is(err, workflow.ErrRecycle) || !errors.As(err, &re) {
		t.Fatalf("stop cause %v, want a *workflow.RecycleError", err)
	}
	return re.Reason
}

// TestRecycleMaxJobs: one worker recycles after MaxJobs attempts, StopCause
// is the RecycleError, and the runs it never claimed stay queued in
// the database for the next client, which runs them.
func TestRecycleMaxJobs(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			c := newClient(t, e, db, prefix, sqlstore.WithWorkers(1), sqlstore.WithRecycle(workflow.Recycle{MaxJobs: 3}))
			ctx := startWith(t, c, echoWorkflow("w"))
			// The workflow is paused while the five runs are enqueued, so
			// all five are queued before the one worker claims any.
			if err := c.Pause(ctx, workflow.PauseWorkflow("w")); err != nil {
				t.Fatal(err)
			}
			var refs []workflow.RunRef
			for range 5 {
				ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "w"})
				if err != nil {
					t.Fatal(err)
				}
				refs = append(refs, ref)
			}
			if err := c.Resume(ctx, workflow.PauseWorkflow("w")); err != nil {
				t.Fatal(err)
			}
			waitDone(t, c)
			if got := recycleReason(t, c.StopCause()); got != workflow.RecycleMaxJobs {
				t.Fatalf("reason %s, want %s", got, workflow.RecycleMaxJobs)
			}
			done := 0
			for _, ref := range refs {
				st, err := c.Status(ctx, ref)
				if err != nil {
					t.Fatal(err)
				}
				switch st {
				case workflow.StatusSucceeded:
					done++
				case workflow.StatusQueued:
				default:
					t.Fatalf("a run is %s after the recycle, want succeeded or queued", st)
				}
			}
			if done != 3 {
				t.Fatalf("%d runs finished before the recycle, want 3", done)
			}

			next := newClient(t, e, db, prefix)
			startWith(t, next, echoWorkflow("w"))
			for _, ref := range refs {
				waitStatus(t, next, ref, workflow.StatusSucceeded)
			}
			if next.StopCause() != nil {
				t.Fatalf("a running client reports stop cause %v", next.StopCause())
			}
		})
	}
}

// TestRecycleMaxTime: the client recycles once MaxTime has passed on the
// scheduling clock, and not before.
func TestRecycleMaxTime(t *testing.T) {
	e := engines(t)[0]
	db, prefix := e.open(t)
	clk := clock.NewFake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	c := newClient(t, e, db, prefix, sqlstore.WithClock(clk), sqlstore.WithRecycle(workflow.Recycle{MaxTime: time.Hour}))
	startWith(t, c, echoWorkflow("w"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := clk.BlockUntil(ctx, 1); err != nil {
		t.Fatal(err)
	}
	clk.Advance(59 * time.Minute)
	select {
	case <-c.Done():
		t.Fatalf("the client stopped before MaxTime: %v", c.StopCause())
	case <-time.After(100 * time.Millisecond):
	}
	clk.Advance(time.Minute)
	waitDone(t, c)
	if got := recycleReason(t, c.StopCause()); got != workflow.RecycleMaxTime {
		t.Fatalf("reason %s, want %s", got, workflow.RecycleMaxTime)
	}
}

// TestRecycleMaxMemory: the client recycles once the injected memory
// reading passes MaxMemory, and not while it stays below.
func TestRecycleMaxMemory(t *testing.T) {
	e := engines(t)[0]
	db, prefix := e.open(t)
	var mem atomic.Uint64
	mem.Store(100)
	c := newClient(t, e, db, prefix, sqlstore.WithRecycle(workflow.Recycle{MaxMemory: 1000,
		Memory: mem.Load, CheckEvery: 5 * time.Millisecond}))
	startWith(t, c, echoWorkflow("w"))
	select {
	case <-c.Done():
		t.Fatalf("the client stopped below MaxMemory: %v", c.StopCause())
	case <-time.After(100 * time.Millisecond):
	}
	mem.Store(1001)
	waitDone(t, c)
	if got := recycleReason(t, c.StopCause()); got != workflow.RecycleMaxMemory {
		t.Fatalf("reason %s, want %s", got, workflow.RecycleMaxMemory)
	}
}

// TestRecycleAskedStopHasNoCause: a Drain that was asked for leaves
// StopCause nil, and Start refuses a negative limit.
func TestRecycleAskedStopHasNoCause(t *testing.T) {
	e := engines(t)[0]
	db, prefix := e.open(t)
	c := newClient(t, e, db, prefix, sqlstore.WithRecycle(workflow.Recycle{MaxJobs: 100}))
	startWith(t, c, echoWorkflow("w"))
	if err := c.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := c.StopCause(); err != nil {
		t.Fatalf("a requested Drain left stop cause %v", err)
	}

	bad := newClient(t, e, db, prefix, sqlstore.WithRecycle(workflow.Recycle{MaxJobs: -1}))
	if err := bad.Start(context.Background()); !errors.Is(err, workflow.ErrInvalid) {
		bad.Stop()
		t.Fatalf("Start with a negative MaxJobs: %v, want ErrInvalid", err)
	}
}
