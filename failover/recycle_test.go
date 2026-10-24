package failover_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/failover"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

// causer is a memory client whose StopCause the test sets.
type causer struct {
	*memory.Client
	cause error
}

func (c *causer) StopCause() error { return c.cause }

func TestStopCauseJoinsSides(t *testing.T) {
	p := &causer{Client: memory.New()}
	s := &causer{Client: memory.New()}
	f := failover.New(p, s)
	if err := f.StopCause(); err != nil {
		t.Fatalf("StopCause while neither stopped itself = %v", err)
	}
	s.cause = &workflow.RecycleError{Reason: workflow.RecycleMaxJobs, Jobs: 1}
	if err := f.StopCause(); !errors.Is(err, workflow.ErrRecycle) {
		t.Fatalf("StopCause with the secondary recycled = %v, want ErrRecycle", err)
	}
	// Wrappers pass StopCause through workflow.Forward.
	w := middleware.WrapClient(p)
	sc, ok := w.(workflow.StopCauser)
	if !ok {
		t.Fatal("WrapClient hides StopCauser")
	}
	if sc.StopCause() != nil {
		t.Fatal("wrapped StopCause before a recycle is not nil")
	}
	p.cause = &workflow.RecycleError{Reason: workflow.RecycleMaxTime}
	if !errors.Is(sc.StopCause(), workflow.ErrRecycle) {
		t.Fatal("WrapClient does not forward StopCause")
	}
	if (workflow.Forward{Inner: workflow.Client(memory.New())}).StopCause() != nil {
		t.Fatal("Forward over a client that never recycled")
	}
}

func TestCallsRefusedOnceDone(t *testing.T) {
	wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}}
	f := failover.New(memory.New(), memory.New())
	if err := f.Register(wf); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := f.Start(ctx); err != nil {
		t.Fatal(err)
	}
	done := f.Done()
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Done never closed")
	}
	bg := context.Background()
	if _, err := f.Enqueue(bg, workflow.Job{Workflow: "w", IdempotencyKey: "k"}); !errors.Is(err, workflow.ErrNotStarted) {
		t.Fatalf("Enqueue after Done = %v", err)
	}
	if err := f.Signal(bg, workflow.Event{Name: "e"}); !errors.Is(err, workflow.ErrNotStarted) {
		t.Fatalf("Signal after Done = %v", err)
	}
	if _, err := f.Schedule(bg, workflow.Schedule{Workflow: "w", Cron: "* * * * *", Location: time.UTC}); !errors.Is(err, workflow.ErrNotStarted) {
		t.Fatalf("Schedule after Done = %v", err)
	}
}
