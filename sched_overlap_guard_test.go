package workflow_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

// guardSetup starts a memory client whose "report" workflow blocks until
// released or cancelled, with GuardOverlap applying mode to it, and a
// yearly schedule fired by hand with TriggerSchedule.
func guardSetup(t *testing.T, mode workflow.OverlapMode, conc *workflow.Concurrency) (workflow.Client, workflow.ScheduleID, chan struct{}) {
	t.Helper()
	release := make(chan struct{})
	c := memory.New()
	if err := c.Register(workflow.Workflow{Name: "report", Concurrency: conc, Steps: []workflow.Step{{Name: "build", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		select {
		case <-release:
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}}}}); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var errs []error
	if err := workflow.GuardOverlap(c, func(err error) { mu.Lock(); errs = append(errs, err); mu.Unlock() }, workflow.OverlapRule{Workflow: "report", Mode: mode}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		if len(errs) > 0 {
			t.Errorf("guard errors: %v", errs)
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	id, err := c.Schedule(ctx, workflow.Schedule{Name: "nightly", Workflow: "report", Cron: "0 0 1 1 *", Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	return c, id, release
}

func trigger(t *testing.T, c workflow.Client, id workflow.ScheduleID) workflow.RunRef {
	t.Helper()
	ref, err := c.(workflow.ScheduleTriggerer).TriggerSchedule(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func statusEventually(t *testing.T, c workflow.Client, ref workflow.RunRef, want workflow.Status) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	var st workflow.Status
	for time.Now().Before(deadline) {
		st, _ = c.Status(context.Background(), ref)
		if st == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("run %v is %s, want %s", ref, st, want)
}

func TestGuardOverlapReplace(t *testing.T) {
	c, id, release := guardSetup(t, workflow.OverlapReplace, nil)
	first := trigger(t, c, id)
	statusEventually(t, c, first, workflow.StatusRunning)
	second := trigger(t, c, id)
	statusEventually(t, c, first, workflow.StatusCancelled)
	statusEventually(t, c, second, workflow.StatusRunning)
	close(release)
	statusEventually(t, c, second, workflow.StatusSucceeded)
}

func TestGuardOverlapBufferOne(t *testing.T) {
	one := &workflow.Concurrency{Key: workflow.KeyFunc(func(workflow.Job) string { return "nightly" }), MaxRuns: 1}
	c, id, release := guardSetup(t, workflow.OverlapBufferOne, one)
	first := trigger(t, c, id)
	statusEventually(t, c, first, workflow.StatusRunning)
	buffered := trigger(t, c, id)
	extra := trigger(t, c, id)
	statusEventually(t, c, extra, workflow.StatusCancelled)
	if st, _ := c.Status(context.Background(), buffered); st != workflow.StatusQueued {
		t.Fatalf("buffered run is %s, want queued behind the running one", st)
	}
	close(release)
	statusEventually(t, c, first, workflow.StatusSucceeded)
	statusEventually(t, c, buffered, workflow.StatusSucceeded)
}

func TestGuardOverlapRefusals(t *testing.T) {
	if err := workflow.GuardOverlap(memory.New(), nil); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("no rules: %v", err)
	}
	if err := workflow.GuardOverlap(memory.New(), nil, workflow.OverlapRule{Workflow: "w", Mode: "sometimes"}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("unknown mode: %v", err)
	}
	if err := workflow.GuardOverlap(memory.New(), nil, workflow.OverlapRule{Mode: workflow.OverlapReplace}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("rule without a target: %v", err)
	}
}
