package workflow_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

// TestSuspendAndResumeOneRun proves WP-18: a suspended run parks at its
// next step while another run of the same workflow carries on, and runs
// on once resumed.
func TestSuspendAndResumeOneRun(t *testing.T) {
	journal := workflow.NewMemoryJournal()
	c, err := workflow.Use(memory.New(), workflow.Journal(journal), workflow.Suspension(journal, 2*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	gate := make(chan struct{})
	var second atomic.Int32
	if err := c.Register(workflow.Workflow{Name: "import", Steps: []workflow.Step{
		{Name: "fetch", Run: func(ctx context.Context, in []byte) ([]byte, error) {
			if string(in) == `"held"` {
				<-gate // the operator suspends the run while this step runs
			}
			return in, nil
		}},
		{Name: "load", After: []string{"fetch"}, Run: func(ctx context.Context, in []byte) ([]byte, error) {
			second.Add(1)
			return in, nil
		}},
	}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	held := enqueue(t, c, workflow.Job{Workflow: "import", Payload: []byte(`"held"`)})
	statusEventually(t, c, held, workflow.StatusRunning)
	if err := workflow.SuspendRun(ctx, journal, held); err != nil {
		t.Fatal(err)
	}
	close(gate) // fetch finishes; load must not run while suspended
	other := enqueue(t, c, workflow.Job{Workflow: "import", Payload: []byte(`"free"`)})
	statusEventually(t, c, other, workflow.StatusSucceeded)
	time.Sleep(20 * time.Millisecond)
	if n := second.Load(); n != 1 {
		t.Fatalf("load ran %d time(s), want once (only the run not suspended)", n)
	}
	if ok, _ := workflow.RunSuspended(ctx, journal, held); !ok {
		t.Fatal("RunSuspended = false")
	}
	if st, _ := c.Status(ctx, held); st.IsTerminal() {
		t.Fatalf("suspended run is %s", st)
	}
	if err := workflow.ResumeRun(ctx, journal, held); err != nil {
		t.Fatal(err)
	}
	statusEventually(t, c, held, workflow.StatusSucceeded)
	if second.Load() != 2 {
		t.Fatalf("load ran %d time(s) after resume", second.Load())
	}
	if err := workflow.SuspendRun(ctx, journal, workflow.RunRef{ScheduledID: "s"}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("suspend a run without RunID = %v", err)
	}
}
