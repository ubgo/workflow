package workflow_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

func TestWorkflowVersionValidate(t *testing.T) {
	step := []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}
	for _, wf := range []workflow.Workflow{
		{Name: "w", Steps: step, Version: -1},
		{Name: "w", Steps: step, Version: 1, MinVersion: 2},
		{Name: "w", Steps: step, MinVersion: -1},
	} {
		if err := workflow.Validate(wf); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("Version %d MinVersion %d: %v; want ErrInvalid", wf.Version, wf.MinVersion, err)
		}
	}
	w := workflow.Workflow{Version: 3, MinVersion: 2}
	for v, want := range map[int]bool{0: false, 1: false, 2: true, 3: true, 4: false} {
		if got := w.RunsVersion(v); got != want {
			t.Errorf("RunsVersion(%d) = %v", v, got)
		}
	}
	if !(workflow.Workflow{}).RunsVersion(0) || (workflow.Workflow{}).RunsVersion(1) {
		t.Error("an unversioned definition must run exactly version 0")
	}
}

// TestWorkflowVersionMemory: one process holds one definition, so every
// run carries the registered version, reported to steps and in RunInfo.
func TestWorkflowVersionMemory(t *testing.T) {
	got := make(chan int, 1)
	c := memory.New()
	if err := c.Register(workflow.Workflow{Name: "w", Version: 4, MinVersion: 3, Steps: []workflow.Step{{Name: "s", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		got <- info.Version
		return nil, nil
	}}}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	ref := enqueue(t, c, workflow.Job{Workflow: "w"})
	if st := waitRun(t, c, ref); st != workflow.StatusSucceeded {
		t.Fatal(st)
	}
	if v := <-got; v != 4 {
		t.Fatalf("StepInfo.Version = %d; want 4", v)
	}
	page, err := c.Runs(ctx, workflow.RunQuery{Ref: ref})
	if err != nil || len(page.Runs) != 1 || page.Runs[0].Version != 4 {
		t.Fatalf("RunInfo = %+v, %v; want Version 4", page.Runs, err)
	}
}
