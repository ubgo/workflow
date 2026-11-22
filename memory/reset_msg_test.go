package memory_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

// TestResetWrongStateNamesResetAndTheOptIn: ResetRun of a succeeded run
// is refused with a message that says reset (not retry) and names the
// opt-in that would take it (ResetOptions.Succeeded, the CLI's
// --succeeded); with the opt-in a run in another state is refused naming
// both statuses; Retry keeps its own wording.
func TestResetWrongStateNamesResetAndTheOptIn(t *testing.T) {
	c := memory.New()
	wf := workflow.Workflow{Name: "once", Steps: []workflow.Step{{Name: "only",
		Run: func(context.Context, []byte) ([]byte, error) { return []byte(`"ok"`), nil }}}}
	if err := c.Register(wf); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := workflow.AwaitResult(ctx, c, ref, workflow.AwaitOptions{Poll: time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	checkResetWrongState(t, c, ref)
}

// checkResetWrongState asserts the wrong-state messages of a succeeded
// run ref on c.
func checkResetWrongState(t *testing.T, c interface {
	workflow.RunResetter
	workflow.RunRetrier
}, ref workflow.RunRef) {
	t.Helper()
	ctx := context.Background()
	_, err := c.ResetRun(ctx, ref, "only")
	if !errors.Is(err, workflow.ErrWrongState) {
		t.Fatalf("ResetRun of a succeeded run: %v, want ErrWrongState", err)
	}
	for _, want := range []string{"only a failed run can be reset", "ResetOptions.Succeeded", "--succeeded"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("ResetRun error = %q, want it to contain %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "retried") {
		t.Errorf("ResetRun error = %q talks about retry", err)
	}
	_, err = c.Retry(ctx, ref)
	if !errors.Is(err, workflow.ErrWrongState) || !strings.Contains(err.Error(), "only a failed run can be retried") {
		t.Errorf("Retry of a succeeded run: %v, want ErrWrongState saying only a failed run can be retried", err)
	}
}
