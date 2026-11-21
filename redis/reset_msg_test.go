package redis_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ubgo/workflow"
)

// TestResetWrongStateNamesResetAndTheOptIn: ResetRun of a succeeded run
// is refused with a message that says reset and names the opt-in that
// would take it (ResetOptions.Succeeded, the CLI's --succeeded); Retry
// keeps its own wording.
func TestResetWrongStateNamesResetAndTheOptIn(t *testing.T) {
	rdb := redisClient(t)
	prefix := testPrefix(t, rdb)
	c := newClient(t, rdb, prefix)
	wf := workflow.Workflow{Name: "once", Steps: []workflow.Step{{Name: "only",
		Run: func(context.Context, []byte) ([]byte, error) { return []byte(`"ok"`), nil }}}}
	if err := c.Register(wf); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := workflow.AwaitResult(ctx, c, ref, workflow.AwaitOptions{Poll: 5 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	_, err = c.ResetRun(ctx, ref, "only")
	if !errors.Is(err, workflow.ErrWrongState) {
		t.Fatalf("ResetRun of a succeeded run: %v, want ErrWrongState", err)
	}
	for _, want := range []string{"only a failed run can be reset", "ResetOptions.Succeeded", "--succeeded"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("ResetRun error = %q, want it to contain %q", err, want)
		}
	}
	_, err = c.Retry(ctx, ref)
	if !errors.Is(err, workflow.ErrWrongState) || !strings.Contains(err.Error(), "not failed") {
		t.Errorf("Retry of a succeeded run: %v, want ErrWrongState saying the run is not failed", err)
	}
}
