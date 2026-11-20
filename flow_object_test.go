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

// TestObjectCallAppliedOnce: a call whose attempt fails after its state
// was saved (a crash before the step's end was recorded) is retried and
// returns the saved result without applying the method again.
func TestObjectCallAppliedOnce(t *testing.T) {
	var applied atomic.Int32
	store := workflow.NewMemoryJournal()
	wf, err := workflow.DefineObject(workflow.ObjectSpec[int]{
		Name: "acct", Store: store, Retry: workflow.RetryPolicy{Max: 3},
		Methods: map[string]workflow.ObjectMethod[int]{"deposit": func(_ context.Context, bal int, _ []byte) (int, []byte, error) {
			applied.Add(1)
			return bal + 10, []byte("ok"), nil
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	inner := wf.Steps[0].Run
	var attempts atomic.Int32
	wf.Steps[0].Run = func(ctx context.Context, in []byte) ([]byte, error) {
		out, err := inner(ctx, in)
		if attempts.Add(1) == 1 {
			return nil, errors.New("lost before the end was recorded")
		}
		return out, err
	}
	c := memory.New()
	if err := c.Register(wf); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	ref, err := workflow.CallObject(ctx, c, "acct", "alice", "deposit", nil, workflow.Job{})
	if err != nil {
		t.Fatal(err)
	}
	out, err := workflow.AwaitResult(ctx, c, ref, workflow.AwaitOptions{Poll: time.Millisecond})
	if err != nil || string(out) != "ok" {
		t.Fatalf("out %q err %v", out, err)
	}
	bal, _, err := workflow.ObjectState[int](ctx, store, "acct", "alice")
	if err != nil || bal != 10 || applied.Load() != 1 || attempts.Load() != 2 {
		t.Fatalf("balance %d, applied %d, attempts %d, err %v: want 10, 1, 2", bal, applied.Load(), attempts.Load(), err)
	}
}

func TestDefineObjectRefuses(t *testing.T) {
	m := map[string]workflow.ObjectMethod[int]{"x": func(_ context.Context, s int, _ []byte) (int, []byte, error) { return s, nil, nil }}
	for name, spec := range map[string]workflow.ObjectSpec[int]{
		"no name":    {Methods: m, Store: workflow.NewMemoryJournal()},
		"no methods": {Name: "o", Store: workflow.NewMemoryJournal()},
		"no store":   {Name: "o", Methods: m},
		"nil method": {Name: "o", Store: workflow.NewMemoryJournal(), Methods: map[string]workflow.ObjectMethod[int]{"x": nil}},
	} {
		if _, err := workflow.DefineObject(spec); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
}
