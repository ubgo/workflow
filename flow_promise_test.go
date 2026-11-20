package workflow_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

func TestPromiseOutcomes(t *testing.T) {
	ctx := context.Background()
	s := workflow.NewMemoryPromises()
	if _, settled, err := workflow.PromiseResult(ctx, s, "p"); settled || err != nil {
		t.Fatalf("unsettled: %v %v", settled, err)
	}
	if err := workflow.RejectPromise(ctx, nil, s, "p", "no budget"); err != nil {
		t.Fatal(err)
	}
	_, settled, err := workflow.PromiseResult(ctx, s, "p")
	var rej *workflow.PromiseRejectedError
	if !settled || !errors.As(err, &rej) || rej.Reason != "no budget" {
		t.Fatalf("rejected: %v %v", settled, err)
	}
	if err := workflow.ResolvePromise(ctx, nil, s, "p", []byte("1")); !errors.Is(err, workflow.ErrPromiseSettled) {
		t.Fatalf("resolve after reject: %v", err)
	}
	if err := workflow.ResolvePromise(ctx, nil, nil, "p", nil); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("nil store: %v", err)
	}
	if _, err := workflow.WaitPromise(ctx, s, "p", workflow.AwaitOptions{Poll: time.Millisecond}); !errors.As(err, &rej) {
		t.Fatalf("WaitPromise of a rejected promise: %v", err)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := workflow.WaitPromise(cctx, s, "never", workflow.AwaitOptions{Poll: time.Millisecond}); !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitPromise on a cancelled ctx: %v", err)
	}
	if id, err := workflow.NewPromiseID(); err != nil || len(id) != 32 {
		t.Fatalf("NewPromiseID: %q %v", id, err)
	}
}

func TestSignalHelpersRefuse(t *testing.T) {
	ctx := context.Background()
	c := memory.New()
	if _, err := workflow.ContinueAsNew(ctx, c, nil); !errors.Is(err, workflow.ErrInvalid) || !workflow.IsPermanent(err) {
		t.Fatalf("ContinueAsNew outside a step: %v", err)
	}
	if _, ok := workflow.ContinuedFrom(ctx); ok {
		t.Fatal("ContinuedFrom outside a step")
	}
	if err := workflow.CancelSteps(ctx, c, workflow.RunRef{RunID: "r"}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("CancelSteps with no steps: %v", err)
	}
	var ue *workflow.UnsupportedError
	if err := workflow.CancelSteps(ctx, noSnooze{nil}, workflow.RunRef{RunID: "r"}, "a"); !errors.As(err, &ue) {
		t.Fatalf("CancelSteps without StepCanceller: %v", err)
	}
	noWait := capsOnly{memory.New(), workflow.Caps{}}
	if _, err := workflow.Update(ctx, noWait, workflow.NewMemoryPromises(), workflow.Event{Name: "e"}, workflow.AwaitOptions{}); !errors.As(err, &ue) || ue.Missing != workflow.CapWaitForEvent {
		t.Fatalf("Update without CapWaitForEvent: %v", err)
	}
	if _, err := workflow.SignalWithStart(ctx, noWait, workflow.Job{Workflow: "w", IdempotencyKey: "k"}, workflow.Event{Name: "e"}); !errors.As(err, &ue) {
		t.Fatalf("SignalWithStart without CapWaitForEvent: %v", err)
	}
	if _, ok := workflow.DecodeUpdate([]byte(`{"reply":"x"}`)); ok {
		t.Fatal("an app payload shaped like a request must not read as an update")
	}
}

// capsOnly reports caps in place of the client's own.
type capsOnly struct {
	workflow.Client
	caps workflow.Caps
}

func (c capsOnly) Capabilities() workflow.Caps { return c.caps }
