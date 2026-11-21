package hatchet_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/hatchet"
)

// TestOwnRunWaitLive proves CapWaitOwnRun on the engine: a wait step with
// WaitSpec.OwnRun resolves its key from the engine's run ID inside the
// durable task, so SignalRun reaches only the run it names (a second run
// waiting on the same event stays waiting, and a plain Signal on the
// event name reaches neither); a signal sent while the run is still in an
// earlier step is kept until its wait begins; and a child's SignalParent
// resumes its parent's OwnRun wait (escalation). Hatchet has no
// RunSignalReader, so OnSignals is refused on it.
func TestOwnRunWaitLive(t *testing.T) {
	skipWithoutEngine(t)
	prefix := "wfown_" + uuid.NewString()[:8]
	var c *hatchet.Client
	gate := make(chan struct{})
	waiter := workflow.Workflow{Name: prefix + "_wait", Steps: []workflow.Step{
		{Name: "wait", Wait: &workflow.WaitSpec{Event: "go", OwnRun: true, Timeout: 5 * time.Minute}},
		{Name: "echo", After: []string{"wait"}, Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }},
	}}
	kept := workflow.Workflow{Name: prefix + "_kept", Steps: []workflow.Step{
		{Name: "gate", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			select {
			case <-gate:
				return nil, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}},
		{Name: "wait", After: []string{"gate"}, Wait: &workflow.WaitSpec{Event: "go", OwnRun: true, Timeout: 5 * time.Minute}},
		{Name: "echo", After: []string{"wait"}, Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }},
	}}
	child := workflow.Workflow{Name: prefix + "_child", Steps: []workflow.Step{{Name: "work", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		return nil, workflow.SignalParent(ctx, c, "escalation", in)
	}}}}
	parent := workflow.Workflow{Name: prefix + "_parent", Steps: []workflow.Step{
		{Name: "spawn", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			info, _ := workflow.StepInfoFrom(ctx)
			_, err := c.Enqueue(workflow.AllowReservedLabels(ctx, workflow.LabelParentRun), workflow.Job{Workflow: prefix + "_child",
				Payload: []byte(`"need approval"`), IdempotencyKey: "child-of-" + info.RunID.String(),
				Labels: map[string]string{workflow.LabelParentRun: info.RunID.String()}})
			return nil, err
		}},
		{Name: "escalated", After: []string{"spawn"}, Wait: &workflow.WaitSpec{Event: "escalation", OwnRun: true, Timeout: 5 * time.Minute}},
		{Name: "echo", After: []string{"escalated"}, Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }},
	}}
	c, _ = startLive(t, hatchet.Config{WorkerName: prefix}, waiter, kept, child, parent)
	ctx := context.Background()

	a, err := c.Enqueue(ctx, workflow.Job{Workflow: waiter.Name})
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.Enqueue(ctx, workflow.Job{Workflow: waiter.Name})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, a, workflow.StatusWaiting)
	waitStatus(t, c, b, workflow.StatusWaiting)
	if err := c.Signal(ctx, workflow.Event{Name: "go", Payload: []byte(`"stray"`)}); err != nil {
		t.Fatal(err)
	}
	if err := workflow.SignalRun(ctx, c, a, "go", []byte(`"for a"`)); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, a, workflow.StatusSucceeded)
	if out, err := c.Result(ctx, a); err != nil || string(out) != `"for a"` {
		t.Fatalf("run a: %q %v", out, err)
	}
	time.Sleep(3 * time.Second) // the engine had time to deliver anything misaddressed
	if st, err := c.Status(ctx, b); err != nil || st != workflow.StatusWaiting {
		t.Fatalf("run b took a signal addressed to run a, or the stray one: %s %v", st, err)
	}
	if err := workflow.SignalRun(ctx, c, b, "go", []byte(`"for b"`)); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, b, workflow.StatusSucceeded)
	if out, err := c.Result(ctx, b); err != nil || string(out) != `"for b"` {
		t.Fatalf("run b: %q %v", out, err)
	}

	k, err := c.Enqueue(ctx, workflow.Job{Workflow: kept.Name})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, k, workflow.StatusRunning)
	if err := workflow.SignalRun(ctx, c, k, "go", []byte(`"early"`)); err != nil {
		t.Fatal(err)
	}
	close(gate)
	waitStatus(t, c, k, workflow.StatusSucceeded)
	if out, err := c.Result(ctx, k); err != nil || string(out) != `"early"` {
		t.Fatalf("a signal sent before the wait began: %q %v", out, err)
	}

	p, err := c.Enqueue(ctx, workflow.Job{Workflow: parent.Name})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, p, workflow.StatusSucceeded)
	out, err := c.Result(ctx, p)
	var got string
	if err != nil || json.Unmarshal(out, &got) != nil || got != "need approval" {
		t.Fatalf("parent: %q %v", out, err)
	}

	on := map[string]workflow.SignalHandler{"x": func(context.Context, workflow.RunSignal) error { return nil }}
	var ue *workflow.UnsupportedError
	if _, err := workflow.Use(c, workflow.OnSignals(workflow.SignalHandlers{On: on})); !errors.As(err, &ue) {
		t.Fatalf("OnSignals on Hatchet: %v; want UnsupportedError", err)
	}
}
