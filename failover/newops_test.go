package failover_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/failover"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/workflowtest"
)

// startPair starts a failover client over two real-clock memory clients
// with wfs registered, for the shared flow suites.
func startPair(t *testing.T, wfs ...workflow.Workflow) workflow.Client {
	t.Helper()
	c := failover.New(memory.New(), memory.New(), failover.AllowWithoutKey())
	for _, wf := range wfs {
		if err := c.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return c
}

// TestResetAndSignalFlows runs the shared ResetRun and signal suites
// through failover, so ResetRun and SignalWaiting keep their driver
// semantics behind the wrapper.
func TestResetAndSignalFlows(t *testing.T) {
	workflowtest.RunResetFlows(t, workflowtest.FlowHarness{Start: startPair})
	workflowtest.RunSignalFlows(t, workflowtest.SignalHarness{Start: startPair,
		Promises: func(*testing.T, workflow.Client) workflow.PromiseStore { return workflow.NewMemoryPromises() }})
}

// TestNewOps_RouteBySide: EditRun and ResetRun reach the backend a ref
// names and return refs with that backend's prefix; SignalWaiting reaches
// the waiting runs of both backends and counts them all.
func TestNewOps_RouteBySide(t *testing.T) {
	fx := newFixture(t)
	var aRuns atomic.Int32
	var fail atomic.Bool
	fail.Store(true)
	echo := workflow.Workflow{Name: "echo", Steps: []workflow.Step{{Name: "s", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}}
	reset := workflow.Workflow{Name: "reset", Steps: []workflow.Step{
		{Name: "a", Run: func(_ context.Context, in []byte) ([]byte, error) { aRuns.Add(1); return in, nil }},
		{Name: "b", After: []string{"a"}, Run: func(context.Context, []byte) ([]byte, error) {
			if fail.Load() {
				return nil, workflow.Permanent(errors.New("boom"))
			}
			return []byte(`"ok"`), nil
		}},
	}}
	fx.up(waitWF, echo, reset)
	onP := fx.enqueue("p")

	// Everything below is placed on the secondary.
	fx.p.down.Store(true)
	onS := fx.enqueue("s")
	delayed, err := fx.c.Enqueue(fx.ctx, workflow.Job{Workflow: "echo", Payload: []byte(`"old"`), RunAt: start.Add(time.Hour), IdempotencyKey: "e"})
	if err != nil {
		t.Fatal(err)
	}
	failing, err := fx.c.Enqueue(fx.ctx, workflow.Job{Workflow: "reset", IdempotencyKey: "r"})
	if err != nil {
		t.Fatal(err)
	}
	fx.p.down.Store(false)
	if !strings.HasPrefix(string(delayed.ScheduledID), "secondary:") || !strings.HasPrefix(string(failing.RunID), "secondary:") {
		t.Fatalf("refs %+v and %+v are not on the secondary", delayed, failing)
	}

	payload, now := []byte(`"edited"`), start
	edited, err := fx.c.EditRun(fx.ctx, delayed, workflow.JobEdit{Payload: &payload, RunAt: &now})
	if err != nil {
		t.Fatalf("EditRun: %v", err)
	}
	if !strings.HasPrefix(string(edited.ScheduledID), "secondary:") || (edited.RunID != "" && !strings.HasPrefix(string(edited.RunID), "secondary:")) {
		t.Fatalf("EditRun ref %+v lost the secondary prefix", edited)
	}
	fx.waitStatus(edited, workflow.StatusSucceeded)
	if out, err := fx.c.Result(fx.ctx, edited); err != nil || string(out) != `"edited"` {
		t.Fatalf("edited run's result = %q, %v", out, err)
	}

	fx.waitStatus(failing, workflow.StatusFailed)
	fail.Store(false)
	again, err := fx.c.ResetRun(fx.ctx, failing, "a")
	if err != nil {
		t.Fatalf("ResetRun: %v", err)
	}
	if again != failing {
		t.Fatalf("ResetRun ref %+v, want %+v", again, failing)
	}
	fx.waitStatus(again, workflow.StatusSucceeded)
	if n := aRuns.Load(); n != 2 {
		t.Fatalf("step a ran %d times, want 2 (once more after the reset)", n)
	}

	fx.waitStatus(onP, workflow.StatusWaiting)
	fx.waitStatus(onS, workflow.StatusWaiting)
	n, err := fx.c.SignalWaiting(fx.ctx, workflow.Event{Name: "go"})
	if err != nil || n != 2 {
		t.Fatalf("SignalWaiting = %d, %v; want the waiting run of each backend", n, err)
	}
	fx.waitStatus(onP, workflow.StatusSucceeded)
	fx.waitStatus(onS, workflow.StatusSucceeded)
	for _, op := range []workflow.Op{workflow.OpEditRun, workflow.OpResetRun, workflow.OpResetSucceeded, workflow.OpForkRun, workflow.OpSignalWaiting} {
		if !workflow.Supports(fx.c, op) {
			t.Errorf("Supports(%s) = false over two memory backends", op)
		}
	}
}

// TestNewOps_RefusedWhenABackendLacksThem: a backend without RunEditor,
// RunResetter or TransientSignaller makes the call an UnsupportedError,
// SignalWaiting delivers to neither backend, and Supports says no.
func TestNewOps_RefusedWhenABackendLacksThem(t *testing.T) {
	p, s := memory.New(), memory.New()
	c := failover.New(bare{p}, s)
	if err := c.Register(waitWF); err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "wait", IdempotencyKey: "k"})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, c, ref, workflow.StatusWaiting)
	if _, err := c.ResetRun(ctx, ref, "w"); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("ResetRun on a backend without it: %v, want ErrUnsupported", err)
	}
	if _, err := c.ResetRunWith(ctx, ref, "w", workflow.ResetOptions{Succeeded: true}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("ResetRunWith on a backend without it: %v, want ErrUnsupported", err)
	}
	if _, err := c.ForkRun(ctx, ref, "w", workflow.ForkOptions{}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("ForkRun on a backend without it: %v, want ErrUnsupported", err)
	}
	payload := []byte(`1`)
	if _, err := c.EditRun(ctx, ref, workflow.JobEdit{Payload: &payload}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("EditRun on a backend without it: %v, want ErrUnsupported", err)
	}
	if n, err := c.SignalWaiting(ctx, workflow.Event{Name: "go"}); !errors.Is(err, workflow.ErrUnsupported) || n != 0 {
		t.Errorf("SignalWaiting with one backend without it = %d, %v; want 0, ErrUnsupported", n, err)
	}
	time.Sleep(20 * time.Millisecond)
	if st, err := c.Status(ctx, ref); err != nil || st != workflow.StatusWaiting {
		t.Errorf("a refused SignalWaiting delivered: %s %v", st, err)
	}
	for _, op := range []workflow.Op{workflow.OpEditRun, workflow.OpResetRun, workflow.OpResetSucceeded, workflow.OpForkRun, workflow.OpSignalWaiting} {
		if workflow.Supports(c, op) {
			t.Errorf("Supports(%s) = true with a backend without it", op)
		}
	}
}

// waitFor polls c until ref reaches want or fails the test after 5s.
func waitFor(t *testing.T, c workflow.Client, ref workflow.RunRef, want workflow.Status) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, err := c.Status(context.Background(), ref)
		if err == nil && st == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Status(%+v) = %q, %v; want %q", ref, st, err, want)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestRedoAndForkFlows runs the shared succeeded-reset and fork suites
// through failover.
func TestRedoAndForkFlows(t *testing.T) {
	t.Run("redo", func(t *testing.T) { workflowtest.RunRedoFlows(t, workflowtest.FlowHarness{Start: startPair}) })
	t.Run("fork", func(t *testing.T) { workflowtest.RunForkFlows(t, workflowtest.FlowHarness{Start: startPair}) })
}
