package cutover_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/cutover"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/workflowtest"
)

// startCutover starts a cutover client over two memory clients with wfs
// registered, for the shared flow suites.
func startCutover(t *testing.T, wfs ...workflow.Workflow) workflow.Client {
	t.Helper()
	c := cutover.New(memory.New(), memory.New())
	for _, wf := range wfs {
		must(t, c.Register(wf))
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	must(t, c.Start(ctx))
	return c
}

// TestResetAndSignalFlows runs the shared ResetRun and signal suites
// through a cutover client.
func TestResetAndSignalFlows(t *testing.T) {
	workflowtest.RunResetFlows(t, workflowtest.FlowHarness{Start: startCutover})
	workflowtest.RunSignalFlows(t, workflowtest.SignalHarness{Start: startCutover,
		Promises: func(*testing.T, workflow.Client) workflow.PromiseStore { return workflow.NewMemoryPromises() }})
}

// TestNewOps_ReachTheOldBackend: a run the old backend held before the
// cutover (an unprefixed ref) is edited and reset there; SignalWaiting
// reaches the waiting runs of both backends.
func TestNewOps_ReachTheOldBackend(t *testing.T) {
	ctx := t.Context()
	old, nw := memory.New(), memory.New()
	c := cutover.New(old, nw)
	var aRuns atomic.Int32
	var fail atomic.Bool
	fail.Store(true)
	wait := workflow.Workflow{Name: "wait", Steps: []workflow.Step{{Name: "w", Wait: &workflow.WaitSpec{Event: "go"}}}}
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
	for _, wf := range []workflow.Workflow{wait, echo, reset} {
		must(t, c.Register(wf))
	}
	must(t, c.Start(ctx))

	// Runs the app started on the old backend before the cutover.
	delayed, err := old.Enqueue(ctx, workflow.Job{Workflow: "echo", Payload: []byte(`"old"`), RunAt: time.Now().Add(time.Hour)})
	must(t, err)
	failing, err := old.Enqueue(ctx, workflow.Job{Workflow: "reset"})
	must(t, err)
	onOld, err := old.Enqueue(ctx, workflow.Job{Workflow: "wait"})
	must(t, err)
	onNew, err := c.Enqueue(ctx, workflow.Job{Workflow: "wait"})
	must(t, err)

	payload, now := []byte(`"edited"`), time.Now()
	edited, err := c.EditRun(ctx, delayed, workflow.JobEdit{Payload: &payload, RunAt: &now})
	if err != nil {
		t.Fatalf("EditRun: %v", err)
	}
	if !strings.HasPrefix(string(edited.ScheduledID), "old:") {
		t.Fatalf("EditRun ref %+v, want the old backend's prefix", edited)
	}
	out, err := workflow.AwaitResult(ctx, c, edited, await)
	if err != nil || string(out) != `"edited"` {
		t.Fatalf("edited run's result = %q, %v", out, err)
	}

	waitFor(t, c, failing, workflow.StatusFailed)
	fail.Store(false)
	again, err := c.ResetRun(ctx, failing, "a")
	if err != nil {
		t.Fatalf("ResetRun: %v", err)
	}
	if !strings.HasPrefix(string(again.RunID), "old:") {
		t.Fatalf("ResetRun ref %+v, want the old backend's prefix", again)
	}
	waitFor(t, c, again, workflow.StatusSucceeded)
	if n := aRuns.Load(); n != 2 {
		t.Fatalf("step a ran %d times, want 2", n)
	}

	waitFor(t, c, onOld, workflow.StatusWaiting)
	waitFor(t, c, onNew, workflow.StatusWaiting)
	n, err := c.SignalWaiting(ctx, workflow.Event{Name: "go"})
	if err != nil || n != 2 {
		t.Fatalf("SignalWaiting = %d, %v; want the waiting run of each backend", n, err)
	}
	waitFor(t, c, onOld, workflow.StatusSucceeded)
	waitFor(t, c, onNew, workflow.StatusSucceeded)
	for _, op := range []workflow.Op{workflow.OpReschedule, workflow.OpCancelStep, workflow.OpEditRun, workflow.OpResetRun, workflow.OpSignalWaiting, workflow.OpNodes} {
		if !workflow.Supports(c, op) {
			t.Errorf("Supports(%s) = false over two memory backends", op)
		}
	}
	for _, op := range []workflow.Op{workflow.OpLifecycle} {
		if workflow.Supports(c, op) {
			t.Errorf("Supports(%s) = true, but cutover does not implement it", op)
		}
	}
}

// bareClient hides every optional interface of the client it wraps.
type bareClient struct{ workflow.Client }

// TestNewOps_RefusedWhenABackendLacksThem: a backend without the
// interfaces makes each call an UnsupportedError, SignalWaiting delivers to
// neither backend, and Supports says no.
func TestNewOps_RefusedWhenABackendLacksThem(t *testing.T) {
	ctx := t.Context()
	c := cutover.New(memory.New(), bareClient{memory.New()})
	must(t, c.Register(workflow.Workflow{Name: "wait", Steps: []workflow.Step{{Name: "w", Wait: &workflow.WaitSpec{Event: "go"}}}}))
	must(t, c.Start(ctx))
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "wait"})
	must(t, err)
	waitFor(t, c, ref, workflow.StatusWaiting)
	if _, err := c.ResetRun(ctx, ref, "w"); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("ResetRun on a backend without it: %v, want ErrUnsupported", err)
	}
	payload := []byte(`1`)
	if _, err := c.EditRun(ctx, ref, workflow.JobEdit{Payload: &payload}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("EditRun on a backend without it: %v, want ErrUnsupported", err)
	}
	if _, err := c.Reschedule(ctx, ref, time.Now()); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("Reschedule on a backend without it: %v, want ErrUnsupported", err)
	}
	if err := c.CancelStep(ctx, ref, "w"); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("CancelStep on a backend without it: %v, want ErrUnsupported", err)
	}
	if n, err := c.SignalWaiting(ctx, workflow.Event{Name: "go"}); !errors.Is(err, workflow.ErrUnsupported) || n != 0 {
		t.Errorf("SignalWaiting with a backend without it = %d, %v; want 0, ErrUnsupported", n, err)
	}
	for _, op := range []workflow.Op{workflow.OpReschedule, workflow.OpCancelStep, workflow.OpEditRun, workflow.OpResetRun, workflow.OpSignalWaiting, workflow.OpNodes} {
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
