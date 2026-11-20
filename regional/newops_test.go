package regional_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/regional"
	"github.com/ubgo/workflow/workflowtest"
)

// startRegions starts a regional client over two real-clock memory
// regions with wfs registered, for the shared flow suites.
func startRegions(t *testing.T, wfs ...workflow.Workflow) workflow.Client {
	t.Helper()
	c, err := regional.New(map[string]workflow.Client{"eu": memory.New(), "us": memory.New()}, router)
	if err != nil {
		t.Fatal(err)
	}
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
// through a regional client.
func TestResetAndSignalFlows(t *testing.T) {
	workflowtest.RunResetFlows(t, workflowtest.FlowHarness{Start: startRegions})
	workflowtest.RunSignalFlows(t, workflowtest.SignalHarness{Start: startRegions,
		Promises: func(*testing.T, workflow.Client) workflow.PromiseStore { return workflow.NewMemoryPromises() }})
}

// TestNewOps_RouteByRegion: EditRun and ResetRun reach the region a ref
// names and return refs with that region's prefix; SignalWaiting reaches
// the waiting runs of every region (or WithSignalRouter's pick) and counts
// them all.
func TestNewOps_RouteByRegion(t *testing.T) {
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
	us := map[string]string{"region": "us"}
	delayed, err := fx.c.Enqueue(fx.ctx, workflow.Job{Workflow: "echo", Payload: []byte(`"old"`), RunAt: start.Add(time.Hour), Labels: us})
	if err != nil {
		t.Fatal(err)
	}
	failing, err := fx.c.Enqueue(fx.ctx, workflow.Job{Workflow: "reset", Labels: us})
	if err != nil {
		t.Fatal(err)
	}

	payload, now := []byte(`"edited"`), start
	edited, err := fx.c.EditRun(fx.ctx, delayed, workflow.JobEdit{Payload: &payload, RunAt: &now})
	if err != nil {
		t.Fatalf("EditRun: %v", err)
	}
	if !strings.HasPrefix(string(edited.ScheduledID), "us:") || (edited.RunID != "" && !strings.HasPrefix(string(edited.RunID), "us:")) {
		t.Fatalf("EditRun ref %+v lost the us prefix", edited)
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
		t.Fatalf("step a ran %d times, want 2", n)
	}
	if _, err := fx.c.ResetRun(fx.ctx, workflow.RunRef{RunID: "mars:x"}, "a"); !errors.Is(err, workflow.ErrNotFound) {
		t.Errorf("ResetRun of an unknown region: %v, want ErrNotFound", err)
	}

	onEU, onUS := fx.enqueue("eu"), fx.enqueue("us")
	fx.waitStatus(onEU, workflow.StatusWaiting)
	fx.waitStatus(onUS, workflow.StatusWaiting)
	n, err := fx.c.SignalWaiting(fx.ctx, workflow.Event{Name: "go"})
	if err != nil || n != 2 {
		t.Fatalf("SignalWaiting = %d, %v; want the waiting run of each region", n, err)
	}
	fx.waitStatus(onEU, workflow.StatusSucceeded)
	fx.waitStatus(onUS, workflow.StatusSucceeded)
	for _, op := range []workflow.Op{workflow.OpEditRun, workflow.OpResetRun, workflow.OpSignalWaiting} {
		if !workflow.Supports(fx.c, op) {
			t.Errorf("Supports(%s) = false over two memory regions", op)
		}
	}
}

// TestSignalWaitingFollowsTheSignalRouter: only the regions the signal
// router picks are asked.
func TestSignalWaitingFollowsTheSignalRouter(t *testing.T) {
	fx := newFixture(t, regional.WithSignalRouter(func(context.Context, workflow.Event) ([]string, error) { return []string{"us"}, nil }))
	fx.up(waitWF)
	onEU, onUS := fx.enqueue("eu"), fx.enqueue("us")
	fx.waitStatus(onEU, workflow.StatusWaiting)
	fx.waitStatus(onUS, workflow.StatusWaiting)
	if n, err := fx.c.SignalWaiting(fx.ctx, workflow.Event{Name: "go"}); err != nil || n != 1 {
		t.Fatalf("SignalWaiting = %d, %v; want only the us run", n, err)
	}
	fx.waitStatus(onUS, workflow.StatusSucceeded)
	if st, err := fx.c.Status(fx.ctx, onEU); err != nil || st != workflow.StatusWaiting {
		t.Fatalf("the eu run, outside the router's pick: %s %v", st, err)
	}
}

// bareClient hides every optional interface of the client it wraps.
type bareClient struct{ workflow.Client }

// TestNewOps_RefusedWhenARegionLacksThem: a region without RunEditor,
// RunResetter or TransientSignaller makes the call an UnsupportedError,
// SignalWaiting delivers to no region, and Supports says no.
func TestNewOps_RefusedWhenARegionLacksThem(t *testing.T) {
	c, err := regional.New(map[string]workflow.Client{"eu": bareClient{memory.New()}, "us": memory.New()}, router)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Register(waitWF); err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	onEU, err := c.Enqueue(ctx, workflow.Job{Workflow: "wait"})
	if err != nil {
		t.Fatal(err)
	}
	onUS, err := c.Enqueue(ctx, workflow.Job{Workflow: "wait", Labels: map[string]string{"region": "us"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, ref := range []workflow.RunRef{onEU, onUS} {
		waitFor(t, c, ref, workflow.StatusWaiting)
	}
	if _, err := c.ResetRun(ctx, onEU, "w"); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("ResetRun in a region without it: %v, want ErrUnsupported", err)
	}
	payload := []byte(`1`)
	if _, err := c.EditRun(ctx, onEU, workflow.JobEdit{Payload: &payload}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("EditRun in a region without it: %v, want ErrUnsupported", err)
	}
	if n, err := c.SignalWaiting(ctx, workflow.Event{Name: "go"}); !errors.Is(err, workflow.ErrUnsupported) || n != 0 {
		t.Errorf("SignalWaiting with a region without it = %d, %v; want 0, ErrUnsupported", n, err)
	}
	time.Sleep(20 * time.Millisecond)
	if st, err := c.Status(ctx, onUS); err != nil || st != workflow.StatusWaiting {
		t.Errorf("a refused SignalWaiting delivered to us: %s %v", st, err)
	}
	for _, op := range []workflow.Op{workflow.OpEditRun, workflow.OpResetRun, workflow.OpSignalWaiting} {
		if workflow.Supports(c, op) {
			t.Errorf("Supports(%s) = true with a region without it", op)
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
