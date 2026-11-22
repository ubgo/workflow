package hybrid_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/hybrid"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/workflowtest"
)

// startHybrid starts a hybrid client over two memory clients with wfs
// registered, for the shared flow suites.
func startHybrid(t *testing.T, wfs ...workflow.Workflow) workflow.Client {
	t.Helper()
	return startOver(t, memory.New(), memory.New(), wfs...)
}

func startOver(t *testing.T, sched, jobs workflow.Client, wfs ...workflow.Workflow) *hybrid.Client {
	t.Helper()
	c := hybrid.New(sched, jobs)
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
// through hybrid: ResetRun, SignalWaiting and the other run-side calls
// reach the jobs client, where the runs live.
//
//ds:def id=hybrid-testresetandsignalflows-dks2u4vp owner=@khanakia stability=stable
func TestResetAndSignalFlows(t *testing.T) {
	workflowtest.RunResetFlows(t, workflowtest.FlowHarness{Start: startHybrid})
	workflowtest.RunSignalFlows(t, workflowtest.SignalHarness{Start: startHybrid,
		Promises: func(*testing.T, workflow.Client) workflow.PromiseStore { return workflow.NewMemoryPromises() }})
}

// TestNewOps_ReachTheJobsClient: EditRun edits a run on the jobs client,
// and Supports answers for the jobs client.
func TestNewOps_ReachTheJobsClient(t *testing.T) {
	echo := workflow.Workflow{Name: "echo", Steps: []workflow.Step{{Name: "s", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}}
	jobs := memory.New()
	c := startOver(t, memory.New(), jobs, echo)
	ctx := t.Context()
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "echo", Payload: []byte(`"old"`), RunAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	payload, now := []byte(`"edited"`), time.Now()
	edited, err := c.EditRun(ctx, ref, workflow.JobEdit{Payload: &payload, RunAt: &now})
	if err != nil {
		t.Fatalf("EditRun: %v", err)
	}
	out, err := workflow.AwaitResult(ctx, jobs, edited, workflow.AwaitOptions{Poll: 2 * time.Millisecond})
	if err != nil || string(out) != `"edited"` {
		t.Fatalf("edited run's result on the jobs client = %q, %v", out, err)
	}
	for _, op := range []workflow.Op{workflow.OpEditRun, workflow.OpResetRun, workflow.OpResetSucceeded, workflow.OpForkRun, workflow.OpSignalWaiting} {
		if !workflow.Supports(c, op) {
			t.Errorf("Supports(%s) = false over a memory jobs client", op)
		}
	}
}

// bareClient hides every optional interface of the client it wraps.
type bareClient struct{ workflow.Client }

// TestNewOps_RefusedWhenTheJobsClientLacksThem: a jobs client without the
// interfaces makes each call an UnsupportedError and Supports says no,
// whatever the scheduler client does.
//
//ds:def id=hybrid-testnewops-refusedwhenthejobsclientlacksthem-mhnntuuh owner=@khanakia stability=stable
func TestNewOps_RefusedWhenTheJobsClientLacksThem(t *testing.T) {
	wait := workflow.Workflow{Name: "wait", Steps: []workflow.Step{{Name: "w", Wait: &workflow.WaitSpec{Event: "go"}}}}
	c := startOver(t, memory.New(), bareClient{memory.New()}, wait)
	ctx := t.Context()
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "wait"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.ResetRun(ctx, ref, "w"); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("ResetRun: %v, want ErrUnsupported", err)
	}
	if _, err := c.ResetRunWith(ctx, ref, "w", workflow.ResetOptions{Succeeded: true}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("ResetRunWith: %v, want ErrUnsupported", err)
	}
	if _, err := c.ForkRun(ctx, ref, "w", workflow.ForkOptions{}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("ForkRun: %v, want ErrUnsupported", err)
	}
	payload := []byte(`1`)
	if _, err := c.EditRun(ctx, ref, workflow.JobEdit{Payload: &payload}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("EditRun: %v, want ErrUnsupported", err)
	}
	if n, err := c.SignalWaiting(ctx, workflow.Event{Name: "go"}); !errors.Is(err, workflow.ErrUnsupported) || n != 0 {
		t.Errorf("SignalWaiting = %d, %v; want 0, ErrUnsupported", n, err)
	}
	for _, op := range []workflow.Op{workflow.OpEditRun, workflow.OpResetRun, workflow.OpResetSucceeded, workflow.OpForkRun, workflow.OpSignalWaiting} {
		if workflow.Supports(c, op) {
			t.Errorf("Supports(%s) = true with a jobs client without it", op)
		}
	}
}

// TestRedoAndForkFlows runs the shared succeeded-reset and fork suites
// through hybrid.
func TestRedoAndForkFlows(t *testing.T) {
	t.Run("redo", func(t *testing.T) { workflowtest.RunRedoFlows(t, workflowtest.FlowHarness{Start: startHybrid}) })
	t.Run("fork", func(t *testing.T) { workflowtest.RunForkFlows(t, workflowtest.FlowHarness{Start: startHybrid}) })
}
