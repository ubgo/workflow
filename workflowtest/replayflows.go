package workflowtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
)

// ReplayHarness builds what the replay cases run on: Start registers the
// workflows on a fresh client of the driver built with the Journal plugin
// over the driver's durable JournalStore (where it has one) and starts
// it; Promises is a PromiseStore for that client.
type ReplayHarness struct {
	Start    func(t *testing.T, wfs ...workflow.Workflow) workflow.Client
	Promises func(t *testing.T, c workflow.Client) workflow.PromiseStore
}

// RunReplayFlows proves NewReplay on a driver (DE-08, XM-01): code that
// re-runs from the top keeps its locals across a sleep and a promise wait
// while each recorded call runs once; a recorded Permanent error replays
// without calling its function again; and code that diverges from its
// history (another call at a position, or fewer calls) fails the run
// with the nondeterminism error. The driver needs CapSnooze.
func RunReplayFlows(t *testing.T, h ReplayHarness) {
	t.Run("locals", func(t *testing.T) { replayLocals(t, h) })
	t.Run("recorded-error", func(t *testing.T) { replayRecordedError(t, h) })
	t.Run("nondeterminism", func(t *testing.T) { replayDiverges(t, h) })
}

func replayWorkflow(t *testing.T, name string, fn workflow.ReplayFunc, opts workflow.ReplayOptions) workflow.Workflow {
	t.Helper()
	run, err := workflow.NewReplay(fn, opts)
	if err != nil {
		t.Fatal(err)
	}
	return workflow.Workflow{Name: name, Steps: []workflow.Step{{Name: "orchestrate", Run: run}},
		Retry: workflow.RetryPolicy{Max: 3, Backoff: workflow.Backoff{Initial: 10 * time.Millisecond}}}
}

func replayLocals(t *testing.T, h ReplayHarness) {
	var bodies, reserves, charges atomic.Int32
	var promises workflow.PromiseStore
	const nap = 300 * time.Millisecond
	wf := replayWorkflow(t, "replay-locals", func(rc *workflow.ReplayCtx, in []byte) ([]byte, error) {
		bodies.Add(1)
		order := string(in)
		hold, err := workflow.ReplayStep(rc, "reserve", func(context.Context) (string, error) {
			reserves.Add(1)
			return "hold-" + fmt.Sprint(reserves.Load()), nil // differs per call: a replay must not call it
		})
		if err != nil {
			return nil, err
		}
		if err := rc.Sleep("cool-off", nap); err != nil {
			return nil, err
		}
		started, err := rc.Now("after-sleep")
		if err != nil {
			return nil, err
		}
		charge, err := workflow.ReplayStep(rc, "charge", func(context.Context) (int, error) {
			charges.Add(1)
			return 40, nil
		})
		if err != nil {
			return nil, err
		}
		approval, err := rc.AwaitPromise("approval", promises, "approve-"+order)
		if err != nil {
			return nil, err
		}
		if started.IsZero() {
			return nil, errors.New("Now returned the zero time")
		}
		return json.Marshal(fmt.Sprintf("%s %s charged %d by %s", order, hold, charge, approval))
	}, workflow.ReplayOptions{Poll: 50 * time.Millisecond})
	c := h.Start(t, wf)
	needCaps(t, c, workflow.CapSnooze)
	promises = h.Promises(t, c)
	ctx := context.Background()
	start := time.Now()
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "replay-locals", Payload: []byte("A-1")})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	// Body 1 stops at the sleep, body 2 at the promise, body 3 looks at
	// the still unsettled promise again; once body 4 has started, body 3
	// surely found it unsettled, and the promise is resolved.
	for charges.Load() == 0 || bodies.Load() < 4 {
		if time.Now().After(deadline) {
			t.Fatalf("the code never waited on the promise: charge %d, bodies %d", charges.Load(), bodies.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := workflow.ResolvePromise(ctx, c, promises, "approve-A-1", []byte("ops")); err != nil {
		t.Fatal(err)
	}
	out, err := flowAwait(t, c, ref)
	if err != nil {
		t.Fatal(err)
	}
	var got string
	if err := json.Unmarshal(out, &got); err != nil || got != "A-1 hold-1 charged 40 by ops" {
		t.Fatalf("result %s %v", out, err)
	}
	if time.Since(start) < nap {
		t.Fatalf("finished after %s, before the %s sleep", time.Since(start), nap)
	}
	if reserves.Load() != 1 || charges.Load() != 1 || bodies.Load() < 4 {
		t.Fatalf("reserve ran %d, charge %d, body %d times; want 1, 1 and at least 4", reserves.Load(), charges.Load(), bodies.Load())
	}
}

func replayRecordedError(t *testing.T, h ReplayHarness) {
	var calls, bodies atomic.Int32
	wf := replayWorkflow(t, "replay-recorded-error", func(rc *workflow.ReplayCtx, _ []byte) ([]byte, error) {
		bodies.Add(1)
		_, err := workflow.ReplayStep(rc, "charge", func(context.Context) (int, error) {
			calls.Add(1)
			return 0, workflow.Permanent(errors.New("card declined"))
		})
		outcome := "charged"
		if err != nil {
			if !workflow.IsPermanent(err) || !strings.Contains(err.Error(), "card declined") {
				return nil, fmt.Errorf("replayed error %v", err)
			}
			outcome = "declined"
		}
		if err := rc.Sleep("then", 50*time.Millisecond); err != nil {
			return nil, err
		}
		return json.Marshal(outcome)
	}, workflow.ReplayOptions{})
	c := h.Start(t, wf)
	needCaps(t, c, workflow.CapSnooze)
	ref, err := c.Enqueue(context.Background(), workflow.Job{Workflow: "replay-recorded-error"})
	if err != nil {
		t.Fatal(err)
	}
	out, err := flowAwait(t, c, ref)
	if err != nil || string(out) != `"declined"` {
		t.Fatalf("result %s %v", out, err)
	}
	if calls.Load() != 1 || bodies.Load() < 2 {
		t.Fatalf("charge called %d times over %d bodies, want once over at least 2", calls.Load(), bodies.Load())
	}
}

func replayDiverges(t *testing.T, h ReplayHarness) {
	var swap, shorter atomic.Int32
	other := replayWorkflow(t, "replay-swap", func(rc *workflow.ReplayCtx, _ []byte) ([]byte, error) {
		// Branches on a value it did not record: the second run calls a
		// different step at position 0.
		name := fmt.Sprintf("step-%d", swap.Add(1))
		if _, err := workflow.ReplayStep(rc, name, func(context.Context) (int, error) { return 1, nil }); err != nil {
			return nil, err
		}
		return nil, rc.Sleep("wait", 30*time.Millisecond)
	}, workflow.ReplayOptions{})
	short := replayWorkflow(t, "replay-short", func(rc *workflow.ReplayCtx, _ []byte) ([]byte, error) {
		if shorter.Add(1) > 1 {
			return []byte(`"skipped"`), nil // the second run makes no call
		}
		if _, err := workflow.ReplayStep(rc, "x", func(context.Context) (int, error) { return 1, nil }); err != nil {
			return nil, err
		}
		return nil, rc.Sleep("wait", 30*time.Millisecond)
	}, workflow.ReplayOptions{})
	c := h.Start(t, other, short)
	needCaps(t, c, workflow.CapSnooze)
	for name, want := range map[string]string{"replay-swap": "history has step step-1", "replay-short": "returned before call 0"} {
		ref, err := c.Enqueue(context.Background(), workflow.Job{Workflow: name})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := flowAwait(t, c, ref); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("%s: got %v, want an error containing %q", name, err, want)
		}
	}
	if swap.Load() != 2 {
		t.Fatalf("a nondeterminism error was retried: the body ran %d times, want 2", swap.Load())
	}
}
