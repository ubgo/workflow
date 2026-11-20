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
	t.Run("child", func(t *testing.T) { replayChild(t, h) })
}

// replayChild: replayed code awaits a child run; the child runs once and
// its result is recorded, so later runs of the body read it from history.
func replayChild(t *testing.T, h ReplayHarness) {
	var children, bodies atomic.Int32
	var c workflow.Client
	release := make(chan struct{})
	child := workflow.Workflow{Name: "replay-child-quote", Steps: []workflow.Step{{Name: "quote", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		children.Add(1)
		select { // runs until the test has seen the parent look at it unfinished
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return json.Marshal("quote for " + string(in))
	}}}}
	parent := replayWorkflow(t, "replay-parent", func(rc *workflow.ReplayCtx, in []byte) ([]byte, error) {
		bodies.Add(1)
		out, err := workflow.ReplayChild(rc, "quote", c, workflow.Job{Workflow: "replay-child-quote", Payload: in}, workflow.ChildOptions{Poll: 20 * time.Millisecond})
		if err != nil {
			return nil, err
		}
		return out, nil
	}, workflow.ReplayOptions{})
	c = h.Start(t, child, parent)
	needCaps(t, c, workflow.CapSnooze, workflow.CapResult, workflow.CapIdempotencyKey)
	ref, err := c.Enqueue(context.Background(), workflow.Job{Workflow: "replay-parent", Payload: []byte("B-7")})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	// Body 1 spawned the child, body 2 found it unfinished; once body 3
	// has started, release the child.
	for bodies.Load() < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("the parent never looked at its child again: %d bodies", bodies.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(release)
	out, err := flowAwait(t, c, ref)
	if err != nil || string(out) != `"quote for B-7"` {
		t.Fatalf("result %s %v", out, err)
	}
	if children.Load() != 1 || bodies.Load() < 3 {
		t.Fatalf("child ran %d times over %d bodies, want once over at least 3", children.Load(), bodies.Load())
	}
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
	var promises workflow.PromiseStore
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
		if _, err := rc.AwaitPromise("then", promises, "recorded-error"); err != nil {
			return nil, err
		}
		return json.Marshal(outcome)
	}, workflow.ReplayOptions{Poll: 20 * time.Millisecond})
	c := h.Start(t, wf)
	needCaps(t, c, workflow.CapSnooze)
	promises = h.Promises(t, c)
	ref, err := c.Enqueue(context.Background(), workflow.Job{Workflow: "replay-recorded-error"})
	if err != nil {
		t.Fatal(err)
	}
	// Resolve once a second body has started: the first stopped at the
	// promise after recording the error, so the second replays it.
	deadline := time.Now().Add(20 * time.Second)
	for bodies.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("the code never woke again: %d bodies", bodies.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := workflow.ResolvePromise(context.Background(), c, promises, "recorded-error", []byte("go")); err != nil {
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
	// Each body diverges only after a run recorded its first call (a
	// flag set once the call returned), so a step retried after a
	// transient store error does not move the divergence.
	var swapped, swapAfter, shortened atomic.Int32
	var promises workflow.PromiseStore
	other := replayWorkflow(t, "replay-swap", func(rc *workflow.ReplayCtx, _ []byte) ([]byte, error) {
		// Branches on a value it did not record: once the first call is
		// recorded, a later run calls a different step at position 0.
		name := "step-1"
		if swapped.Load() > 0 {
			name = "step-2"
			swapAfter.Add(1)
		}
		if _, err := workflow.ReplayStep(rc, name, func(context.Context) (int, error) { return 1, nil }); err != nil {
			return nil, err
		}
		swapped.Store(1)
		_, err := rc.AwaitPromise("wait", promises, "never-swap") // never resolved: always suspends
		return nil, err
	}, workflow.ReplayOptions{Poll: 20 * time.Millisecond})
	short := replayWorkflow(t, "replay-short", func(rc *workflow.ReplayCtx, _ []byte) ([]byte, error) {
		if shortened.Load() > 0 {
			return []byte(`"skipped"`), nil // a later run makes no call
		}
		if _, err := workflow.ReplayStep(rc, "x", func(context.Context) (int, error) { return 1, nil }); err != nil {
			return nil, err
		}
		shortened.Store(1)
		_, err := rc.AwaitPromise("wait", promises, "never-short")
		return nil, err
	}, workflow.ReplayOptions{Poll: 20 * time.Millisecond})
	c := h.Start(t, other, short)
	needCaps(t, c, workflow.CapSnooze)
	promises = h.Promises(t, c)
	for name, want := range map[string]string{"replay-swap": "history has step step-1", "replay-short": "returned before call 0"} {
		ref, err := c.Enqueue(context.Background(), workflow.Job{Workflow: name})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := flowAwait(t, c, ref); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("%s: got %v, want an error containing %q", name, err, want)
		}
	}
	if swapAfter.Load() != 1 {
		t.Fatalf("a nondeterminism error was retried: the diverging body ran %d times, want once", swapAfter.Load())
	}
}
