package workflowtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
)

// FlowHarness builds the client the child-flow cases run on. Start must
// register wfs on a client that has the Journal plugin (workflow.Use with
// workflow.Journal over the driver's durable JournalStore where it has
// one), start it, and stop it when t ends.
type FlowHarness struct {
	Start func(t *testing.T, wfs ...workflow.Workflow) workflow.Client
}

// childGate is a child workflow whose runs end when told: payload "ok"
// succeeds, "fail" fails, "hold" waits until released or cancelled.
type childGate struct {
	name     string
	mu       sync.Mutex
	released bool
	cond     chan struct{}
}

func newChildGate(name string) *childGate { return &childGate{name: name, cond: make(chan struct{})} }

func (g *childGate) release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.released {
		g.released = true
		close(g.cond)
	}
}

func (g *childGate) workflow() workflow.Workflow {
	return workflow.Workflow{Name: g.name, Steps: []workflow.Step{{Name: "work", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		var mode string
		if err := json.Unmarshal(in, &mode); err != nil {
			return nil, workflow.Permanent(err)
		}
		switch mode {
		case "fail":
			return nil, workflow.Permanent(errors.New("child failed"))
		case "hold":
			select {
			case <-g.cond:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return json.Marshal("done " + mode)
	}}}}
}

func (g *childGate) jobs(modes ...string) []workflow.Job {
	out := make([]workflow.Job, len(modes))
	for i, m := range modes {
		out[i] = workflow.Job{Workflow: g.name, Payload: []byte(`"` + m + `"`)}
	}
	return out
}

func flowAwait(t *testing.T, c workflow.Client, ref workflow.RunRef) ([]byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return workflow.AwaitResult(ctx, c, ref, workflow.AwaitOptions{Poll: 5 * time.Millisecond})
}

func flowStatus(t *testing.T, c workflow.Client, ref workflow.RunRef, want workflow.Status) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		st, err := c.Status(context.Background(), ref)
		if err != nil {
			t.Fatal(err)
		}
		if st == want {
			return
		}
		if st.IsTerminal() || time.Now().After(deadline) {
			t.Fatalf("run %v is %s, want %s", ref, st, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// RunChildFlows proves the child-run helpers of the core on a driver:
// JoinChildren (discriminator, cancelling and completing partial joins, a
// failed join), CancelChildren and MapRuns. The driver needs CapSnooze,
// CapIdempotencyKey, CapResult, CapCancel, CapCancelAll, CapForceRun and
// CapRunQuery; a driver without one of them skips the cases that need it.
func RunChildFlows(t *testing.T, h FlowHarness) {
	t.Run("discriminator", func(t *testing.T) {
		g := newChildGate("flow-child")
		defer g.release()
		var c workflow.Client
		parent := workflow.Workflow{Name: "flow-parent", Steps: []workflow.Step{{Name: "join", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			res, err := workflow.JoinChildren(ctx, c, "kids", g.jobs("hold", "ok", "hold"), workflow.JoinSpec{Need: 1, Poll: 5 * time.Millisecond})
			if err != nil {
				return nil, err
			}
			return json.Marshal(res)
		}}}}
		c = h.Start(t, g.workflow(), parent)
		needCaps(t, c, workflow.CapSnooze, workflow.CapIdempotencyKey, workflow.CapResult)
		ref, err := c.Enqueue(context.Background(), workflow.Job{Workflow: "flow-parent"})
		if err != nil {
			t.Fatal(err)
		}
		out, err := flowAwait(t, c, ref)
		if err != nil {
			t.Fatal(err)
		}
		var res workflow.JoinResult
		if err := json.Unmarshal(out, &res); err != nil {
			t.Fatal(err)
		}
		if len(res.Winners) != 1 || res.Winners[0] != 1 || string(res.Outputs[0]) != `"done ok"` {
			t.Fatalf("winners %v outputs %q", res.Winners, res.Outputs)
		}
		for _, i := range []int{0, 2} {
			if st, err := c.Status(context.Background(), res.Refs[i]); err != nil || st.IsTerminal() {
				t.Fatalf("loser %d is %s (%v), want still running", i, st, err)
			}
		}
		g.release()
		flowStatus(t, c, res.Refs[0], workflow.StatusSucceeded)
	})

	t.Run("cancelling-partial", func(t *testing.T) {
		g := newChildGate("flow-child")
		defer g.release()
		var c workflow.Client
		parent := workflow.Workflow{Name: "flow-parent", Steps: []workflow.Step{{Name: "join", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			res, err := workflow.JoinChildren(ctx, c, "kids", g.jobs("ok", "hold", "ok"), workflow.JoinSpec{Need: 2, Rest: workflow.JoinCancelRest, Poll: 5 * time.Millisecond})
			if err != nil {
				return nil, err
			}
			return json.Marshal(res)
		}}}}
		c = h.Start(t, g.workflow(), parent)
		needCaps(t, c, workflow.CapSnooze, workflow.CapIdempotencyKey, workflow.CapResult, workflow.CapCancel)
		ref, err := c.Enqueue(context.Background(), workflow.Job{Workflow: "flow-parent"})
		if err != nil {
			t.Fatal(err)
		}
		out, err := flowAwait(t, c, ref)
		if err != nil {
			t.Fatal(err)
		}
		var res workflow.JoinResult
		if err := json.Unmarshal(out, &res); err != nil {
			t.Fatal(err)
		}
		if fmt.Sprint(res.Winners) != "[0 2]" {
			t.Fatalf("winners %v", res.Winners)
		}
		flowStatus(t, c, res.Refs[1], workflow.StatusCancelled)
	})

	t.Run("complete-rest", func(t *testing.T) {
		g := newChildGate("flow-child")
		defer g.release()
		var c workflow.Client
		parent := workflow.Workflow{Name: "flow-parent", Steps: []workflow.Step{{Name: "join", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			res, err := workflow.JoinChildren(ctx, c, "kids", g.jobs("hold", "ok"), workflow.JoinSpec{Need: 1, Rest: workflow.JoinCompleteRest, Poll: 5 * time.Millisecond})
			if err != nil {
				return nil, err
			}
			return json.Marshal(res)
		}}}}
		c = h.Start(t, g.workflow(), parent)
		needCaps(t, c, workflow.CapSnooze, workflow.CapIdempotencyKey, workflow.CapResult, workflow.CapForceRun, workflow.CapCancel)
		ref, err := c.Enqueue(context.Background(), workflow.Job{Workflow: "flow-parent"})
		if err != nil {
			t.Fatal(err)
		}
		out, err := flowAwait(t, c, ref)
		if err != nil {
			t.Fatal(err)
		}
		var res workflow.JoinResult
		if err := json.Unmarshal(out, &res); err != nil {
			t.Fatal(err)
		}
		flowStatus(t, c, res.Refs[0], workflow.StatusSucceeded)
	})

	t.Run("failed-join", func(t *testing.T) {
		g := newChildGate("flow-child")
		defer g.release()
		var c workflow.Client
		parent := workflow.Workflow{Name: "flow-parent", Steps: []workflow.Step{{Name: "join", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			_, err := workflow.JoinChildren(ctx, c, "kids", g.jobs("fail", "hold", "fail"), workflow.JoinSpec{Need: 2, Rest: workflow.JoinCancelRest, Poll: 5 * time.Millisecond})
			return nil, err
		}}}}
		c = h.Start(t, g.workflow(), parent)
		needCaps(t, c, workflow.CapSnooze, workflow.CapIdempotencyKey, workflow.CapCancel, workflow.CapRunQuery)
		ref, err := c.Enqueue(context.Background(), workflow.Job{Workflow: "flow-parent"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := flowAwait(t, c, ref); err == nil {
			t.Fatal("a join that cannot pass must fail its step")
		}
		page, err := c.(workflow.RunQuerier).Runs(context.Background(), workflow.RunQuery{Workflow: g.name, Statuses: []workflow.Status{workflow.StatusCancelled}})
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Runs) != 1 {
			t.Fatalf("%d children cancelled, want the one still running", len(page.Runs))
		}
	})

	t.Run("cancel-children", func(t *testing.T) {
		g := newChildGate("flow-child")
		defer g.release()
		var c workflow.Client
		ids := make(chan workflow.RunID, 1)
		parent := workflow.Workflow{Name: "flow-parent", Steps: []workflow.Step{{Name: "fan", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			if _, err := workflow.Spawn(ctx, c, "kids", g.jobs("hold", "hold")); err != nil {
				return nil, err
			}
			info, _ := workflow.StepInfoFrom(ctx)
			select {
			case ids <- info.RunID:
			default:
			}
			return nil, nil
		}}}}
		c = h.Start(t, g.workflow(), parent)
		needCaps(t, c, workflow.CapIdempotencyKey, workflow.CapCancelAll, workflow.CapRunQuery)
		ref, err := c.Enqueue(context.Background(), workflow.Job{Workflow: "flow-parent"})
		if err != nil {
			t.Fatal(err)
		}
		flowStatus(t, c, ref, workflow.StatusSucceeded)
		n, err := workflow.CancelChildren(context.Background(), c, <-ids, "fan")
		if err != nil || n != 2 {
			t.Fatalf("CancelChildren = %d, %v; want 2", n, err)
		}
	})

	t.Run("map-runs", func(t *testing.T) {
		g := newChildGate("flow-child")
		defer g.release()
		var c workflow.Client
		modes := []string{"ok", "fail", "ok", "ok", "ok", "fail", "ok"}
		parent := workflow.Workflow{Name: "flow-parent", Steps: []workflow.Step{{Name: "map", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			res, err := workflow.MapRuns(ctx, c, "items", func(_ context.Context, cursor string) (workflow.MapPage[string], error) {
				start := 0
				if cursor != "" {
					if _, err := fmt.Sscan(cursor, &start); err != nil {
						return workflow.MapPage[string]{}, err
					}
				}
				end := min(start+3, len(modes))
				return workflow.MapPage[string]{Items: modes[start:end], Next: fmt.Sprint(end), Done: end == len(modes)}, nil
			}, func(_ int, m string) (workflow.Job, error) {
				return g.jobs(m)[0], nil
			}, workflow.MapRunsOptions{InFlight: 2, MaxFailures: 2, Poll: 5 * time.Millisecond})
			if err != nil {
				return nil, err
			}
			return json.Marshal(res)
		}}}}
		c = h.Start(t, g.workflow(), parent)
		needCaps(t, c, workflow.CapSnooze, workflow.CapIdempotencyKey)
		ref, err := c.Enqueue(context.Background(), workflow.Job{Workflow: "flow-parent"})
		if err != nil {
			t.Fatal(err)
		}
		out, err := flowAwait(t, c, ref)
		if err != nil {
			t.Fatal(err)
		}
		var res workflow.MapRunsResult
		if err := json.Unmarshal(out, &res); err != nil {
			t.Fatal(err)
		}
		if res.Started != 7 || res.Succeeded != 5 || res.Failed != 2 {
			t.Fatalf("map result %+v", res)
		}
	})
}

// needCaps skips the case when c lacks any of want.
func needCaps(t *testing.T, c workflow.Client, want ...workflow.Caps) {
	t.Helper()
	for _, w := range want {
		if !c.Capabilities().Has(w) {
			t.Skipf("driver lacks %s", w)
		}
	}
}
