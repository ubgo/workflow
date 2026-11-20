package workflowtest

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
)

// redoETL is the three-step workflow the redo and fork flows use: extract,
// transform (whose output depends on fixed), load. counts counts the
// attempts of each step.
type redoETL struct {
	name                           string
	extracted, transformed, loaded atomic.Int32
	fixed                          atomic.Bool
	failLoadUnfixed                bool
	next                           string // the chained workflow, when set
	nextRuns                       atomic.Int32
	hold                           chan struct{} // load blocks on it when set
	holdOn                         atomic.Bool
}

func (e *redoETL) workflows() []workflow.Workflow {
	wf := workflow.Workflow{Name: e.name, Steps: []workflow.Step{
		{Name: "extract", Run: func(context.Context, []byte) ([]byte, error) {
			e.extracted.Add(1)
			return json.Marshal("rows")
		}},
		{Name: "transform", After: []string{"extract"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
			e.transformed.Add(1)
			var rows string
			if err := json.Unmarshal(in, &rows); err != nil {
				return nil, workflow.Permanent(err)
			}
			if e.fixed.Load() {
				return json.Marshal(rows + " cleaned")
			}
			return json.Marshal(rows + " raw")
		}},
		{Name: "load", After: []string{"transform"}, Run: func(ctx context.Context, in []byte) ([]byte, error) {
			e.loaded.Add(1)
			if e.holdOn.Load() {
				select {
				case <-e.hold:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			var v string
			if err := json.Unmarshal(in, &v); err != nil {
				return nil, workflow.Permanent(err)
			}
			if e.failLoadUnfixed && v != "rows cleaned" {
				return nil, workflow.Permanent(errors.New("load refused " + v))
			}
			return json.Marshal("loaded " + v)
		}},
	}}
	out := []workflow.Workflow{wf}
	if e.next != "" {
		out = append(out, workflow.Workflow{Name: e.next, Steps: []workflow.Step{{Name: "n",
			Run: func(context.Context, []byte) ([]byte, error) { e.nextRuns.Add(1); return nil, nil }}}})
	}
	return out
}

func (e *redoETL) counts() [3]int32 {
	return [3]int32{e.extracted.Load(), e.transformed.Load(), e.loaded.Load()}
}

// runLabels reads ref's labels through RunQuerier; nil when c has none.
func runLabels(t *testing.T, c workflow.Client, ref workflow.RunRef) map[string]string {
	t.Helper()
	rq, ok := c.(workflow.RunQuerier)
	if !ok || !c.Capabilities().Has(workflow.CapRunQuery) {
		return nil
	}
	page, err := rq.Runs(context.Background(), workflow.RunQuery{Ref: ref})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Runs) != 1 {
		t.Fatalf("Runs(%v): %d runs, want 1", ref, len(page.Runs))
	}
	return page.Runs[0].Labels
}

// findByLabel asserts that a label query for key=val returns exactly
// want, when c has RunQuerier.
func findByLabel(t *testing.T, c workflow.Client, key, val string, want workflow.RunRef) {
	t.Helper()
	rq, ok := c.(workflow.RunQuerier)
	if !ok || !c.Capabilities().Has(workflow.CapRunQuery) {
		return
	}
	page, err := rq.Runs(context.Background(), workflow.RunQuery{Labels: map[string]string{key: val}})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Runs) != 1 || page.Runs[0].Ref.RunID != want.RunID {
		t.Fatalf("runs with %s=%s: %d runs (%v), want only %v", key, val, len(page.Runs), page.Runs, want.RunID)
	}
}

// waitCount waits until n() is want, or fails after 10 seconds.
func waitCount(t *testing.T, what string, n func() int32, want int32) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for n() != want {
		if time.Now().After(deadline) {
			t.Fatalf("%s = %d, want %d", what, n(), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// RunRedoFlows proves workflow.RunResetterWith on a driver: ResetRun and
// ResetRunWith without Succeeded refuse a succeeded run; with Succeeded the
// run keeps its RunRef, runs the chosen step and every step after it
// again (not the step before), ends with the new result, carries
// LabelRedo, and does not hand its chain on a second time. A driver
// without the operation (workflow.Supports) skips.
func RunRedoFlows(t *testing.T, h FlowHarness) {
	e := &redoETL{name: "redo-etl", next: "redo-next"}
	c := h.Start(t, e.workflows()...)
	if !workflow.Supports(c, workflow.OpResetSucceeded) {
		t.Skip("driver does not reset a succeeded run")
	}
	rs := c.(workflow.RunResetterWith)
	ctx := context.Background()
	job := workflow.Job{Workflow: e.name, Labels: map[string]string{"app": "x"}}
	chained := c.Capabilities().Has(workflow.CapChain)
	if chained {
		job.Chain = []workflow.Job{{Workflow: e.next}}
	}
	ref, err := c.Enqueue(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	if out, err := flowAwait(t, c, ref); err != nil || string(out) != `"loaded rows raw"` {
		t.Fatalf("first run: %q %v", out, err)
	}
	if chained {
		waitCount(t, "chained runs", e.nextRuns.Load, 1)
	}
	if _, err := rs.ResetRunWith(ctx, ref, "transform", workflow.ResetOptions{}); !errors.Is(err, workflow.ErrWrongState) {
		t.Fatalf("ResetRunWith without Succeeded on a succeeded run: %v, want ErrWrongState", err)
	}
	if r, ok := c.(workflow.RunResetter); ok {
		if _, err := r.ResetRun(ctx, ref, "transform"); !errors.Is(err, workflow.ErrWrongState) {
			t.Fatalf("ResetRun on a succeeded run: %v, want ErrWrongState", err)
		}
	}
	if _, err := rs.ResetRunWith(ctx, ref, "nope", workflow.ResetOptions{Succeeded: true}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("unknown step: %v, want ErrInvalid", err)
	}
	e.fixed.Store(true)
	again, err := rs.ResetRunWith(ctx, ref, "transform", workflow.ResetOptions{Succeeded: true})
	if err != nil {
		t.Fatal(err)
	}
	if again.RunID != ref.RunID {
		t.Fatalf("reset changed the run: %v -> %v", ref, again)
	}
	if out, err := flowAwait(t, c, again); err != nil || string(out) != `"loaded rows cleaned"` {
		t.Fatalf("reset run: %q %v", out, err)
	}
	if got := e.counts(); got != [3]int32{1, 2, 2} {
		t.Fatalf("extract, transform, load ran %v times, want [1 2 2]", got)
	}
	if rr, ok := c.(workflow.ResultReader); ok {
		if out, err := rr.Result(ctx, ref); err != nil || string(out) != `"loaded rows cleaned"` {
			t.Fatalf("Result after the reset: %q %v", out, err)
		}
	}
	if l := runLabels(t, c, ref); l != nil && (l[workflow.LabelRedo] != "true" || l["app"] != "x") {
		t.Fatalf("labels after the reset: %v, want %s=true and app=x", l, workflow.LabelRedo)
	}
	findByLabel(t, c, workflow.LabelRedo, "true", ref)
	if chained {
		time.Sleep(50 * time.Millisecond) // give a wrong second handoff time to show
		if n := e.nextRuns.Load(); n != 1 {
			t.Fatalf("the chain ran %d times, want 1: a reset of a succeeded run must not hand it on again", n)
		}
	}
	// A reset run that failed is still reset by plain ResetRun later: it
	// does not need Succeeded once it has failed.
}

// RunForkFlows proves workflow.RunForker on a driver: a fork of a failed
// run starts from the chosen step under a new RunID with the outputs of
// the steps before it, ends with its own result and leaves the original
// failed; a fork of a succeeded run gets LabelRedo; forks carry
// LabelForkOf, LabelForkStep and the app's labels but not the parent-run
// label or a chain; an unfinished run, an unknown step, an empty step and
// a reserved label in ForkOptions are refused. A driver without the
// operation skips.
func RunForkFlows(t *testing.T, h FlowHarness) {
	e := &redoETL{name: "fork-etl", next: "fork-next", failLoadUnfixed: true, hold: make(chan struct{})}
	c := h.Start(t, e.workflows()...)
	if !workflow.Supports(c, workflow.OpForkRun) {
		t.Skip("driver does not fork runs")
	}
	t.Cleanup(func() { close(e.hold) })
	fk := c.(workflow.RunForker)
	ctx := workflow.AllowReservedLabels(context.Background(), workflow.LabelParentRun)
	job := workflow.Job{Workflow: e.name, Labels: map[string]string{"app": "x", workflow.LabelParentRun: "some-parent"}}
	chained := c.Capabilities().Has(workflow.CapChain)
	if chained {
		job.Chain = []workflow.Job{{Workflow: e.next}}
	}
	if c.Capabilities().Has(workflow.CapIdempotencyKey) {
		job.IdempotencyKey = "fork-original"
	}
	ref, err := c.Enqueue(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := flowAwait(t, c, ref); err == nil {
		t.Fatal("the first run must fail at load")
	}
	for _, tc := range []struct {
		name string
		step string
		opts workflow.ForkOptions
	}{
		{"unknown step", "nope", workflow.ForkOptions{}},
		{"empty step", "", workflow.ForkOptions{}},
		{"reserved label", "transform", workflow.ForkOptions{Labels: map[string]string{workflow.LabelForkOf: "x"}}},
	} {
		if _, err := fk.ForkRun(ctx, ref, tc.step, tc.opts); !errors.Is(err, workflow.ErrInvalid) {
			t.Fatalf("fork with %s: %v, want ErrInvalid", tc.name, err)
		}
	}
	if _, err := fk.ForkRun(ctx, workflow.RunRef{RunID: "no-such-run"}, "transform", workflow.ForkOptions{}); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("fork of an unknown run: %v, want ErrNotFound", err)
	}

	e.fixed.Store(true)
	fork, err := fk.ForkRun(ctx, ref, "transform", workflow.ForkOptions{Labels: map[string]string{"ticket": "42"}})
	if err != nil {
		t.Fatal(err)
	}
	if fork.RunID.IsZero() || fork.RunID == ref.RunID {
		t.Fatalf("fork ref %v: want a new RunID beside %v", fork, ref)
	}
	if out, err := flowAwait(t, c, fork); err != nil || string(out) != `"loaded rows cleaned"` {
		t.Fatalf("fork: %q %v", out, err)
	}
	if got := e.counts(); got != [3]int32{1, 2, 2} {
		t.Fatalf("extract, transform, load ran %v times, want [1 2 2]", got)
	}
	if st, err := c.Status(ctx, ref); err != nil || st != workflow.StatusFailed {
		t.Fatalf("the original after the fork: %s %v, want failed", st, err)
	}
	if l := runLabels(t, c, fork); l != nil {
		want := map[string]string{"app": "x", "ticket": "42", workflow.LabelForkOf: ref.RunID.String(), workflow.LabelForkStep: "transform"}
		for k, v := range want {
			if l[k] != v {
				t.Fatalf("fork label %s = %q, want %q (labels %v)", k, l[k], v, l)
			}
		}
		for _, k := range []string{workflow.LabelParentRun, workflow.LabelRedo} {
			if _, ok := l[k]; ok {
				t.Fatalf("fork of a failed run carries %s: %v", k, l)
			}
		}
	}
	findByLabel(t, c, workflow.LabelForkOf, ref.RunID.String(), fork)
	if chained {
		time.Sleep(50 * time.Millisecond)
		if n := e.nextRuns.Load(); n != 0 {
			t.Fatalf("a fork handed on the original's chain: %d chained runs", n)
		}
	}

	// A fork of the succeeded fork, from load, gets LabelRedo and runs load only.
	again, err := fk.ForkRun(ctx, fork, "load", workflow.ForkOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if out, err := flowAwait(t, c, again); err != nil || string(out) != `"loaded rows cleaned"` {
		t.Fatalf("fork of the fork: %q %v", out, err)
	}
	if got := e.counts(); got != [3]int32{1, 2, 3} {
		t.Fatalf("after the second fork extract, transform, load ran %v times, want [1 2 3]", got)
	}
	if l := runLabels(t, c, again); l != nil && (l[workflow.LabelRedo] != "true" || l[workflow.LabelForkOf] != fork.RunID.String()) {
		t.Fatalf("fork of a succeeded run: labels %v, want %s=true and %s=%s", l, workflow.LabelRedo, workflow.LabelForkOf, fork.RunID)
	}

	// An unfinished run cannot be forked.
	e.holdOn.Store(true)
	held, err := c.Enqueue(ctx, workflow.Job{Workflow: e.name})
	if err != nil {
		t.Fatal(err)
	}
	waitCount(t, "load attempts", e.loaded.Load, 4)
	if _, err := fk.ForkRun(ctx, held, "transform", workflow.ForkOptions{}); !errors.Is(err, workflow.ErrWrongState) {
		t.Fatalf("fork of a running run: %v, want ErrWrongState", err)
	}
}
