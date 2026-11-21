package workflowtest

import (
	"context"
	"errors"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

// TestFakeResetSucceeded: ResetRunWith refuses a succeeded run without
// Succeeded, resets it with Succeeded (stamping LabelRedo and recording
// it), and FakeLike of a client without the op refuses it.
func TestFakeResetSucceeded(t *testing.T) {
	ctx := context.Background()
	f := flowFake(t)
	ref, err := f.Enqueue(ctx, workflow.Job{Workflow: "etl", Labels: map[string]string{"app": "x"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Finish(ref, workflow.StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	if _, err := f.ResetRun(ctx, ref, "load"); !errors.Is(err, workflow.ErrWrongState) {
		t.Fatalf("ResetRun of a succeeded run: %v, want ErrWrongState", err)
	}
	got, err := f.ResetRunWith(ctx, ref, "load", workflow.ResetOptions{Succeeded: true})
	if err != nil || got != ref {
		t.Fatalf("ResetRunWith = %+v, %v", got, err)
	}
	f.AssertReset(t, ref, "load")
	if r := f.Resets(); len(r) != 1 || !r[0].Succeeded {
		t.Fatalf("resets %+v, want one with Succeeded", r)
	}
	if j := f.Enqueued("etl"); len(j) != 1 || j[0].Labels[workflow.LabelRedo] != "true" || j[0].Labels["app"] != "x" {
		t.Fatalf("recorded job after the reset: %+v", j)
	}

	like := flowFake(t, FakeLike(bareRunner{memory.New()}))
	if workflow.Supports(like, workflow.OpResetSucceeded) || workflow.Supports(like, workflow.OpForkRun) {
		t.Fatal("FakeLike of a client without the ops says it does them")
	}
	if _, err := like.ResetRunWith(ctx, ref, "load", workflow.ResetOptions{Succeeded: true}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("ResetRunWith on a Fake like a client without it: %v", err)
	}
	if _, err := like.ForkRun(ctx, ref, "load", workflow.ForkOptions{}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("ForkRun on a Fake like a client without it: %v", err)
	}
}

// bareRunner hides every optional interface of the client it wraps.
type bareRunner struct{ workflow.Client }

// TestFakeForkRun: ForkRun refuses an unfinished run, an unknown or empty
// step and a reserved label, then records a fork as a new queued run with
// the fork labels, without the original's key or membership labels, and
// AssertForked finds it.
func TestFakeForkRun(t *testing.T) {
	ctx := workflow.AllowReservedLabels(context.Background(), workflow.LabelParentRun)
	f := flowFake(t)
	ref, err := f.Enqueue(ctx, workflow.Job{Workflow: "etl", IdempotencyKey: "k",
		Labels: map[string]string{"app": "x", workflow.LabelParentRun: "p"}, Chain: []workflow.Job{{Workflow: "etl"}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.ForkRun(ctx, ref, "load", workflow.ForkOptions{}); !errors.Is(err, workflow.ErrWrongState) {
		t.Fatalf("fork of a queued run: %v, want ErrWrongState", err)
	}
	if err := f.Finish(ref, workflow.StatusFailed); err != nil {
		t.Fatal(err)
	}
	for _, step := range []string{"", "nope"} {
		if _, err := f.ForkRun(ctx, ref, step, workflow.ForkOptions{}); !errors.Is(err, workflow.ErrInvalid) {
			t.Fatalf("fork from step %q: %v, want ErrInvalid", step, err)
		}
	}
	if _, err := f.ForkRun(ctx, ref, "load", workflow.ForkOptions{Labels: map[string]string{workflow.LabelRedo: "true"}}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("fork with a reserved label: %v, want ErrInvalid", err)
	}
	fork, err := f.ForkRun(ctx, ref, "load", workflow.ForkOptions{Labels: map[string]string{"ticket": "1"}})
	if err != nil || fork.RunID == "" || fork == ref {
		t.Fatalf("ForkRun = %+v, %v", fork, err)
	}
	if got := f.AssertForked(t, ref, "load"); got != fork {
		t.Fatalf("AssertForked = %+v, want %+v", got, fork)
	}
	if st, _ := f.Status(ctx, fork); st != workflow.StatusQueued {
		t.Fatalf("fork status %s, want queued", st)
	}
	jobs := f.Enqueued("etl")
	if len(jobs) != 2 {
		t.Fatalf("recorded jobs %d, want 2", len(jobs))
	}
	j := jobs[1]
	if j.IdempotencyKey != "" || j.Chain != nil || j.Labels["app"] != "x" || j.Labels["ticket"] != "1" ||
		j.Labels[workflow.LabelForkOf] != ref.RunID.String() || j.Labels[workflow.LabelForkStep] != "load" {
		t.Fatalf("fork job %+v", j)
	}
	if _, ok := j.Labels[workflow.LabelParentRun]; ok {
		t.Fatalf("fork kept the parent-run label: %v", j.Labels)
	}
	if _, ok := j.Labels[workflow.LabelRedo]; ok {
		t.Fatalf("fork of a failed run carries %s", workflow.LabelRedo)
	}
	if len(spyErrs(func(rt testing.TB) { f.AssertForked(rt, ref, "extract") })) == 0 {
		t.Error("AssertForked passed for a step the run was not forked from")
	}
	if _, err := f.ForkRun(ctx, workflow.RunRef{RunID: "nope"}, "load", workflow.ForkOptions{}); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("fork of an unknown run: %v", err)
	}
}
