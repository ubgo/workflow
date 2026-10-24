package workflow_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ubgo/workflow"
)

// fakeDrainer records the ctx it was drained with.
type fakeDrainer struct {
	got    context.Context
	report workflow.DrainReport
}

func (f *fakeDrainer) Drain(ctx context.Context) error { f.got = ctx; return nil }
func (f *fakeDrainer) Done() <-chan struct{}           { return nil }

type reportingFake struct{ fakeDrainer }

func (f *reportingFake) Drain(ctx context.Context) error { f.got = ctx; return ctx.Err() }

func (f *reportingFake) DrainWithReport(ctx context.Context) (workflow.DrainReport, error) {
	f.got = ctx
	return f.report, ctx.Err()
}

// TestDrainOutcome pins the one decision: an error only when something was
// cut off.
func TestDrainOutcome(t *testing.T) {
	cut := workflow.DrainReport{Cut: []workflow.DrainCut{{RunID: "r", Step: "s"}}}
	for _, tc := range []struct {
		name   string
		report workflow.DrainReport
		ctxErr error
		want   error
	}{
		{"clean, ctx fine", workflow.DrainReport{}, nil, nil},
		{"clean, ctx also ended", workflow.DrainReport{}, context.DeadlineExceeded, nil},
		{"cut, ctx ended", cut, context.DeadlineExceeded, context.DeadlineExceeded},
		{"cut, cancelled", cut, context.Canceled, context.Canceled},
		{"cut, no ctx error", cut, nil, nil},
	} {
		if got := workflow.DrainOutcome(tc.report, tc.ctxErr); !errors.Is(got, tc.want) || (tc.want == nil && got != nil) {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestDrainContext: grace bounds the wait; zero or negative means cut off
// at once, never unlimited; a tighter parent deadline wins.
func TestDrainContext(t *testing.T) {
	for _, g := range []time.Duration{0, -time.Second} {
		ctx, cancel := workflow.DrainContext(context.Background(), g)
		if ctx.Err() == nil {
			t.Errorf("grace %v: ctx still open, want ended", g)
		}
		cancel()
	}
	ctx, cancel := workflow.DrainContext(context.Background(), time.Hour)
	if dl, ok := ctx.Deadline(); !ok || time.Until(dl) > time.Hour || ctx.Err() != nil {
		t.Errorf("grace 1h: deadline %v ok=%v err=%v", dl, ok, ctx.Err())
	}
	cancel()
	parent, pcancel := context.WithTimeout(context.Background(), time.Minute)
	defer pcancel()
	ctx, cancel = workflow.DrainContext(parent, time.Hour)
	defer cancel()
	pdl, _ := parent.Deadline()
	if dl, _ := ctx.Deadline(); dl.After(pdl) {
		t.Errorf("child deadline %v is after the parent's %v", dl, pdl)
	}
}

func TestDrainTimeoutAndHooks(t *testing.T) {
	plain := &fakeDrainer{}
	if err := workflow.DrainTimeout(context.Background(), plain, time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, ok := plain.got.Deadline(); !ok {
		t.Fatal("Drain ctx has no deadline")
	}
	rep, detailed, err := workflow.DrainTimeoutReport(context.Background(), plain, time.Hour)
	if err != nil || detailed || !rep.Clean() {
		t.Fatalf("plain drainer: %+v detailed=%v err=%v", rep, detailed, err)
	}
	rf := &reportingFake{fakeDrainer{report: workflow.DrainReport{FinishedSteps: 2}}}
	rep, detailed, err = workflow.DrainTimeoutReport(context.Background(), rf, time.Hour)
	if err != nil || !detailed || rep.FinishedSteps != 2 {
		t.Fatalf("reporting drainer: %+v detailed=%v err=%v", rep, detailed, err)
	}

	var seen workflow.DrainReport
	var seenDetailed bool
	hook := workflow.ShutdownHookReport(rf, time.Hour, func(r workflow.DrainReport, d bool) { seen, seenDetailed = r, d })
	if err := hook(context.Background()); err != nil || seen.FinishedSteps != 2 || !seenDetailed {
		t.Fatalf("hook: err=%v seen=%+v detailed=%v", err, seen, seenDetailed)
	}
	// The phase's own ctx ending first wins over the grace.
	dead, cancel := context.WithCancel(context.Background())
	cancel()
	if err := workflow.ShutdownHook(rf, time.Hour)(dead); !errors.Is(err, context.Canceled) {
		t.Fatalf("hook with ended phase ctx: %v, want Canceled", err)
	}
}

func TestDrainTracker(t *testing.T) {
	var tr workflow.DrainTracker
	endBefore := tr.Begin(workflow.DrainCut{RunID: "a", Step: "s", Attempt: 1})
	tr.RunFinished("early") // before StartDrain: not counted
	tr.StartDrain()
	endBefore()
	endBefore() // idempotent
	tr.RunFinished("a")
	endCut2 := tr.Begin(workflow.DrainCut{RunID: "z", Step: "s", Attempt: 2})
	tr.Begin(workflow.DrainCut{RunID: "b", Step: "t", Attempt: 1})
	tr.Freeze(workflow.DrainRequeued)
	tr.RunFinished("late") // after Freeze, before Seal: still counted
	tr.RunFinished("z")    // has a cut attempt: never counted
	rep := tr.Seal()
	if rep.FinishedSteps != 1 || rep.FinishedRuns != 2 || len(rep.Cut) != 2 || rep.Cut[0].RunID != "b" || rep.Cut[1].RunID != "z" {
		t.Fatalf("report = %+v", rep)
	}
	for _, c := range rep.Cut {
		if c.Disposition != workflow.DrainRequeued {
			t.Fatalf("disposition %q", c.Disposition)
		}
	}
	endCut2() // after Freeze: not counted as finished
	tr.RunFinished("after")
	if again := tr.Freeze(workflow.DrainCancelled); again.FinishedSteps != 1 || again.FinishedRuns != 2 || len(again.Cut) != 2 || again.Cut[0].Disposition != workflow.DrainRequeued {
		t.Fatalf("second Freeze changed the report: %+v", again)
	}
}
