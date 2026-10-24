package hatchet

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/ubgo/workflow"
)

// TestDrainWithReportEngineOwned: with no engine (a client started offline),
// a handler still running at the grace's end is listed as a cut with the
// engine-owned disposition and its StepInfo identity; one that returned
// during the drain is counted as a finished step. FinishedRuns stays 0.
func TestDrainWithReportEngineOwned(t *testing.T) {
	c := New(Config{})
	startOffline(t, c)
	info := workflow.StepInfo{Workflow: "w", Step: "s", RunID: "run-1", Attempt: 2}
	entered, release := make(chan struct{}), make(chan struct{})
	stuck := c.wrap(func(context.Context, []byte) ([]byte, error) { close(entered); <-release; return nil, nil })
	go func() { _, _ = stuck(workflow.WithStepInfo(context.Background(), info), nil) }()
	<-entered
	defer close(release)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	report, err := c.DrainWithReport(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	want := workflow.DrainCut{RunID: "run-1", Workflow: "w", Step: "s", Attempt: 2, Disposition: workflow.DrainEngineOwned}
	if len(report.Cut) != 1 || report.Cut[0] != want || report.FinishedRuns != 0 {
		t.Fatalf("report = %+v, want one cut %+v", report, want)
	}
}

// TestDrainIdleWithEndedContextIsClean: nothing running and a ctx that has
// already ended (a grace of zero) is a clean drain that returns nil, not the
// ctx error; both ready cases of the wait must not be picked at random.
func TestDrainIdleWithEndedContextIsClean(t *testing.T) {
	for range 50 { // the old select chose at random, so one pass proves little
		c := New(Config{})
		startOffline(t, c)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		report, err := c.DrainWithReport(ctx)
		if err != nil || !report.Clean() || report.FinishedSteps != 0 || report.FinishedRuns != 0 {
			t.Fatalf("idle drain = %+v, %v; want an empty clean report and nil", report, err)
		}
	}
}

// TestDrainAfterDoneReturnsTheFirstReport: a second call, after the first
// drain cut a handler that never returns, must not wait on that handler
// again: it returns the first drain's report and error at once.
func TestDrainAfterDoneReturnsTheFirstReport(t *testing.T) {
	c := New(Config{})
	startOffline(t, c)
	info := workflow.StepInfo{Workflow: "w", Step: "s", RunID: "run-1", Attempt: 1}
	entered, release := make(chan struct{}), make(chan struct{})
	stuck := c.wrap(func(context.Context, []byte) ([]byte, error) { close(entered); <-release; return nil, nil })
	go func() { _, _ = stuck(workflow.WithStepInfo(context.Background(), info), nil) }()
	<-entered
	defer close(release)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	first, firstErr := c.DrainWithReport(ctx)

	got := make(chan struct{})
	go func() {
		defer close(got)
		again, err := c.DrainWithReport(context.Background())
		if !reflect.DeepEqual(again, first) || !errors.Is(err, firstErr) {
			t.Errorf("second drain = %+v, %v; want the first's %+v, %v", again, err, first, firstErr)
		}
	}()
	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("a Drain after the first one finished did not return")
	}
}
