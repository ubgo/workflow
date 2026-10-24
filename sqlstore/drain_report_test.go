package sqlstore_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ubgo/workflow"
)

// blockingWorkflow's step reports its attempt number on started, then
// holds until its ctx ends (a cut-off) or release closes (a clean finish).
func blockingWorkflow(name string, started chan<- int, release <-chan struct{}) workflow.Workflow {
	return workflow.Workflow{Name: name, Steps: []workflow.Step{{Name: "slow",
		Run: func(ctx context.Context, in []byte) ([]byte, error) {
			info, _ := workflow.StepInfoFrom(ctx)
			started <- info.Attempt
			select {
			case <-release:
				return in, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}}}}
}

// TestDrainReportCutOffIsRequeuedAndNotCharged: a drain whose grace ends
// while a step runs reports that attempt as requeued, returns the ctx
// error, and a second client then runs the same step as attempt 1 again,
// because the cut attempt was not charged. Draining twice and after Done
// returns the same report.
func TestDrainReportCutOffIsRequeuedAndNotCharged(t *testing.T) {
	e := engines(t)[0]
	db, prefix := e.open(t)
	a := newClient(t, e, db, prefix)
	startedA := make(chan int, 4)
	startWith(t, a, blockingWorkflow("w", startedA, nil))
	id, err := a.Enqueue(context.Background(), workflow.Job{Workflow: "w"})
	if err != nil {
		t.Fatal(err)
	}
	if got := <-startedA; got != 1 {
		t.Fatalf("first attempt number %d, want 1", got)
	}

	var _ workflow.ReportingDrainer = a
	report, err := a.DrainWithReport(drainCtx(t, 50*time.Millisecond))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("DrainWithReport err = %v, want DeadlineExceeded", err)
	}
	want := workflow.DrainCut{RunID: id.RunID, Workflow: "w", Step: "slow", Attempt: 1, Disposition: workflow.DrainRequeued}
	if len(report.Cut) != 1 || report.Cut[0] != want || report.CutRuns() != 1 || report.Clean() {
		t.Fatalf("report = %+v, want one cut %+v", report, want)
	}
	waitDone(t, a)

	again, err2 := a.DrainWithReport(context.Background())
	if err2 != nil && !errors.Is(err2, context.DeadlineExceeded) {
		t.Fatalf("second drain err = %v", err2)
	}
	if len(again.Cut) != 1 || again.Cut[0] != want {
		t.Fatalf("second report = %+v, want the first", again)
	}

	b := newClient(t, e, db, prefix)
	startedB := make(chan int, 4)
	release := make(chan struct{})
	close(release)
	startWith(t, b, blockingWorkflow("w", startedB, release))
	select {
	case got := <-startedB:
		if got != 1 {
			t.Fatalf("after the cut-off the step ran as attempt %d, want 1 (not charged)", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the cut-off step was not requeued for another client")
	}
}

// TestDrainReportCleanCountsFinished: when the step ends inside the grace,
// nothing is cut, Drain's error is nil, and the finished run and step are
// counted.
func TestDrainReportCleanCountsFinished(t *testing.T) {
	e := engines(t)[0]
	db, prefix := e.open(t)
	c := newClient(t, e, db, prefix)
	started := make(chan int, 1)
	release := make(chan struct{})
	startWith(t, c, blockingWorkflow("w", started, release))
	if _, err := c.Enqueue(context.Background(), workflow.Job{Workflow: "w"}); err != nil {
		t.Fatal(err)
	}
	<-started
	go func() { time.Sleep(30 * time.Millisecond); close(release) }()
	report, err := c.DrainWithReport(drainCtx(t, 10*time.Second))
	if err != nil {
		t.Fatalf("clean drain err = %v", err)
	}
	if !report.Clean() || report.FinishedSteps != 1 || report.FinishedRuns != 1 {
		t.Fatalf("report = %+v, want clean with 1 step and 1 run finished", report)
	}
}

// TestDrainReportBeforeStart: nothing to drain yet is ErrNotStarted.
func TestDrainReportBeforeStart(t *testing.T) {
	e := engines(t)[0]
	db, prefix := e.open(t)
	c := newClient(t, e, db, prefix)
	if _, err := c.DrainWithReport(context.Background()); !errors.Is(err, workflow.ErrNotStarted) {
		t.Fatalf("err = %v, want ErrNotStarted", err)
	}
}

func drainCtx(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}
