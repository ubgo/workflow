package memory_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

func slowWorkflow(started chan<- struct{}, release <-chan struct{}) workflow.Workflow {
	return workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "slow", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		started <- struct{}{}
		select {
		case <-release:
			return in, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}}}}
}

// TestDrainWithReportCut: a step still running at the grace's end is
// reported cancelled (nothing in memory outlives the client), with the
// ctx error; the second call and one after Done return the same report.
func TestDrainWithReportCut(t *testing.T) {
	c := memory.New()
	started := make(chan struct{}, 1)
	if err := c.Register(slowWorkflow(started, nil)); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	ref, err := c.Enqueue(context.Background(), workflow.Job{Workflow: "w"})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	report, detailed, err := workflow.DrainTimeoutReport(context.Background(), c, 30*time.Millisecond)
	if !detailed || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("detailed=%v err=%v, want a detailed report and DeadlineExceeded", detailed, err)
	}
	want := workflow.DrainCut{RunID: ref.RunID, Workflow: "w", Step: "slow", Attempt: 1, Disposition: workflow.DrainCancelled}
	if len(report.Cut) != 1 || report.Cut[0] != want {
		t.Fatalf("report = %+v, want one cut %+v", report, want)
	}
	<-c.Done()
	again, err := c.DrainWithReport(context.Background())
	if len(again.Cut) != 1 || again.Cut[0] != want {
		t.Fatalf("report after Done = %+v err=%v, want the first report", again, err)
	}
	st, qerr := c.Status(context.Background(), ref)
	if qerr != nil || st != workflow.StatusCancelled {
		t.Fatalf("cut run status = %v err=%v, want cancelled", st, qerr)
	}
}

// TestDrainWithReportClean: a step that ends inside the grace is counted
// as finished and nothing is cut.
func TestDrainWithReportClean(t *testing.T) {
	c := memory.New()
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	if err := c.Register(slowWorkflow(started, release)); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Enqueue(context.Background(), workflow.Job{Workflow: "w"}); err != nil {
		t.Fatal(err)
	}
	<-started
	go func() { time.Sleep(20 * time.Millisecond); close(release) }()
	report, err := c.DrainWithReport(context.Background())
	if err != nil || !report.Clean() || report.FinishedSteps != 1 || report.FinishedRuns != 1 {
		t.Fatalf("report = %+v err=%v, want clean, 1 step, 1 run", report, err)
	}
}

// TestDrainWithReportBeforeStart: ErrNotStarted, as Drain.
func TestDrainWithReportBeforeStart(t *testing.T) {
	if _, err := memory.New().DrainWithReport(context.Background()); !errors.Is(err, workflow.ErrNotStarted) {
		t.Fatalf("err = %v", err)
	}
}
