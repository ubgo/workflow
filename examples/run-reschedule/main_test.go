package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/examples/internal/capture"
	"github.com/ubgo/workflow/memory"
)

// TestRun runs the program and checks its whole output: delayed runs due in the next hour, one moved by Reschedule, a finished run refused, and one step withdrawn with and without a fallback.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `due in the next hour:
  r-30m due at +30m0s, scheduled
rescheduled r-2h to +45m, same ref true
due in the next hour:
  r-2h due at +45m0s, scheduled
  r-30m due at +30m0s, scheduled
after an hour: r-2h succeeded, r-30m succeeded
reschedule a finished run: ErrWrongState true
cancel one step:
  store got "yesterday's prices"
  update-prices succeeded
  sync-feed failed, ErrStepCancelled true
  cancel it again: ErrWrongState true
`
	if got := out; got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}

// plainClient hides every optional interface of the client it embeds:
// only the workflow.Client methods are promoted, so moveRun takes its
// cancel-and-enqueue fallback.
type plainClient struct{ workflow.Client }

// movedTo is the fallback test's new start: an hour after the run's first
// one, so the move is visible.
const movedTo = 2 * time.Hour

// TestMoveRunFallback drives moveRun on a client without RunRescheduler:
// the run is cancelled and enqueued again with a new ref and the new
// start, and a run that is no longer scheduled is refused with
// ErrWrongState.
func TestMoveRunFallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := memory.New()
	if err := c.Register(workflow.Workflow{Name: "remind", Steps: []workflow.Step{{Name: "send", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}}); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Stop()
	job := workflow.Job{Workflow: "remind", RunAt: time.Now().Add(time.Hour)}
	ref, err := c.Enqueue(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().Add(movedTo)
	moved, err := moveRun(ctx, plainClient{c}, ref, job, at)
	if err != nil {
		t.Fatal(err)
	}
	if moved == ref {
		t.Fatal("the fallback must give the run a new ref")
	}
	if st, _ := c.Status(ctx, ref); st != workflow.StatusCancelled {
		t.Fatalf("old run is %s, want cancelled", st)
	}
	if _, err := moveRun(ctx, plainClient{c}, ref, job, at); !errors.Is(err, workflow.ErrWrongState) {
		t.Fatalf("moving a cancelled run: %v, want ErrWrongState", err)
	}
}
