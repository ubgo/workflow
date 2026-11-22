package hatchet_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/hatchet"
)

// TestWaitTimeoutLive proves on the engine that a wait step that reaches
// its Timeout with no event fails the run with the error the other
// drivers report: RunInfo.Error starts with workflow.ErrWaitTimeout's text
// (inside the quotes the engine stores it in), and the run-finished
// event's error matches ErrWaitTimeout and ErrTimeout (not
// ErrStepTimeout) and is a *workflow.RunError naming the wait step,
// although the engine keeps only the text.
func TestWaitTimeoutLive(t *testing.T) {
	skipWithoutEngine(t)
	name := "wfwaittimeout_" + uuid.NewString()[:8]
	wf := workflow.Workflow{Name: name, Retry: *workflow.RetriesNone(), Steps: []workflow.Step{
		{Name: "approve", Wait: &workflow.WaitSpec{Event: "approved", OwnRun: true, Timeout: 2 * time.Second}},
		{Name: "after", After: []string{"approve"}, Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }},
	}}
	c := hatchet.New(hatchet.Config{WorkerName: name})
	if err := c.Register(wf); err != nil {
		t.Fatal(err)
	}
	rec := newRecorder()
	rec.c = c
	if err := c.OnRunFinished(rec.observe); err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	ref, err := c.Enqueue(context.Background(), workflow.Job{Workflow: name})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, ref, workflow.StatusFailed)
	page, err := c.Runs(context.Background(), workflow.RunQuery{Ref: ref})
	if err != nil || len(page.Runs) != 1 {
		t.Fatalf("Runs(ref) = %+v, %v", page, err)
	}
	// The engine hands the message back as a JSON string, in quotes.
	if msg := page.Runs[0].Error; !strings.HasPrefix(strings.TrimPrefix(msg, `"`), workflow.ErrWaitTimeout.Error()) {
		t.Errorf("RunInfo.Error = %q, want it to start with %q", msg, workflow.ErrWaitTimeout)
	}
	ev := rec.wait(t, ref)
	if !errors.Is(ev.Err, workflow.ErrWaitTimeout) || !workflow.IsTimeout(ev.Err) || errors.Is(ev.Err, workflow.ErrStepTimeout) {
		t.Errorf("RunEvent.Err = %v, want it to match ErrWaitTimeout and ErrTimeout only", ev.Err)
	}
	var re *workflow.RunError
	if !errors.As(ev.Err, &re) || re.Step != "approve" {
		t.Errorf("RunEvent.Err = %#v, want a *workflow.RunError naming step approve", ev.Err)
	}
}
