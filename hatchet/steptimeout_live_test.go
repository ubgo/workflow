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

// TestStepTimeoutLive proves on the engine that the worker, not the
// engine, ends an attempt that runs past Step.Timeout: the run fails well
// before the engine's own deadline (Step.Timeout plus a margin), the
// error is the worker's workflow.TimeoutError (RunInfo.Error starts with
// ErrStepTimeout's text, not the engine's timeout message), the handler
// saw it as its context's cause, and the run-finished event's error
// matches ErrStepTimeout and ErrTimeout although the engine keeps only
// its text.
func TestStepTimeoutLive(t *testing.T) {
	skipWithoutEngine(t)
	const limit = 2 * time.Second
	name := "wfsteptimeout_" + uuid.NewString()[:8]
	causes := make(chan error, 4)
	wf := workflow.Workflow{Name: name, Retry: *workflow.RetriesNone(), Steps: []workflow.Step{{Name: "slow", Timeout: limit, Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		<-ctx.Done()
		causes <- context.Cause(ctx)
		return []byte(`"late"`), nil // ignores the cancellation: still a timeout
	}}}}
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
	start := time.Now()
	ref, err := c.Enqueue(context.Background(), workflow.Job{Workflow: name})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, ref, workflow.StatusFailed)
	// The engine's deadline is limit rounded up plus 5s; the worker's
	// fires at limit. Status polls every 200ms and the read side trails.
	if took := time.Since(start); took > limit+4*time.Second {
		t.Errorf("the run failed %s after enqueue, want about %s (the worker's timeout, not the engine's)", took, limit)
	}
	select {
	case cause := <-causes:
		if !errors.Is(cause, workflow.ErrStepTimeout) || !workflow.IsTimeout(cause) {
			t.Errorf("context.Cause in the handler = %v, want ErrStepTimeout", cause)
		}
	default:
		t.Error("the handler never saw its context end")
	}
	page, err := c.Runs(context.Background(), workflow.RunQuery{Ref: ref})
	if err != nil || len(page.Runs) != 1 {
		t.Fatalf("Runs(ref) = %+v, %v", page, err)
	}
	// The engine hands the message back as a JSON string, in quotes.
	if msg := page.Runs[0].Error; !strings.HasPrefix(strings.TrimPrefix(msg, `"`), workflow.ErrStepTimeout.Error()) {
		t.Errorf("RunInfo.Error = %q, want it to start with %q", msg, workflow.ErrStepTimeout)
	}
	ev := rec.wait(t, ref)
	if !errors.Is(ev.Err, workflow.ErrStepTimeout) || !workflow.IsTimeout(ev.Err) {
		t.Errorf("RunEvent.Err = %v, want it to match ErrStepTimeout and ErrTimeout", ev.Err)
	}
	var re *workflow.RunError
	if !errors.As(ev.Err, &re) || re.Step != "slow" {
		t.Errorf("RunEvent.Err = %#v, want a *workflow.RunError naming step slow", ev.Err)
	}
}
