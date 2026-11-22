package hatchet_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/hatchet"
)

// TestLongWaitLive proves on the engine that a wait or sleep step lasts as
// long as its WaitSpec.Timeout or its Sleep, not its execution timeout: a
// durable wait counts against that timeout on the engine, so the driver
// declares a wait step's as at least its Timeout plus a margin, and a
// sleep step's as at least its Sleep plus a margin. Config.StepTimeout is
// 3 seconds here, standing in for the default's day, and one workflow has
// a 2-second Workflow.Timeout meant for its run step; every wait and sleep
// outlasts both. A Deadline wait gets the time left to its Deadline, and a
// year-long Timeout is taken by the engine.
func TestLongWaitLive(t *testing.T) {
	skipWithoutEngine(t)
	prefix := "wflongwait_" + uuid.NewString()[:8]
	echo := func(_ context.Context, in []byte) ([]byte, error) { return in, nil }
	waiter := workflow.Workflow{Name: prefix + "_wait", Retry: *workflow.RetriesNone(), Steps: []workflow.Step{
		{Name: "approve", Wait: &workflow.WaitSpec{Event: "go", OwnRun: true, Timeout: 30 * time.Second}},
		{Name: "echo", After: []string{"approve"}, Run: echo},
	}}
	short := workflow.Workflow{Name: prefix + "_short", Retry: *workflow.RetriesNone(), Timeout: 2 * time.Second, Steps: []workflow.Step{
		{Name: "prepare", Run: echo},
		{Name: "approve", After: []string{"prepare"}, Wait: &workflow.WaitSpec{Event: "go", OwnRun: true, Timeout: 30 * time.Second}},
		{Name: "echo", After: []string{"approve"}, Run: echo},
	}}
	deadline := workflow.Workflow{Name: prefix + "_deadline", Retry: *workflow.RetriesNone(), Steps: []workflow.Step{
		{Name: "approve", Wait: &workflow.WaitSpec{Event: "go", OwnRun: true, Deadline: time.Now().Add(time.Minute)}},
		{Name: "echo", After: []string{"approve"}, Run: echo},
	}}
	// A year: the engine takes an execution timeout that long.
	year := workflow.Workflow{Name: prefix + "_year", Retry: *workflow.RetriesNone(), Steps: []workflow.Step{
		{Name: "approve", Wait: &workflow.WaitSpec{Event: "go", OwnRun: true, Timeout: 365 * 24 * time.Hour}},
		{Name: "echo", After: []string{"approve"}, Run: echo},
	}}
	expires := workflow.Workflow{Name: prefix + "_expires", Retry: *workflow.RetriesNone(), Steps: []workflow.Step{
		{Name: "approve", Wait: &workflow.WaitSpec{Event: "never", OwnRun: true, Timeout: 9 * time.Second}},
		{Name: "echo", After: []string{"approve"}, Run: echo},
	}}
	sleeper := workflow.Workflow{Name: prefix + "_sleep", Retry: *workflow.RetriesNone(), Steps: []workflow.Step{
		{Name: "nap", Sleep: 9 * time.Second},
		{Name: "echo", After: []string{"nap"}, Run: echo},
	}}
	c, _ := startLive(t, hatchet.Config{WorkerName: prefix, StepTimeout: 3 * time.Second}, waiter, short, deadline, year, expires, sleeper)
	ctx := context.Background()
	refs := map[string]workflow.RunRef{}
	for _, wf := range []workflow.Workflow{waiter, short, deadline, year, expires, sleeper} {
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, Payload: []byte(`"in"`)})
		if err != nil {
			t.Fatal(err)
		}
		refs[wf.Name] = ref
	}
	start := time.Now()
	signalled := []string{waiter.Name, short.Name, deadline.Name, year.Name}
	for _, name := range signalled {
		waitStatus(t, c, refs[name], workflow.StatusWaiting)
	}
	// Past both execution timeouts the waits would have had (3s, and 2s
	// on short), with the engine's margin and a reclaim on top.
	time.Sleep(time.Until(start.Add(12 * time.Second)))
	for _, name := range signalled {
		if st, err := c.Status(ctx, refs[name]); err != nil || st != workflow.StatusWaiting {
			t.Errorf("%s 12s into a longer wait: %s %v, want waiting", name, st, err)
			continue
		}
		if err := workflow.SignalRun(ctx, c, refs[name], "go", []byte(`"approved"`)); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range signalled {
		if st := finalStatus(t, c, refs[name]); st != workflow.StatusSucceeded {
			t.Errorf("%s: %s, want succeeded", name, st)
			continue
		}
		if out, err := c.Result(ctx, refs[name]); err != nil || string(out) != `"approved"` {
			t.Errorf("%s result %q %v", name, out, err)
		}
	}
	if st := finalStatus(t, c, refs[sleeper.Name]); st != workflow.StatusSucceeded {
		t.Errorf("%s: %s, want succeeded", sleeper.Name, st)
	} else if took := time.Since(start); took < 9*time.Second {
		t.Errorf("the 9s sleep ended after %s", took)
	}
	// A wait that outlasts its execution timeout and then reaches its own
	// Timeout fails with the wait's error, not the engine's.
	if st := finalStatus(t, c, refs[expires.Name]); st != workflow.StatusFailed {
		t.Errorf("%s: %s, want failed", expires.Name, st)
	}
	page, err := c.Runs(ctx, workflow.RunQuery{Ref: refs[expires.Name]})
	if err != nil || len(page.Runs) != 1 {
		t.Fatalf("Runs(ref) = %+v, %v", page, err)
	}
	if msg := page.Runs[0].Error; !strings.HasPrefix(strings.TrimPrefix(msg, `"`), workflow.ErrWaitTimeout.Error()) {
		t.Errorf("RunInfo.Error = %q, want it to start with %q", msg, workflow.ErrWaitTimeout)
	}
}

// finalStatus polls ref until it ends and returns how; it fails the test
// when the run has not ended within liveWait.
func finalStatus(t *testing.T, c workflow.Client, ref workflow.RunRef) workflow.Status {
	t.Helper()
	deadline := time.Now().Add(liveWait)
	for time.Now().Before(deadline) {
		st, err := c.Status(context.Background(), ref)
		if err == nil && st.IsTerminal() {
			return st
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("run %+v did not end within %s", ref, liveWait)
	return ""
}
