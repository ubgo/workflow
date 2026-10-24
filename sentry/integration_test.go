package sentry_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
	"github.com/ubgo/workflow/sentry"
)

// TestReportThroughPlugin wires the Reporter as an app would, through
// middleware.Report on the memory driver: a run that fails after its
// retries is one error event naming the workflow and the step, with no
// label sent; a run that succeeds sends nothing.
func TestReportThroughPlugin(t *testing.T) {
	hub, tr := newHub(t)
	c, err := workflow.Use(memory.New(), middleware.Report(sentry.New(hub), middleware.ReportConfig{}))
	if err != nil {
		t.Fatal(err)
	}
	fail := workflow.Workflow{Name: "charge", Steps: []workflow.Step{{
		Name:  "card",
		Retry: &workflow.RetryPolicy{Max: 1},
		Run:   func(context.Context, []byte) ([]byte, error) { return nil, errors.New("card declined") },
	}}}
	ok := workflow.Workflow{Name: "noop", Steps: []workflow.Step{{Name: "a", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}}
	for _, wf := range []workflow.Workflow{fail, ok} {
		if err := c.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	okRef, err := c.Enqueue(ctx, workflow.Job{Workflow: "noop"})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "charge", Labels: map[string]string{"tenant": "t1"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range []workflow.RunRef{okRef, ref} {
		deadline := time.Now().Add(10 * time.Second)
		for {
			st, err := c.Status(ctx, r)
			if err == nil && st.IsTerminal() {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("run %v did not end: %s %v", r, st, err)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(tr.sent()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	ev := tr.sent()
	if len(ev) != 1 {
		t.Fatalf("events: %d, want 1", len(ev))
	}
	e := ev[0]
	if e.Tags[sentry.TagWorkflow] != "charge" || e.Tags[sentry.TagStep] != "card" || e.Tags["workflow.label.tenant"] != "" {
		t.Fatalf("tags %v", e.Tags)
	}
	if e.Contexts[sentry.ContextName]["run_id"] != string(ref.RunID) {
		t.Fatalf("run_id %v, want %s", e.Contexts[sentry.ContextName]["run_id"], ref.RunID)
	}
}
