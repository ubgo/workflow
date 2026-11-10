package middleware_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/middleware"
)

// passThrough is the EnqueueFunc under test middleware: it counts calls
// and returns a fixed ref.
func passThrough(calls *int) workflow.EnqueueFunc {
	return func(context.Context, workflow.Job) (workflow.RunRef, error) {
		*calls++
		return workflow.RunRef{RunID: "r1"}, nil
	}
}

func TestRequireSource(t *testing.T) {
	ctx := context.Background()
	var calls int
	enq := middleware.RequireSource()(passThrough(&calls))

	if _, err := enq(ctx, workflow.Job{Workflow: "w"}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("no source: %v, want ErrInvalid", err)
	}
	if calls != 0 {
		t.Fatal("a refused job reached next")
	}
	if _, err := enq(ctx, workflow.WithSource(workflow.Job{Workflow: "w"}, "api", "")); err != nil {
		t.Fatalf("explicit source: %v", err)
	}
	// A derived source passes too.
	derived := workflow.Job{Workflow: "w", Labels: map[string]string{workflow.LabelParentRun: "p"}}
	if _, err := enq(ctx, derived); err != nil {
		t.Fatalf("derived source: %v", err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
}

func TestRequireSourceAllowList(t *testing.T) {
	ctx := context.Background()
	var calls int
	enq := middleware.RequireSource("api", "webhook")(passThrough(&calls))
	if _, err := enq(ctx, workflow.WithSource(workflow.Job{Workflow: "w"}, "webhook", "d")); err != nil {
		t.Fatalf("allowed: %v", err)
	}
	if _, err := enq(ctx, workflow.WithSource(workflow.Job{Workflow: "w"}, "webook", "d")); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("typo: %v, want ErrInvalid", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestRequireSourceBadAllowList(t *testing.T) {
	var calls int
	enq := middleware.RequireSource("Not Valid")(passThrough(&calls))
	_, err := enq(context.Background(), workflow.WithSource(workflow.Job{Workflow: "w"}, "api", ""))
	if !errors.Is(err, workflow.ErrInvalid) || calls != 0 {
		t.Fatalf("bad allow-list: err %v calls %d", err, calls)
	}
}
