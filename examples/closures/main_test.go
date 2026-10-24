package main

import (
	"context"
	"strings"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/workflowtest"
)

// want is the program's whole output, the one its README and guide show.
const want = `closure warm-price-cache: succeeded after 2 attempts, workflow "ubgo.workflow.closure", label "ubgo.workflow/closure"
Status: succeeded, step "closure"
closure nudge: cancelled before it started
closure without Fn: ErrInvalid true
sqlstore refuses: missing Closure, ErrUnsupported true
hatchet refuses: missing Closure, ErrUnsupported true
`

// TestRun runs the program and checks every line it prints.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}

// snippet:fake-closure
func TestWarmUpEnqueuesAClosure(t *testing.T) {
	ctx := context.Background()
	fake := workflowtest.NewFake()
	if err := fake.Start(ctx); err != nil {
		t.Fatal(err)
	}
	warmed := false
	warm := workflow.ClosureJob{Name: "warm-price-cache", Fn: func(context.Context) error { warmed = true; return nil }}
	if _, err := workflow.EnqueueClosure(ctx, fake, warm); err != nil {
		t.Fatal(err)
	}
	fake.AssertClosureEnqueued(t, "warm-price-cache")
	if warmed {
		t.Fatal("the Fake runs nothing until RunClosures")
	}
	if err := fake.RunClosures(ctx); err != nil {
		t.Fatal(err)
	}
	if !warmed {
		t.Error("RunClosures did not call the closure")
	}
}

// snippet:end
