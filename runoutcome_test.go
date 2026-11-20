package workflow_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ubgo/workflow"
)

func TestOutcomeValid(t *testing.T) {
	for _, o := range workflow.Outcomes() {
		if !o.Valid() {
			t.Errorf("%q not valid", o)
		}
	}
	for _, o := range []workflow.Outcome{"", "ok", "SUCCESS"} {
		if o.Valid() {
			t.Errorf("%q valid", o)
		}
	}
	a := workflow.Outcomes()
	a[0] = "x"
	if workflow.Outcomes()[0] == "x" {
		t.Fatal("Outcomes handed out its backing array")
	}
}

func TestDefaultOutcome(t *testing.T) {
	cases := map[workflow.Status]workflow.Outcome{
		workflow.StatusSucceeded: workflow.OutcomeSuccess,
		workflow.StatusFailed:    workflow.OutcomeError,
		workflow.StatusDiscarded: workflow.OutcomeError,
		workflow.StatusCancelled: "",
		workflow.StatusRunning:   "",
		workflow.StatusQueued:    "",
	}
	for st, want := range cases {
		if got := workflow.DefaultOutcome(st); got != want {
			t.Errorf("DefaultOutcome(%q) = %q, want %q", st, got, want)
		}
	}
}

func TestSetOutcome(t *testing.T) {
	ctx := context.Background()
	if err := workflow.SetOutcome(ctx, workflow.OutcomePartial, "n"); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("no setter: %v, want ErrUnsupported", err)
	}
	var got workflow.Outcome
	var note string
	ctx = workflow.WithOutcomeSetter(ctx, workflow.OutcomeFunc(func(_ context.Context, o workflow.Outcome, n string) error {
		got, note = o, n
		return nil
	}))
	if err := workflow.SetOutcome(ctx, workflow.OutcomePartial, "3 of 100 failed"); err != nil {
		t.Fatal(err)
	}
	if got != workflow.OutcomePartial || note != "3 of 100 failed" {
		t.Fatalf("got %q %q", got, note)
	}
	if err := workflow.SetOutcome(ctx, "nope", ""); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("bad outcome: %v, want ErrInvalid", err)
	}
	if err := workflow.SetOutcome(ctx, workflow.OutcomeSkipped, strings.Repeat("n", workflow.MaxOutcomeNote+1)); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("long note: %v, want ErrInvalid", err)
	}
	// The setter's own error is returned.
	boom := errors.New("store down")
	ctx = workflow.WithOutcomeSetter(ctx, workflow.OutcomeFunc(func(context.Context, workflow.Outcome, string) error { return boom }))
	if err := workflow.SetOutcome(ctx, workflow.OutcomeSuccess, ""); !errors.Is(err, boom) {
		t.Fatalf("setter error: %v", err)
	}
}
