package workflow

import (
	"context"
	"errors"
	"testing"
)

// allStatuses lists every Status constant; the walk below fails when a
// constant is added to status.go without being added here and to the table.
var allStatuses = []Status{
	StatusScheduled, StatusQueued, StatusRunning, StatusWaiting, StatusRetrying,
	StatusSucceeded, StatusFailed, StatusCancelled, StatusDiscarded,
}

func TestTransitionTableWalk(t *testing.T) {
	for _, s := range allStatuses {
		if !s.Valid() {
			t.Errorf("%s is not Valid", s)
		}
	}
	for _, from := range allStatuses {
		for _, to := range allStatuses {
			got := from.CanTransitionTo(to)
			want := false
			for _, tr := range Transitions() {
				if tr.From == from && tr.To == to {
					want = true
				}
			}
			if got != want {
				t.Errorf("%s -> %s: CanTransitionTo %v, table says %v", from, to, got, want)
			}
		}
	}
	for _, tr := range Transitions() {
		if !tr.From.Valid() || !tr.To.Valid() || tr.From == tr.To || tr.Trigger == "" {
			t.Errorf("malformed transition %+v", tr)
		}
	}
}

func TestTransitionTableShape(t *testing.T) {
	for _, s := range allStatuses {
		out := s.TransitionsFrom()
		switch {
		case s == StatusFailed:
			if len(out) != 1 || out[0].To != StatusQueued || out[0].Needs != CapRetryRun {
				t.Errorf("failed must only re-open to queued under CapRetryRun, got %+v", out)
			}
		case s.IsTerminal():
			if len(out) != 0 {
				t.Errorf("terminal %s has outgoing moves %+v", s, out)
			}
		default:
			if len(out) == 0 {
				t.Errorf("%s is a dead end", s)
			}
		}
	}
	// The two ways into discarded are separately gated, and a run reaches
	// a terminal status from every non-terminal one.
	var plain, exhaust bool
	for _, tr := range StatusRunning.TransitionsFrom() {
		if tr.To == StatusDiscarded && tr.Needs.Has(CapDiscard|CapRunStates) {
			plain = true
		}
		if tr.To == StatusDiscarded && tr.Needs.Has(CapDiscardExhausted|CapRunStates) {
			exhaust = true
		}
	}
	if !plain || !exhaust {
		t.Errorf("running -> discarded gates: Discard %v, DiscardOnExhaust %v", plain, exhaust)
	}
	for _, from := range []Status{StatusScheduled, StatusQueued, StatusRunning, StatusWaiting, StatusRetrying} {
		for _, to := range []Status{StatusCancelled, StatusFailed, StatusSucceeded} {
			if !from.CanTransitionTo(to) {
				t.Errorf("%s -> %s missing", from, to)
			}
		}
	}
	if StatusRetrying.CanTransitionTo(StatusScheduled) || StatusQueued.CanTransitionTo(StatusScheduled) {
		t.Error("nothing returns to scheduled")
	}
	if StatusRetrying.CanTransitionTo(StatusDiscarded) {
		t.Error("a run in backoff is discarded only after its next attempt fails (via running)")
	}
}

func TestEffectiveRetriesForPrecedence(t *testing.T) {
	job := &RetryPolicy{Max: 1}
	step := &RetryPolicy{Max: 2}
	wf := Workflow{Retry: RetryPolicy{Max: 3}}
	cases := []struct {
		name string
		step *RetryPolicy
		job  *RetryPolicy
		wf   Workflow
		want int
	}{
		{"job wins over step and workflow", step, job, wf, 1},
		{"step wins over workflow", step, nil, wf, 2},
		{"workflow when step and job are unset", nil, nil, wf, 3},
		{"default when nothing is set", nil, nil, Workflow{}, DefaultRetryMax},
		{"RetriesNone on a step beats the workflow", RetriesNone(), nil, wf, 0},
		{"job beats RetriesNone", RetriesNone(), job, wf, 1},
	}
	for _, c := range cases {
		got := EffectiveRetriesFor(Step{Retry: c.step}, c.wf, Job{Retry: c.job}).Max
		if got != c.want {
			t.Errorf("%s: Max %d, want %d", c.name, got, c.want)
		}
		if c.job == nil {
			if got := EffectiveRetries(Step{Retry: c.step}, c.wf).Max; got != c.want {
				t.Errorf("%s: EffectiveRetries Max %d, want %d", c.name, got, c.want)
			}
		}
	}
	// A winning policy is whole: the step's zero DiscardOnExhaust does not
	// inherit the workflow's.
	wf.Retry.DiscardOnExhaust = true
	if EffectiveRetriesFor(Step{Retry: step}, wf, Job{}).DiscardOnExhaust {
		t.Error("policies must not merge field by field")
	}
}

func TestExhaustedError(t *testing.T) {
	boom := errors.New("boom")
	on := RetryPolicy{DiscardOnExhaust: true}
	derr := on.ExhaustedError(3, boom)
	if derr == nil || !IsDiscarded(derr) || !errors.Is(derr, boom) {
		t.Fatalf("want a discard wrapping boom, got %v", derr)
	}
	if got := EndStatus(CapRunStates|CapDiscard, derr); got != StatusDiscarded {
		t.Errorf("EndStatus = %s, want discarded", got)
	}
	if err := (RetryPolicy{}).ExhaustedError(3, boom); err != nil {
		t.Errorf("option off must not discard, got %v", err)
	}
	for _, err := range []error{nil, Discard(boom), ErrCancelled} {
		if got := on.ExhaustedError(1, err); got != nil {
			t.Errorf("ExhaustedError(%v) = %v, want nil", err, got)
		}
	}
	if !Requires(Workflow{Steps: []Step{{Name: "a", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }, Retry: &on}}}).Has(CapDiscardExhausted | CapRunStates) {
		t.Error("DiscardOnExhaust must require CapDiscardExhausted and CapRunStates")
	}
}
