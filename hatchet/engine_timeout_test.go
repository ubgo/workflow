package hatchet

import (
	"testing"
	"time"

	"github.com/ubgo/workflow"
)

// TestEngineTimeout pins the execution timeout a run step is declared
// with: the fallback when neither the step nor the workflow sets one, and
// otherwise the step's own timeout (or the workflow's) rounded up to whole
// seconds, which the SDK sends, plus engineTimeoutMargin, so the worker's
// timer always fires first.
func TestEngineTimeout(t *testing.T) {
	const fallback = 24 * time.Hour
	cases := []struct {
		step, wf time.Duration
		want     time.Duration
	}{
		{0, 0, fallback},
		{2 * time.Second, 0, 2*time.Second + engineTimeoutMargin},
		{1500 * time.Millisecond, 0, 2*time.Second + engineTimeoutMargin},
		{100 * time.Millisecond, 0, time.Second + engineTimeoutMargin},
		{0, 3 * time.Second, 3*time.Second + engineTimeoutMargin},
		{time.Second, time.Minute, time.Second + engineTimeoutMargin},
	}
	for _, tc := range cases {
		s := workflow.Step{Name: "a", Timeout: tc.step}
		wf := workflow.Workflow{Name: "w", Timeout: tc.wf}
		if got := engineTimeout(s, wf, fallback); got != tc.want {
			t.Errorf("engineTimeout(step %s, workflow %s) = %s, want %s", tc.step, tc.wf, got, tc.want)
		}
		if userTimeout(s, wf) > 0 && engineTimeout(s, wf, fallback) <= userTimeout(s, wf) {
			t.Errorf("engine timeout not above the worker's for step %s", tc.step)
		}
	}
}

// TestWaitTimeout pins the execution timeout a wait step is declared with.
// The engine counts a durable wait against it, so it is at least as long
// as the wait can last plus engineTimeoutMargin: its Timeout, or the time
// left to its Deadline at declaration, in whole seconds rounded up; the
// fallback for a wait without either. Step.Timeout and Workflow.Timeout
// raise it, never lower it.
func TestWaitTimeout(t *testing.T) {
	const fallback = 24 * time.Hour
	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name     string
		wait     workflow.WaitSpec
		step, wf time.Duration
		want     time.Duration
	}{
		{"forever", workflow.WaitSpec{}, 0, 0, fallback},
		{"forever, a shorter workflow timeout", workflow.WaitSpec{}, 0, 2 * time.Second, fallback},
		{"forever, a longer step timeout", workflow.WaitSpec{}, 48 * time.Hour, 0, 48 * time.Hour},
		{"timeout", workflow.WaitSpec{Timeout: 30 * time.Second}, 0, 0, 30*time.Second + engineTimeoutMargin},
		{"timeout past the fallback", workflow.WaitSpec{Timeout: 72 * time.Hour}, 0, 0, 72*time.Hour + engineTimeoutMargin},
		{"timeout, rounded up", workflow.WaitSpec{Timeout: 1500 * time.Millisecond}, 0, 0, 2*time.Second + engineTimeoutMargin},
		{"timeout, a shorter workflow timeout", workflow.WaitSpec{Timeout: 30 * time.Second}, 0, 2 * time.Second, 30*time.Second + engineTimeoutMargin},
		{"timeout, a shorter step timeout", workflow.WaitSpec{Timeout: 30 * time.Second}, 3 * time.Second, 0, 30*time.Second + engineTimeoutMargin},
		{"timeout, a longer step timeout", workflow.WaitSpec{Timeout: 30 * time.Second}, time.Hour, 0, time.Hour},
		{"timeout, a longer step timeout rounded up", workflow.WaitSpec{Timeout: time.Second}, 7500 * time.Millisecond, 0, 8 * time.Second},
		{"deadline", workflow.WaitSpec{Deadline: now.Add(90 * time.Second)}, 0, 0, 90*time.Second + engineTimeoutMargin},
		{"deadline, rounded up", workflow.WaitSpec{Deadline: now.Add(90*time.Second + time.Millisecond)}, 0, 0, 91*time.Second + engineTimeoutMargin},
		{"deadline passed", workflow.WaitSpec{Deadline: now.Add(-time.Hour)}, 0, 0, engineTimeoutMargin},
	}
	for _, tc := range cases {
		w := tc.wait
		s := workflow.Step{Name: "a", Wait: &w, Timeout: tc.step}
		wf := workflow.Workflow{Name: "w", Timeout: tc.wf}
		if got := waitTimeout(s, wf, fallback, now); got != tc.want {
			t.Errorf("%s: waitTimeout = %s, want %s", tc.name, got, tc.want)
		}
	}
}
