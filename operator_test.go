package workflow_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ubgo/workflow"
)

func TestOverlapBlocks(t *testing.T) {
	for _, tc := range []struct {
		p    workflow.OverlapPolicy
		prev workflow.Status
		want bool
	}{
		{workflow.OverlapSkip, workflow.StatusRunning, true},
		{workflow.OverlapSkip, workflow.StatusQueued, true},
		{workflow.OverlapSkip, workflow.StatusRetrying, true},
		{workflow.OverlapSkip, workflow.StatusWaiting, true},
		{workflow.OverlapSkip, workflow.StatusScheduled, true},
		{workflow.OverlapSkip, workflow.StatusSucceeded, false},
		{workflow.OverlapSkip, workflow.StatusFailed, false},
		{workflow.OverlapSkip, workflow.StatusCancelled, false},
		{workflow.OverlapSkip, workflow.StatusDiscarded, false},
		{workflow.OverlapSkip, "", false}, // never fired, or forgotten
		{workflow.OverlapAllow, workflow.StatusRunning, false},
		{"", workflow.StatusRunning, false},
	} {
		if got := tc.p.Blocks(tc.prev); got != tc.want {
			t.Errorf("%q.Blocks(%q) = %v, want %v", tc.p, tc.prev, got, tc.want)
		}
	}
}

func TestOverlapValidationAndCaps(t *testing.T) {
	wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "s", Run: noop}}, Cron: []string{"* * * * *"}, CronOverlap: workflow.OverlapSkip}
	if err := workflow.Validate(wf); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if !workflow.Requires(wf).Has(workflow.CapOverlapSkip) {
		t.Errorf("Requires = %s, want OverlapSkip", workflow.Requires(wf))
	}
	bad := wf
	bad.CronOverlap = "sometimes"
	if err := workflow.Validate(bad); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("unknown CronOverlap: %v, want ErrInvalid", err)
	}
	noCron := wf
	noCron.Cron = nil
	if err := workflow.Validate(noCron); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("CronOverlap without Cron: %v, want ErrInvalid", err)
	}
	s := workflow.Schedule{Workflow: "w", Cron: "* * * * *", Location: time.UTC, Overlap: workflow.OverlapSkip}
	if err := workflow.ValidateSchedule(s); err != nil {
		t.Fatalf("ValidateSchedule: %v", err)
	}
	err := workflow.CheckScheduleCaps(workflow.CapCron, s)
	var ue *workflow.UnsupportedError
	if !errors.As(err, &ue) || ue.Field != "Schedule.Overlap" || !ue.Missing.Has(workflow.CapOverlapSkip) {
		t.Errorf("CheckScheduleCaps without OverlapSkip: %v, want Schedule.Overlap missing OverlapSkip", err)
	}
	s.Overlap = "x"
	if err := workflow.ValidateSchedule(s); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("unknown Overlap: %v, want ErrInvalid", err)
	}
}

func TestOverlapRefusalAndTriggeredLabels(t *testing.T) {
	err := workflow.OverlapRefusal("sch", workflow.RunRef{RunID: "r1"}, workflow.StatusRunning)
	if !errors.Is(err, workflow.ErrOverlap) || !strings.Contains(err.Error(), "r1") {
		t.Errorf("OverlapRefusal = %v, want ErrOverlap naming the run", err)
	}
	in := map[string]string{"a": "b"}
	l := workflow.TriggeredLabels(in, "sch", "nightly")
	if l["a"] != "b" || l[workflow.LabelSchedule] != "sch" || l[workflow.LabelScheduleName] != "nightly" || l[workflow.LabelTrigger] != workflow.LabelTriggerManual {
		t.Errorf("TriggeredLabels = %v", l)
	}
	if _, ok := in[workflow.LabelTrigger]; ok {
		t.Error("TriggeredLabels modified its input")
	}
}

func TestNewCapabilityNames(t *testing.T) {
	got := (workflow.CapOverlapSkip | workflow.CapScheduleTrigger | workflow.CapScheduleHistory | workflow.CapEnqueueOnly |
		workflow.CapErrorHistory | workflow.CapShutdownRequeue | workflow.CapForceRun).String()
	want := "OverlapSkip|ScheduleTrigger|ScheduleHistory|EnqueueOnly|ErrorHistory|ShutdownRequeue|ForceRun"
	if got != want {
		t.Errorf("String = %q, want %q", got, want)
	}
}

type stackErr struct{ stack string }

func (e stackErr) Error() string      { return "boom" }
func (e stackErr) StackTrace() string { return e.stack }

func TestAppendAttemptError(t *testing.T) {
	var h []workflow.AttemptError
	for i := 1; i <= 5; i++ {
		h = workflow.AppendAttemptError(h, workflow.AttemptError{Attempt: i}, 3)
	}
	if len(h) != 3 || h[0].Attempt != 3 || h[2].Attempt != 5 {
		t.Errorf("history = %+v, want attempts 3..5 (newest kept, oldest first)", h)
	}
	if got := workflow.AppendAttemptError(h, workflow.AttemptError{Attempt: 6}, 0); got != nil {
		t.Errorf("max 0 = %+v, want nil (history off)", got)
	}
	// Trimming must not write into a slice a caller still holds.
	held := h[:3:3]
	_ = workflow.AppendAttemptError(h, workflow.AttemptError{Attempt: 9}, 3)
	if held[0].Attempt != 3 {
		t.Errorf("a held history changed: %+v", held)
	}
}

func TestStackOfAndNewAttemptError(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	err := errors.Join(errors.New("ctx"), stackErr{stack: "main.go:1"})
	ae := workflow.NewAttemptError("s", 2, err, at)
	if ae.Step != "s" || ae.Attempt != 2 || ae.Stack != "main.go:1" || !ae.At.Equal(at) || !strings.Contains(ae.Error, "boom") {
		t.Errorf("NewAttemptError = %+v", ae)
	}
	if got := workflow.StackOf(errors.New("plain")); got != "" {
		t.Errorf("StackOf(plain) = %q, want empty", got)
	}
	long := workflow.StackOf(stackErr{stack: strings.Repeat("x", workflow.MaxErrorStack+10)})
	if len(long) != workflow.MaxErrorStack {
		t.Errorf("long stack = %d bytes, want cut to %d", len(long), workflow.MaxErrorStack)
	}
}

func TestRunInfoCloneCopiesErrors(t *testing.T) {
	info := workflow.RunInfo{Errors: []workflow.AttemptError{{Attempt: 1}}}
	c := info.Clone()
	c.Errors[0].Attempt = 9
	if info.Errors[0].Attempt != 1 {
		t.Error("Clone shares Errors")
	}
}

func TestForcedFailureAndStates(t *testing.T) {
	err := workflow.ForcedFailure("bad data")
	if !errors.Is(err, workflow.ErrForced) || !strings.HasSuffix(err.Error(), "bad data") {
		t.Errorf("ForcedFailure = %v", err)
	}
	if got := workflow.ForcedFailure("").Error(); got != workflow.ErrForced.Error() {
		t.Errorf("ForcedFailure(\"\") = %q", got)
	}
	for _, tc := range []struct {
		st             workflow.Status
		complete, fail bool
	}{
		{workflow.StatusQueued, true, true},
		{workflow.StatusRunning, true, true},
		{workflow.StatusScheduled, true, true},
		{workflow.StatusFailed, true, false},
		{workflow.StatusSucceeded, false, false},
		{workflow.StatusCancelled, false, false},
		{workflow.StatusDiscarded, false, false},
	} {
		if got := workflow.CanForceComplete(tc.st); got != tc.complete {
			t.Errorf("CanForceComplete(%q) = %v, want %v", tc.st, got, tc.complete)
		}
		if got := workflow.CanForceFail(tc.st); got != tc.fail {
			t.Errorf("CanForceFail(%q) = %v, want %v", tc.st, got, tc.fail)
		}
	}
}

func TestFailureOf(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ev := workflow.RunEvent{Ref: workflow.RunRef{RunID: "r"}, Workflow: "w", Status: workflow.StatusFailed, Err: errors.New("x"), Step: "s", Labels: map[string]string{"k": "v"}}
	f, ok := workflow.FailureOf(ev, false, at)
	if !ok || !f.Final || f.Step != "s" || f.Workflow != "w" || f.Ref.RunID != "r" || !f.At.Equal(at) || f.Labels["k"] != "v" {
		t.Errorf("FailureOf(failed) = %+v, %v", f, ok)
	}
	ev.Status = workflow.StatusDiscarded
	if _, ok := workflow.FailureOf(ev, false, at); ok {
		t.Error("a discarded run counted without the opt-in")
	}
	if _, ok := workflow.FailureOf(ev, true, at); !ok {
		t.Error("a discarded run did not count with the opt-in")
	}
	for _, st := range []workflow.Status{workflow.StatusSucceeded, workflow.StatusCancelled} {
		ev.Status = st
		if _, ok := workflow.FailureOf(ev, true, at); ok {
			t.Errorf("%q counted as a failure", st)
		}
	}
	var got workflow.Failure
	workflow.ReporterFunc(func(_ context.Context, f workflow.Failure) { got = f }).Report(context.Background(), workflow.Failure{Workflow: "z"})
	if got.Workflow != "z" {
		t.Error("ReporterFunc did not call through")
	}
}

func TestForwardTriggerAndForce(t *testing.T) {
	f := workflow.Forward{Inner: bareClient{}}
	ctx := context.Background()
	if _, err := f.TriggerSchedule(ctx, "x"); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("TriggerSchedule on a bare client: %v, want ErrUnsupported", err)
	}
	if err := f.ForceComplete(ctx, workflow.RunRef{}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("ForceComplete on a bare client: %v, want ErrUnsupported", err)
	}
	if err := f.ForceFail(ctx, workflow.RunRef{}, ""); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("ForceFail on a bare client: %v, want ErrUnsupported", err)
	}
}
