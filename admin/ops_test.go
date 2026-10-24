package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/middleware"
)

// TestHandler_ScheduleInspectAndTrigger: one schedule shows its overlap
// policy and last fire; trigger returns the new run; an overlap refusal
// is a 409 and an unknown schedule a 404.
func TestHandler_ScheduleInspectAndTrigger(t *testing.T) {
	f := seeded()
	fire := time.Date(2026, 10, 13, 2, 0, 0, 0, time.UTC)
	f.schedules[0].Overlap = workflow.OverlapSkip
	f.schedules[0].Last = workflow.ScheduleLast{Fire: fire, Run: workflow.RunRef{RunID: "r9"}, Status: workflow.StatusRunning}
	h := NewHandler(New(f), bearerAuth())

	rec := do(t, h, "GET", "/schedules/s1", "")
	var s scheduleJSON
	if err := json.Unmarshal(rec.Body.Bytes(), &s); err != nil || rec.Code != http.StatusOK {
		t.Fatalf("show = %d %s %v", rec.Code, rec.Body, err)
	}
	if s.Overlap != workflow.OverlapSkip || s.Last == nil || !s.Last.Fire.Equal(fire) || s.Last.Run.RunID != "r9" || s.Last.Status != workflow.StatusRunning || s.Next.IsZero() {
		t.Errorf("schedule = %+v last %+v", s, s.Last)
	}
	if rec := do(t, h, "GET", "/schedules/nope", ""); rec.Code != http.StatusNotFound {
		t.Errorf("unknown schedule = %d", rec.Code)
	}

	rec = do(t, h, "POST", "/schedules/s1/trigger", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"run_id":"t-s1"`) {
		t.Errorf("trigger = %d %s", rec.Code, rec.Body)
	}

	f.err = workflow.OverlapRefusal("s1", workflow.RunRef{RunID: "r9"}, workflow.StatusRunning)
	if rec := do(t, h, "POST", "/schedules/s1/trigger", ""); rec.Code != http.StatusConflict || errCode(t, rec) != CodeConflict {
		t.Errorf("overlap trigger = %d %s, want 409", rec.Code, rec.Body)
	}
}

// TestHandler_ScheduleDefaultsAllow: a schedule without a policy reads
// "allow" and has no last fire before it fired.
func TestHandler_ScheduleDefaultsAllow(t *testing.T) {
	rec := do(t, NewHandler(New(seeded()), bearerAuth()), "GET", "/schedules/s1", "")
	if !strings.Contains(rec.Body.String(), `"overlap":"allow"`) || strings.Contains(rec.Body.String(), `"last"`) {
		t.Errorf("show = %s", rec.Body)
	}
}

// TestHandler_Force: complete and fail reach the RunForcer with the
// reason; a wrong state is 409; a bad body or an over-long reason is
// refused before the driver is called.
func TestHandler_Force(t *testing.T) {
	f := seeded()
	h := NewHandler(New(f), bearerAuth())
	if rec := do(t, h, "POST", "/runs/r1/complete", ""); rec.Code != http.StatusOK {
		t.Errorf("complete = %d %s", rec.Code, rec.Body)
	}
	if rec := do(t, h, "POST", "/runs/r1/fail", `{"reason":"host gone"}`); rec.Code != http.StatusOK {
		t.Errorf("fail = %d %s", rec.Code, rec.Body)
	}
	want := []string{"ForceComplete r1", "ForceFail r1 host gone"}
	if got := f.Calls(); !slices.Equal(got, want) {
		t.Errorf("calls = %v, want %v", got, want)
	}

	for _, body := range []string{`{"reason":1}`, `{"why":"x"}`, `not json`, `{"reason":"` + strings.Repeat("x", MaxForceReason+1) + `"}`} {
		if rec := do(t, h, "POST", "/runs/r1/fail", body); rec.Code != http.StatusBadRequest {
			t.Errorf("fail body %.20q = %d, want 400", body, rec.Code)
		}
	}
	if got := f.Calls(); len(got) != 2 {
		t.Errorf("a refused body reached the driver: %v", got)
	}

	f.err = fmt.Errorf("%w: run is succeeded", workflow.ErrWrongState)
	if rec := do(t, h, "POST", "/runs/r1/complete", ""); rec.Code != http.StatusConflict {
		t.Errorf("wrong state = %d", rec.Code)
	}
}

// TestHandler_ForceReasonReachesAuthorizer: the Authorizer sees the
// reason, so a policy can require one.
func TestHandler_ForceReasonReachesAuthorizer(t *testing.T) {
	f := seeded()
	auth := bearerAuth()
	auth.Authorize = func(_ context.Context, req Request[user]) error {
		if req.Action == ActionForceFail && req.Resource.Reason == "" {
			return errors.New("a reason is required")
		}
		return nil
	}
	h := NewHandler(New(f), auth)
	if rec := do(t, h, "POST", "/runs/r1/fail", `{}`); rec.Code != http.StatusForbidden {
		t.Errorf("no reason = %d, want 403", rec.Code)
	}
	if rec := do(t, h, "POST", "/runs/r1/fail", `{"reason":"x"}`); rec.Code != http.StatusOK {
		t.Errorf("with reason = %d", rec.Code)
	}
}

// TestAdmin_NewOpsRefusedWithoutInterface: a client that declares the
// capabilities but lacks the interfaces is refused, and Support says so.
func TestAdmin_NewOpsRefusedWithoutInterface(t *testing.T) {
	a := New(bareClient{caps: allCaps})
	ctx := context.Background()
	if _, err := a.TriggerSchedule(ctx, "s1"); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("trigger = %v", err)
	}
	if err := a.ForceComplete(ctx, workflow.RunRef{RunID: "r1"}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("complete = %v", err)
	}
	if err := a.ForceFail(ctx, workflow.RunRef{RunID: "r1"}, ""); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("fail = %v", err)
	}
	for _, s := range a.Support() {
		if (s.Operation == OpScheduleTrigger || s.Operation == OpForce) && (s.Supported || s.Missing == 0) {
			t.Errorf("support %+v", s)
		}
	}

	// A client that lists schedules but cannot trigger them.
	lo := New(listerOnly{bareClient{caps: allCaps}})
	if !lo.Supports(OpSchedules) || lo.Supports(OpScheduleTrigger) {
		t.Errorf("lister only: schedules %v trigger %v", lo.Supports(OpSchedules), lo.Supports(OpScheduleTrigger))
	}
	if _, err := lo.TriggerSchedule(ctx, "s1"); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("lister only trigger = %v", err)
	}

	// And a client with the interfaces but not the capabilities.
	f := seeded()
	f.caps = allCaps &^ (workflow.CapScheduleTrigger | workflow.CapForceRun)
	h := NewHandler(New(f), bearerAuth())
	for _, p := range []string{"/schedules/s1/trigger", "/runs/r1/complete"} {
		if rec := do(t, h, "POST", p, ""); rec.Code != http.StatusNotImplemented {
			t.Errorf("%s without capability = %d, want 501", p, rec.Code)
		}
	}
	if rec := do(t, h, "POST", "/runs/r1/fail", `{}`); rec.Code != http.StatusNotImplemented {
		t.Errorf("fail without capability = %d", rec.Code)
	}
	if c := f.Calls(); len(c) != 0 {
		t.Errorf("refused calls reached the driver: %v", c)
	}
	if _, err := New(f).TriggerSchedule(ctx, ""); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("trigger without capability = %v", err)
	}
	if _, err := New(seeded()).TriggerSchedule(ctx, ""); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("empty id = %v", err)
	}
}

// TestHandler_ErrorHistory: attempt errors are shown; stacks only with
// WithStacks.
func TestHandler_ErrorHistory(t *testing.T) {
	f := seeded()
	at := time.Date(2026, 10, 13, 9, 0, 0, 0, time.UTC)
	f.runs[1].Errors = []workflow.AttemptError{
		{Step: "send", Attempt: 1, Error: "timeout", At: at, Stack: "goroutine 1 [running]:\nmain.send()"},
		{Step: "send", Attempt: 2, Error: "boom", At: at.Add(time.Minute)},
	}
	rec := do(t, NewHandler(New(f), bearerAuth()), "GET", "/runs/r2", "")
	var r runJSON
	if err := json.Unmarshal(rec.Body.Bytes(), &r); err != nil {
		t.Fatal(err)
	}
	if len(r.Errors) != 2 || r.Errors[0].Error != "timeout" || r.Errors[1].Attempt != 2 || !r.Errors[0].At.Equal(at) || r.Errors[0].Stack != "" {
		t.Errorf("errors = %+v", r.Errors)
	}
	rec = do(t, NewHandler(New(f), bearerAuth(), WithStacks()), "GET", "/runs/r2", "")
	if !strings.Contains(rec.Body.String(), "main.send()") {
		t.Errorf("WithStacks: %s", rec.Body)
	}
}

// TestHandler_Output: without WithOutputs the route is 501 and the
// capabilities route says output is false; with a MemoryOutputs wired
// as the CaptureOutput sink the run's attempts come back in order.
func TestHandler_Output(t *testing.T) {
	f := seeded()
	h := NewHandler(New(f), bearerAuth())
	if rec := do(t, h, "GET", "/runs/r1/output", ""); rec.Code != http.StatusNotImplemented {
		t.Errorf("unwired = %d", rec.Code)
	}
	if rec := do(t, h, "GET", "/capabilities", ""); !strings.Contains(rec.Body.String(), `"output":false`) {
		t.Errorf("capabilities = %s", rec.Body)
	}

	mo := NewMemoryOutputs(MemoryOutputLimits{})
	t0 := time.Date(2026, 10, 13, 9, 0, 0, 0, time.UTC)
	for i, err := range []error{errors.New("flaky"), nil} {
		out := middleware.Output{HasInfo: true, Info: workflow.StepInfo{Workflow: "w", Step: "send", RunID: "r1", Attempt: i + 1},
			Started: t0, Finished: t0.Add(time.Second), Err: err,
			Records: []middleware.OutputRecord{{Time: t0, Message: fmt.Sprint("hello ", i+1)}}}
		if err := mo.Deliver(context.Background(), out); err != nil {
			t.Fatal(err)
		}
	}
	h = NewHandler(New(f, WithOutputs(mo)), bearerAuth())
	if rec := do(t, h, "GET", "/capabilities", ""); !strings.Contains(rec.Body.String(), `"output":true`) {
		t.Errorf("capabilities = %s", rec.Body)
	}
	rec := do(t, h, "GET", "/runs/r1/output", "")
	var got struct{ Attempts []outputJSON }
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Attempts) != 2 || got.Attempts[0].Error != "flaky" || got.Attempts[1].Error != "" || !strings.Contains(got.Attempts[1].Text, "hello 2") || got.Attempts[0].Attempt != 1 {
		t.Errorf("output = %+v", got)
	}
	if rec := do(t, h, "GET", "/runs/r2/output", ""); !strings.Contains(rec.Body.String(), `"attempts":[]`) {
		t.Errorf("no output = %s", rec.Body)
	}
}

// TestMemoryOutputs_Bounds: attempts per run, runs kept and bytes per
// attempt are each bounded; output without StepInfo is refused.
func TestMemoryOutputs_Bounds(t *testing.T) {
	ctx := context.Background()
	mo := NewMemoryOutputs(MemoryOutputLimits{Runs: 2, PerRun: 2, Bytes: 80})
	deliver := func(run workflow.RunID, attempt int, msg string) {
		t.Helper()
		out := middleware.Output{HasInfo: true, Info: workflow.StepInfo{Step: "s", RunID: run, Attempt: attempt}, Records: []middleware.OutputRecord{{Message: msg}}}
		if err := mo.Deliver(ctx, out); err != nil {
			t.Fatal(err)
		}
	}
	for i := 1; i <= 3; i++ {
		deliver("a", i, "x")
	}
	got, _ := mo.RunOutput(ctx, "a")
	if len(got) != 2 || got[0].Attempt != 2 || got[1].Attempt != 3 {
		t.Errorf("per run = %+v", got)
	}
	deliver("b", 1, strings.Repeat("y", 500))
	got, _ = mo.RunOutput(ctx, "b")
	if len(got) != 1 || !got[0].Truncated || len(got[0].Text) != 80 {
		t.Errorf("bytes = %d %v", len(got[0].Text), got[0].Truncated)
	}
	deliver("c", 1, "z")
	if got, _ := mo.RunOutput(ctx, "a"); len(got) != 0 {
		t.Errorf("oldest run kept past Runs: %+v", got)
	}
	if got, _ := mo.RunOutput(ctx, "c"); len(got) != 1 {
		t.Errorf("newest run = %+v", got)
	}
	if err := mo.Deliver(ctx, middleware.Output{}); err == nil {
		t.Error("output without StepInfo was stored")
	}
}

// listerOnly lists schedules and implements nothing else.
type listerOnly struct{ bareClient }

func (listerOnly) Schedules(context.Context) ([]workflow.ScheduleInfo, error) { return nil, nil }

// TestOutputSourceFunc: a function wired with WithOutputs serves the
// output route.
func TestOutputSourceFunc(t *testing.T) {
	src := OutputSourceFunc(func(_ context.Context, run workflow.RunID) ([]AttemptOutput, error) {
		return []AttemptOutput{{Step: "s", Attempt: 1, Text: "from " + run.String()}}, nil
	})
	rec := do(t, NewHandler(New(seeded(), WithOutputs(src)), bearerAuth()), "GET", "/runs/r1/output", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "from r1") {
		t.Errorf("output = %d %s", rec.Code, rec.Body)
	}
}
