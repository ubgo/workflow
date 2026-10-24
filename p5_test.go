package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ubgo/workflow"
)

func TestRequiresNewFields(t *testing.T) {
	wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "a", Run: noop, HeartbeatTimeout: time.Second}},
		RunTimeout: time.Minute, Concurrency: &workflow.Concurrency{MaxRuns: 1, FIFO: true},
		Cron: []string{"* * * * * *"}, CronSeconds: true, CronActive: workflow.Active{Until: time.Unix(10, 0)}, CronKeepRuns: 3}
	want := workflow.CapHeartbeat | workflow.CapRunTimeout | workflow.CapKeyFIFO | workflow.CapCronSeconds |
		workflow.CapScheduleActive | workflow.CapScheduleKeep
	if got := workflow.Requires(wf); !got.Has(want) {
		t.Fatalf("Requires = %s, want it to hold %s", got, want)
	}
	if got := workflow.Requires(workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "a", Run: noop}}, OnTimeout: noop}); !got.Has(workflow.CapRunTimeout) {
		t.Errorf("Requires with OnTimeout = %s, want CapRunTimeout", got)
	}
	err := workflow.CheckJob(workflow.CapCron, workflow.Job{Workflow: "w", Tenant: "acme"})
	var ue *workflow.UnsupportedError
	if !errors.As(err, &ue) || ue.Field != "Job.Tenant" || !ue.Missing.Has(workflow.CapTenant) {
		t.Errorf("CheckJob(Tenant) = %v, want Job.Tenant missing CapTenant", err)
	}
	for field, s := range map[string]workflow.Schedule{
		"Schedule.Seconds":  {Seconds: true},
		"Schedule.Active":   {Active: workflow.Active{From: time.Unix(1, 0)}},
		"Schedule.KeepRuns": {KeepRuns: 1},
	} {
		err := workflow.CheckScheduleCaps(workflow.CapCron, s)
		if !errors.As(err, &ue) || ue.Field != field {
			t.Errorf("CheckScheduleCaps for %s = %v", field, err)
		}
	}
}

func TestValidateNewFields(t *testing.T) {
	base := func() workflow.Workflow {
		return workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "a", Run: noop}}}
	}
	cases := map[string]func(*workflow.Workflow){
		"negative HeartbeatTimeout": func(w *workflow.Workflow) { w.Steps[0].HeartbeatTimeout = -1 },
		"HeartbeatTimeout on sleep": func(w *workflow.Workflow) {
			w.Steps = []workflow.Step{{Name: "s", Sleep: time.Second, HeartbeatTimeout: time.Second}}
		},
		"FIFO with drop": func(w *workflow.Workflow) {
			w.Concurrency = &workflow.Concurrency{MaxRuns: 1, FIFO: true, Strategy: workflow.ConcurrencyDropNewest}
		},
		"CronSeconds w/o Cron": func(w *workflow.Workflow) { w.CronSeconds = true },
		"CronKeepRuns < 0":     func(w *workflow.Workflow) { w.Cron = []string{"* * * * *"}; w.CronKeepRuns = -1 },
		"negative RunTimeout":  func(w *workflow.Workflow) { w.RunTimeout = -1 },
		"empty CronActive": func(w *workflow.Workflow) {
			w.Cron = []string{"* * * * *"}
			w.CronActive = workflow.Active{From: time.Unix(10, 0), Until: time.Unix(5, 0)}
		},
	}
	for name, edit := range cases {
		wf := base()
		edit(&wf)
		if err := workflow.Validate(wf); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("%s: Validate = %v, want ErrInvalid", name, err)
		}
	}
	ok := base()
	ok.Concurrency = &workflow.Concurrency{MaxRuns: 1, FIFO: true}
	if err := workflow.Validate(ok); err != nil {
		t.Errorf("FIFO with QueueNewest: %v", err)
	}
	if err := workflow.ValidateJob(workflow.Job{Workflow: "w", Tenant: "bad tenant"}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("ValidateJob(bad tenant) = %v, want ErrInvalid", err)
	}
	if err := workflow.ValidateSchedule(workflow.Schedule{Workflow: "w", Cron: "* * * * *", Location: time.UTC, KeepRuns: -1}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("ValidateSchedule(KeepRuns -1) = %v, want ErrInvalid", err)
	}
	if err := workflow.ValidateQueues([]workflow.QueueConfig{{Name: "q", Weight: -1}}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("ValidateQueues(Weight -1) = %v, want ErrInvalid", err)
	}
}

func TestValidateTenant(t *testing.T) {
	for _, ok := range []string{"acme", "a.b_c:d-e@f", strings.Repeat("x", 128)} {
		if err := workflow.ValidateTenant(ok); err != nil {
			t.Errorf("ValidateTenant(%q) = %v", ok, err)
		}
	}
	for _, bad := range []string{"", "a b", "a/b", strings.Repeat("x", 129)} {
		if err := workflow.ValidateTenant(bad); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("ValidateTenant(%q) = %v, want ErrInvalid", bad, err)
		}
	}
}

func TestRunQueryTenantAndDue(t *testing.T) {
	at := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	info := workflow.RunInfo{Tenant: "acme", RunAt: at}
	cases := []struct {
		q    workflow.RunQuery
		want bool
	}{
		{workflow.RunQuery{Tenant: "acme"}, true},
		{workflow.RunQuery{Tenant: "other"}, false},
		{workflow.RunQuery{DueFrom: at}, true},
		{workflow.RunQuery{DueFrom: at.Add(time.Nanosecond)}, false},
		{workflow.RunQuery{DueTo: at}, false},
		{workflow.RunQuery{DueTo: at.Add(time.Nanosecond)}, true},
	}
	for i, c := range cases {
		if got := c.q.Match(info); got != c.want {
			t.Errorf("case %d: Match = %v, want %v", i, got, c.want)
		}
	}
	if (workflow.RunQuery{DueFrom: at}).Match(workflow.RunInfo{}) {
		t.Error("a run without RunAt matched a due filter")
	}
	if err := workflow.ValidateRunQuery(workflow.RunQuery{DueFrom: at, DueTo: at}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("DueFrom == DueTo: %v, want ErrInvalid", err)
	}
	if err := workflow.CheckRunQuery(workflow.CapRunQuery, workflow.RunQuery{DueTo: at}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("CheckRunQuery(Due) without CapDueQuery = %v", err)
	}
	if err := workflow.CheckRunQuery(workflow.CapRunQuery, workflow.RunQuery{Tenant: "acme"}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("CheckRunQuery(Tenant) without CapTenant = %v", err)
	}
}

func TestPickWeighted(t *testing.T) {
	w := []int{3, 1}
	cases := []struct {
		r    float64
		want int
	}{{0, 0}, {0.74, 0}, {0.75, 1}, {0.99, 1}}
	for _, c := range cases {
		if got := workflow.PickWeighted(w, c.r); got != c.want {
			t.Errorf("PickWeighted(%v, %v) = %d, want %d", w, c.r, got, c.want)
		}
	}
	if got := workflow.PickWeighted([]int{0, 0}, 0.6); got != 1 {
		t.Errorf("zero weights count as 1: got %d, want 1", got)
	}
	if got := workflow.PickWeighted(nil, 0.5); got != -1 {
		t.Errorf("no weights = %d, want -1", got)
	}
}

func TestRunOutput(t *testing.T) {
	one := workflow.Workflow{Steps: []workflow.Step{{Name: "a"}, {Name: "b", After: []string{"a"}}}}
	if got := string(workflow.RunOutput(one, map[string][]byte{"a": []byte("x"), "b": []byte(`"y"`)})); got != `"y"` {
		t.Errorf("one sink: %s", got)
	}
	two := workflow.Workflow{Steps: []workflow.Step{{Name: "x"}, {Name: "y"}}}
	var got map[string]int
	if err := json.Unmarshal(workflow.RunOutput(two, map[string][]byte{"x": []byte("1"), "y": []byte("2")}), &got); err != nil || got["x"] != 1 || got["y"] != 2 {
		t.Errorf("two sinks: %v %v", got, err)
	}
	if s := workflow.Sinks(one); len(s) != 1 || s[0] != "b" {
		t.Errorf("Sinks = %v", s)
	}
}

func TestTimeouts(t *testing.T) {
	for _, kind := range []error{workflow.ErrStepTimeout, workflow.ErrWaitTimeout, workflow.ErrHeartbeatTimeout, workflow.ErrRunTimeout} {
		err := workflow.TimeoutError(kind, "detail")
		if !errors.Is(err, kind) || !workflow.IsTimeout(err) || !strings.Contains(err.Error(), "detail") {
			t.Errorf("TimeoutError(%v) = %v", kind, err)
		}
	}
	if workflow.IsTimeout(errors.New("x")) {
		t.Error("an ordinary error is a timeout")
	}
	if errors.Is(workflow.TimeoutError(workflow.ErrStepTimeout, ""), workflow.ErrRunTimeout) {
		t.Error("a step timeout matches ErrRunTimeout")
	}
	onT := func(context.Context, []byte) ([]byte, error) { return []byte("t"), nil }
	onF := func(context.Context, []byte) ([]byte, error) { return []byte("f"), nil }
	wf := workflow.Workflow{OnTimeout: onT, OnFailure: onF}
	out := func(h workflow.Handler) string { b, _ := h(context.Background(), nil); return string(b) }
	if got := out(workflow.FailureHandler(wf, workflow.TimeoutError(workflow.ErrRunTimeout, ""))); got != "t" {
		t.Errorf("a timeout ran %q, want OnTimeout", got)
	}
	if got := out(workflow.FailureHandler(wf, errors.New("x"))); got != "f" {
		t.Errorf("a failure ran %q, want OnFailure", got)
	}
	wf.OnTimeout = nil
	if got := out(workflow.FailureHandler(wf, workflow.TimeoutError(workflow.ErrRunTimeout, ""))); got != "f" {
		t.Errorf("a timeout without OnTimeout ran %q, want OnFailure", got)
	}
}

func TestSnooze(t *testing.T) {
	if d, ok := workflow.SnoozeDelay(workflow.Snooze(time.Minute)); !ok || d != time.Minute {
		t.Errorf("SnoozeDelay = %v %v", d, ok)
	}
	if d, _ := workflow.SnoozeDelay(workflow.Snooze(-time.Second)); d != 0 {
		t.Errorf("negative snooze = %v, want 0", d)
	}
	if d, _ := workflow.SnoozeDelay(workflow.Snooze(time.Hour * 24 * 365)); d != workflow.MaxSnooze {
		t.Errorf("huge snooze = %v, want MaxSnooze", d)
	}
	wrapped := errors.Join(errors.New("ctx"), workflow.Snooze(time.Second))
	if _, ok := workflow.SnoozeDelay(wrapped); !ok || !errors.Is(wrapped, workflow.ErrSnoozed) {
		t.Error("a wrapped snooze is not found")
	}
	if _, ok := workflow.SnoozeDelay(errors.New("x")); ok {
		t.Error("an ordinary error is a snooze")
	}
}

func TestEnqueueOutcome(t *testing.T) {
	workflow.MarkDuplicate(context.Background()) // nobody asked: no panic
	ctx, out := workflow.WithEnqueueOutcome(context.Background())
	workflow.MarkDuplicate(ctx)
	workflow.MarkInserted(ctx) // the first mark wins
	if !out.Reported || !out.Duplicate {
		t.Errorf("outcome = %+v, want a reported duplicate", *out)
	}
	ctx, out = workflow.WithEnqueueOutcome(context.Background())
	workflow.MarkInserted(ctx)
	if !out.Reported || out.Duplicate {
		t.Errorf("outcome = %+v, want a reported insert", *out)
	}
}

func TestRunError(t *testing.T) {
	base := errors.New("boom")
	err := error(&workflow.RunError{RunID: "r-1", Workflow: "w", Step: "s", Attempt: 2, Err: base})
	if !errors.Is(err, base) || err.Error() != "boom" {
		t.Errorf("RunError text = %q, want the step's own message", err)
	}
	if d := err.(*workflow.RunError).Describe(); d != `workflow "w" run r-1 step "s" attempt 2: boom` {
		t.Errorf("Describe = %q", d)
	}
	if re, ok := workflow.RunErrorOf(errors.Join(err)); !ok || re.RunID != "r-1" {
		t.Errorf("RunErrorOf = %v %v", re, ok)
	}
}

func TestPayloads(t *testing.T) {
	type in struct{ N int }
	job, err := workflow.JobOf("w", in{N: 3})
	if err != nil || string(job.Payload) != `{"N":3}` {
		t.Fatalf("JobOf = %s %v", job.Payload, err)
	}
	got, err := workflow.DecodePayload[in](job.Payload)
	if err != nil || got.N != 3 {
		t.Errorf("DecodePayload = %v %v", got, err)
	}
	if _, err := workflow.DecodePayload[in]([]byte("{")); !workflow.IsPermanent(err) {
		t.Errorf("a bad payload is not Permanent: %v", err)
	}
	big := workflow.Job{Workflow: "w", Payload: make([]byte, 11)}
	err = workflow.CheckPayload(big, 10, "test")
	var pe *workflow.PayloadTooLargeError
	if !errors.As(err, &pe) || pe.Size != 11 || !errors.Is(err, workflow.ErrPayloadTooLarge) || !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("CheckPayload = %v", err)
	}
	if err := workflow.CheckPayload(workflow.Job{Workflow: "w", Chain: []workflow.Job{big}}, 10, "test"); err == nil {
		t.Error("a chained job over the limit passed")
	}
	if err := workflow.CheckPayload(big, 0, "test"); err != nil {
		t.Errorf("limit 0 means none: %v", err)
	}
}

func TestUpcast(t *testing.T) {
	ups := map[int]workflow.Upcaster{
		0: func(b []byte) ([]byte, error) { return []byte(strings.ReplaceAll(string(b), "old", "mid")), nil },
		1: func(b []byte) ([]byte, error) { return []byte(strings.ReplaceAll(string(b), "mid", "new")), nil },
	}
	out, err := workflow.Upcast([]byte(`{"old":1}`), 0, 2, ups)
	if err != nil || string(out) != `{"new":1}` {
		t.Errorf("Upcast 0->2 = %s %v", out, err)
	}
	if _, err := workflow.Upcast(nil, 3, 2, ups); !workflow.IsPermanent(err) {
		t.Errorf("a newer payload: %v, want Permanent", err)
	}
	if _, err := workflow.Upcast(nil, 0, 3, ups); !workflow.IsPermanent(err) {
		t.Errorf("a gap in the chain: %v, want Permanent", err)
	}
	if v, err := workflow.PayloadVersionOf(map[string]string{workflow.LabelPayloadVersion: "x"}); err == nil {
		t.Errorf("PayloadVersionOf(x) = %d, want an error", v)
	}
	type in struct{ New int }
	h := workflow.DefineVersioned(2, ups, func(_ context.Context, v in) (int, error) { return v.New, nil })
	ctx := workflow.WithStepInfo(context.Background(), workflow.StepInfo{Labels: map[string]string{workflow.LabelPayloadVersion: "0"}})
	out, err = h(ctx, []byte(`{"old":7}`))
	if err != nil || string(out) != "7" {
		t.Errorf("DefineVersioned = %s %v", out, err)
	}
}

func TestProgressAndHeartbeatHooks(t *testing.T) {
	if err := workflow.ReportProgress(context.Background(), workflow.Progress{Percent: 1}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("ReportProgress without a reporter = %v, want ErrUnsupported", err)
	}
	if err := workflow.ValidateProgress(workflow.Progress{Data: make([]byte, workflow.MaxProgressData+1)}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("oversized Data: %v", err)
	}
	var got workflow.Progress
	ctx := workflow.WithProgressReporter(context.Background(), workflow.ProgressFunc(func(_ context.Context, p workflow.Progress) error {
		got = p
		return nil
	}))
	if err := workflow.ReportProgress(ctx, workflow.Progress{Percent: 40}); err != nil || got.Percent != 40 {
		t.Errorf("ReportProgress = %v, got %+v", err, got)
	}
	beats := 0
	ctx = workflow.WithHeartbeater(context.Background(), workflow.HeartbeatFunc(func(context.Context, []byte) error { beats++; return nil }))
	_ = workflow.Heartbeat(ctx, nil)
	if beats != 1 {
		t.Errorf("Heartbeat reached the driver %d times, want 1", beats)
	}
}

func TestCheckHealthFallback(t *testing.T) {
	rep := workflow.CheckHealth(context.Background(), nil)
	if !rep.Live || !rep.Ready || len(rep.Checks) != 1 {
		t.Errorf("CheckHealth fallback = %+v", rep)
	}
}

func TestCheckJobLabels(t *testing.T) {
	forged := workflow.Job{Workflow: "w", Labels: map[string]string{workflow.LabelPrefixInternal + "parent-run": "r-1", "app": "x"}}
	if err := workflow.CheckJobLabels(context.Background(), forged); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("a reserved label without allowance = %v, want ErrInvalid", err)
	}
	ok := workflow.AllowReservedLabels(context.Background(), workflow.LabelPrefixInternal+"parent-run")
	if err := workflow.CheckJobLabels(ok, forged); err != nil {
		t.Fatalf("an allowed key: %v", err)
	}
	other := workflow.AllowReservedLabels(context.Background(), workflow.LabelPrefixInternal+"unique-key")
	if err := workflow.CheckJobLabels(other, forged); err == nil {
		t.Fatal("allowing one key let another through")
	}
	batch := workflow.Job{Workflow: "w", Labels: map[string]string{workflow.LabelPrefixBatch + "id": "b"}}
	if err := workflow.CheckJobLabels(workflow.AllowReservedPrefix(context.Background(), workflow.LabelPrefixBatch), batch); err != nil {
		t.Fatalf("an allowed prefix: %v", err)
	}
	if err := workflow.CheckJobLabels(workflow.AllowReservedPrefix(context.Background(), workflow.LabelPrefixOTel), batch); err == nil {
		t.Fatal("allowing one prefix let another through")
	}
	chained := workflow.Job{Workflow: "w", Chain: []workflow.Job{batch}}
	if err := workflow.CheckJobLabels(context.Background(), chained); err == nil {
		t.Fatal("a reserved label in a chained job passed")
	}
	if err := workflow.CheckJobLabels(context.Background(), workflow.Job{Workflow: "w", Labels: map[string]string{"app": "x"}}); err != nil {
		t.Fatalf("app labels: %v", err)
	}
}

func TestChainEditsRefuseReservedLabels(t *testing.T) {
	ed := workflow.NewChainEdits(workflow.CapChain, nil)
	ctx := workflow.WithChainEditor(context.Background(), ed)
	forged := workflow.Job{Workflow: "w", Labels: map[string]string{workflow.LabelParentRun: "r"}}
	if err := workflow.PrependToChain(ctx, forged); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("PrependToChain with a reserved label = %v, want ErrInvalid", err)
	}
	if err := workflow.AppendToChain(ctx, workflow.Job{Workflow: "w"}, forged); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("AppendToChain with a reserved label = %v, want ErrInvalid", err)
	}
	if ed.Changed() {
		t.Error("a refused edit was buffered")
	}
	owned := workflow.AllowReservedLabels(ctx, workflow.LabelParentRun)
	if err := workflow.AppendToChain(owned, forged); err != nil || !ed.Changed() {
		t.Errorf("an owner-allowed label: %v (changed %v), want it buffered", err, ed.Changed())
	}
}

// bare is a Client implementing none of the optional interfaces.
type bare struct{ workflow.Client }

// wrapped is the documented wrapper shape: Client plus Forward.
type wrapped struct {
	workflow.Client
	workflow.Forward
}

type rescheduler struct{ bare }

func (rescheduler) Reschedule(context.Context, workflow.RunRef, time.Time) (workflow.RunRef, error) {
	return workflow.RunRef{}, nil
}

func TestSupportsSeesThroughForward(t *testing.T) {
	inner := bare{}
	w := wrapped{Client: inner, Forward: workflow.Forward{Inner: inner}}
	if _, ok := any(w).(workflow.RunRescheduler); !ok {
		t.Fatal("the wrapper should implement RunRescheduler through Forward")
	}
	for _, op := range []workflow.Op{workflow.OpReschedule, workflow.OpCancelStep, workflow.OpNodes, workflow.OpLifecycle} {
		if workflow.Supports(w, op) {
			t.Errorf("Supports(%s) through Forward over a client without it = true", op)
		}
	}
	r := rescheduler{}
	w = wrapped{Client: r, Forward: workflow.Forward{Inner: r}}
	if !workflow.Supports(w, workflow.OpReschedule) || !workflow.Supports(r, workflow.OpReschedule) {
		t.Error("Supports(reschedule) = false for a client that does it")
	}
	if workflow.Supports(r, "nope") {
		t.Error("an unknown op is supported")
	}
}

func TestScheduleTenant(t *testing.T) {
	s := workflow.Schedule{Workflow: "w", Cron: "* * * * *", Location: time.UTC, Tenant: "acme"}
	if !workflow.RequiresSchedule(s).Has(workflow.CapTenant) {
		t.Error("RequiresSchedule ignores Tenant")
	}
	var ue *workflow.UnsupportedError
	if err := workflow.CheckScheduleCaps(workflow.CapCron, s); !errors.As(err, &ue) || ue.Field != "Schedule.Tenant" {
		t.Errorf("CheckScheduleCaps = %v, want Schedule.Tenant", err)
	}
	s.Tenant = "a b"
	if err := workflow.ValidateSchedule(s); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("ValidateSchedule(bad tenant) = %v", err)
	}
	s.Tenant = "acme"
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var back workflow.Schedule
	if err := json.Unmarshal(b, &back); err != nil || back.Tenant != "acme" {
		t.Errorf("JSON round trip Tenant = %q, %v", back.Tenant, err)
	}
}
