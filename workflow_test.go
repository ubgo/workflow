package workflow_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ubgo/workflow"
)

func noop(context.Context, []byte) ([]byte, error) { return nil, nil }

func TestValidate(t *testing.T) {
	ok := workflow.Workflow{Name: "w", Steps: []workflow.Step{
		{Name: "a", Run: noop},
		{Name: "b", After: []string{"a"}, Sleep: time.Hour},
		{Name: "c", After: []string{"a", "b"}, Wait: &workflow.WaitSpec{Event: "e", Timeout: time.Minute}},
	}}
	if err := workflow.Validate(ok); err != nil {
		t.Fatalf("valid workflow refused: %v", err)
	}

	cases := map[string]struct {
		wf   workflow.Workflow
		want string
	}{
		"no name":   {workflow.Workflow{Steps: ok.Steps}, "no name"},
		"no steps":  {workflow.Workflow{Name: "w"}, "no steps"},
		"step name": {workflow.Workflow{Name: "w", Steps: []workflow.Step{{Run: noop}}}, "step 0 has no name"},
		"duplicate": {workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "a", Run: noop}, {Name: "a", Run: noop}}}, "used twice"},
		"no kind":   {workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "a"}}}, "exactly one of"},
		"two kinds": {workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "a", Run: noop, Sleep: time.Second}}}, "exactly one of"},
		"unknown":   {workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "a", After: []string{"x"}, Run: noop}}}, "unknown step"},
		"self":      {workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "a", After: []string{"a"}, Run: noop}}}, "itself"},
		"cycle": {workflow.Workflow{Name: "w", Steps: []workflow.Step{
			{Name: "a", Run: noop},
			{Name: "b", After: []string{"a", "c"}, Run: noop},
			{Name: "c", After: []string{"b"}, Run: noop},
		}}, "cycle"},
		"wait both": {workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "a", Wait: &workflow.WaitSpec{Event: "e", Timeout: 1, Deadline: time.Unix(1, 0)}}}}, "both Deadline"},
		"wait name": {workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "a", Wait: &workflow.WaitSpec{}}}}, "Wait.Event"},
		"max runs":  {workflow.Workflow{Name: "w", Steps: ok.Steps, Concurrency: &workflow.Concurrency{}}, "MaxRuns"},
		"strategy":  {workflow.Workflow{Name: "w", Steps: ok.Steps, Concurrency: &workflow.Concurrency{MaxRuns: 1, Strategy: "x"}}, "Strategy"},
		"rate":      {workflow.Workflow{Name: "w", Steps: ok.Steps, RateLimits: []workflow.RateLimit{{Limit: 1}}}, "RateLimits[0]"},
		"policy":    {workflow.Workflow{Name: "w", Steps: ok.Steps, Idempotency: "sometimes"}, "Idempotency"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := workflow.Validate(tc.wf)
			if !errors.Is(err, workflow.ErrInvalid) {
				t.Fatalf("got %v, want ErrInvalid", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

func TestValidateSchedule(t *testing.T) {
	if err := workflow.ValidateSchedule(workflow.Schedule{Workflow: "w", Cron: "* * * * *", Location: time.UTC}); err != nil {
		t.Fatal(err)
	}
	if err := workflow.ValidateSchedule(workflow.Schedule{Workflow: "w", Cron: "* * * * *"}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("schedule without a zone accepted: %v", err)
	}
}

func TestRequiresAndMissing(t *testing.T) {
	wf := workflow.Workflow{
		Name:        "w",
		Cron:        []string{"0 * * * *"},
		Concurrency: &workflow.Concurrency{MaxRuns: 1},
		RateLimits:  []workflow.RateLimit{{Limit: 1, Per: time.Second}},
		Idempotency: workflow.IdempotencyRequired,
		Steps: []workflow.Step{
			{Name: "a", Run: noop},
			{Name: "b", Wait: &workflow.WaitSpec{Event: "e"}},
			{Name: "c", Sleep: time.Second},
		},
	}
	want := workflow.CapCron | workflow.CapConcurrencyKey | workflow.CapRateLimit | workflow.CapIdempotencyKey | workflow.CapWaitForEvent | workflow.CapDurableSleep
	if got := workflow.Requires(wf); got != want {
		t.Fatalf("Requires = %s, want %s", got, want)
	}
	if got := workflow.Requires(workflow.Workflow{Steps: []workflow.Step{{Name: "a", Run: noop}}}); got != 0 {
		t.Fatalf("plain job requires %s", got)
	}

	have := workflow.CapCron | workflow.CapRateLimit | workflow.CapCancel
	missing := have.Missing(want)
	if missing != workflow.CapConcurrencyKey|workflow.CapIdempotencyKey|workflow.CapWaitForEvent|workflow.CapDurableSleep {
		t.Fatalf("Missing = %s", missing)
	}
	if !have.Has(workflow.CapCron|workflow.CapCancel) || have.Has(workflow.CapDurable) {
		t.Fatal("Has is wrong")
	}
	if s := missing.String(); s != "WaitForEvent|DurableSleep|ConcurrencyKey|IdempotencyKey" {
		t.Fatalf("String = %q", s)
	}

	err := workflow.CheckCaps(have, wf)
	var ue *workflow.UnsupportedError
	if !errors.Is(err, workflow.ErrUnsupported) || !errors.As(err, &ue) || ue.Missing != missing {
		t.Fatalf("CheckCaps = %v", err)
	}
	if err := workflow.CheckCaps(have|missing, wf); err != nil {
		t.Fatalf("CheckCaps with every cap = %v", err)
	}

	job := workflow.Job{RunAt: time.Unix(1, 0), IdempotencyKey: "k"}
	if got := workflow.RequiresJob(job); got != workflow.CapDelayedStart|workflow.CapIdempotencyKey {
		t.Fatalf("RequiresJob = %s", got)
	}
}

func TestEffectiveRetries(t *testing.T) {
	wf := workflow.Workflow{Retry: workflow.RetryPolicy{Max: 3}}
	if got := workflow.EffectiveRetries(workflow.Step{}, wf); got.Max != 3 {
		t.Fatalf("default: Max = %d", got.Max)
	}
	if got := workflow.EffectiveRetries(workflow.Step{Retry: &workflow.RetryPolicy{Max: 5}}, wf); got.Max != 5 {
		t.Fatalf("override: Max = %d", got.Max)
	}
	if got := workflow.EffectiveRetries(workflow.Step{Retry: workflow.RetriesNone()}, wf); got.Max != 0 {
		t.Fatalf("RetriesNone: Max = %d", got.Max)
	}
}

func TestRetryable(t *testing.T) {
	boom := errors.New("boom")
	p := workflow.RetryPolicy{Max: 2, NoRetryOn: func(err error) bool { return err.Error() == "fatal" }}
	if !p.Retryable(boom, 1) || !p.Retryable(boom, 2) || p.Retryable(boom, 3) {
		t.Fatal("Max not honoured")
	}
	if p.Retryable(workflow.Permanent(boom), 1) {
		t.Fatal("permanent error retried")
	}
	if p.Retryable(errors.New("fatal"), 1) {
		t.Fatal("NoRetryOn ignored")
	}
	if p.Retryable(workflow.ErrCancelled, 1) {
		t.Fatal("cancelled run retried")
	}
}

func TestBackoffDelay(t *testing.T) {
	b := workflow.Backoff{Initial: time.Second, Factor: 2, Max: 5 * time.Second}
	for n, want := range map[int]time.Duration{0: 0, 1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second, 4: 5 * time.Second, 50: 5 * time.Second} {
		if got := b.Delay(n); got != want {
			t.Errorf("Delay(%d) = %s, want %s", n, got, want)
		}
	}
	if got := (workflow.Backoff{Initial: time.Second}).Delay(3); got != time.Second {
		t.Errorf("constant backoff = %s", got)
	}
}

func TestPermanent(t *testing.T) {
	base := errors.New("bad input")
	p := workflow.Permanent(base)
	if !workflow.IsPermanent(p) || !errors.Is(p, base) {
		t.Fatal("Permanent does not mark or wrap")
	}
	if !workflow.IsPermanent(errors.Join(errors.New("ctx"), p)) {
		t.Fatal("wrapped permanent error not found")
	}
	if workflow.IsPermanent(base) || workflow.Permanent(nil) != nil {
		t.Fatal("plain error or nil treated as permanent")
	}
}

func TestStatusIsTerminal(t *testing.T) {
	for _, s := range []workflow.Status{workflow.StatusSucceeded, workflow.StatusFailed, workflow.StatusCancelled} {
		if !s.IsTerminal() {
			t.Errorf("%s not terminal", s)
		}
	}
	for _, s := range []workflow.Status{workflow.StatusScheduled, workflow.StatusQueued, workflow.StatusRunning, workflow.StatusWaiting} {
		if s.IsTerminal() {
			t.Errorf("%s terminal", s)
		}
	}
}

func TestDefine(t *testing.T) {
	type in struct{ N int }
	type out struct{ Double int }
	h := workflow.Define(func(_ context.Context, v in) (out, error) { return out{v.N * 2}, nil })

	b, err := h(context.Background(), []byte(`{"N":21}`))
	if err != nil || string(b) != `{"Double":42}` {
		t.Fatalf("got %s, %v", b, err)
	}
	if b, err := h(context.Background(), nil); err != nil || string(b) != `{"Double":0}` {
		t.Fatalf("empty input: %s, %v", b, err)
	}
	if _, err := h(context.Background(), []byte(`not json`)); !workflow.IsPermanent(err) {
		t.Fatalf("bad input not permanent: %v", err)
	}

	boom := errors.New("boom")
	failing := workflow.Define(func(context.Context, in) (out, error) { return out{}, boom })
	if _, err := failing(context.Background(), nil); !errors.Is(err, boom) || workflow.IsPermanent(err) {
		t.Fatalf("handler error changed: %v", err)
	}
}

func TestWaitResolveKey(t *testing.T) {
	job := workflow.Job{Payload: []byte(`{"order":"o-7"}`), Labels: map[string]string{"order": "o-7"}}

	fixed := workflow.WaitSpec{Event: "payment.succeeded", Key: "k"}
	if k, err := fixed.ResolveKey(job); err != nil || k != "k" {
		t.Fatalf("fixed key: %q %v", k, err)
	}
	perRun := workflow.WaitSpec{Event: "payment.succeeded", KeyFrom: workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["order"] })}
	if k, err := perRun.ResolveKey(job); err != nil || k != "o-7" {
		t.Fatalf("per-run key: %q %v", k, err)
	}
	expr := workflow.WaitSpec{Event: "payment.succeeded", KeyFrom: workflow.KeyExpr("input.order")}
	if _, err := expr.ResolveKey(job); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("expression key resolved in Go: %v", err)
	}

	for name, w := range map[string]workflow.WaitSpec{
		"Key and KeyFrom": {Event: "e", Key: "k", KeyFrom: workflow.KeyExpr("input.k")},
		"Func and Expr":   {Event: "e", KeyFrom: workflow.ConcurrencyKey{Func: func(workflow.Job) string { return "" }, Expr: "x"}},
	} {
		wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "a", Wait: &w}}}
		if err := workflow.Validate(wf); !errors.Is(err, workflow.ErrInvalid) || !strings.Contains(err.Error(), "both") {
			t.Errorf("%s accepted: %v", name, err)
		}
	}
}

func TestStepInfo(t *testing.T) {
	if _, ok := workflow.StepInfoFrom(context.Background()); ok {
		t.Fatal("StepInfo found on a bare context")
	}
	want := workflow.StepInfo{Workflow: "w", Step: "a", RunID: "r1", Attempt: 2, Labels: map[string]string{"tenant": "t"}}
	got, ok := workflow.StepInfoFrom(workflow.WithStepInfo(context.Background(), want))
	if !ok || got.Workflow != "w" || got.Step != "a" || got.RunID != "r1" || got.Attempt != 2 || got.Labels["tenant"] != "t" {
		t.Fatalf("got %+v %v", got, ok)
	}
}

func TestCapRunFinishedName(t *testing.T) {
	if s := (workflow.CapCancel | workflow.CapRunFinished).String(); s != "Cancel|RunFinished" {
		t.Fatalf("String = %q", s)
	}
}
