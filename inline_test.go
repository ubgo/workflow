package workflow_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/ubgo/workflow"
)

// noSleep records the waits RunInline asks for and returns at once.
type noSleep struct{ waits []time.Duration }

func (n *noSleep) sleep(_ context.Context, d time.Duration) error {
	n.waits = append(n.waits, d)
	return nil
}

func TestRunInlineRunsTheDAGInOrderWithMiddleware(t *testing.T) {
	var order []string
	step := func(name string) workflow.Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			info, _ := workflow.StepInfoFrom(ctx)
			if info.RunID != workflow.InlineRunID || info.Step != name || info.Queue != workflow.DefaultQueue {
				t.Errorf("StepInfo = %+v", info)
			}
			order = append(order, name)
			return append(in, name...), nil
		}
	}
	wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{
		{Name: "c", After: []string{"a", "b"}, Run: step("c")},
		{Name: "a", Run: step("a")},
		{Name: "b", Run: step("b")},
	}}
	var wrapped int
	mw := func(next workflow.Handler) workflow.Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) { wrapped++; return next(ctx, in) }
	}
	res, err := workflow.RunInline(context.Background(), wf, workflow.Job{Payload: []byte("p")}, workflow.InlineMiddleware(mw))
	if err != nil {
		t.Fatalf("RunInline: %v", err)
	}
	if !slices.Equal(order, []string{"a", "b", "c"}) {
		t.Errorf("order = %v, want a, b, c (declaration order among ready steps)", order)
	}
	if wrapped != 3 {
		t.Errorf("middleware ran %d times, want 3", wrapped)
	}
	if got := string(res.Outputs["c"]); got != `{"a":"pa","b":"pb"}c` {
		t.Errorf("c's output = %s", got)
	}
}

func TestRunInlineRetriesAndRunsOnFailureOnce(t *testing.T) {
	boom := errors.New("boom")
	attempts, failures := 0, 0
	wf := workflow.Workflow{
		Name:      "w",
		Steps:     []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) { attempts++; return nil, boom }}},
		Retry:     workflow.RetryPolicy{Max: 2, Backoff: workflow.Backoff{Initial: time.Second, Factor: 2}},
		OnFailure: func(context.Context, []byte) ([]byte, error) { failures++; return nil, nil },
	}
	var ns noSleep
	res, err := workflow.RunInline(context.Background(), wf, workflow.Job{}, workflow.InlineSleep(ns.sleep))
	if !errors.Is(err, boom) {
		t.Fatalf("RunInline = %v, want boom", err)
	}
	if attempts != 3 || res.Attempts["s"] != 3 || failures != 1 {
		t.Errorf("attempts %d (result %d), OnFailure %d; want 3, 3, 1", attempts, res.Attempts["s"], failures)
	}
	if !slices.Equal(ns.waits, []time.Duration{time.Second, 2 * time.Second}) {
		t.Errorf("backoff waits = %v", ns.waits)
	}
}

func TestRunInlineJobRetryAndUntil(t *testing.T) {
	boom := errors.New("boom")
	n := 0
	wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) { n++; return nil, boom }}}}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	sleep := func(_ context.Context, d time.Duration) error { now = now.Add(d); return nil }
	job := workflow.Job{Retry: &workflow.RetryPolicy{Max: 100, Backoff: workflow.Backoff{Initial: time.Minute}, Until: 150 * time.Second}}
	_, err := workflow.RunInline(context.Background(), wf, job, workflow.InlineSleep(sleep), workflow.InlineNow(func() time.Time { return now }))
	if !errors.Is(err, boom) || n != 3 {
		t.Fatalf("Until 2.5m with 1m backoff: %d attempts, %v; want 3", n, err)
	}
}

func TestRunInlineDiscardSkipsOnFailure(t *testing.T) {
	failures := 0
	wf := workflow.Workflow{
		Name:      "w",
		Steps:     []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) { return nil, workflow.Discard(errors.New("gone")) }}},
		OnFailure: func(context.Context, []byte) ([]byte, error) { failures++; return nil, nil },
	}
	_, err := workflow.RunInline(context.Background(), wf, workflow.Job{})
	if !errors.Is(err, workflow.ErrDiscarded) || !errors.Is(err, workflow.ErrCancelled) || failures != 0 {
		t.Fatalf("RunInline = %v with %d OnFailure runs; want ErrDiscarded and none", err, failures)
	}
}

func TestRunInlineWaitAndSleep(t *testing.T) {
	wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{
		{Name: "nap", Sleep: time.Hour},
		{Name: "wait", After: []string{"nap"}, Wait: &workflow.WaitSpec{Event: "paid", KeyFrom: workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["order"] })}},
	}}
	job := workflow.Job{Labels: map[string]string{"order": "o1"}}
	if _, err := workflow.RunInline(context.Background(), wf, job); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("a wait step without InlineEvents: %v, want ErrUnsupported", err)
	}
	var ns noSleep
	res, err := workflow.RunInline(context.Background(), wf, job, workflow.InlineSleep(ns.sleep),
		workflow.InlineEvents(workflow.Event{Name: "paid", Key: "o2", Payload: []byte("wrong")}, workflow.Event{Name: "paid", Key: "o1", Payload: []byte("ok")}))
	if err != nil || string(res.Outputs["wait"]) != "ok" {
		t.Fatalf("RunInline = %v, wait output %q; want the o1 event", err, res.Outputs["wait"])
	}
	if !slices.Equal(ns.waits, []time.Duration{time.Hour}) {
		t.Errorf("sleeps = %v, want one hour", ns.waits)
	}
	_, err = workflow.RunInline(context.Background(), wf, job, workflow.InlineSleep(ns.sleep), workflow.InlineEvents(workflow.Event{Name: "paid", Key: "o2"}))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("no matching event: %v, want a timeout", err)
	}
}

func TestRunInlineTimeoutAndPanic(t *testing.T) {
	wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "s", Timeout: time.Millisecond, Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}}}
	if _, err := workflow.RunInline(context.Background(), wf, workflow.Job{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a timed-out step: %v, want DeadlineExceeded", err)
	}
	wf.Steps[0] = workflow.Step{Name: "s", Run: func(context.Context, []byte) ([]byte, error) { panic("bad") }}
	if _, err := workflow.RunInline(context.Background(), wf, workflow.Job{}); err == nil {
		t.Fatal("a panicking step must fail, not crash")
	}
}

func TestRunInlineValidates(t *testing.T) {
	if _, err := workflow.RunInline(context.Background(), workflow.Workflow{Name: "w"}, workflow.Job{}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("an invalid workflow: %v", err)
	}
	wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "s", Run: noop}}}
	if _, err := workflow.RunInline(context.Background(), wf, workflow.Job{Workflow: "other"}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("a job naming another workflow: %v", err)
	}
}

// RunInline honours DiscardOnExhaust like a driver: every way the policy
// stops retrying ends the call discarded without OnFailure when the flag is
// on, and failed with OnFailure when it is off.
func TestRunInlineDiscardOnExhaust(t *testing.T) {
	boom := errors.New("boom")
	cases := []struct {
		name string
		err  error
		pol  workflow.RetryPolicy
	}{
		{"Max exhausted", boom, workflow.RetryPolicy{Max: 2}},
		{"Permanent", workflow.Permanent(boom), workflow.RetryPolicy{Max: 2}},
		{"NoRetryOn", boom, workflow.RetryPolicy{Max: 2, NoRetryOn: func(error) bool { return true }}},
		{"MaxErrors", boom, workflow.RetryPolicy{Max: 5, MaxErrors: 2}},
	}
	for _, c := range cases {
		for _, on := range []bool{true, false} {
			t.Run(c.name, func(t *testing.T) {
				pol := c.pol
				pol.DiscardOnExhaust = on
				failures := 0
				wf := workflow.Workflow{
					Name:      "w",
					Steps:     []workflow.Step{{Name: "s", Retry: &pol, Run: func(context.Context, []byte) ([]byte, error) { return nil, c.err }}},
					OnFailure: func(context.Context, []byte) ([]byte, error) { failures++; return nil, nil },
				}
				_, err := workflow.RunInline(context.Background(), wf, workflow.Job{}, workflow.InlineSleep(func(context.Context, time.Duration) error { return nil }))
				if got := errors.Is(err, workflow.ErrDiscarded); got != on {
					t.Fatalf("flag %v: ErrDiscarded = %v (%v)", on, got, err)
				}
				if wantFail := 1; !on && failures != wantFail || on && failures != 0 {
					t.Fatalf("flag %v: OnFailure ran %d times", on, failures)
				}
			})
		}
	}
}
