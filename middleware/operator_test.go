package middleware_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

func TestUniqueByArgs(t *testing.T) {
	key := middleware.UniqueByArgs()
	a := key(workflow.Job{Workflow: "send", Payload: []byte(`{"to":"a","n":1}`)})
	b := key(workflow.Job{Workflow: "send", Payload: []byte(`{ "n":1, "to":"a" }`)})
	if a != b {
		t.Errorf("key order and whitespace changed the key: %q vs %q", a, b)
	}
	if !strings.HasPrefix(a, middleware.DefaultArgsKeyPrefix+"send:") {
		t.Errorf("key = %q, want the prefix and workflow", a)
	}
	for name, other := range map[string]workflow.Job{
		"other args":     {Workflow: "send", Payload: []byte(`{"to":"b","n":1}`)},
		"other workflow": {Workflow: "mail", Payload: []byte(`{"to":"a","n":1}`)},
		"number text":    {Workflow: "send", Payload: []byte(`{"to":"a","n":1.0}`)},
	} {
		if key(other) == a {
			t.Errorf("%s: same key %q", name, a)
		}
	}
	// A raw payload never collides with a JSON one.
	if key(workflow.Job{Workflow: "w", Payload: []byte("not json")}) == key(workflow.Job{Workflow: "w", Payload: []byte(`"not json"`)}) {
		t.Error("a raw and a JSON payload share a key")
	}
	if key(workflow.Job{Workflow: "w", Payload: []byte(`{"a":1} {"b":2}`)}) == key(workflow.Job{Workflow: "w", Payload: []byte(`{"a":1}`)}) {
		t.Error("trailing JSON values were ignored")
	}
}

func TestUniqueByArgsOptions(t *testing.T) {
	fields := middleware.UniqueByArgs(middleware.ArgsFields("to"))
	x := fields(workflow.Job{Workflow: "w", Payload: []byte(`{"to":"a","at":1}`)})
	y := fields(workflow.Job{Workflow: "w", Payload: []byte(`{"to":"a","at":2}`)})
	if x != y {
		t.Error("ArgsFields: a field that does not count changed the key")
	}
	if x == fields(workflow.Job{Workflow: "w", Payload: []byte(`{"to":"b","at":1}`)}) {
		t.Error("ArgsFields: the counted field did not change the key")
	}
	if fields(workflow.Job{Workflow: "w", Payload: []byte(`{"at":1}`)}) == fields(workflow.Job{Workflow: "w", Payload: []byte(`{"to":null,"at":1}`)}) {
		t.Error("ArgsFields: a missing field and a null one share a key")
	}
	// A payload that is not an object cannot be cut: its whole bytes count.
	if fields(workflow.Job{Workflow: "w", Payload: []byte(`[1]`)}) == fields(workflow.Job{Workflow: "w", Payload: []byte(`[2]`)}) {
		t.Error("ArgsFields on an array: different payloads share a key")
	}
	q := middleware.UniqueByArgs(middleware.ArgsQueue())
	if q(workflow.Job{Workflow: "w", Queue: "a"}) == q(workflow.Job{Workflow: "w", Queue: "b"}) {
		t.Error("ArgsQueue: the queue did not count")
	}
	if middleware.UniqueByArgs()(workflow.Job{Workflow: "w", Queue: "a"}) != middleware.UniqueByArgs()(workflow.Job{Workflow: "w", Queue: "b"}) {
		t.Error("without ArgsQueue the queue counted")
	}
	l := middleware.UniqueByArgs(middleware.ArgsLabels("tenant"))
	if l(workflow.Job{Workflow: "w", Labels: map[string]string{"tenant": "1"}}) == l(workflow.Job{Workflow: "w", Labels: map[string]string{"tenant": "2"}}) {
		t.Error("ArgsLabels: the label did not count")
	}
	if l(workflow.Job{Workflow: "w"}) == l(workflow.Job{Workflow: "w", Labels: map[string]string{"tenant": ""}}) {
		t.Error("ArgsLabels: a missing label and an empty one share a key")
	}
	if p := middleware.UniqueByArgs(middleware.ArgsKeyPrefix("u/"))(workflow.Job{Workflow: "w"}); !strings.HasPrefix(p, "u/w:") {
		t.Errorf("ArgsKeyPrefix: key %q", p)
	}
}

func TestUniqueByArgsRefusesDuplicate(t *testing.T) {
	store := middleware.NewMemoryUnique(clock.NewFake(time.Unix(0, 0)))
	mw := middleware.Unique(middleware.UniqueByArgs(), store, time.Hour)
	enq := mw(func(context.Context, workflow.Job) (workflow.RunRef, error) { return workflow.RunRef{RunID: "r"}, nil })
	ctx := context.Background()
	if _, err := enq(ctx, workflow.Job{Workflow: "w", Payload: []byte(`{"a":1,"b":2}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := enq(ctx, workflow.Job{Workflow: "w", Payload: []byte(`{"b":2,"a":1}`)}); !errors.Is(err, workflow.ErrDuplicate) {
		t.Errorf("same args reordered: %v, want ErrDuplicate", err)
	}
	if _, err := enq(ctx, workflow.Job{Workflow: "w", Payload: []byte(`{"a":2}`)}); err != nil {
		t.Errorf("other args: %v, want accepted", err)
	}
}

// alertRec records alerts.
type alertRec struct {
	mu  sync.Mutex
	got []middleware.Alert
	err error
}

func (r *alertRec) Alert(_ context.Context, a middleware.Alert) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.got = append(r.got, a)
	return r.err
}

func (r *alertRec) n() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.got)
}

func failed(wf, step string) workflow.RunEvent {
	return workflow.RunEvent{Ref: workflow.RunRef{RunID: "r"}, Workflow: wf, Step: step, Status: workflow.StatusFailed, Err: errors.New("boom")}
}

func TestAlertsDecision(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	var rec alertRec
	obs := middleware.Alerts(&rec, middleware.AlertConfig{Clock: clk})
	ctx := context.Background()
	obs(ctx, failed("w", "s"))
	obs(ctx, failed("w", "s")) // same key within DefaultAlertDedup: held back
	obs(ctx, failed("w", "other"))
	obs(ctx, workflow.RunEvent{Workflow: "w", Status: workflow.StatusSucceeded})
	obs(ctx, workflow.RunEvent{Workflow: "w", Status: workflow.StatusDiscarded})
	if rec.n() != 2 {
		t.Fatalf("alerts = %+v, want 2 (one per key; no success, no discard)", rec.got)
	}
	clk.Advance(middleware.DefaultAlertDedup)
	obs(ctx, failed("w", "s"))
	if rec.n() != 3 {
		t.Fatalf("after the dedup window: %d alerts, want 3", rec.n())
	}
	last := rec.got[2]
	if last.Suppressed != 1 || last.Key != "w/s" || !last.Final || last.Step != "s" {
		t.Errorf("alert = %+v, want key w/s with 1 suppressed", last)
	}
}

func TestAlertsOptions(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	var rec alertRec
	obs := middleware.Alerts(&rec, middleware.AlertConfig{
		Clock:     clk,
		Discarded: true,
		When:      func(f workflow.Failure) bool { return f.Workflow != "quiet" },
		Key:       func(f workflow.Failure) string { return f.Workflow },
		Limiter:   middleware.NewMemoryAlertLimiter(clk, middleware.AlertLimits{}),
	})
	ctx := context.Background()
	obs(ctx, workflow.RunEvent{Workflow: "w", Status: workflow.StatusDiscarded})
	obs(ctx, failed("w", "s"))
	obs(ctx, failed("quiet", "s"))
	if rec.n() != 2 || rec.got[0].Key != "w" {
		t.Errorf("alerts = %+v, want the discard and the failure, keyed by workflow, quiet filtered", rec.got)
	}
}

// errLimiter always fails.
type errLimiter struct{}

func (errLimiter) Allow(context.Context, string, time.Time) (bool, int, error) {
	return false, 0, errors.New("store down")
}

func TestAlertsLimiterErrorStillAlerts(t *testing.T) {
	var rec alertRec
	middleware.Alerts(&rec, middleware.AlertConfig{Limiter: errLimiter{}})(context.Background(), failed("w", "s"))
	if rec.n() != 1 {
		t.Error("a failing limiter hid the alert")
	}
}

func TestMemoryAlertLimiterRateCap(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	lim := middleware.NewMemoryAlertLimiter(clk, middleware.AlertLimits{Max: 2, Per: time.Minute})
	ctx := context.Background()
	allow := func(key string) bool {
		ok, _, err := lim.Allow(ctx, key, clk.Now())
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}
	if !allow("a") || !allow("b") {
		t.Fatal("the first two alerts were held back")
	}
	if allow("c") {
		t.Error("a third alert within Per passed a cap of 2")
	}
	clk.Advance(time.Minute)
	ok, suppressed, _ := lim.Allow(ctx, "c", clk.Now())
	if !ok || suppressed != 1 {
		t.Errorf("after Per: ok %v suppressed %d, want true and 1", ok, suppressed)
	}
}

func TestReportPlugin(t *testing.T) {
	var mu sync.Mutex
	var got []workflow.Failure
	rep := workflow.ReporterFunc(func(_ context.Context, f workflow.Failure) {
		mu.Lock()
		got = append(got, f)
		mu.Unlock()
	})
	clk := clock.NewFake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	mem := memory.New(memory.WithClock(clk))
	c, err := workflow.Use(mem, middleware.Report(rep, middleware.ReportConfig{Attempts: true, Clock: clk}))
	if err != nil {
		t.Fatal(err)
	}
	wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) {
		return nil, workflow.Permanent(errors.New("boom"))
	}}}}
	if err := c.Register(wf); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "w", Payload: []byte(`"secret"`)})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n >= 2 || time.Now().After(deadline) {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("reports = %+v, want the attempt and the final failure", got)
	}
	attempt, final := got[0], got[1]
	if attempt.Final || attempt.Attempt != 1 || attempt.Step != "s" || attempt.Ref.RunID != ref.RunID || attempt.Status != workflow.StatusRetrying {
		t.Errorf("attempt report = %+v", attempt)
	}
	if !final.Final || final.Status != workflow.StatusFailed || final.Step != "s" || final.Ref.RunID != ref.RunID {
		t.Errorf("final report = %+v", final)
	}
}

func TestReportAttemptsSkipsRelease(t *testing.T) {
	n := 0
	rep := workflow.ReporterFunc(func(context.Context, workflow.Failure) { n++ })
	h := middleware.ReportAttempts(rep, middleware.ReportConfig{})(func(context.Context, []byte) ([]byte, error) {
		return nil, workflow.RetryAfter(middleware.ErrReleased, time.Second)
	})
	if _, err := h(context.Background(), nil); err == nil {
		t.Fatal("the release error was swallowed")
	}
	if n != 0 {
		t.Error("a release was reported as a failure")
	}
}

func TestRecoverCarriesStack(t *testing.T) {
	h := middleware.Recover(nil)(func(context.Context, []byte) ([]byte, error) { panic("kaboom") })
	_, err := h(context.Background(), nil)
	if !errors.Is(err, middleware.ErrPanic) || !strings.Contains(err.Error(), "kaboom") {
		t.Fatalf("err = %v, want ErrPanic naming the value", err)
	}
	if strings.Contains(err.Error(), "goroutine") {
		t.Error("the stack leaked into the message")
	}
	if st := workflow.StackOf(err); !strings.Contains(st, "goroutine") {
		t.Errorf("StackOf = %q, want the stack", st)
	}
}
