package admin

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

func TestSupport(t *testing.T) {
	a := New(&fakeClient{caps: workflow.CapRunQuery | workflow.CapPauseQueue | workflow.CapCancel})
	got := map[Operation]OperationSupport{}
	for _, s := range a.Support() {
		got[s.Operation] = s
	}
	if len(got) != len(Operations) {
		t.Fatalf("Support covers %d operations, want %d", len(got), len(Operations))
	}
	for op, want := range map[Operation]bool{OpRuns: true, OpQueues: true, OpCancel: true, OpPause: true, OpRetry: false, OpSchedules: false} {
		if got[op].Supported != want {
			t.Errorf("%s supported = %v, want %v", op, got[op].Supported, want)
		}
	}
	if got[OpRetry].Missing != workflow.CapRetryRun {
		t.Errorf("retry missing = %v", got[OpRetry].Missing)
	}

	// A queue-only pauser refuses a workflow pause, naming the capability.
	err := a.Pause(context.Background(), workflow.PauseWorkflow("w"))
	var ue *workflow.UnsupportedError
	if !errors.As(err, &ue) || ue.Missing != workflow.CapPauseWorkflow {
		t.Errorf("Pause(workflow) = %v", err)
	}
	if err := a.Pause(context.Background(), workflow.PauseQueue("q")); err != nil {
		t.Errorf("Pause(queue) = %v", err)
	}
}

// TestSupport_CapabilityWithoutInterface: a client that declares a
// capability but lacks the interface is refused, never type-asserted into
// a panic.
func TestSupport_CapabilityWithoutInterface(t *testing.T) {
	a := New(bareClient{caps: allCaps})
	for _, op := range Operations {
		if op == OpCancel || op == OpHealth {
			continue // Client.Cancel and CheckHealth need no optional interface
		}
		if a.Supports(op) {
			t.Errorf("%s supported without its interface", op)
		}
	}
	for _, s := range a.Support() {
		if !s.Supported && s.Missing == 0 && (Requirements(s.Operation) != 0 || s.Reason == "") {
			t.Errorf("%s unsupported with Missing zero and no reason", s.Operation)
		}
	}
	if _, err := a.Runs(context.Background(), workflow.RunQuery{}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("Runs = %v", err)
	}
}

func TestOperations_Validate(t *testing.T) {
	a := New(seeded())
	ctx := context.Background()
	if _, err := a.Failures(ctx, workflow.RunQuery{Statuses: []workflow.Status{workflow.StatusQueued}}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("Failures with statuses = %v", err)
	}
	if _, err := a.CancelAll(ctx, workflow.RunQuery{Limit: 5}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("CancelAll with Limit = %v", err)
	}
	if _, err := a.Run(ctx, workflow.RunRef{}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("Run(zero) = %v", err)
	}
	info, err := a.Run(ctx, workflow.RunRef{RunID: "r2"})
	if err != nil || info.FailedStep != "send" {
		t.Errorf("Run = %+v, %v", info, err)
	}
	page, err := a.Failures(ctx, workflow.RunQuery{})
	if err != nil || len(page.Runs) != 1 || page.Runs[0].Ref.RunID != "r2" {
		t.Errorf("Failures = %+v, %v", page, err)
	}
}

func queued(n int, queue string, start time.Time) []workflow.RunInfo {
	out := make([]workflow.RunInfo, 0, n)
	for i := n - 1; i >= 0; i-- { // newest first, as RunQuerier pages
		out = append(out, workflow.RunInfo{Ref: workflow.RunRef{RunID: workflow.RunID(fmt.Sprint(queue, i))}, Queue: queue,
			Status: workflow.StatusQueued, EnqueuedAt: start.Add(time.Duration(i) * time.Second)})
	}
	return out
}

func TestQueueStats(t *testing.T) {
	t0 := time.Date(2026, 10, 12, 9, 0, 0, 0, time.UTC)
	clk := clock.NewFake(t0.Add(time.Hour))
	f := &fakeClient{caps: allCaps}
	f.runs = append(queued(2500, "default", t0), queued(3, "emails", t0.Add(10*time.Minute))...)
	f.runs = append(f.runs, workflow.RunInfo{Ref: workflow.RunRef{RunID: "done"}, Queue: "emails", Status: workflow.StatusSucceeded, EnqueuedAt: t0})

	rep, err := New(f, WithClock(clk)).QueueStats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rep.Truncated || len(rep.Queues) != 2 {
		t.Fatalf("report = %+v", rep)
	}
	d, e := rep.Queues[0], rep.Queues[1]
	if d.Queue != "default" || d.Depth != 2500 || d.OldestWait != time.Hour || !d.OldestEnqueuedAt.Equal(t0) {
		t.Errorf("default = %+v", d)
	}
	if e.Queue != "emails" || e.Depth != 3 || e.OldestWait != 50*time.Minute {
		t.Errorf("emails = %+v", e)
	}
	if n := len(f.queries); n != 3 {
		t.Errorf("pages read = %d, want 3 (1000 each)", n)
	}

	f.queries = nil
	rep, err = New(f, WithClock(clk), WithMaxScan(1500)).QueueStats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Truncated || rep.Queues[0].Depth != 1500 || len(f.queries) != 2 {
		t.Errorf("truncated report = %+v after %d pages", rep, len(f.queries))
	}

	depths, err := New(f, WithClock(clk)).QueueDepths(context.Background())
	if err != nil || depths["default"] != 2500 || depths["emails"] != 3 {
		t.Errorf("QueueDepths = %v, %v", depths, err)
	}
}

// recorder collects alerts.
type recorder struct {
	mu     sync.Mutex
	alerts []Alert
}

func (r *recorder) Alert(_ context.Context, a Alert) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.alerts = append(r.alerts, a)
}

func (r *recorder) take() []Alert {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.alerts
	r.alerts = nil
	return out
}

func TestMonitor_Validation(t *testing.T) {
	a := New(seeded())
	rec := &recorder{}
	for name, ths := range map[string][]Threshold{
		"none":        nil,
		"negative":    {{MaxDepth: -1}},
		"empty":       {{Queue: "q"}},
		"bad queue":   {{Queue: "a b", MaxDepth: 1}},
		"dup queue":   {{Queue: "q", MaxDepth: 1}, {Queue: "q", MaxWait: time.Second}},
		"dup default": {{MaxDepth: 1}, {MaxWait: time.Second}},
	} {
		if _, err := NewMonitor(a, rec, ths); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("%s: err = %v, want ErrInvalid", name, err)
		}
	}
	if _, err := NewMonitor(a, nil, []Threshold{{MaxDepth: 1}}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("nil alerter: %v", err)
	}
	if _, err := NewMonitor(New(&fakeClient{}), rec, []Threshold{{MaxDepth: 1}}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("client without RunQuery: %v", err)
	}
}

func TestMonitor_CrossResolveRepeat(t *testing.T) {
	t0 := time.Date(2026, 10, 12, 9, 0, 0, 0, time.UTC)
	clk := clock.NewFake(t0)
	f := &fakeClient{caps: allCaps}
	rec := &recorder{}
	m, err := NewMonitor(New(f, WithClock(clk)), rec, []Threshold{
		{MaxDepth: 100}, // default for every queue
		{Queue: "emails", MaxDepth: 2, MaxWait: time.Minute}, // emails is stricter
	}, WithRepeat(10*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// emails: depth 3 > 2 and oldest wait 0; default: depth 50 < 100.
	f.runs = append(queued(3, "emails", t0), queued(50, "default", t0)...)
	_ = m.Check(ctx)
	got := rec.take()
	if len(got) != 1 || got[0].Kind != AlertQueueDepth || got[0].Queue != "emails" || got[0].Depth != 3 || got[0].Resolved {
		t.Fatalf("first poll = %+v", got)
	}

	// Still over, a minute and a half later: no repeat yet, but the wait
	// threshold is crossed now.
	clk.Advance(90 * time.Second)
	_ = m.Check(ctx)
	got = rec.take()
	if len(got) != 1 || got[0].Kind != AlertQueueWait || got[0].Wait != 90*time.Second {
		t.Fatalf("second poll = %+v", got)
	}

	// Past the repeat interval both re-fire.
	clk.Advance(10 * time.Minute)
	_ = m.Check(ctx)
	if got = rec.take(); len(got) != 2 {
		t.Fatalf("repeat poll = %+v", got)
	}

	// emails drains: both resolve, once.
	f.runs = queued(50, "default", t0)
	_ = m.Check(ctx)
	got = rec.take()
	if len(got) != 2 || !got[0].Resolved || !got[1].Resolved || got[0].Queue != "emails" {
		t.Fatalf("drain poll = %+v", got)
	}
	_ = m.Check(ctx)
	if got = rec.take(); len(got) != 0 {
		t.Fatalf("quiet poll = %+v", got)
	}
}

func TestMonitor_RunOnClockAndSurvivesPanicsAndErrors(t *testing.T) {
	t0 := time.Date(2026, 10, 12, 9, 0, 0, 0, time.UTC)
	clk := clock.NewFake(t0)
	f := &fakeClient{caps: allCaps, runs: queued(5, "default", t0)}
	var mu sync.Mutex
	var errs []error
	calls := 0
	alerter := AlerterFunc(func(context.Context, Alert) {
		mu.Lock()
		calls++
		mu.Unlock()
		panic("chat webhook down")
	})
	m, err := NewMonitor(New(f, WithClock(clk)), alerter, []Threshold{{MaxDepth: 1}},
		WithInterval(time.Minute), WithRepeat(time.Minute), WithErrorHandler(func(err error) {
			mu.Lock()
			errs = append(errs, err)
			mu.Unlock()
		}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()

	for i := range 3 {
		if err := clk.BlockUntil(ctx, 1); err != nil {
			t.Fatal(err)
		}
		if i == 1 {
			f.mu.Lock()
			f.err = errors.New("db blip")
			f.mu.Unlock()
		}
		if i == 2 {
			f.mu.Lock()
			f.err = nil
			f.mu.Unlock()
		}
		clk.Advance(time.Minute)
	}
	if err := clk.BlockUntil(ctx, 1); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("Run = %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	// Polls 1 and 3 alerted (and panicked); poll 2 failed.
	if calls != 2 || len(errs) != 3 {
		t.Errorf("alerter calls = %d, errors = %v", calls, errs)
	}
}
