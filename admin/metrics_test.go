package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

func TestMemoryMetrics_Series(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)
	clk := clock.NewFake(t0.Add(time.Hour))
	m := NewMemoryMetrics(MemoryMetricsConfig{Clock: clk})
	ev := func(id string, wf string, st workflow.Status, at time.Time) workflow.RunEvent {
		return workflow.RunEvent{Ref: workflow.RunRef{RunID: workflow.RunID(id)}, Workflow: wf, Status: st, FinishedAt: at}
	}
	m.Observe(ctx, ev("a", "send", workflow.StatusSucceeded, t0.Add(30*time.Second)))
	m.Observe(ctx, ev("a", "send", workflow.StatusSucceeded, t0.Add(30*time.Second))) // redelivery
	m.Observe(ctx, ev("b", "send", workflow.StatusFailed, t0.Add(90*time.Second)))
	m.Observe(ctx, ev("c", "report", workflow.StatusCancelled, t0.Add(5*time.Minute)))
	m.Observe(ctx, ev("d", "report", workflow.StatusDiscarded, t0.Add(5*time.Minute)))
	m.Observe(ctx, ev("e", "report", workflow.StatusRunning, t0)) // not an end

	s, err := m.Series(ctx, MetricsQuery{From: t0, To: t0.Add(10 * time.Minute), Step: 2 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Points) != 5 || s.Step != 2*time.Minute {
		t.Fatalf("points %d step %s", len(s.Points), s.Step)
	}
	if p := s.Points[0]; p.Succeeded != 1 || p.Failed != 1 || p.Total() != 2 || !p.At.Equal(t0) {
		t.Errorf("point 0 = %+v", p)
	}
	if p := s.Points[2]; p.Cancelled != 1 || p.Discarded != 1 {
		t.Errorf("point 2 = %+v", p)
	}
	if p := s.Points[1]; p.Total() != 0 {
		t.Errorf("empty point = %+v", p)
	}
	// Ends older than the source's start move Since back to cover them.
	if !s.Since.Equal(t0.Add(30 * time.Second)) {
		t.Errorf("since = %s, want the earliest end", s.Since)
	}

	// A workflow filter, and a step finer than the bucket rounded up.
	m.Observe(ctx, ev("f", "report", workflow.StatusSucceeded, t0.Add(40*time.Second)))
	s, _ = m.Series(ctx, MetricsQuery{From: t0, To: t0.Add(3 * time.Minute), Step: time.Second, Workflow: "send"})
	if s.Step != time.Minute || len(s.Points) != 3 || s.Points[0].Succeeded != 1 || s.Points[1].Failed != 1 {
		t.Errorf("filtered = %+v", s)
	}

	// Counts older than Keep are dropped.
	clk.Advance(DefaultMetricsKeep)
	s, _ = m.Series(ctx, MetricsQuery{From: t0, To: t0.Add(10 * time.Minute), Step: 10 * time.Minute})
	if s.Points[0].Total() != 0 {
		t.Errorf("kept past Keep: %+v", s.Points[0])
	}
	if !s.Since.Equal(clk.Now().Add(-DefaultMetricsKeep)) {
		t.Errorf("since = %s", s.Since)
	}
}

func TestMemoryMetrics_DedupeBounded(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)
	m := NewMemoryMetrics(MemoryMetricsConfig{Dedupe: 2, Clock: clock.NewFake(t0)})
	e := func(id string) workflow.RunEvent {
		return workflow.RunEvent{Ref: workflow.RunRef{RunID: workflow.RunID(id)}, Status: workflow.StatusSucceeded, FinishedAt: t0}
	}
	m.Observe(ctx, e("a"))
	m.Observe(ctx, e("b"))
	m.Observe(ctx, e("a")) // still remembered
	m.Observe(ctx, e("c")) // pushes a out
	m.Observe(ctx, e("a")) // counted again
	if len(m.seen) != 2 || len(m.ring) != 2 {
		t.Errorf("dedupe holds %d/%d, want 2", len(m.seen), len(m.ring))
	}
	s, _ := m.Series(ctx, MetricsQuery{From: t0, To: t0.Add(time.Minute), Step: time.Minute})
	if s.Points[0].Succeeded != 4 {
		t.Errorf("succeeded = %d, want 4", s.Points[0].Succeeded)
	}
}

func TestValidateMetricsQuery(t *testing.T) {
	t0 := time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)
	for name, q := range map[string]MetricsQuery{
		"no from":     {To: t0, Step: time.Minute},
		"reversed":    {From: t0, To: t0, Step: time.Minute},
		"zero step":   {From: t0, To: t0.Add(time.Hour)},
		"too many":    {From: t0, To: t0.Add(MaxMetricsPoints*time.Minute + time.Second), Step: time.Minute},
		"neg step":    {From: t0, To: t0.Add(time.Hour), Step: -time.Minute},
		"many points": {From: t0, To: t0.Add(365 * 24 * time.Hour), Step: time.Minute},
	} {
		if err := ValidateMetricsQuery(q); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("%s: %v, want ErrInvalid", name, err)
		}
	}
	if err := ValidateMetricsQuery(MetricsQuery{From: t0, To: t0.Add(MaxMetricsPoints * time.Minute), Step: time.Minute}); err != nil {
		t.Errorf("exactly the max: %v", err)
	}
}

func TestHandler_Metrics(t *testing.T) {
	t0 := time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)
	clk := clock.NewFake(t0.Add(time.Hour))
	m := NewMemoryMetrics(MemoryMetricsConfig{Clock: clk})
	m.Observe(context.Background(), workflow.RunEvent{Ref: workflow.RunRef{RunID: "a"}, Workflow: "w", Status: workflow.StatusFailed, FinishedAt: t0.Add(50 * time.Minute)})

	// Without WithMetrics the route is 501 and capabilities says so.
	h := NewHandler(New(seeded()), bearerAuth())
	if rec := do(t, h, "GET", "/metrics", ""); rec.Code != http.StatusNotImplemented {
		t.Errorf("no source = %d", rec.Code)
	}

	var seen *MetricsQuery
	auth := bearerAuth()
	auth.Authorize = func(_ context.Context, req Request[user]) error {
		if req.Action == ActionMetrics {
			seen = req.Resource.Metrics
		}
		return nil
	}
	h = NewHandler(New(seeded(), WithClock(clk), WithMetrics(m)), auth)
	rec := do(t, h, "GET", "/metrics?window=1h&step=10m&workflow=w", "")
	var out struct {
		StepSeconds float64            `json:"step_seconds"`
		Points      []metricsPointJSON `json:"points"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil || rec.Code != 200 {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
	if out.StepSeconds != 600 || len(out.Points) != 6 || out.Points[5].Failed != 1 {
		t.Errorf("metrics = %+v", out)
	}
	if seen == nil || seen.Workflow != "w" || !seen.From.Equal(t0) {
		t.Errorf("Authorizer saw %+v", seen)
	}
	// Defaults: the last hour in 60 steps.
	rec = do(t, h, "GET", "/metrics", "")
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	if out.StepSeconds != 60 || len(out.Points) != 60 {
		t.Errorf("default = %v steps of %v", len(out.Points), out.StepSeconds)
	}
	for _, bad := range []string{"/metrics?step=x", "/metrics?window=-1h", "/metrics?from=2026-10-14T09:00:00Z&window=1h", "/metrics?window=8760h&step=1s", "/metrics?to=nope"} {
		if rec := do(t, h, "GET", bad, ""); rec.Code != http.StatusBadRequest {
			t.Errorf("%s = %d, want 400", bad, rec.Code)
		}
	}
	rec = do(t, h, "GET", "/capabilities", "")
	var c struct {
		Metrics bool `json:"metrics"`
	}
	if _ = json.Unmarshal(rec.Body.Bytes(), &c); !c.Metrics {
		t.Errorf("capabilities metrics = false: %s", rec.Body)
	}
}

// TestActions_ReadOnlyExact pins which actions are read-only: an
// Authorizer that gives a viewer role ReadOnly() must never let it write,
// and must let it see the charts.
func TestActions_ReadOnlyExact(t *testing.T) {
	want := map[Action]bool{
		ActionCapabilities: true, ActionListRuns: true, ActionShowRun: true, ActionListFailures: true, ActionListDiscarded: true,
		ActionListPauses: true, ActionListSchedules: true, ActionQueueStats: true, ActionShowSchedule: true, ActionShowOutput: true,
		ActionUI: true, ActionMetrics: true, ActionListNodes: true, ActionHealth: true,
	}
	for _, a := range Actions {
		if a.ReadOnly() != want[a] {
			t.Errorf("%s ReadOnly = %v, want %v", a, a.ReadOnly(), want[a])
		}
	}
}
