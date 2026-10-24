package otel_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
	wfotel "github.com/ubgo/workflow/otel"
)

// TestDiscardedOutcomeAndRunStatus runs a discarding and a succeeding
// workflow on the memory driver with the step middleware and the
// run-finished observer, and checks the discarded step outcome and the
// workflow.run.status values of the run-end counter.
func TestDiscardedOutcomeAndRunStatus(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c := memory.New(memory.WithMiddleware(h.inst.Step()))
	for _, wf := range []workflow.Workflow{
		{Name: "drop", Steps: []workflow.Step{{Name: "a", Run: func(context.Context, []byte) ([]byte, error) {
			return nil, workflow.Discard(errors.New("stale"))
		}}}},
		{Name: "ok", Steps: []workflow.Step{{Name: "a", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}},
	} {
		if err := c.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	observe := h.inst.RunFinished()
	ended := make(chan struct{}, 2)
	if err := c.OnRunFinished(func(ctx context.Context, ev workflow.RunEvent) {
		observe(ctx, ev)
		ended <- struct{}{}
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"drop", "ok"} {
		if _, err := c.Enqueue(ctx, workflow.Job{Workflow: name}); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		select {
		case <-ended:
		case <-ctx.Done():
			t.Fatal("runs did not end")
		}
	}

	var rm metricdata.ResourceMetrics
	if err := h.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	outcomes := map[string]int64{}
	runEnds := map[string]string{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			d, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, p := range d.DataPoints {
				switch m.Name {
				case wfotel.MetricStepOutcomes:
					o, _ := p.Attributes.Value(wfotel.AttrOutcome)
					outcomes[o.AsString()] += p.Value
				case wfotel.MetricRunsFinished:
					wf, _ := p.Attributes.Value(wfotel.AttrWorkflow)
					st, _ := p.Attributes.Value(wfotel.AttrRunStatus)
					runEnds[wf.AsString()] = st.AsString()
				}
			}
		}
	}
	if outcomes[string(wfotel.OutcomeDiscarded)] != 1 || outcomes[string(wfotel.OutcomeOK)] != 1 || len(outcomes) != 2 {
		t.Fatalf("step outcomes = %v", outcomes)
	}
	if runEnds["drop"] != string(workflow.StatusDiscarded) || runEnds["ok"] != string(workflow.StatusSucceeded) {
		t.Fatalf("run ends = %v", runEnds)
	}
}
