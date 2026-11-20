package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// TestMapEachIsSequentialByDefault proves WP-19 (sequential
// multi-instance): without MapConcurrency, MapEach runs one item at a
// time, in order.
func TestMapEachIsSequentialByDefault(t *testing.T) {
	var inFlight, peak atomic.Int32
	var order []int
	wf := workflow.Workflow{Name: "seq", Steps: []workflow.Step{{Name: "each", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		out, err := workflow.MapEach(ctx, "items", []int{1, 2, 3, 4}, func(_ context.Context, i int, item int) (int, error) {
			if n := inFlight.Add(1); n > peak.Load() {
				peak.Store(n)
			}
			time.Sleep(2 * time.Millisecond)
			order = append(order, item)
			inFlight.Add(-1)
			return item * 10, nil
		})
		if err != nil {
			return nil, err
		}
		return json.Marshal(out)
	}}}}
	c := flowClient(t, workflow.NewMemoryJournal(), wf)
	out, err := workflow.AwaitResult(awaitCtx(t), c, enqueue(t, c, workflow.Job{Workflow: "seq"}), fastAwait)
	if err != nil || string(out) != "[10,20,30,40]" || peak.Load() != 1 || len(order) != 4 || order[0] != 1 || order[3] != 4 {
		t.Fatalf("out %s err %v peak %d order %v", out, err, peak.Load(), order)
	}
}

// TestUpstreamLimitOutsideJobs proves FC-16 (a shared limiter outside
// jobs) and AI-02 (weighted limits): a request handler takes from the same
// cost-based bucket a step would, with no job involved.
func TestUpstreamLimitOutsideJobs(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC))
	limit, err := workflow.NewUpstreamLimit(nil, workflow.BucketSpec{Capacity: 1000, LeakPerSecond: 100}, clk)
	if err != nil {
		t.Fatal(err)
	}
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The cost is the tokens the call will spend, not one per call.
		if err := limit.Take(r.Context(), "llm-key", 600); errors.Is(err, workflow.ErrThrottled) {
			http.Error(w, "slow down", http.StatusTooManyRequests)
			return
		} else if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	codes := func() int {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/complete", nil))
		return rec.Code
	}
	if a, b := codes(), codes(); a != http.StatusNoContent || b != http.StatusTooManyRequests {
		t.Fatalf("codes %d %d, want 204 then 429 (600 + 600 tokens > 1000)", a, b)
	}
	clk.Advance(6 * time.Second) // 600 tokens leaked
	if c := codes(); c != http.StatusNoContent {
		t.Fatalf("after the leak: %d", c)
	}
}
