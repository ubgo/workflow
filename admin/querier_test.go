package admin

import (
	"context"
	"errors"
	"testing"

	"github.com/ubgo/workflow"
)

// history is a RunQuerier standing for a run recorder's store: it holds
// its own runs, which the client may no longer have.
func history(runs ...workflow.RunInfo) *fakeClient {
	return &fakeClient{caps: workflow.CapRunQuery | workflow.CapLabelPrefix, runs: runs}
}

func TestWithRunQuerierReadsTheQuerier(t *testing.T) {
	ctx := context.Background()
	rec := history(
		workflow.RunInfo{Ref: workflow.RunRef{RunID: "old"}, Workflow: "w", Status: workflow.StatusFailed, Queue: "q"},
		workflow.RunInfo{Ref: workflow.RunRef{RunID: "queued"}, Workflow: "w", Status: workflow.StatusQueued, Queue: "q"},
	)
	// A client that cannot list runs at all: the admin still can, from
	// the querier.
	c := &fakeClient{caps: workflow.CapCancel}
	a := New(c, WithRunQuerier(rec, rec.caps))
	for _, op := range []Operation{OpRuns, OpRun, OpFailures, OpQueues} {
		if !a.Supports(op) {
			t.Errorf("%s unsupported with a querier", op)
		}
	}
	if a.Supports(OpDiscarded) {
		t.Error("discarded supported though the client lacks CapRunStates")
	}
	if a.Supports(OpRetryAll) {
		t.Error("retry_all must still need the client's own run query")
	}
	page, err := a.Runs(ctx, workflow.RunQuery{})
	if err != nil || len(page.Runs) != 2 {
		t.Fatalf("Runs = %+v, %v", page, err)
	}
	fails, err := a.Failures(ctx, workflow.RunQuery{})
	if err != nil || len(fails.Runs) != 1 || fails.Runs[0].Ref.RunID != "old" {
		t.Fatalf("Failures = %+v, %v", fails, err)
	}
	if info, err := a.Run(ctx, workflow.RunRef{RunID: "old"}); err != nil || info.Status != workflow.StatusFailed {
		t.Fatalf("Run = %+v, %v", info, err)
	}
	cnt, err := a.CountRuns(ctx, workflow.RunQuery{Limit: 1})
	if err != nil || cnt.Total != 2 {
		t.Fatalf("CountRuns = %+v, %v", cnt, err)
	}
	qs, err := a.QueueStats(ctx)
	if err != nil || len(qs.Queues) != 1 || qs.Queues[0].Depth != 1 {
		t.Fatalf("QueueStats = %+v, %v", qs, err)
	}
	// The querier's own filters decide: label prefixes pass here...
	if _, err := a.Runs(ctx, workflow.RunQuery{LabelPrefixes: map[string]string{"k": "v"}}); err != nil {
		t.Fatalf("label prefix on a querier that has it: %v", err)
	}
	// ...and tenant does not.
	if _, err := a.Runs(ctx, workflow.RunQuery{Tenant: "acme"}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("tenant on a querier without it: %v", err)
	}
	if len(c.queries) != 0 {
		t.Fatalf("the client was asked: %+v", c.queries)
	}
}

func TestWithRunQuerierRunFallsBackToClient(t *testing.T) {
	ctx := context.Background()
	rec := history(workflow.RunInfo{Ref: workflow.RunRef{RunID: "a"}, Workflow: "w", Status: workflow.StatusSucceeded})
	c := &fakeClient{caps: allCaps, runs: []workflow.RunInfo{{Ref: workflow.RunRef{RunID: "b"}, Workflow: "w", Status: workflow.StatusRunning}}}
	a := New(c, WithRunQuerier(rec, rec.caps))
	if info, err := a.Run(ctx, workflow.RunRef{RunID: "b"}); err != nil || info.Status != workflow.StatusRunning {
		t.Fatalf("fallback Run = %+v, %v", info, err)
	}
	if _, err := a.Run(ctx, workflow.RunRef{RunID: "zzz"}); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("unknown run = %v", err)
	}
	// A client without a run query is not asked.
	a2 := New(&fakeClient{caps: workflow.CapCancel}, WithRunQuerier(rec, rec.caps))
	if _, err := a2.Run(ctx, workflow.RunRef{RunID: "b"}); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("no fallback client = %v", err)
	}
	// Bulk actions list on the client and check the client's filters,
	// whatever the querier holds.
	res, err := a.RetryAll(ctx, workflow.RunQuery{Statuses: []workflow.Status{workflow.StatusFailed}}, BulkOptions{})
	if err != nil || res.Matched != 0 {
		t.Fatalf("RetryAll over the client = %+v, %v", res, err)
	}
}

func TestWithRunQuerierNilKeepsClient(t *testing.T) {
	c := &fakeClient{caps: allCaps, runs: []workflow.RunInfo{{Ref: workflow.RunRef{RunID: "x"}, Workflow: "w", Status: workflow.StatusQueued}}}
	a := New(c, WithRunQuerier(nil, workflow.CapRunQuery))
	page, err := a.Runs(context.Background(), workflow.RunQuery{})
	if err != nil || len(page.Runs) != 1 || len(c.queries) != 1 {
		t.Fatalf("Runs = %+v, %v (client queries %d)", page, err, len(c.queries))
	}
}
