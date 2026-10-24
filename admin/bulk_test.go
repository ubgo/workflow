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
)

// failedRuns is a fake client with n failed runs f1..fn, newest first.
func failedRuns(n int) *fakeClient {
	t0 := time.Date(2026, 10, 12, 9, 0, 0, 0, time.UTC)
	f := &fakeClient{caps: allCaps}
	for i := n; i >= 1; i-- {
		f.runs = append(f.runs, workflow.RunInfo{Ref: workflow.RunRef{RunID: workflow.RunID(fmt.Sprintf("f%d", i))}, Workflow: "w", Queue: "default", Status: workflow.StatusFailed, EnqueuedAt: t0.Add(time.Duration(i) * time.Second)})
	}
	f.runs = append(f.runs, workflow.RunInfo{Ref: workflow.RunRef{RunID: "ok1"}, Workflow: "w", Status: workflow.StatusSucceeded, EnqueuedAt: t0})
	return f
}

// acted is the run IDs the fake was asked to act on with verb.
func acted(f *fakeClient, verb string) []string {
	var out []string
	for _, c := range f.Calls() {
		if id, ok := strings.CutPrefix(c, verb+" "); ok {
			out = append(out, id)
		}
	}
	return out
}

func TestRetryAll(t *testing.T) {
	ctx := context.Background()
	f := failedRuns(5)
	f.refErr = map[workflow.RunID]error{
		"f4": fmt.Errorf("moved on: %w", workflow.ErrWrongState),
		"f2": workflow.ErrDuplicate,
	}
	a := New(f)

	// A dry run lists and changes nothing.
	res, err := a.RetryAll(ctx, workflow.RunQuery{}, BulkOptions{DryRun: true})
	if err != nil || res.Matched != 5 || res.Done != 0 || len(res.Sample) != 5 || res.Sample[0].RunID != "f5" {
		t.Fatalf("dry run = %+v, %v", res, err)
	}
	if got := acted(f, "Retry"); len(got) != 0 {
		t.Fatalf("dry run retried %v", got)
	}

	// Only failed runs are listed, and runs that changed are skipped.
	res, err = a.RetryAll(ctx, workflow.RunQuery{Workflow: "w"}, BulkOptions{})
	if err != nil || res.Matched != 5 || res.Done != 3 || res.Skipped != 2 || res.Truncated {
		t.Fatalf("retry = %+v, %v", res, err)
	}
	if got := acted(f, "Retry"); !slices.Equal(got, []string{"f5", "f4", "f3", "f2", "f1"}) {
		t.Errorf("retried %v", got)
	}
	if q := f.queries[len(f.queries)-1]; !slices.Equal(q.Statuses, []workflow.Status{workflow.StatusFailed}) {
		t.Errorf("listed statuses %v, want [failed]", q.Statuses)
	}

	// Any other status is refused.
	if _, err := a.RetryAll(ctx, workflow.RunQuery{Statuses: []workflow.Status{workflow.StatusSucceeded}}, BulkOptions{}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("retry succeeded = %v, want ErrInvalid", err)
	}
}

func TestBulk_BoundAndTruncated(t *testing.T) {
	ctx := context.Background()
	f := failedRuns(7)
	a := New(f, WithMaxBulk(4))
	res, err := a.RetryAll(ctx, workflow.RunQuery{}, BulkOptions{})
	if err != nil || res.Matched != 4 || res.Done != 4 || !res.Truncated {
		t.Fatalf("bounded = %+v, %v", res, err)
	}
	// A per-call Max lowers the bound but never raises it.
	res, _ = a.RetryAll(ctx, workflow.RunQuery{}, BulkOptions{DryRun: true, Max: 2})
	if res.Matched != 2 || !res.Truncated {
		t.Errorf("max 2 = %+v", res)
	}
	res, _ = a.RetryAll(ctx, workflow.RunQuery{}, BulkOptions{DryRun: true, Max: 100})
	if res.Matched != 4 {
		t.Errorf("max 100 over bound 4 = %+v", res)
	}
	// Exactly the bound matching is not truncated.
	res, _ = New(failedRuns(4), WithMaxBulk(4)).RetryAll(ctx, workflow.RunQuery{}, BulkOptions{DryRun: true})
	if res.Matched != 4 || res.Truncated {
		t.Errorf("exact bound = %+v", res)
	}
	if _, err := a.RetryAll(ctx, workflow.RunQuery{}, BulkOptions{Max: -1}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("negative max = %v", err)
	}
	if _, err := a.RetryAll(ctx, workflow.RunQuery{Limit: 5}, BulkOptions{}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("limit = %v", err)
	}
}

// TestBulk_StopsOnHardError: an error that is not a per-run change stops
// the call and is returned with the count so far.
func TestBulk_StopsOnHardError(t *testing.T) {
	f := failedRuns(4)
	down := errors.New("db down")
	f.refErr = map[workflow.RunID]error{"f3": down}
	res, err := New(f).RetryAll(context.Background(), workflow.RunQuery{}, BulkOptions{})
	if !errors.Is(err, down) || res.Done != 1 {
		t.Fatalf("hard error = %+v, %v", res, err)
	}
	if got := acted(f, "Retry"); !slices.Equal(got, []string{"f4", "f3"}) {
		t.Errorf("went on after the error: %v", got)
	}
}

func TestDeleteAll(t *testing.T) {
	ctx := context.Background()
	f := failedRuns(2)
	a := New(f)
	if _, err := a.DeleteAll(ctx, workflow.RunQuery{}, BulkOptions{}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("empty filter = %v, want ErrInvalid", err)
	}
	if _, err := a.DeleteAll(ctx, workflow.RunQuery{Statuses: []workflow.Status{workflow.StatusQueued}}, BulkOptions{}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("queued = %v, want ErrInvalid", err)
	}
	res, err := a.DeleteAll(ctx, workflow.RunQuery{Workflow: "w"}, BulkOptions{})
	if err != nil || res.Done != 3 {
		t.Fatalf("delete = %+v, %v", res, err)
	}
	if got := acted(f, "Delete"); !slices.Equal(got, []string{"f2", "f1", "ok1"}) {
		t.Errorf("deleted %v", got)
	}
	// Every terminal status by default; discarded with CapRunStates.
	q := f.queries[len(f.queries)-1]
	want := []workflow.Status{workflow.StatusSucceeded, workflow.StatusFailed, workflow.StatusCancelled, workflow.StatusDiscarded}
	if !slices.Equal(q.Statuses, want) {
		t.Errorf("listed %v, want %v", q.Statuses, want)
	}
	f2 := failedRuns(1)
	f2.caps = allCaps &^ workflow.CapRunStates
	_, _ = New(f2).DeleteAll(ctx, workflow.RunQuery{Queue: "default"}, BulkOptions{DryRun: true})
	if q := f2.queries[len(f2.queries)-1]; slices.Contains(q.Statuses, workflow.StatusDiscarded) {
		t.Errorf("discarded listed without CapRunStates: %v", q.Statuses)
	}
}

func TestBulk_Unsupported(t *testing.T) {
	ctx := context.Background()
	a := New(&fakeClient{caps: workflow.CapRetryRun | workflow.CapDeleteRun})
	if _, err := a.RetryAll(ctx, workflow.RunQuery{}, BulkOptions{}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("retry_all without RunQuery = %v", err)
	}
	if _, err := a.DeleteAll(ctx, workflow.RunQuery{Workflow: "w"}, BulkOptions{}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("delete_all without RunQuery = %v", err)
	}
	b := New(bareClient{caps: allCaps})
	if _, err := b.RetryAll(ctx, workflow.RunQuery{}, BulkOptions{}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("retry_all without interfaces = %v", err)
	}
}

func TestPreviewCancelAll(t *testing.T) {
	f := seeded()
	res, err := New(f).PreviewCancelAll(context.Background(), workflow.RunQuery{Queue: "emails"})
	if err != nil || res.Matched != 2 {
		t.Fatalf("preview = %+v, %v", res, err)
	}
	if c := f.Calls(); slices.Contains(c, "CancelAll") {
		t.Errorf("preview cancelled: %v", c)
	}
	if q := f.queries[len(f.queries)-1]; !slices.Equal(q.Statuses, workflow.CancelStatuses(workflow.RunQuery{})) {
		t.Errorf("preview listed %v", q.Statuses)
	}
	if _, err := New(f).PreviewCancelAll(context.Background(), workflow.RunQuery{Statuses: []workflow.Status{workflow.StatusFailed}}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("terminal status = %v", err)
	}
}

func TestHandler_Bulk(t *testing.T) {
	f := failedRuns(3)
	var dry []bool
	auth := bearerAuth()
	auth.Authorize = func(_ context.Context, req Request[user]) error {
		dry = append(dry, req.Resource.DryRun)
		if req.Resource.Query == nil {
			return errors.New("no query")
		}
		return nil
	}
	h := NewHandler(New(f), auth)
	var out bulkJSON
	rec := do(t, h, "POST", "/runs/retry", `{"workflow":"w","dry_run":true}`)
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil || rec.Code != http.StatusOK || !out.DryRun || out.Matched != 3 || out.Done != 0 || len(out.Sample) != 3 {
		t.Fatalf("dry retry = %d %s", rec.Code, rec.Body)
	}
	rec = do(t, h, "POST", "/runs/retry", `{"workflow":"w","max":2}`)
	out = bulkJSON{}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil || out.Done != 2 || !out.Truncated || out.DryRun {
		t.Fatalf("retry = %d %s", rec.Code, rec.Body)
	}
	if !slices.Equal(dry, []bool{true, false}) {
		t.Errorf("Authorizer saw DryRun %v", dry)
	}
	if rec := do(t, h, "POST", "/runs/delete", `{}`); rec.Code != http.StatusBadRequest {
		t.Errorf("unfiltered delete = %d %s", rec.Code, rec.Body)
	}
	if rec := do(t, h, "POST", "/runs/delete", `{"workflw":"w"}`); rec.Code != http.StatusBadRequest {
		t.Errorf("typo field = %d, want 400", rec.Code)
	}
	rec = do(t, h, "POST", "/runs/cancel", `{"queue":"default","dry_run":true}`)
	if rec.Code != http.StatusOK || slices.Contains(f.Calls(), "CancelAll") {
		t.Errorf("dry cancel = %d %s %v", rec.Code, rec.Body, f.Calls())
	}
}
