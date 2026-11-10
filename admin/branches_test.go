package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/ubgo/workflow"
)

var errStore = errors.New("store down")

func TestClientReturnsTheClient(t *testing.T) {
	c := &fakeClient{caps: allCaps}
	if got := New(c).Client(); got != c {
		t.Fatalf("Client() = %v, want the client New was given", got)
	}
}

// TestUnsupportedOperationsReachNoClient: every single-run and pause or
// schedule operation on a client with no capability refuses with an
// UnsupportedError naming the operation, before calling the client.
func TestUnsupportedOperationsReachNoClient(t *testing.T) {
	ctx := context.Background()
	ref := workflow.RunRef{RunID: "r1"}
	a := New(bareClient{})
	for _, tc := range []struct {
		op   Operation
		call func() error
	}{
		{OpRun, func() error { _, err := a.Run(ctx, ref); return err }},
		{OpRetry, func() error { _, err := a.Retry(ctx, ref); return err }},
		{OpDelete, func() error { return a.Delete(ctx, ref) }},
		{OpCancel, func() error { return a.Cancel(ctx, ref) }},
		{OpCancelAll, func() error { _, err := a.CancelAll(ctx, workflow.RunQuery{}); return err }},
		{OpPause, func() error { return a.Resume(ctx, workflow.PauseAll()) }},
		{OpPause, func() error { _, err := a.Paused(ctx); return err }},
		{OpSchedules, func() error { _, err := a.Schedules(ctx); return err }},
		{OpSchedules, func() error { _, err := a.Schedule(ctx, "s1"); return err }},
	} {
		err := tc.call()
		var ue *workflow.UnsupportedError
		if !errors.Is(err, workflow.ErrUnsupported) || !errors.As(err, &ue) || ue.Field != "admin."+string(tc.op) {
			t.Errorf("%s on a bare client = %v, want an UnsupportedError for admin.%s", tc.op, err, tc.op)
		}
	}
}

func TestRunErrors(t *testing.T) {
	ctx := context.Background()
	ref := workflow.RunRef{RunID: "r1"}
	if _, err := New(&fakeClient{caps: allCaps}).Run(ctx, workflow.RunRef{}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("empty ref = %v, want ErrInvalid", err)
	}
	failing := &fakeClient{caps: allCaps, err: errStore}
	if _, err := New(failing).Run(ctx, ref); !errors.Is(err, errStore) {
		t.Errorf("client query error = %v, want it returned", err)
	}
	rec := history()
	rec.err = errStore
	c := &fakeClient{caps: allCaps, runs: []workflow.RunInfo{{Ref: ref, Workflow: "w", Status: workflow.StatusQueued}}}
	if _, err := New(c, WithRunQuerier(rec, rec.caps)).Run(ctx, ref); !errors.Is(err, errStore) {
		t.Errorf("querier error = %v, want it returned", err)
	}
	if len(c.queries) != 0 {
		t.Errorf("a failed querier fell back to the client: %+v", c.queries)
	}
	// The querier does not hold the run and the client's fallback fails.
	if _, err := New(failing, WithRunQuerier(history(), workflow.CapRunQuery)).Run(ctx, ref); !errors.Is(err, errStore) {
		t.Errorf("fallback error = %v, want it returned", err)
	}
}

// TestQueryShapeRefusedBeforeTheReader: a malformed query is refused with
// ErrInvalid before the reader (querier or client) is asked.
func TestQueryShapeRefusedBeforeTheReader(t *testing.T) {
	ctx := context.Background()
	rec := history()
	a := New(&fakeClient{caps: workflow.CapCancel}, WithRunQuerier(rec, rec.caps))
	for _, q := range []workflow.RunQuery{
		{Limit: -1},
		{Statuses: []workflow.Status{"nope"}},
		{Queue: "bad queue"},
	} {
		if _, err := a.Runs(ctx, q); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("Runs(%+v) = %v, want ErrInvalid", q, err)
		}
	}
	if len(rec.queries) != 0 {
		t.Errorf("the querier was asked: %+v", rec.queries)
	}
}

func TestCancelAllRefusesFilterTheClientLacks(t *testing.T) {
	c := &fakeClient{caps: allCaps &^ workflow.CapLabelPrefix}
	_, err := New(c).CancelAll(context.Background(), workflow.RunQuery{Workflow: "w", LabelPrefixes: map[string]string{"k": "v"}})
	if !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("CancelAll with a label prefix = %v, want ErrUnsupported", err)
	}
	if calls := c.Calls(); len(calls) != 0 {
		t.Fatalf("calls = %q, want none", calls)
	}
}

func TestScheduleErrors(t *testing.T) {
	ctx := context.Background()
	f := seeded()
	if _, err := New(f).Schedule(ctx, ""); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("empty id = %v, want ErrInvalid", err)
	}
	if calls := f.Calls(); len(calls) != 0 {
		t.Errorf("an empty id reached the client: %q", calls)
	}
	if _, err := New(f).Schedule(ctx, "missing"); !errors.Is(err, workflow.ErrNotFound) {
		t.Errorf("unknown id = %v, want ErrNotFound", err)
	}
	f.err = errStore
	if _, err := New(f).Schedule(ctx, "s1"); !errors.Is(err, errStore) {
		t.Errorf("lister error = %v, want it returned", err)
	}
}

// TestHandler_CancelAndDeleteRefusals: a refused or failing cancel or
// delete answers with the mapped status and code, and a refused one never
// reaches the client.
func TestHandler_CancelAndDeleteRefusals(t *testing.T) {
	for _, tc := range []struct {
		name, method, path string
		action             Action
	}{
		{"cancel", "POST", "/runs/r1/cancel", ActionCancelRun},
		{"delete", "DELETE", "/runs/r1", ActionDeleteRun},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := seeded()
			auth := bearerAuth()
			auth.Authorize = func(_ context.Context, r Request[user]) error {
				if r.Action == tc.action {
					return errors.New("no")
				}
				return nil
			}
			rec := do(t, NewHandler(New(f), auth), tc.method, tc.path, "")
			if rec.Code != http.StatusForbidden || errCode(t, rec) != CodeForbidden {
				t.Fatalf("refused = %d %s, want 403 forbidden", rec.Code, rec.Body)
			}
			if calls := f.Calls(); len(calls) != 0 {
				t.Fatalf("a refused request reached the client: %q", calls)
			}
			f.err = workflow.ErrWrongState
			rec = do(t, NewHandler(New(f), bearerAuth()), tc.method, tc.path, "")
			if rec.Code != http.StatusConflict || errCode(t, rec) != CodeConflict {
				t.Fatalf("wrong state = %d %s, want 409 conflict", rec.Code, rec.Body)
			}
		})
	}
}

// TestHandler_WithLoggerLogsInternalErrors: an internal error is answered
// with a bare 500 and its detail goes to the logger; an Authorize panic is
// logged too.
func TestHandler_WithLoggerLogsInternalErrors(t *testing.T) {
	var buf bytes.Buffer
	l := slog.New(slog.NewJSONHandler(&buf, nil))
	f := seeded()
	f.err = errors.New("pq: connection refused")
	rec := do(t, NewHandler(New(f), bearerAuth(), WithLogger(l)), "POST", "/runs/r1/cancel", "")
	if rec.Code != http.StatusInternalServerError || errCode(t, rec) != CodeInternal || strings.Contains(rec.Body.String(), "pq:") {
		t.Fatalf("internal error = %d %s, want a bare 500", rec.Code, rec.Body)
	}
	var line struct {
		Level, Msg, Action, Err string
	}
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatalf("log %q: %v", buf.String(), err)
	}
	if line.Level != "ERROR" || line.Msg != "admin: operation failed" || line.Action != string(ActionCancelRun) || line.Err != "pq: connection refused" {
		t.Fatalf("log line = %+v", line)
	}

	buf.Reset()
	auth := bearerAuth()
	auth.Authorize = func(context.Context, Request[user]) error { panic("policy bug") }
	rec = do(t, NewHandler(New(seeded()), auth, WithLogger(l)), "DELETE", "/runs/r1", "")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("panicking Authorize = %d, want 403", rec.Code)
	}
	if s := buf.String(); !strings.Contains(s, "admin: Authorize panicked") || !strings.Contains(s, "policy bug") ||
		!strings.Contains(s, string(ActionDeleteRun)) {
		t.Fatalf("log = %q, want the Authorize panic", s)
	}
	if !slices.Equal(f.Calls(), []string{"Cancel r1"}) {
		t.Fatalf("calls = %q", f.Calls())
	}
}

// TestEmptyTargetsRefused: an action on an empty run ref or schedule id is
// refused with ErrInvalid and never reaches the client.
func TestEmptyTargetsRefused(t *testing.T) {
	ctx := context.Background()
	f := seeded()
	a := New(f)
	for name, call := range map[string]func() error{
		"ForceComplete":  func() error { return a.ForceComplete(ctx, workflow.RunRef{}) },
		"ForceFail":      func() error { return a.ForceFail(ctx, workflow.RunRef{}, "why") },
		"PauseSchedule":  func() error { return a.PauseSchedule(ctx, "") },
		"ResumeSchedule": func() error { return a.ResumeSchedule(ctx, "") },
	} {
		if err := call(); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("%s on an empty target = %v, want ErrInvalid", name, err)
		}
	}
	if calls := f.Calls(); len(calls) != 0 {
		t.Errorf("an empty target reached the client: %q", calls)
	}
}

func TestQueueDepthsError(t *testing.T) {
	f := seeded()
	f.err = errStore
	if got, err := New(f).QueueDepths(context.Background()); !errors.Is(err, errStore) || got != nil {
		t.Fatalf("QueueDepths = %v, %v; want nil and the store error", got, err)
	}
}

// TestRunNotFoundNamesScheduledRef: an unknown scheduled ref is reported
// by its scheduled ID.
func TestRunNotFoundNamesScheduledRef(t *testing.T) {
	_, err := New(&fakeClient{caps: allCaps}).Run(context.Background(), workflow.RunRef{ScheduledID: "s9"})
	if !errors.Is(err, workflow.ErrNotFound) || !strings.Contains(err.Error(), "run scheduled s9") {
		t.Fatalf("Run = %v, want ErrNotFound naming scheduled s9", err)
	}
}
