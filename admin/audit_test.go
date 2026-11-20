package admin

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// TestAudit_EveryWriteRoute walks every route: each write action records
// exactly one entry with who, the action, the resource, the status and
// the Admin clock's time; each read records none.
func TestAudit_EveryWriteRoute(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC))
	for _, r := range routes {
		audit := &MemoryAudit{}
		h := NewHandler(New(seeded(), WithClock(clk)), bearerAuth(), WithAudit(audit))
		rec := do(t, h, r.method, r.path, r.body)
		got := audit.Entries()
		if r.action.ReadOnly() {
			if len(got) != 0 {
				t.Errorf("%s %s (read) recorded %d entries", r.method, r.path, len(got))
			}
			continue
		}
		if len(got) != 1 {
			t.Fatalf("%s %s recorded %d entries, want 1", r.method, r.path, len(got))
		}
		e := got[0]
		if e.Action != r.action || e.Who != (user{"ann", "admin"}) || !e.Allowed || e.Status != rec.Code || !e.At.Equal(clk.Now()) {
			t.Errorf("%s %s: entry %+v, response %d", r.method, r.path, e, rec.Code)
		}
		if (e.Err == nil) != (rec.Code < 400) {
			t.Errorf("%s %s: Err %v with status %d", r.method, r.path, e.Err, rec.Code)
		}
	}
}

// TestAudit_ResourceAndOutcome: the entry carries the resource the
// action was about, a refusal, the operation's own error (in full, while
// the response says only "internal error"), and a dry run.
func TestAudit_ResourceAndOutcome(t *testing.T) {
	cases := []struct {
		name         string
		setup        func(f *fakeClient)
		who          string // "" means the admin
		method, path string
		body         string
		check        func(t *testing.T, e AuditEntry)
	}{
		{name: "retry names the run", method: "POST", path: "/runs/r2/retry", check: func(t *testing.T, e AuditEntry) {
			if e.Resource.Run.RunID != "r2" || e.Err != nil || e.Status != 200 {
				t.Errorf("entry %+v", e)
			}
		}},
		{name: "refused write is recorded", who: "viewer", method: "POST", path: "/runs/r2/cancel", check: func(t *testing.T, e AuditEntry) {
			if e.Allowed || e.Status != http.StatusForbidden || e.Err == nil || e.Who != (user{"vic", "viewer"}) {
				t.Errorf("entry %+v", e)
			}
		}},
		{name: "conflict keeps the sentinel", setup: func(f *fakeClient) {
			f.refErr = map[workflow.RunID]error{"r1": workflow.ErrWrongState}
		}, method: "POST", path: "/runs/r1/retry", check: func(t *testing.T, e AuditEntry) {
			if !errors.Is(e.Err, workflow.ErrWrongState) || e.Status != http.StatusConflict {
				t.Errorf("entry %+v", e)
			}
		}},
		{name: "internal error kept in full", setup: func(f *fakeClient) {
			f.err = errors.New("dial tcp 10.0.0.7:5432: refused")
		}, method: "DELETE", path: "/runs/r2", check: func(t *testing.T, e AuditEntry) {
			if e.Err == nil || !strings.Contains(e.Err.Error(), "10.0.0.7") || e.Status != 500 {
				t.Errorf("entry %+v", e)
			}
		}},
		{name: "dry run says so", method: "POST", path: "/runs/delete", body: `{"workflow":"w","statuses":["failed"],"dry_run":true}`, check: func(t *testing.T, e AuditEntry) {
			if !e.Resource.DryRun || e.Resource.Query == nil || e.Resource.Query.Workflow != "w" {
				t.Errorf("entry %+v", e)
			}
		}},
		{name: "force fail carries the reason", method: "POST", path: "/runs/r1/fail", body: `{"reason":"stuck"}`, check: func(t *testing.T, e AuditEntry) {
			if e.Resource.Reason != "stuck" || e.Action != ActionForceFail {
				t.Errorf("entry %+v", e)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := seeded()
			if tc.setup != nil {
				tc.setup(f)
			}
			auth := bearerAuth()
			auth.Identify = func(r *http.Request) (user, error) {
				if r.Header.Get("X-Role") == "viewer" {
					return user{"vic", "viewer"}, nil
				}
				return user{"ann", "admin"}, nil
			}
			auth.Authorize = func(_ context.Context, req Request[user]) error {
				if req.Who.role == "viewer" && !req.Action.ReadOnly() {
					return errors.New("read only")
				}
				return nil
			}
			audit := &MemoryAudit{}
			h := NewHandler(New(f), auth, WithAudit(audit))
			req := httptestRequest(tc.method, tc.path, tc.body)
			if tc.who == "viewer" {
				req.Header.Set("X-Role", "viewer")
			}
			rec := serve(h, req)
			if strings.Contains(rec.Body.String(), "10.0.0.7") {
				t.Fatal("the internal error reached the response")
			}
			got := audit.Entries()
			if len(got) != 1 {
				t.Fatalf("%d entries, response %d %s", len(got), rec.Code, rec.Body)
			}
			tc.check(t, got[0])
		})
	}
}

// TestAudit_SinkFailureNeverChangesTheResponse: a sink that errors or
// panics is logged; the operation's response stands.
func TestAudit_SinkFailureNeverChangesTheResponse(t *testing.T) {
	for name, sink := range map[string]AuditSink{
		"error": AuditFunc(func(context.Context, AuditEntry) error { return errors.New("audit table gone") }),
		"panic": AuditFunc(func(context.Context, AuditEntry) error { panic("boom") }),
	} {
		h := NewHandler(New(seeded()), bearerAuth(), WithAudit(sink))
		if rec := do(t, h, "POST", "/runs/r2/retry", ""); rec.Code != http.StatusOK {
			t.Errorf("%s: %d %s", name, rec.Code, rec.Body)
		}
	}
}

// TestAudit_NotBeforeAuthorize: a request refused before Authorize (no
// identity, no JSON content type) asked for no action and records none;
// without WithAudit nothing wraps the writer.
func TestAudit_NotBeforeAuthorize(t *testing.T) {
	audit := &MemoryAudit{}
	h := NewHandler(New(seeded()), bearerAuth(), WithAudit(audit))
	req := httptestRequest("POST", "/runs/r2/retry", "")
	req.Header.Del("Authorization")
	serve(h, req)
	req = httptestRequest("POST", "/runs/r2/retry", "")
	req.Header.Del("Content-Type")
	serve(h, req)
	if n := len(audit.Entries()); n != 0 {
		t.Fatalf("%d entries", n)
	}
}
