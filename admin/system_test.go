package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/failover"
)

func TestHandler_NodesAndHealth(t *testing.T) {
	f := seeded()
	h := NewHandler(New(f), bearerAuth())
	rec := do(t, h, "GET", "/nodes", "")
	var n struct {
		Nodes []nodeJSON `json:"nodes"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &n); err != nil || rec.Code != 200 || len(n.Nodes) != 2 {
		t.Fatalf("nodes = %d %s", rec.Code, rec.Body)
	}
	if n.Nodes[0].ID != "web-1:9" || n.Nodes[1].Queues == nil {
		t.Errorf("nodes not sorted by ID, or nil queues: %+v", n.Nodes)
	}
	// Nodes follows the interface, not a capability: a client that lists
	// nodes without CapNodeIdentity is served, one without NodeLister is
	// refused.
	hn := NewHandler(New(&fakeClient{caps: workflow.CapRunQuery}), bearerAuth())
	if rec := do(t, hn, "GET", "/nodes", ""); rec.Code != http.StatusOK {
		t.Errorf("nodes without the cap = %d, want 200", rec.Code)
	}
	if _, err := New(bareClient{caps: allCaps}).Nodes(context.Background()); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("nodes without NodeLister = %v", err)
	}
	// Health of a client without HealthChecker: live, ready, cannot tell.
	rec = do(t, h, "GET", "/health", "")
	var hr struct {
		Live, Ready bool
		Checks      []healthCheckJSON
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &hr); err != nil || !hr.Live || !hr.Ready || len(hr.Checks) != 1 {
		t.Errorf("health = %s", rec.Body)
	}
	// A client with a health report is passed through, not-ready included,
	// and still answered 200 (this is a screen, not a probe).
	hc := healthyClient{fakeClient: seeded(), rep: workflow.HealthReport{Live: true, Checks: []workflow.HealthCheck{{Name: "database", Detail: "connection refused"}}}}
	rec = do(t, NewHandler(New(&hc), bearerAuth()), "GET", "/health", "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"ready":false`) || !strings.Contains(rec.Body.String(), "connection refused") {
		t.Errorf("not ready = %d %s", rec.Code, rec.Body)
	}
}

type healthyClient struct {
	*fakeClient
	rep workflow.HealthReport
}

func (h *healthyClient) Health(context.Context) workflow.HealthReport { return h.rep }

func TestHandler_RescheduleAndCancelStep(t *testing.T) {
	f := seeded()
	var res []Resource
	auth := bearerAuth()
	auth.Authorize = func(_ context.Context, req Request[user]) error {
		res = append(res, req.Resource)
		return nil
	}
	h := NewHandler(New(f), auth)
	if rec := do(t, h, "POST", "/runs/r1/reschedule", `{"at":"2026-10-15T09:00:00Z"}`); rec.Code != 200 {
		t.Fatalf("reschedule = %d %s", rec.Code, rec.Body)
	}
	if rec := do(t, h, "POST", "/runs/r1/reschedule", `{}`); rec.Code != http.StatusBadRequest {
		t.Errorf("no time = %d, want 400", rec.Code)
	}
	if rec := do(t, h, "POST", "/runs/r1/steps/send/cancel", ""); rec.Code != 200 {
		t.Fatalf("cancel step = %d %s", rec.Code, rec.Body)
	}
	want := []string{"Reschedule r1 2026-10-15T09:00:00Z", "CancelStep r1 send"}
	if c := f.Calls(); !slices.Equal(c, want) {
		t.Errorf("calls = %v, want %v", c, want)
	}
	if !res[0].At.Equal(time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)) || res[2].Step != "send" || res[2].Run.RunID != "r1" {
		t.Errorf("Authorizer saw %+v", res)
	}
	f.refErr = map[workflow.RunID]error{"r1": workflow.ErrWrongState}
	if rec := do(t, h, "POST", "/runs/r1/reschedule", `{"at":"2026-10-15T09:00:00Z"}`); rec.Code != http.StatusConflict {
		t.Errorf("wrong state = %d, want 409", rec.Code)
	}
	// A client without the interface: 501 from the operation.
	b := New(bareClient{caps: allCaps})
	if _, err := b.Reschedule(context.Background(), workflow.RunRef{RunID: "r1"}, time.Now()); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("reschedule on a bare client = %v", err)
	}
	if err := b.CancelStep(context.Background(), workflow.RunRef{RunID: "r1"}, "send"); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("cancel step on a bare client = %v", err)
	}
}

// TestHandler_RunFieldsP5: tenant, due time, progress, result and nodes
// reach the run JSON; progress data and the result only with
// WithPayloads.
func TestHandler_RunFieldsP5(t *testing.T) {
	at := time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)
	f := &fakeClient{caps: allCaps | workflow.CapTenant, runs: []workflow.RunInfo{{
		Ref: workflow.RunRef{RunID: "r1"}, Workflow: "w", Status: workflow.StatusSucceeded, Tenant: "acme", RunAt: at,
		Output: []byte(`{"rows":3}`), AttemptedBy: []string{"web-1:9"},
		Progress: &workflow.Progress{Percent: 40, Message: "4 of 10", Data: []byte("cursor-9"), Step: "export", At: at},
	}}}
	rec := do(t, NewHandler(New(f), bearerAuth()), "GET", "/runs/r1", "")
	body := rec.Body.String()
	for _, s := range []string{`"tenant":"acme"`, `"run_at":"2026-10-14T09:00:00Z"`, `"percent":40`, `"message":"4 of 10"`, `"has_output":true`, `"attempted_by":["web-1:9"]`} {
		if !strings.Contains(body, s) {
			t.Errorf("run JSON lacks %s: %s", s, body)
		}
	}
	if strings.Contains(body, `"output"`) || strings.Contains(body, `"data"`) {
		t.Errorf("result or progress data sent without WithPayloads: %s", body)
	}
	body = do(t, NewHandler(New(f), bearerAuth(), WithPayloads()), "GET", "/runs/r1", "").Body.String()
	if !strings.Contains(body, `"output":"eyJyb3dzIjozfQ=="`) || !strings.Contains(body, `"data":"Y3Vyc29yLTk="`) {
		t.Errorf("WithPayloads run JSON = %s", body)
	}
	// The tenant filter reaches the query, and needs CapTenant.
	if rec := do(t, NewHandler(New(f), bearerAuth()), "GET", "/runs?tenant=acme", ""); rec.Code != 200 || f.queries[len(f.queries)-1].Tenant != "acme" {
		t.Errorf("tenant filter = %d", rec.Code)
	}
	f.caps = allCaps
	if rec := do(t, NewHandler(New(f), bearerAuth()), "GET", "/runs?tenant=acme", ""); rec.Code != http.StatusNotImplemented {
		t.Errorf("tenant filter without CapTenant = %d, want 501", rec.Code)
	}
}

// fwdWrapper is a wrapper built on workflow.Forward: it implements every
// optional interface and answers workflow.Supports for its inner client.
type fwdWrapper struct {
	workflow.Client
	workflow.Forward
}

func wrap(c workflow.Client) fwdWrapper {
	return fwdWrapper{Client: c, Forward: workflow.Forward{Inner: c}}
}

// Capabilities and the Client methods both embedded fields have resolve
// to the wrapped client.
func (w fwdWrapper) Capabilities() workflow.Caps { return w.Client.Capabilities() }

// TestSupport_AsksWrappers: behind workflow.Forward or failover an
// operation with no capability bit is supported only when what is
// underneath does it (workflow.Supports), though the wrapper implements
// its interface.
func TestSupport_AsksWrappers(t *testing.T) {
	bare := bareClient{caps: allCaps}
	ops := []Operation{OpNodes, OpReschedule, OpCancelStep}
	for _, op := range ops {
		if New(wrap(bare)).Supports(op) {
			t.Errorf("%s supported behind Forward over a client without it", op)
		}
		if !New(wrap(seeded())).Supports(op) {
			t.Errorf("%s unsupported behind Forward over a client with it", op)
		}
		if New(wrap(wrap(bare))).Supports(op) {
			t.Errorf("%s supported two wrappers deep over a client without it", op)
		}
		// failover: both backends must do it, since a run may sit on either.
		if New(failover.New(seeded(), bare)).Supports(op) {
			t.Errorf("%s supported by failover whose secondary lacks it", op)
		}
		if !New(failover.New(seeded(), seeded())).Supports(op) {
			t.Errorf("%s unsupported by failover over two clients with it", op)
		}
	}
	// The capabilities route reports it, so the UI hides the button.
	rec := do(t, NewHandler(New(failover.New(seeded(), bare)), bearerAuth()), "GET", "/capabilities", "")
	if !strings.Contains(rec.Body.String(), `{"operation":"reschedule","supported":false`) {
		t.Errorf("capabilities behind failover = %s", rec.Body)
	}
}

// TestHandler_ScheduleTenant: a schedule's tenant reaches the list and
// the detail JSON, and is left out when unset.
func TestHandler_ScheduleTenant(t *testing.T) {
	f := seeded()
	f.schedules = append(f.schedules, workflow.ScheduleInfo{ID: "s2", Workflow: "w", Cron: "0 3 * * *", Location: time.UTC, Tenant: "acme"})
	h := NewHandler(New(f), bearerAuth())
	if body := do(t, h, "GET", "/schedules/s2", "").Body.String(); !strings.Contains(body, `"tenant":"acme"`) {
		t.Errorf("detail = %s", body)
	}
	body := do(t, h, "GET", "/schedules", "").Body.String()
	if strings.Count(body, `"tenant"`) != 1 || !strings.Contains(body, `"tenant":"acme"`) {
		t.Errorf("list = %s", body)
	}
}
