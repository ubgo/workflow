package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

// TestTenantRoutes_CoverEveryRoute keeps tenantRoutes equal to the routes
// NewHandler registers, read from its source: a route added without a
// tenant rule would be refused to every scoped caller (the default deny),
// and one removed would leave a stale rule.
func TestTenantRoutes_CoverEveryRoute(t *testing.T) {
	src, err := os.ReadFile("http.go")
	if err != nil {
		t.Fatal(err)
	}
	var registered []string
	for _, m := range regexp.MustCompile(`mux\.HandleFunc\("([^"]+)"`).FindAllSubmatch(src, -1) {
		registered = append(registered, string(m[1]))
	}
	if len(registered) < 20 {
		t.Fatalf("found %d routes in http.go; the pattern no longer matches NewHandler", len(registered))
	}
	for _, p := range registered {
		if _, ok := tenantRoutes[p]; !ok {
			t.Errorf("route %q has no tenantRoutes rule", p)
		}
	}
	for p := range tenantRoutes {
		if !slices.Contains(registered, p) {
			t.Errorf("tenantRoutes has %q, which NewHandler does not register", p)
		}
	}
}

// tenants is a memory client holding two tenants' runs and schedules.
type tenants struct {
	c                  *memory.Client
	aWait, aFail       workflow.RunRef
	bWait, bFail       workflow.RunRef
	aSched, bSched, no workflow.ScheduleID
}

var errBoom = errors.New("boom")

func newTenants(t *testing.T) *tenants {
	t.Helper()
	c := memory.New()
	for _, wf := range []workflow.Workflow{
		{Name: "wait", Steps: []workflow.Step{{Name: "w", Wait: &workflow.WaitSpec{Event: "go"}}}},
		{Name: "boom", Steps: []workflow.Step{{Name: "b", Run: func(context.Context, []byte) ([]byte, error) { return nil, workflow.Permanent(errBoom) }}}},
	} {
		if err := c.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	ts := &tenants{c: c}
	enq := func(wf, tenant string, want workflow.Status) workflow.RunRef {
		ref, err := c.Enqueue(t.Context(), workflow.Job{Workflow: wf, Tenant: tenant})
		if err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for {
			if st, _ := c.Status(t.Context(), ref); st == want {
				return ref
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s for %s never reached %s", wf, tenant, want)
			}
			time.Sleep(time.Millisecond)
		}
	}
	ts.aWait, ts.aFail = enq("wait", "a", workflow.StatusWaiting), enq("boom", "a", workflow.StatusFailed)
	ts.bWait, ts.bFail = enq("wait", "b", workflow.StatusWaiting), enq("boom", "b", workflow.StatusFailed)
	sched := func(name, tenant string) workflow.ScheduleID {
		id, err := c.Schedule(t.Context(), workflow.Schedule{Name: name, Workflow: "boom", Cron: "0 * * * *", Location: time.UTC, Tenant: tenant})
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	ts.aSched, ts.bSched, ts.no = sched("a-hourly", "a"), sched("b-hourly", "b"), sched("shared", "")
	return ts
}

// scopedAuth identifies "Bearer <name>" as that user and allows all.
func scopedAuth() Auth[user] {
	return Auth[user]{
		Identify: func(r *http.Request) (user, error) {
			name, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			if !ok {
				return user{}, errors.New("no token")
			}
			return user{name: name}, nil
		},
		Authorize: func(context.Context, Request[user]) error { return nil },
	}
}

// byName scopes users "a" and "b" to their tenant, "ops" to every tenant,
// and fails for "err", "empty", "panic" and "bad".
func byName(_ *http.Request, u user) (string, error) {
	switch u.name {
	case "ops":
		return AllTenants, nil
	case "err":
		return "", errors.New("lookup failed")
	case "empty":
		return "", nil
	case "panic":
		panic("scope bug")
	case "bad":
		return "a b", nil
	}
	return u.name, nil
}

func as(t *testing.T, h http.Handler, who, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if method != http.MethodGet {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Authorization", "Bearer "+who)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func runIDs(t *testing.T, rec *httptest.ResponseRecorder) []string {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	var page struct {
		Runs []struct {
			Ref    refJSON `json:"ref"`
			Tenant string  `json:"tenant"`
		} `json:"runs"`
		Total *int `json:"total"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, r := range page.Runs {
		out = append(out, r.Ref.RunID)
	}
	slices.Sort(out)
	return out
}

func ids(refs ...workflow.RunRef) []string {
	var out []string
	for _, r := range refs {
		out = append(out, string(r.RunID))
	}
	slices.Sort(out)
	return out
}

func TestTenantScope_ListsPinned(t *testing.T) {
	ts := newTenants(t)
	h := NewHandler(New(ts.c), scopedAuth(), WithTenantScope(byName))

	if got := runIDs(t, as(t, h, "a", "GET", "/runs", "")); !slices.Equal(got, ids(ts.aWait, ts.aFail)) {
		t.Fatalf("a's /runs = %v, want %v", got, ids(ts.aWait, ts.aFail))
	}
	if got := runIDs(t, as(t, h, "a", "GET", "/runs?tenant=a", "")); !slices.Equal(got, ids(ts.aWait, ts.aFail)) {
		t.Fatalf("a's /runs?tenant=a = %v", got)
	}
	if got := runIDs(t, as(t, h, "b", "GET", "/failures", "")); !slices.Equal(got, ids(ts.bFail)) {
		t.Fatalf("b's /failures = %v", got)
	}
	rec := as(t, h, "a", "GET", "/runs?count=1", "")
	var counted struct {
		Total int `json:"total"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &counted); err != nil || counted.Total != 2 {
		t.Fatalf("a's count = %d, %v (%s)", counted.Total, err, rec.Body)
	}
	for _, path := range []string{"/runs?tenant=b", "/failures?tenant=a&tenant=b", "/discarded?tenant=b"} {
		if rec := as(t, h, "a", "GET", path, ""); rec.Code != http.StatusForbidden {
			t.Errorf("a GET %s = %d, want 403", path, rec.Code)
		}
	}

	// The Authorizer sees the pinned filter and the tenant.
	var seen []string
	auth := scopedAuth()
	auth.Authorize = func(_ context.Context, req Request[user]) error {
		tenant, _ := TenantFrom(req.HTTP.Context())
		seen = append(seen, req.Resource.Query.Tenant+"/"+tenant)
		return nil
	}
	as(t, NewHandler(New(ts.c), auth, WithTenantScope(byName)), "b", "GET", "/runs", "")
	if !slices.Equal(seen, []string{"b/b"}) {
		t.Fatalf("Authorize saw %v, want [b/b]", seen)
	}
}

func TestTenantScope_BulkPinned(t *testing.T) {
	ts := newTenants(t)
	h := NewHandler(New(ts.c), scopedAuth(), WithTenantScope(byName))

	rec := as(t, h, "a", "POST", "/runs/retry", `{"dry_run":true}`)
	var res bulkJSON
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil || res.Matched != 1 || res.Sample[0].RunID != string(ts.aFail.RunID) {
		t.Fatalf("a's retry preview = %+v, %v (%s)", res, err, rec.Body)
	}
	rec = as(t, h, "a", "POST", "/runs/cancel", `{"workflow":"wait","statuses":["waiting"]}`)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"cancelled":1`) {
		t.Fatalf("a's cancel = %d %s", rec.Code, rec.Body)
	}
	if st, _ := ts.c.Status(t.Context(), ts.bWait); st != workflow.StatusWaiting {
		t.Fatalf("b's waiting run is %s after a's cancel", st)
	}
	for _, body := range []string{`{"tenant":"b"}`, `{"Tenant":"b"}`, `{"TENANT":"b","workflow":"wait"}`, `{"tenant":7}`} {
		if rec := as(t, h, "a", "POST", "/runs/delete", body); rec.Code != http.StatusForbidden {
			t.Errorf("a POST /runs/delete %s = %d, want 403", body, rec.Code)
		}
	}
	for _, body := range []string{``, `[1]`, `{"a":1}{"b":2}`, `null`} {
		if rec := as(t, h, "a", "POST", "/runs/delete", body); rec.Code != http.StatusBadRequest {
			t.Errorf("a POST /runs/delete %q = %d, want 400", body, rec.Code)
		}
	}
	if rec := as(t, h, "a", "POST", "/runs/delete", `{"tenant":"a","workflow":"boom","dry_run":true}`); rec.Code != http.StatusOK {
		t.Fatalf("own tenant named = %d %s", rec.Code, rec.Body)
	}
}

func TestTenantScope_OneRun(t *testing.T) {
	ts := newTenants(t)
	h := NewHandler(New(ts.c), scopedAuth(), WithTenantScope(byName))

	if rec := as(t, h, "a", "GET", "/runs/"+string(ts.aWait.RunID), ""); rec.Code != http.StatusOK {
		t.Fatalf("a reads its own run: %d %s", rec.Code, rec.Body)
	}
	missing := as(t, h, "a", "GET", "/runs/nope", "")
	other := string(ts.bWait.RunID)
	for _, r := range []route{
		{"GET", "/runs/" + other, "", ActionShowRun},
		{"POST", "/runs/" + other + "/cancel", "", ActionCancelRun},
		{"POST", "/runs/" + string(ts.bFail.RunID) + "/retry", "", ActionRetryRun},
		{"DELETE", "/runs/" + string(ts.bFail.RunID), "", ActionDeleteRun},
		{"POST", "/runs/" + other + "/complete", "", ActionForceComplete},
		{"POST", "/runs/" + other + "/fail", `{"reason":"x"}`, ActionForceFail},
		{"GET", "/runs/" + other + "/output", "", ActionShowOutput},
		{"POST", "/runs/" + other + "/reschedule", `{"at":"2030-01-01T00:00:00Z"}`, ActionRescheduleRun},
		{"POST", "/runs/" + other + "/steps/w/cancel", "", ActionCancelStep},
		{"POST", "/runs/" + other + "/edit", `{"priority":1}`, ActionEditRun},
		{"POST", "/runs/" + string(ts.bFail.RunID) + "/reset", `{"step":"b"}`, ActionResetRun},
		{"POST", "/runs/" + string(ts.bFail.RunID) + "/fork", `{"step":"b"}`, ActionForkRun},
	} {
		rec := as(t, h, "a", r.method, r.path, r.body)
		if rec.Code != http.StatusNotFound || errCode(t, rec) != CodeNotFound {
			t.Errorf("a %s %s = %d %s, want 404", r.method, r.path, rec.Code, rec.Body)
		}
		// The answer reads like a run that does not exist.
		if want := strings.Replace(missing.Body.String(), "nope", strings.Split(r.path, "/")[2], 1); rec.Body.String() != want {
			t.Errorf("a %s %s body %s, want %s", r.method, r.path, rec.Body, want)
		}
	}
	if st, _ := ts.c.Status(t.Context(), ts.bWait); st != workflow.StatusWaiting {
		t.Fatalf("b's run is %s after a's refused calls", st)
	}
	if rec := as(t, h, "a", "POST", "/runs/"+string(ts.aWait.RunID)+"/cancel", ""); rec.Code != http.StatusOK {
		t.Fatalf("a cancels its own run: %d %s", rec.Code, rec.Body)
	}
}

func TestTenantScope_Schedules(t *testing.T) {
	ts := newTenants(t)
	h := NewHandler(New(ts.c), scopedAuth(), WithTenantScope(byName))

	rec := as(t, h, "a", "GET", "/schedules", "")
	var list struct {
		Schedules []scheduleJSON `json:"schedules"`
		Total     int            `json:"total"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil || list.Total != 1 || list.Schedules[0].ID != string(ts.aSched) {
		t.Fatalf("a's schedules = %+v, %v", list, err)
	}
	for _, id := range []workflow.ScheduleID{ts.bSched, ts.no} {
		for _, r := range []route{
			{"GET", "/schedules/" + string(id), "", ActionShowSchedule},
			{"POST", "/schedules/" + string(id) + "/pause", "", ActionPauseSchedule},
			{"POST", "/schedules/" + string(id) + "/resume", "", ActionResumeSchedule},
			{"POST", "/schedules/" + string(id) + "/trigger", "", ActionTriggerSchedule},
		} {
			if rec := as(t, h, "a", r.method, r.path, ""); rec.Code != http.StatusNotFound {
				t.Errorf("a %s %s = %d %s, want 404", r.method, r.path, rec.Code, rec.Body)
			}
		}
	}
	if rec := as(t, h, "a", "POST", "/schedules/"+string(ts.aSched)+"/trigger", ""); rec.Code != http.StatusOK {
		t.Fatalf("a triggers its own schedule: %d %s", rec.Code, rec.Body)
	}
	infos, _ := ts.c.Schedules(t.Context())
	for _, s := range infos {
		if s.ID != ts.aSched && !s.Last.Fire.IsZero() {
			t.Errorf("schedule %s fired", s.ID)
		}
	}
}

func TestTenantScope_SystemRoutesRefused(t *testing.T) {
	ts := newTenants(t)
	h := NewHandler(New(ts.c, WithMetrics(NewMemoryMetrics(MemoryMetricsConfig{}))), scopedAuth(), WithTenantScope(byName))
	for _, r := range append(slices.Clone(routes), route{"GET", "/nope", "", ""}, route{"PUT", "/runs", "{}", ""}) {
		if tenantRouteOf(r).rule != tenantRefused && r.action != "" {
			continue
		}
		rec := as(t, h, "a", r.method, r.path, r.body)
		if rec.Code != http.StatusForbidden || errCode(t, rec) != CodeForbidden {
			t.Errorf("a %s %s = %d %s, want 403", r.method, r.path, rec.Code, rec.Body)
		}
		// AllTenants is served as without a scope.
		if r.action != "" {
			if rec := as(t, h, "ops", r.method, r.path, r.body); rec.Code == http.StatusForbidden {
				t.Errorf("ops %s %s = 403: %s", r.method, r.path, rec.Body)
			}
		}
	}
	if got := runIDs(t, as(t, h, "ops", "GET", "/runs", "")); len(got) != 4 {
		t.Fatalf("ops sees %d runs, want 4", len(got))
	}

	rec := as(t, h, "a", "GET", "/capabilities", "")
	var caps struct {
		Operations []struct {
			Operation Operation `json:"operation"`
			Supported bool      `json:"supported"`
		} `json:"operations"`
		Metrics bool   `json:"metrics"`
		Tenant  string `json:"tenant"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &caps); err != nil || caps.Tenant != "a" || caps.Metrics {
		t.Fatalf("a's capabilities = %+v, %v", caps, err)
	}
	for _, o := range caps.Operations {
		if refusedOps[o.Operation] == o.Supported {
			t.Errorf("a's capabilities: %s supported = %v", o.Operation, o.Supported)
		}
	}
}

// tenantRouteOf finds the tenant rule of a test route by asking a mux
// built from tenantRoutes which pattern it matches.
func tenantRouteOf(r route) tenantRoute {
	mux := http.NewServeMux()
	for p := range tenantRoutes {
		mux.HandleFunc(p, func(http.ResponseWriter, *http.Request) {})
	}
	_, pattern := mux.Handler(httptest.NewRequest(r.method, r.path, nil))
	tr, ok := tenantRoutes[pattern]
	if !ok {
		return tenantRoute{rule: tenantRefused}
	}
	return tr
}

func TestTenantScope_FailsClosed(t *testing.T) {
	ts := newTenants(t)
	h := NewHandler(New(ts.c), scopedAuth(), WithTenantScope(byName))
	for _, who := range []string{"err", "empty", "panic", "bad"} {
		rec := as(t, h, who, "GET", "/runs", "")
		if rec.Code != http.StatusForbidden || strings.Contains(rec.Body.String(), "lookup failed") {
			t.Errorf("%s: %d %s, want a bare 403", who, rec.Code, rec.Body)
		}
	}
	if rec := as(t, h, "a", "GET", "/runs", ""); rec.Code != http.StatusOK {
		t.Fatalf("a after the refusals: %d", rec.Code)
	}
	// Anonymous callers still get 401 before any scope runs.
	req := httptest.NewRequest("GET", "/runs", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous = %d", rec.Code)
	}
	// Without Auth the handler is closed whatever the scope.
	if rec := as(t, NewHandler(New(ts.c), Auth[user]{}, WithTenantScope(byName)), "a", "GET", "/runs", ""); rec.Code != http.StatusForbidden {
		t.Fatalf("no Auth = %d", rec.Code)
	}
}

// noTenant is a memory client that does not declare CapTenant.
type noTenant struct{ *memory.Client }

func (n noTenant) Capabilities() workflow.Caps {
	return n.Client.Capabilities().Without(workflow.CapTenant)
}

func TestTenantScope_RefusedWithoutCapTenant(t *testing.T) {
	c := noTenant{memory.New()}
	h := NewHandler(New(c), scopedAuth(), WithTenantScope(byName))
	for _, r := range routes {
		for _, who := range []string{"a", "ops"} {
			rec := as(t, h, who, r.method, r.path, r.body)
			if rec.Code != http.StatusNotImplemented || errCode(t, rec) != CodeUnsupported || !strings.Contains(rec.Body.String(), "Tenant") {
				t.Errorf("%s %s %s = %d %s, want 501 naming CapTenant", who, r.method, r.path, rec.Code, rec.Body)
			}
		}
	}
	// A WithRunQuerier querier without CapTenant is refused the same way.
	ts := newTenants(t)
	h = NewHandler(New(ts.c, WithRunQuerier(ts.c, workflow.CapRunQuery)), scopedAuth(), WithTenantScope(byName))
	if rec := as(t, h, "a", "GET", "/runs", ""); rec.Code != http.StatusNotImplemented {
		t.Fatalf("querier without CapTenant = %d", rec.Code)
	}
}

func TestTenantScope_PrincipalTypeMismatch(t *testing.T) {
	ts := newTenants(t)
	other := WithTenantScope(func(*http.Request, string) (string, error) { return "a", nil })
	h := NewHandler(New(ts.c), scopedAuth(), other)
	if rec := as(t, h, "a", "GET", "/runs", ""); rec.Code != http.StatusForbidden {
		t.Fatalf("mismatched scope = %d %s, want 403", rec.Code, rec.Body)
	}
}
