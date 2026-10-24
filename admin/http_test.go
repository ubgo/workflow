package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ubgo/workflow"
)

// route is one call per admin route, with the action it must authorize.
// TestRoutes_CoverEveryAction keeps it complete: a route added without a
// row here fails that test, so the anonymous walks below always cover
// every route.
type route struct {
	method, path, body string
	action             Action
}

var routes = []route{
	{"GET", "/capabilities", "", ActionCapabilities},
	{"GET", "/runs?workflow=w&status=failed", "", ActionListRuns},
	{"GET", "/runs/r1", "", ActionShowRun},
	{"GET", "/failures", "", ActionListFailures},
	{"GET", "/discarded", "", ActionListDiscarded},
	{"POST", "/runs/r1/retry", "", ActionRetryRun},
	{"POST", "/runs/r1/cancel", "", ActionCancelRun},
	{"DELETE", "/runs/r1", "", ActionDeleteRun},
	{"POST", "/runs/cancel", `{"workflow":"w"}`, ActionCancelRuns},
	{"POST", "/pause", `{"queue":"emails"}`, ActionPause},
	{"POST", "/resume", `{"all":true}`, ActionResume},
	{"GET", "/pauses", "", ActionListPauses},
	{"GET", "/schedules", "", ActionListSchedules},
	{"GET", "/queues", "", ActionQueueStats},
	{"POST", "/schedules/s1/pause", "", ActionPauseSchedule},
	{"POST", "/schedules/s1/resume", "", ActionResumeSchedule},
	{"GET", "/schedules/s1", "", ActionShowSchedule},
	{"POST", "/schedules/s1/trigger", "", ActionTriggerSchedule},
	{"POST", "/runs/r1/complete", "", ActionForceComplete},
	{"POST", "/runs/r1/fail", `{"reason":"stuck on a dead host"}`, ActionForceFail},
	{"GET", "/runs/r1/output", "", ActionShowOutput},
	{"POST", "/runs/retry", `{"workflow":"w"}`, ActionRetryRuns},
	{"POST", "/runs/delete", `{"workflow":"w","dry_run":true}`, ActionDeleteRuns},
	{"GET", "/metrics?window=1h&step=1m", "", ActionMetrics},
	{"GET", "/nodes", "", ActionListNodes},
	{"GET", "/health", "", ActionHealth},
	{"POST", "/runs/r1/reschedule", `{"at":"2026-10-15T09:00:00Z"}`, ActionRescheduleRun},
	{"POST", "/runs/r1/steps/send/cancel", "", ActionCancelStep},
	{"GET", "/", "", ActionUI},
	{"GET", "/ui", "", ActionUI},
	{"GET", "/ui/", "", ActionUI},
	{"GET", "/ui/app.js", "", ActionUI},
	{"GET", "/ui/app.css", "", ActionUI},
	{"GET", "/ui/index.html", "", ActionUI},
}

// okStatus is the status a route answers an allowed caller with: the UI
// redirects answer 302, everything else 200.
func okStatus(r route) int {
	if r.path == "/" || r.path == "/ui" {
		return http.StatusFound
	}
	return http.StatusOK
}

// user is the principal type the tests' Auth uses.
type user struct{ name, role string }

func do(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if method != http.MethodGet {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Authorization", "Bearer admin")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func errCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var out struct {
		Error struct{ Code string } `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("body %q: %v", rec.Body.String(), err)
	}
	return out.Error.Code
}

// bearerAuth identifies "Bearer admin" as an admin and allows everything.
func bearerAuth() Auth[user] {
	return Auth[user]{
		Identify: func(r *http.Request) (user, error) {
			if r.Header.Get("Authorization") == "Bearer admin" {
				return user{"ann", "admin"}, nil
			}
			return user{}, errors.New("no token")
		},
		Authorize: func(context.Context, Request[user]) error { return nil },
	}
}

func seeded() *fakeClient {
	t0 := time.Date(2026, 10, 12, 9, 0, 0, 0, time.UTC)
	return &fakeClient{caps: allCaps, runs: []workflow.RunInfo{
		{Ref: workflow.RunRef{RunID: "r3"}, Workflow: "w", Queue: "emails", Status: workflow.StatusQueued, EnqueuedAt: t0.Add(3 * time.Minute), Payload: []byte("secret")},
		{Ref: workflow.RunRef{RunID: "r2"}, Workflow: "w", Queue: "default", Status: workflow.StatusFailed, Error: "boom", FailedStep: "send", EnqueuedAt: t0.Add(2 * time.Minute), Labels: map[string]string{"tenant": "a"}},
		{Ref: workflow.RunRef{RunID: "r1"}, Workflow: "w", Queue: "emails", Status: workflow.StatusQueued, EnqueuedAt: t0.Add(time.Minute)},
	}, schedules: []workflow.ScheduleInfo{
		{ID: "s1", Name: "nightly", Workflow: "w", Cron: "0 2 * * *", Location: time.UTC, Next: t0.Add(17 * time.Hour)},
	}}
}

func TestRoutes_CoverEveryAction(t *testing.T) {
	var got []Action
	for _, r := range routes {
		got = append(got, r.action)
	}
	for _, a := range Actions {
		if !slices.Contains(got, a) {
			t.Errorf("action %s has no row in routes", a)
		}
	}
	// Every row really reaches its route and authorizes its action.
	for _, r := range routes {
		var seen []Action
		auth := bearerAuth()
		auth.Authorize = func(_ context.Context, req Request[user]) error {
			seen = append(seen, req.Action)
			if req.Who.name != "ann" {
				t.Errorf("%s %s: Who = %+v", r.method, r.path, req.Who)
			}
			return nil
		}
		do(t, NewHandler(New(seeded()), auth), r.method, r.path, r.body)
		if len(seen) != 1 || seen[0] != r.action {
			t.Errorf("%s %s authorized %v, want [%s]", r.method, r.path, seen, r.action)
		}
	}
}

// TestHandler_ClosedByDefault: without Auth every route, and an unknown
// path, is refused before any operation runs.
func TestHandler_ClosedByDefault(t *testing.T) {
	for name, auth := range map[string]Auth[user]{
		"zero":         {},
		"no Authorize": {Identify: bearerAuth().Identify},
		"no Identify":  {Authorize: bearerAuth().Authorize},
	} {
		f := seeded()
		h := NewHandler(New(f), auth)
		for _, r := range append(routes, route{"GET", "/nope", "", ""}) {
			rec := do(t, h, r.method, r.path, r.body)
			if rec.Code != http.StatusForbidden || errCode(t, rec) != CodeForbidden {
				t.Errorf("%s: %s %s = %d %s, want 403", name, r.method, r.path, rec.Code, rec.Body)
			}
		}
		if c := f.Calls(); len(c) != 0 {
			t.Errorf("%s: operations ran: %v", name, c)
		}
	}
}

// TestHandler_AnonymousRefused walks every route without a token, with a
// panicking Identify, and with a forged form post.
func TestHandler_AnonymousRefused(t *testing.T) {
	f := seeded()
	h := NewHandler(New(f), bearerAuth())
	for _, r := range append(routes, route{"GET", "/nope", "", ""}) {
		req := httptest.NewRequest(r.method, r.path, strings.NewReader(r.body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized || errCode(t, rec) != CodeUnauthorized {
			t.Errorf("anonymous %s %s = %d %s, want 401", r.method, r.path, rec.Code, rec.Body)
		}
	}

	panicky := bearerAuth()
	panicky.Identify = func(*http.Request) (user, error) { panic("token store down") }
	hp := NewHandler(New(f), panicky)
	for _, r := range routes {
		if rec := do(t, hp, r.method, r.path, r.body); rec.Code != http.StatusUnauthorized {
			t.Errorf("panicking Identify: %s %s = %d, want 401", r.method, r.path, rec.Code)
		}
	}

	// A browser form post from another site cannot send a JSON content
	// type without a preflight, so writes without one are refused.
	for _, r := range routes {
		if r.method == http.MethodGet {
			continue
		}
		req := httptest.NewRequest(r.method, r.path, strings.NewReader(r.body))
		req.Header.Set("Authorization", "Bearer admin")
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnsupportedMediaType {
			t.Errorf("form post %s %s = %d, want 415", r.method, r.path, rec.Code)
		}
	}
	if c := f.Calls(); len(c) != 0 {
		t.Errorf("operations ran for refused requests: %v", c)
	}
}

// TestHandler_AuthorizeFailsClosed: an error or a panic from Authorize
// refuses every route, and a read-only role is refused every write.
func TestHandler_AuthorizeFailsClosed(t *testing.T) {
	for name, fn := range map[string]func(context.Context, Request[user]) error{
		"error": func(context.Context, Request[user]) error { return errors.New("policy store down") },
		"panic": func(context.Context, Request[user]) error { panic("bug") },
	} {
		f := seeded()
		auth := bearerAuth()
		auth.Authorize = fn
		h := NewHandler(New(f), auth)
		for _, r := range routes {
			if rec := do(t, h, r.method, r.path, r.body); rec.Code != http.StatusForbidden {
				t.Errorf("%s: %s %s = %d, want 403", name, r.method, r.path, rec.Code)
			}
		}
		if c := f.Calls(); len(c) != 0 {
			t.Errorf("%s: operations ran: %v", name, c)
		}
	}

	f := seeded()
	auth := bearerAuth()
	auth.Authorize = func(_ context.Context, req Request[user]) error {
		if !req.Action.ReadOnly() {
			return fmt.Errorf("%s is read-only", req.Who.name)
		}
		return nil
	}
	h := NewHandler(New(f, WithOutputs(NewMemoryOutputs(MemoryOutputLimits{})), WithMetrics(NewMemoryMetrics(MemoryMetricsConfig{}))), auth)
	for _, r := range routes {
		rec := do(t, h, r.method, r.path, r.body)
		want := okStatus(r)
		if !r.action.ReadOnly() {
			want = http.StatusForbidden
		}
		if rec.Code != want {
			t.Errorf("read-only: %s %s = %d, want %d (%s)", r.method, r.path, rec.Code, want, rec.Body)
		}
	}
	for _, c := range f.Calls() {
		if c != "Runs" && c != "Paused" && c != "Schedules" && c != "Nodes" {
			t.Errorf("read-only caller reached %s", c)
		}
	}
}

// TestHandler_ResourceScoping: the Authorizer sees the parsed filter, so
// it can confine a caller to one tenant's runs.
func TestHandler_ResourceScoping(t *testing.T) {
	auth := bearerAuth()
	auth.Authorize = func(_ context.Context, req Request[user]) error {
		if req.Resource.Query == nil || req.Resource.Query.Labels["tenant"] != "a" {
			return errors.New("scope to tenant a")
		}
		return nil
	}
	h := NewHandler(New(seeded()), auth)
	if rec := do(t, h, "GET", "/runs", ""); rec.Code != http.StatusForbidden {
		t.Errorf("unscoped list = %d, want 403", rec.Code)
	}
	rec := do(t, h, "GET", "/runs?label=tenant=a", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"run_id":"r2"`) || strings.Contains(rec.Body.String(), `"r1"`) {
		t.Errorf("scoped list = %d %s", rec.Code, rec.Body)
	}
}

func TestHandler_Runs(t *testing.T) {
	f := seeded()
	h := NewHandler(New(f), bearerAuth())

	rec := do(t, h, "GET", "/runs?queue=emails&status=queued,failed&limit=1", "")
	var page struct {
		Runs []struct {
			Ref     refJSON `json:"ref"`
			Payload []byte  `json:"payload"`
		} `json:"runs"`
		Next string `json:"next"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil || rec.Code != 200 {
		t.Fatalf("%d %s %v", rec.Code, rec.Body, err)
	}
	if len(page.Runs) != 1 || page.Runs[0].Ref.RunID != "r3" || page.Next == "" {
		t.Fatalf("page 1 = %+v", page)
	}
	if page.Runs[0].Payload != nil {
		t.Errorf("payload sent without WithPayloads")
	}
	rec = do(t, h, "GET", "/runs?queue=emails&status=queued,failed&limit=1&cursor="+page.Next, "")
	page.Runs, page.Next = nil, ""
	_ = json.Unmarshal(rec.Body.Bytes(), &page)
	if len(page.Runs) != 1 || page.Runs[0].Ref.RunID != "r1" || page.Next != "" {
		t.Fatalf("page 2 = %+v", page)
	}
	if q := f.queries[0]; !slices.Equal(q.Statuses, []workflow.Status{workflow.StatusQueued, workflow.StatusFailed}) || q.Queue != "emails" || q.Limit != 1 {
		t.Errorf("query = %+v", q)
	}

	hp := NewHandler(New(f), bearerAuth(), WithPayloads())
	if rec := do(t, hp, "GET", "/runs/r3", ""); !strings.Contains(rec.Body.String(), `"payload":"c2VjcmV0"`) {
		t.Errorf("WithPayloads: %s", rec.Body)
	}

	for _, bad := range []string{"/runs?status=nope", "/runs?limit=5000", "/runs?label=x", "/runs?from=yesterday", "/failures?status=failed", "/discarded?status=failed", "/schedules?limit=0"} {
		if rec := do(t, h, "GET", bad, ""); rec.Code != http.StatusBadRequest || errCode(t, rec) != CodeInvalid {
			t.Errorf("%s = %d, want 400", bad, rec.Code)
		}
	}

	rec = do(t, h, "GET", "/failures", "")
	if !strings.Contains(rec.Body.String(), `"failed_step":"send"`) || strings.Contains(rec.Body.String(), `"r3"`) {
		t.Errorf("failures = %s", rec.Body)
	}
	if rec := do(t, h, "GET", "/runs/missing", ""); rec.Code != http.StatusNotFound {
		t.Errorf("unknown run = %d, want 404", rec.Code)
	}
}

func TestHandler_ErrorMapping(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code int
		body string
	}{
		{fmt.Errorf("x: %w", workflow.ErrWrongState), http.StatusConflict, CodeConflict},
		{workflow.ErrDuplicate, http.StatusConflict, CodeConflict},
		{workflow.ErrNotFound, http.StatusNotFound, CodeNotFound},
		{&workflow.UnsupportedError{Field: "f"}, http.StatusNotImplemented, CodeUnsupported},
		{errors.New("pq: password=hunter2 connection refused"), http.StatusInternalServerError, CodeInternal},
	} {
		f := seeded()
		f.err = tc.err
		rec := do(t, NewHandler(New(f), bearerAuth()), "POST", "/runs/r2/retry", "")
		if rec.Code != tc.code || errCode(t, rec) != tc.body {
			t.Errorf("%v: %d %s", tc.err, rec.Code, rec.Body)
		}
		if strings.Contains(rec.Body.String(), "hunter2") {
			t.Errorf("internal error leaked: %s", rec.Body)
		}
	}
}

func TestHandler_Unsupported(t *testing.T) {
	h := NewHandler(New(&fakeClient{caps: workflow.CapRunQuery}), bearerAuth())
	rec := do(t, h, "POST", "/pause", `{"workflow":"w"}`)
	if rec.Code != http.StatusNotImplemented || !strings.Contains(rec.Body.String(), "PauseWorkflow") {
		t.Errorf("pause on a client without it = %d %s", rec.Code, rec.Body)
	}
	rec = do(t, h, "GET", "/capabilities", "")
	if !strings.Contains(rec.Body.String(), `{"operation":"retry","supported":false,"missing":"RetryRun"}`) ||
		!strings.Contains(rec.Body.String(), `{"operation":"runs","supported":true}`) {
		t.Errorf("capabilities = %s", rec.Body)
	}
}

func TestHandler_Writes(t *testing.T) {
	f := seeded()
	h := NewHandler(New(f), bearerAuth())
	rec := do(t, h, "POST", "/runs/cancel", `{"workflow":"w","statuses":["queued"]}`)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"cancelled":3`) {
		t.Fatalf("cancel all = %d %s", rec.Code, rec.Body)
	}
	for _, bad := range []string{`{"workflow":"w","limit":1}`, `{"statuses":["succeeded"]}`, `{} {}`, `not json`} {
		if rec := do(t, h, "POST", "/runs/cancel", bad); rec.Code != http.StatusBadRequest {
			t.Errorf("cancel body %s = %d, want 400", bad, rec.Code)
		}
	}
	for _, bad := range []string{`{}`, `{"workflow":"w","all":true}`, `{"queue":"bad queue"}`} {
		if rec := do(t, h, "POST", "/pause", bad); rec.Code != http.StatusBadRequest {
			t.Errorf("pause body %s = %d, want 400", bad, rec.Code)
		}
	}
	if rec := do(t, h, "POST", "/pause", `{"queue":"emails"}`); rec.Code != 200 {
		t.Fatalf("pause = %d %s", rec.Code, rec.Body)
	}
	if rec := do(t, h, "GET", "/pauses", ""); !strings.Contains(rec.Body.String(), `{"pauses":[{"queue":"emails"}]}`) {
		t.Errorf("pauses = %s", rec.Body)
	}
	if rec := do(t, h, "POST", "/runs/s9/cancel?scheduled=1", ""); rec.Code != 200 {
		t.Errorf("cancel scheduled = %d", rec.Code)
	}
	if rec := do(t, h, "DELETE", "/runs/r2", ""); rec.Code != 200 {
		t.Errorf("delete = %d", rec.Code)
	}
	want := []string{"CancelAll", "Pause", "Paused", "Cancel ", "Delete r2"}
	if got := f.Calls(); !slices.Equal(got, want) {
		t.Errorf("calls = %q, want %q", got, want)
	}
}

func TestHandler_SchedulesPaginate(t *testing.T) {
	f := seeded()
	f.schedules = nil
	for i := range 5 {
		f.schedules = append(f.schedules, workflow.ScheduleInfo{ID: workflow.ScheduleID(fmt.Sprint("s", i)), Workflow: "w", Cron: "0 9 * * *", Location: time.UTC,
			Windows: workflow.Windows{Between: []workflow.Window{{From: "09:00", To: "17:00"}}}})
	}
	h := NewHandler(New(f), bearerAuth())
	rec := do(t, h, "GET", "/schedules?limit=2&cursor=2", "")
	var out struct {
		Schedules []scheduleJSON `json:"schedules"`
		Next      string         `json:"next"`
		Total     int            `json:"total"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Schedules) != 2 || out.Schedules[0].ID != "s2" || out.Next != "4" || out.Total != 5 || out.Schedules[0].Location != "UTC" || out.Schedules[0].Between[0].From != "09:00" {
		t.Errorf("schedules = %+v", out)
	}
	rec = do(t, h, "GET", "/schedules?cursor=99", "")
	if !strings.Contains(rec.Body.String(), `"schedules":[]`) {
		t.Errorf("past the end = %s", rec.Body)
	}
}
