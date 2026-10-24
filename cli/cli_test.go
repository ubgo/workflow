package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// call is one request the fake API received.
type call struct {
	method, path, query, body string
	header                    http.Header
}

// fakeAPI answers canned JSON per "METHOD path" and records each call.
type fakeAPI struct {
	mu      sync.Mutex
	calls   []call
	answers map[string]func(r *http.Request) (int, string)
}

func (f *fakeAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	f.calls = append(f.calls, call{r.Method, r.URL.Path, r.URL.RawQuery, string(b), r.Header.Clone()})
	h := f.answers[r.Method+" "+r.URL.Path]
	f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if h == nil {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"error":{"code":"not_found","message":"no such route"}}`)
		return
	}
	code, body := h(r)
	w.WriteHeader(code)
	_, _ = io.WriteString(w, body)
}

func (f *fakeAPI) Calls() []call {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]call(nil), f.calls...)
}

func fixed(code int, body string) func(*http.Request) (int, string) {
	return func(*http.Request) (int, string) { return code, body }
}

const failedRun = `{"ref":{"run_id":"r1"},"workflow":"send","queue":"emails","status":"failed","error":"smtp 550","attempts":3,"enqueued_at":"2026-10-14T09:00:00Z","errors":[{"step":"send","attempt":1,"error":"smtp 421","at":"2026-10-14T09:00:01Z"}]}`

func newFake(t *testing.T) (*fakeAPI, *httptest.Server) {
	f := &fakeAPI{answers: map[string]func(*http.Request) (int, string){
		"GET /admin/runs/r1":                    fixed(200, failedRun),
		"GET /admin/failures":                   fixed(200, `{"runs":[`+failedRun+`],"next":"50"}`),
		"GET /admin/runs":                       fixed(200, `{"runs":[],"total":0}`),
		"POST /admin/runs/r1/retry":             fixed(200, `{"ref":{"run_id":"r1"}}`),
		"DELETE /admin/runs/r1":                 fixed(409, `{"error":{"code":"conflict","message":"run r1 is not finished"}}`),
		"POST /admin/runs/retry":                fixed(200, `{"matched":3,"done":0,"skipped":0,"truncated":false,"dry_run":true,"sample":[{"run_id":"a"},{"run_id":"b"},{"run_id":"c"}]}`),
		"POST /admin/runs/cancel":               fixed(200, `{"cancelled":4}`),
		"POST /admin/pause":                     fixed(200, `{"ok":true}`),
		"GET /admin/capabilities":               fixed(200, `{"capabilities":"Cancel|RunQuery","operations":[{"operation":"runs","supported":true},{"operation":"retry","supported":false,"missing":"RetryRun"}],"output":false,"metrics":true}`),
		"GET /admin/queues":                     fixed(200, `{"queues":[{"queue":"emails","depth":2,"oldest_wait_seconds":75}],"truncated":false}`),
		"GET /admin/schedules":                  fixed(200, `{"schedules":[{"id":"s1","name":"nightly","workflow":"w","cron":"0 2 * * *","tenant":"acme","location":"UTC","paused":false,"next":"2026-10-15T02:00:00Z","overlap":"allow"}],"total":1}`),
		"GET /admin/schedules/s1":               fixed(200, `{"id":"s1","workflow":"w","cron":"0 2 * * *","overlap":"skip"}`),
		"POST /admin/schedules/s1/trigger":      fixed(200, `{"ref":{"run_id":"t1"}}`),
		"POST /admin/runs/r1/reschedule":        fixed(200, `{"ref":{"run_id":"r1"}}`),
		"POST /admin/runs/r1/steps/send/cancel": fixed(200, `{"ok":true}`),
		"GET /admin/pauses":                     fixed(200, `{"pauses":[{"queue":"emails"}]}`),
		"GET /admin/health":                     fixed(200, `{"live":true,"ready":false,"checks":[{"name":"database","ok":false,"detail":"connection refused"}]}`),
		"GET /admin/nodes":                      fixed(200, `{"nodes":[{"id":"web-1:9","mode":"full","queues":[],"workers":4,"running":1}]}`),
		"GET /admin/metrics":                    fixed(200, `{"step_seconds":60,"points":[{"at":"2026-10-14T09:00:00Z","succeeded":5,"failed":1,"cancelled":0,"discarded":0},{"at":"2026-10-14T09:01:00Z","succeeded":2,"failed":0,"cancelled":1,"discarded":0}]}`),
	}}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return f, srv
}

// runCLI runs the CLI against srv with env and returns the exit code and
// both streams.
func runCLI(t *testing.T, srv *httptest.Server, env map[string]string, args ...string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	if env == nil {
		env = map[string]string{}
	}
	if _, ok := env[EnvURL]; !ok && srv != nil {
		env[EnvURL] = srv.URL + "/admin"
	}
	code := Run(context.Background(), args, Options{Stdout: &out, Stderr: &errb, Getenv: func(k string) string { return env[k] }})
	return code, out.String(), errb.String()
}

func TestRun_UsageErrorsExit2(t *testing.T) {
	f, srv := newFake(t)
	for _, args := range [][]string{
		{},
		{"nope"},
		{"retry"},
		{"retry", "r1", "r2"},
		{"retry", "--all", "r1"},
		{"retry", "r1", "--workflow", "w"},
		{"pause"},
		{"pause", "--queue", "q", "--everything"},
		{"runs", "--label", "novalue"},
		{"runs", "--from", "yesterday"},
		{"failures", "--status", "failed"},
		{"delete", "--all", "--max", "-1"},
		{"runs", "--bogus"},
		{"drain", "--wait", "0s"},
		{"--header", "no colon here", "caps"},
	} {
		if code, _, stderr := runCLI(t, srv, nil, args...); code != ExitUsage || stderr == "" {
			t.Errorf("%q: exit %d stderr %q, want 2 with a message", args, code, stderr)
		}
	}
	if c := f.Calls(); len(c) != 0 {
		t.Errorf("usage errors reached the API: %v", c)
	}
	if code, _, stderr := runCLI(t, nil, map[string]string{}, "caps"); code != ExitUsage || !strings.Contains(stderr, EnvURL) {
		t.Errorf("no URL: %d %q", code, stderr)
	}
}

func TestRun_HelpExitsZero(t *testing.T) {
	for _, args := range [][]string{{"help"}, {"--help"}, {"help", "retry"}, {"retry", "--help"}} {
		code, out, _ := runCLI(t, nil, nil, args...)
		if code != ExitOK || out == "" {
			t.Errorf("%q: exit %d out %q", args, code, out)
		}
	}
}

// TestRun_AuthHeadersAndTokenNeverPrinted: the token and headers reach
// the API, and the token never appears in any output, not even help.
func TestRun_AuthHeadersAndTokenNeverPrinted(t *testing.T) {
	f, srv := newFake(t)
	env := map[string]string{EnvToken: "s3cret-token", EnvHeader: "X-Tenant: acme"}
	code, out, errOut := runCLI(t, srv, env, "--header", "Cookie: s=ok", "caps")
	if code != ExitOK {
		t.Fatalf("exit %d %s", code, errOut)
	}
	h := f.Calls()[0].header
	if h.Get("Authorization") != "Bearer s3cret-token" || h.Get("X-Tenant") != "acme" || h.Get("Cookie") != "s=ok" {
		t.Errorf("headers = %v", h)
	}
	_, help, _ := runCLI(t, srv, env, "help", "caps")
	_, _, failErr := runCLI(t, srv, env, "inspect", "missing")
	for _, s := range []string{out, errOut, help, failErr} {
		if strings.Contains(s, "s3cret") {
			t.Errorf("token printed: %q", s)
		}
	}
	// A later --token wins over the environment and the top level.
	runCLI(t, srv, env, "--token", "a", "caps", "--token", "b")
	if got := f.Calls()[len(f.Calls())-1].header.Get("Authorization"); got != "Bearer b" {
		t.Errorf("Authorization = %q, want Bearer b", got)
	}
}

func TestRun_APIErrorExit1(t *testing.T) {
	_, srv := newFake(t)
	code, _, stderr := runCLI(t, srv, nil, "delete", "r1")
	if code != ExitError || !strings.Contains(stderr, "409") || !strings.Contains(stderr, "not finished") {
		t.Errorf("delete conflict: %d %q", code, stderr)
	}
	// An answer that is not the API's JSON.
	plain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "<html>") }))
	defer plain.Close()
	if code, _, stderr := runCLI(t, nil, map[string]string{EnvURL: plain.URL}, "caps"); code != ExitError || !strings.Contains(stderr, "--url") {
		t.Errorf("html answer: %d %q", code, stderr)
	}
}

func TestRun_ListAndJSON(t *testing.T) {
	f, srv := newFake(t)
	code, out, _ := runCLI(t, srv, nil, "failures", "--workflow", "send", "--label", "tenant=ac*", "--limit", "5")
	if code != ExitOK || !strings.Contains(out, "r1") || !strings.Contains(out, "smtp 550") || !strings.Contains(out, "--cursor 50") {
		t.Fatalf("failures: %d %q", code, out)
	}
	if q := f.Calls()[0].query; !strings.Contains(q, "workflow=send") || !strings.Contains(q, "label=tenant%3Dac%2A") || !strings.Contains(q, "limit=5") {
		t.Errorf("query = %s", q)
	}
	code, out, _ = runCLI(t, srv, nil, "inspect", "r1", "--json")
	var r map[string]any
	if code != ExitOK || json.Unmarshal([]byte(out), &r) != nil || r["status"] != "failed" {
		t.Errorf("inspect --json: %d %q", code, out)
	}
	code, out, _ = runCLI(t, srv, nil, "inspect", "r1")
	if code != ExitOK || !strings.Contains(out, "failed attempts") || !strings.Contains(out, "smtp 421") {
		t.Errorf("inspect: %q", out)
	}
	for _, args := range [][]string{{"caps"}, {"queues"}, {"schedules"}, {"schedule", "s1"}, {"metrics", "--window", "2m"}} {
		if code, out, errOut := runCLI(t, srv, nil, args...); code != ExitOK || out == "" {
			t.Errorf("%q: %d %q %q", args, code, out, errOut)
		}
	}
	_, out, _ = runCLI(t, srv, nil, "metrics")
	if !strings.Contains(out, "total") || !strings.Contains(out, "7") {
		t.Errorf("metrics totals: %q", out)
	}
}

// TestRun_DryRunNeverWrites: every destructive command with --dry-run
// sends only GETs, or the bulk preview body with dry_run set.
func TestRun_DryRunNeverWrites(t *testing.T) {
	f, srv := newFake(t)
	for _, args := range [][]string{
		{"retry", "r1", "--dry-run"},
		{"cancel", "--all", "--queue", "emails", "--dry-run"},
		{"retry", "--all", "--workflow", "send", "--dry-run"},
		{"delete", "--all", "--status", "failed", "--dry-run"},
		{"trigger", "s1", "--dry-run"},
	} {
		runCLI(t, srv, nil, args...)
	}
	for _, c := range f.Calls() {
		if c.method == http.MethodGet {
			continue
		}
		var b map[string]any
		if json.Unmarshal([]byte(c.body), &b) != nil || b["dry_run"] != true {
			t.Errorf("dry run wrote: %s %s %s", c.method, c.path, c.body)
		}
	}
	// A dry run of an action the server would refuse says so and exits 1:
	// a failed run cannot be cancelled. One it would accept exits 0.
	_, srv2 := newFake(t)
	code, _, stderr := runCLI(t, srv2, nil, "cancel", "r1", "--dry-run")
	if code != ExitError || !strings.Contains(stderr, "would be refused") {
		t.Errorf("dry cancel of a failed run: %d %q", code, stderr)
	}
	if code, out, _ := runCLI(t, srv2, nil, "delete", "r1", "--dry-run"); code != ExitOK || !strings.Contains(out, "would delete run r1") {
		t.Errorf("dry delete of a failed run: %d %q", code, out)
	}
}

func TestRun_BulkBodyAndOutput(t *testing.T) {
	f, srv := newFake(t)
	code, out, _ := runCLI(t, srv, nil, "retry", "--all", "--workflow", "send", "--label", "tenant=acme", "--label", "region=eu*", "--dry-run", "--max", "10")
	if code != ExitOK || !strings.Contains(out, "would retry 3 runs") {
		t.Fatalf("%d %q", code, out)
	}
	var b filterBody
	c := f.Calls()[0]
	if err := json.Unmarshal([]byte(c.body), &b); err != nil || c.header.Get("Content-Type") != "application/json" {
		t.Fatal(err, c.header)
	}
	if b.Workflow != "send" || b.Labels["tenant"] != "acme" || b.LabelPrefixes["region"] != "eu" || !b.DryRun || b.Max != 10 {
		t.Errorf("body = %+v", b)
	}
	code, out, _ = runCLI(t, srv, nil, "cancel", "--all", "--queue", "emails")
	if code != ExitOK || !strings.Contains(out, "cancelled 4") {
		t.Errorf("cancel --all: %q", out)
	}
	// A write with no body still sends JSON (the API's CSRF guard).
	runCLI(t, srv, nil, "retry", "r1")
	last := f.Calls()[len(f.Calls())-1]
	if last.path != "/admin/runs/r1/retry" || last.body != "{}" || last.header.Get("Content-Type") != "application/json" {
		t.Errorf("retry r1 sent %+v", last)
	}
	// Flags after the positional argument still apply.
	if code, out, _ := runCLI(t, srv, nil, "pause", "--queue", "emails"); code != ExitOK || out != "paused queue emails\n" {
		t.Errorf("pause: %q", out)
	}
}

func TestRun_RescheduleCancelStepPauseTenant(t *testing.T) {
	f, srv := newFake(t)
	if code, out, _ := runCLI(t, srv, nil, "reschedule", "r1", "--at", "2026-10-15T09:00:00Z"); code != ExitOK || !strings.Contains(out, "rescheduled r1") {
		t.Errorf("reschedule: %d %q", code, out)
	}
	c := f.Calls()[0]
	if c.path != "/admin/runs/r1/reschedule" || !strings.Contains(c.body, `"at":"2026-10-15T09:00:00Z"`) {
		t.Errorf("reschedule sent %+v", c)
	}
	if code, _, _ := runCLI(t, srv, nil, "reschedule", "r1", "--at", "tomorrow"); code != ExitUsage {
		t.Errorf("bad --at = %d", code)
	}
	// r1 is failed: a dry run says the move would be refused.
	if code, _, stderr := runCLI(t, srv, nil, "reschedule", "r1", "--at", "2026-10-15T09:00:00Z", "--dry-run"); code != ExitError || !strings.Contains(stderr, "would be refused") {
		t.Errorf("dry reschedule: %d %q", code, stderr)
	}
	if code, out, _ := runCLI(t, srv, nil, "cancel-step", "r1", "send"); code != ExitOK || !strings.Contains(out, "cancelled step send") {
		t.Errorf("cancel-step: %d %q", code, out)
	}
	if code, _, stderr := runCLI(t, srv, nil, "cancel-step", "r1", "send", "--dry-run"); code != ExitError || !strings.Contains(stderr, "would be refused") {
		t.Errorf("dry cancel-step of a failed run: %d %q", code, stderr)
	}
	if code, _, _ := runCLI(t, srv, nil, "cancel-step", "r1"); code != ExitUsage {
		t.Errorf("cancel-step without a step = %d", code)
	}
	n := len(f.Calls())
	if code, out, _ := runCLI(t, srv, nil, "pause", "--queue", "emails", "--dry-run"); code != ExitOK || !strings.Contains(out, "would pause queue emails (paused now") {
		t.Errorf("dry pause: %d %q", code, out)
	}
	if code, out, _ := runCLI(t, srv, nil, "resume", "--workflow", "w", "--dry-run"); code != ExitOK || !strings.Contains(out, "not paused now") {
		t.Errorf("dry resume: %d %q", code, out)
	}
	for _, c := range f.Calls()[n:] {
		if c.method != http.MethodGet {
			t.Errorf("dry pause wrote %s %s", c.method, c.path)
		}
	}
	runCLI(t, srv, nil, "runs", "--tenant", "acme")
	if q := f.Calls()[len(f.Calls())-1].query; !strings.Contains(q, "tenant=acme") {
		t.Errorf("tenant filter: %s", q)
	}
	runCLI(t, srv, nil, "retry", "--all", "--tenant", "acme", "--dry-run")
	if b := f.Calls()[len(f.Calls())-1].body; !strings.Contains(b, `"tenant":"acme"`) {
		t.Errorf("tenant in bulk body: %s", b)
	}
}

func TestRun_SchedulesTenantColumn(t *testing.T) {
	_, srv := newFake(t)
	code, out, _ := runCLI(t, srv, nil, "schedules")
	lines := strings.Split(out, "\n")
	if code != ExitOK || !strings.Contains(lines[0], "TENANT") || !strings.Contains(lines[1], "acme") {
		t.Errorf("schedules: %d %q", code, out)
	}
	// A schedule without a tenant shows a dash, and the detail omits the row.
	if code, out, _ := runCLI(t, srv, nil, "schedule", "s1"); code != ExitOK || strings.Contains(out, "tenant") {
		t.Errorf("schedule s1: %d %q", code, out)
	}
}

func TestRun_HealthAndNodes(t *testing.T) {
	_, srv := newFake(t)
	if code, out, stderr := runCLI(t, srv, nil, "health"); code != ExitError || !strings.Contains(out, "connection refused") || !strings.Contains(stderr, "not ready") {
		t.Errorf("health not ready: %d %q %q", code, out, stderr)
	}
	if code, out, _ := runCLI(t, srv, nil, "nodes"); code != ExitOK || !strings.Contains(out, "web-1:9") || !strings.Contains(out, "all") {
		t.Errorf("nodes: %d %q", code, out)
	}
}

func TestRun_Drain(t *testing.T) {
	left := []int{3, 1, 0}
	var mu sync.Mutex
	f, srv := newFake(t)
	f.answers["GET /admin/runs"] = func(r *http.Request) (int, string) {
		mu.Lock()
		defer mu.Unlock()
		n := left[0]
		if len(left) > 1 {
			left = left[1:]
		}
		return 200, `{"runs":[],"total":` + strconv.Itoa(n) + `}`
	}
	now := time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)
	var slept []time.Duration
	opts := func(out, errb *bytes.Buffer) Options {
		return Options{Stdout: out, Stderr: errb, Getenv: func(k string) string {
			if k == EnvURL {
				return srv.URL + "/admin"
			}
			return ""
		}, Now: func() time.Time { return now }, Sleep: func(_ context.Context, d time.Duration) error {
			if len(slept) > 100 {
				// A drain that never gives up would spin here forever.
				return errors.New("test: drain did not stop")
			}
			slept = append(slept, d)
			now = now.Add(d)
			return nil
		}}
	}
	var out, errb bytes.Buffer
	code := Run(context.Background(), []string{"drain", "--queue", "emails", "--interval", "5s"}, opts(&out, &errb))
	if code != ExitOK || len(slept) != 2 || !strings.Contains(out.String(), "drained after 10s") {
		t.Fatalf("drain: %d %q %v", code, out.String(), slept)
	}
	if q := f.Calls()[0].query; !strings.Contains(q, "queue=emails") || !strings.Contains(q, "status=queued%2Crunning%2Cretrying") || !strings.Contains(q, "count=1") {
		t.Errorf("drain query %s", q)
	}
	// Never empty: timeout exits 1.
	left = []int{2}
	slept = nil
	out.Reset()
	errb.Reset()
	code = Run(context.Background(), []string{"drain", "--wait", "6s", "--interval", "2s"}, opts(&out, &errb))
	if code != ExitError || !strings.Contains(errb.String(), "2 runs still") {
		t.Errorf("timeout: %d %q", code, errb.String())
	}
}
