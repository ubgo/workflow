// Command admin-routes calls every route of the admin JSON API once and
// prints the method, the path, the status and a one-line summary of the
// answer, then makes the handler answer each of its error codes. It runs
// admin.NewHandler on an httptest server over a memory client with a fake
// clock, so every ID, time and count it prints is the same on every run.
//
// It is the program behind the routes and error-code sections of
// docs/admin/http-api.md: the guide copies its regions (marked
// "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/admin"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "admin-routes:", err)
		os.Exit(1)
	}
}

// start is the fake clock's first instant, so every time printed is fixed.
var start = time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)

// The tokens the example's Identify accepts: an operator who may do
// anything and a viewer who may only read.
const (
	adminToken  = "t-ann"
	viewerToken = "t-vic"
)

// The workflow, queue, step and event names the routes are called with.
const (
	wfETL       = "etl"
	wfEmail     = "send-email"
	wfCleanup   = "cleanup"
	wfRefresh   = "cache-refresh"
	wfReport    = "nightly-report"
	queueEmails = "emails"
	eventName   = "cache.refresh"
	// regionLabel keys each cache-refresh run's wait, so a signal can
	// reach one region's run and not the others.
	regionLabel = "region"
)

// delayedBy is how far ahead the delayed send-email run is due, so it is
// still waiting to start when it is rescheduled and edited.
const delayedBy = time.Hour

// The fixture's sizes, the data its steps log and the windows the routes
// are called with.
const (
	// etlRuns is how many failed etl runs the fixture leaves: two, so the
	// runs list pages with count=1 and one run can be reset while the
	// other answers the error-code calls.
	etlRuns = 2
	// defaultWorkers is the default queue's worker count, which /queues
	// and /nodes report.
	defaultWorkers = 2
	// extractedRows is what the etl extract step logs and returns.
	extractedRows = 42
	// badRow is the row the broken transform step fails on.
	badRow = 7
	// rescheduledBy moves the delayed run a further hour out, so it is
	// still waiting when it is edited and failed.
	rescheduledBy = 2 * delayedBy
	// metricsWindow is the range /metrics is asked for: two 5m points
	// that cover every run the fixture ends.
	metricsWindow = 10 * time.Minute
)

// user is the app's own principal type; admin.Auth is generic over it.
type user struct {
	Name     string
	ReadOnly bool
}

// tokens stands in for the app's session or API-token store.
var tokens = map[string]user{adminToken: {"ann", false}, viewerToken: {"vic", true}}

// auth logs a request in by its bearer token and lets a read-only user
// call only the routes whose Action is ReadOnly.
var auth = admin.Auth[user]{
	Identify: func(r *http.Request) (user, error) {
		u, ok := tokens[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]
		if !ok {
			return user{}, errors.New("no session")
		}
		return u, nil
	},
	Authorize: func(_ context.Context, req admin.Request[user]) error {
		if req.Who.ReadOnly && !req.Action.ReadOnly() {
			return fmt.Errorf("%s may only read", req.Who.Name)
		}
		return nil
	},
}

// snippet:codes
// apiError is the body of every error answer: {"error": {"code", "message"}}.
type apiError struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// nextStep says what a client of the API does about each error code. The
// codes are stable, so a client switches on them rather than on the
// message, which is for people.
func nextStep(code string) string {
	switch code {
	case admin.CodeInvalid:
		return "fix the request"
	case admin.CodeUnauthorized:
		return "log in again"
	case admin.CodeForbidden:
		return "ask for a role that allows it"
	case admin.CodeNotFound:
		return "refresh the list: the run or schedule is gone"
	case admin.CodeConflict:
		return "reload the run: its state changed"
	case admin.CodeMediaType:
		return "send Content-Type: application/json"
	case admin.CodeUnsupported:
		return "hide the button: the server cannot do it"
	case admin.CodeInternal:
		return "try again later: the server logged the cause"
	}
	return "unknown code " + code
}

// snippet:end

// fixture is the memory client the routes act on, with the runs each
// route needs, and the admin handlers in front of it.
type fixture struct {
	c        *memory.Client
	clk      *clock.Fake
	broken   *atomic.Bool // the etl transform step fails while true
	smtpDown *atomic.Bool // send-email fails for "bob" while true
	etl      [etlRuns]workflow.RunRef
	emails   map[string]workflow.RunRef // by recipient
	waiting  map[string]workflow.RunRef // cache-refresh runs by region
	schedule workflow.ScheduleID
}

// newFixture registers the workflows, starts the client and leaves it
// with two failed etl runs, three send-email runs (bob's failed), one
// discarded cleanup run and four cache-refresh runs waiting for their
// region's event. Steps log through middleware.OutputLogger, which
// CaptureOutput hands to outs.
func newFixture(ctx context.Context, clk *clock.Fake, outs *admin.MemoryOutputs, mm *admin.MemoryMetrics) (*fixture, error) {
	f := &fixture{
		clk:      clk,
		broken:   &atomic.Bool{},
		smtpDown: &atomic.Bool{},
		emails:   map[string]workflow.RunRef{},
		waiting:  map[string]workflow.RunRef{},
	}
	f.broken.Store(true)
	f.smtpDown.Store(true)
	capture := middleware.CaptureOutput(outs, middleware.CaptureClock(f.clk))
	f.c = memory.New(memory.WithClock(f.clk), memory.WithNodeID("worker-1"), memory.WithWorkers(defaultWorkers),
		// Declared queues are the ones the memory driver reports workers for.
		memory.WithQueues(workflow.QueueConfig{Name: workflow.DefaultQueue}, workflow.QueueConfig{Name: queueEmails, Workers: 1}),
		memory.WithMiddleware(capture))
	if err := f.c.OnRunFinished(mm.Observe); err != nil {
		return nil, err
	}
	// pass is a step that hands its input on unchanged.
	pass := func(_ context.Context, in []byte) ([]byte, error) { return in, nil }
	workflows := []workflow.Workflow{
		{Name: wfETL, Steps: []workflow.Step{
			{Name: "extract", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
				middleware.OutputLogger(ctx, nil).Info("extracted", "rows", extractedRows)
				return fmt.Appendf(nil, "%d", extractedRows), nil
			}},
			{Name: "transform", After: []string{"extract"}, Run: func(ctx context.Context, in []byte) ([]byte, error) {
				if f.broken.Load() {
					middleware.OutputLogger(ctx, nil).Error("bad date column", "row", badRow)
					return nil, workflow.Permanent(fmt.Errorf("transform: bad date in row %d", badRow))
				}
				return in, nil
			}},
			{Name: "publish", After: []string{"transform"}, Run: pass},
		}},
		{Name: wfEmail, Queue: queueEmails, Steps: []workflow.Step{{Name: "send", Run: func(_ context.Context, in []byte) ([]byte, error) {
			if string(in) == `"bob"` && f.smtpDown.Load() {
				return nil, workflow.Permanent(errors.New("smtp: connection refused"))
			}
			return nil, nil
		}}}},
		{Name: wfCleanup, Steps: []workflow.Step{{Name: "sweep", Run: func(context.Context, []byte) ([]byte, error) {
			return nil, workflow.Discard(errors.New("nothing to clean"))
		}}}},
		{Name: wfRefresh, Steps: []workflow.Step{
			{Name: "await", Wait: &workflow.WaitSpec{
				Event:   eventName,
				KeyFrom: workflow.KeyFunc(func(j workflow.Job) string { return j.Labels[regionLabel] }),
			}},
			{Name: "reload", After: []string{"await"}, Run: pass},
		}},
		{Name: wfReport, Steps: []workflow.Step{{Name: "build", Run: pass}}},
	}
	for _, wf := range workflows {
		if err := f.c.Register(wf); err != nil {
			return nil, err
		}
	}
	if err := f.c.Start(ctx); err != nil {
		return nil, err
	}
	// Every run is enqueued under a pause, so the IDs the driver hands out
	// do not depend on which runs its workers pick up first.
	if err := f.c.Pause(ctx, workflow.PauseAll()); err != nil {
		return nil, err
	}
	var err error
	if f.schedule, err = f.c.Schedule(ctx, workflow.Schedule{Name: "nightly", Workflow: wfReport, Cron: "0 2 * * *", Location: time.UTC}); err != nil {
		return nil, err
	}
	for i := range f.etl {
		if f.etl[i], err = f.c.Enqueue(ctx, workflow.Job{Workflow: wfETL, Labels: map[string]string{"team": "data-eng"}}); err != nil {
			return nil, err
		}
	}
	for _, to := range []string{"ann", "bob", "cat"} {
		if f.emails[to], err = f.c.Enqueue(ctx, workflow.Job{Workflow: wfEmail, Payload: []byte(`"` + to + `"`)}); err != nil {
			return nil, err
		}
	}
	if _, err = f.c.Enqueue(ctx, workflow.Job{Workflow: wfCleanup}); err != nil {
		return nil, err
	}
	for _, region := range []string{"eu", "us", "apac", "latam"} {
		if f.waiting[region], err = f.c.Enqueue(ctx, workflow.Job{Workflow: wfRefresh, Labels: map[string]string{regionLabel: region}}); err != nil {
			return nil, err
		}
	}
	if err := f.c.Resume(ctx, workflow.PauseAll()); err != nil {
		return nil, err
	}
	return f, f.settle(ctx)
}

// settle runs every run that can move until none can, so the next route
// sees a fixed state. The memory driver runs steps on its own goroutines.
func (f *fixture) settle(ctx context.Context) error {
	_, err := f.c.RunAll(ctx)
	return err
}

// caller sends requests to one handler mounted at /admin/ and prints a
// line per answer.
type caller struct {
	ctx context.Context
	srv *httptest.Server
	w   io.Writer
}

// call sends one request with token (none when empty) and a JSON body
// (none when empty), and returns the status and the body. A write gets
// Content-Type: application/json unless noType is set.
func (cl caller) call(method, path, token, body string, noType bool) (int, []byte, http.Header, error) {
	req, err := http.NewRequestWithContext(cl.ctx, method, cl.srv.URL+"/admin"+path, strings.NewReader(body))
	if err != nil {
		return 0, nil, nil, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if method != http.MethodGet && !noType {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := cl.srv.Client().Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	b, err := io.ReadAll(resp.Body)
	return resp.StatusCode, b, resp.Header, errors.Join(err, resp.Body.Close())
}

// show calls a route as the operator, prints "status METHOD path  summary"
// and returns the decoded body for the next call to use. summarize turns
// the JSON answer into the summary; nil prints none. The answer is decoded
// into a map[string]any on purpose: some thirty routes each answer a
// different shape, and the program only reads a field or two of each for
// a one-line summary, so one struct per route would add nothing.
func (cl caller) show(method, path, body string, summarize func(map[string]any) string) (map[string]any, error) {
	code, b, hdr, err := cl.call(method, path, adminToken, body, false)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if len(b) > 0 && strings.HasPrefix(hdr.Get("Content-Type"), "application/json") {
		if err := json.Unmarshal(b, &out); err != nil {
			return nil, fmt.Errorf("%s %s: %w", method, path, err)
		}
	}
	line := fmt.Sprintf("%d %s %s", code, method, path)
	if loc := hdr.Get("Location"); loc != "" {
		line += " -> " + loc
	}
	if code >= http.StatusBadRequest {
		return nil, fmt.Errorf("%s: %s", line, b)
	}
	if summarize != nil {
		line += "  " + summarize(out)
	}
	_, err = fmt.Fprintln(cl.w, line)
	return out, err
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, w io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	clk := clock.NewFake(start)
	outs := admin.NewMemoryOutputs(admin.MemoryOutputLimits{})
	mm := admin.NewMemoryMetrics(admin.MemoryMetricsConfig{Clock: clk})
	f, err := newFixture(ctx, clk, outs, mm)
	if err != nil {
		return err
	}
	defer f.c.Stop()

	// snippet:mount
	adm := admin.New(f.c,
		admin.WithClock(clk),    // queue waits measured on the app's clock
		admin.WithOutputs(outs), // GET /runs/{id}/output
		admin.WithMetrics(mm),   // GET /metrics
	)
	srv := httptest.NewServer(http.StripPrefix("/admin", admin.NewHandler(adm, auth)))
	// snippet:end
	defer srv.Close()
	srv.Client().CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	cl := caller{ctx: ctx, srv: srv, w: w}
	// id is the path segment that names a started run.
	id := func(ref workflow.RunRef) string { return ref.RunID.String() }

	fmt.Fprintln(w, "-- read")
	steps := []struct {
		method, path, body string
		sum                func(map[string]any) string
	}{
		{http.MethodGet, "/capabilities", "", func(m map[string]any) string {
			return fmt.Sprintf("%d operations, output=%v metrics=%v", len(list(m, "operations")), m["output"], m["metrics"])
		}},
		{http.MethodGet, "/runs?workflow=" + wfETL + "&count=1&limit=1", "", func(m map[string]any) string {
			return fmt.Sprintf("%d run on this page, total %v, pages %d", len(list(m, "runs")), m["total"], len(list(m, "cursors")))
		}},
		{http.MethodGet, "/runs?label=team=data*", "", func(m map[string]any) string {
			return fmt.Sprintf("%d runs with a team label starting data", len(list(m, "runs")))
		}},
		{http.MethodGet, "/runs/" + id(f.etl[0]), "", func(m map[string]any) string {
			return fmt.Sprintf("%v %v, failed step %v", m["workflow"], m["status"], m["failed_step"])
		}},
		{http.MethodGet, "/failures", "", func(m map[string]any) string { return fmt.Sprintf("%d failed runs", len(list(m, "runs"))) }},
		{http.MethodGet, "/discarded", "", func(m map[string]any) string {
			return fmt.Sprintf("%d discarded run of %v", len(list(m, "runs")), field(list(m, "runs"), 0, "workflow"))
		}},
		{http.MethodGet, "/runs/" + id(f.etl[1]) + "/output", "", func(m map[string]any) string {
			a := list(m, "attempts")
			return fmt.Sprintf("%d attempts captured, last: step %v, error %q", len(a), field(a, len(a)-1, "step"), field(a, len(a)-1, "error"))
		}},
	}
	for _, s := range steps {
		if _, err := cl.show(s.method, s.path, s.body, s.sum); err != nil {
			return err
		}
	}

	fmt.Fprintln(w, "-- act on many runs")
	// bulk summarizes a bulk action's answer: matched, done, dry run.
	bulk := func(m map[string]any) string {
		return fmt.Sprintf("matched %v, done %v, dry run %v", m["matched"], m["done"], m["dry_run"])
	}
	if _, err := cl.show(http.MethodPost, "/runs/cancel", `{"workflow":"`+wfRefresh+`","statuses":["waiting"],"dry_run":true}`, bulk); err != nil {
		return err
	}
	if _, err := cl.show(http.MethodPost, "/runs/retry", `{"workflow":"`+wfETL+`","dry_run":true}`, bulk); err != nil {
		return err
	}
	if _, err := cl.show(http.MethodPost, "/runs/delete", `{"workflow":"`+wfEmail+`","statuses":["succeeded"],"max":1}`, func(m map[string]any) string {
		return bulk(m) + fmt.Sprintf(", more match %v", m["truncated"])
	}); err != nil {
		return err
	}

	fmt.Fprintln(w, "-- act on one run")
	f.smtpDown.Store(false) // the mail server is back
	if _, err := cl.show(http.MethodPost, "/runs/"+id(f.emails["bob"])+"/retry", "", refSum); err != nil {
		return err
	}
	if _, err := cl.show(http.MethodPost, "/runs/"+id(f.waiting["us"])+"/cancel", "", okSum); err != nil {
		return err
	}
	if _, err := cl.show(http.MethodDelete, "/runs/"+id(f.emails["ann"]), "", okSum); err != nil {
		return err
	}
	if _, err := cl.show(http.MethodPost, "/runs/"+id(f.waiting["apac"])+"/steps/await/cancel", "", okSum); err != nil {
		return err
	}
	if err := awaitEnd(ctx, f.c, f.waiting["apac"]); err != nil {
		return err
	}
	f.broken.Store(false) // the transform fix is deployed
	if _, err := cl.show(http.MethodPost, "/runs/"+id(f.etl[0])+"/reset", `{"step":"transform"}`, refSum); err != nil {
		return err
	}
	if err := f.settle(ctx); err != nil {
		return err
	}
	if _, err := cl.show(http.MethodPost, "/runs/"+id(f.etl[0])+"/fork", `{"step":"publish","idempotency_key":"replay-etl","labels":{"ticket":"OPS-7"}}`, refSum); err != nil {
		return err
	}
	if _, err := cl.show(http.MethodPost, "/runs/"+id(f.waiting["latam"])+"/complete", "", okSum); err != nil {
		return err
	}
	if _, err := cl.show(http.MethodPost, "/signals/waiting", `{"name":"`+eventName+`","key":"eu"}`, func(m map[string]any) string {
		return fmt.Sprintf("resumed %v", m["resumed"])
	}); err != nil {
		return err
	}
	if err := f.settle(ctx); err != nil {
		return err
	}

	fmt.Fprintln(w, "-- pauses and queues")
	if _, err := cl.show(http.MethodPost, "/pause", `{"queue":"`+queueEmails+`"}`, okSum); err != nil {
		return err
	}
	if _, err := cl.show(http.MethodGet, "/pauses", "", func(m map[string]any) string {
		return fmt.Sprintf("%d active, queue %v", len(list(m, "pauses")), field(list(m, "pauses"), 0, "queue"))
	}); err != nil {
		return err
	}
	if _, err := cl.show(http.MethodPost, "/resume", `{"queue":"`+queueEmails+`"}`, okSum); err != nil {
		return err
	}
	if _, err := cl.show(http.MethodGet, "/queues", "", func(m map[string]any) string {
		q := list(m, "queues")
		return fmt.Sprintf("%d queues: %v workers %v, %v workers %v", len(q), field(q, 0, "queue"), field(q, 0, "workers"), field(q, 1, "queue"), field(q, 1, "workers"))
	}); err != nil {
		return err
	}

	fmt.Fprintln(w, "-- schedules")
	sid := string(f.schedule)
	if _, err := cl.show(http.MethodGet, "/schedules", "", func(m map[string]any) string {
		return fmt.Sprintf("%v schedule, next fire %v", m["total"], field(list(m, "schedules"), 0, "next"))
	}); err != nil {
		return err
	}
	if _, err := cl.show(http.MethodGet, "/schedules/"+sid, "", func(m map[string]any) string {
		return fmt.Sprintf("%v %q in %v, paused %v", m["workflow"], m["cron"], m["location"], m["paused"])
	}); err != nil {
		return err
	}
	for _, op := range []string{"pause", "resume"} {
		if _, err := cl.show(http.MethodPost, "/schedules/"+sid+"/"+op, "", okSum); err != nil {
			return err
		}
	}
	if _, err := cl.show(http.MethodPost, "/schedules/"+sid+"/trigger", "", refSum); err != nil {
		return err
	}
	if err := f.settle(ctx); err != nil {
		return err
	}

	fmt.Fprintln(w, "-- a delayed run")
	delayed, err := f.c.Enqueue(ctx, workflow.Job{Workflow: wfEmail, Payload: []byte(`"dan"`), RunAt: start.Add(delayedBy)})
	if err != nil {
		return err
	}
	// A delayed run has not started, so its ref holds only a ScheduledID;
	// ?scheduled=1 says the path's ID is one.
	sched := "/runs/" + string(delayed.ScheduledID)
	if _, err := cl.show(http.MethodGet, sched+"?scheduled=1", "", func(m map[string]any) string {
		return fmt.Sprintf("%v, due %v", m["status"], m["run_at"])
	}); err != nil {
		return err
	}
	if _, err := cl.show(http.MethodPost, sched+"/reschedule?scheduled=1", `{"at":"`+start.Add(rescheduledBy).Format(time.RFC3339)+`"}`, schedSum); err != nil {
		return err
	}
	if _, err := cl.show(http.MethodPost, sched+"/edit?scheduled=1", `{"payload":"dana","priority":5}`, schedSum); err != nil {
		return err
	}
	if _, err := cl.show(http.MethodPost, sched+"/fail?scheduled=1", `{"reason":"recipient left the company"}`, okSum); err != nil {
		return err
	}

	fmt.Fprintln(w, "-- charts, system and the UI")
	from := start.Format(time.RFC3339)
	to := start.Add(metricsWindow).Format(time.RFC3339)
	if _, err := cl.show(http.MethodGet, "/metrics?from="+from+"&to="+to+"&step=5m", "", func(m map[string]any) string {
		p := list(m, "points")
		return fmt.Sprintf("%d points, first: succeeded %v failed %v cancelled %v discarded %v", len(p),
			field(p, 0, "succeeded"), field(p, 0, "failed"), field(p, 0, "cancelled"), field(p, 0, "discarded"))
	}); err != nil {
		return err
	}
	if _, err := cl.show(http.MethodGet, "/nodes", "", func(m map[string]any) string {
		return fmt.Sprintf("%v, %v workers", field(list(m, "nodes"), 0, "id"), field(list(m, "nodes"), 0, "workers"))
	}); err != nil {
		return err
	}
	if _, err := cl.show(http.MethodGet, "/health", "", func(m map[string]any) string {
		return fmt.Sprintf("live %v ready %v, %d checks", m["live"], m["ready"], len(list(m, "checks")))
	}); err != nil {
		return err
	}
	for _, p := range []string{"/", "/ui", "/ui/"} {
		if _, err := cl.show(http.MethodGet, p, "", nil); err != nil {
			return err
		}
	}

	fmt.Fprintln(w, "-- error codes")
	// A second Admin over the same client with no metrics source and an
	// output store that is down, for the 501 and 500 answers.
	broken := admin.New(f.c, admin.WithOutputs(admin.OutputSourceFunc(func(context.Context, workflow.RunID) ([]admin.AttemptOutput, error) {
		return nil, errors.New("output store: connection reset")
	})))
	bare := httptest.NewServer(http.StripPrefix("/admin", admin.NewHandler(broken, auth)))
	defer bare.Close()
	errCalls := []struct {
		srv                *httptest.Server
		method, path, body string
		token              string
		noType             bool
	}{
		{srv, http.MethodPost, "/runs/delete", `{}`, adminToken, false},
		{srv, http.MethodGet, "/failures", "", "", false},
		{srv, http.MethodPost, "/runs/" + id(f.etl[1]) + "/retry", "", viewerToken, false},
		{srv, http.MethodGet, "/runs/no-such-run", "", adminToken, false},
		{srv, http.MethodPost, "/runs/" + id(f.etl[0]) + "/retry", "", adminToken, false},
		{srv, http.MethodPost, "/runs/" + id(f.etl[1]) + "/retry", "", adminToken, true},
		{bare, http.MethodGet, "/metrics", "", adminToken, false},
		{bare, http.MethodGet, "/runs/" + id(f.etl[1]) + "/output", "", adminToken, false},
	}
	for _, e := range errCalls {
		code, b, _, err := caller{ctx: ctx, srv: e.srv, w: w}.call(e.method, e.path, e.token, e.body, e.noType)
		if err != nil {
			return err
		}
		var ae apiError
		if err := json.Unmarshal(b, &ae); err != nil {
			return fmt.Errorf("%s %s: %w", e.method, e.path, err)
		}
		fmt.Fprintf(w, "%d %s %s  %s: %s\n", code, e.method, e.path, ae.Error.Code, nextStep(ae.Error.Code))
	}
	return nil
}

// refSum summarizes an answer carrying {"ref": {...}}.
func refSum(m map[string]any) string {
	ref, _ := m["ref"].(map[string]any)
	s := fmt.Sprintf("ref %v", ref["run_id"])
	if m["duplicate"] == true {
		s += " (duplicate)"
	}
	return s
}

// schedSum summarizes an answer carrying the ref of a run that has not
// started yet.
func schedSum(m map[string]any) string {
	ref, _ := m["ref"].(map[string]any)
	return fmt.Sprintf("ref %v", ref["scheduled_id"])
}

// okSum summarizes an answer carrying {"ok": true}.
func okSum(m map[string]any) string { return fmt.Sprintf("ok %v", m["ok"]) }

// list returns m[key] as a list, or nil. It and field read the generic
// JSON that show decodes (see show for why it is not a struct).
func list(m map[string]any, key string) []any {
	l, _ := m[key].([]any)
	return l
}

// field returns the key field of the i-th object in l, or nil.
func field(l []any, i int, key string) any {
	if i < 0 || i >= len(l) {
		return nil
	}
	o, _ := l[i].(map[string]any)
	return o[key]
}

// endPoll is how often awaitEnd looks at a run, and endTimeout how long it
// waits for one to end before giving up.
const (
	endPoll    = time.Millisecond
	endTimeout = 10 * time.Second
)

// awaitEnd waits until the run ref has ended. A step cancel on a parked
// step ends the run on the driver's own goroutine, after CancelStep has
// returned, so RunAll alone may come back before it.
func awaitEnd(ctx context.Context, c *memory.Client, ref workflow.RunRef) error {
	deadline := time.Now().Add(endTimeout)
	for {
		page, err := c.Runs(ctx, workflow.RunQuery{Ref: ref})
		if err != nil {
			return err
		}
		if len(page.Runs) == 1 && page.Runs[0].Status.IsTerminal() {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("run %s did not end within %s", ref.RunID, endTimeout)
		}
		time.Sleep(endPoll)
	}
}
