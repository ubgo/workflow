// Command admin-audit shows two additions to the admin HTTP handler on the
// memory driver: an audit log of every write action (who retried,
// cancelled or deleted what, how it ended and when, refused attempts
// included), and a link from each run's detail to its trace, built from
// the trace ID the OpenTelemetry middleware carried in the run's labels.
//
// It is the program behind docs/admin/audit-and-traces.md: the guide
// copies its regions (marked "snippet:") and task docs:snippets fails
// when they drift.
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

	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/admin"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	wfotel "github.com/ubgo/workflow/otel"
)

// awaitPoll is how often Await re-reads the run: a few milliseconds keeps
// the in-memory demo fast without spinning.
const awaitPoll = 2 * time.Millisecond

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "admin-audit:", err)
		os.Exit(1)
	}
}

// role is the app's own closed set of principal roles.
type role string

// The roles the demo's tokens carry: an admin may write, a viewer only read.
const (
	roleAdmin  role = "admin"
	roleViewer role = "viewer"
)

// user is the app's own principal type.
type user struct {
	Name string
	Role role
}

// tokens maps each bearer token to its user, standing in for a session store.
var tokens = map[string]user{"t-ann": {"ann", roleAdmin}, "t-vic": {"vic", roleViewer}}

// auth identifies the caller from the bearer token and lets admins write
// while everyone else may only read.
var auth = admin.Auth[user]{
	Identify: func(r *http.Request) (user, error) {
		u, ok := tokens[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]
		if !ok {
			return user{}, errors.New("no session")
		}
		return u, nil
	},
	Authorize: func(_ context.Context, req admin.Request[user]) error {
		if req.Who.Role == roleAdmin || req.Action.ReadOnly() {
			return nil
		}
		return errors.New("read only")
	},
}

// snippet:tracelink
// traceLink builds the run page's "Open trace" link from the trace ID the
// otel enqueue middleware carried in the run's labels. A run enqueued
// without that middleware carries none, and gets no link.
func traceLink(r workflow.RunInfo) string {
	id, ok := wfotel.TraceIDOf(r.Labels) // 32 hex digits from the W3C traceparent
	if !ok {
		return ""
	}
	return "https://tracing.example.com/trace/" + id
}

// snippet:end

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var calls atomic.Int32
	charge := workflow.Workflow{Name: "charge", Steps: []workflow.Step{{Name: "card", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		if calls.Add(1) == 1 {
			return nil, workflow.Permanent(errors.New("card network down"))
		}
		return []byte(`"charged"`), nil
	}}}}
	c := memory.New()
	if err := c.Register(charge); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}

	// The otel enqueue middleware carries the caller's trace in the run's
	// labels. Here the caller's span is a fixed one, so the output is
	// stable; an app's comes from its incoming request.
	inst, err := wfotel.New(wfotel.WithTracerProvider(noop.NewTracerProvider()))
	if err != nil {
		return err
	}
	ref, err := inst.Enqueue()(c.Enqueue)(requestSpan(ctx), workflow.Job{Workflow: "charge"})
	if err != nil {
		return err
	}
	if _, err := workflow.Await(ctx, c, ref, workflow.AwaitOptions{Poll: awaitPoll}); err != nil {
		return err
	}

	clk := clock.NewFake(time.Date(2026, 11, 20, 9, 30, 0, 0, time.UTC))
	// snippet:handler
	audit := &admin.MemoryAudit{} // an app writes to its own audit table
	h := admin.NewHandler(admin.New(c, admin.WithClock(clk)), auth,
		admin.WithAudit(audit),
		admin.WithTraceLink(traceLink))
	// snippet:end
	srv := httptest.NewServer(http.StripPrefix("/admin", h))
	defer srv.Close()
	runURL := srv.URL + "/admin/runs/" + ref.RunID.String()

	fmt.Println("actions:")
	for _, a := range []struct{ token, method, url string }{
		{"t-vic", http.MethodPost, runURL + "/retry"}, // a viewer may not
		{"t-ann", http.MethodPost, runURL + "/retry"}, // an admin may
		{"t-ann", http.MethodGet, runURL},             // reads are not audited
		{"t-ann", http.MethodDelete, srv.URL + "/admin/runs/no-such-run"},
	} {
		code, _, err := call(ctx, a.token, a.method, a.url)
		if err != nil {
			return err
		}
		path := strings.Replace(strings.TrimPrefix(a.url, srv.URL), ref.RunID.String(), "<run>", 1)
		fmt.Printf("  %s %s %s: %d\n", tokens[a.token].Name, a.method, path, code)
	}
	if _, err := workflow.Await(ctx, c, ref, workflow.AwaitOptions{Poll: awaitPoll}); err != nil {
		return err
	}

	// snippet:entries
	fmt.Println("audit log:")
	for _, e := range audit.Entries() {
		who := e.Who.(user) // the handler's own principal type
		outcome := "ok"
		if errors.Is(e.Err, workflow.ErrNotFound) {
			outcome = "not found"
		} else if e.Err != nil {
			outcome = e.Err.Error()
		}
		run := strings.Replace(e.Resource.Run.RunID.String(), ref.RunID.String(), "<run>", 1)
		fmt.Printf("  %s %s %s %s allowed=%v %d %s\n", e.At.Format(time.TimeOnly), who.Name, e.Action, run, e.Allowed, e.Status, outcome)
	}
	// snippet:end

	_, body, err := call(ctx, "t-ann", http.MethodGet, runURL)
	if err != nil {
		return err
	}
	var detail struct {
		Status   string `json:"status"`
		TraceURL string `json:"trace_url"`
	}
	if err := json.Unmarshal(body, &detail); err != nil {
		return err
	}
	fmt.Println("run detail:")
	fmt.Printf("  status %s\n  trace_url %s\n", detail.Status, detail.TraceURL)

	page, err := c.Runs(ctx, workflow.RunQuery{Ref: ref})
	if err != nil {
		return err
	}
	// snippet:spancontext
	// The whole span context, for code that starts a span linked to the
	// enqueue (nil: the W3C propagator, the otel module's default).
	sc := wfotel.SpanContextOf(page.Runs[0].Labels, nil)
	fmt.Printf("  span context: span %s, sampled %v\n", sc.SpanID(), sc.IsSampled())
	// snippet:end
	return nil
}

// requestSpan stands in for the span of the request that enqueued the
// job.
func requestSpan(ctx context.Context) context.Context {
	tid, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	sid, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
	return trace.ContextWithSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled}))
}

// call sends one admin request with a bearer token.
func call(ctx context.Context, token, method, url string) (int, []byte, error) {
	var body io.Reader
	if method != http.MethodGet {
		body = strings.NewReader(`{}`)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if method != http.MethodGet {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return resp.StatusCode, b, err
}
