// Command admin runs the operations side of ubgo/workflow on the memory
// driver: the admin JSON API behind an app's own login (closed until the
// app says who may do what), a failed run listed and retried from the step
// that failed, a queue paused and resumed, a queue-depth alert from the
// Monitor, and the same depth as an OpenTelemetry gauge.
//
// It is the program behind docs/admin/*.md and the queue metrics in
// docs/observability/opentelemetry.md: the guides copy its regions (marked
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

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/admin"
	"github.com/ubgo/workflow/memory"
	wfotel "github.com/ubgo/workflow/otel"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "admin:", err)
		os.Exit(1)
	}
}

// snippet:auth
// user is the app's own principal type; admin.Auth is generic over it.
type user struct {
	Name string
	Role string // "admin" may do anything, "viewer" may only read
}

// tokens stands in for the app's session or API-token store.
var tokens = map[string]user{"t-ann": {"ann", "admin"}, "t-vic": {"vic", "viewer"}}

var auth = admin.Auth[user]{
	Identify: func(r *http.Request) (user, error) {
		u, ok := tokens[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]
		if !ok {
			return user{}, errors.New("no session")
		}
		return u, nil
	},
	Authorize: func(_ context.Context, req admin.Request[user]) error {
		if req.Who.Role == "admin" || (req.Who.Role == "viewer" && req.Action.ReadOnly()) {
			return nil
		}
		return fmt.Errorf("%s may not %s", req.Who.Name, req.Action)
	},
}

// snippet:end

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var smtpDown atomic.Bool
	smtpDown.Store(true)
	c := memory.New()
	if err := c.Register(workflow.Workflow{
		Name:  "send-email",
		Queue: "emails",
		Steps: []workflow.Step{
			{Name: "render", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }},
			{Name: "send", After: []string{"render"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
				if string(in) == `"bob"` && smtpDown.Load() {
					return nil, workflow.Permanent(errors.New("smtp: connection refused"))
				}
				return nil, nil
			}},
		},
	}); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	for _, to := range []string{"ann", "bob", "cat"} {
		if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "send-email", Payload: []byte(`"` + to + `"`)}); err != nil {
			return err
		}
	}
	if err := waitFor(ctx, c, map[workflow.Status]int{workflow.StatusSucceeded: 2, workflow.StatusFailed: 1}); err != nil {
		return err
	}

	// snippet:mount
	adm := admin.New(c)
	mux := http.NewServeMux()
	mux.Handle("/admin/", http.StripPrefix("/admin", admin.NewHandler(adm, auth)))
	// snippet:end
	srv := httptest.NewServer(mux)
	defer srv.Close()
	call := func(method, path, token, body string) (int, string) {
		req, _ := http.NewRequestWithContext(ctx, method, srv.URL+path, strings.NewReader(body))
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		if method != http.MethodGet {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := srv.Client().Do(req)
		if err != nil {
			return 0, err.Error()
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, strings.TrimSpace(string(b))
	}

	code, _ := call("GET", "/admin/failures", "", "")
	fmt.Fprintln(out, "anonymous GET /failures:", code)

	code, body := call("GET", "/admin/failures", "t-vic", "")
	var page struct {
		Runs []struct {
			Ref struct {
				RunID string `json:"run_id"`
			} `json:"ref"`
			Error      string `json:"error"`
			FailedStep string `json:"failed_step"`
		} `json:"runs"`
	}
	if err := json.Unmarshal([]byte(body), &page); err != nil || len(page.Runs) != 1 {
		return fmt.Errorf("failures: %d %s", code, body)
	}
	failed := page.Runs[0]
	fmt.Fprintf(out, "viewer GET /failures: %d, 1 failed run, step %q: %s\n", code, failed.FailedStep, failed.Error)

	code, body = call("POST", "/admin/runs/"+failed.Ref.RunID+"/retry", "t-vic", "")
	fmt.Fprintln(out, "viewer POST retry:", code, body)

	smtpDown.Store(false)
	code, _ = call("POST", "/admin/runs/"+failed.Ref.RunID+"/retry", "t-ann", "")
	if err := waitFor(ctx, c, map[workflow.Status]int{workflow.StatusSucceeded: 3, workflow.StatusFailed: 0}); err != nil {
		return err
	}
	info, err := adm.Run(ctx, workflow.RunRef{RunID: workflow.RunID(failed.Ref.RunID)})
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "admin POST retry: %d, run now %s\n", code, info.Status)

	// Pause the queue: new runs are accepted and wait.
	code, _ = call("POST", "/admin/pause", "t-ann", `{"queue":"emails"}`)
	fmt.Fprintln(out, "admin POST /pause emails:", code)
	for _, to := range []string{"dan", "eve", "fay"} {
		if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "send-email", Payload: []byte(`"` + to + `"`)}); err != nil {
			return err
		}
	}
	if err := waitFor(ctx, c, map[workflow.Status]int{workflow.StatusQueued: 3}); err != nil {
		return err
	}

	// snippet:monitor
	m, err := admin.NewMonitor(adm, admin.AlerterFunc(func(_ context.Context, a admin.Alert) {
		state := "ALERT"
		if a.Resolved {
			state = "resolved"
		}
		fmt.Fprintf(out, "%s %s queue=%s depth=%d (max %d)\n", state, a.Kind, a.Queue, a.Depth, a.Threshold.MaxDepth)
	}), []admin.Threshold{{Queue: "emails", MaxDepth: 2}, {MaxDepth: 1000, MaxWait: 10 * time.Minute}},
		admin.WithInterval(30*time.Second))
	if err != nil {
		return err
	}
	// go m.Run(ctx) polls every 30s until ctx ends; Check polls once.
	if err := m.Check(ctx); err != nil {
		return err
	}
	// snippet:end

	// snippet:gauge
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	inst, err := wfotel.New(wfotel.WithMeterProvider(mp))
	if err != nil {
		return err
	}
	reg, err := inst.ObserveQueueDepth(adm.QueueDepths) // read at every collection
	if err != nil {
		return err
	}
	defer reg.Unregister()
	// snippet:end
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		return err
	}
	for _, sm := range rm.ScopeMetrics {
		for _, mt := range sm.Metrics {
			if g, ok := mt.Data.(metricdata.Gauge[int64]); ok && mt.Name == wfotel.MetricQueueDepth {
				for _, p := range g.DataPoints {
					q, _ := p.Attributes.Value(wfotel.AttrQueue)
					fmt.Fprintf(out, "%s{workflow.queue=%s} = %d\n", mt.Name, q.AsString(), p.Value)
				}
			}
		}
	}

	code, _ = call("POST", "/admin/resume", "t-ann", `{"queue":"emails"}`)
	fmt.Fprintln(out, "admin POST /resume emails:", code)
	if err := waitFor(ctx, c, map[workflow.Status]int{workflow.StatusSucceeded: 6, workflow.StatusQueued: 0}); err != nil {
		return err
	}
	return m.Check(ctx)
}

// waitFor polls until each listed status holds exactly that many runs.
// The memory driver runs steps on its own goroutines, so the example
// waits rather than sleeps.
func waitFor(ctx context.Context, c *memory.Client, want map[workflow.Status]int) error {
	deadline := time.Now().Add(10 * time.Second)
	for {
		done := true
		for st, n := range want {
			page, err := c.Runs(ctx, workflow.RunQuery{Statuses: []workflow.Status{st}})
			if err != nil {
				return err
			}
			done = done && len(page.Runs) == n
		}
		if done {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for %v", want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
