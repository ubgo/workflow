package middleware_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/middleware"
)

// pingRecorder is a PingFunc that records each target and returns err.
type pingRecorder struct {
	mu      sync.Mutex
	targets []string
	err     error
}

func (r *pingRecorder) send(_ context.Context, target string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.targets = append(r.targets, target)
	return r.err
}

func TestPinger_TickPingsOnlyFiredRuns(t *testing.T) {
	rec := &pingRecorder{}
	hook := middleware.NewPinger(rec.send).Tick("https://hc.example/start")
	ctx := context.Background()
	for _, tick := range []workflow.Tick{
		{Workflow: "backup", Outcome: workflow.TickFired, Run: workflow.RunRef{RunID: "r1"}},
		{Workflow: "backup", Outcome: workflow.TickFired, Err: errors.New("enqueue failed")},
		{Workflow: "backup", Outcome: workflow.TickSkipped, Reason: workflow.TickReasonWindow},
		{Workflow: "backup", Outcome: workflow.TickMisfired, Missed: 3},
	} {
		hook(ctx, tick)
	}
	if len(rec.targets) != 1 || rec.targets[0] != "https://hc.example/start" {
		t.Fatalf("pings = %v, want one for the fired run", rec.targets)
	}
}

func TestPinger_RunPingsEveryEventAndLogsFailuresWithoutTheURL(t *testing.T) {
	var logs bytes.Buffer
	rec := &pingRecorder{err: errors.New("status 503")}
	p := middleware.NewPinger(rec.send, middleware.WithPingLogger(slog.New(slog.NewTextHandler(&logs, nil))))
	hook := p.Run("https://hc.example/secret-token")
	hook(context.Background(), workflow.RunEvent{Workflow: "backup", Status: workflow.StatusFailed})
	if len(rec.targets) != 1 {
		t.Fatalf("pings = %v", rec.targets)
	}
	line := logs.String()
	if !strings.Contains(line, "level=WARN") || !strings.Contains(line, "status 503") || !strings.Contains(line, "workflow=backup") {
		t.Fatalf("log = %q", line)
	}
	if strings.Contains(line, "secret-token") {
		t.Fatalf("the log leaked the ping URL: %q", line)
	}
}

func TestPinger_TimeoutBoundsEachPing(t *testing.T) {
	var deadline time.Time
	p := middleware.NewPinger(func(ctx context.Context, _ string) error {
		deadline, _ = ctx.Deadline()
		return nil
	}, middleware.WithPingTimeout(time.Second), middleware.WithPingLogger(nil))
	start := time.Now()
	p.Run("u")(context.Background(), workflow.RunEvent{})
	if d := deadline.Sub(start); d <= 0 || d > time.Second+100*time.Millisecond {
		t.Fatalf("deadline %v after start, want about 1s", d)
	}
	// A non-positive timeout keeps the default rather than removing it.
	p = middleware.NewPinger(func(ctx context.Context, _ string) error {
		deadline, _ = ctx.Deadline()
		return nil
	}, middleware.WithPingTimeout(0))
	start = time.Now()
	p.Run("u")(context.Background(), workflow.RunEvent{})
	if d := deadline.Sub(start); d < middleware.DefaultPingTimeout-time.Second || d > middleware.DefaultPingTimeout+time.Second {
		t.Fatalf("deadline %v after start, want DefaultPingTimeout", d)
	}
}

func TestHTTPPing(t *testing.T) {
	var gotMethod, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		if r.URL.Path == "/fail" {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_, _ = w.Write([]byte("OK"))
	}))
	defer srv.Close()
	ping := middleware.HTTPPing(srv.Client())
	ctx := context.Background()

	if err := ping(ctx, srv.URL+"/abc"); err != nil || gotMethod != http.MethodGet || gotPath != "/abc" {
		t.Fatalf("ok ping: err=%v method=%s path=%s", err, gotMethod, gotPath)
	}
	err := ping(ctx, srv.URL+"/fail")
	if err == nil || !strings.Contains(err.Error(), "status 503") {
		t.Fatalf("non-2xx: %v", err)
	}
	// A transport error names the cause, never the URL and its token.
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL + "/secret-token"
	dead.Close()
	if err := ping(ctx, deadURL); err == nil || strings.Contains(err.Error(), "secret-token") || strings.Contains(err.Error(), dead.URL) {
		t.Fatalf("transport error = %v, want one without the URL", err)
	}
	if err := ping(ctx, "://bad url"); err == nil || strings.Contains(err.Error(), "bad url") {
		t.Fatalf("invalid URL error = %v", err)
	}
}
