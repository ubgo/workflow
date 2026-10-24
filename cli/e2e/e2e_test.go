package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/admin"
	"github.com/ubgo/workflow/cli"
	"github.com/ubgo/workflow/memory"
)

// token is the bearer token the test server's Identify accepts.
const token = "e2e-token"

// server is a memory client with failed, blocked and scheduled runs,
// served by the real admin handler behind bearer auth.
func server(t *testing.T) (*memory.Client, string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c := memory.New(memory.WithQueues(workflow.QueueConfig{Name: "default"}, workflow.QueueConfig{Name: "emails"}))
	once := workflow.RetryPolicy{Max: 1}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(c.Register(workflow.Workflow{Name: "send", Queue: "emails", Retry: once, Steps: []workflow.Step{{Name: "send", Run: func(_ context.Context, in []byte) ([]byte, error) {
		if string(in) == "bad" {
			return nil, errors.New("smtp 550")
		}
		return nil, nil
	}}}}))
	must(c.Register(workflow.Workflow{Name: "block", Steps: []workflow.Step{{Name: "wait", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}}}))
	m := admin.NewMemoryMetrics(admin.MemoryMetricsConfig{})
	must(c.OnRunFinished(m.Observe))
	must(c.Start(ctx))
	for _, p := range []string{"bad", "bad", "ok"} {
		_, err := c.Enqueue(ctx, workflow.Job{Workflow: "send", Payload: []byte(p)})
		must(err)
	}
	_, err := c.Schedule(ctx, workflow.Schedule{Name: "nightly", Workflow: "send", Cron: "0 2 * * *", Location: time.UTC, Input: []byte("ok")})
	must(err)
	waitFor(t, func() bool {
		p, _ := c.Runs(ctx, workflow.RunQuery{Statuses: []workflow.Status{workflow.StatusFailed}})
		return len(p.Runs) == 2
	})
	auth := admin.Auth[string]{
		Identify: func(r *http.Request) (string, error) {
			if r.Header.Get("Authorization") == "Bearer "+token {
				return "ops", nil
			}
			return "", errors.New("no token")
		},
		Authorize: func(context.Context, admin.Request[string]) error { return nil },
	}
	mux := http.NewServeMux()
	mux.Handle("/admin/", http.StripPrefix("/admin", admin.NewHandler(admin.New(c, admin.WithMetrics(m)), auth)))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return c, srv.URL + "/admin"
}

func waitFor(t *testing.T, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func run(t *testing.T, url string, withToken bool, args ...string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	env := map[string]string{cli.EnvURL: url}
	if withToken {
		env[cli.EnvToken] = token
	}
	code := cli.Run(context.Background(), args, cli.Options{Stdout: &out, Stderr: &errb, Getenv: func(k string) string { return env[k] }})
	return code, out.String(), errb.String()
}

func TestCLI_AgainstAdminHandler(t *testing.T) {
	c, url := server(t)
	ctx := context.Background()

	// Anonymous is refused by the server, and the CLI exits 1.
	if code, _, stderr := run(t, url, false, "failures"); code != cli.ExitError || !strings.Contains(stderr, "401") {
		t.Fatalf("anonymous: %d %q", code, stderr)
	}

	code, out, stderr := run(t, url, true, "failures", "--workflow", "send")
	if code != cli.ExitOK || strings.Count(out, "  failed  ") != 2 {
		t.Fatalf("failures: %d %q %q", code, out, stderr)
	}

	// Bulk retry: the dry run changes nothing, the real one retries both.
	code, out, _ = run(t, url, true, "retry", "--all", "--workflow", "send", "--dry-run")
	if code != cli.ExitOK || !strings.Contains(out, "would retry 2 runs") {
		t.Fatalf("dry retry: %q", out)
	}
	p, _ := c.Runs(ctx, workflow.RunQuery{Statuses: []workflow.Status{workflow.StatusFailed}})
	if len(p.Runs) != 2 {
		t.Fatalf("dry run changed state: %d failed", len(p.Runs))
	}
	code, out, _ = run(t, url, true, "retry", "--all", "--workflow", "send")
	if code != cli.ExitOK || !strings.Contains(out, "retried 2") {
		t.Fatalf("retry --all: %q", out)
	}
	// They fail again (the payload is still bad) and come back as failures.
	waitFor(t, func() bool {
		p, _ := c.Runs(ctx, workflow.RunQuery{Statuses: []workflow.Status{workflow.StatusFailed}})
		return len(p.Runs) == 2
	})

	// One run: inspect, then delete with a dry run first.
	p, _ = c.Runs(ctx, workflow.RunQuery{Statuses: []workflow.Status{workflow.StatusFailed}, Limit: 1})
	id := p.Runs[0].Ref.RunID.String()
	if code, out, _ := run(t, url, true, "inspect", id); code != cli.ExitOK || !strings.Contains(out, "failed") || !strings.Contains(out, "emails") {
		t.Fatalf("inspect: %q", out)
	}
	if code, _, stderr := run(t, url, true, "cancel", id, "--dry-run"); code != cli.ExitError || !strings.Contains(stderr, "would be refused") {
		t.Errorf("dry cancel of a failed run: %d %q", code, stderr)
	}
	if code, out, _ := run(t, url, true, "delete", id); code != cli.ExitOK || !strings.Contains(out, "deleted") {
		t.Fatalf("delete: %q", out)
	}
	if code, _, stderr := run(t, url, true, "inspect", id); code != cli.ExitError || !strings.Contains(stderr, "404") {
		t.Errorf("inspect deleted: %d %q", code, stderr)
	}
	// Delete by filter refuses an empty filter (server-side) and works with one.
	if code, _, stderr := run(t, url, true, "delete", "--all"); code != cli.ExitError || !strings.Contains(stderr, "400") {
		t.Errorf("unfiltered delete --all: %d %q", code, stderr)
	}
	if code, out, _ := run(t, url, true, "delete", "--all", "--status", "failed"); code != cli.ExitOK || !strings.Contains(out, "deleted 1") {
		t.Errorf("delete --all failed: %q", out)
	}

	// Pause a queue: a new run waits; cancel --all clears it; resume.
	if code, out, _ := run(t, url, true, "pause", "--queue", "emails"); code != cli.ExitOK || out != "paused queue emails\n" {
		t.Fatalf("pause: %q", out)
	}
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "send", Payload: []byte("ok")}); err != nil {
		t.Fatal(err)
	}
	if code, out, _ := run(t, url, true, "pauses"); code != cli.ExitOK || !strings.Contains(out, "queue emails") {
		t.Errorf("pauses: %q", out)
	}
	if code, out, _ := run(t, url, true, "queues"); code != cli.ExitOK || !strings.Contains(out, "emails") {
		t.Errorf("queues: %q", out)
	}
	if code, out, _ := run(t, url, true, "cancel", "--all", "--queue", "emails", "--dry-run"); code != cli.ExitOK || !strings.Contains(out, "would cancel 1 runs") {
		t.Errorf("dry cancel --all: %q", out)
	}
	if code, out, _ := run(t, url, true, "cancel", "--all", "--queue", "emails"); code != cli.ExitOK || !strings.Contains(out, "cancelled 1") {
		t.Errorf("cancel --all: %q", out)
	}
	if code, _, _ := run(t, url, true, "resume", "--queue", "emails"); code != cli.ExitOK {
		t.Errorf("resume: %d", code)
	}

	// Schedules and trigger.
	code, out, _ = run(t, url, true, "schedules", "--json")
	var sch struct {
		Schedules []struct {
			ID string `json:"id"`
		} `json:"schedules"`
	}
	if code != cli.ExitOK || json.Unmarshal([]byte(out), &sch) != nil || len(sch.Schedules) != 1 {
		t.Fatalf("schedules --json: %q", out)
	}
	sid := sch.Schedules[0].ID
	if code, out, _ := run(t, url, true, "trigger", sid); code != cli.ExitOK || !strings.Contains(out, "triggered") {
		t.Errorf("trigger: %q", out)
	}

	// Reschedule a delayed run (dry run first), then cancel it.
	delayed, err := c.Enqueue(ctx, workflow.Job{Workflow: "send", Payload: []byte("ok"), RunAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	did := delayed.ScheduledID.String()
	if code, out, stderr := run(t, url, true, "reschedule", did, "--scheduled", "--at", "2030-01-02T03:04:05Z", "--dry-run"); code != cli.ExitOK || !strings.Contains(out, "would move") {
		t.Fatalf("dry reschedule: %d %q %q", code, out, stderr)
	}
	if code, out, stderr := run(t, url, true, "reschedule", did, "--scheduled", "--at", "2030-01-02T03:04:05Z"); code != cli.ExitOK || !strings.Contains(out, "rescheduled") {
		t.Fatalf("reschedule: %d %q %q", code, out, stderr)
	}
	rp, _ := c.Runs(ctx, workflow.RunQuery{Ref: delayed})
	if len(rp.Runs) != 1 || !rp.Runs[0].RunAt.Equal(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)) {
		t.Errorf("RunAt after reschedule = %+v", rp.Runs)
	}
	if code, _, _ := run(t, url, true, "cancel", did, "--scheduled"); code != cli.ExitOK {
		t.Errorf("cancel delayed: %d", code)
	}

	// Cancel the step of a blocked run: the run fails with the step.
	blocked, err := c.Enqueue(ctx, workflow.Job{Workflow: "block"})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { st, _ := c.Status(ctx, blocked); return st == workflow.StatusRunning })
	if code, out, stderr := run(t, url, true, "cancel-step", blocked.RunID.String(), "wait"); code != cli.ExitOK || !strings.Contains(out, "cancelled step wait") {
		t.Fatalf("cancel-step: %d %q %q", code, out, stderr)
	}
	waitFor(t, func() bool { st, _ := c.Status(ctx, blocked); return st == workflow.StatusFailed })

	// Drain: everything finishes, so drain returns at once.
	if code, out, stderr := run(t, url, true, "drain", "--wait", "5s", "--interval", "50ms"); code != cli.ExitOK || !strings.Contains(out, "drained") {
		t.Errorf("drain: %d %q %q", code, out, stderr)
	}
	// A blocked run never drains: exit 1 when the wait ends.
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "block"}); err != nil {
		t.Fatal(err)
	}
	if code, _, stderr := run(t, url, true, "drain", "--queue", "default", "--wait", "200ms", "--interval", "50ms"); code != cli.ExitError || !strings.Contains(stderr, "still queued or running") {
		t.Errorf("drain timeout: %d %q", code, stderr)
	}

	// Metrics counted the run ends from the run-finished hook.
	code, out, _ = run(t, url, true, "metrics", "--window", "10m", "--step", "10m", "--json")
	var m struct {
		Points []struct {
			Succeeded, Failed int64
		} `json:"points"`
	}
	if code != cli.ExitOK || json.Unmarshal([]byte(out), &m) != nil || len(m.Points) == 0 {
		t.Fatalf("metrics: %q", out)
	}
	var failed, ok int64
	for _, pt := range m.Points {
		failed += pt.Failed
		ok += pt.Succeeded
	}
	if failed < 4 || ok < 2 {
		t.Errorf("metrics failed=%d succeeded=%d, want at least 4 and 2", failed, ok)
	}
	if code, out, _ := run(t, url, true, "caps"); code != cli.ExitOK || !strings.Contains(out, "retry_all") {
		t.Errorf("caps: %q", out)
	}
}
