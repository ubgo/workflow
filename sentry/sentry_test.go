package sentry_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	sentrygo "github.com/getsentry/sentry-go"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/sentry"
)

// fakeTransport records events instead of sending them; no network.
type fakeTransport struct {
	mu     sync.Mutex
	events []*sentrygo.Event
}

func (t *fakeTransport) Flush(time.Duration) bool              { return true }
func (t *fakeTransport) FlushWithContext(context.Context) bool { return true }
func (t *fakeTransport) Configure(sentrygo.ClientOptions)      {}
func (t *fakeTransport) Close()                                {}
func (t *fakeTransport) SendEvent(e *sentrygo.Event) {
	t.mu.Lock()
	t.events = append(t.events, e)
	t.mu.Unlock()
}
func (t *fakeTransport) sent() []*sentrygo.Event {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]*sentrygo.Event(nil), t.events...)
}

func newHub(t *testing.T) (*sentrygo.Hub, *fakeTransport) {
	t.Helper()
	tr := &fakeTransport{}
	client, err := sentrygo.NewClient(sentrygo.ClientOptions{Dsn: "https://key@sentry.invalid/1", Transport: tr})
	if err != nil {
		t.Fatal(err)
	}
	return sentrygo.NewHub(client, sentrygo.NewScope()), tr
}

type codeErr struct{ code int }

func (e *codeErr) Error() string { return fmt.Sprintf("code %d", e.code) }

func failure() workflow.Failure {
	return workflow.Failure{
		Ref:      workflow.RunRef{RunID: "run-1"},
		Workflow: "email",
		Status:   workflow.StatusFailed,
		Step:     "send",
		Err:      fmt.Errorf("send: %w", &codeErr{503}),
		Final:    true,
		Labels:   map[string]string{"tenant": "t1", "user": "u9"},
		At:       time.Date(2026, 10, 13, 9, 0, 0, 0, time.UTC),
	}
}

// TestReportFinal: one event per failure, with the fixed tags, the run
// context, error level, the default fingerprint, and no labels.
func TestReportFinal(t *testing.T) {
	hub, tr := newHub(t)
	sentry.New(hub).Report(context.Background(), failure())
	ev := tr.sent()
	if len(ev) != 1 {
		t.Fatalf("events: %d, want 1", len(ev))
	}
	e := ev[0]
	if e.Level != sentrygo.LevelError {
		t.Errorf("level %q", e.Level)
	}
	want := map[string]string{sentry.TagWorkflow: "email", sentry.TagStep: "send", sentry.TagStatus: "failed", sentry.TagFinal: "true"}
	for k, v := range want {
		if e.Tags[k] != v {
			t.Errorf("tag %s = %q, want %q", k, e.Tags[k], v)
		}
	}
	for k := range e.Tags {
		if len(k) > len(sentry.TagLabelPrefix) && k[:len(sentry.TagLabelPrefix)] == sentry.TagLabelPrefix {
			t.Errorf("label tag %s sent without opt-in", k)
		}
	}
	if got := e.Contexts[sentry.ContextName]["run_id"]; got != "run-1" {
		t.Errorf("run_id context %v", got)
	}
	if fp := fmt.Sprint(e.Fingerprint); fp != "[ubgo-workflow email send *sentry_test.codeErr]" {
		t.Errorf("fingerprint %s", fp)
	}
	if len(e.Exception) == 0 || e.Exception[len(e.Exception)-1].Value != "send: code 503" {
		t.Errorf("exception %+v", e.Exception)
	}
	if !e.Timestamp.Equal(failure().At) {
		t.Errorf("timestamp %v", e.Timestamp)
	}
}

// TestReportLabelsOptIn: WithLabels sends only the chosen keys,
// WithAllLabels every key.
func TestReportLabelsOptIn(t *testing.T) {
	hub, tr := newHub(t)
	sentry.New(hub, sentry.WithLabels("tenant")).Report(context.Background(), failure())
	sentry.New(hub, sentry.WithAllLabels()).Report(context.Background(), failure())
	ev := tr.sent()
	if len(ev) != 2 {
		t.Fatalf("events: %d", len(ev))
	}
	if ev[0].Tags["workflow.label.tenant"] != "t1" || ev[0].Tags["workflow.label.user"] != "" {
		t.Errorf("WithLabels tags %v", ev[0].Tags)
	}
	if ev[1].Tags["workflow.label.tenant"] != "t1" || ev[1].Tags["workflow.label.user"] != "u9" {
		t.Errorf("WithAllLabels tags %v", ev[1].Tags)
	}
}

// TestReportAttempt: an attempt failure is a warning carrying its
// attempt number; WithLevels changes both levels.
func TestReportAttempt(t *testing.T) {
	hub, tr := newHub(t)
	f := failure()
	f.Final, f.Status, f.Attempt = false, workflow.StatusRetrying, 2
	sentry.New(hub).Report(context.Background(), f)
	sentry.New(hub, sentry.WithLevels(sentrygo.LevelFatal, sentrygo.LevelInfo)).Report(context.Background(), f)
	ev := tr.sent()
	if ev[0].Level != sentrygo.LevelWarning || ev[0].Tags[sentry.TagFinal] != "false" || ev[0].Contexts[sentry.ContextName]["attempt"] != 2 {
		t.Errorf("attempt event: level %q tags %v ctx %v", ev[0].Level, ev[0].Tags, ev[0].Contexts)
	}
	if ev[1].Level != sentrygo.LevelInfo {
		t.Errorf("WithLevels attempt level %q", ev[1].Level)
	}
}

// TestReportHooks: WithFingerprint replaces the grouping, WithEvent can
// change or drop the event, a nil Err still produces an event.
func TestReportHooks(t *testing.T) {
	hub, tr := newHub(t)
	r := sentry.New(hub,
		sentry.WithFingerprint(func(f workflow.Failure) []string { return []string{f.Workflow} }),
		sentry.WithEvent(func(e *sentrygo.Event, f workflow.Failure) *sentrygo.Event {
			if f.Step == "drop" {
				return nil
			}
			e.Tags["team"] = "billing"
			return e
		}))
	f := failure()
	f.Err = nil
	r.Report(context.Background(), f)
	f.Step = "drop"
	r.Report(context.Background(), f)
	ev := tr.sent()
	if len(ev) != 1 {
		t.Fatalf("events: %d, want 1 (one dropped)", len(ev))
	}
	if fmt.Sprint(ev[0].Fingerprint) != "[email]" || ev[0].Tags["team"] != "billing" {
		t.Errorf("event %v %v", ev[0].Fingerprint, ev[0].Tags)
	}
	if len(ev[0].Exception) == 0 || ev[0].Exception[0].Value != "workflow: run failed" {
		t.Errorf("nil Err exception %+v", ev[0].Exception)
	}
}

// TestReportHubFromContext: with a nil hub the ctx's hub is used, and the
// app's scope is not changed by a report.
func TestReportHubFromContext(t *testing.T) {
	hub, tr := newHub(t)
	hub.Scope().SetTag("release", "r1")
	ctx := sentrygo.SetHubOnContext(context.Background(), hub)
	sentry.New(nil).Report(ctx, failure())
	ev := tr.sent()
	if len(ev) != 1 || ev[0].Tags["release"] != "r1" {
		t.Fatalf("events %d tags %v", len(ev), ev)
	}
	probe := sentrygo.NewEvent()
	hub.Scope().ApplyToEvent(probe, nil, hub.Client())
	if _, leaked := probe.Tags[sentry.TagWorkflow]; leaked {
		t.Error("report changed the app's scope")
	}
}

// TestReportConcurrent: concurrent reports on one hub each send their
// own event with their own tags.
func TestReportConcurrent(t *testing.T) {
	hub, tr := newHub(t)
	r := sentry.New(hub)
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f := failure()
			f.Workflow = fmt.Sprintf("wf-%d", i)
			r.Report(context.Background(), f)
		}()
	}
	wg.Wait()
	ev := tr.sent()
	if len(ev) != 50 {
		t.Fatalf("events: %d", len(ev))
	}
	seen := map[string]bool{}
	for _, e := range ev {
		if e.Fingerprint[1] != e.Tags[sentry.TagWorkflow] {
			t.Errorf("tags and fingerprint disagree: %v %v", e.Tags, e.Fingerprint)
		}
		seen[e.Tags[sentry.TagWorkflow]] = true
	}
	if len(seen) != 50 {
		t.Errorf("distinct workflows %d", len(seen))
	}
}
