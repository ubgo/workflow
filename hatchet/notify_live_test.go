package hatchet_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/hatchet"
)

// liveWait bounds how long the live notifier tests wait for an event.
const liveWait = 60 * time.Second

// recorder collects run ends and the Status each observer call saw.
type recorder struct {
	c      workflow.Client
	mu     sync.Mutex
	events map[workflow.RunID][]workflow.RunEvent
	byID   map[workflow.ScheduledID][]workflow.RunEvent
	seen   map[workflow.RunID]workflow.Status
}

func newRecorder() *recorder {
	return &recorder{events: map[workflow.RunID][]workflow.RunEvent{}, byID: map[workflow.ScheduledID][]workflow.RunEvent{}, seen: map[workflow.RunID]workflow.Status{}}
}

func (r *recorder) observe(ctx context.Context, ev workflow.RunEvent) {
	var st workflow.Status
	if !ev.Ref.RunID.IsZero() {
		st, _ = r.c.Status(ctx, workflow.RunRef{RunID: ev.Ref.RunID})
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !ev.Ref.RunID.IsZero() {
		r.events[ev.Ref.RunID] = append(r.events[ev.Ref.RunID], ev)
		r.seen[ev.Ref.RunID] = st
	}
	if !ev.Ref.ScheduledID.IsZero() {
		r.byID[ev.Ref.ScheduledID] = append(r.byID[ev.Ref.ScheduledID], ev)
	}
}

// wait returns the event for ref once one arrived.
func (r *recorder) wait(t *testing.T, ref workflow.RunRef) workflow.RunEvent {
	t.Helper()
	deadline := time.Now().Add(liveWait)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		evs := r.events[ref.RunID]
		if ref.RunID.IsZero() {
			evs = r.byID[ref.ScheduledID]
		}
		r.mu.Unlock()
		if len(evs) > 0 {
			return evs[0]
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("no run-finished event for %+v within %s", ref, liveWait)
	return workflow.RunEvent{}
}

// TestRunFinishedLive proves OnRunFinished on the real engine for each way
// a run ends: success, failure, cancel while running, cancel of a delayed
// start before it fires, and a delayed start that fires; then that a
// restarted client with RunFinishedCatchUp replays ends it did not see.
func TestRunFinishedLive(t *testing.T) {
	if os.Getenv(envToken) == "" {
		t.Skipf("%s is not set: the live notifier test needs a Hatchet engine (task test:live)", envToken)
	}
	prefix := "wfnotify_" + uuid.NewString()[:8]
	errApp := errors.New("app failure")
	release := make(chan struct{})
	ok := func(context.Context, []byte) ([]byte, error) { return []byte(`"ok"`), nil }
	wfs := []workflow.Workflow{
		{Name: prefix + "_ok", Steps: []workflow.Step{{Name: "a", Run: ok}}},
		{Name: prefix + "_bad", Steps: []workflow.Step{{Name: "a", Run: func(context.Context, []byte) ([]byte, error) { return nil, workflow.Permanent(errApp) }}}},
		{Name: prefix + "_slow", Steps: []workflow.Step{{Name: "a", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-release:
				return nil, nil
			}
		}}}},
	}
	defer close(release)

	rec := newRecorder()
	c := hatchet.New(hatchet.Config{WorkerName: "wfnotify"})
	rec.c = c
	if c.Capabilities().Has(workflow.CapRunFinished) {
		t.Fatal("the Hatchet driver must not claim CapRunFinished: its delivery is best-effort")
	}
	if err := c.OnRunFinished(rec.observe); err != nil {
		t.Fatal(err)
	}
	for _, wf := range wfs {
		if err := c.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.OnRunFinished(rec.observe); !errors.Is(err, workflow.ErrAlreadyStarted) {
		t.Fatalf("OnRunFinished after Start: %v", err)
	}
	enqueue := func(job workflow.Job) workflow.RunRef {
		t.Helper()
		ref, err := c.Enqueue(ctx, job)
		if err != nil {
			t.Fatal(err)
		}
		return ref
	}

	labels := map[string]string{"tenant": "t1"}
	succeeded := enqueue(workflow.Job{Workflow: prefix + "_ok", Labels: labels})
	failed := enqueue(workflow.Job{Workflow: prefix + "_bad"})
	running := enqueue(workflow.Job{Workflow: prefix + "_slow"})
	delayedCancelled := enqueue(workflow.Job{Workflow: prefix + "_ok", RunAt: time.Now().Add(time.Hour)})
	delayedFired := enqueue(workflow.Job{Workflow: prefix + "_ok", RunAt: time.Now().Add(3 * time.Second)})
	time.Sleep(3 * time.Second) // let the slow run start
	if err := c.Cancel(ctx, running); err != nil {
		t.Fatal(err)
	}
	if err := c.Cancel(ctx, delayedCancelled); err != nil {
		t.Fatal(err)
	}

	ev := rec.wait(t, succeeded)
	if ev.Status != workflow.StatusSucceeded || ev.Err != nil || ev.Workflow != prefix+"_ok" || ev.Labels["tenant"] != "t1" || ev.FinishedAt.IsZero() {
		t.Fatalf("succeeded run event %+v", ev)
	}
	if _, internal := ev.Labels["wf__workflow"]; internal {
		t.Fatalf("event labels leak driver metadata: %v", ev.Labels)
	}
	ev = rec.wait(t, failed)
	if ev.Status != workflow.StatusFailed || ev.Err == nil {
		t.Fatalf("failed run event %+v", ev)
	}
	ev = rec.wait(t, running)
	if ev.Status != workflow.StatusCancelled || !errors.Is(ev.Err, workflow.ErrCancelled) {
		t.Fatalf("cancelled run event %+v", ev)
	}
	ev = rec.wait(t, delayedCancelled)
	if ev.Status != workflow.StatusCancelled || ev.Ref.ScheduledID != delayedCancelled.ScheduledID || ev.Workflow != prefix+"_ok" {
		t.Fatalf("cancelled delayed start event %+v", ev)
	}
	ev = rec.wait(t, delayedFired)
	if ev.Status != workflow.StatusSucceeded || ev.Ref.ScheduledID != delayedFired.ScheduledID || ev.Ref.RunID.IsZero() {
		t.Fatalf("fired delayed start event %+v, want both IDs", ev)
	}
	rec.mu.Lock()
	for id, st := range rec.seen {
		if want := rec.events[id][0].Status; st != want {
			t.Errorf("observer saw Status %q for an event with %q", st, want)
		}
	}
	rec.mu.Unlock()

	// A restarted client replays the ends it missed within its catch-up.
	stop()
	time.Sleep(time.Second)
	rec2 := newRecorder()
	c2 := hatchet.New(hatchet.Config{WorkerName: "wfnotify", RunFinishedCatchUp: 5 * time.Minute})
	rec2.c = c2
	if err := c2.OnRunFinished(rec2.observe); err != nil {
		t.Fatal(err)
	}
	for _, wf := range wfs {
		if err := c2.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	ctx2, stop2 := context.WithCancel(context.Background())
	defer stop2()
	if err := c2.Start(ctx2); err != nil {
		t.Fatal(err)
	}
	for _, ref := range []workflow.RunRef{succeeded, failed, running} {
		rec2.wait(t, ref)
	}
}
