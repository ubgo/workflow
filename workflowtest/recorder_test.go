package workflowtest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ubgo/workflow"
)

// tbSpy is a testing.TB that records Errorf calls instead of failing, so
// the assertions themselves can be tested for failing.
type tbSpy struct {
	testing.TB
	errs []string
}

func (s *tbSpy) Helper() {}
func (s *tbSpy) Errorf(format string, args ...any) {
	s.errs = append(s.errs, fmt.Sprintf(format, args...))
}

func noopHandler(context.Context, []byte) ([]byte, error) { return nil, nil }

func TestRecorderFake(t *testing.T) {
	ctx := context.Background()
	r := NewRecorder(nil)
	if err := r.Register(oneStep("send-email", noopHandler)); err != nil {
		t.Fatal(err)
	}
	if err := r.Register(workflow.Workflow{Name: "bad"}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("Register of an invalid workflow: %v, want ErrInvalid", err)
	}
	if err := r.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.Start(ctx); !errors.Is(err, workflow.ErrAlreadyStarted) {
		t.Fatalf("second Start: %v, want ErrAlreadyStarted", err)
	}

	labels := map[string]string{"tenant": "t1"}
	job := workflow.Job{Workflow: "send-email", Payload: []byte("a"), Labels: labels}
	ref1, err := r.Enqueue(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	job.Payload[0] = 'z' // the caller reusing its buffers must not change the record
	labels["tenant"] = "t2"
	ref2, _ := r.Enqueue(ctx, workflow.Job{Workflow: "send-email", Payload: []byte("b")})
	if ref1 == ref2 || ref1.IsZero() {
		t.Fatalf("refs %+v and %+v must be distinct and set", ref1, ref2)
	}
	if st, err := r.Status(ctx, ref1); err != nil || st != workflow.StatusQueued {
		t.Fatalf("Status = %q, %v; want queued", st, err)
	}
	if _, err := r.Status(ctx, workflow.RunRef{RunID: "nope"}); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("Status of an unknown run: %v, want ErrNotFound", err)
	}
	if err := r.Cancel(ctx, ref1); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Schedule(ctx, workflow.Schedule{Workflow: "send-email", Cron: "* * * * *"}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("Schedule without Location: %v, want ErrInvalid", err)
	}
	if id, err := r.Schedule(ctx, workflow.Schedule{Workflow: "send-email", Cron: "* * * * *", Location: time.UTC}); err != nil || id.IsZero() {
		t.Fatalf("Schedule = %q, %v", id, err)
	}
	if r.Capabilities() != allCaps {
		t.Fatalf("fake Capabilities = %s, want every capability", r.Capabilities())
	}

	jobs := r.Jobs()
	if len(jobs) != 2 || string(jobs[0].Payload) != "a" || jobs[0].Labels["tenant"] != "t1" {
		t.Fatalf("recorded jobs = %+v, want copies of the two enqueued jobs", jobs)
	}

	// Passing assertions report nothing.
	pass := &tbSpy{TB: t}
	r.AssertEnqueued(pass, "send-email", 2)
	r.AssertNotEnqueued(pass, "other")
	r.AssertEnqueuedWith(pass, "send-email", func(j workflow.Job) bool { return string(j.Payload) == "b" })
	if len(pass.errs) != 0 {
		t.Fatalf("passing assertions reported %v", pass.errs)
	}

	// Each failing assertion reports exactly once.
	for name, assert := range map[string]func(testing.TB){
		"count":   func(tb testing.TB) { r.AssertEnqueued(tb, "send-email", 3) },
		"not":     func(tb testing.TB) { r.AssertNotEnqueued(tb, "send-email") },
		"with":    func(tb testing.TB) { r.AssertEnqueuedWith(tb, "send-email", func(workflow.Job) bool { return false }) },
		"nothing": func(tb testing.TB) { r.AssertNothingEnqueued(tb) },
	} {
		spy := &tbSpy{TB: t}
		assert(spy)
		if len(spy.errs) != 1 {
			t.Errorf("%s: failing assertion reported %d errors, want 1", name, len(spy.errs))
		}
	}
	empty := &tbSpy{TB: t}
	NewRecorder(nil).AssertNothingEnqueued(empty)
	if len(empty.errs) != 0 {
		t.Fatalf("AssertNothingEnqueued on an empty recorder reported %v", empty.errs)
	}
}

// refusing is a Client whose Enqueue fails, to prove the wrapping
// Recorder records only what the inner client accepted.
type refusing struct{ workflow.Client }

func (refusing) Enqueue(context.Context, workflow.Job) (workflow.RunRef, error) {
	return workflow.RunRef{}, workflow.ErrNotStarted
}

func TestRecorderWrapsInner(t *testing.T) {
	ctx := context.Background()
	inner := NewFake()
	if err := inner.Register(oneStep("w", noopHandler)); err != nil {
		t.Fatal(err)
	}
	if err := inner.Start(ctx); err != nil {
		t.Fatal(err)
	}
	r := NewRecorder(inner)
	ref, err := r.Enqueue(ctx, workflow.Job{Workflow: "w"})
	if err != nil {
		t.Fatal(err)
	}
	if st, err := r.Status(ctx, ref); err != nil || st != workflow.StatusQueued {
		t.Fatalf("Status through the wrapper = %q, %v; want the inner client's answer", st, err)
	}
	r.AssertEnqueued(t, "w", 1)
	inner.AssertEnqueuedTimes(t, "w", 1)

	refused := NewRecorder(refusing{NewFake()})
	if _, err := refused.Enqueue(ctx, workflow.Job{Workflow: "w"}); !errors.Is(err, workflow.ErrNotStarted) {
		t.Fatalf("err = %v, want the inner error", err)
	}
	refused.AssertNothingEnqueued(t)
}

// notifying is a fake inner client that also implements RunNotifier.
type notifying struct {
	workflow.Client
	got int
}

func (n *notifying) OnRunFinished(workflow.RunFinishedFunc) error { n.got++; return nil }

func TestRecorderForwardsOnRunFinished(t *testing.T) {
	inner := &notifying{Client: NewFake()}
	if err := NewRecorder(inner).OnRunFinished(func(context.Context, workflow.RunEvent) {}); err != nil || inner.got != 1 {
		t.Fatalf("OnRunFinished through the wrapper: err = %v, inner calls = %d; want nil and 1", err, inner.got)
	}
	for name, r := range map[string]*Recorder{"fake": NewRecorder(nil), "plain inner": NewRecorder(refusing{NewFake()})} {
		if err := r.OnRunFinished(func(context.Context, workflow.RunEvent) {}); !errors.Is(err, workflow.ErrUnsupported) {
			t.Errorf("%s: OnRunFinished err = %v, want ErrUnsupported", name, err)
		}
	}
}
