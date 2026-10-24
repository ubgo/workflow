package workflowtest

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

func TestFakeQueuesAndJobRetry(t *testing.T) {
	ctx := context.Background()
	mail := oneStep("mail", noopHandler)
	mail.Queue = "mail"
	f := startedFake(t, nil, mail, oneStep("plain", noopHandler))
	policy := workflow.RetryPolicy{Max: 5, Backoff: workflow.Backoff{Initial: time.Second}}
	job := workflow.Job{Workflow: "mail", Queue: "bulk", Retry: &policy}
	for _, j := range []workflow.Job{{Workflow: "mail"}, job, {Workflow: "plain"}} {
		if _, err := f.Enqueue(ctx, j); err != nil {
			t.Fatal(err)
		}
	}
	policy.Max = 99 // the recorded copy must not change
	f.AssertEnqueuedOn(t, "mail", "mail")
	f.AssertEnqueuedOn(t, "bulk", "mail", HasQueue("bulk"), HasRetry(workflow.RetryPolicy{Max: 5, Backoff: workflow.Backoff{Initial: time.Second}}))
	f.AssertEnqueuedOn(t, workflow.DefaultQueue, "plain")
	for name, assert := range map[string]func(testing.TB){
		"wrong queue":         func(tb testing.TB) { f.AssertEnqueuedOn(tb, "bulk", "plain") },
		"matcher excludes it": func(tb testing.TB) { f.AssertEnqueuedOn(tb, "mail", "mail", HasQueue("bulk")) },
		"retry differs":       func(tb testing.TB) { f.AssertEnqueued(tb, "mail", HasRetry(workflow.RetryPolicy{Max: 99})) },
	} {
		if errs := spyErrs(assert); len(errs) != 1 {
			t.Errorf("%s: reported %q, want one error", name, errs)
		}
	}

	bad := workflow.RetryPolicy{Max: 1, NoRetryOn: func(error) bool { return true }}
	if _, err := f.Enqueue(ctx, workflow.Job{Workflow: "mail", Retry: &bad}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("a job Retry with a function = %v, want ErrInvalid (ValidateJob)", err)
	}

	noOverride := startedFake(t, []FakeOption{FakeCaps(fakeDefaultCaps &^ workflow.CapQueueOverride)}, mail)
	if _, err := noOverride.Enqueue(ctx, workflow.Job{Workflow: "mail", Queue: "bulk"}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("a queue override without CapQueueOverride = %v, want ErrUnsupported", err)
	}
	if _, err := noOverride.Enqueue(ctx, workflow.Job{Workflow: "mail", Queue: "mail"}); err != nil {
		t.Errorf("the workflow's own queue without CapQueueOverride = %v, want accepted", err)
	}
	noRetry := startedFake(t, []FakeOption{FakeCaps(fakeDefaultCaps &^ workflow.CapJobRetry)}, oneStep("plain", noopHandler))
	if _, err := noRetry.Enqueue(ctx, workflow.Job{Workflow: "plain", Retry: &workflow.RetryPolicy{Max: 1}}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("a job Retry without CapJobRetry = %v, want ErrUnsupported", err)
	}
}

func TestFakeRetryAndDelete(t *testing.T) {
	ctx := context.Background()
	wf := oneStep("sync", noopHandler)
	f := startedFake(t, nil, wf)
	ref, _ := f.Enqueue(ctx, workflow.Job{Workflow: "sync", IdempotencyKey: "k"})
	if _, err := f.Retry(ctx, ref); !errors.Is(err, workflow.ErrWrongState) {
		t.Errorf("Retry of a queued run = %v, want ErrWrongState", err)
	}
	if err := f.Delete(ctx, ref); !errors.Is(err, workflow.ErrWrongState) {
		t.Errorf("Delete of a queued run = %v, want ErrWrongState", err)
	}
	if err := f.Finish(ref, workflow.StatusFailed); err != nil {
		t.Fatal(err)
	}
	// Another run took the key the failed one released.
	other, _ := f.Enqueue(ctx, workflow.Job{Workflow: "sync", IdempotencyKey: "k"})
	if _, err := f.Retry(ctx, ref); !errors.Is(err, workflow.ErrDuplicate) {
		t.Errorf("Retry while another run holds the key = %v, want ErrDuplicate", err)
	}
	if err := f.Finish(other, workflow.StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	got, err := f.Retry(ctx, ref)
	if err != nil || got != ref {
		t.Fatalf("Retry = %+v, %v; want the same ref", got, err)
	}
	if st, _ := f.Status(ctx, ref); st != workflow.StatusQueued {
		t.Errorf("status after Retry = %s, want queued", st)
	}
	if dup, _ := f.Enqueue(ctx, workflow.Job{Workflow: "sync", IdempotencyKey: "k"}); dup != ref {
		t.Errorf("the retried run did not take its key again: a new enqueue got %+v", dup)
	}
	f.AssertRetried(t, ref)
	if errs := spyErrs(func(tb testing.TB) { f.AssertRetried(tb, other) }); len(errs) != 1 {
		t.Errorf("AssertRetried of a run never retried reported %q", errs)
	}

	if err := f.Delete(ctx, other); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Status(ctx, other); !errors.Is(err, workflow.ErrNotFound) {
		t.Errorf("Status after Delete = %v, want ErrNotFound", err)
	}
	f.AssertDeleted(t, other)
	f.AssertEnqueuedTimes(t, "sync", 2) // a deleted run's job stays recorded
	if errs := spyErrs(func(tb testing.TB) { f.AssertDeleted(tb, ref) }); len(errs) != 1 {
		t.Errorf("AssertDeleted of a live run reported %q", errs)
	}
	if _, err := f.Retry(ctx, workflow.RunRef{RunID: "nope"}); !errors.Is(err, workflow.ErrNotFound) {
		t.Errorf("Retry of an unknown ref = %v, want ErrNotFound", err)
	}

	bare := startedFake(t, []FakeOption{FakeCaps(fakeDefaultCaps &^ (workflow.CapRetryRun | workflow.CapDeleteRun))}, wf)
	if _, err := bare.Retry(ctx, ref); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("Retry without CapRetryRun = %v", err)
	}
	if err := bare.Delete(ctx, ref); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("Delete without CapDeleteRun = %v", err)
	}
}

func TestFakePause(t *testing.T) {
	ctx := context.Background()
	f := startedFake(t, nil)
	for _, p := range []workflow.PauseTarget{workflow.PauseQueue("mail"), workflow.PauseWorkflow("sync"), workflow.PauseAll(), workflow.PauseWorkflow("billing")} {
		if err := f.Pause(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := f.Paused(ctx)
	want := []workflow.PauseTarget{workflow.PauseAll(), workflow.PauseWorkflow("billing"), workflow.PauseWorkflow("sync"), workflow.PauseQueue("mail")}
	if !slices.Equal(got, want) {
		t.Errorf("Paused = %+v, want %+v", got, want)
	}
	f.AssertPaused(t, workflow.PauseQueue("mail"))
	if err := f.Resume(ctx, workflow.PauseWorkflow("sync")); err != nil {
		t.Fatal(err)
	}
	f.AssertNotPaused(t, workflow.PauseWorkflow("sync"))
	f.AssertPaused(t, workflow.PauseWorkflow("billing"))
	if errs := spyErrs(func(tb testing.TB) { f.AssertPaused(tb, workflow.PauseWorkflow("sync")) }); len(errs) != 1 {
		t.Errorf("AssertPaused of a resumed target reported %q", errs)
	}
	if errs := spyErrs(func(tb testing.TB) { f.AssertNotPaused(tb, workflow.PauseAll()) }); len(errs) != 1 {
		t.Errorf("AssertNotPaused of a paused target reported %q", errs)
	}
	if err := f.Resume(ctx, workflow.PauseAll()); err != nil {
		t.Fatal(err)
	}
	if got, _ := f.Paused(ctx); len(got) != 0 {
		t.Errorf("Paused after Resume(All) = %+v, want none", got)
	}
	if err := f.Pause(ctx, workflow.PauseTarget{}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("Pause of an empty target = %v, want ErrInvalid", err)
	}
	noQueue := startedFake(t, []FakeOption{FakeCaps(fakeDefaultCaps &^ workflow.CapPauseQueue)})
	if err := noQueue.Pause(ctx, workflow.PauseQueue("mail")); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("PauseQueue without CapPauseQueue = %v, want ErrUnsupported", err)
	}
}

func TestFakeCapsDropsWhatItDoesNotServe(t *testing.T) {
	f := NewFake(FakeLike(memory.New()))
	if f.Capabilities().Has(workflow.CapRunQuery) || f.Capabilities().Has(workflow.CapDrain) || f.Capabilities().Has(workflow.CapRunFinished) {
		t.Errorf("FakeLike(memory) claims %s", f.Capabilities())
	}
	if !f.Capabilities().Has(workflow.CapRetryRun | workflow.CapPauseQueue | workflow.CapQueue) {
		t.Errorf("FakeLike(memory) lost a capability the Fake serves: %s", f.Capabilities())
	}
}

func TestRecorderRetryDeletePause(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := NewRecorder(memory.New())
	fail := oneStep("fail", func(context.Context, []byte) ([]byte, error) { return nil, workflow.Permanent(errors.New("boom")) })
	if err := r.Register(fail); err != nil {
		t.Fatal(err)
	}
	if err := r.Start(ctx); err != nil {
		t.Fatal(err)
	}
	ref, err := r.Enqueue(ctx, workflow.Job{Workflow: "fail"})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, r, ref, workflow.StatusFailed)
	if _, err := r.Retry(ctx, ref); err != nil {
		t.Fatal(err)
	}
	r.AssertRetried(t, ref)
	waitStatus(t, r, ref, workflow.StatusFailed)
	if err := r.Delete(ctx, ref); err != nil {
		t.Fatal(err)
	}
	r.AssertDeleted(t, ref)
	if err := r.Pause(ctx, workflow.PauseQueue("mail")); err != nil {
		t.Fatal(err)
	}
	r.AssertPaused(t, workflow.PauseQueue("mail"))
	r.AssertNotPaused(t, workflow.PauseWorkflow("fail"))
	r.AssertEnqueuedOn(t, workflow.DefaultQueue, "fail")

	fake := NewRecorder(nil)
	if _, err := fake.Retry(ctx, ref); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("NewRecorder(nil).Retry = %v, want ErrUnsupported", err)
	}
	if len(fake.Retries()) != 0 {
		t.Error("a refused Retry was recorded")
	}
}

func waitStatus(t *testing.T, c workflow.Client, ref workflow.RunRef, want workflow.Status) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if st, err := c.Status(context.Background(), ref); err == nil && st == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("run %+v never reached %s", ref, want)
}
