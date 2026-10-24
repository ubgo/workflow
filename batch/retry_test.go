package batch_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
	"github.com/ubgo/workflow/clock"
)

var errEngineDown = errors.New("engine down")

// retryStub is a Client that records every Enqueue and Cancel, fails the
// enqueues of chosen workflows a set number of times, and reports every
// run as succeeded to Status. It is not a RunNotifier, so tests drive
// RunFinished themselves.
type retryStub struct {
	mu        sync.Mutex
	jobs      []workflow.Job
	cancelled []workflow.RunRef
	failLeft  map[string]int
	n         int
}

func newRetryStub() *retryStub { return &retryStub{failLeft: map[string]int{}} }

func (s *retryStub) Register(workflow.Workflow) error             { return nil }
func (s *retryStub) Start(context.Context) error                  { return nil }
func (s *retryStub) Signal(context.Context, workflow.Event) error { return nil }
func (s *retryStub) Schedule(context.Context, workflow.Schedule) (workflow.ScheduleID, error) {
	return "", nil
}
func (s *retryStub) Unschedule(context.Context, workflow.ScheduleID) error { return nil }
func (s *retryStub) Capabilities() workflow.Caps {
	return workflow.CapIdempotencyKey | workflow.CapCancel
}

func (s *retryStub) Status(context.Context, workflow.RunRef) (workflow.Status, error) {
	return workflow.StatusSucceeded, nil
}

func (s *retryStub) Cancel(_ context.Context, ref workflow.RunRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cancelled = append(s.cancelled, ref)
	return nil
}

func (s *retryStub) Enqueue(_ context.Context, job workflow.Job) (workflow.RunRef, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs = append(s.jobs, job)
	if s.failLeft[job.Workflow] > 0 {
		s.failLeft[job.Workflow]--
		return workflow.RunRef{}, errEngineDown
	}
	s.n++
	return workflow.RunRef{RunID: workflow.RunID(fmt.Sprintf("run-%d", s.n))}, nil
}

// enqueued returns the jobs enqueued for workflow name, failed or not.
func (s *retryStub) enqueued(name string) []workflow.Job {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []workflow.Job
	for _, j := range s.jobs {
		if j.Workflow == name {
			out = append(out, j)
		}
	}
	return out
}

func (s *retryStub) fail(name string, times int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failLeft[name] = times
}

func newFake() *clock.Fake { return clock.NewFake(time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)) }

// finish reports job idx of batch id as ended with status.
func finish(t *testing.T, m *batch.Manager, id batch.ID, idx int, status workflow.Status) error {
	t.Helper()
	return m.RunFinished(context.Background(), map[string]string{batch.LabelBatch: string(id), batch.LabelIndex: fmt.Sprint(idx)}, status)
}

// TestCallbackRetry_RetryCallbacksAfterClaim: a Finally whose enqueue fails
// stays pending with its error, is not retried while its claim holds, and
// lands on the first retry after it, under the same idempotency key.
func TestCallbackRetry_RetryCallbacksAfterClaim(t *testing.T) {
	ctx := context.Background()
	fake, s, store := newFake(), newRetryStub(), batch.NewMemoryStore()
	m := batch.New(s, store, batch.WithClock(fake))
	id, err := m.Dispatch(ctx, batch.Batch{Jobs: []workflow.Job{{Workflow: "w"}}, Finally: "fin"})
	if err != nil {
		t.Fatal(err)
	}
	s.fail("fin", 1)
	if err := finish(t, m, id, 0, workflow.StatusSucceeded); !errors.Is(err, errEngineDown) {
		t.Fatalf("RunFinished err = %v, want the failed enqueue", err)
	}
	st, _ := store.Get(ctx, id)
	if !st.Finished || len(st.Pending) != 1 || st.Pending[0].Kind != batch.CallbackFinally || st.Pending[0].Attempts != 1 || !strings.Contains(st.Pending[0].LastError, "engine down") {
		t.Fatalf("after the failed enqueue: finished=%v pending=%+v", st.Finished, st.Pending)
	}
	if !st.Open() {
		t.Fatal("a finished batch with a pending callback must stay open for the poller")
	}

	if err := m.RetryCallbacks(ctx); err != nil {
		t.Fatal(err)
	}
	if n := len(s.enqueued("fin")); n != 1 {
		t.Fatalf("retried while the claim held: %d enqueues", n)
	}

	fake.Advance(batch.DefaultCallbackRetry)
	if err := m.RetryCallbacks(ctx); err != nil {
		t.Fatal(err)
	}
	got := s.enqueued("fin")
	if len(got) != 2 {
		t.Fatalf("enqueues of fin = %d, want 2 (one failed, one retry)", len(got))
	}
	want := "batch:" + string(id) + ":finally"
	if got[0].IdempotencyKey != want || got[1].IdempotencyKey != want {
		t.Fatalf("idempotency keys = %q, %q; want %q on both", got[0].IdempotencyKey, got[1].IdempotencyKey, want)
	}
	st, _ = store.Get(ctx, id)
	if len(st.Pending) != 0 || st.Open() {
		t.Fatalf("after the retry landed: pending=%+v open=%v", st.Pending, st.Open())
	}
	if open, _ := store.Unfinished(ctx); len(open) != 0 {
		t.Fatalf("Unfinished still lists the closed batch: %d", len(open))
	}
	if err := m.RetryCallbacks(ctx); err != nil || len(s.enqueued("fin")) != 2 {
		t.Fatalf("a landed callback was enqueued again: err=%v n=%d", err, len(s.enqueued("fin")))
	}
}

// TestCallbackRetry_NextRecordRetries: a Catch that fails on the first
// failure is retried by the next job end recorded for the batch.
func TestCallbackRetry_NextRecordRetries(t *testing.T) {
	ctx := context.Background()
	fake, s, store := newFake(), newRetryStub(), batch.NewMemoryStore()
	m := batch.New(s, store, batch.WithClock(fake), batch.WithCallbackRetry(time.Second))
	id, err := m.Dispatch(ctx, batch.Batch{Jobs: []workflow.Job{{Workflow: "w"}, {Workflow: "w"}}, Catch: "cat", Finally: "fin", AllowFailures: true})
	if err != nil {
		t.Fatal(err)
	}
	s.fail("cat", 1)
	if err := finish(t, m, id, 0, workflow.StatusFailed); !errors.Is(err, errEngineDown) {
		t.Fatalf("err = %v", err)
	}
	// A repeat of the same job end inside the claim must not retry.
	if err := finish(t, m, id, 0, workflow.StatusFailed); err != nil {
		t.Fatal(err)
	}
	if n := len(s.enqueued("cat")); n != 1 {
		t.Fatalf("catch retried while its claim held: %d enqueues", n)
	}
	fake.Advance(time.Second)
	if err := finish(t, m, id, 1, workflow.StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	if c, f := len(s.enqueued("cat")), len(s.enqueued("fin")); c != 2 || f != 1 {
		t.Fatalf("catch enqueues = %d (want 2), finally = %d (want 1)", c, f)
	}
	if st, _ := store.Get(ctx, id); len(st.Pending) != 0 {
		t.Fatalf("pending = %+v", st.Pending)
	}
}

// TestCallbackRetry_PollTickRetries: the status poller retries a callback
// of a batch whose jobs have all ended, with no further job end to trigger
// it.
func TestCallbackRetry_PollTickRetries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fake, s, store := newFake(), newRetryStub(), batch.NewMemoryStore()
	m := batch.New(s, store, batch.WithClock(fake), batch.WithCallbackRetry(pollEvery), batch.WithCompletionSource(batch.PollSource(pollEvery)))
	if err := m.Attach(ctx); err != nil {
		t.Fatal(err)
	}
	id, err := m.Dispatch(ctx, batch.Batch{Jobs: []workflow.Job{{Workflow: "w"}}, Then: "then"})
	if err != nil {
		t.Fatal(err)
	}
	s.fail("then", 1)
	if err := finish(t, m, id, 0, workflow.StatusSucceeded); !errors.Is(err, errEngineDown) {
		t.Fatalf("err = %v", err)
	}
	wctx, wcancel := context.WithTimeout(ctx, waitTimeout)
	defer wcancel()
	if err := fake.BlockUntil(wctx, 1); err != nil {
		t.Fatal(err)
	}
	fake.Advance(pollEvery)
	deadline := time.Now().Add(waitTimeout)
	for len(s.enqueued("then")) < 2 {
		if time.Now().After(deadline) {
			t.Fatal("the poll tick never retried the callback")
		}
		time.Sleep(time.Millisecond)
	}
	for {
		if st, _ := store.Get(ctx, id); len(st.Pending) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the retried callback was never cleared")
		}
		time.Sleep(time.Millisecond)
	}
}

// hookOnly is a retryStub whose run-finished hook is guaranteed, so the
// default sources are the hook alone and no poller runs. The test drives
// RunFinished itself, as the hook would.
type hookOnly struct{ *retryStub }

func (h hookOnly) Capabilities() workflow.Caps {
	return h.retryStub.Capabilities() | workflow.CapRunFinished
}

func (hookOnly) OnRunFinished(workflow.RunFinishedFunc) error { return nil }

// failFinallyHookOnly attaches a Manager with the default sources on a
// hook-only client, then fails the Finally of a one-job batch, leaving it
// pending on a finished batch that no further job end will touch.
func failFinallyHookOnly(t *testing.T, opts ...batch.Option) (*clock.Fake, *retryStub, *batch.MemoryStore, batch.ID, context.Context) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	fake, s, store := newFake(), newRetryStub(), batch.NewMemoryStore()
	m := batch.New(hookOnly{s}, store, append([]batch.Option{batch.WithClock(fake)}, opts...)...)
	if err := m.Attach(ctx); err != nil {
		t.Fatal(err)
	}
	id, err := m.Dispatch(ctx, batch.Batch{Jobs: []workflow.Job{{Workflow: "w"}}, Finally: "fin"})
	if err != nil {
		t.Fatal(err)
	}
	s.fail("fin", 1)
	if err := finish(t, m, id, 0, workflow.StatusSucceeded); !errors.Is(err, errEngineDown) {
		t.Fatalf("err = %v", err)
	}
	return fake, s, store, id, ctx
}

// TestCallbackRetry_HookOnlyLoopRetries: with no poller, the retry loop
// Attach starts lands a failed Finally after the interval, with no call
// from the app.
func TestCallbackRetry_HookOnlyLoopRetries(t *testing.T) {
	fake, s, store, id, ctx := failFinallyHookOnly(t)
	wctx, wcancel := context.WithTimeout(ctx, waitTimeout)
	defer wcancel()
	if err := fake.BlockUntil(wctx, 1); err != nil {
		t.Fatalf("no retry loop armed a timer: %v", err)
	}
	fake.Advance(batch.DefaultCallbackRetry)
	deadline := time.Now().Add(waitTimeout)
	for {
		st, _ := store.Get(ctx, id)
		if len(s.enqueued("fin")) == 2 && len(st.Pending) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Finally not retried by the loop: enqueues=%d pending=%+v", len(s.enqueued("fin")), st.Pending)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestCallbackRetry_ManualRetryStartsNoLoop: WithManualCallbackRetry leaves
// retrying to the app, so no timer is armed and nothing is retried.
func TestCallbackRetry_ManualRetryStartsNoLoop(t *testing.T) {
	fake, s, _, _, ctx := failFinallyHookOnly(t, batch.WithManualCallbackRetry())
	wctx, wcancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer wcancel()
	if err := fake.BlockUntil(wctx, 1); err == nil {
		t.Fatal("a timer was armed although retry is manual")
	}
	fake.Advance(batch.DefaultCallbackRetry)
	if n := len(s.enqueued("fin")); n != 1 {
		t.Fatalf("Finally retried without the app asking: %d enqueues", n)
	}
}
