package batch_test

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
	"github.com/ubgo/workflow/memory"
)

// recStub is a Client that records every enqueued job in order and runs
// nothing, so a test reads the labels and keys the Manager put on each
// job and delivers run ends itself through RunFinished.
type recStub struct {
	mu   sync.Mutex
	caps workflow.Caps
	jobs []workflow.Job
	fail map[string]bool // workflows whose Enqueue fails
}

func newRecStub() *recStub {
	return &recStub{caps: workflow.CapIdempotencyKey | workflow.CapCancel, fail: map[string]bool{}}
}

func (s *recStub) Register(workflow.Workflow) error { return nil }
func (s *recStub) Start(context.Context) error      { return nil }
func (s *recStub) Enqueue(_ context.Context, job workflow.Job) (workflow.RunRef, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail[job.Workflow] {
		return workflow.RunRef{}, errors.New("engine unavailable")
	}
	s.jobs = append(s.jobs, job)
	return workflow.RunRef{RunID: workflow.RunID("run-" + strconv.Itoa(len(s.jobs)))}, nil
}
func (s *recStub) Cancel(context.Context, workflow.RunRef) error { return nil }
func (s *recStub) Status(context.Context, workflow.RunRef) (workflow.Status, error) {
	return workflow.StatusQueued, nil
}
func (s *recStub) Signal(context.Context, workflow.Event) error { return nil }
func (s *recStub) Schedule(context.Context, workflow.Schedule) (workflow.ScheduleID, error) {
	return "", nil
}
func (s *recStub) Unschedule(context.Context, workflow.ScheduleID) error { return nil }
func (s *recStub) Capabilities() workflow.Caps                           { return s.caps }

// of returns the recorded jobs of workflow wf, in order.
func (s *recStub) of(wf string) []workflow.Job {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []workflow.Job
	for _, j := range s.jobs {
		if j.Workflow == wf {
			out = append(out, j)
		}
	}
	return out
}

// workflows returns the workflow of every recorded job, in order.
func (s *recStub) workflows() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.jobs))
	for i, j := range s.jobs {
		out[i] = j.Workflow
	}
	return out
}

// end reports a run end of job idx at attempt (0 sends no attempt label).
func end(t *testing.T, m *batch.Manager, id batch.ID, idx, attempt int, st workflow.Status) {
	t.Helper()
	labels := batch.JobLabels(id, idx, attempt)
	if err := m.RunFinished(context.Background(), labels, st); err != nil {
		t.Fatal(err)
	}
}

func newMgr(c workflow.Client, opts ...batch.Option) (*batch.Manager, *batch.MemoryStore) {
	store := batch.NewMemoryStore()
	return batch.New(c, store, append([]batch.Option{batch.WithManualCallbackRetry()}, opts...)...), store
}

func TestAdd_CallbacksWaitForAddedJobs(t *testing.T) {
	ctx := context.Background()
	s := newRecStub()
	m, _ := newMgr(s)
	id, err := m.Dispatch(ctx, batch.Batch{Jobs: jobs(2), Then: "then", Finally: "finally"})
	if err != nil {
		t.Fatal(err)
	}
	end(t, m, id, 0, 0, workflow.StatusSucceeded)
	if err := m.Add(ctx, id, workflow.Job{Workflow: "job", Labels: map[string]string{"tenant": "acme"}}); err != nil {
		t.Fatal(err)
	}
	added := s.of("job")[2]
	if added.Labels[batch.LabelIndex] != "2" || added.Labels[batch.LabelBatch] != string(id) || added.Labels["tenant"] != "acme" {
		t.Errorf("added job labels = %v", added.Labels)
	}
	end(t, m, id, 1, 0, workflow.StatusSucceeded)
	if n := len(s.of("finally")); n != 0 {
		t.Fatalf("Finally enqueued %d times before the added job ended", n)
	}
	end(t, m, id, 2, 0, workflow.StatusSucceeded)
	if len(s.of("then")) != 1 || len(s.of("finally")) != 1 {
		t.Errorf("then=%d finally=%d, want 1 and 1", len(s.of("then")), len(s.of("finally")))
	}
	p, err := m.Progress(ctx, id)
	if err != nil || p.Total != 3 || p.Succeeded != 3 || !p.Finished {
		t.Errorf("Progress = %+v, %v", p, err)
	}
}

func TestAdd_Refusals(t *testing.T) {
	ctx := context.Background()
	s := newRecStub()
	m, store := newMgr(s)

	done, _ := m.Dispatch(ctx, batch.Batch{Jobs: jobs(1)})
	end(t, m, done, 0, 0, workflow.StatusSucceeded)
	if err := m.Add(ctx, done, jobs(1)...); !errors.Is(err, batch.ErrClosed) {
		t.Errorf("Add to a finished batch = %v, want ErrClosed", err)
	}
	cancelled, _ := m.Dispatch(ctx, batch.Batch{Jobs: jobs(1)})
	if err := m.Cancel(ctx, cancelled); err != nil {
		t.Fatal(err)
	}
	if err := m.Add(ctx, cancelled, jobs(1)...); !errors.Is(err, batch.ErrClosed) {
		t.Errorf("Add to a cancelled batch = %v, want ErrClosed", err)
	}
	open, _ := m.Dispatch(ctx, batch.Batch{Jobs: jobs(1)})
	bad := workflow.Job{Workflow: "job", Labels: map[string]string{batch.LabelIndex: "9"}}
	if err := m.Add(ctx, open, bad); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("Add with a reserved label = %v, want ErrInvalid", err)
	}
	if err := m.Add(ctx, "nope", jobs(1)...); !errors.Is(err, workflow.ErrNotFound) {
		t.Errorf("Add to an unknown batch = %v, want ErrNotFound", err)
	}
	if err := m.Add(ctx, open); err != nil {
		t.Errorf("Add of no jobs = %v, want nil", err)
	}

	// A dispatch still holding its lease refuses the change.
	if err := store.Create(ctx, batch.State{ID: "busy", Jobs: make([]batch.JobState, 1), DispatchUntil: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if err := m.Add(ctx, "busy", jobs(1)...); !errors.Is(err, batch.ErrDispatching) {
		t.Errorf("Add during a dispatch = %v, want ErrDispatching", err)
	}
	if _, err := m.RetryFailed(ctx, "busy"); err == nil {
		t.Error("RetryFailed on an unfinished batch succeeded")
	}
	if err := store.Create(ctx, batch.State{ID: "coord", Jobs: make([]batch.JobState, 1), Dispatched: true, Coordinated: true}); err != nil {
		t.Fatal(err)
	}
	if err := m.Add(ctx, "coord", jobs(1)...); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("Add to a coordinated batch = %v, want ErrUnsupported", err)
	}
}

func TestRetryFailed_RerunsFailedAndRearmsCallbacks(t *testing.T) {
	ctx := context.Background()
	s := newRecStub()
	m, _ := newMgr(s, batch.WithRetryableJobs())
	id, err := m.Dispatch(ctx, batch.Batch{Jobs: jobs(3), Then: "then", Catch: "catch", Finally: "finally"})
	if err != nil {
		t.Fatal(err)
	}
	end(t, m, id, 0, 0, workflow.StatusSucceeded)
	end(t, m, id, 1, 0, workflow.StatusFailed)    // cancels the batch
	end(t, m, id, 2, 0, workflow.StatusCancelled) // stopped by that cancel
	if len(s.of("catch")) != 1 || len(s.of("finally")) != 1 || len(s.of("then")) != 0 {
		t.Fatalf("round 0: catch=%d finally=%d then=%d", len(s.of("catch")), len(s.of("finally")), len(s.of("then")))
	}

	n, err := m.RetryFailed(ctx, id)
	if err != nil || n != 2 {
		t.Fatalf("RetryFailed = %d, %v; want 2, nil", n, err)
	}
	retried := s.of("job")[3:]
	if len(retried) != 2 || retried[0].Labels[batch.LabelIndex] != "1" || retried[1].Labels[batch.LabelIndex] != "2" {
		t.Fatalf("retried jobs = %+v", retried)
	}
	for _, j := range retried {
		if j.Labels[batch.LabelAttempt] != "1" {
			t.Errorf("retried job %s attempt label = %q, want 1", j.Labels[batch.LabelIndex], j.Labels[batch.LabelAttempt])
		}
	}
	p, _ := m.Progress(ctx, id)
	if p.Finished || p.Cancelled || p.Pending != 2 || p.Succeeded != 1 {
		t.Errorf("Progress after RetryFailed = %+v", p)
	}

	// A late end of the replaced run (attempt 0) is ignored.
	end(t, m, id, 1, 0, workflow.StatusFailed)
	if len(s.of("catch")) != 1 {
		t.Fatalf("a late end of the earlier attempt fired Catch")
	}
	end(t, m, id, 1, 1, workflow.StatusSucceeded)
	end(t, m, id, 2, 1, workflow.StatusSucceeded)
	then, fin := s.of("then"), s.of("finally")
	if len(then) != 1 || len(fin) != 2 {
		t.Fatalf("round 1: then=%d finally=%d, want 1 and 2", len(then), len(fin))
	}
	if then[0].IdempotencyKey != "batch:"+string(id)+":then:r1" || fin[1].IdempotencyKey != "batch:"+string(id)+":finally:r1" || fin[0].IdempotencyKey != "batch:"+string(id)+":finally" {
		t.Errorf("keys: then %q, finally %q then %q", then[0].IdempotencyKey, fin[0].IdempotencyKey, fin[1].IdempotencyKey)
	}
	if n, err := m.RetryFailed(ctx, id); n != 0 || err != nil {
		t.Errorf("RetryFailed of a batch that succeeded = %d, %v; want 0, nil", n, err)
	}
}

func TestRetryFailed_AllowFailuresRetriesOnlyFailed(t *testing.T) {
	ctx := context.Background()
	s := newRecStub()
	m, _ := newMgr(s, batch.WithRetryableJobs())
	id, _ := m.Dispatch(ctx, batch.Batch{Jobs: jobs(3), AllowFailures: true})
	end(t, m, id, 0, 0, workflow.StatusFailed)
	end(t, m, id, 1, 0, workflow.StatusCancelled) // cancelled on its own
	end(t, m, id, 2, 0, workflow.StatusSucceeded)
	n, err := m.RetryFailed(ctx, id)
	if err != nil || n != 1 {
		t.Fatalf("RetryFailed = %d, %v; want 1, nil", n, err)
	}
	if got := s.of("job")[3].Labels[batch.LabelIndex]; got != "0" {
		t.Errorf("retried job index = %s, want 0", got)
	}
}

func TestRetryFailed_Refusals(t *testing.T) {
	ctx := context.Background()
	s := newRecStub()
	plain, _ := newMgr(s)
	id, _ := plain.Dispatch(ctx, batch.Batch{Jobs: jobs(1)})
	end(t, plain, id, 0, 0, workflow.StatusFailed)
	if _, err := plain.RetryFailed(ctx, id); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("RetryFailed without WithRetryableJobs = %v, want ErrUnsupported", err)
	}

	m, _ := newMgr(s, batch.WithRetryableJobs())
	running, _ := m.Dispatch(ctx, batch.Batch{Jobs: jobs(2), AllowFailures: true})
	end(t, m, running, 0, 0, workflow.StatusFailed)
	if _, err := m.RetryFailed(ctx, running); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("RetryFailed of a running batch = %v, want ErrInvalid", err)
	}
	cancelled, _ := m.Dispatch(ctx, batch.Batch{Jobs: jobs(1)})
	_ = m.Cancel(ctx, cancelled)
	end(t, m, cancelled, 0, 0, workflow.StatusCancelled)
	if _, err := m.RetryFailed(ctx, cancelled); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("RetryFailed of a batch cancelled by Cancel = %v, want ErrInvalid", err)
	}
}

func TestBeforeAndProgressCallbacks(t *testing.T) {
	ctx := context.Background()
	s := newRecStub()
	m, _ := newMgr(s)
	id, err := m.Dispatch(ctx, batch.Batch{Jobs: jobs(2), Before: "before", Progress: "progress", Finally: "finally"})
	if err != nil {
		t.Fatal(err)
	}
	if got := s.workflows(); len(got) != 3 || got[0] != "before" {
		t.Fatalf("enqueue order = %v, want before first", got)
	}
	end(t, m, id, 0, 0, workflow.StatusFailed)
	end(t, m, id, 0, 0, workflow.StatusFailed) // a repeat is not a new end
	end(t, m, id, 1, 0, workflow.StatusCancelled)
	prog := s.of("progress")
	if len(prog) != 2 {
		t.Fatalf("Progress enqueued %d times, want 2", len(prog))
	}
	if prog[0].IdempotencyKey != "batch:"+string(id)+":progress:0" || prog[1].IdempotencyKey != "batch:"+string(id)+":progress:1" {
		t.Errorf("progress keys = %q, %q", prog[0].IdempotencyKey, prog[1].IdempotencyKey)
	}
	var p batch.Progress
	if err := json.Unmarshal(prog[0].Payload, &p); err != nil || p.Failed != 1 || p.Total != 2 {
		t.Errorf("first progress payload = %+v, %v", p, err)
	}
	got := s.workflows()
	if got[len(got)-1] != "finally" || got[len(got)-2] != "progress" {
		t.Errorf("enqueue order = %v, want the last Progress before Finally", got)
	}
}

func TestBeforeEnqueueFailureStaysPending(t *testing.T) {
	ctx := context.Background()
	s := newRecStub()
	s.fail["before"] = true
	m, store := newMgr(s, batch.WithCallbackRetry(time.Nanosecond))
	id, err := m.Dispatch(ctx, batch.Batch{Jobs: jobs(1), Before: "before"})
	if err != nil {
		t.Fatalf("a failed Before failed Dispatch: %v", err)
	}
	st, _ := store.Get(ctx, id)
	if len(st.Pending) != 1 || st.Pending[0].Kind != batch.CallbackBefore {
		t.Fatalf("pending = %+v, want the Before", st.Pending)
	}
	s.mu.Lock()
	s.fail["before"] = false
	s.mu.Unlock()
	time.Sleep(time.Millisecond) // past the nanosecond claim, on the real clock
	if err := m.RetryCallbacks(ctx); err != nil {
		t.Fatal(err)
	}
	if len(s.of("before")) != 1 {
		t.Errorf("Before enqueued %d times after the retry, want 1", len(s.of("before")))
	}
}

func TestPruneFilterMatches(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cut := t0.Add(time.Hour)
	closed := batch.State{Finished: true, CreatedAt: t0, FinishedAt: t0.Add(time.Minute)}
	closedLate := batch.State{Finished: true, CreatedAt: t0, FinishedAt: cut.Add(time.Minute)}
	pendingCB := closed
	pendingCB.Pending = []batch.PendingCallback{{Kind: batch.CallbackFinally}}
	open := batch.State{CreatedAt: t0, Jobs: make([]batch.JobState, 1)}
	openLate := batch.State{CreatedAt: cut.Add(time.Minute), Jobs: make([]batch.JobState, 1)}
	cancelled := batch.State{CreatedAt: t0, Cancelled: true, CancelledAt: t0, Jobs: make([]batch.JobState, 1)}

	for _, tc := range []struct {
		name string
		f    batch.PruneFilter
		st   batch.State
		want bool
	}{
		{"finished before", batch.PruneFilter{FinishedBefore: cut}, closed, true},
		{"finished after", batch.PruneFilter{FinishedBefore: cut}, closedLate, false},
		{"finished with a callback pending", batch.PruneFilter{FinishedBefore: cut}, pendingCB, false},
		{"open is not finished", batch.PruneFilter{FinishedBefore: cut}, open, false},
		{"unfinished created before", batch.PruneFilter{UnfinishedBefore: cut}, open, true},
		{"unfinished created after", batch.PruneFilter{UnfinishedBefore: cut}, openLate, false},
		{"pending callback counts as unfinished", batch.PruneFilter{UnfinishedBefore: cut}, pendingCB, true},
		{"closed is not unfinished", batch.PruneFilter{UnfinishedBefore: cut}, closed, false},
		{"cancelled before", batch.PruneFilter{CancelledBefore: cut}, cancelled, true},
		{"cancelled rule skips uncancelled", batch.PruneFilter{CancelledBefore: cut}, open, false},
		{"cancelled after", batch.PruneFilter{CancelledBefore: t0}, cancelled, false},
		{"no rule", batch.PruneFilter{}, closed, false},
	} {
		if got := tc.f.Matches(tc.st); got != tc.want {
			t.Errorf("%s: Matches = %t, want %t", tc.name, got, tc.want)
		}
	}
}

// noPrune hides MemoryStore's Prune.
type noPrune struct{ batch.Store }

func TestManagerPrune(t *testing.T) {
	ctx := context.Background()
	s := newRecStub()
	m, store := newMgr(s)
	old, _ := m.Dispatch(ctx, batch.Batch{Jobs: jobs(1)})
	end(t, m, old, 0, 0, workflow.StatusSucceeded)
	running, _ := m.Dispatch(ctx, batch.Batch{Jobs: jobs(1)})
	n, err := m.Prune(ctx, batch.PruneFilter{FinishedBefore: time.Now().Add(time.Minute)})
	if err != nil || n != 1 {
		t.Fatalf("Prune = %d, %v; want 1, nil", n, err)
	}
	if _, err := store.Get(ctx, old); !errors.Is(err, workflow.ErrNotFound) {
		t.Errorf("pruned batch still there: %v", err)
	}
	if _, err := store.Get(ctx, running); err != nil {
		t.Errorf("running batch was pruned: %v", err)
	}
	if _, err := m.Prune(ctx, batch.PruneFilter{}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("Prune with no rule = %v, want ErrInvalid", err)
	}
	other := batch.New(s, noPrune{store})
	if _, err := other.Prune(ctx, batch.PruneFilter{FinishedBefore: time.Now()}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("Prune on a store without Pruner = %v, want ErrUnsupported", err)
	}
}

// awaitEnv runs a two-step workflow on the memory driver: "fan" is an
// AwaitStep over a batch of the given job workflows, and "after"
// reports what fan returned.
func awaitEnv(t *testing.T, jobWorkflow string, opts ...batch.AwaitOption) (workflow.Status, string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := memory.New(memory.WithWorkers(4))
	m := batch.New(c, batch.NewMemoryStore())
	got := make(chan string, 1)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(c.Register(workflow.Workflow{Name: "ok", Steps: []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}}))
	must(c.Register(workflow.Workflow{Name: "fail", Steps: []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) {
		return nil, workflow.Permanent(errors.New("boom"))
	}}}}))
	build := func(_ context.Context, in []byte) (batch.Batch, error) {
		n, err := strconv.Atoi(string(in))
		if err != nil {
			return batch.Batch{}, err
		}
		b := batch.Batch{Name: "fan"}
		for range n {
			b.Jobs = append(b.Jobs, workflow.Job{Workflow: jobWorkflow})
		}
		return b, nil
	}
	fan := m.AwaitStep("fan", build, append([]batch.AwaitOption{batch.AwaitPoll(5 * time.Millisecond)}, opts...)...)
	must(c.Register(workflow.Workflow{Name: "parent", Steps: []workflow.Step{
		fan,
		{Name: "after", After: []string{"fan"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
			got <- string(in)
			return nil, nil
		}},
	}}))
	must(m.Attach(ctx))
	must(c.Start(ctx))
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "parent", Payload: []byte("3")})
	must(err)
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		st, err := c.Status(ctx, ref)
		must(err)
		if st.IsTerminal() {
			select {
			case out := <-got:
				return st, out
			default:
				return st, ""
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("parent run never finished")
	return "", ""
}

func TestAwaitStep_ResumesAfterTheBatch(t *testing.T) {
	st, out := awaitEnv(t, "ok")
	if st != workflow.StatusSucceeded {
		t.Fatalf("parent = %s", st)
	}
	var p batch.Progress
	if err := json.Unmarshal([]byte(out), &p); err != nil || p.Total != 3 || p.Succeeded != 3 || !p.Finished {
		t.Errorf("after got %q (%v)", out, err)
	}
}

func TestAwaitStep_FailedBatchFailsTheStep(t *testing.T) {
	if st, out := awaitEnv(t, "fail"); st != workflow.StatusFailed || out != "" {
		t.Errorf("parent = %s, after got %q; want failed and after not run", st, out)
	}
	st, out := awaitEnv(t, "fail", batch.AwaitAllowFailures())
	var p batch.Progress
	if st != workflow.StatusSucceeded || json.Unmarshal([]byte(out), &p) != nil || p.Failed == 0 {
		t.Errorf("with AwaitAllowFailures: parent = %s, after got %q", st, out)
	}
}

func TestAwaitStep_NeedsStepInfo(t *testing.T) {
	m := batch.New(newRecStub(), batch.NewMemoryStore())
	step := m.AwaitStep("fan", func(context.Context, []byte) (batch.Batch, error) { return batch.Batch{Jobs: jobs(1)}, nil })
	if _, err := step.Run(context.Background(), nil); !workflow.IsPermanent(err) {
		t.Errorf("Run without StepInfo = %v, want a permanent error", err)
	}
	if batch.AwaitID("r1", "fan") != batch.AwaitID("r1", "fan") || batch.AwaitID("r1", "fan") == batch.AwaitID("r2", "fan") {
		t.Error("AwaitID is not a function of run and step")
	}
}

// TestResume_RetriedJobIgnoresTheFinder: a finder lists runs by index, so
// for a job RetryFailed started again it may list the earlier attempt's
// run. A resumed dispatch must not save that ref as the new attempt's.
func TestResume_RetriedJobIgnoresTheFinder(t *testing.T) {
	ctx := context.Background()
	clk := newFake()
	f := &finderStub{groupStub: &groupStub{retryStub: newRetryStub(), refs: allRefs}, found: map[int]workflow.RunRef{1: {RunID: "attempt-0"}}}
	store := batch.NewMemoryStore()
	m := batch.New(enqueueFinder{f}, store, batch.WithClock(clk))
	id := deadDispatch(t, clk, store, 2, false, true, 0)
	if _, err := store.Update(ctx, id, func(s *batch.State) error { s.Jobs[1].Attempt = 1; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := m.ResumeDispatches(ctx); err != nil {
		t.Fatal(err)
	}
	st, s := statuses(t, store, id)
	if !st.Jobs[1].Ref.IsZero() || s[1] != workflow.StatusCancelled {
		t.Fatalf("retried job = %+v, want no ref and cancelled (it may have started with its ref lost)", st.Jobs[1])
	}
}

// enqueueFinder is a RunFinder without RunGroup, for the one-Enqueue-per-
// job path.
type enqueueFinder struct{ f *finderStub }

func (e enqueueFinder) FindBatchRuns(ctx context.Context, id batch.ID, since time.Time) (map[int]workflow.RunRef, error) {
	return e.f.FindBatchRuns(ctx, id, since)
}
func (e enqueueFinder) Register(wf workflow.Workflow) error { return e.f.Register(wf) }
func (e enqueueFinder) Start(ctx context.Context) error     { return e.f.Start(ctx) }
func (e enqueueFinder) Enqueue(ctx context.Context, j workflow.Job) (workflow.RunRef, error) {
	return e.f.Enqueue(ctx, j)
}
func (e enqueueFinder) Cancel(ctx context.Context, r workflow.RunRef) error {
	return e.f.Cancel(ctx, r)
}
func (e enqueueFinder) Status(ctx context.Context, r workflow.RunRef) (workflow.Status, error) {
	return e.f.Status(ctx, r)
}
func (e enqueueFinder) Signal(ctx context.Context, ev workflow.Event) error {
	return e.f.Signal(ctx, ev)
}
func (e enqueueFinder) Schedule(ctx context.Context, s workflow.Schedule) (workflow.ScheduleID, error) {
	return e.f.Schedule(ctx, s)
}
func (e enqueueFinder) Unschedule(ctx context.Context, id workflow.ScheduleID) error {
	return e.f.Unschedule(ctx, id)
}
func (e enqueueFinder) Capabilities() workflow.Caps { return e.f.Capabilities() }

// manyStub answers EnqueueMany itself and records that it did.
type manyStub struct {
	*groupStub
	calls int
}

func (m *manyStub) EnqueueMany(_ context.Context, jobs []workflow.Job) ([]workflow.RunRef, error) {
	m.calls++
	return allRefs(jobs), nil
}

func TestEnqueueMany_PicksThePath(t *testing.T) {
	ctx := context.Background()
	js := []workflow.Job{{Workflow: "a"}, {Workflow: "b"}}

	ms := &manyStub{groupStub: &groupStub{retryStub: newRetryStub(), refs: allRefs}}
	if refs, err := batch.EnqueueMany(ctx, ms, js); err != nil || len(refs) != 2 || ms.calls != 1 || len(ms.groups) != 0 {
		t.Errorf("ManyEnqueuer: refs=%v err=%v calls=%d groups=%d", refs, err, ms.calls, len(ms.groups))
	}

	g := &groupStub{retryStub: newRetryStub(), refs: allRefs}
	refs, err := batch.EnqueueMany(ctx, g, js)
	if err != nil || len(g.groups) != 1 || len(g.jobs) != 0 || refs[1].RunID != "child-1" {
		t.Errorf("GroupRunner: refs=%v err=%v groups=%d enqueues=%d", refs, err, len(g.groups), len(g.jobs))
	}
	if _, ok := g.groups[0][0].Labels[batch.LabelBatch]; ok {
		t.Error("a bulk enqueue put batch labels on its jobs")
	}

	keyed := []workflow.Job{{Workflow: "a", IdempotencyKey: "k"}, {Workflow: "b"}}
	g = &groupStub{retryStub: newRetryStub(), refs: allRefs}
	if _, err := batch.EnqueueMany(ctx, g, keyed); err != nil || len(g.groups) != 0 || len(g.jobs) != 2 {
		t.Errorf("a keyed job must take the loop: err=%v groups=%d enqueues=%d", err, len(g.groups), len(g.jobs))
	}

	declined := &groupStub{retryStub: newRetryStub(), err: workflow.Unsupported("RunGroup", "test")}
	if _, err := batch.EnqueueMany(ctx, declined, js); err != nil || len(declined.jobs) != 2 {
		t.Errorf("a declined group must fall back to the loop: err=%v enqueues=%d", err, len(declined.jobs))
	}

	s := newRecStub()
	s.fail["b"] = true
	refs, err = batch.EnqueueMany(ctx, s, []workflow.Job{{Workflow: "a"}, {Workflow: "b"}, {Workflow: "a"}})
	if err == nil || len(refs) != 3 || refs[0].IsZero() || !refs[1].IsZero() || !refs[2].IsZero() || len(s.of("a")) != 1 {
		t.Errorf("loop failure: refs=%v err=%v a=%d; want job 0 started, the loop stopped at job 1", refs, err, len(s.of("a")))
	}

	short := &groupStub{retryStub: newRetryStub(), refs: func([]workflow.Job) []workflow.RunRef { return []workflow.RunRef{{RunID: "x"}} }}
	if refs, err := batch.EnqueueMany(ctx, short, js); err == nil || len(refs) != 2 || !refs[0].IsZero() {
		t.Errorf("a wrong ref count: refs=%v err=%v; want zero refs and an error", refs, err)
	}
	if refs, err := batch.EnqueueMany(ctx, s, nil); refs != nil || err != nil {
		t.Errorf("no jobs = %v, %v", refs, err)
	}
}

func TestAdd_UsesTheGroupRunner(t *testing.T) {
	ctx := context.Background()
	g := &groupStub{retryStub: newRetryStub(), refs: allRefs}
	m, store := newMgr(g)
	id, err := m.Dispatch(ctx, batch.Batch{Jobs: []workflow.Job{{Workflow: "a"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Add(ctx, id, workflow.Job{Workflow: "a"}, workflow.Job{Workflow: "a"}); err != nil {
		t.Fatal(err)
	}
	if len(g.groups) != 2 || len(g.groups[1]) != 2 || g.groups[1][0].Labels[batch.LabelIndex] != "1" {
		t.Fatalf("groups = %v, want the added jobs as one group from index 1", g.groups)
	}
	st, _ := store.Get(ctx, id)
	if !st.Dispatched || st.Spec != nil || st.Jobs[2].Ref.IsZero() {
		t.Errorf("state after Add = %+v", st)
	}
}
