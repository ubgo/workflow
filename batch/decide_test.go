package batch_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
)

// stub is a Client that records enqueues and cancels and never runs
// anything, so a test delivers run ends itself through RunFinished and
// controls their order and concurrency exactly.
type stub struct {
	mu        sync.Mutex
	caps      workflow.Caps
	enqueued  map[string]int
	cancelled int
	failAt    int // the 1-based enqueue that fails; 0 never
	calls     int
}

func newStub(caps workflow.Caps) *stub { return &stub{caps: caps, enqueued: map[string]int{}} }

func (s *stub) Register(workflow.Workflow) error { return nil }
func (s *stub) Start(context.Context) error      { return nil }
func (s *stub) Enqueue(_ context.Context, job workflow.Job) (workflow.RunRef, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.calls == s.failAt {
		return workflow.RunRef{}, errors.New("enqueue refused")
	}
	s.enqueued[job.Workflow]++
	return workflow.RunRef{RunID: workflow.RunID("r" + strconv.Itoa(s.calls))}, nil
}
func (s *stub) Cancel(context.Context, workflow.RunRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cancelled++
	return nil
}
func (s *stub) Status(context.Context, workflow.RunRef) (workflow.Status, error) {
	return "", workflow.ErrNotFound
}
func (s *stub) Signal(context.Context, workflow.Event) error { return nil }
func (s *stub) Schedule(context.Context, workflow.Schedule) (workflow.ScheduleID, error) {
	return "", nil
}
func (s *stub) Unschedule(context.Context, workflow.ScheduleID) error { return nil }
func (s *stub) Capabilities() workflow.Caps                           { return s.caps }

func (s *stub) count(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.enqueued[name]
}

func jobs(n int) []workflow.Job {
	out := make([]workflow.Job, n)
	for i := range out {
		out[i] = workflow.Job{Workflow: "job"}
	}
	return out
}

func finished(id batch.ID, idx int) map[string]string {
	return map[string]string{batch.LabelBatch: string(id), batch.LabelIndex: strconv.Itoa(idx)}
}

// TestConcurrentEndsDecideOnce delivers every job's end from its own
// goroutine, twice, many times over: Then and Finally are enqueued exactly
// once per batch, never zero and never twice.
func TestConcurrentEndsDecideOnce(t *testing.T) {
	const size, rounds = 20, 50
	ctx := context.Background()
	for round := range rounds {
		s := newStub(0)
		m := batch.New(s, batch.NewMemoryStore())
		id, err := m.Dispatch(ctx, batch.Batch{Jobs: jobs(size), Then: "then", Catch: "catch", Finally: "finally"})
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		for i := range size * 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := m.RunFinished(ctx, finished(id, i%size), workflow.StatusSucceeded); err != nil {
					t.Error(err)
				}
			}()
		}
		wg.Wait()
		if s.count("then") != 1 || s.count("finally") != 1 || s.count("catch") != 0 {
			t.Fatalf("round %d: then=%d catch=%d finally=%d, want 1 0 1", round, s.count("then"), s.count("catch"), s.count("finally"))
		}
	}
}

// TestConcurrentFailuresCatchOnce races several failures: one Catch, one
// Finally, no Then, and the batch cancels the jobs still running.
func TestConcurrentFailuresCatchOnce(t *testing.T) {
	const size = 10
	ctx := context.Background()
	s := newStub(0)
	m := batch.New(s, batch.NewMemoryStore())
	id, err := m.Dispatch(ctx, batch.Batch{Jobs: jobs(size), Then: "then", Catch: "catch", Finally: "finally"})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := range size / 2 {
		wg.Add(1)
		go func() { defer wg.Done(); _ = m.RunFinished(ctx, finished(id, i), workflow.StatusFailed) }()
	}
	wg.Wait()
	if s.count("catch") != 1 || s.count("finally") != 0 {
		t.Fatalf("catch=%d finally=%d before the rest finished", s.count("catch"), s.count("finally"))
	}
	// The first failure's cancel works from its own snapshot, so runs
	// failing at the same moment may also get a Cancel, which is
	// idempotent; every run still going must get one.
	if s.cancelled < size/2 || s.cancelled > size-1 {
		t.Errorf("cancelled %d runs, want between %d and %d", s.cancelled, size/2, size-1)
	}
	for i := size / 2; i < size; i++ {
		_ = m.RunFinished(ctx, finished(id, i), workflow.StatusCancelled)
	}
	if s.count("then") != 0 || s.count("catch") != 1 || s.count("finally") != 1 {
		t.Errorf("then=%d catch=%d finally=%d, want 0 1 1", s.count("then"), s.count("catch"), s.count("finally"))
	}
}

// TestCancelledJobIsNotAFailure: a job cancelled on its own fires no
// Catch, but the batch does not count as a success either.
func TestCancelledJobIsNotAFailure(t *testing.T) {
	ctx := context.Background()
	s := newStub(0)
	m := batch.New(s, batch.NewMemoryStore())
	id, _ := m.Dispatch(ctx, batch.Batch{Jobs: jobs(2), Then: "then", Catch: "catch", Finally: "finally"})
	_ = m.RunFinished(ctx, finished(id, 0), workflow.StatusCancelled)
	_ = m.RunFinished(ctx, finished(id, 1), workflow.StatusSucceeded)
	if s.count("then") != 0 || s.count("catch") != 0 || s.count("finally") != 1 {
		t.Errorf("then=%d catch=%d finally=%d, want 0 0 1", s.count("then"), s.count("catch"), s.count("finally"))
	}
}

func TestIgnoredEnds(t *testing.T) {
	ctx := context.Background()
	s := newStub(0)
	m := batch.New(s, batch.NewMemoryStore())
	id, _ := m.Dispatch(ctx, batch.Batch{Jobs: jobs(1), Finally: "finally"})
	for _, labels := range []map[string]string{
		nil,
		{batch.LabelBatch: string(id)},
		{batch.LabelBatch: string(id), batch.LabelIndex: "x"},
		{batch.LabelBatch: string(id), batch.LabelIndex: "5"},
		{batch.LabelBatch: "unknown", batch.LabelIndex: "0"},
	} {
		if err := m.RunFinished(ctx, labels, workflow.StatusSucceeded); err != nil {
			t.Errorf("RunFinished(%v) = %v", labels, err)
		}
	}
	if err := m.RunFinished(ctx, finished(id, 0), workflow.StatusRunning); err != nil {
		t.Error(err)
	}
	if p, _ := m.Progress(ctx, id); p.Pending != 1 || s.count("finally") != 0 {
		t.Errorf("an ignored end changed the batch: %+v", p)
	}
}

// TestEnqueueFailureAborts: when the second of three enqueues fails,
// Dispatch returns the error and the ID, the third job is never enqueued,
// the first is cancelled, and Finally runs once the first reports back.
func TestEnqueueFailureAborts(t *testing.T) {
	ctx := context.Background()
	s := newStub(0)
	s.failAt = 2
	m := batch.New(s, batch.NewMemoryStore())
	id, err := m.Dispatch(ctx, batch.Batch{Jobs: jobs(3), Then: "then", Finally: "finally"})
	if err == nil || id == "" {
		t.Fatalf("Dispatch = %q, %v; want the ID and an error", id, err)
	}
	if s.count("job") != 1 || s.cancelled != 1 {
		t.Errorf("enqueued %d jobs, cancelled %d; want 1 and 1", s.count("job"), s.cancelled)
	}
	p, _ := m.Progress(ctx, id)
	if !p.Cancelled || p.CancelledJobs != 2 || p.Pending != 1 || s.count("finally") != 0 {
		t.Errorf("Progress = %+v", p)
	}
	_ = m.RunFinished(ctx, finished(id, 0), workflow.StatusCancelled)
	if s.count("finally") != 1 || s.count("then") != 0 {
		t.Errorf("then=%d finally=%d, want 0 1", s.count("then"), s.count("finally"))
	}
}

// TestAttachRefusesOnlyImpossibleSources: the defaults work on any client
// (a plain Client gets the poller), and Attach refuses only a source that
// cannot work.
func TestAttachRefusesOnlyImpossibleSources(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := batch.New(newStub(0), batch.NewMemoryStore()).Attach(ctx); err != nil {
		t.Errorf("default sources on a plain client: %v", err)
	}
	hook := batch.New(newStub(workflow.CapRunFinished), batch.NewMemoryStore(), batch.WithCompletionSource(batch.HookSource()))
	if err := hook.Attach(ctx); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("HookSource on a client without RunNotifier = %v, want ErrUnsupported", err)
	}
	poll := batch.New(newStub(0), batch.NewMemoryStore(), batch.WithStatusPoll(0))
	if err := poll.Attach(ctx); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("zero poll interval = %v, want ErrInvalid", err)
	}
}

func TestCallbackIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	keys := &keyStub{stub: newStub(workflow.CapIdempotencyKey)}
	m := batch.New(keys, batch.NewMemoryStore())
	id, _ := m.Dispatch(ctx, batch.Batch{Jobs: jobs(1), Then: "then", Finally: "finally"})
	_ = m.RunFinished(ctx, finished(id, 0), workflow.StatusSucceeded)
	want := map[string]string{"then": "batch:" + string(id) + ":then", "finally": "batch:" + string(id) + ":finally"}
	for name, key := range want {
		if keys.keys[name] != key {
			t.Errorf("%s key = %q, want %q", name, keys.keys[name], key)
		}
	}
	if keys.keys["job"] != "" {
		t.Errorf("a batch job got a key it did not ask for: %q", keys.keys["job"])
	}
}

type keyStub struct {
	*stub
	keys map[string]string
}

func (k *keyStub) Enqueue(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
	if k.keys == nil {
		k.keys = map[string]string{}
	}
	k.keys[job.Workflow] = job.IdempotencyKey
	return k.stub.Enqueue(ctx, job)
}

// TestCancelledBatchSkipsThen: jobs that all succeed after Cancel (they
// finished before the cancel reached them) still do not run Then.
func TestCancelledBatchSkipsThen(t *testing.T) {
	ctx := context.Background()
	s := newStub(0)
	m := batch.New(s, batch.NewMemoryStore())
	id, _ := m.Dispatch(ctx, batch.Batch{Jobs: jobs(2), Then: "then", Catch: "catch", Finally: "finally"})
	if err := m.Cancel(ctx, id); err != nil {
		t.Fatal(err)
	}
	_ = m.RunFinished(ctx, finished(id, 0), workflow.StatusSucceeded)
	_ = m.RunFinished(ctx, finished(id, 1), workflow.StatusSucceeded)
	if s.count("then") != 0 || s.count("catch") != 0 || s.count("finally") != 1 {
		t.Errorf("then=%d catch=%d finally=%d, want 0 0 1", s.count("then"), s.count("catch"), s.count("finally"))
	}
}
