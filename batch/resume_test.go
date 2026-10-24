package batch_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
	"github.com/ubgo/workflow/clock"
)

// finderStub is a groupStub that also answers FindBatchRuns from found.
type finderStub struct {
	*groupStub
	found    map[int]workflow.RunRef
	findErr  error
	findings int
}

var _ batch.RunFinder = (*finderStub)(nil)

func (f *finderStub) FindBatchRuns(context.Context, batch.ID, time.Time) (map[int]workflow.RunRef, error) {
	f.findings++
	return f.found, f.findErr
}

// enqueueOnly hides groupStub's RunGroup, for the one-Enqueue-per-job path.
type enqueueOnly struct{ *retryStub }

// deadDispatch seeds the store with batch "b1" of n jobs of workflow "a"
// as a process that died mid-dispatch left it: refs saved for the jobs in
// started, the lease ended at the fake clock's now, and the labelled
// specs kept when keepSpec is set.
func deadDispatch(t *testing.T, clk *clock.Fake, store batch.Store, n int, grouped, keepSpec bool, started ...int) batch.ID {
	t.Helper()
	id := batch.ID("b1")
	st := batch.State{
		ID: id, Then: "then", Finally: "finally", Jobs: make([]batch.JobState, n),
		CreatedAt: clk.Now().Add(-time.Hour), DispatchOwner: "dead", DispatchUntil: clk.Now(), Grouped: grouped,
	}
	for _, i := range started {
		st.Jobs[i].Ref = workflow.RunRef{RunID: workflow.RunID(fmt.Sprintf("old-%d", i))}
	}
	if keepSpec {
		for i := range n {
			st.Spec = append(st.Spec, workflow.Job{Workflow: "a", Labels: map[string]string{batch.LabelBatch: string(id), batch.LabelIndex: strconv.Itoa(i)}})
		}
	}
	if err := store.Create(context.Background(), st); err != nil {
		t.Fatal(err)
	}
	return id
}

func statuses(t *testing.T, store batch.Store, id batch.ID) (batch.State, []workflow.Status) {
	t.Helper()
	st, err := store.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]workflow.Status, len(st.Jobs))
	for i, j := range st.Jobs {
		out[i] = j.Status
	}
	return st, out
}

func TestResume_EnqueuePathStartsJobsAfterTheAmbiguousOne(t *testing.T) {
	ctx := context.Background()
	clk := newFake()
	stub := newRetryStub()
	store := batch.NewMemoryStore()
	m := batch.New(enqueueOnly{stub}, store, batch.WithClock(clk))
	id := deadDispatch(t, clk, store, 4, false, true, 0)

	if err := m.ResumeDispatches(ctx); err != nil {
		t.Fatal(err)
	}
	got := stub.enqueued("a")
	if len(got) != 2 || got[0].Labels[batch.LabelIndex] != "2" || got[1].Labels[batch.LabelIndex] != "3" {
		t.Fatalf("resumed enqueues = %v, want jobs 2 and 3", got)
	}
	st, s := statuses(t, store, id)
	if s[1] != workflow.StatusCancelled || s[0] != "" || s[2] != "" || s[3] != "" {
		t.Fatalf("statuses = %v, want only job 1 (may have started) cancelled", s)
	}
	if !st.Dispatched || st.Spec != nil || st.Jobs[2].Ref.IsZero() || st.Jobs[3].Ref.IsZero() {
		t.Fatalf("state after resume = %+v, want dispatched, specs dropped, refs for 2 and 3", st)
	}
	for _, i := range []int{0, 2, 3} {
		if err := m.RunFinished(ctx, map[string]string{batch.LabelBatch: string(id), batch.LabelIndex: strconv.Itoa(i)}, workflow.StatusSucceeded); err != nil {
			t.Fatal(err)
		}
	}
	if len(stub.enqueued("finally")) != 1 || len(stub.enqueued("then")) != 0 {
		t.Fatalf("finally=%d then=%d, want Finally once and no Then (job 1 was cancelled)", len(stub.enqueued("finally")), len(stub.enqueued("then")))
	}
}

func TestResume_WithoutSpecsCancelsTheUnstartedJobs(t *testing.T) {
	ctx := context.Background()
	clk := newFake()
	stub := newRetryStub()
	store := batch.NewMemoryStore()
	m := batch.New(enqueueOnly{stub}, store, batch.WithClock(clk))
	id := deadDispatch(t, clk, store, 3, false, false, 0)
	if err := m.ResumeDispatches(ctx); err != nil {
		t.Fatal(err)
	}
	if n := len(stub.enqueued("a")); n != 0 {
		t.Fatalf("resume enqueued %d jobs without specs, want 0", n)
	}
	st, s := statuses(t, store, id)
	if s[1] != workflow.StatusCancelled || s[2] != workflow.StatusCancelled || !st.Dispatched {
		t.Fatalf("statuses = %v dispatched=%v, want jobs 1 and 2 cancelled and the batch dispatched", s, st.Dispatched)
	}
}

func TestResume_GroupedWithFinderSavesFoundRefsAndStartsTheRest(t *testing.T) {
	ctx := context.Background()
	clk := newFake()
	f := &finderStub{groupStub: &groupStub{retryStub: newRetryStub(), refs: allRefs}, found: map[int]workflow.RunRef{1: {RunID: "found-1"}}}
	store := batch.NewMemoryStore()
	m := batch.New(f, store, batch.WithClock(clk))
	id := deadDispatch(t, clk, store, 4, true, true, 0)

	if err := m.ResumeDispatches(ctx); err != nil {
		t.Fatal(err)
	}
	if len(f.groups) != 1 || len(f.groups[0]) != 2 {
		t.Fatalf("groups = %v, want one group of jobs 2 and 3", f.groups)
	}
	for k, want := range []string{"2", "3"} {
		if got := f.groups[0][k].Labels[batch.LabelIndex]; got != want {
			t.Fatalf("group job %d has index %s, want %s", k, got, want)
		}
	}
	st, s := statuses(t, store, id)
	if st.Jobs[1].Ref.RunID != "found-1" || st.Jobs[2].Ref.IsZero() || st.Jobs[3].Ref.IsZero() || !st.Dispatched {
		t.Fatalf("state = %+v, want the found ref saved, refs for 2 and 3, dispatched", st)
	}
	for i, x := range s {
		if x != "" {
			t.Fatalf("job %d status %s, want none cancelled", i, x)
		}
	}
}

func TestResume_GroupedWithoutFinderCancelsEveryJobWithoutRef(t *testing.T) {
	ctx := context.Background()
	clk := newFake()
	g := &groupStub{retryStub: newRetryStub(), refs: allRefs}
	store := batch.NewMemoryStore()
	m := batch.New(g, store, batch.WithClock(clk))
	id := deadDispatch(t, clk, store, 3, true, true)
	if err := m.ResumeDispatches(ctx); err != nil {
		t.Fatal(err)
	}
	if len(g.groups) != 0 || len(g.enqueued("a")) != 0 {
		t.Fatal("resume started jobs that may already run")
	}
	st, s := statuses(t, store, id)
	if !st.Finished || s[0] != workflow.StatusCancelled || s[2] != workflow.StatusCancelled {
		t.Fatalf("state = %+v, want every job cancelled and the batch finished", st)
	}
	if len(g.enqueued("finally")) != 1 {
		t.Fatalf("Finally enqueued %d times, want once", len(g.enqueued("finally")))
	}
}

func TestResume_DelayedJobIsNotRestartedOnTheFindersWord(t *testing.T) {
	ctx := context.Background()
	clk := newFake()
	f := &finderStub{groupStub: &groupStub{retryStub: newRetryStub(), refs: allRefs}}
	store := batch.NewMemoryStore()
	m := batch.New(enqueueOnlyFinder{f}, store, batch.WithClock(clk), batch.WithResumableDispatch())
	id := deadDispatch(t, clk, store, 2, false, true)
	if _, err := store.Update(ctx, id, func(s *batch.State) error {
		s.Spec[0].RunAt = clk.Now().Add(time.Hour)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.ResumeDispatches(ctx); err != nil {
		t.Fatal(err)
	}
	_, s := statuses(t, store, id)
	if s[0] != workflow.StatusCancelled || s[1] != "" {
		t.Fatalf("statuses = %v, want the delayed job cancelled and job 1 started", s)
	}
	if got := f.enqueued("a"); len(got) != 1 || got[0].Labels[batch.LabelIndex] != "1" {
		t.Fatalf("enqueued %v, want only job 1", got)
	}
}

// enqueueOnlyFinder is a finder without RunGroup.
type enqueueOnlyFinder struct{ f *finderStub }

func (e enqueueOnlyFinder) Register(w workflow.Workflow) error { return e.f.Register(w) }
func (e enqueueOnlyFinder) Start(ctx context.Context) error    { return e.f.Start(ctx) }
func (e enqueueOnlyFinder) Signal(ctx context.Context, ev workflow.Event) error {
	return e.f.Signal(ctx, ev)
}
func (e enqueueOnlyFinder) Schedule(ctx context.Context, s workflow.Schedule) (workflow.ScheduleID, error) {
	return e.f.Schedule(ctx, s)
}
func (e enqueueOnlyFinder) Unschedule(ctx context.Context, id workflow.ScheduleID) error {
	return e.f.Unschedule(ctx, id)
}
func (e enqueueOnlyFinder) Capabilities() workflow.Caps { return e.f.Capabilities() }
func (e enqueueOnlyFinder) Status(ctx context.Context, r workflow.RunRef) (workflow.Status, error) {
	return e.f.Status(ctx, r)
}
func (e enqueueOnlyFinder) Cancel(ctx context.Context, r workflow.RunRef) error {
	return e.f.Cancel(ctx, r)
}
func (e enqueueOnlyFinder) Enqueue(ctx context.Context, j workflow.Job) (workflow.RunRef, error) {
	return e.f.Enqueue(ctx, j)
}
func (e enqueueOnlyFinder) FindBatchRuns(ctx context.Context, id batch.ID, since time.Time) (map[int]workflow.RunRef, error) {
	return e.f.FindBatchRuns(ctx, id, since)
}

func TestResume_CancelledBatchStartsNothingAndCancelsFoundRuns(t *testing.T) {
	ctx := context.Background()
	clk := newFake()
	f := &finderStub{groupStub: &groupStub{retryStub: newRetryStub(), refs: allRefs}, found: map[int]workflow.RunRef{0: {RunID: "found-0"}}}
	store := batch.NewMemoryStore()
	m := batch.New(f, store, batch.WithClock(clk))
	id := deadDispatch(t, clk, store, 3, true, true)
	if err := m.Cancel(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := m.ResumeDispatches(ctx); err != nil {
		t.Fatal(err)
	}
	if len(f.groups) != 0 || len(f.enqueued("a")) != 0 {
		t.Fatal("a cancelled batch started jobs on resume")
	}
	if len(f.cancelled) != 1 || f.cancelled[0].RunID != "found-0" {
		t.Fatalf("cancelled = %v, want the found run", f.cancelled)
	}
	_, s := statuses(t, store, id)
	if s[1] != workflow.StatusCancelled || s[2] != workflow.StatusCancelled || s[0] != "" {
		t.Fatalf("statuses = %v, want 1 and 2 cancelled, 0 waiting for its run's end", s)
	}
}

func TestResume_WaitsForTheLeaseAndDoesNotTouchFinishedDispatches(t *testing.T) {
	ctx := context.Background()
	clk := newFake()
	stub := newRetryStub()
	store := batch.NewMemoryStore()
	m := batch.New(enqueueOnly{stub}, store, batch.WithClock(clk), batch.WithDispatchLease(time.Minute))
	id := deadDispatch(t, clk, store, 3, false, true, 0)
	if _, err := store.Update(ctx, id, func(s *batch.State) error {
		s.DispatchUntil = clk.Now().Add(time.Minute)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.ResumeDispatches(ctx); err != nil {
		t.Fatal(err)
	}
	if len(stub.enqueued("a")) != 0 {
		t.Fatal("resumed a dispatch whose lease had not ended")
	}
	clk.Advance(time.Minute)
	if err := m.ResumeDispatches(ctx); err != nil {
		t.Fatal(err)
	}
	if n := len(stub.enqueued("a")); n != 1 {
		t.Fatalf("enqueued %d after the lease ended, want 1 (job 2)", n)
	}
	// A second pass finds the batch dispatched and does nothing.
	if err := m.ResumeDispatches(ctx); err != nil {
		t.Fatal(err)
	}
	if n := len(stub.enqueued("a")); n != 1 {
		t.Fatalf("enqueued %d after a second pass, want still 1", n)
	}
}

func TestResume_StateFromBeforeTheDispatchFieldsIsOnlyMarkedDispatched(t *testing.T) {
	ctx := context.Background()
	clk := newFake()
	stub := newRetryStub()
	store := batch.NewMemoryStore()
	m := batch.New(enqueueOnly{stub}, store, batch.WithClock(clk))
	st := batch.State{ID: "old", Jobs: []batch.JobState{{Ref: workflow.RunRef{RunID: "r0"}}, {Ref: workflow.RunRef{RunID: "r1"}}}, CreatedAt: clk.Now()}
	if err := store.Create(ctx, st); err != nil {
		t.Fatal(err)
	}
	if err := m.ResumeDispatches(ctx); err != nil {
		t.Fatal(err)
	}
	got, _ := statuses(t, store, "old")
	if !got.Dispatched || len(stub.enqueued("a")) != 0 || got.Jobs[0].Status != "" {
		t.Fatalf("state = %+v, want only Dispatched set", got)
	}
}

func TestResume_DispatcherThatLostItsLeaseStops(t *testing.T) {
	ctx := context.Background()
	clk := newFake()
	stub := &takeoverStub{retryStub: newRetryStub(), clk: clk}
	store := batch.NewMemoryStore()
	m := batch.New(stub, store, batch.WithClock(clk), batch.WithDispatchLease(time.Minute), batch.WithResumableDispatch())
	stub.m = m
	_, err := m.Dispatch(ctx, batch.Batch{Jobs: []workflow.Job{{Workflow: "a"}, {Workflow: "a"}, {Workflow: "a"}}})
	if err == nil {
		t.Fatal("Dispatch returned nil after another process took its lease")
	}
	// Job 0 by the dispatcher; the resumer cancelled job 1 (in flight, ref
	// lost) and started job 2; the dispatcher started nothing after that.
	if n := len(stub.enqueued("a")); n != 3 {
		t.Fatalf("enqueued %d runs, want 3 (no job started twice)", n)
	}
}

func TestDispatch_StopsWhenAnotherOwnerHoldsTheLease(t *testing.T) {
	ctx := context.Background()
	clk := newFake()
	store := batch.NewMemoryStore()
	stub := &takeoverStub{retryStub: newRetryStub(), clk: clk, store: store}
	m := batch.New(stub, store, batch.WithClock(clk))
	_, err := m.Dispatch(ctx, batch.Batch{Jobs: []workflow.Job{{Workflow: "a"}, {Workflow: "a"}, {Workflow: "a"}}})
	if err == nil {
		t.Fatal("Dispatch returned nil after another owner took its lease")
	}
	if n := len(stub.enqueued("a")); n != 2 {
		t.Fatalf("enqueued %d runs, want 2: the dispatcher must stop once it lost the lease", n)
	}
}

// takeoverStub stalls the second Enqueue past the dispatch lease and runs
// a resumer from another "process" (the same Manager) meanwhile.
type takeoverStub struct {
	*retryStub
	clk   *clock.Fake
	m     *batch.Manager
	store batch.Store // when set, the takeover only claims the lease, as a resumer that is still working
	calls int
}

func (s *takeoverStub) Enqueue(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
	s.calls++
	ref, err := s.retryStub.Enqueue(ctx, job)
	if s.calls == 2 && s.store != nil {
		id := batch.ID(job.Labels[batch.LabelBatch])
		_, uerr := s.store.Update(ctx, id, func(st *batch.State) error {
			st.DispatchOwner, st.DispatchUntil = "resumer", s.clk.Now().Add(time.Hour)
			return nil
		})
		return ref, errors.Join(err, uerr)
	}
	if s.calls == 2 {
		s.clk.Advance(2 * time.Minute)
		if rerr := s.m.ResumeDispatches(ctx); rerr != nil && !errors.Is(rerr, context.Canceled) {
			return ref, rerr
		}
	}
	return ref, err
}

func TestDispatch_MarksDispatchedAndDropsSpecs(t *testing.T) {
	ctx := context.Background()
	for name, c := range map[string]workflow.Client{
		"enqueue": enqueueOnly{newRetryStub()},
		"group":   &groupStub{retryStub: newRetryStub(), refs: allRefs},
	} {
		t.Run(name, func(t *testing.T) {
			store := batch.NewMemoryStore()
			id, err := batch.New(c, store).Dispatch(ctx, batch.Batch{Jobs: []workflow.Job{{Workflow: "a"}, {Workflow: "a"}}})
			if err != nil {
				t.Fatal(err)
			}
			st, _ := statuses(t, store, id)
			if !st.Dispatched || st.Spec != nil || st.DispatchOwner == "" {
				t.Fatalf("state = %+v, want dispatched with specs dropped", st)
			}
		})
	}
}
