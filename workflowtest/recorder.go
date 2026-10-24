package workflowtest

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/ubgo/workflow"
)

// allCaps is what the Recorder's fake mode claims: every capability the
// core defines except CapRunFinished. It refuses nothing because it runs
// nothing, but it implements no RunNotifier, and no run of it ever ends,
// so claiming guaranteed run-finished delivery would be false.
const allCaps = workflow.CapDurable | workflow.CapDelayedStart | workflow.CapWaitForEvent |
	workflow.CapDurableSleep | workflow.CapConcurrencyKey | workflow.CapRateLimit |
	workflow.CapIdempotencyKey | workflow.CapCron | workflow.CapCancel |
	workflow.CapRetryJitter | workflow.CapRetryDelay | workflow.CapCronZone |
	workflow.CapMisfireSkip | workflow.CapMisfireFireOnce | workflow.CapMisfireFireAll

// Recorder is a workflow.Client that records every successful Enqueue so
// an app test can assert what its code enqueued.
//
// It has two modes. NewRecorder(inner) wraps a real client: every call
// goes to inner and successful enqueues are recorded on the way back.
// NewRecorder(nil) is a fake, deprecated in favour of NewFake: Register
// validates and stores the definition, Start succeeds once, Enqueue
// records the job and returns a synthetic RunRef, Status reports
// StatusQueued for a recorded run, and nothing ever runs. The fake claims every capability, because a test
// double that refused fields would test the double, not the app.
//
// A Recorder is safe for concurrent use. Jobs are copied on record, so a
// caller reusing its Job or Labels map cannot change what was recorded.
type Recorder struct {
	inner workflow.Client

	// Forward implements the optional client interfaces (run query,
	// pause, schedule listing, drain and the rest) by forwarding to inner
	// and refusing in the fake mode, so wrapping a client never hides one.
	workflow.Forward

	mu        sync.Mutex
	jobs      []workflow.Job
	refs      map[workflow.RunRef]bool
	wfs       map[string]workflow.Workflow // every definition registered successfully
	retries   []workflow.RunRef            // successful Retry calls, in order
	deletes   []workflow.RunRef            // successful Delete calls, in order
	started   bool
	schedules int
}

// NewRecorder returns a Recorder wrapping inner, or a pure fake when inner
// is nil.
//
// NewRecorder(nil) is discouraged and kept only for compatibility; use
// NewFake for a client that runs nothing, since it refuses what a real
// driver refuses instead of claiming every capability. Wrapping a real
// client with NewRecorder(inner) is fully supported.
func NewRecorder(inner workflow.Client) *Recorder {
	return &Recorder{inner: inner, Forward: workflow.Forward{Inner: inner}, refs: map[workflow.RunRef]bool{}, wfs: map[string]workflow.Workflow{}}
}

// Register registers wf with the inner client; the fake validates it and
// refuses a Register after Start, as a real client does.
// Either way a registered definition is kept for Workflow and
// AssertChained.
func (r *Recorder) Register(wf workflow.Workflow) error {
	if r.inner != nil {
		if err := r.inner.Register(wf); err != nil {
			return err
		}
		r.mu.Lock()
		r.wfs[wf.Name] = wf
		r.mu.Unlock()
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return workflow.ErrAlreadyStarted
	}
	if err := workflow.Validate(wf); err != nil {
		return err
	}
	r.wfs[wf.Name] = wf
	return nil
}

// Start starts the inner client; the fake succeeds once and returns
// ErrAlreadyStarted after that.
func (r *Recorder) Start(ctx context.Context) error {
	if r.inner != nil {
		return r.inner.Start(ctx)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return workflow.ErrAlreadyStarted
	}
	r.started = true
	return nil
}

// Enqueue enqueues through the inner client and records job when that
// succeeds. The fake records every job and never runs it. A refused job is
// not recorded: it was not enqueued.
func (r *Recorder) Enqueue(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
	if r.inner != nil {
		ref, err := r.inner.Enqueue(ctx, job)
		if err == nil {
			r.mu.Lock()
			r.recordLocked(job, ref)
			r.mu.Unlock()
		}
		return ref, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	ref := workflow.RunRef{RunID: workflow.RunID("recorded-" + strconv.Itoa(len(r.jobs)+1))}
	r.recordLocked(job, ref)
	return ref, nil
}

// recordLocked stores a copy of job; r.mu must be held.
func (r *Recorder) recordLocked(job workflow.Job, ref workflow.RunRef) {
	job.Payload = append([]byte(nil), job.Payload...)
	if job.Labels != nil {
		labels := make(map[string]string, len(job.Labels))
		maps.Copy(labels, job.Labels)
		job.Labels = labels
	}
	if job.Retry != nil {
		p := *job.Retry
		job.Retry = &p
	}
	r.jobs = append(r.jobs, job)
	r.refs[ref] = true
}

// Cancel cancels through the inner client; the fake accepts any recorded
// run and returns ErrNotFound for others.
func (r *Recorder) Cancel(ctx context.Context, ref workflow.RunRef) error {
	if r.inner != nil {
		return r.inner.Cancel(ctx, ref)
	}
	if !r.known(ref) {
		return workflow.ErrNotFound
	}
	return nil
}

// Status asks the inner client; the fake reports StatusQueued for a
// recorded run, since nothing ever runs it, and ErrNotFound otherwise.
func (r *Recorder) Status(ctx context.Context, ref workflow.RunRef) (workflow.Status, error) {
	if r.inner != nil {
		return r.inner.Status(ctx, ref)
	}
	if !r.known(ref) {
		return "", workflow.ErrNotFound
	}
	return workflow.StatusQueued, nil
}

func (r *Recorder) known(ref workflow.RunRef) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.refs[ref]
}

// Signal signals through the inner client; the fake drops it, as a real
// client drops a signal nobody waits for.
func (r *Recorder) Signal(ctx context.Context, ev workflow.Event) error {
	if r.inner != nil {
		return r.inner.Signal(ctx, ev)
	}
	return nil
}

// Schedule schedules through the inner client; the fake validates s and
// returns a synthetic ScheduleID.
func (r *Recorder) Schedule(ctx context.Context, s workflow.Schedule) (workflow.ScheduleID, error) {
	if r.inner != nil {
		return r.inner.Schedule(ctx, s)
	}
	if err := workflow.ValidateSchedule(s); err != nil {
		return "", err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.schedules++
	return workflow.ScheduleID("recorded-schedule-" + strconv.Itoa(r.schedules)), nil
}

// Unschedule unschedules through the inner client; the fake accepts any id.
func (r *Recorder) Unschedule(ctx context.Context, id workflow.ScheduleID) error {
	if r.inner != nil {
		return r.inner.Unschedule(ctx, id)
	}
	return nil
}

// Capabilities is the inner client's; the fake claims every capability.
func (r *Recorder) Capabilities() workflow.Caps {
	if r.inner != nil {
		return r.inner.Capabilities()
	}
	return allCaps
}

// OnRunFinished forwards fn to the inner client when it implements
// workflow.RunNotifier, so wrapping a client never hides its run-finished
// events. Recorder always has this method, so a type assertion on it
// always succeeds; the fake, or a Recorder over a client without
// RunNotifier, answers with an UnsupportedError instead of silently
// registering an observer that would never be called.
func (r *Recorder) OnRunFinished(fn workflow.RunFinishedFunc) error {
	if rn, ok := r.inner.(workflow.RunNotifier); ok {
		return rn.OnRunFinished(fn)
	}
	return workflow.Unsupported("OnRunFinished", "the client under the Recorder does not implement workflow.RunNotifier")
}

// Jobs returns a copy of every recorded job, in enqueue order.
func (r *Recorder) Jobs() []workflow.Job {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]workflow.Job(nil), r.jobs...)
}

// jobsOf returns the recorded jobs of workflow wf.
func (r *Recorder) jobsOf(wf string) []workflow.Job {
	var out []workflow.Job
	for _, j := range r.Jobs() {
		if j.Workflow == wf {
			out = append(out, j)
		}
	}
	return out
}

// AssertEnqueued fails t unless workflow wf was enqueued exactly n times.
func (r *Recorder) AssertEnqueued(t testing.TB, wf string, n int) {
	t.Helper()
	if got := len(r.jobsOf(wf)); got != n {
		t.Errorf("workflow %q was enqueued %d times, want %d", wf, got, n)
	}
}

// AssertNotEnqueued fails t if workflow wf was enqueued at all.
func (r *Recorder) AssertNotEnqueued(t testing.TB, wf string) {
	t.Helper()
	if got := len(r.jobsOf(wf)); got != 0 {
		t.Errorf("workflow %q was enqueued %d times, want none", wf, got)
	}
}

// AssertEnqueuedWith fails t unless at least one recorded job of workflow
// wf satisfies match.
func (r *Recorder) AssertEnqueuedWith(t testing.TB, wf string, match func(workflow.Job) bool) {
	t.Helper()
	jobs := r.jobsOf(wf)
	if slices.ContainsFunc(jobs, match) {
		return
	}
	t.Errorf("no job of workflow %q matched (%d enqueued)", wf, len(jobs))
}

// AssertNothingEnqueued fails t if any job was enqueued.
func (r *Recorder) AssertNothingEnqueued(t testing.TB) {
	t.Helper()
	if jobs := r.Jobs(); len(jobs) != 0 {
		t.Errorf("%d jobs were enqueued, want none (first: %q)", len(jobs), jobs[0].Workflow)
	}
}

// Workflow returns the registered definition of name and whether there
// is one.
func (r *Recorder) Workflow(name string) (workflow.Workflow, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	wf, ok := r.wfs[name]
	return wf, ok
}

// Batches returns the batches whose jobs were enqueued through the
// Recorder (see FakeBatch), as Fake.Batches does.
func (r *Recorder) Batches() []FakeBatch { return batchesOf(r.Jobs()) }

// AssertBatched fails t unless a batch matching every m was enqueued.
func (r *Recorder) AssertBatched(t testing.TB, m ...BatchMatcher) {
	t.Helper()
	assertBatched(t, r.Batches(), m)
}

// AssertBatchCount fails t unless exactly n batches were enqueued.
func (r *Recorder) AssertBatchCount(t testing.TB, n int) {
	t.Helper()
	if bs := r.Batches(); len(bs) != n {
		t.Errorf("%d batches were dispatched, want %d; %s", len(bs), n, listBatches(bs))
	}
}

// AssertNothingBatched fails t if any batch was enqueued.
func (r *Recorder) AssertNothingBatched(t testing.TB) {
	t.Helper()
	r.AssertBatchCount(t, 0)
}

// AssertChained fails t unless workflow wf is registered as exactly the
// chain steps and was enqueued at least once, as Fake.AssertChained does.
func (r *Recorder) AssertChained(t testing.TB, wf string, steps ...string) {
	t.Helper()
	def, ok := r.Workflow(wf)
	if !ok {
		t.Errorf("workflow %q is not registered", wf)
		return
	}
	if why := checkChain(def, steps); why != "" {
		t.Errorf("workflow %q is not the chain %v: %s", wf, steps, why)
		return
	}
	if len(r.jobsOf(wf)) == 0 {
		t.Errorf("chain %q was not enqueued", wf)
	}
}

// Retry retries through the inner client's workflow.RunRetrier and
// records ref when that succeeds; the fake, or an inner client without
// it, refuses with an UnsupportedError.
func (r *Recorder) Retry(ctx context.Context, ref workflow.RunRef) (workflow.RunRef, error) {
	got, err := r.Forward.Retry(ctx, ref)
	if err == nil {
		r.mu.Lock()
		r.retries = append(r.retries, ref)
		r.mu.Unlock()
	}
	return got, err
}

// Delete deletes through the inner client's workflow.RunDeleter and
// records ref when that succeeds; the fake, or an inner client without
// it, refuses with an UnsupportedError.
func (r *Recorder) Delete(ctx context.Context, ref workflow.RunRef) error {
	err := r.Forward.Delete(ctx, ref)
	if err == nil {
		r.mu.Lock()
		r.deletes = append(r.deletes, ref)
		r.mu.Unlock()
	}
	return err
}

// Retries returns every ref passed to a successful Retry, in order.
func (r *Recorder) Retries() []workflow.RunRef {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.retries)
}

// Deletes returns every ref passed to a successful Delete, in order.
func (r *Recorder) Deletes() []workflow.RunRef {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.deletes)
}

// AssertRetried fails t unless ref was retried through the Recorder.
func (r *Recorder) AssertRetried(t testing.TB, ref workflow.RunRef) {
	t.Helper()
	assertRef(t, "retried", ref, r.Retries())
}

// AssertDeleted fails t unless ref was deleted through the Recorder.
func (r *Recorder) AssertDeleted(t testing.TB, ref workflow.RunRef) {
	t.Helper()
	assertRef(t, "deleted", ref, r.Deletes())
}

// AssertPaused fails t unless the inner client reports target paused
// (its workflow.Pauser.Paused), so the assertion reads the real state.
func (r *Recorder) AssertPaused(t testing.TB, target workflow.PauseTarget) {
	t.Helper()
	r.assertPaused(t, target, true)
}

// AssertNotPaused fails t if the inner client reports target paused.
func (r *Recorder) AssertNotPaused(t testing.TB, target workflow.PauseTarget) {
	t.Helper()
	r.assertPaused(t, target, false)
}

func (r *Recorder) assertPaused(t testing.TB, target workflow.PauseTarget, want bool) {
	t.Helper()
	ps, err := r.Paused(context.Background())
	if err != nil {
		t.Errorf("list pauses: %v", err)
		return
	}
	assertPause(t, target, ps, want)
}

// AssertEnqueuedOn fails t unless a job of workflow wf matching every m
// was enqueued to run on queue (workflow.EffectiveQueue), as
// Fake.AssertEnqueuedOn does.
func (r *Recorder) AssertEnqueuedOn(t testing.TB, queue, wf string, m ...JobMatcher) {
	t.Helper()
	def, _ := r.Workflow(wf)
	var jobs []workflow.Job
	for _, j := range r.jobsOf(wf) {
		if matchAll(j, m) {
			jobs = append(jobs, j)
		}
	}
	assertOn(t, queue, wf, def, jobs, m, fmt.Sprintf("%d jobs of %q recorded", len(r.jobsOf(wf)), wf))
}

// Compile-time proof that Recorder is a Client and forwards RunNotifier.
var (
	_ workflow.Client      = (*Recorder)(nil)
	_ workflow.RunNotifier = (*Recorder)(nil)
)
