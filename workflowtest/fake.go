package workflowtest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/cron"
)

// fakeDefaultCaps is what the memory driver claims minus fakeUnserved:
// the driver apps test against, so a test on a default Fake accepts what
// the memory driver accepts. It includes the declarative fields the Fake
// only validates (queues, per-job retry, Discard, schedule conditions)
// and the calls it records (RunRetrier, RunDeleter, Pauser).
const fakeDefaultCaps = workflow.CapDelayedStart | workflow.CapWaitForEvent | workflow.CapDurableSleep |
	workflow.CapConcurrencyKey | workflow.CapRateLimit | workflow.CapIdempotencyKey |
	workflow.CapCron | workflow.CapCancel |
	workflow.CapRetryJitter | workflow.CapRetryDelay | workflow.CapCronZone |
	workflow.CapMisfireSkip | workflow.CapMisfireFireOnce | workflow.CapMisfireFireAll |
	workflow.CapQueue | workflow.CapQueueOverride | workflow.CapJobRetry |
	workflow.CapRetryUntil | workflow.CapRetryMaxErrors | workflow.CapDiscard |
	workflow.CapRetryRun | workflow.CapDeleteRun | workflow.CapPauseWorkflow | workflow.CapPauseQueue |
	workflow.CapScheduleWhen | workflow.CapScheduleWindow |
	workflow.CapRunStates | workflow.CapChain | workflow.CapClosure |
	workflow.CapOverlapSkip | workflow.CapScheduleTrigger | workflow.CapForceRun |
	workflow.CapTenant | workflow.CapHeartbeat | workflow.CapRunTimeout | workflow.CapKeyFIFO |
	workflow.CapCronSeconds | workflow.CapScheduleActive | workflow.CapScheduleKeep

// fakeUnserved are the capabilities the Fake never claims, whatever
// FakeCaps says: it reports no run ends (no RunNotifier), and it answers
// the run query (with its label prefix filter), bulk cancel, schedule pause, schedule listing, tick and
// drain and worker-scaling interfaces only by forwarding to a pass-through
// client, or refusing without one, so claiming them would be false. It
// also keeps no schedule history or error history (nothing fires or
// fails), cannot be stopped mid-step, and has no enqueue-only mode. Nor
// does it run steps, so it cannot snooze one, name a node, keep a result,
// list due runs or stamp a tick time.
const fakeUnserved = workflow.CapRunFinished | workflow.CapRunQuery | workflow.CapCancelAll |
	workflow.CapSchedulePause | workflow.CapScheduleList | workflow.CapTickEvents | workflow.CapDrain |
	workflow.CapWorkerScale | workflow.CapLabelPrefix |
	workflow.CapScheduleHistory | workflow.CapErrorHistory | workflow.CapShutdownRequeue | workflow.CapEnqueueOnly |
	workflow.CapSnooze | workflow.CapNodeIdentity | workflow.CapDueQuery | workflow.CapResult | workflow.CapTickTime |
	workflow.CapQueueLimitGlobal | workflow.CapDrainReport

// FakeOption configures a Fake.
type FakeOption func(*Fake)

// FakeCaps sets the capabilities the Fake claims and enforces. Pass the
// production driver's set (or use FakeLike) so a test cannot pass on the
// Fake with a field the real driver would refuse. CapRunFinished is
// dropped: the Fake reports no run ends; so are the run query (and its
// label prefix filter), bulk cancel, schedule pause, schedule listing,
// tick and drain capabilities, which the Fake does not serve itself.
func FakeCaps(c workflow.Caps) FakeOption {
	return func(f *Fake) { f.caps = c &^ fakeUnserved }
}

// FakeLike copies the capabilities of c, typically the production client
// built (but not started) with test configuration. It reads only
// c.Capabilities.
func FakeLike(c workflow.Client) FakeOption { return FakeCaps(c.Capabilities()) }

// FakeKeyExpr makes the Fake accept expression keys (workflow.KeyExpr), as
// an engine such as Hatchet does. By default it refuses them at Start, as
// the in-process memory driver does.
func FakeKeyExpr() FakeOption { return func(f *Fake) { f.keyExpr = true } }

// FakeCronParser makes the Fake check cron expressions with p instead of
// cron.Parse, matching a production driver built with a custom parser
// (memory.WithCronParser). Nil is ignored.
func FakeCronParser(p cron.Parser) FakeOption {
	return func(f *Fake) {
		if p != nil {
			f.parse = p
		}
	}
}

// FakeExcept passes the jobs, schedules and registrations of the named
// workflows through to inner, a real client, and fakes every other
// workflow. FakeOnly is the
// inverse. Passed-through work is not recorded: inner runs it, and the
// Fake's assertions cover only what it faked. Start starts inner too,
// Status and Cancel of a ref the Fake did not issue go to inner, and
// Signal is recorded and also delivered to inner. A nil inner is ignored.
func FakeExcept(inner workflow.Client, workflows ...string) FakeOption {
	return fakePass(inner, workflows, true)
}

// FakeOnly fakes only the named workflows and passes every other one
// through to inner; see FakeExcept.
func FakeOnly(inner workflow.Client, workflows ...string) FakeOption {
	return fakePass(inner, workflows, false)
}

func fakePass(inner workflow.Client, workflows []string, listedPass bool) FakeOption {
	return func(f *Fake) {
		if inner == nil {
			return
		}
		set := map[string]bool{}
		for _, w := range workflows {
			set[w] = true
		}
		f.inner = inner
		f.Forward = workflow.Forward{Inner: inner}
		f.pass = func(wf string) bool { return set[wf] == listedPass }
	}
}

// Fake is a workflow.Client that records what an app asked for and runs
// nothing. An app
// test hands it to the code under test, then asserts on what was
// enqueued, scheduled, signalled and cancelled.
//
// It refuses what a real driver refuses, so a test cannot pass on the Fake
// while production fails: Register validates (Validate, cron expressions)
// and refuses after Start; Start checks every workflow against the claimed
// capabilities (FakeCaps, FakeLike) and refuses expression keys unless
// FakeKeyExpr; Enqueue, Signal and Schedule before Start return
// ErrNotStarted; an unregistered workflow is ErrNotFound; a Job or
// Schedule needing a missing capability is an UnsupportedError; a workflow
// with IdempotencyRequired refuses a job without a key.
//
// Recorded runs never finish: Status reports StatusScheduled for a RunAt
// job and StatusQueued otherwise until Cancel or Finish moves it. An
// idempotency key held by an unfinished run dedups as on a real driver:
// the second Enqueue returns the first run's RunRef and records nothing.
//
// Recorder is the other test double in this package: it wraps a real
// client and records its enqueues. Its standalone mode, NewRecorder(nil),
// fakes a client that claims every capability and is deprecated in favour
// of Fake.
//
// Batches dispatched by a batch.Manager over the Fake are recorded as
// their labelled jobs: Batches, AssertBatched, AssertBatchCount and
// AssertNothingBatched read them.
// AssertChained checks a registered workflow's step graph. FakeExcept and
// FakeOnly pass chosen workflows through to a real client.
//
// A Fake is safe for concurrent use. Jobs are copied when recorded.
type Fake struct {
	caps    workflow.Caps
	keyExpr bool
	parse   cron.Parser

	// Forward implements the optional client interfaces (run query,
	// pause, drain and the rest) by forwarding to the pass-through client
	// and refusing without one; the Fake claims none of their
	// capabilities. It does not provide ScheduleLister, whose Schedules
	// method the Fake's own Schedules (the recorded calls) shadows.
	workflow.Forward

	inner workflow.Client              // pass-through target; nil when none
	pass  func(wf string) bool         // reports a workflow inner runs; nil when none
	pwfs  map[string]workflow.Workflow // passed-through registrations, for Workflow

	mu       sync.Mutex
	wfs      map[string]workflow.Workflow
	started  bool
	runs     []*fakeRun
	byRef    map[workflow.RunRef]*fakeRun
	idem     map[[2]string]*fakeRun
	scheds   []FakeSchedule
	signals  []workflow.Event
	cancels  []workflow.RunRef
	retries  []workflow.RunRef             // successful Retry calls, in order
	deletes  []workflow.RunRef             // successful Delete calls, in order
	triggers []workflow.ScheduleID         // successful TriggerSchedule calls, in order
	forced   []FakeForce                   // successful ForceComplete and ForceFail calls, in order
	paused   map[workflow.PauseTarget]bool // pauses in force
	unscheds []workflow.ScheduleID
	closures []workflow.ClosureJob // EnqueueClosure calls, in order
}

// fakeRun is one recorded, never-executed run.
type fakeRun struct {
	job    workflow.Job
	ref    workflow.RunRef
	status workflow.Status
	key    [2]string // idempotency entry it holds; zero when none
}

// FakeSchedule is one recorded Schedule call and whether it was removed.
type FakeSchedule struct {
	ID          workflow.ScheduleID
	Schedule    workflow.Schedule
	Unscheduled bool

	// LastRun is the run the last TriggerSchedule of it enqueued; the
	// Fake fires no cron ticks, so only triggers set it.
	LastRun workflow.RunRef
}

var _ workflow.Client = (*Fake)(nil)

// NewFake returns an unstarted Fake claiming the memory driver's
// capabilities unless FakeCaps or FakeLike says otherwise.
func NewFake(opts ...FakeOption) *Fake {
	f := &Fake{
		caps:   fakeDefaultCaps,
		parse:  cron.Parse,
		wfs:    map[string]workflow.Workflow{},
		byRef:  map[workflow.RunRef]*fakeRun{},
		idem:   map[[2]string]*fakeRun{},
		pwfs:   map[string]workflow.Workflow{},
		paused: map[workflow.PauseTarget]bool{},
	}
	for _, o := range opts {
		o(f)
	}
	return f
}

// parseCron runs the configured parser and fails closed, as the memory
// driver does: a parser that returns neither a schedule nor an error is
// refused.
func (f *Fake) parseCron(expr string) (cron.Schedule, error) { return f.parseCronFor(expr, false) }

// parseCronFor parses with cron.ParseSeconds for a seconds schedule
// (Schedule.Seconds, Workflow.CronSeconds), as the drivers do.
func (f *Fake) parseCronFor(expr string, seconds bool) (cron.Schedule, error) {
	parse := f.parse
	if seconds {
		parse = cron.ParseSeconds
	}
	s, err := parse(expr)
	if err == nil && s == nil {
		err = fmt.Errorf("cron parser returned no schedule for %q", expr)
	}
	return s, err
}

// Capabilities is the set the Fake was built with.
func (f *Fake) Capabilities() workflow.Caps { return f.caps }

// passes reports whether wf goes to the pass-through client.
func (f *Fake) passes(wf string) bool { return f.pass != nil && f.pass(wf) }

// Register validates wf, including its cron expressions, and stores it. A
// passed-through workflow (FakeExcept, FakeOnly) is registered with the
// real client instead.
func (f *Fake) Register(wf workflow.Workflow) error {
	if f.passes(wf.Name) {
		if err := f.inner.Register(wf); err != nil {
			return err
		}
		f.mu.Lock()
		f.pwfs[wf.Name] = wf
		f.mu.Unlock()
		return nil
	}
	if err := workflow.Validate(wf); err != nil {
		return err
	}
	for i, expr := range wf.Cron {
		if _, err := f.parseCronFor(expr, wf.CronSeconds); err != nil {
			return fmt.Errorf("%w: workflow %q: Cron[%d]: %w", workflow.ErrInvalid, wf.Name, i, err)
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.started {
		return workflow.ErrAlreadyStarted
	}
	f.wfs[wf.Name] = wf
	return nil
}

// Start checks every workflow against the capabilities and, unless
// FakeKeyExpr, refuses expression keys. It starts nothing of its own; a
// pass-through client (FakeExcept, FakeOnly) is started with ctx.
func (f *Fake) Start(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.started {
		return workflow.ErrAlreadyStarted
	}
	for _, wf := range f.wfs {
		if err := workflow.CheckCaps(f.caps, wf); err != nil {
			return err
		}
		if !f.keyExpr {
			if err := refuseKeyExpr(wf); err != nil {
				return err
			}
		}
	}
	if f.inner != nil {
		if err := f.inner.Start(ctx); err != nil {
			return err
		}
	}
	f.started = true
	return nil
}

// refuseKeyExpr names the first expression key in wf.
func refuseKeyExpr(wf workflow.Workflow) error {
	reason := "the Fake was built without FakeKeyExpr; workflow " + wf.Name
	if wf.Concurrency != nil && wf.Concurrency.Key.Expr != "" {
		return workflow.Unsupported("Workflow.Concurrency.Key.Expr", reason)
	}
	for i, r := range wf.RateLimits {
		if r.Key.Expr != "" {
			return workflow.Unsupported("Workflow.RateLimits["+strconv.Itoa(i)+"].Key.Expr", reason)
		}
	}
	for _, s := range wf.Steps {
		if s.Wait != nil && s.Wait.KeyFrom.Expr != "" {
			return workflow.Unsupported("WaitSpec.KeyFrom.Expr", reason+", step "+s.Name)
		}
	}
	return nil
}

// Enqueue records job and returns a synthetic RunRef: a ScheduledID for a
// RunAt job, a RunID otherwise. It refuses what a real driver refuses (see
// Fake).
func (f *Fake) Enqueue(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
	if err := ctx.Err(); err != nil {
		return workflow.RunRef{}, err
	}
	if f.passes(job.Workflow) {
		return f.inner.Enqueue(ctx, job)
	}
	if err := workflow.ValidateJob(job); err != nil {
		return workflow.RunRef{}, err
	}
	if err := workflow.CheckJobLabels(ctx, job); err != nil {
		return workflow.RunRef{}, err
	}
	if err := workflow.CheckJob(f.caps, job); err != nil {
		return workflow.RunRef{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.started {
		return workflow.RunRef{}, workflow.ErrNotStarted
	}
	wf, ok := f.wfs[job.Workflow]
	if !ok {
		return workflow.RunRef{}, fmt.Errorf("%w: workflow %q is not registered", workflow.ErrNotFound, job.Workflow)
	}
	if err := workflow.CheckJobQueue(f.caps, wf, job); err != nil {
		return workflow.RunRef{}, err
	}
	key := job.IdempotencyKey
	switch wf.Idempotency {
	case workflow.IdempotencyOff:
		key = ""
	case workflow.IdempotencyRequired:
		if key == "" {
			return workflow.RunRef{}, fmt.Errorf("%w: workflow %q requires an IdempotencyKey", workflow.ErrInvalid, wf.Name)
		}
	}
	r := &fakeRun{job: copyJob(job), status: workflow.StatusQueued}
	if key != "" {
		r.key = [2]string{wf.Name, key}
		if holder, ok := f.idem[r.key]; ok {
			workflow.MarkDuplicate(ctx)
			return holder.ref, nil
		}
		f.idem[r.key] = r
	}
	workflow.MarkInserted(ctx)
	n := strconv.Itoa(len(f.runs) + 1)
	if job.RunAt.IsZero() {
		r.ref.RunID = workflow.RunID("fake-run-" + n)
	} else {
		r.ref.ScheduledID = workflow.ScheduledID("fake-delayed-" + n)
		r.status = workflow.StatusScheduled
	}
	f.runs = append(f.runs, r)
	f.byRef[r.ref] = r
	return r.ref, nil
}

func copyJob(j workflow.Job) workflow.Job { return workflow.CloneJob(j) }

// Status reports a recorded run's status; a ref the Fake did not issue
// goes to the pass-through client, or is ErrNotFound without one.
func (f *Fake) Status(ctx context.Context, ref workflow.RunRef) (workflow.Status, error) {
	f.mu.Lock()
	r, ok := f.byRef[ref]
	var st workflow.Status
	if ok {
		st = r.status
	}
	f.mu.Unlock()
	switch {
	case ok:
		return st, nil
	case f.inner != nil:
		return f.inner.Status(ctx, ref)
	}
	return "", fmt.Errorf("%w: run %+v", workflow.ErrNotFound, ref)
}

// Cancel records the call and marks a recorded run cancelled. It needs
// CapCancel; cancelling a finished run is a no-op; a ref the Fake did not
// issue goes to the pass-through client, or is ErrNotFound without one.
func (f *Fake) Cancel(ctx context.Context, ref workflow.RunRef) error {
	if !f.caps.Has(workflow.CapCancel) {
		return &workflow.UnsupportedError{Field: "Cancel", Missing: workflow.CapCancel}
	}
	f.mu.Lock()
	r, ok := f.byRef[ref]
	if !ok {
		f.mu.Unlock()
		if f.inner != nil {
			return f.inner.Cancel(ctx, ref)
		}
		return fmt.Errorf("%w: run %+v", workflow.ErrNotFound, ref)
	}
	defer f.mu.Unlock()
	f.cancels = append(f.cancels, ref)
	f.finishLocked(r, workflow.StatusCancelled)
	return nil
}

// Finish moves a recorded run to a terminal status, releasing its
// idempotency key, so a test can model a run that completed. It returns
// ErrNotFound for an unknown ref and ErrInvalid for a status that is not
// terminal; finishing a finished run is a no-op.
func (f *Fake) Finish(ref workflow.RunRef, st workflow.Status) error {
	if !st.IsTerminal() {
		return fmt.Errorf("%w: Finish needs a terminal status, got %q", workflow.ErrInvalid, st)
	}
	if st == workflow.StatusDiscarded && !f.caps.Has(workflow.CapRunStates) {
		return &workflow.UnsupportedError{Field: "Finish", Missing: workflow.CapRunStates,
			Reason: "a driver without it reports a discarded run as StatusCancelled or StatusFailed (workflow.EndStatus)"}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.byRef[ref]
	if !ok {
		return fmt.Errorf("%w: run %+v", workflow.ErrNotFound, ref)
	}
	f.finishLocked(r, st)
	return nil
}

func (f *Fake) finishLocked(r *fakeRun, st workflow.Status) {
	if r.status.IsTerminal() {
		return
	}
	r.status = st
	if r.key != ([2]string{}) && f.idem[r.key] == r {
		delete(f.idem, r.key)
	}
}

// Signal records ev. Nothing waits on the Fake, so nothing resumes; with a
// pass-through client, ev is also delivered to it, where passed-through
// runs may wait.
func (f *Fake) Signal(ctx context.Context, ev workflow.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	if !f.started {
		f.mu.Unlock()
		return workflow.ErrNotStarted
	}
	rec := ev
	rec.Payload = bytes.Clone(ev.Payload)
	f.signals = append(f.signals, rec)
	f.mu.Unlock()
	if f.inner != nil {
		return f.inner.Signal(ctx, ev)
	}
	return nil
}

// Schedule validates s (including its cron expression), needs CapCron
// plus what RequiresSchedule names (a zone, a misfire policy) and a
// registered workflow, and records it under a synthetic ID.
func (f *Fake) Schedule(ctx context.Context, s workflow.Schedule) (workflow.ScheduleID, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if f.passes(s.Workflow) {
		return f.inner.Schedule(ctx, s)
	}
	if !f.caps.Has(workflow.CapCron) {
		return "", &workflow.UnsupportedError{Field: "Schedule", Missing: workflow.CapCron}
	}
	if err := workflow.ValidateSchedule(s); err != nil {
		return "", err
	}
	if err := workflow.CheckScheduleCaps(f.caps, s); err != nil {
		return "", err
	}
	if _, err := f.parseCronFor(s.Cron, s.Seconds); err != nil {
		return "", fmt.Errorf("%w: schedule for %q: %w", workflow.ErrInvalid, s.Workflow, err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.started {
		return "", workflow.ErrNotStarted
	}
	if _, ok := f.wfs[s.Workflow]; !ok {
		return "", fmt.Errorf("%w: workflow %q is not registered", workflow.ErrNotFound, s.Workflow)
	}
	s.Input = bytes.Clone(s.Input)
	s.Labels = maps.Clone(s.Labels)
	id := workflow.ScheduleID("fake-schedule-" + strconv.Itoa(len(f.scheds)+1))
	f.scheds = append(f.scheds, FakeSchedule{ID: id, Schedule: s})
	return id, nil
}

// Unschedule marks a recorded schedule removed. An empty id or a second
// call is a no-op; an id the Fake never issued goes to the pass-through
// client, or is ErrNotFound without one.
func (f *Fake) Unschedule(ctx context.Context, id workflow.ScheduleID) error {
	if id.IsZero() {
		return nil
	}
	f.mu.Lock()
	for i := range f.scheds {
		if f.scheds[i].ID == id {
			f.scheds[i].Unscheduled = true
			f.mu.Unlock()
			return nil
		}
	}
	f.mu.Unlock()
	if f.inner != nil {
		return f.inner.Unschedule(ctx, id)
	}
	return fmt.Errorf("%w: schedule %q", workflow.ErrNotFound, id)
}

// Workflow returns the registered definition of name, faked or passed
// through, and whether there is one.
func (f *Fake) Workflow(name string) (workflow.Workflow, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if wf, ok := f.wfs[name]; ok {
		return wf, true
	}
	wf, ok := f.pwfs[name]
	return wf, ok
}

// Batches returns the batches dispatched through the Fake (see
// FakeBatch), in the order their first job was recorded.
func (f *Fake) Batches() []FakeBatch { return batchesOf(f.Jobs()) }

// AssertBatched fails t unless a dispatched batch matches every m.
func (f *Fake) AssertBatched(t testing.TB, m ...BatchMatcher) {
	t.Helper()
	assertBatched(t, f.Batches(), m)
}

// AssertBatchCount fails t unless exactly n batches were dispatched.
func (f *Fake) AssertBatchCount(t testing.TB, n int) {
	t.Helper()
	if bs := f.Batches(); len(bs) != n {
		t.Errorf("%d batches were dispatched, want %d; %s", len(bs), n, listBatches(bs))
	}
}

// AssertNothingBatched fails t if any batch was dispatched.
func (f *Fake) AssertNothingBatched(t testing.TB) {
	t.Helper()
	f.AssertBatchCount(t, 0)
}

// AssertChained fails t unless workflow wf is registered with exactly the
// steps named, in declaration order, each running after the one before it
// (the first after none), and at least one job of wf was enqueued. It is
// the chain assertion for a chain written as one workflow; a
// passed-through workflow's graph is checked, but its enqueues are not
// recorded, so it never passes.
func (f *Fake) AssertChained(t testing.TB, wf string, steps ...string) {
	t.Helper()
	def, ok := f.Workflow(wf)
	if !ok {
		t.Errorf("workflow %q is not registered", wf)
		return
	}
	if why := checkChain(def, steps); why != "" {
		t.Errorf("workflow %q is not the chain %v: %s", wf, steps, why)
		return
	}
	if len(f.Enqueued(wf)) == 0 {
		t.Errorf("chain %q was not enqueued; %s", wf, f.recorded())
	}
}

// Jobs returns a copy of every recorded job, in enqueue order. A deduped
// enqueue is not a job: it created no run.
func (f *Fake) Jobs() []workflow.Job {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]workflow.Job, len(f.runs))
	for i, r := range f.runs {
		out[i] = copyJob(r.job)
	}
	return out
}

// Enqueued returns the recorded jobs of workflow wf matching every m.
func (f *Fake) Enqueued(wf string, m ...JobMatcher) []workflow.Job {
	var out []workflow.Job
	for _, j := range f.Jobs() {
		if j.Workflow == wf && matchAll(j, m) {
			out = append(out, j)
		}
	}
	return out
}

// Schedules returns every recorded Schedule call, in order.
func (f *Fake) Schedules() []FakeSchedule {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]FakeSchedule(nil), f.scheds...)
}

// Signals returns every recorded Signal, in order.
func (f *Fake) Signals() []workflow.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]workflow.Event(nil), f.signals...)
}

// Cancels returns every ref passed to a successful Cancel, in order.
func (f *Fake) Cancels() []workflow.RunRef {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]workflow.RunRef(nil), f.cancels...)
}

// JobMatcher narrows an assertion to jobs with a property. Build it with
// HasPayload, HasPayloadJSON, HasLabel, HasKey, HasRunAt, HasPriority or
// Where; its description appears in failure messages.
type JobMatcher struct {
	desc  string
	match func(workflow.Job) bool
}

// String is the matcher's description.
func (m JobMatcher) String() string { return m.desc }

// Where matches jobs for which fn is true; desc names the condition in
// failure messages.
func Where(desc string, fn func(workflow.Job) bool) JobMatcher {
	return JobMatcher{desc: desc, match: fn}
}

// HasPayload matches a payload byte for byte.
func HasPayload(p []byte) JobMatcher {
	return Where(fmt.Sprintf("payload %q", p), func(j workflow.Job) bool { return bytes.Equal(j.Payload, p) })
}

// HasPayloadJSON matches a JSON payload equal to v once both are decoded,
// so key order and spacing do not matter. A v that does not marshal
// matches nothing.
func HasPayloadJSON(v any) JobMatcher {
	want, err := json.Marshal(v)
	return Where("JSON payload "+string(want), func(j workflow.Job) bool {
		if err != nil {
			return false
		}
		var a, b any
		if json.Unmarshal(j.Payload, &a) != nil || json.Unmarshal(want, &b) != nil {
			return false
		}
		return reflect.DeepEqual(a, b)
	})
}

// HasLabel matches a job whose Labels[k] is v.
func HasLabel(k, v string) JobMatcher {
	return Where(fmt.Sprintf("label %s=%s", k, v), func(j workflow.Job) bool {
		got, ok := j.Labels[k]
		return ok && got == v
	})
}

// HasKey matches a job's IdempotencyKey.
func HasKey(key string) JobMatcher {
	return Where("idempotency key "+strconv.Quote(key), func(j workflow.Job) bool { return j.IdempotencyKey == key })
}

// HasRunAt matches a job delayed to exactly t.
func HasRunAt(t time.Time) JobMatcher {
	return Where("RunAt "+t.Format(time.RFC3339Nano), func(j workflow.Job) bool { return j.RunAt.Equal(t) })
}

// HasPriority matches a job's Priority.
func HasPriority(p int) JobMatcher {
	return Where("priority "+strconv.Itoa(p), func(j workflow.Job) bool { return j.Priority == p })
}

// HasChain matches jobs whose Job.Chain runs exactly the named workflows,
// in order, nested chains flattened as the driver would run them
// (workflow.NextInChain). HasChain() matches a job with no chain.
func HasChain(workflows ...string) JobMatcher {
	return Where(fmt.Sprintf("chain %v", workflows), func(j workflow.Job) bool {
		return slices.Equal(chainNames(j.Chain), workflows)
	})
}

// chainNames is the order the jobs of chain run in.
func chainNames(chain []workflow.Job) []string {
	var out []string
	for next, ok := workflow.NextInChain(chain); ok; next, ok = workflow.NextInChain(next.Chain) {
		out = append(out, next.Workflow)
	}
	return out
}

func matchAll(j workflow.Job, ms []JobMatcher) bool {
	for _, m := range ms {
		if !m.match(j) {
			return false
		}
	}
	return true
}

// AssertEnqueued fails t unless at least one job of workflow wf matching
// every m was enqueued.
func (f *Fake) AssertEnqueued(t testing.TB, wf string, m ...JobMatcher) {
	t.Helper()
	if len(f.Enqueued(wf, m...)) == 0 {
		t.Errorf("workflow %q%s was not enqueued; %s", wf, describe(m), f.recorded())
	}
}

// AssertEnqueuedTimes fails t unless exactly n jobs of workflow wf
// matching every m were enqueued.
func (f *Fake) AssertEnqueuedTimes(t testing.TB, wf string, n int, m ...JobMatcher) {
	t.Helper()
	if got := len(f.Enqueued(wf, m...)); got != n {
		t.Errorf("workflow %q%s was enqueued %d times, want %d; %s", wf, describe(m), got, n, f.recorded())
	}
}

// AssertNotEnqueued fails t if any job of workflow wf matching every m was
// enqueued.
func (f *Fake) AssertNotEnqueued(t testing.TB, wf string, m ...JobMatcher) {
	t.Helper()
	if got := len(f.Enqueued(wf, m...)); got != 0 {
		t.Errorf("workflow %q%s was enqueued %d times, want none; %s", wf, describe(m), got, f.recorded())
	}
}

// AssertNothingEnqueued fails t if any job was enqueued.
func (f *Fake) AssertNothingEnqueued(t testing.TB) {
	t.Helper()
	if len(f.Jobs()) != 0 {
		t.Errorf("jobs were enqueued, want none; %s", f.recorded())
	}
}

// AssertScheduled fails t unless a schedule of workflow wf is in place
// (recorded and not unscheduled).
func (f *Fake) AssertScheduled(t testing.TB, wf string) {
	t.Helper()
	var all []string
	for _, s := range f.Schedules() {
		if s.Schedule.Workflow == wf && !s.Unscheduled {
			return
		}
		all = append(all, fmt.Sprintf("%s %q (unscheduled: %t)", s.Schedule.Workflow, s.Schedule.Cron, s.Unscheduled))
	}
	t.Errorf("no schedule of workflow %q is in place; schedules: [%s]", wf, strings.Join(all, ", "))
}

// AssertSignalled fails t unless an event named name with key key was
// signalled.
func (f *Fake) AssertSignalled(t testing.TB, name, key string) {
	t.Helper()
	var all []string
	for _, ev := range f.Signals() {
		if ev.Name == name && ev.Key == key {
			return
		}
		all = append(all, ev.Name+"/"+ev.Key)
	}
	t.Errorf("event %q with key %q was not signalled; signals: [%s]", name, key, strings.Join(all, ", "))
}

// AssertCancelled fails t unless ref was cancelled.
func (f *Fake) AssertCancelled(t testing.TB, ref workflow.RunRef) {
	t.Helper()
	cancels := f.Cancels()
	for _, c := range cancels {
		if c == ref {
			return
		}
	}
	t.Errorf("run %+v was not cancelled; cancelled: %v", ref, cancels)
}

func describe(ms []JobMatcher) string {
	if len(ms) == 0 {
		return ""
	}
	parts := make([]string, len(ms))
	for i, m := range ms {
		parts[i] = m.desc
	}
	return " with " + strings.Join(parts, ", ")
}

// recordedLimit caps how many jobs a failure message lists.
const recordedLimit = 20

// recorded lists the recorded jobs for a failure message.
func (f *Fake) recorded() string {
	jobs := f.Jobs()
	if len(jobs) == 0 {
		return "nothing was enqueued"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%d enqueued:", len(jobs))
	for i, j := range jobs {
		if i == recordedLimit {
			fmt.Fprintf(&b, "\n  ... and %d more", len(jobs)-recordedLimit)
			break
		}
		fmt.Fprintf(&b, "\n  %s payload=%q", j.Workflow, j.Payload)
		if j.IdempotencyKey != "" {
			fmt.Fprintf(&b, " key=%q", j.IdempotencyKey)
		}
		if len(j.Labels) != 0 {
			fmt.Fprintf(&b, " labels=%v", j.Labels)
		}
		if !j.RunAt.IsZero() {
			fmt.Fprintf(&b, " runAt=%s", j.RunAt.Format(time.RFC3339))
		}
		if j.Priority != 0 {
			fmt.Fprintf(&b, " priority=%d", j.Priority)
		}
	}
	return b.String()
}
