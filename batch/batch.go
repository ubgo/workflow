// Package batch runs a group of jobs as one unit with Then, Catch and
// Finally callbacks, on any workflow.Client.
//
// It is built on the public Client only: each job is an ordinary Enqueue,
// tagged with two labels (LabelBatch and LabelIndex) so the batch can tell
// which of its jobs a finished run was.
//
// # How a batch learns that a job ended
//
// Through Sources (see Source), connected by Manager.Attach. Unless
// WithCompletionSource names them, Attach picks them by what the client can
// do:
//
//   - a client declaring workflow.CapRunFinished (the memory driver): the
//     run-finished hook alone (HookSource), because every run end is
//     guaranteed to arrive;
//   - a workflow.RunNotifier without that capability (the Hatchet driver,
//     whose delivery is best-effort): the hook for speed, plus a status
//     poller (PollSource, every DefaultPollInterval or WithStatusPoll) as
//     the safety net, so a lost event delays a batch by one poll instead of
//     hanging it;
//   - any other client: the poller alone, since Client.Status exists on
//     every driver.
//
// Attach therefore works on every driver; it refuses only a Source that
// cannot work, such as HookSource on a client that is not a RunNotifier or
// a poll interval that is not positive. Two Sources reporting the same job
// are safe: the first terminal status recorded wins.
//
// # Semantics
//
//   - Then runs once when every job succeeded and the batch was not
//     cancelled.
//   - Catch runs once, on the first failed job. Without AllowFailures that
//     failure also cancels the batch and the jobs still running; with it,
//     the other jobs keep running (Then still does not run, because not
//     every job succeeded).
//   - Finally runs once when every job has finished, whatever the outcome,
//     including after Cancel.
//   - A job cancelled on its own is not a failure: it fires no Catch, but it
//     stops Then.
//
// Each callback is an Enqueue of the named workflow with a JSON Progress as
// its payload and Batch.Labels as its labels (without the reserved batch
// labels), so a callback passes a client that requires a tenant label. The once-only decisions are made inside Store.Update, so a
// hook that fires twice for one run, or two hooks racing, still fire each
// callback at most once, and so does a job reported by both the hook and the
// poller.
//
// # Callbacks that fail to enqueue
//
// A decided callback is saved as pending (State.Pending) in the same
// Store.Update as the decision, then enqueued, then removed. One whose
// Enqueue fails stays pending and is retried once its claim ends (see
// WithCallbackRetry): on the status poller's next tick, on the next job
// end recorded for that batch, or when the app calls
// Manager.RetryCallbacks. When no Source is a poller (a client whose hook
// is guaranteed), Attach also starts a retry loop that calls
// RetryCallbacks every WithCallbackRetry interval, so a Finally that failed
// after the last job end is still retried; WithManualCallbackRetry turns
// the loop off for an app that calls RetryCallbacks itself. A claim stops
// two attempts from overlapping.
// When the client has workflow.CapIdempotencyKey every callback carries
// the key "batch:<id>:<kind>" (a Progress callback adds ":<job>", and one
// decided after RetryFailed adds ":r<round>"), so a retry after an enqueue
// that landed but was not recorded (a crash between the two) cannot double
// it; without that capability such a crash can enqueue a callback twice,
// so callbacks are delivered at least once.
//
// Before and Progress (Batch.Before, Batch.Progress) are callbacks too:
// Before is enqueued once when the batch is created, before its jobs
// start, and Progress once per job end that a run reports.
//
// # Changing a batch after Dispatch
//
//   - Manager.Add starts more jobs in a batch that is still running (a job
//     of the batch fanning out further work); Then, Catch and Finally wait
//     for them too.
//   - Manager.RetryFailed starts a finished batch's failed jobs again and
//     re-arms its callbacks; it needs WithRetryableJobs, which keeps every
//     job's spec in the State. A retried job's run carries LabelAttempt,
//     and the late end of an earlier attempt is ignored.
//   - Manager.Prune deletes old batch state from a Store that implements
//     Pruner, by age of finished, unfinished and cancelled batches
//     (PruneFilter).
//
// # Waiting for a batch inside a workflow
//
// Manager.AwaitStep is a step that dispatches a batch and completes when
// it finishes, so a workflow fans out and resumes after the batch
// (a batch inside a chain). EnqueueMany starts many jobs in one call
// without any batch tracking.
//
// # Native group runs
//
// When the client implements GroupRunner and no job has RunAt or an
// IdempotencyKey, Dispatch starts every job in one RunGroup call instead
// of one Enqueue each (see GroupRunner). A client that is a GroupSpawner
// with SpawnsGroups on starts such a batch as one coordinator run instead,
// whose children have no refs; that needs a LabelSource (see
// GroupSpawner).
//
// # A dispatch that dies part-way
//
// Dispatch holds a lease on its batch (WithDispatchLease) and renews it
// with every write. When the dispatching process dies, the jobs it had not
// started would otherwise never finish; ResumeDispatches, which the status
// poller, the callback retry loop and the Hatchet BatchSource call each
// tick, takes such a batch over once its lease has ended. It starts again
// only the jobs known never to have started (from a RunFinder on the
// client, or the per-job path's order) whose specs were kept, and records
// the others as cancelled; see ResumeDispatches.
package batch

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// The labels a batch puts on each of its jobs. RunFinished reads them back;
// a run without both is not a batch job and is ignored. They live under
// workflow.LabelPrefixBatch, which this package owns: Dispatch refuses a
// job or a Batch.Labels carrying any label under that prefix with
// workflow.ErrInvalid, rather than overwriting it.
const (
	LabelBatch = "workflow.batch.id"
	LabelIndex = "workflow.batch.index"

	// LabelAttempt is JobState.Attempt, put only on the run of a job that
	// Manager.RetryFailed started again (attempt 1 and up); a run without
	// it is attempt 0. A run end whose attempt is not the job's current
	// one is ignored.
	LabelAttempt = "workflow.batch.attempt"
)

// idBytes is the random length of a batch ID: 128 bits, so IDs made by
// different processes sharing one Store do not collide.
const idBytes = 16

// Batch is a group of jobs to run together.
type Batch struct {
	// Name is a human-readable name, shown in Progress.
	Name string

	// Jobs are enqueued in order. At least one is required.
	Jobs []workflow.Job

	// Then, Catch and Finally name registered workflows to enqueue when the
	// batch succeeds, on its first failure, and when it finishes. Empty
	// means none.
	Then, Catch, Finally string

	// Before names a workflow enqueued once when the batch is created,
	// before its jobs start. Progress names a workflow
	// enqueued once per job end that a run reports, whatever the status,
	// after that end is recorded (not only successes; the payload's counts
	// tell them apart). A job recorded as
	// cancelled without a run (a dispatch that failed, a resumed dispatch)
	// fires no Progress. Both get the same payload and labels as the
	// other callbacks, and both are retried like them when their enqueue
	// fails. Empty means none.
	Before, Progress string

	// AllowFailures keeps the other jobs running after a failure instead of
	// cancelling them; Catch still runs on the first failure either way.
	AllowFailures bool

	// Labels are copied onto every Then, Catch and Finally job, so a
	// callback carries what the batch was for (a tenant, a user) and passes
	// enqueue middleware that requires a label or routes on one. They are
	// saved in State, so a callback enqueued by another process or after a
	// restart carries them too. A label under workflow.LabelPrefixBatch is
	// refused by Dispatch, as on the jobs. They are not put on the batch's
	// own jobs; set those on each Job.
	Labels map[string]string

	// DiscardedAs is how a job that ended workflow.StatusDiscarded (a step
	// returned workflow.Discard, on a driver with CapRunStates) counts:
	// workflow.StatusFailed (the zero value's meaning: it fires Catch and,
	// without AllowFailures, cancels the rest), workflow.StatusCancelled
	// (stops Then, fires no Catch) or workflow.StatusSucceeded (as if it
	// had done its work). Any other value is refused by Dispatch. It is
	// saved in State, so every process decides the same way.
	DiscardedAs workflow.Status
}

// Progress is a snapshot of a batch; it is also the payload of every
// callback, as JSON.
type Progress struct {
	ID            ID     `json:"id"`
	Name          string `json:"name,omitempty"`
	Total         int    `json:"total"`
	Pending       int    `json:"pending"`
	Succeeded     int    `json:"succeeded"`
	Failed        int    `json:"failed"`
	CancelledJobs int    `json:"cancelledJobs"`
	// Discarded counts the jobs that ended StatusDiscarded; each is also
	// counted in the bucket Batch.DiscardedAs names.
	Discarded int  `json:"discarded,omitempty"`
	Cancelled bool `json:"cancelled"` // the batch itself was cancelled
	Finished  bool `json:"finished"`  // every job is terminal
}

// Processed is the number of jobs that have finished, in any status.
func (p Progress) Processed() int { return p.Total - p.Pending }

// Percent is Processed as a whole percentage of Total.
func (p Progress) Percent() int {
	if p.Total == 0 {
		return 0
	}
	return p.Processed() * 100 / p.Total
}

func progressOf(st State) Progress {
	p := Progress{ID: st.ID, Name: st.Name, Total: len(st.Jobs), Cancelled: st.Cancelled, Finished: st.Finished}
	for _, j := range st.Jobs {
		if j.Status == workflow.StatusDiscarded {
			p.Discarded++
		}
		switch st.counted(j.Status) {
		case workflow.StatusSucceeded:
			p.Succeeded++
		case workflow.StatusFailed:
			p.Failed++
		case workflow.StatusCancelled:
			p.CancelledJobs++
		default:
			p.Pending++
		}
	}
	return p
}

// Manager dispatches batches and tracks them. It holds no package-level
// state; several Managers may share one Client and one Store.
type Manager struct {
	client workflow.Client
	store  Store
	clk    clock.Clock
	log    *slog.Logger

	pollEvery     time.Duration // interval of the default PollSource
	callbackRetry time.Duration // claim length of a callback enqueue
	manualRetry   bool          // no retry loop; the app calls RetryCallbacks
	sources       []Source      // nil means defaultSources
	newID         func() (ID, error)
	dispatchLease time.Duration // how long a dispatch may go without writing before a resumer takes over
	resumable     bool          // keep job specs on the one-Enqueue-per-job path too
	retainSpecs   bool          // keep job specs after the dispatch, for RetryFailed
	byLabel       bool          // Attach attached a LabelSource
}

// Option configures a Manager.
type Option func(*Manager)

// WithClock sets the clock used for the CreatedAt, CancelledAt and
// FinishedAt times. The default is clock.Real.
func WithClock(c clock.Clock) Option { return func(m *Manager) { m.clk = c } }

// WithLogger sets where the Manager reports what the run-finished hook
// cannot return to anyone: a callback that failed to enqueue, a cancel that
// failed. The default discards, as the drivers do.
func WithLogger(l *slog.Logger) Option {
	return func(m *Manager) {
		if l != nil {
			m.log = l
		}
	}
}

// WithStatusPoll sets the interval of the status poller Attach adds by
// default (see the package doc); the default is DefaultPollInterval. It
// does not add a poller where the defaults would not; name PollSource in
// WithCompletionSource for that.
func WithStatusPoll(interval time.Duration) Option {
	return func(m *Manager) { m.pollEvery = interval }
}

// WithCompletionSource replaces the default Sources with srcs, for example
// a native engine feed, or HookSource plus PollSource on a client whose
// hook is guaranteed but which the app wants double-checked.
func WithCompletionSource(srcs ...Source) Option {
	return func(m *Manager) { m.sources = append(m.sources, srcs...) }
}

// WithIDFunc makes batch IDs with fn instead of 128 random bits as hex,
// for example a UUIDv7 or ULID so IDs sort by time or match the app's own.
// An ID must be unique across every process sharing the Store (Create
// refuses a repeat) and non-empty; Dispatch fails with fn's error, or
// workflow.ErrInvalid for an empty ID, and saves nothing. The ID appears in
// the LabelBatch label and the callbacks' idempotency keys, so keep it to
// characters the engine accepts in a label value. A nil fn keeps the
// default.
func WithIDFunc(fn func() (ID, error)) Option {
	return func(m *Manager) {
		if fn != nil {
			m.newID = fn
		}
	}
}

// New returns a Manager that enqueues on client and keeps state in store.
// Call Attach before client.Start so run ends reach the batch.
func New(client workflow.Client, store Store, opts ...Option) *Manager {
	m := &Manager{client: client, store: store, clk: clock.Real{}, log: slog.New(slog.DiscardHandler), pollEvery: DefaultPollInterval, callbackRetry: DefaultCallbackRetry, dispatchLease: DefaultDispatchLease, newID: newID}
	for _, o := range opts {
		o(m)
	}
	return m
}

// Dispatch saves the batch and enqueues its jobs in order, returning its
// ID. On a GroupRunner client it starts them in one RunGroup call instead
// when every job is eligible (see GroupRunner). When an Enqueue fails, the
// jobs not yet enqueued are recorded as cancelled, the batch is cancelled
// (which cancels the jobs already enqueued and, once they finish, runs
// Finally) and the error is returned together with the ID, so the caller
// can still inspect the batch.
//
// Dispatch holds a lease on the batch (WithDispatchLease) and renews it
// with every saved ref. If the process dies part-way, another process's
// Manager.ResumeDispatches finishes the dispatch once the lease has ended.
func (m *Manager) Dispatch(ctx context.Context, b Batch) (ID, error) {
	if err := checkBatch(b); err != nil {
		return "", err
	}
	id, err := m.newID()
	if err != nil {
		return "", fmt.Errorf("batch: generate id: %w", err)
	}
	return m.dispatch(ctx, b, id)
}

// checkBatch refuses a batch without jobs and app labels under the
// reserved prefix.
func checkBatch(b Batch) error {
	if len(b.Jobs) == 0 {
		return fmt.Errorf("%w: batch %q has no jobs", workflow.ErrInvalid, b.Name)
	}
	if err := refuseBatchLabels(b.Name, "Labels", b.Labels); err != nil {
		return err
	}
	switch b.DiscardedAs {
	case "", workflow.StatusFailed, workflow.StatusCancelled, workflow.StatusSucceeded:
	default:
		return fmt.Errorf("%w: batch %q: DiscardedAs %q; use StatusFailed, StatusCancelled or StatusSucceeded", workflow.ErrInvalid, b.Name, b.DiscardedAs)
	}
	return checkJobs(b.Name, "job ", b.Jobs)
}

// checkJobs refuses batch jobs with labels under the reserved prefix or a
// Job.Chain: a batch tracks one run per job, so a chain's later jobs
// would run untracked.
func checkJobs(batch, what string, jobs []workflow.Job) error {
	for i, job := range jobs {
		if err := refuseBatchLabels(batch, what+strconv.Itoa(i), job.Labels); err != nil {
			return err
		}
		if len(job.Chain) > 0 {
			return fmt.Errorf("%w: batch %q: %s%d has a Job.Chain; a batch tracks one run per job, so put the chained work in the job's workflow or a Then callback", workflow.ErrInvalid, batch, what, i)
		}
	}
	return nil
}

// dispatch is Dispatch with the ID chosen; b has passed checkBatch.
func (m *Manager) dispatch(ctx context.Context, b Batch, id ID) (ID, error) {
	if id == "" {
		return "", fmt.Errorf("%w: batch %q: the ID function returned an empty ID", workflow.ErrInvalid, b.Name)
	}
	// The dispatch owner is a lease token, not an ID: it stays random
	// whatever WithIDFunc says, so no other process can guess it.
	owner, err := newID()
	if err != nil {
		return "", err
	}
	jobs := labelJobs(id, 0, b.Jobs)
	now := m.clk.Now()
	st := State{
		ID: id, Name: b.Name, Labels: callbackLabels(b.Labels), Then: b.Then, Catch: b.Catch, Finally: b.Finally,
		Before: b.Before, Progress: b.Progress,
		AllowFailures: b.AllowFailures, DiscardedAs: b.DiscardedAs, Jobs: make([]JobState, len(b.Jobs)), CreatedAt: now,
		DispatchOwner: string(owner), DispatchUntil: now.Add(m.dispatchLease),
	}
	if b.Before != "" {
		st.Pending = []PendingCallback{{Kind: CallbackBefore, Workflow: b.Before}}
	}
	sp, spawns := m.client.(GroupSpawner)
	spawns = spawns && sp.SpawnsGroups() && groupEligible(jobs)
	if spawns && !m.byLabel {
		return "", fmt.Errorf("%w: batch %q: a coordinated batch (GroupSpawner) learns its jobs' ends only by label; attach a LabelSource such as the Hatchet BatchSource", workflow.ErrInvalid, b.Name)
	}
	if _, ok := m.client.(GroupRunner); (ok && groupEligible(jobs)) || spawns || m.resumable || m.retainSpecs {
		st.Spec = jobs
	}
	if err := m.store.Create(ctx, st); err != nil {
		return "", err
	}
	if b.Before != "" {
		// A Before that fails to enqueue stays pending and is retried like
		// any callback; it does not fail the dispatch.
		if err := m.retryBatch(ctx, id); err != nil {
			m.log.ErrorContext(ctx, "batch: enqueue before callback", "batch", string(id), "error", err)
		}
	}
	if spawns {
		if handled, err := m.dispatchSpawn(ctx, sp, id, string(owner), jobs); handled {
			return id, err
		}
	}
	return id, m.start(ctx, id, string(owner), jobs, allIndexes(len(jobs)))
}

// labelJobs copies jobs with the batch labels added, the first at index
// first.
func labelJobs(id ID, first int, jobs []workflow.Job) []workflow.Job {
	out := make([]workflow.Job, len(jobs))
	for k, job := range jobs {
		job.Labels = maps.Clone(job.Labels)
		if job.Labels == nil {
			job.Labels = map[string]string{}
		}
		job.Labels[LabelBatch] = string(id)
		job.Labels[LabelIndex] = strconv.Itoa(first + k)
		out[k] = job
	}
	return out
}

// refuseBatchLabels refuses an app label under workflow.LabelPrefixBatch,
// the prefix this package owns, with an error wrapping workflow.ErrInvalid
// that names the batch, where the label was and the key. Keys are checked
// in sorted order, so the error is the same on every call.
func refuseBatchLabels(batchName, where string, labels map[string]string) error {
	for _, k := range slices.Sorted(maps.Keys(labels)) {
		if strings.HasPrefix(k, workflow.LabelPrefixBatch) {
			return fmt.Errorf("%w: batch %q: %s: label %q uses the reserved prefix %q", workflow.ErrInvalid, batchName, where, k, workflow.LabelPrefixBatch)
		}
	}
	return nil
}

// callbackLabels copies labels without the reserved batch labels, which
// would make a callback look like a job of the batch; nil for none.
// Dispatch already refuses them, so this only guards a State written by
// an older version or by hand.
func callbackLabels(labels map[string]string) map[string]string {
	var out map[string]string
	for k, v := range labels {
		if k == LabelBatch || k == LabelIndex {
			continue
		}
		if out == nil {
			out = make(map[string]string, len(labels))
		}
		out[k] = v
	}
	return out
}

// errLeaseLost stops a dispatch whose lease another process took over.
var errLeaseLost = errors.New("dispatch lease taken over by another process")

// start starts jobs[i] for each i in idxs (ascending, none started yet)
// under owner's lease, as one group when the client can, else one Enqueue
// at a time with the ref saved before the next one starts. That order is
// what lets a resumer know that only the first job without a ref may have
// started. It ends by marking the batch dispatched.
func (m *Manager) start(ctx context.Context, id ID, owner string, jobs []workflow.Job, idxs []int) error {
	if len(idxs) == 0 {
		return m.finishDispatch(ctx, id, owner)
	}
	sub := make([]workflow.Job, len(idxs))
	for k, i := range idxs {
		sub[k] = jobs[i]
	}
	if gr, ok := m.client.(GroupRunner); ok && groupEligible(sub) {
		if handled, err := m.dispatchGroup(ctx, gr, id, owner, idxs, sub); handled {
			return err
		}
	}
	for k, i := range idxs {
		ref, enqErr := m.client.Enqueue(owned(ctx), sub[k])
		if enqErr != nil {
			return errors.Join(fmt.Errorf("batch %s: enqueue job %d: %w", id, i, enqErr), m.abortJobs(ctx, id, owner, idxs[k:], nil))
		}
		saved, err := m.store.Update(ctx, id, func(s *State) error {
			if err := m.renew(s, owner); err != nil {
				return err
			}
			s.Jobs[i].Ref = ref
			if !m.resumable {
				m.dropSpec(s)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("batch %s: save job %d: %w", id, i, err)
		}
		// A failure or Cancel may have cancelled the batch while this job
		// was being enqueued, before its ref was saved; cancel it here and
		// stop enqueuing.
		if saved.Cancelled {
			cerr := m.cancelRun(ctx, saved.Jobs[i])
			return errors.Join(cerr, m.abortJobs(ctx, id, owner, idxs[k+1:], nil))
		}
	}
	return m.finishDispatch(ctx, id, owner)
}

// renew runs inside Store.Update: it refuses when owner no longer holds
// the dispatch lease and otherwise extends it.
func (m *Manager) renew(s *State, owner string) error {
	if s.Dispatched || s.DispatchOwner != owner {
		return errLeaseLost
	}
	s.DispatchUntil = m.clk.Now().Add(m.dispatchLease)
	return nil
}

// finishDispatch marks the batch dispatched and drops the kept jobs.
func (m *Manager) finishDispatch(ctx context.Context, id ID, owner string) error {
	_, err := m.store.Update(ctx, id, func(s *State) error {
		if err := m.renew(s, owner); err != nil {
			return err
		}
		s.Dispatched = true
		m.dropSpec(s)
		return nil
	})
	if err != nil {
		return fmt.Errorf("batch %s: finish dispatch: %w", id, err)
	}
	return nil
}

// abortJobs ends a dispatch that failed: it saves refs[k] for idxs[k]
// where a ref is known (a group that partly started), records the other
// jobs of idxs as cancelled, cancels the batch, marks it dispatched and
// carries out the effects (which cancel the started jobs).
func (m *Manager) abortJobs(ctx context.Context, id ID, owner string, idxs []int, refs []workflow.RunRef) error {
	var eff effects
	var claimed []PendingCallback
	st, err := m.store.Update(ctx, id, func(s *State) error {
		if err := m.renew(s, owner); err != nil {
			return err
		}
		now := m.clk.Now()
		eff = markCancelled(s, now)
		for k, i := range idxs {
			if k < len(refs) && !refs[k].IsZero() {
				s.Jobs[i].Ref = refs[k]
				continue
			}
			e := recordFinish(s, i, workflow.StatusCancelled, now)
			eff.then, eff.finally = eff.then || e.then, eff.finally || e.finally
		}
		s.Dispatched = true
		m.dropSpec(s)
		claimed = m.claimCallbacks(s, eff, now)
		return nil
	})
	if err != nil {
		return fmt.Errorf("batch %s: abort dispatch: %w", id, err)
	}
	return m.apply(ctx, st, eff, claimed)
}

// Attach connects the Manager's Sources (see the package doc for the
// defaults) and must be called once, before client.Start, because a hook
// cannot be registered after it. When no Source is a poller it also starts
// the callback retry loop (see WithManualCallbackRetry). Background
// sources such as the poller, and the retry loop, run until ctx is
// cancelled; pass the ctx given to client.Start. An error
// from any Source is returned and the Manager must not be used.
func (m *Manager) Attach(ctx context.Context) error {
	srcs := m.sources
	if srcs == nil {
		srcs = m.defaultSources()
	}
	polled := false
	for _, src := range srcs {
		if err := src.Attach(ctx, m); err != nil {
			return err
		}
		_, isPoll := src.(pollSource)
		polled = polled || isPoll
		if _, ok := src.(LabelSource); ok {
			m.byLabel = true
		}
	}
	if !polled && !m.manualRetry {
		go m.retryLoop(ctx)
	}
	return nil
}

// observe adapts RunFinished to workflow.RunFinishedFunc, which has no
// error to return, so errors go to the logger.
func (m *Manager) observe(ctx context.Context, ev workflow.RunEvent) {
	if err := m.RunFinished(ctx, ev.Labels, ev.Status); err != nil {
		m.log.ErrorContext(ctx, "batch: run-finished handling failed", "run", ev.Ref.RunID.String(), "error", err)
	}
}

// RunFinished is the run-finished hook: the app calls it (directly or
// through the driver's hook) with a finished run's Job.Labels and terminal
// status. A run that is not a batch job, a non-terminal status, an unknown
// batch, or a repeat for a job already recorded is ignored and returns nil,
// so the hook can be wired to every run.
func (m *Manager) RunFinished(ctx context.Context, labels map[string]string, status workflow.Status) error {
	id, idx, ok := batchJob(labels)
	if !ok {
		return nil
	}
	attempt := 0
	if a, ok := labels[LabelAttempt]; ok {
		n, err := strconv.Atoi(a)
		if err != nil {
			return nil // not a label this package wrote
		}
		attempt = n
	}
	return m.record(ctx, id, idx, attempt, status)
}

// record applies one job's end; every Source ends up here. An end from
// another attempt than the job's current one (a late report of a run
// RetryFailed replaced) changes nothing. The same Update claims the
// batch's due pending callbacks, so a job end also retries them.
func (m *Manager) record(ctx context.Context, id ID, idx, attempt int, status workflow.Status) error {
	if !status.IsTerminal() {
		return nil
	}
	var eff effects
	var claimed []PendingCallback
	st, err := m.store.Update(ctx, id, func(s *State) error {
		now := m.clk.Now()
		if idx >= 0 && idx < len(s.Jobs) && s.Jobs[idx].Attempt != attempt {
			claimed = m.claimCallbacks(s, eff, now)
			return nil
		}
		eff = recordFinish(s, idx, status, now)
		if eff.recorded {
			eff.progress = []int{idx}
		}
		claimed = m.claimCallbacks(s, eff, now)
		return nil
	})
	if errors.Is(err, workflow.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	return m.apply(ctx, st, eff, claimed)
}

// JobLabels returns the batch labels of job idx of batch id at attempt
// (JobState.Attempt): what a Source that learned a job's end by its ref,
// not from the run's own labels, passes to RunFinished. LabelAttempt is
// set only for attempt 1 and up, as on the runs.
func JobLabels(id ID, idx, attempt int) map[string]string {
	labels := map[string]string{LabelBatch: string(id), LabelIndex: strconv.Itoa(idx)}
	if attempt > 0 {
		labels[LabelAttempt] = strconv.Itoa(attempt)
	}
	return labels
}

func batchJob(labels map[string]string) (ID, int, bool) {
	id, idx := labels[LabelBatch], labels[LabelIndex]
	if id == "" || idx == "" {
		return "", 0, false
	}
	n, err := strconv.Atoi(idx)
	if err != nil {
		return "", 0, false
	}
	return ID(id), n, true
}

// Cancel cancels the batch and every job still running. Then will not run;
// Finally runs once the cancelled jobs report back. Cancelling a finished
// or already cancelled batch is a no-op; an unknown ID returns an error
// wrapping workflow.ErrNotFound.
func (m *Manager) Cancel(ctx context.Context, id ID) error {
	var eff effects
	var claimed []PendingCallback
	st, err := m.store.Update(ctx, id, func(s *State) error {
		now := m.clk.Now()
		eff = markCancelled(s, now)
		claimed = m.claimCallbacks(s, eff, now)
		return nil
	})
	if err != nil {
		return err
	}
	return m.apply(ctx, st, eff, claimed)
}

// Progress returns a snapshot of the batch; an unknown ID returns an error
// wrapping workflow.ErrNotFound.
func (m *Manager) Progress(ctx context.Context, id ID) (Progress, error) {
	st, err := m.store.Get(ctx, id)
	if err != nil {
		return Progress{}, err
	}
	return progressOf(st), nil
}

// apply carries out effects decided against st, after the store committed
// them: the cancels, then the callbacks this caller claimed.
func (m *Manager) apply(ctx context.Context, st State, eff effects, claimed []PendingCallback) error {
	var errs []error
	if eff.cancelRest {
		for _, j := range st.Jobs {
			errs = append(errs, m.cancelRun(ctx, j))
		}
		// A coordinated batch's children have no refs; the group cancel
		// reaches them, and the parent so it starts no more.
		if sp, ok := m.client.(GroupSpawner); ok && !st.Parent.IsZero() {
			errs = append(errs, sp.CancelGroup(ctx, string(st.ID), st.Parent, st.CreatedAt))
		}
	}
	errs = append(errs, m.enqueueCallbacks(ctx, st, claimed))
	return errors.Join(errs...)
}

// cancelRun cancels a job's run when it has one and has not finished.
func (m *Manager) cancelRun(ctx context.Context, j JobState) error {
	if j.Ref.IsZero() || j.Status.IsTerminal() {
		return nil
	}
	return m.client.Cancel(ctx, j.Ref)
}

func newID() (ID, error) {
	b := make([]byte, idBytes)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("batch: generate id: %w", err)
	}
	return ID(hex.EncodeToString(b)), nil
}
