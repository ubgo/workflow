package memory

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/ubgo/workflow"
)

// idemKey scopes an idempotency key to its workflow.
type idemKey struct{ workflow, key string }

// eventKey is what a WaitSpec and an Event match on.
type eventKey struct{ name, key string }

// bucketKey is one concurrency bucket of one workflow.
type bucketKey struct{ workflow, key string }

// rateKey is one rate-limit bucket: the workflow, the index of the limit in
// Workflow.RateLimits and the key value.
type rateKey struct {
	workflow string
	limit    int
	key      string
}

// run is one run. Fields below the line are guarded by Client.mu; the
// coordinator goroutine (drive) owns the step bookkeeping.
type run struct {
	reg    *registered
	job    workflow.Job
	ctx    context.Context // cancelled when the run ends, with the reason as cause
	cancel context.CancelCauseFunc
	events chan runEvent // to the coordinator

	// guarded by Client.mu
	id        workflow.RunID
	sid       workflow.ScheduledID
	status    workflow.Status // Scheduled, Queued until admitted, then a terminal status; see statusLocked
	admitted  bool
	idem      *idemKey
	bucket    string   // concurrency key value
	rateKeys  []string // one per Workflow.RateLimits entry
	seq       uint64
	executing int // step attempts running on a worker
	// resultsPending counts attempts that returned whose result the
	// coordinator (drive) has not handled yet: the run's status is still
	// the pre-result one. Drain waits for it, or the shutdown that follows
	// would cancel a run that had in fact finished.
	resultsPending int
	queued         int                  // step attempts waiting for a worker
	retrying       map[string]retryWait // steps waiting on their retry backoff, by step name
	suspended      int                  // wait and sleep steps in progress
	consumed       map[uint64]bool      // signal seqs this run's wait steps have taken
	err            error                // why the run failed or was cancelled; set before finishLocked

	queue      string            // workflow.EffectiveQueue, fixed at enqueue
	outputs    map[string][]byte // steps that succeeded; kept so Retry resumes after them
	enqueuedAt time.Time
	startedAt  time.Time // first step attempt
	finishedAt time.Time
	failedStep string
	attempts   int                     // step attempts made
	errs       []workflow.AttemptError // failed attempts, oldest first, bounded by Client.errHist
	chain      []workflow.Job          // the rest of the chain (Job.Chain plus step edits); enqueued from on success

	runAt       time.Time                          // Job.RunAt or the Reschedule time: RunInfo.RunAt
	rearm       chan struct{}                      // closed to retire the delayed-start timer (Reschedule)
	output      []byte                             // workflow.RunOutput once succeeded: RunInfo.Output, Result
	progress    *workflow.Progress                 // the latest ReportProgress
	attemptedBy []string                           // nodes that ran attempts, first-run order
	running     map[string]context.CancelCauseFunc // executing attempts' cancel, by step: CancelStep
	cancelled   map[string]bool                    // steps CancelStep interrupted while running
	parked      map[string]bool                    // steps in backoff, snooze, wait or sleep: CancelStep
	sched       *schedule                          // the schedule that fired the run, for KeepRuns; nil otherwise
	forgotten   bool                               // dropped by KeepRuns
	awaiting    int                                // wait steps waiting for an event (part of suspended): RunAll leaves them
}

// retryWait is one step waiting on its retry backoff: the attempt it will
// run as and when the backoff ends (RunInfo.NextAttempt, NextAttemptAt).
type retryWait struct {
	attempt int
	at      time.Time
	err     error // the failed attempt's error: RunInfo.Error while retrying
}

func (c *Client) newRunLocked(reg *registered, job workflow.Job) *run {
	ctx, cancel := context.WithCancelCause(c.ctx)
	c.seq++
	r := &run{
		reg:        reg,
		job:        job,
		ctx:        ctx,
		cancel:     cancel,
		events:     make(chan runEvent, 1),
		seq:        c.seq,
		queue:      workflow.EffectiveQueue(reg.wf, job),
		outputs:    map[string][]byte{},
		retrying:   map[string]retryWait{},
		enqueuedAt: c.clk.Now(),
		chain:      job.Chain,
	}
	c.live++
	c.idleEpoch++
	if conc := reg.wf.Concurrency; conc != nil && conc.Key.Func != nil {
		r.bucket = conc.Key.Func(job)
	}
	for _, rl := range reg.wf.RateLimits {
		k := ""
		if rl.Key.Func != nil {
			k = rl.Key.Func(job)
		}
		r.rateKeys = append(r.rateKeys, k)
	}
	return r
}

func (r *run) ref() workflow.RunRef { return workflow.RunRef{RunID: r.id, ScheduledID: r.sid} }

// statusLocked derives the public status. An admitted run is Running while
// any attempt executes, Retrying while a step waits on its retry backoff,
// Queued while an attempt waits for a worker, Waiting while only waits
// and sleeps are in progress.
func (r *run) statusLocked() workflow.Status {
	if r.status.IsTerminal() || !r.admitted {
		return r.status
	}
	switch {
	case r.executing > 0:
		return workflow.StatusRunning
	case len(r.retrying) > 0:
		return workflow.StatusRetrying
	case r.queued > 0:
		return workflow.StatusQueued
	case r.suspended > 0:
		return workflow.StatusWaiting
	}
	return workflow.StatusQueued
}

// endLocked finishes r and admits whatever its freed slot lets in.
func (c *Client) endLocked(r *run, st workflow.Status) {
	c.finishLocked(r, st)
	c.admitLocked()
}

// finishLocked makes r terminal once: it releases the idempotency key and
// the concurrency slot and cancels the run's context. A run already
// terminal is left alone, which is what makes Cancel idempotent. It does
// not admit other runs, so a caller can free several slots before the
// newest run claims one; most callers want endLocked.
func (c *Client) finishLocked(r *run, st workflow.Status) {
	if r.status.IsTerminal() {
		return
	}
	r.status = st
	r.finishedAt = c.clk.Now()
	c.drainTrack.RunFinished(r.id)
	c.resultsPending -= r.resultsPending // a finished run has nothing left to handle
	r.resultsPending = 0
	c.maybeDrainedLocked()
	c.finished = append(c.finished, r)
	c.pruneLocked()
	c.live--
	c.checkIdleLocked()
	switch {
	case st == workflow.StatusSucceeded:
		r.err = nil
	case st == workflow.StatusCancelled && !errors.Is(r.err, workflow.ErrCancelled):
		r.err = workflow.ErrCancelled
	}
	c.queueEndedLocked(r)
	if r.idem != nil && c.idem[*r.idem] == r {
		delete(c.idem, *r.idem)
	}
	c.pending = slices.DeleteFunc(c.pending, func(p *run) bool { return p == r })
	if r.admitted && r.reg.wf.Concurrency != nil {
		bk := bucketKey{workflow: r.reg.wf.Name, key: r.bucket}
		c.buckets[bk] = slices.DeleteFunc(c.buckets[bk], func(p *run) bool { return p == r })
		if len(c.buckets[bk]) == 0 {
			delete(c.buckets, bk)
		}
	}
	cause := workflow.ErrCancelled
	if st == workflow.StatusSucceeded {
		cause = errRunDone
	}
	r.cancel(cause)
	if r.sched != nil {
		c.keepLocked(r.sched)
	}
}

// errRunDone is the context cause of a run that finished normally.
var errRunDone = errors.New("memory: run finished")

// admitLocked starts every pending run its concurrency bucket and rate
// limits allow, highest priority first, and arms a timer for the earliest
// moment a rate-limited run could start.
func (c *Client) admitLocked() {
	if c.stopped || len(c.pending) == 0 {
		return
	}
	slices.SortStableFunc(c.pending, func(a, b *run) int {
		if a.job.Priority != b.job.Priority {
			return cmp.Compare(b.job.Priority, a.job.Priority)
		}
		return cmp.Compare(a.seq, b.seq)
	})
	now := c.clk.Now()
	var wake time.Time
	kept := c.pending[:0]
	for _, r := range c.pending {
		wf := r.reg.wf
		bk := bucketKey{workflow: wf.Name, key: r.bucket}
		if wf.Concurrency != nil && len(c.buckets[bk]) >= wf.Concurrency.MaxRuns {
			kept = append(kept, r)
			continue
		}
		if at, ok := c.rateCheckLocked(r, now); !ok {
			if wake.IsZero() || at.Before(wake) {
				wake = at
			}
			kept = append(kept, r)
			continue
		}
		for i := range wf.RateLimits {
			rk := rateKey{workflow: wf.Name, limit: i, key: r.rateKeys[i]}
			c.rates[rk] = append(c.rates[rk], now)
		}
		if wf.Concurrency != nil {
			c.buckets[bk] = append(c.buckets[bk], r)
		}
		r.admitted = true
		c.armRunTimeoutLocked(r)
		// Ready Run steps (the roots, or after Retry the steps whose
		// parents all succeeded) are queued here, not in drive, so a run's
		// first attempts are in the queue by the time Enqueue returns.
		for _, s := range wf.Steps {
			if s.Kind() == workflow.StepRun && r.readyLocked(s) {
				c.pushTaskLocked(r, s, r.inputLocked(s), 1)
			}
		}
		c.wg.Add(1)
		go c.drive(r)
	}
	clear(c.pending[len(kept):])
	c.pending = kept
	if !wake.IsZero() {
		c.armRateLocked(wake)
	}
}

// rateCheckLocked prunes r's rate windows and reports whether a start is
// allowed now; when not, it returns when the earliest window frees.
func (c *Client) rateCheckLocked(r *run, now time.Time) (time.Time, bool) {
	var wake time.Time
	ok := true
	for i, rl := range r.reg.wf.RateLimits {
		rk := rateKey{workflow: r.reg.wf.Name, limit: i, key: r.rateKeys[i]}
		starts := c.rates[rk]
		cut := 0
		for cut < len(starts) && !starts[cut].Add(rl.Per).After(now) {
			cut++
		}
		starts = starts[cut:]
		if len(starts) == 0 {
			delete(c.rates, rk)
		} else {
			c.rates[rk] = starts
		}
		if len(starts) >= rl.Limit {
			ok = false
			at := starts[len(starts)-rl.Limit].Add(rl.Per)
			if wake.IsZero() || at.After(wake) {
				wake = at // every limit must allow, so wait for the latest
			}
		}
	}
	return wake, ok
}

// armRateLocked makes sure a timer fires by at to retry admission.
func (c *Client) armRateLocked(at time.Time) {
	if c.rateTm != nil && !c.rateAt.After(at) {
		return
	}
	if c.rateTm != nil {
		c.rateTm.Stop()
	}
	t := c.clk.NewTimer(at.Sub(c.clk.Now()))
	c.rateTm, c.rateAt = t, at
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		select {
		case <-t.C():
		case <-c.ctx.Done():
			return
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.rateTm == t {
			c.rateTm, c.rateAt = nil, time.Time{}
		}
		c.admitLocked()
	}()
}

// readyLocked reports whether step s of r has not succeeded and every
// parent has.
func (r *run) readyLocked(s workflow.Step) bool {
	if _, done := r.outputs[s.Name]; done {
		return false
	}
	for _, p := range s.After {
		if _, ok := r.outputs[p]; !ok {
			return false
		}
	}
	return true
}

// inputLocked is the input of step s from the outputs of its parents.
func (r *run) inputLocked(s workflow.Step) []byte {
	outs := make(map[string][]byte, len(s.After))
	for _, p := range s.After {
		outs[p] = r.outputs[p]
	}
	return workflow.StepInput(r.job.Payload, s.After, outs)
}

// runEvent is what reaches a run's coordinator.
type runEvent struct {
	step    string
	kind    eventKind
	out     []byte
	err     error
	started time.Time            // evResult: when the attempt started
	edits   *workflow.ChainEdits // evResult: the attempt's chain edits, applied only on success
}

type eventKind int

const (
	evResult     eventKind = iota // a Run step attempt returned
	evRetry                       // a backoff ended
	evWaitDone                    // a Signal matched a wait step
	evWaitTimed                   // a wait reached its deadline
	evSleepDone                   // a sleep step ended
	evSnoozeDone                  // a snooze ended: run the same attempt again
	evRunTimeout                  // Workflow.RunTimeout passed
	evStepCancel                  // CancelStep on a step with no attempt running
)

// stepState is the coordinator's bookkeeping for one step.
type stepState struct {
	waiting int // parents not yet succeeded
	in      []byte
	att     workflow.Attempts
	done    bool
	out     []byte
}

// drive is the coordinator of one admitted run: it starts steps as their
// parents finish, handles retries and failures, and ends the run. A run
// that Retry re-opened starts with the outputs of the steps that already
// succeeded and runs only the rest.
func (c *Client) drive(r *run) {
	defer c.wg.Done()
	owed := false // the event handled by the current pass was an attempt's result
	defer func() { c.resultHandled(r, &owed) }()
	reg := r.reg
	st := make(map[string]*stepState, len(reg.wf.Steps))
	remaining := len(reg.wf.Steps)
	c.mu.Lock()
	for _, s := range reg.wf.Steps {
		ss := &stepState{}
		if out, ok := r.outputs[s.Name]; ok {
			ss.done, ss.out = true, out
			remaining--
		}
		for _, p := range s.After {
			if _, ok := r.outputs[p]; !ok {
				ss.waiting++
			}
		}
		st[s.Name] = ss
	}
	var start []workflow.Step
	for _, s := range reg.wf.Steps {
		if ss := st[s.Name]; !ss.done && ss.waiting == 0 {
			ss.in = r.inputLocked(s)
			if s.Kind() != workflow.StepRun { // ready Run steps were queued at admission
				start = append(start, s)
			}
		}
	}
	c.mu.Unlock()
	for _, s := range start {
		c.startStep(r, s, st[s.Name])
	}
	for {
		c.resultHandled(r, &owed) // the previous pass is over: its result, if any, is applied
		var ev runEvent
		select {
		case <-r.ctx.Done():
			c.mu.Lock()
			c.endLocked(r, workflow.StatusCancelled)
			c.mu.Unlock()
			return
		case ev = <-r.events:
		}
		owed = ev.kind == evResult
		if ev.kind == evRunTimeout {
			c.fail(r, "", 0, r.job.Payload, workflow.TimeoutError(workflow.ErrRunTimeout, "after "+r.reg.wf.RunTimeout.String()))
			return
		}
		step, ss := reg.steps[ev.step], st[ev.step]
		switch ev.kind {
		case evStepCancel:
			if ss.done {
				continue
			}
			c.fail(r, step.Name, ss.att.N, ss.in, fmt.Errorf("%w: by an operator", workflow.ErrStepCancelled))
			return
		case evRetry:
			c.mu.Lock()
			delete(r.retrying, step.Name)
			delete(r.parked, step.Name)
			c.pushTaskLocked(r, step, ss.in, ss.att.N+1)
			c.mu.Unlock()
			continue
		case evSnoozeDone:
			c.mu.Lock()
			delete(r.parked, step.Name)
			c.pushTaskLocked(r, step, ss.in, ss.att.N+1)
			c.mu.Unlock()
			continue
		case evResult:
			if ss.att.First.IsZero() {
				ss.att.First = ev.started
			}
			c.mu.Lock()
			interrupted := r.cancelled[step.Name]
			delete(r.cancelled, step.Name)
			c.mu.Unlock()
			if ev.err != nil && interrupted {
				ss.att = ss.att.Count(ev.err)
				c.recordErr(r, step.Name, ss.att.N, ev.err)
				c.fail(r, step.Name, ss.att.N, ss.in, fmt.Errorf("%w: %w", workflow.ErrStepCancelled, ev.err))
				return
			}
			if d, ok := workflow.SnoozeDelay(ev.err); ok && ev.err != nil {
				c.snooze(r, step.Name, ss.att.N+1, d)
				continue
			}
			if ev.err != nil {
				ss.att = ss.att.Count(ev.err)
				c.recordErr(r, step.Name, ss.att.N, ev.err)
				if workflow.IsDiscarded(ev.err) {
					c.discard(r, step.Name, ev.err)
					return
				}
				policy := workflow.EffectiveRetriesFor(step, reg.wf, r.job)
				if delay, retry := policy.Next(ev.err, ss.att, c.clk.Now(), c.rnd); retry {
					c.backoff(r, step.Name, ss.att.N+1, delay, ev.err)
					continue
				}
				if derr := policy.ExhaustedError(ss.att.N, ev.err); derr != nil {
					c.discard(r, step.Name, derr)
					return
				}
				c.fail(r, step.Name, ss.att.N, ss.in, fmt.Errorf("memory: step %q failed after %d attempt(s): %w", step.Name, ss.att.N, ev.err))
				return
			}
		case evWaitTimed:
			c.fail(r, step.Name, 0, ss.in, ev.err)
			return
		case evWaitDone, evSleepDone:
			c.mu.Lock()
			delete(r.parked, step.Name)
			c.mu.Unlock()
		}
		ss.done, ss.out = true, ev.out
		c.mu.Lock()
		r.outputs[step.Name] = ev.out
		if ev.edits != nil && ev.edits.Changed() && !r.status.IsTerminal() {
			r.chain = ev.edits.Apply(r.chain) // only a successful attempt's edits count
		}
		c.mu.Unlock()
		remaining--
		if remaining == 0 {
			c.mu.Lock()
			succeeded := !r.status.IsTerminal()
			if succeeded {
				r.output = workflow.RunOutput(reg.wf, r.outputs)
			}
			c.endLocked(r, workflow.StatusSucceeded)
			if succeeded {
				c.continueChainLocked(r)
			}
			c.mu.Unlock()
			return
		}
		for _, child := range reg.children[step.Name] {
			cs := st[child]
			cs.waiting--
			if cs.waiting > 0 {
				continue
			}
			cstep := reg.steps[child]
			outs := make(map[string][]byte, len(cstep.After))
			for _, p := range cstep.After {
				outs[p] = st[p].out
			}
			cs.in = workflow.StepInput(r.job.Payload, cstep.After, outs)
			c.startStep(r, cstep, cs)
		}
	}
}

// discard ends r as StatusDiscarded (workflow.EndStatus) because step
// returned workflow.Discard: no retry, no OnFailure, no chain.
func (c *Client) discard(r *run, step string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !r.status.IsTerminal() {
		r.err = workflow.DiscardedError(step, err)
	}
	c.endLocked(r, workflow.EndStatus(caps, err))
}

// continueChainLocked enqueues the next job of r's chain once r succeeded.
// It runs under the lock that recorded the success, so the next job exists
// before anyone sees r as succeeded with nothing after it. A job that
// cannot be enqueued (its workflow not registered, the client stopping)
// ends the chain; the reason goes to the logger, since the run that
// carried the chain has already succeeded.
func (c *Client) continueChainLocked(r *run) {
	next, ok := workflow.NextInChain(r.chain)
	if !ok {
		return
	}
	if _, err := c.enqueueLocked(context.Background(), next); err != nil {
		c.log.Error("memory: chained job not enqueued; the chain ends here",
			"workflow", r.reg.wf.Name, "run", r.id, "next", next.Workflow, "remaining", len(next.Chain), "error", err)
	}
}

// fail ends r as failed: it cancels the run's other steps, runs OnFailure
// (OnTimeout for a timeout, workflow.FailureHandler's rule) once, then
// marks the run StatusFailed with cause wrapped in a workflow.RunError. A
// run a Cancel or a RunForcer already ended keeps its status and does not
// run OnFailure.
func (c *Client) fail(r *run, step string, attempt int, in []byte, cause error) {
	cause = &workflow.RunError{RunID: r.id, Workflow: r.reg.wf.Name, Step: step, Attempt: attempt, Err: cause}
	r.cancel(cause)
	c.mu.Lock()
	ended := r.status.IsTerminal() // a Cancel or a force got there first
	c.mu.Unlock()
	h := r.reg.onFailure
	if r.reg.onTimeout != nil && workflow.IsTimeout(cause) {
		h = r.reg.onTimeout
	}
	if h != nil && !ended {
		if _, err := safeCall(h, c.ctx, in); err != nil {
			c.log.Error("memory: OnFailure handler failed", "workflow", r.reg.wf.Name, "run", r.id, "error", err)
		}
	}
	c.mu.Lock()
	if !r.status.IsTerminal() {
		r.err = cause
		r.failedStep = step
	}
	c.endLocked(r, workflow.StatusFailed)
	c.mu.Unlock()
}

// startStep begins one step of r according to its kind.
func (c *Client) startStep(r *run, s workflow.Step, ss *stepState) {
	switch s.Kind() {
	case workflow.StepRun:
		c.mu.Lock()
		c.pushTaskLocked(r, s, ss.in, 1)
		c.mu.Unlock()
	case workflow.StepSleep:
		c.mu.Lock()
		r.suspended++
		r.park(s.Name)
		c.mu.Unlock()
		t := c.clk.NewTimer(s.Sleep)
		in := ss.in
		c.forward(r, t, runEvent{step: s.Name, kind: evSleepDone, out: in}, func() {
			c.mu.Lock()
			r.suspended--
			c.mu.Unlock()
		})
	case workflow.StepWait:
		c.startWait(r, s)
	}
}

// forward delivers ev to r's coordinator when t fires, unless the run ends
// first. after runs before delivery.
//
// RunAll's hurry ends the wait early: forward carries only delays RunAll
// ends (backoffs, snoozes, sleeps), never timeouts.
func (c *Client) forward(r *run, t interface{ C() <-chan time.Time }, ev runEvent, after func()) {
	c.mu.Lock()
	hurry := c.hurry
	c.mu.Unlock()
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		select {
		case <-t.C():
			after()
			c.send(r, ev)
		case <-hurry:
			if s, ok := t.(interface{ Stop() bool }); ok {
				s.Stop()
			}
			after()
			c.send(r, ev)
		case <-r.ctx.Done():
			if s, ok := t.(interface{ Stop() bool }); ok {
				s.Stop()
			}
		}
	}()
}

func (c *Client) send(r *run, ev runEvent) {
	select {
	case r.events <- ev:
	case <-r.ctx.Done():
	}
}

// backoff waits d on the injected clock, then asks the coordinator to run
// attempt next of step.
func (c *Client) backoff(r *run, step string, next int, d time.Duration, err error) {
	c.mu.Lock()
	r.retrying[step] = retryWait{attempt: next, at: c.clk.Now().Add(d), err: err}
	r.park(step)
	c.emitLocked(r, workflow.LifecycleRetrying, step, next-1, err)
	c.mu.Unlock()
	c.forward(r, c.clk.NewTimer(d), runEvent{step: step, kind: evRetry}, func() {})
}

// snooze puts step back for d without spending an attempt (workflow.Snooze):
// the run reports StatusWaiting meanwhile, and the same attempt number runs
// again. The worker counted the attempt; it is taken back here.
func (c *Client) snooze(r *run, step string, attempt int, d time.Duration) {
	c.mu.Lock()
	r.attempts--
	r.suspended++
	r.park(step)
	c.emitLocked(r, workflow.LifecycleSnoozed, step, attempt, workflow.Snooze(d))
	c.mu.Unlock()
	c.forward(r, c.clk.NewTimer(d), runEvent{step: step, kind: evSnoozeDone}, func() {
		c.mu.Lock()
		r.suspended--
		c.mu.Unlock()
	})
}

// recordErr adds a failed attempt to r's error history, naming this node.
func (c *Client) recordErr(r *run, step string, attempt int, err error) {
	c.mu.Lock()
	ae := workflow.NewAttemptError(step, attempt, err, c.clk.Now())
	ae.Node = c.nodeID()
	r.errs = workflow.AppendAttemptError(r.errs, ae, c.errHist)
	c.mu.Unlock()
}

// park records that step has no attempt running but is not done (backoff,
// snooze, wait, sleep), so CancelStep can end it. Callers hold Client.mu.
func (r *run) park(step string) {
	if r.parked == nil {
		r.parked = map[string]bool{}
	}
	r.parked[step] = true
}

// waiter is one wait step registered for Signal.
type waiter struct {
	r  *run
	ch chan []byte // buffered 1: Signal never blocks

	// delivered is set, under Client.mu, when an event was handed to ch:
	// from then on the wait no longer counts in r.awaiting, so RunAll sees
	// the run as able to move before the waiter goroutine wakes.
	delivered bool
}

// deliverLocked marks w as given its event and stops counting it as
// waiting. Callers hold Client.mu.
func (w *waiter) deliverLocked() {
	if !w.delivered {
		w.delivered = true
		w.r.awaiting--
	}
}

// signal is one received Event, kept so a run that reaches its wait step
// after the Signal still sees it (the look-back engines such as Hatchet
// give: a lost "payment succeeded" is the worst outcome). seq orders it
// against run enqueues on the client's counter.
type signal struct {
	seq     uint64
	payload []byte
}

// consumeLocked returns the first kept signal for k that r may take: sent
// after r was enqueued and not taken by r before. It marks it taken.
func (c *Client) consumeLocked(r *run, k eventKey) ([]byte, bool) {
	for _, sg := range c.signals[k] {
		if sg.seq > r.seq && !r.consumed[sg.seq] {
			r.markConsumedLocked(sg.seq)
			return sg.payload, true
		}
	}
	return nil, false
}

func (r *run) markConsumedLocked(seq uint64) {
	if r.consumed == nil {
		r.consumed = map[uint64]bool{}
	}
	r.consumed[seq] = true
}

func (c *Client) startWait(r *run, s workflow.Step) {
	key, err := s.Wait.ResolveKey(r.job) // Start refused Expr keys, so this is Func or the fixed Key
	if err != nil {
		c.wg.Add(1)
		go func() { // the coordinator calls startWait, so it cannot send to itself
			defer c.wg.Done()
			c.send(r, runEvent{step: s.Name, kind: evWaitTimed, err: err})
		}()
		return
	}
	w := &waiter{r: r, ch: make(chan []byte, 1)}
	k := eventKey{name: s.Wait.Event, key: key}
	c.mu.Lock()
	r.suspended++
	r.awaiting++
	r.park(s.Name)
	if p, ok := c.consumeLocked(r, k); ok {
		w.ch <- p // a signal arrived before the wait began; resume at once
		w.deliverLocked()
	} else {
		c.waiters[k] = append(c.waiters[k], w)
	}
	c.mu.Unlock()

	var timeout <-chan time.Time
	var stop func() bool
	d := s.Wait.Timeout
	if !s.Wait.Deadline.IsZero() {
		d = s.Wait.Deadline.Sub(c.clk.Now())
		if d <= 0 {
			d = -1 // already past: time out at once
		}
	}
	if d != 0 {
		t := c.clk.NewTimer(d)
		timeout, stop = t.C(), t.Stop
	}
	remove := func() {
		c.mu.Lock()
		r.suspended--
		if !w.delivered {
			w.r.awaiting--
			w.delivered = true
		}
		c.waiters[k] = slices.DeleteFunc(c.waiters[k], func(o *waiter) bool { return o == w })
		if len(c.waiters[k]) == 0 {
			delete(c.waiters, k)
		}
		c.mu.Unlock()
	}
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		select {
		case p := <-w.ch:
			if stop != nil {
				stop()
			}
			remove()
			c.send(r, runEvent{step: s.Name, kind: evWaitDone, out: p})
		case <-timeout:
			remove()
			c.send(r, runEvent{step: s.Name, kind: evWaitTimed,
				err: fmt.Errorf("%w: %w", workflow.TimeoutError(workflow.ErrWaitTimeout,
					fmt.Sprintf("memory: wait step %q for event %q key %q", s.Name, s.Wait.Event, key)), context.DeadlineExceeded)})
		case <-r.ctx.Done():
			if stop != nil {
				stop()
			}
			remove()
		}
	}()
}

// task is one step attempt waiting for a worker.
type task struct {
	r        *run
	step     workflow.Step
	in       []byte
	attempt  int
	priority int
	seq      uint64
	readyAt  time.Time // when it was pushed: StepInfo.ReadyAt
}

func (c *Client) pushTaskLocked(r *run, s workflow.Step, in []byte, attempt int) {
	c.seq++
	r.queued++
	c.tasks = append(c.tasks, &task{r: r, step: s, in: in, attempt: attempt, priority: r.job.Priority, seq: c.seq, readyAt: c.clk.Now()})
	c.taskCond.Broadcast() // a worker idle on a full queue cannot take it, so wake them all
}

// before reports whether task a goes before b: the higher queue priority
// (QueueConfig.Priority), then the higher Job.Priority, then arrival.
func (c *Client) before(a, b *task) bool {
	if qa, qb := c.queues[a.r.queue].Priority, c.queues[b.r.queue].Priority; qa != qb {
		return qa > qb
	}
	if a.priority != b.priority {
		return a.priority > b.priority
	}
	return a.seq < b.seq
}

// pickLocked removes and returns the first task a worker may claim now,
// or nil. It drops tasks of runs that already ended, and passes over
// tasks held by a pause or by their queue's worker cap. A linear scan, not
// a heap, because eligibility changes as pauses and queue slots come and
// go; an in-memory queue is small enough for that.
func (c *Client) pickLocked() *task {
	best := -1
	var eligible []int // indexes into kept of the tasks a worker may claim now
	kept := c.tasks[:0]
	for _, t := range c.tasks {
		if t.r.ctx.Err() != nil {
			t.r.queued--
			continue
		}
		kept = append(kept, t)
		if c.heldLocked(t.r) {
			continue
		}
		if q, ok := c.queues[t.r.queue]; ok && queueFull(q, c.queueExec[t.r.queue]) {
			continue
		}
		eligible = append(eligible, len(kept)-1)
		if best < 0 || c.before(t, kept[best]) {
			best = len(kept) - 1
		}
	}
	clear(c.tasks[len(kept):])
	c.tasks = kept
	if best < 0 {
		return nil
	}
	if w := c.weightedLocked(eligible, c.queues[c.tasks[best].r.queue].Priority); w >= 0 {
		best = w
	}
	t := c.tasks[best]
	c.tasks = slices.Delete(c.tasks, best, best+1)
	return t
}

// weightedLocked applies QueueConfig.Weight: among the eligible tasks of
// queues at priority prio, it draws a queue with workflow.PickWeighted
// (queues in name order, so a pinned random source picks the same one
// every time) and returns that queue's first task in c.before order. It
// returns -1 when fewer than two such queues have work or none of them
// has a weight, which keeps the strict order. Callers hold c.mu.
func (c *Client) weightedLocked(eligible []int, prio int) int {
	byQueue := map[string]int{} // queue -> index of its first task
	weighted := false
	for _, i := range eligible {
		t := c.tasks[i]
		q := c.queues[t.r.queue]
		if q.Priority != prio {
			continue
		}
		weighted = weighted || q.Weight > 0
		if j, ok := byQueue[t.r.queue]; !ok || c.before(t, c.tasks[j]) {
			byQueue[t.r.queue] = i
		}
	}
	if !weighted || len(byQueue) < 2 {
		return -1
	}
	names := slices.Sorted(maps.Keys(byQueue))
	weights := make([]int, len(names))
	for i, n := range names {
		weights[i] = c.queues[n].Weight
	}
	rnd := c.rnd
	if rnd == nil {
		rnd = rand.Float64
	}
	return byQueue[names[workflow.PickWeighted(weights, rnd())]]
}

// heldLocked reports whether a pause in force holds r.
func (c *Client) heldLocked(r *run) bool {
	for t := range c.pauses {
		if t.Holds(r.reg.wf.Name, r.queue) {
			return true
		}
	}
	return false
}

// worker executes step attempts until the client stops.
func (c *Client) worker() {
	defer c.wg.Done()
	for {
		c.mu.Lock()
		var t *task
		for !c.stopped {
			if !c.draining {
				if t = c.pickLocked(); t != nil {
					break
				}
			}
			c.taskCond.Wait()
		}
		if c.stopped {
			c.mu.Unlock()
			return
		}
		t.r.queued--
		t.r.executing++
		t.r.attempts++
		started := c.clk.Now()
		if t.r.startedAt.IsZero() {
			t.r.startedAt = started
		}
		if node := c.nodeID(); !slices.Contains(t.r.attemptedBy, node) {
			t.r.attemptedBy = append(t.r.attemptedBy, node)
		}
		c.executing++
		c.queueExec[t.r.queue]++
		c.emitLocked(t.r, workflow.LifecycleStarted, t.step.Name, t.attempt, nil)
		c.mu.Unlock()
		endTrack := c.drainTrack.Begin(workflow.DrainCut{RunID: t.r.id, Workflow: t.r.job.Workflow, Step: t.step.Name, Attempt: t.attempt})

		out, edits, err := c.exec(t)

		endTrack()
		c.mu.Lock()
		t.r.executing--
		c.executing--
		c.queueExec[t.r.queue]--
		if !t.r.status.IsTerminal() {
			t.r.resultsPending++
			c.resultsPending++
		}
		c.maybeDrainedLocked()
		if len(c.tasks) > 0 {
			c.taskCond.Broadcast() // a queue slot freed
		}
		c.mu.Unlock()
		c.send(t.r, runEvent{step: t.step.Name, kind: evResult, out: out, err: err, started: started, edits: edits})
		c.recycleAfterAttempt()
	}
}

// errStepTimeout is the context cause of an attempt that ran past its
// timeout; errHeartbeat of one that went its HeartbeatTimeout without a
// Heartbeat.
var (
	errStepTimeout = errors.New("memory: step attempt timed out")
	errHeartbeat   = errors.New("memory: step attempt missed its heartbeat")
)

// exec runs one attempt with its timeout on the injected clock, and
// returns the chain edits the attempt made (nil for a closure run, which
// is never chained).
func (c *Client) exec(t *task) ([]byte, *workflow.ChainEdits, error) {
	h := t.r.reg.handlers[t.step.Name]
	var edits *workflow.ChainEdits
	ctx := t.r.ctx
	if !t.r.reg.closure {
		c.mu.Lock()
		edits = workflow.NewChainEdits(caps, t.r.chain)
		c.mu.Unlock()
		ctx = workflow.WithChainEditor(ctx, edits)
	}
	rctx := workflow.WithStepInfo(ctx, workflow.StepInfo{
		Workflow: t.r.reg.wf.Name,
		Step:     t.step.Name,
		RunID:    t.r.id,
		Attempt:  t.attempt,
		Labels:   t.r.job.Labels,
		Queue:    t.r.queue,
		ReadyAt:  t.readyAt,
		Tenant:   t.r.job.Tenant,
		Node:     c.nodeID(),
	})
	actx, cancel := context.WithCancelCause(rctx)
	defer cancel(nil)
	c.mu.Lock()
	if t.r.running == nil {
		t.r.running = map[string]context.CancelCauseFunc{}
	}
	t.r.running[t.step.Name] = cancel
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(t.r.running, t.step.Name)
		c.mu.Unlock()
	}()
	actx = workflow.WithProgressReporter(actx, workflow.ProgressFunc(func(_ context.Context, p workflow.Progress) error {
		return c.reportProgress(t, p)
	}))
	beat := c.watchHeartbeat(actx, cancel, t.step.HeartbeatTimeout)
	actx = workflow.WithHeartbeater(actx, beat)
	d := t.step.Timeout
	if d == 0 {
		d = t.r.reg.wf.Timeout
	}
	if d > 0 {
		tm := c.clk.NewTimer(d)
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			select {
			case <-tm.C():
				cancel(errStepTimeout)
			case <-actx.Done():
				tm.Stop()
			}
		}()
	}
	out, err := safeCall(h, actx, t.in)
	switch cause := context.Cause(actx); {
	case errors.Is(cause, errStepTimeout):
		return nil, edits, fmt.Errorf("%w: %w", workflow.TimeoutError(workflow.ErrStepTimeout,
			fmt.Sprintf("memory: step %q attempt %d after %s", t.step.Name, t.attempt, d)), context.DeadlineExceeded)
	case errors.Is(cause, errHeartbeat):
		return nil, edits, workflow.TimeoutError(workflow.ErrHeartbeatTimeout,
			fmt.Sprintf("memory: step %q attempt %d went %s without a heartbeat", t.step.Name, t.attempt, t.step.HeartbeatTimeout))
	}
	return out, edits, err
}

// safeCall runs h and turns a panic into an error, so one bad handler
// cannot crash the process that hosts every other run.
func safeCall(h workflow.Handler, ctx context.Context, in []byte) (out []byte, err error) {
	defer func() {
		if p := recover(); p != nil {
			out, err = nil, fmt.Errorf("memory: handler panicked: %v", p)
		}
	}()
	out, err = h(ctx, in)
	if err != nil && errors.Is(context.Cause(ctx), workflow.ErrCancelled) {
		err = fmt.Errorf("%w: %w", workflow.ErrCancelled, err)
	}
	return out, err
}

// resultHandled releases the claim an attempt's result holds on a draining
// client once drive has applied it to the run (or the run ended): the
// worker raises resultsPending when the attempt returns, and Drain must not
// finish before the run's status reflects it.
func (c *Client) resultHandled(r *run, owed *bool) {
	if !*owed {
		return
	}
	*owed = false
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.resultsPending > 0 {
		r.resultsPending--
		c.resultsPending--
	}
	c.maybeDrainedLocked()
}

// settledLocked reports whether no attempt is executing and no returned
// attempt's result is waiting to be applied: the moment a drain is done.
func (c *Client) settledLocked() bool { return c.executing == 0 && c.resultsPending == 0 }

// maybeDrainedLocked closes the drain signal once a draining client is
// settled.
func (c *Client) maybeDrainedLocked() {
	if c.draining && c.drained != nil && c.settledLocked() {
		closeOnce(c.drained)
	}
}
