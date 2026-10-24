package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/ubgo/workflow"
)

// stepRec is one step's stored progress inside a run's steps JSON. A run
// is claimed whole, and its holder runs every step whose parents are done,
// so this record is the only per-step state.
type stepRec struct {
	Done   bool   `json:"d,omitempty"`
	Out    []byte `json:"o,omitempty"`
	N      int    `json:"n,omitempty"`  // attempts made (Attempts.N)
	Errors int    `json:"e,omitempty"`  // attempts that failed with a non-release error
	First  int64  `json:"f,omitempty"`  // first attempt's start, Unix ns
	Due    int64  `json:"t,omitempty"`  // retry ready at, or sleep end, Unix ns on the scheduling clock
	Err    string `json:"er,omitempty"` // the last failed attempt's error
	Z      bool   `json:"z,omitempty"`  // Due is the end of a snooze, not a retry backoff
}

func (s *stepRec) attempts() workflow.Attempts {
	a := workflow.Attempts{N: s.N, Errors: s.Errors}
	if s.First != 0 {
		a.First = time.Unix(0, s.First)
	}
	return a
}

// held is a run this client claimed, while it executes.
type held struct {
	*stored
	reg    *registered
	token  string
	ctx    context.Context // cancelled with ErrCancelled on Cancel, errLost when the lease is lost
	cancel context.CancelCauseFunc

	mu          sync.Mutex
	live        map[string]context.CancelCauseFunc // running attempts by step
	withdrawn   map[string]bool                    // steps CancelStep withdrew while held
	roundCancel context.CancelCauseFunc            // ends the current round (RunTimeout)
	timedOut    bool                               // told the run is past its RunTimeout
}

// cancelStep withdraws step (CancelStep): its running attempt's ctx is
// cancelled with cause workflow.ErrStepCancelled, and a step with no
// attempt running fails at the next plan.
func (h *held) cancelStep(step string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.withdrawn == nil {
		h.withdrawn = map[string]bool{}
	}
	h.withdrawn[step] = true
	if f := h.live[step]; f != nil {
		f(workflow.ErrStepCancelled)
	}
}

// isWithdrawn reports whether step was withdrawn, stored or told.
func (h *held) isWithdrawn(step string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.withdrawn[step] || h.stepCancels[step]
}

// timeOut ends the current round: the run is past its RunTimeout.
func (h *held) timeOut() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.timedOut = true
	if h.roundCancel != nil {
		h.roundCancel(errRunTimeout)
	}
}

// errRunTimeout is the context cause of attempts cut off because their run
// went past Workflow.RunTimeout.
var errRunTimeout = errors.New("redis: run timed out")

// errLost is the context cause of a run whose lease this client lost.
var errLost = errors.New("redis: lease lost; another client holds the run")

// errStepTimeout is the context cause of an attempt past its timeout.
var errStepTimeout = errors.New("redis: step attempt timed out")

// worker claims and executes runs until the client stops or drains.
func (c *Client) worker() {
	defer c.wg.Done()
	for {
		c.mu.Lock()
		stop := c.stopped || c.draining || c.ctx.Err() != nil
		wake := c.wakeCh
		c.mu.Unlock()
		if stop {
			return
		}
		h, err := c.claim()
		if err != nil && c.ctx.Err() == nil {
			c.log.Error("redis: claim a run", "error", err)
		}
		if h != nil {
			c.execute(h)
			continue
		}
		t := c.leaseClk.NewTimer(c.poll)
		select {
		case <-c.ctx.Done():
			t.Stop()
			return
		case <-wake:
			t.Stop()
		case <-t.C():
		}
	}
}

// claim takes the best ready run this client may take, or returns nil.
// It counts the run in flight before the claim so Drain cannot miss it.
// While a queue has a worker cap, claims are serialised so the last slot
// of a queue goes to one worker.
func (c *Client) claim() (*held, error) {
	c.mu.Lock()
	if c.crashed || c.draining || c.stopped {
		c.mu.Unlock()
		return nil, nil
	}
	c.inflight++
	capped := len(c.queueCfg) > 0
	c.mu.Unlock()
	if capped {
		c.claimMu.Lock()
		defer c.claimMu.Unlock()
	}
	h, err := c.claimOnce()
	if h == nil {
		c.doneInflight()
	}
	return h, err
}

// queueArgs is the queue part of the claim script's arguments: 'all' with
// no triples for a client that serves every queue, else 'some' and the
// served queues not at their worker cap, each with its priority and a
// rank. Among queues of equal priority the ranks are a weighted random
// order (workflow.PickWeighted over QueueConfig.Weight, drawn from the
// jitter source), so the first of them with ready work is chosen in
// proportion to its weight; with every weight zero all ranks are equal
// and the old strict order (Job.Priority, then age) holds.
func (c *Client) queueArgs() []any {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.queueCfg) == 0 {
		return []any{"all", 0}
	}
	byPrio := map[int][]workflow.QueueConfig{}
	for name, q := range c.queueCfg {
		if q.Workers > 0 && c.heldQ[name] >= q.Workers {
			continue
		}
		byPrio[q.Priority] = append(byPrio[q.Priority], q)
	}
	var out []any
	n := 0
	for prio, qs := range byPrio {
		sort.Slice(qs, func(i, j int) bool { return qs[i].Name < qs[j].Name })
		weighted := false
		for _, q := range qs {
			weighted = weighted || q.Weight > 0
		}
		if !weighted {
			for _, q := range qs {
				out = append(out, q.Name, prio, 0)
				n++
			}
			continue
		}
		rest := slices.Clone(qs)
		for rank := 0; len(rest) > 0; rank++ {
			ws := make([]int, len(rest))
			for i, q := range rest {
				ws[i] = q.Weight
			}
			i := workflow.PickWeighted(ws, c.draw())
			out = append(out, rest[i].Name, prio, rank)
			n++
			rest = slices.Delete(rest, i, i+1)
		}
	}
	return append([]any{"some", n}, out...)
}

// draw is a uniform value in [0, 1) from the jitter source
// (WithJitterSource), math/rand by default.
func (c *Client) draw() float64 {
	if c.rnd != nil {
		return c.rnd()
	}
	return rand.Float64()
}

func (c *Client) claimOnce() (*held, error) {
	token := c.newID()
	args := []any{c.base, c.clk.Now().UnixMilli(), c.leaseClk.Now().UnixMilli(),
		c.leaseClk.Now().Add(c.lease).UnixMilli(), token, c.clk.Now().UnixNano()}
	qa := c.queueArgs()
	if qa[0] == "some" && qa[1] == 0 {
		return nil, nil // every served queue is at its cap
	}
	args = append(args, qa...)
	for _, n := range c.names {
		args = append(args, n)
	}
	res, err := claimScript.Run(c.ctx, c.rdb, c.keys(), args...).Slice()
	if errors.Is(err, goredis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, classify(err)
	}
	id := str(res[0])
	flat, _ := res[1].([]any)
	m := make(map[string]string, len(flat)/2)
	for i := 0; i+1 < len(flat); i += 2 {
		m[str(flat[i])] = str(flat[i+1])
	}
	r, err := decodeRun(id, m)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	reg := c.wfs[r.wf]
	ctx, cancel := context.WithCancelCause(c.ctx)
	h := &held{stored: r, reg: reg, token: token, ctx: ctx, cancel: cancel, live: map[string]context.CancelCauseFunc{}}
	c.running[id] = h
	c.heldQ[r.queue]++
	c.mu.Unlock()
	return h, nil
}

func (c *Client) doneInflight() {
	c.mu.Lock()
	c.inflight--
	if c.inflight == 0 && c.idle != nil {
		select {
		case <-c.idle:
		default:
			close(c.idle)
		}
	}
	c.mu.Unlock()
}

// execute runs a held run's steps round by round under a renewed lease,
// recording each attempt, until the run ends or is handed back.
func (c *Client) execute(h *held) {
	defer c.doneInflight()
	stopBeat := c.heartbeat(h)
	defer func() {
		stopBeat()
		h.cancel(nil)
		c.mu.Lock()
		delete(c.running, h.id)
		c.heldQ[h.queue]--
		c.mu.Unlock()
	}()
	if h.reg == nil { // claimed only for registered workflows; never expected
		c.release(h, time.Time{}, "queued")
		return
	}
	if h.reclaims > c.maxReclaims {
		c.finish(h, workflow.StatusFailed, "", fmt.Errorf("%w: its lease expired %d times in a row (WithMaxReclaims %d)", ErrAbandoned, h.reclaims, c.maxReclaims))
		return
	}
	job := workflow.Job{Workflow: h.wf, Payload: h.payload, Labels: h.labels, Priority: h.priority, Retry: h.retry, Queue: h.queue}
	for rounds := 0; ; rounds++ {
		if c.gone(h) {
			return
		}
		if rounds > 0 && c.held(h) {
			c.release(h, time.Time{}, "queued") // paused meanwhile: the next steps wait
			return
		}
		now := c.clk.Now()
		if c.pastDeadline(h, now) {
			step, in := c.current(h, job)
			c.fail(h, step, in, c.runTimeoutErr(h))
			return
		}
		p, err := c.plan(h, job, now)
		if err != nil {
			if !errors.Is(err, errLost) {
				c.log.Error("redis: begin a wait step (the run is handed back)", "run", h.id, "error", err)
				c.release(h, time.Time{}, "queued")
			}
			return
		}
		switch {
		case p.timedOut != nil:
			c.fail(h, p.timedOut.step, p.timedOut.in, p.timedOut.err)
			return
		case p.withdrawn != nil:
			c.fail(h, p.withdrawn.step, p.withdrawn.in, p.withdrawn.err)
			return
		case p.all:
			c.finish(h, workflow.StatusSucceeded, "", nil)
			return
		case len(p.ready) == 0:
			status, due := "queued", p.wake
			switch {
			case c.retrying(h, now):
				status = string(workflow.StatusRetrying)
			case p.waiting || c.sleeping(h, now) || c.snoozed(h, now):
				status = string(workflow.StatusWaiting)
			}
			if due.IsZero() && p.waiting {
				c.park(h, status)
				return
			}
			c.release(h, due, status)
			return
		}
		if ended := c.round(h, job, p.ready, now); ended {
			return
		}
	}
}

// held reports whether a pause now holds h's next steps. A failed read
// holds nothing: the claim script checks pauses again.
func (c *Client) held(h *held) bool {
	ctx, cancel := c.writeCtx()
	defer cancel()
	vals, err := c.rdb.HMGet(ctx, c.base+":paused", "all", "w:"+h.wf, "q:"+h.queue).Result()
	if err != nil {
		return false
	}
	for _, v := range vals {
		if v != nil {
			return true
		}
	}
	return false
}

// gone reports whether the run must stop here without writing: it was
// cancelled or its lease lost (the fence refuses writes anyway), or the
// client stopped, in which case the run is handed back.
func (c *Client) gone(h *held) bool {
	if c.isCrashed() {
		return true
	}
	if cause := context.Cause(h.ctx); cause != nil {
		if c.ctx.Err() != nil && !errors.Is(cause, workflow.ErrCancelled) && !errors.Is(cause, workflow.ErrForced) && !errors.Is(cause, errLost) {
			c.release(h, time.Time{}, "queued")
		}
		return true
	}
	return false
}

// planned is what plan found: the Run steps ready now, the earliest
// future due time when none is (zero if none), whether every step is
// done, whether a wait step is waiting for a signal, and a wait that
// reached its deadline.
type planned struct {
	ready     []workflow.Step
	wake      time.Time
	all       bool
	waiting   bool
	timedOut  *waitTimeout
	withdrawn *waitTimeout // a step CancelStep withdrew with no attempt running
}

// waitTimeout is a wait step whose deadline passed with no signal.
type waitTimeout struct {
	step string
	in   []byte
	err  error
}

// plan finishes due sleep steps and resolved waits, begins waits whose
// parents are done, and reports what is ready.
func (c *Client) plan(h *held, job workflow.Job, now time.Time) (planned, error) {
	p := planned{all: true}
	for _, s := range h.reg.wf.Steps {
		rec := h.rec(s.Name)
		if rec.Done {
			continue
		}
		p.all = false
		if !h.parentsDone(s) {
			continue
		}
		if h.isWithdrawn(s.Name) {
			p.withdrawn = &waitTimeout{step: s.Name, in: h.input(s, job),
				err: fmt.Errorf("%w: redis: step %q was cancelled", workflow.ErrStepCancelled, s.Name)}
			return p, nil
		}
		var at int64
		switch s.Kind() {
		case workflow.StepSleep:
			if rec.Due == 0 {
				rec.Due = now.Add(s.Sleep).UnixNano()
			}
			if rec.Due <= now.UnixNano() {
				rec.Done, rec.Out = true, h.input(s, job)
				return c.plan(h, job, now) // its children may be ready now
			}
			at = rec.Due
		case workflow.StepWait:
			out, got, deadline, err := c.beginWait(h, s, job, now)
			if err != nil {
				return planned{}, err
			}
			if got {
				rec.Done, rec.Out = true, out
				return c.plan(h, job, now)
			}
			if deadline != 0 && deadline <= now.UnixNano() {
				key, _ := s.Wait.ResolveKey(job)
				p.timedOut = &waitTimeout{step: s.Name, in: h.input(s, job),
					err: fmt.Errorf("%w: %w", workflow.TimeoutError(workflow.ErrWaitTimeout,
						fmt.Sprintf("redis: wait step %q for event %q key %q", s.Name, s.Wait.Event, key)), context.DeadlineExceeded)}
				return p, nil
			}
			p.waiting = true
			at = deadline
		default:
			if rec.Due <= now.UnixNano() {
				p.ready = append(p.ready, s)
				continue
			}
			at = rec.Due
		}
		if at != 0 {
			if t := time.Unix(0, at); p.wake.IsZero() || t.Before(p.wake) {
				p.wake = t
			}
		}
	}
	return p, nil
}

// beginWait resolves wait step s from a signal already taken, or begins
// it (once): a kept signal the run may take resolves it at once, else the
// wait is registered with its deadline (ns on the scheduling clock, 0 for
// none) so Signal can resume the run. It returns errLost when the run is
// no longer held.
func (c *Client) beginWait(h *held, s workflow.Step, job workflow.Job, now time.Time) (out []byte, got bool, deadline int64, err error) {
	if v, ok := h.waitVals[s.Name]; ok {
		return v, true, 0, nil
	}
	key, err := s.Wait.ResolveKey(job) // Start refused Expr keys, so this is Func or the fixed Key
	if err != nil {
		return nil, false, 0, err
	}
	switch {
	case !s.Wait.Deadline.IsZero():
		deadline = s.Wait.Deadline.UnixNano()
	case s.Wait.Timeout > 0:
		deadline = now.Add(s.Wait.Timeout).UnixNano()
	}
	ctx, cancel := c.writeCtx()
	defer cancel()
	res, err := waitScript.Run(ctx, c.rdb, c.keys(), c.base, h.id, h.token, s.Name, sigKey(s.Wait.Event, key), deadline).Slice()
	if err != nil {
		return nil, false, 0, classify(err)
	}
	switch str(res[0]) {
	case "lost":
		h.cancel(errLost)
		return nil, false, 0, errLost
	case "got":
		return []byte(str(res[1])), true, 0, nil
	}
	d, _ := strconv.ParseInt(str(res[1]), 10, 64)
	return nil, false, d, nil
}

// snoozed reports whether a Run step waits out a Snooze (the run then
// reads StatusWaiting while handed back).
func (c *Client) snoozed(h *held, now time.Time) bool {
	for _, s := range h.reg.wf.Steps {
		if rec := h.rec(s.Name); !rec.Done && rec.Z && rec.Due > now.UnixNano() {
			return true
		}
	}
	return false
}

// retrying reports whether a Run step waits on its retry backoff (the run
// then reads StatusRetrying while handed back).
func (c *Client) retrying(h *held, now time.Time) bool {
	for _, s := range h.reg.wf.Steps {
		if rec := h.rec(s.Name); s.Kind() == workflow.StepRun && !rec.Done && !rec.Z && rec.N > 0 && rec.Due > now.UnixNano() {
			return true
		}
	}
	return false
}

// sleeping reports whether a sleep step is in progress (the run then reads
// StatusWaiting while handed back).
func (c *Client) sleeping(h *held, now time.Time) bool {
	for _, s := range h.reg.wf.Steps {
		if rec := h.rec(s.Name); s.Kind() == workflow.StepSleep && !rec.Done && rec.Due > now.UnixNano() {
			return true
		}
	}
	return false
}

func (h *held) rec(step string) *stepRec {
	r := h.steps[step]
	if r == nil {
		r = &stepRec{}
		h.steps[step] = r
	}
	return r
}

func (h *held) parentsDone(s workflow.Step) bool {
	for _, p := range s.After {
		if !h.rec(p).Done {
			return false
		}
	}
	return true
}

// input is step s's input from its parents' outputs.
func (h *held) input(s workflow.Step, job workflow.Job) []byte {
	outs := make(map[string][]byte, len(s.After))
	for _, p := range s.After {
		outs[p] = h.rec(p).Out
	}
	return workflow.StepInput(job.Payload, s.After, outs)
}

// result is one attempt's outcome.
type result struct {
	step      workflow.Step
	in        []byte
	out       []byte
	err       error
	started   time.Time
	edits     *workflow.ChainEdits
	withdrawn bool // the attempt's ctx was cancelled by CancelStep
}

// round runs the ready steps at once and records each outcome as it
// arrives. It reports whether the run ended or was handed back.
func (c *Client) round(h *held, job workflow.Job, ready []workflow.Step, now time.Time) bool {
	results := make(chan result, len(ready))
	c.noteNode(h)
	rctx, rcancel := context.WithCancelCause(h.ctx)
	defer rcancel(nil)
	h.mu.Lock()
	h.roundCancel = rcancel
	if h.timedOut {
		rcancel(errRunTimeout)
	}
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		h.roundCancel = nil
		h.mu.Unlock()
	}()
	if h.rto > 0 {
		tm := c.clk.NewTimer(time.UnixMilli(h.rto).Sub(now))
		go func() {
			select {
			case <-tm.C():
				rcancel(errRunTimeout)
			case <-rctx.Done():
				tm.Stop()
			}
		}()
	}
	for _, s := range ready {
		in := h.input(s, job)
		attempt := h.rec(s.Name).N + 1
		edits := workflow.NewChainEdits(caps, h.chain)
		c.emit(workflow.LifecycleEvent{Kind: workflow.LifecycleStarted, Ref: refOf(h.id, workflow.StatusRunning, h.sid), Workflow: h.wf,
			Queue: h.queue, Tenant: h.tenant, Step: s.Name, Attempt: attempt, Status: workflow.StatusRunning, Labels: h.labels, At: c.clk.Now()})
		go func() {
			c.attemptRunning(1)
			defer c.attemptRunning(-1)
			started := c.clk.Now()
			out, err, withdrawn := c.attempt(rctx, h, s, in, attempt, now, edits)
			results <- result{step: s, in: in, out: out, err: err, started: started, edits: edits, withdrawn: withdrawn}
		}()
	}
	var failed *result
	var discarded *result
	for range ready {
		res := <-results
		if c.isCrashed() || context.Cause(h.ctx) != nil || errors.Is(context.Cause(rctx), errRunTimeout) {
			continue // handed back, cancelled, lost or timed out: the attempt does not count
		}
		rec := h.rec(res.step.Name)
		if rec.First == 0 {
			rec.First = res.started.UnixNano()
		}
		rec.Z = false
		if d, ok := workflow.SnoozeDelay(res.err); ok {
			// Put back for d with the same attempt number: no attempt is
			// spent, counted or recorded, and the retry policy is not asked.
			rec.Due, rec.Z = c.clk.Now().Add(d).UnixNano(), true
			c.save(h, true)
			c.emit(workflow.LifecycleEvent{Kind: workflow.LifecycleSnoozed, Ref: refOf(h.id, workflow.StatusWaiting, h.sid), Workflow: h.wf,
				Queue: h.queue, Tenant: h.tenant, Step: res.step.Name, Attempt: rec.N + 1, Status: workflow.StatusWaiting, Err: res.err, Labels: h.labels, At: c.clk.Now()})
			continue
		}
		h.attempts++
		if res.err == nil {
			rec.Done, rec.Out, rec.Due, rec.Err = true, res.out, 0, ""
			rec.N++
			if res.edits.Changed() {
				h.chain = res.edits.Apply(h.chain) // only a successful attempt's edits count
			}
			c.save(h, true)
			continue
		}
		att := rec.attempts().Count(res.err)
		rec.N, rec.Errors, rec.Err = att.N, att.Errors, res.err.Error()
		if c.errHistory > 0 {
			ae := workflow.NewAttemptError(res.step.Name, att.N, res.err, c.clk.Now())
			ae.Node = c.node
			h.errs = workflow.AppendAttemptError(h.errs, ae, c.errHistory)
		}
		if workflow.IsDiscarded(res.err) {
			discarded = &res
			continue
		}
		if res.withdrawn {
			// CancelStep: the step fails for good, without retry.
			if !errors.Is(res.err, workflow.ErrStepCancelled) {
				res.err = fmt.Errorf("%w: %w", workflow.ErrStepCancelled, res.err)
			}
			failed = &res
			continue
		}
		policy := workflow.EffectiveRetriesFor(res.step, h.reg.wf, job)
		if delay, retry := policy.Next(res.err, att, c.clk.Now(), c.rnd); retry {
			rec.Due = c.clk.Now().Add(delay).UnixNano()
			c.save(h, false)
			c.emit(workflow.LifecycleEvent{Kind: workflow.LifecycleRetrying, Ref: refOf(h.id, workflow.StatusRetrying, h.sid), Workflow: h.wf,
				Queue: h.queue, Tenant: h.tenant, Step: res.step.Name, Attempt: att.N, Status: workflow.StatusRetrying, Err: res.err, Labels: h.labels, At: c.clk.Now()})
			continue
		}
		if derr := policy.ExhaustedError(att.N, res.err); derr != nil {
			res.err = derr
			discarded = &res
			continue
		}
		failed = &res
	}
	if c.gone(h) {
		return true
	}
	if errors.Is(context.Cause(rctx), errRunTimeout) {
		step, in := c.current(h, job)
		c.fail(h, step, in, c.runTimeoutErr(h))
		return true
	}
	switch {
	case discarded != nil:
		c.finish(h, workflow.EndStatus(caps, discarded.err), discarded.step.Name, workflow.DiscardedError(discarded.step.Name, discarded.err))
		return true
	case failed != nil:
		c.fail(h, failed.step.Name, failed.in,
			fmt.Errorf("redis: step %q failed after %d attempt(s): %w", failed.step.Name, h.rec(failed.step.Name).N, failed.err))
		return true
	}
	return false
}

// fail runs the failure handler once with the failing step's input (under
// the lease, so a crash reruns it: at least once), then ends h failed. The
// handler is the one workflow.FailureHandler picks: OnTimeout for a
// timeout when set, else OnFailure.
func (c *Client) fail(h *held, step string, in []byte, cause error) {
	hook := h.reg.onFailure
	if h.reg.onTimeout != nil && workflow.IsTimeout(cause) {
		hook = h.reg.onTimeout
	}
	if hook != nil {
		if _, err := safeCall(hook, c.ctx, in); err != nil {
			c.log.Error("redis: OnFailure handler failed", "workflow", h.wf, "run", h.id, "error", err)
		}
	}
	c.finish(h, workflow.StatusFailed, step, cause)
}

// pastDeadline reports whether h is past its Workflow.RunTimeout.
func (c *Client) pastDeadline(h *held, now time.Time) bool {
	h.mu.Lock()
	told := h.timedOut
	h.mu.Unlock()
	return told || (h.rto > 0 && now.UnixMilli() >= h.rto)
}

// runTimeoutErr is the error of a run past its Workflow.RunTimeout.
func (c *Client) runTimeoutErr(h *held) error {
	return fmt.Errorf("%w: %w", workflow.TimeoutError(workflow.ErrRunTimeout,
		fmt.Sprintf("redis: run %s went past its RunTimeout %s", h.id, h.reg.wf.RunTimeout)), context.DeadlineExceeded)
}

// current is the step a run-level failure is charged to, with its input:
// the first unfinished step whose parents are done, or none and the
// run's payload.
func (c *Client) current(h *held, job workflow.Job) (string, []byte) {
	for _, s := range h.reg.wf.Steps {
		if !h.rec(s.Name).Done && h.parentsDone(s) {
			return s.Name, h.input(s, job)
		}
	}
	return "", job.Payload
}

// attempt runs one step attempt with its timeout and heartbeat deadline
// on the scheduling clock, under parent (the round's ctx). withdrawn
// reports that CancelStep cancelled it.
func (c *Client) attempt(parent context.Context, h *held, s workflow.Step, in []byte, attempt int, readyAt time.Time, edits *workflow.ChainEdits) (out []byte, err error, withdrawn bool) {
	ctx := workflow.WithStepInfo(parent, workflow.StepInfo{
		Workflow: h.wf, Step: s.Name, RunID: workflow.RunID(h.id), Attempt: attempt,
		Labels: h.labels, Queue: h.queue, ReadyAt: readyAt, Tenant: h.tenant, Node: c.node,
	})
	ctx = workflow.WithChainEditor(ctx, edits)
	ctx = workflow.WithProgressReporter(ctx, progressReporter{c: c, h: h, step: s.Name, attempt: attempt})
	defer c.drainTrack.Begin(workflow.DrainCut{RunID: workflow.RunID(h.id), Workflow: h.wf, Step: s.Name, Attempt: attempt})()
	handler := h.reg.handlers[s.Name]
	d := s.Timeout
	if d == 0 {
		d = h.reg.wf.Timeout
	}
	actx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	h.mu.Lock()
	h.live[s.Name] = cancel
	if h.withdrawn[s.Name] {
		cancel(workflow.ErrStepCancelled)
	}
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		delete(h.live, s.Name)
		h.mu.Unlock()
	}()
	beat := make(chan struct{}, 1)
	actx = workflow.WithHeartbeater(actx, workflow.HeartbeatFunc(func(context.Context, []byte) error {
		select {
		case beat <- struct{}{}:
		default:
		}
		return c.renew(h)
	}))
	if d > 0 {
		tm := c.clk.NewTimer(d)
		go func() {
			select {
			case <-tm.C():
				cancel(errStepTimeout)
			case <-actx.Done():
				tm.Stop()
			}
		}()
	}
	if hb := s.HeartbeatTimeout; hb > 0 {
		go func() {
			tm := c.clk.NewTimer(hb)
			for {
				select {
				case <-beat:
					tm.Stop()
					tm = c.clk.NewTimer(hb)
				case <-tm.C():
					cancel(errHeartbeatTimeout)
					return
				case <-actx.Done():
					tm.Stop()
					return
				}
			}
		}()
	}
	out, err = safeCall(handler, actx, in)
	switch cause := context.Cause(actx); {
	case errors.Is(cause, errStepTimeout):
		return nil, fmt.Errorf("%w: %w", workflow.TimeoutError(workflow.ErrStepTimeout,
			fmt.Sprintf("redis: step %q attempt %d after %s", s.Name, attempt, d)), context.DeadlineExceeded), false
	case errors.Is(cause, errHeartbeatTimeout):
		return nil, fmt.Errorf("%w: %w", workflow.TimeoutError(workflow.ErrHeartbeatTimeout,
			fmt.Sprintf("redis: step %q attempt %d sent no heartbeat for %s", s.Name, attempt, s.HeartbeatTimeout)), context.DeadlineExceeded), false
	case errors.Is(cause, workflow.ErrStepCancelled):
		return out, err, true
	}
	return out, err, false
}

// errHeartbeatTimeout is the context cause of an attempt that went
// Step.HeartbeatTimeout without a heartbeat.
var errHeartbeatTimeout = errors.New("redis: step attempt sent no heartbeat in time")

// attemptRunning counts the attempts executing now (NodeInfo.Running).
func (c *Client) attemptRunning(d int) {
	c.mu.Lock()
	c.attemptsRunning += d
	c.mu.Unlock()
}

// renew extends h's lease now, for a heartbeat. It fails with an error
// matching workflow.ErrCancelled when the run is no longer held here.
func (c *Client) renew(h *held) error {
	if c.isCrashed() {
		return nil
	}
	ctx, cancel := c.writeCtx()
	defer cancel()
	res, err := renewScript.Run(ctx, c.rdb, c.keys(), c.base, h.id, h.token, c.leaseClk.Now().Add(c.lease).UnixMilli()).Text()
	switch {
	case err != nil:
		return fmt.Errorf("redis: heartbeat: %w", classify(err))
	case res == "ok":
		return nil
	case res == "lost":
		h.cancel(errLost)
	default:
		h.cancel(workflow.ErrCancelled)
	}
	return fmt.Errorf("%w: the run is no longer held by this worker (%s)", workflow.ErrCancelled, res)
}

// safeCall runs h and turns a panic into an error, so one bad handler
// cannot crash the process that hosts every other run.
func safeCall(h workflow.Handler, ctx context.Context, in []byte) (out []byte, err error) {
	defer func() {
		if p := recover(); p != nil {
			out, err = nil, fmt.Errorf("redis: handler panicked: %v", p)
		}
	}()
	out, err = h(ctx, in)
	if err != nil && errors.Is(context.Cause(ctx), workflow.ErrCancelled) {
		err = fmt.Errorf("%w: %w", workflow.ErrCancelled, err)
	}
	return out, err
}

// heartbeat renews h's lease every third of the lease until stopped. A
// renewal that finds the run cancelled or claimed by another client
// cancels h's context, so its steps stop.
func (c *Client) heartbeat(h *held) (stop func()) {
	quit := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for {
			t := c.leaseClk.NewTimer(c.lease / 3)
			select {
			case <-quit:
				t.Stop()
				return
			case <-t.C():
			}
			if c.isCrashed() {
				continue
			}
			res, err := renewScript.Run(context.WithoutCancel(h.ctx), c.rdb, c.keys(), c.base, h.id, h.token,
				c.leaseClk.Now().Add(c.lease).UnixMilli()).Text()
			switch {
			case err != nil:
				c.log.Error("redis: renew a run lease", "run", h.id, "error", err)
			case res == "lost":
				h.cancel(errLost)
			case res != "ok":
				h.cancel(workflow.ErrCancelled)
			}
		}
	}()
	return func() { close(quit); <-finished }
}

// encodeSteps is the steps JSON of h.
func encodeSteps(h *held) string {
	b, err := json.Marshal(h.steps)
	if err != nil { // a map of plain structs always encodes
		panic(err)
	}
	return string(b)
}

// encodeErrs is the error history JSON of h ("" when empty).
func encodeErrs(h *held) string {
	if len(h.errs) == 0 {
		return ""
	}
	b, err := json.Marshal(h.errs)
	if err != nil { // plain fields always encode
		panic(err)
	}
	return string(b)
}

// writeCtx is the context of a fenced write: it outlives the client's own
// context briefly, so a run is handed back after Start's ctx ended.
func (c *Client) writeCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(c.ctx), releaseTimeout)
}

// save records h's progress; progress resets the reclaim count.
func (c *Client) save(h *held, progress bool) {
	if c.isCrashed() {
		return
	}
	ctx, cancel := c.writeCtx()
	defer cancel()
	p := "0"
	if progress {
		p = "1"
	}
	chain, err := encodeChain(h.chain)
	if err != nil { // checked jobs of plain fields always encode
		c.log.Error("redis: encode a run's chain", "run", h.id, "error", err)
		return
	}
	ok, err := saveScript.Run(ctx, c.rdb, c.keys(), c.base, h.id, h.token, encodeSteps(h), h.attempts, p, encodeErrs(h), chain).Int()
	if err != nil {
		c.log.Error("redis: record a step", "run", h.id, "error", err)
		return
	}
	if ok == 0 {
		h.cancel(errLost)
	}
}

// release hands h back: ready now (zero wake) or at wake.
func (c *Client) release(h *held, wake time.Time, status string) {
	due := int64(0)
	if !wake.IsZero() {
		// Round up: the claim compares whole milliseconds, and a run
		// claimed a fraction of a millisecond before its step is due would
		// be handed straight back, over and over, until the clock moved.
		due = max((wake.UnixNano()+int64(time.Millisecond)-1)/int64(time.Millisecond), 1)
	}
	c.handBack(h, due, status)
	if due != 0 {
		c.pokeDispatcher()
	}
}

// park hands h back with no due time: only a signal resolving one of its
// waits makes it ready again.
func (c *Client) park(h *held, status string) { c.handBack(h, -1, status) }

func (c *Client) handBack(h *held, due int64, status string) {
	if c.isCrashed() {
		return
	}
	ctx, cancel := c.writeCtx()
	defer cancel()
	chain, err := encodeChain(h.chain)
	if err != nil {
		c.log.Error("redis: encode a run's chain", "run", h.id, "error", err)
		return
	}
	if _, err := releaseScript.Run(ctx, c.rdb, c.keys(), c.base, h.id, h.token, encodeSteps(h), h.attempts, due, status, encodeErrs(h), chain, c.clk.Now().UnixMilli()).Result(); err != nil {
		c.log.Error("redis: hand a run back (it runs again once its lease expires)", "run", h.id, "error", err)
	}
}

// finish ends h with st. A succeeded run hands its chain on in the same
// script.
func (c *Client) finish(h *held, st workflow.Status, failedStep string, cause error) {
	if c.isCrashed() {
		return
	}
	ctx, cancel := c.writeCtx()
	defer cancel()
	msg := ""
	if cause != nil {
		msg = cause.Error()
	}
	args := []any{c.base, h.id, h.token, string(st),
		c.clk.Now().UnixNano(), msg, failedStep, encodeSteps(h), h.attempts,
		strconv.FormatInt(c.retention.Milliseconds(), 10), encodeErrs(h)}
	output := ""
	if st == workflow.StatusSucceeded {
		outs := make(map[string][]byte, len(h.steps))
		for name, rec := range h.steps {
			outs[name] = rec.Out
		}
		output = string(workflow.RunOutput(h.reg.wf, outs))
	}
	args = append(args, output)
	if st == workflow.StatusSucceeded {
		next, err := c.chainArgs(h.chain, c.clk.Now())
		if err != nil {
			c.log.Error("redis: hand a chain on (the chain stops here)", "run", h.id, "error", err)
		}
		args = append(args, next...)
	}
	if _, err := finishScript.Run(ctx, c.rdb, c.keys(), args...).Result(); err != nil {
		c.log.Error("redis: record a finished run (it runs again once its lease expires)", "run", h.id, "status", st, "error", err)
		return
	}
	c.drainTrack.RunFinished(workflow.RunID(h.id))
	c.pokeNotifier()
	ev := workflow.LifecycleEvent{Kind: workflow.LifecycleFinished, Ref: refOf(h.id, st, h.sid), Workflow: h.wf, Queue: h.queue,
		Tenant: h.tenant, Status: st, Labels: h.labels, At: c.clk.Now()}
	if st != workflow.StatusSucceeded {
		r := *h.stored
		r.status, r.err, r.failedStep = st, msg, failedStep
		ev.Err = r.runError()
	}
	c.emit(ev)
}

func (c *Client) isCrashed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.crashed
}
