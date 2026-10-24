package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/workflow"
)

// attempt is one claimed step execution on this client.
type attempt struct {
	stepID   string
	runID    string
	workflow string
	name     string
	in       []byte
	attempts int64  // executions started, this one included
	failures int64  // failed executions before this one
	reclaims int64  // lease expiries of this attempt, this claim included
	queue    string // the run's queue (counted in Client.running)
	tenant   string // the run's Job.Tenant
	started  int64  // when it was claimed, on the scheduling clock
	readyAt  int64  // when it became claimable (steps.due), StepInfo.ReadyAt
	token    string
	// leaseFrom is the lease clock's time when the lease end now stored
	// was computed (the claim, then each renewal that landed): the holder
	// fences itself a whole lease after it (heartbeat).
	leaseFrom time.Time
	cancel    context.CancelCauseFunc
	edits     *chainEditor // PrependToChain and AppendToChain of this attempt
}

// releaseTimeout bounds the write that hands a step back when the client
// stops; the client's own context is already cancelled by then.
const releaseTimeout = 5 * time.Second

// worker claims and executes step attempts until the client stops.
func (c *Client) worker() {
	defer c.wg.Done()
	for {
		if c.ctx.Err() != nil {
			return
		}
		// The attempt is counted before the claim lands, so a Drain that
		// starts while the claim is in flight waits for it; once draining,
		// nothing more is claimed and the worker waits for the shutdown.
		if !c.beginAttempt() {
			<-c.ctx.Done()
			return
		}
		a, err := c.claim(c.ctx)
		if err != nil && c.ctx.Err() == nil {
			c.log.Error("sqlstore: claim a step", "error", err)
		}
		if a != nil {
			// More work may be waiting: hand the turn to one more idle
			// worker, which claims in parallel with this execution.
			c.signalWorker()
			c.execute(a)
			continue
		}
		c.endAttempt()
		poll := c.leaseClk.NewTimer(c.poll)
		select {
		case <-c.ctx.Done():
		case <-c.work:
		case <-poll.C():
		}
		poll.Stop()
	}
}

// claimWhere selects a step this client may run: one of its workflows,
// either ready and due or running under a lease that expired (its worker
// died), on one of queues (nil: any queue), and held by no pause. The
// order is priority first, then the run's arrival. The step table is
// aliased st.
func (c *Client) claimWhere(queues []any) string {
	q := ""
	if len(queues) > 0 {
		q = " AND " + queueOf + " IN (" + inList(len(queues)) + ")"
	}
	return "st.workflow IN (" + inList(len(c.names)) + ") AND ((st.status IN (?, ?) AND st.due <= ?) OR (st.status = ? AND st.lease_until < ?))" +
		q + c.limitSQL() + " AND " + notPaused + " ORDER BY st.priority DESC, st.run_seq, st.id LIMIT 1"
}

func (c *Client) claimArgs(queues []any) []any {
	args := append([]any(nil), c.names...)
	args = append(args, stepReady, stepSnoozed, c.nowNanos(), stepRunning, c.expiredBefore())
	args = append(args, queues...)
	return append(args, c.limitArgs()...)
}

// claim takes one step and leases it to this client, or returns nil when
// nothing is due. Without WithQueues it claims from every queue at once;
// with it, it walks the queue tiers from the highest priority down, each
// limited to the queues with room under their Workers cap, and takes the
// first step it finds. Claims are serialised on this client while a cap
// exists, so two workers cannot both take the last slot of a queue.
func (c *Client) claim(ctx context.Context) (*attempt, error) {
	if len(c.queues) == 0 {
		return c.claimFrom(ctx, nil)
	}
	if c.capped() {
		c.claimMu.Lock()
		defer c.claimMu.Unlock()
	}
	for _, tier := range c.tiers {
		open := c.openQueues(tier)
		if len(open) == 0 {
			continue
		}
		if !weighted(tier) {
			a, err := c.claimFrom(ctx, open)
			if err != nil || a != nil {
				return a, err
			}
			continue
		}
		a, err := c.claimWeighted(ctx, tier, open)
		if err != nil || a != nil {
			return a, err
		}
	}
	return nil, nil
}

// weighted reports whether a tier sets any QueueConfig.Weight; without one
// its queues are served in one claim, oldest ready work first.
func weighted(tier []workflow.QueueConfig) bool {
	// Index, not range-by-value: copying a QueueConfig reads its Workers,
	// which SetWorkers rewrites under c.mu while this runs without it.
	for i := range tier {
		if tier[i].Weight > 0 {
			return true
		}
	}
	return false
}

// claimWeighted claims from the open queues of a weighted tier: it draws a
// queue by weight (workflow.PickWeighted, on the jitter source) and claims
// from it, drawing again among the rest while the drawn one has nothing
// ready, so a queue with work is never passed over for an empty one.
func (c *Client) claimWeighted(ctx context.Context, tier []workflow.QueueConfig, open []any) (*attempt, error) {
	weight := map[string]int{}
	for i := range tier { // by index: see weighted
		weight[tier[i].Name] = tier[i].Weight
	}
	left := append([]any(nil), open...)
	for len(left) > 0 {
		ws := make([]int, len(left))
		for i, q := range left {
			ws[i] = weight[q.(string)]
		}
		i := workflow.PickWeighted(ws, c.draw())
		a, err := c.claimFrom(ctx, left[i:i+1])
		if err != nil || a != nil {
			return a, err
		}
		left = append(left[:i], left[i+1:]...)
	}
	return nil, nil
}

// claimFrom claims one step of queues (nil: any). Postgres and MySQL lock
// the candidate row with FOR UPDATE SKIP LOCKED, so concurrent claimers
// pass over each other instead of queueing; SQLite, which has one writer,
// claims with a single conditional UPDATE whose subquery and update cannot
// interleave with another writer.
func (c *Client) claimFrom(ctx context.Context, queues []any) (*attempt, error) {
	token := randomID("lease")
	leaseFrom := c.leaseClk.Now()
	until := leaseFrom.UnixNano() + int64(c.lease)
	// reclaims is assigned first: MySQL evaluates SET left to right with
	// the new values, and the CASE must see the status before the claim
	// (running means the previous holder's lease expired).
	set := "UPDATE {steps} SET reclaims = reclaims + CASE WHEN status = ? THEN 1 ELSE 0 END, status = ?, lease_token = ?, lease_until = ?, attempts = attempts + 1 WHERE id = "
	// The claim and the read of the claimed step are one transaction: a
	// read that failed after a committed claim would leave the step leased
	// to nobody until its lease expired, then count that as a reclaim.
	var a *attempt
	err := c.st.tx(ctx, func(tx *sql.Tx) error {
		a = nil
		if err := c.lockLimits(ctx, tx); err != nil {
			return err
		}
		if c.st.engine == engineSQLite {
			args := append([]any{stepRunning, stepRunning, token, until}, c.claimArgs(queues)...)
			n, err := c.st.execN(ctx, tx, set+"(SELECT st.id FROM {steps} st WHERE "+c.claimWhere(queues)+")", args...)
			if err != nil || n == 0 {
				return err
			}
		} else {
			var id string
			err := c.st.one(ctx, tx, "SELECT st.id FROM {steps} st WHERE "+c.claimWhere(queues)+" FOR UPDATE SKIP LOCKED", c.claimArgs(queues), &id)
			if errors.Is(err, sqlb.ErrNoRows) {
				return nil
			}
			if err != nil {
				return err
			}
			if _, err := c.st.exec(ctx, tx, set+"?", stepRunning, stepRunning, token, until, id); err != nil {
				return err
			}
		}
		cand := &attempt{token: token, started: c.nowNanos(), leaseFrom: leaseFrom}
		var tenant, nodes sql.NullString
		err := c.st.one(ctx, tx, "SELECT st.id, st.run_id, st.workflow, st.name, st.in_data, st.attempts, st.failures, st.reclaims, st.due, "+queueOf+
			", i.tenant, i.nodes FROM {steps} st LEFT JOIN {run_info} i ON i.run_id = st.run_id WHERE st.lease_token = ?",
			[]any{token}, &cand.stepID, &cand.runID, &cand.workflow, &cand.name, &cand.in, &cand.attempts, &cand.failures, &cand.reclaims, &cand.readyAt, &cand.queue,
			&tenant, &nodes)
		if err != nil {
			return fmt.Errorf("sqlstore: read claimed step: %w", err)
		}
		cand.tenant = tenant.String
		if nodes.Valid {
			if err := c.addRunNode(ctx, tx, cand.runID, nodes.String); err != nil {
				return err
			}
		}
		a = cand
		return nil
	})
	if err != nil || a == nil {
		return nil, err
	}
	c.countRunning(a.queue, 1)
	return a, nil
}

// execute runs one attempt: the handler under a renewed lease, then its
// result recorded, or the step handed back when the client is stopping.
func (c *Client) execute(a *attempt) {
	defer c.endAttempt()
	// Tracked from the first instant, so a drain whose deadline ends now
	// still lists this attempt as cut off.
	defer c.drainTrack.Begin(workflow.DrainCut{RunID: workflow.RunID(a.runID), Workflow: a.workflow, Step: a.name, Attempt: int(a.attempts)})()
	actx, cancel := context.WithCancelCause(c.ctx)
	defer cancel(nil)
	a.cancel = cancel
	c.addLocal(a)
	done := make(chan struct{})
	c.wg.Add(1)
	// The heartbeat starts before anything else touches the database: on a
	// busy database the writes and reads below can wait on its lock for
	// longer than the lease, and an unrenewed lease would hand the step to
	// another live worker while this one is about to run it.
	go c.heartbeat(a, done)

	// The lease is renewed until the outcome is written, not only while the
	// handler runs: a record waiting on a busy database must not let the
	// lease expire and hand the finished step to another client.
	defer close(done)

	if a.attempts == 1 {
		// The run's first attempt, as far as this step knows: record the
		// run's start once (RunInfo.StartedAt). A miss only leaves the time
		// unset, so the error is logged, not returned.
		if _, err := c.st.exec(c.ctx, c.st.db, "UPDATE {run_ext} SET started_at = ? WHERE run_id = ? AND started_at = 0", a.started, a.runID); err != nil {
			c.log.Error("sqlstore: record a run's start time", "run", a.runID, "error", err)
		}
	}
	reg := c.reg(a.workflow)
	var labels map[string]string
	var raw string
	if err := c.st.one(c.ctx, c.st.db, "SELECT labels FROM {runs} WHERE id = ?", []any{a.runID}, &raw); err == nil {
		r := runRow{id: a.runID, labels: raw}
		var lerr error
		if labels, lerr = r.labelMap(); lerr != nil {
			c.log.Error("sqlstore: step runs without labels", "run", a.runID, "error", lerr)
		}
	}

	step := reg.steps[a.name]
	c.emit(c.ctx, workflow.LifecycleEvent{Kind: workflow.LifecycleStarted, Ref: workflow.RunRef{RunID: workflow.RunID(a.runID)}, Workflow: a.workflow,
		Queue: a.queue, Tenant: a.tenant, Step: a.name, Attempt: int(a.attempts), Status: workflow.StatusRunning, Labels: labels})
	var out []byte
	var herr error
	if a.reclaims > int64(c.maxReclaims) {
		// A poison step (one that kills its worker every time) would
		// otherwise be passed from worker to worker forever.
		herr = fmt.Errorf("sqlstore: step %q abandoned by %d workers (its lease expired %d times, over WithMaxReclaims %d): %w",
			a.name, a.reclaims, a.reclaims, c.maxReclaims, ErrAbandoned)
	} else {
		out, herr = c.call(actx, reg, step, a, labels)
	}
	c.removeLocal(a)
	c.countRunning(a.queue, -1)

	if c.ctx.Err() != nil {
		if !c.crashed.Load() {
			c.release(a)
		}
		return
	}
	if errors.Is(context.Cause(actx), ErrLeaseLost) {
		// Another client may hold the step now; writing an outcome could
		// overwrite its. The step is retaken once its lease expires.
		return
	}
	if errors.Is(context.Cause(actx), workflow.ErrStepCancelled) {
		herr = cancelledOutcome(herr)
	}
	c.record(a, reg, step, out, herr)
	c.recycleAfterAttempt()
}

// call runs the step handler with StepInfo on its context and its timeout
// on the scheduling clock.
func (c *Client) call(ctx context.Context, reg *registered, step workflow.Step, a *attempt, labels map[string]string) ([]byte, error) {
	h := reg.handlers[a.name]
	if h == nil {
		return nil, workflow.Permanent(fmt.Errorf("sqlstore: step %q of workflow %q has no handler", a.name, a.workflow))
	}
	ctx = workflow.WithStepInfo(ctx, workflow.StepInfo{
		Workflow: a.workflow,
		Step:     a.name,
		RunID:    workflow.RunID(a.runID),
		Queue:    a.queue,
		ReadyAt:  unixTime(a.readyAt),
		Attempt:  int(a.attempts),
		Labels:   labels,
		Tenant:   a.tenant,
		Node:     c.nodeID,
	})
	ctx = workflow.WithProgressReporter(ctx, c.progressReporter(a))
	a.edits = c.newChainEditor(a.runID)
	ctx = workflow.WithChainEditor(ctx, a.edits)
	if hb := step.HeartbeatTimeout; hb > 0 {
		var stop func()
		ctx, stop = c.watchHeartbeat(ctx, a, hb)
		defer stop()
		out, err := c.callTimed(ctx, h, reg, step, a)
		if errors.Is(context.Cause(ctx), errHeartbeatTimeout) {
			return nil, workflow.TimeoutError(workflow.ErrHeartbeatTimeout,
				fmt.Sprintf("step %q attempt %d sent no heartbeat for %s", a.name, a.attempts, hb))
		}
		return out, err
	}
	return c.callTimed(ctx, h, reg, step, a)
}

// callTimed runs h under the step's Timeout (or the workflow's), on the
// scheduling clock.
func (c *Client) callTimed(ctx context.Context, h workflow.Handler, reg *registered, step workflow.Step, a *attempt) ([]byte, error) {
	d := step.Timeout
	if d == 0 {
		d = reg.wf.Timeout
	}
	if d <= 0 {
		return safeCall(h, ctx, a.in)
	}
	actx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
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
	out, err := safeCall(h, actx, a.in)
	if errors.Is(context.Cause(actx), errStepTimeout) {
		// It matches workflow.ErrStepTimeout (and ErrTimeout, which picks
		// OnTimeout) as well as context.DeadlineExceeded, as before.
		return nil, fmt.Errorf("sqlstore: step %q attempt %d timed out after %s: %w: %w", a.name, a.attempts, d,
			workflow.TimeoutError(workflow.ErrStepTimeout, ""), context.DeadlineExceeded)
	}
	return out, err
}

// ErrAbandoned is wrapped by the error of an attempt that failed because
// its lease expired more than WithMaxReclaims times: every worker that took
// it died before finishing. The retry policy treats it like any failure.
var ErrAbandoned = errors.New("sqlstore: step abandoned by its workers")

// errStepTimeout is the context cause of an attempt that ran past its
// timeout.
var errStepTimeout = errors.New("sqlstore: step attempt timed out")

// errHeartbeatTimeout is the context cause of an attempt that sent no
// heartbeat for its step's HeartbeatTimeout.
var errHeartbeatTimeout = errors.New("sqlstore: step heartbeat timed out")

// watchHeartbeat gives a's handler a workflow.Heartbeater and cancels it
// with errHeartbeatTimeout when no beat came for hb on the scheduling
// clock (from the start, then from each beat). A beat also renews the
// lease at once; a beat for a step that is no longer this attempt's returns
// an error matching workflow.ErrCancelled. stop ends the watch.
func (c *Client) watchHeartbeat(ctx context.Context, a *attempt, hb time.Duration) (context.Context, func()) {
	actx, cancel := context.WithCancelCause(ctx)
	// lastBeat is the scheduling clock's time of the latest beat (the start
	// counts as one). The watch sleeps until a heartbeat timeout after it
	// and fails the attempt only if no beat came meanwhile, so a beat is
	// never lost to a watch that has not woken yet.
	var lastBeat atomic.Int64
	lastBeat.Store(c.clk.Now().UnixNano())
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		wait := hb
		for {
			t := c.clk.NewTimer(wait)
			select {
			case <-t.C():
			case <-actx.Done():
				t.Stop()
				return
			}
			since := time.Duration(c.clk.Now().UnixNano() - lastBeat.Load())
			if since >= hb {
				cancel(errHeartbeatTimeout)
				return
			}
			wait = hb - since
		}
	}()
	var mu sync.Mutex
	wrote := c.leaseClk.Now() // when the lease was last renewed for a beat; the claim counts
	var lost atomic.Bool      // a beat's renewal found the step taken from this attempt
	beat := workflow.HeartbeatFunc(func(context.Context, []byte) error {
		if lost.Load() {
			return fmt.Errorf("%w: step %q is no longer this attempt's", workflow.ErrCancelled, a.name)
		}
		if actx.Err() != nil {
			return fmt.Errorf("%w: the attempt has ended", workflow.ErrCancelled)
		}
		// The beat is the handler's proof of life whatever the write below
		// costs, so the watch is reset first.
		lastBeat.Store(c.clk.Now().UnixNano())
		// The lease write is spaced like the heartbeat goroutine's renewals:
		// a handler beating in a tight loop must not queue on the database.
		// It runs beside the handler, which never waits on the database to
		// beat; a lost lease is reported by the next beat.
		mu.Lock()
		now := c.leaseClk.Now()
		due := now.Sub(wrote) >= c.lease/3
		if due {
			wrote = now
		}
		mu.Unlock()
		if !due {
			return nil
		}
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			n, err := c.st.execN(actx, c.st.db, "UPDATE {steps} SET lease_until = ? WHERE id = ? AND lease_token = ? AND status = ?",
				now.UnixNano()+int64(c.lease), a.stepID, a.token, stepRunning)
			if err == nil && n == 0 {
				lost.Store(true)
			}
		}()
		return nil
	})
	return workflow.WithHeartbeater(actx, beat), func() { cancel(nil) }
}

// ErrLeaseLost is the context cause of an attempt whose lease went
// unrenewed for a whole lease (the database was unreachable or too slow):
// from then on another client may take the step over, so this one stops.
// Its outcome is not written; the step runs again once the lease expires
// for the other clients (WithLeaseSkew later), and the takeover counts
// toward WithMaxReclaims. A handler can tell it apart with context.Cause.
var ErrLeaseLost = errors.New("sqlstore: step lease lost")

// heartbeat renews a's lease every third of the lease until done closes.
// A renewal that changes no row means the step is no longer this
// attempt's: its run was cancelled or failed elsewhere, or the lease
// expired and another client took it. The handler's context is then
// cancelled, so a run cancelled on another node stops here too.
//
// The holder also fences itself: when a whole lease has passed on the
// lease clock since the stored lease end was computed (a.leaseFrom)
// without a renewal landing, other clients may already see the lease
// expired, so the attempt is cancelled with ErrLeaseLost. The elapsed time
// is measured on the lease clock, which on clock.Real and DatabaseClock is
// monotonic: a wall-clock jump on this node neither fences early nor late.
func (c *Client) heartbeat(a *attempt, done <-chan struct{}) {
	defer c.wg.Done()
	for {
		wait := c.lease / 3
		if left := c.lease - c.leaseClk.Now().Sub(a.leaseFrom); left < wait {
			wait = left
		}
		if wait <= 0 {
			c.log.Error("sqlstore: a step lease went unrenewed for a whole lease; stopping the attempt", "run", a.runID, "step", a.name)
			a.cancel(ErrLeaseLost)
			return
		}
		t := c.leaseClk.NewTimer(wait)
		select {
		case <-done:
			t.Stop()
			return
		case <-c.ctx.Done():
			t.Stop()
			return
		case <-t.C():
		}
		if c.lease-c.leaseClk.Now().Sub(a.leaseFrom) > c.lease*2/3 {
			continue // woken early for the fence check only: not due for renewal yet
		}
		from := c.leaseClk.Now()
		n, err, fenced := c.renew(a, from)
		if fenced {
			c.log.Error("sqlstore: a step lease went unrenewed for a whole lease; stopping the attempt", "run", a.runID, "step", a.name)
			a.cancel(ErrLeaseLost)
			return
		}
		if err != nil {
			if c.ctx.Err() == nil {
				c.log.Error("sqlstore: renew a step lease", "run", a.runID, "step", a.name, "error", err)
			}
			continue
		}
		if n == 0 {
			a.cancel(workflow.ErrCancelled)
			return
		}
		a.leaseFrom = from
		if stop, err := c.stepCancelRequested(c.ctx, a); err != nil {
			if c.ctx.Err() == nil {
				c.log.Error("sqlstore: read a step cancel request", "run", a.runID, "step", a.name, "error", err)
			}
		} else if stop {
			a.cancel(workflow.ErrStepCancelled)
		}
	}
}

// renew writes a's new lease end, from + the lease, racing the fence: a
// write still waiting on a lock when the current lease ends (a.leaseFrom
// plus the lease) reports fenced, because a busy wait inside the driver
// (SQLite's busy_timeout) does not always return when its context ends.
// The write is cancelled then and finishes on its own goroutine.
func (c *Client) renew(a *attempt, from time.Time) (n int64, err error, fenced bool) {
	left := c.lease - from.Sub(a.leaseFrom)
	rctx, rcancel := context.WithTimeout(c.ctx, left)
	defer rcancel()
	type result struct {
		n   int64
		err error
	}
	res := make(chan result, 1)
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		n, err := c.st.execN(rctx, c.st.db, "UPDATE {steps} SET lease_until = ? WHERE id = ? AND lease_token = ? AND status = ?",
			from.UnixNano()+int64(c.lease), a.stepID, a.token, stepRunning)
		res <- result{n, err}
	}()
	fence := c.leaseClk.NewTimer(left)
	defer fence.Stop()
	select {
	case r := <-res:
		return r.n, r.err, false
	case <-fence.C():
		return 0, nil, true
	}
}

// release hands a's step back as ready when the client stops mid-attempt,
// so another client runs it at once instead of after the lease. The
// interrupted execution does not count as an attempt.
func (c *Client) release(a *attempt) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(c.ctx), releaseTimeout)
	defer cancel()
	if _, err := c.st.exec(ctx, c.st.db, `UPDATE {steps} SET status = ?, lease_token = NULL, lease_until = 0, attempts = attempts - 1
		WHERE id = ? AND lease_token = ? AND status = ?`, stepReady, a.stepID, a.token, stepRunning); err != nil {
		c.log.Error("sqlstore: release a step on stop (it runs again once its lease expires)", "run", a.runID, "step", a.name, "error", err)
	}
}

// record writes an attempt's outcome, if the step is still this attempt's
// and the run still active: success completes the step, a Discard ends the
// run cancelled, a retryable error requeues the step after the policy's
// backoff (recordFailure), anything else fails the run.
func (c *Client) record(a *attempt, reg *registered, step workflow.Step, out []byte, herr error) {
	ctx := c.ctx
	var o txOut
	err := c.st.tx(ctx, func(tx *sql.Tx) error {
		o = txOut{}
		row, err := c.touch(ctx, tx, a.runID)
		if errors.Is(err, workflow.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if row.status != runActive {
			return nil
		}
		var mine int64
		if err := c.st.one(ctx, tx, "SELECT COUNT(*) FROM {steps} WHERE id = ? AND lease_token = ? AND status = ?",
			[]any{a.stepID, a.token, stepRunning}, &mine); err != nil {
			return err
		}
		if mine == 0 {
			return nil
		}
		if _, err := c.st.exec(ctx, tx, "DELETE FROM {step_cancel} WHERE step_id = ?", a.stepID); err != nil {
			return err
		}
		if herr == nil {
			if err := c.applyChainEdits(ctx, tx, a); err != nil {
				return err
			}
			return c.completeStep(ctx, tx, reg, row, a.name, out, &o)
		}
		if d, ok := workflow.SnoozeDelay(herr); ok {
			return c.snoozeStep(ctx, tx, a, d, herr, &o)
		}
		if workflow.IsDiscarded(herr) {
			if err := c.addAttemptError(ctx, tx, a, herr); err != nil {
				return err
			}
			return c.discardRun(ctx, tx, row, a.name, herr, &o)
		}
		return c.recordFailure(ctx, tx, reg, row, step, a, herr, &o)
	})
	if err != nil {
		// The step stays claimed and runs again once its lease expires.
		if ctx.Err() == nil {
			c.log.Error("sqlstore: record a step result", "run", a.runID, "step", a.name, "error", err)
		}
		return
	}
	c.afterTx(ctx, o)
}

// addLocal and removeLocal track the attempts running here, so a Cancel or
// a failure on this client stops them at once rather than at the next
// lease renewal.
func (c *Client) addLocal(a *attempt) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.local[a.runID] == nil {
		c.local[a.runID] = map[*attempt]bool{}
	}
	c.local[a.runID][a] = true
}

func (c *Client) removeLocal(a *attempt) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.local[a.runID], a)
	if len(c.local[a.runID]) == 0 {
		delete(c.local, a.runID)
	}
}

// cancelLocal cancels every attempt of run runID executing on this client
// with cause (nil: workflow.ErrCancelled).
func (c *Client) cancelLocal(runID string, cause error) {
	if cause == nil {
		cause = workflow.ErrCancelled
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for a := range c.local[runID] {
		a.cancel(cause)
	}
}
