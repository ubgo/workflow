package memory

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/ubgo/workflow"
)

// WithNodeID names this client's node in StepInfo.Node, AttemptError.Node,
// RunInfo.AttemptedBy and Nodes (workflow.CapNodeIdentity). The default is
// workflow.DefaultNodeID. An empty id is ignored.
func WithNodeID(id string) Option {
	return func(m *Client) {
		if id != "" {
			m.node = id
		}
	}
}

// WithPayloadLimit refuses at Enqueue a job whose payload (or a chained
// job's) is over n bytes, with a *workflow.PayloadTooLargeError, so a test
// on this driver catches a payload a production engine would reject.
// Zero or less means no limit, the default.
func WithPayloadLimit(n int) Option { return func(m *Client) { m.payloadLimit = max(n, 0) } }

// nodeID is the node identity: WithNodeID's, else the default New set.
func (c *Client) nodeID() string { return c.node }

// PayloadLimit reports the WithPayloadLimit cap (workflow.PayloadLimiter);
// zero means none.
func (c *Client) PayloadLimit() int { return c.payloadLimit }

// OnLifecycle registers fn to observe every run's lifecycle (see
// workflow.LifecycleNotifier). Lifecycle observers share the run-finished
// observers' goroutine: in registration order, in the order things
// happened, never while the client's lock is held.
func (c *Client) OnLifecycle(fn workflow.LifecycleFunc) error {
	if fn == nil {
		return fmt.Errorf("%w: OnLifecycle with a nil func", workflow.ErrInvalid)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return workflow.ErrAlreadyStarted
	}
	c.lifecycle = append(c.lifecycle, fn)
	return nil
}

// emitLocked queues a lifecycle event of r for the observers. Callers hold
// c.mu.
func (c *Client) emitLocked(r *run, kind workflow.LifecycleKind, step string, attempt int, err error) {
	if len(c.lifecycle) == 0 {
		return
	}
	ev := workflow.LifecycleEvent{
		Kind:     kind,
		Ref:      r.ref(),
		Workflow: r.reg.wf.Name,
		Queue:    r.queue,
		Tenant:   r.job.Tenant,
		Step:     step,
		Attempt:  attempt,
		Status:   r.statusLocked(),
		Err:      err,
		Node:     c.nodeID(),
		Labels:   maps.Clone(r.job.Labels),
		At:       c.clk.Now(),
	}
	if kind == workflow.LifecycleProgress && r.progress != nil {
		p := r.progress.Clone()
		ev.Progress = &p
	}
	c.ended = append(c.ended, notice{life: &ev})
	c.pokeNotifier()
}

// observeLife calls one lifecycle observer, recovering a panic.
func (c *Client) observeLife(fn workflow.LifecycleFunc, ev workflow.LifecycleEvent) {
	defer func() {
		if p := recover(); p != nil {
			c.log.Error("memory: lifecycle observer panicked", "workflow", ev.Workflow, "run", ev.Ref.RunID, "panic", p)
		}
	}()
	fn(c.ctx, ev)
}

// reportProgress keeps p as t's run's progress (workflow.ReportProgress).
func (c *Client) reportProgress(t *task, p workflow.Progress) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if t.r.status.IsTerminal() {
		return fmt.Errorf("%w: the run has ended", workflow.ErrCancelled)
	}
	p = p.Clone()
	p.Step, p.At = t.step.Name, c.clk.Now()
	t.r.progress = &p
	c.emitLocked(t.r, workflow.LifecycleProgress, t.step.Name, t.attempt, nil)
	return nil
}

// watchHeartbeat returns the attempt's Heartbeater. With a HeartbeatTimeout
// d it also starts a watchdog that cancels the attempt with errHeartbeat
// when d passes without a beat; each beat restarts the wait. Without one a
// beat does nothing (this driver has no lease to extend).
func (c *Client) watchHeartbeat(ctx context.Context, cancel context.CancelCauseFunc, d time.Duration) workflow.Heartbeater {
	if d <= 0 {
		return workflow.HeartbeatFunc(func(context.Context, []byte) error { return nil })
	}
	beats := make(chan struct{}, 1)
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			tm := c.clk.NewTimer(d)
			select {
			case <-tm.C():
				cancel(errHeartbeat)
				return
			case <-beats:
				tm.Stop()
			case <-ctx.Done():
				tm.Stop()
				return
			}
		}
	}()
	return workflow.HeartbeatFunc(func(context.Context, []byte) error {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("%w: the attempt has ended", workflow.ErrCancelled)
		}
		select {
		case beats <- struct{}{}:
		default: // a beat is already pending; one is enough
		}
		return nil
	})
}

// armRunTimeoutLocked starts r's Workflow.RunTimeout clock at admission;
// when it fires, the coordinator fails the run with ErrRunTimeout.
// Callers hold c.mu.
func (c *Client) armRunTimeoutLocked(r *run) {
	d := r.reg.wf.RunTimeout
	if d <= 0 {
		return
	}
	tm := c.clk.NewTimer(d)
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		select {
		case <-tm.C():
			c.send(r, runEvent{kind: evRunTimeout})
		case <-r.ctx.Done():
			tm.Stop()
		}
	}()
}

// Reschedule moves a delayed run that has not started to at (see
// workflow.RunRescheduler).
func (c *Client) Reschedule(ctx context.Context, ref workflow.RunRef, at time.Time) (workflow.RunRef, error) {
	if err := ctx.Err(); err != nil {
		return workflow.RunRef{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.usableLocked(); err != nil {
		return workflow.RunRef{}, err
	}
	r, err := c.lookupLocked(ref)
	if err != nil {
		return workflow.RunRef{}, err
	}
	if r.status != workflow.StatusScheduled {
		return workflow.RunRef{}, fmt.Errorf("%w: run %+v is %s, not scheduled", workflow.ErrWrongState, ref, r.statusLocked())
	}
	now := c.clk.Now()
	if at.IsZero() || !at.After(now) {
		at = now
	}
	r.runAt = at
	c.armDelayLocked(r, at.Sub(now))
	return r.ref(), nil
}

// CancelStep withdraws one step of a run (see workflow.StepCanceller).
func (c *Client) CancelStep(ctx context.Context, ref workflow.RunRef, step string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	r, err := c.lookupLocked(ref)
	if err != nil {
		return err
	}
	if _, ok := r.reg.steps[step]; !ok {
		return fmt.Errorf("%w: run %+v has no step %q", workflow.ErrNotFound, ref, step)
	}
	if r.status.IsTerminal() {
		return fmt.Errorf("%w: run %+v has ended", workflow.ErrWrongState, ref)
	}
	if cancel, ok := r.running[step]; ok {
		if r.cancelled == nil {
			r.cancelled = map[string]bool{}
		}
		r.cancelled[step] = true
		cancel(workflow.ErrStepCancelled)
		return nil
	}
	if r.parked[step] {
		delete(r.parked, step)
		c.wg.Add(1)
		go func() { // never send to the coordinator under the lock
			defer c.wg.Done()
			c.send(r, runEvent{step: step, kind: evStepCancel})
		}()
		return nil
	}
	return fmt.Errorf("%w: step %q of run %+v is not running", workflow.ErrWrongState, step, ref)
}

// Result returns a succeeded run's result (see workflow.ResultReader).
func (c *Client) Result(ctx context.Context, ref workflow.RunRef) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	r, err := c.lookupLocked(ref)
	if err != nil {
		return nil, err
	}
	if st := r.statusLocked(); st != workflow.StatusSucceeded {
		return nil, fmt.Errorf("%w: run %+v is %s, not succeeded", workflow.ErrWrongState, ref, st)
	}
	return slices.Clone(r.output), nil
}

// Nodes reports this client as the one node there is (see
// workflow.NodeLister).
func (c *Client) Nodes(ctx context.Context) ([]workflow.NodeInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.started {
		return nil, nil
	}
	n := workflow.NodeInfo{
		ID:        c.nodeID(),
		Mode:      workflow.NodeFull,
		Workers:   c.workers,
		Running:   c.executing,
		StartedAt: c.startedAt,
		LastSeen:  c.clk.Now(),
	}
	for _, q := range c.queueCfg {
		n.Queues = append(n.Queues, q.Name)
	}
	slices.Sort(n.Queues)
	return []workflow.NodeInfo{n}, nil
}

// Health reports the client live unless it was stopped, and ready while
// it is started and neither draining nor stopped (see
// workflow.HealthChecker). There is no backend to reach.
func (c *Client) Health(context.Context) workflow.HealthReport {
	c.mu.Lock()
	defer c.mu.Unlock()
	workers := workflow.HealthCheck{Name: "workers", OK: c.started && !c.stopped && !c.draining}
	switch {
	case !c.started:
		workers.Detail = "not started"
	case c.stopped:
		workers.Detail = "stopped"
	case c.draining:
		workers.Detail = "draining"
	default:
		workers.Detail = fmt.Sprintf("%d of %d busy", c.executing, c.workers)
	}
	return workflow.HealthReport{Live: !c.stopped, Ready: workers.OK, Checks: []workflow.HealthCheck{workers}}
}
