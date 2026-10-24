package redis

import (
	"context"
	"fmt"
	"strings"

	"github.com/ubgo/workflow"
)

// Run ends. Every script that ends a run adds its ID to
// {base}:ended:<workflow> (score 0). A client with OnRunFinished observers
// claims ends of the workflows it registered under a lease (the score
// becomes the lease end), calls every observer, then removes the entry, so
// a client that dies mid-delivery leaves the end to another: at least
// once, including ends that happened while no client was running. Each end
// is delivered on one client, not on every client. Ends wait for an
// observer as long as their run exists; a client's housekeeping drops ends
// whose run its retention expired.

// OnRunFinished registers fn to observe run ends (workflow.RunNotifier),
// with the guaranteed delivery CapRunFinished promises. It returns
// workflow.ErrAlreadyStarted after Start.
func (c *Client) OnRunFinished(fn workflow.RunFinishedFunc) error {
	if fn == nil {
		return fmt.Errorf("%w: OnRunFinished needs a function", workflow.ErrInvalid)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return workflow.ErrAlreadyStarted
	}
	c.observers = append(c.observers, fn)
	return nil
}

// notifier delivers run ends while the client runs: on a poke after this
// client ended a run, on a wake-up from another client, and every poll.
func (c *Client) notifier() {
	defer c.wg.Done()
	c.mu.Lock()
	n := len(c.observers)
	c.mu.Unlock()
	if n == 0 {
		return
	}
	for {
		if !c.isCrashed() {
			if err := c.deliver(c.ctx); err != nil && c.ctx.Err() == nil {
				c.log.Error("redis: deliver run ends", "error", err)
			}
		}
		t := c.leaseClk.NewTimer(c.poll)
		select {
		case <-c.ctx.Done():
			t.Stop()
			return
		case <-c.notifyCh:
			t.Stop()
		case <-t.C():
		}
	}
}

// deliver claims a batch of undelivered ends and hands each to every
// observer, then marks it delivered.
func (c *Client) deliver(ctx context.Context) error {
	c.mu.Lock()
	obs := c.observers
	c.mu.Unlock()
	until := c.leaseClk.Now().Add(c.lease).UnixMilli()
	args := []any{c.base, c.leaseClk.Now().UnixMilli(), until, deliverBatch}
	for _, n := range c.names {
		args = append(args, n)
	}
	res, err := deliverScript.Run(ctx, c.rdb, c.keys(), args...).Slice()
	if err != nil {
		return classify(err)
	}
	for i := 0; i+2 < len(res); i += 3 {
		wf, id := str(res[i]), str(res[i+1])
		flat, _ := res[i+2].([]any)
		h := make(map[string]string, len(flat)/2)
		for j := 0; j+1 < len(flat); j += 2 {
			h[str(flat[j])] = str(flat[j+1])
		}
		r, err := decodeRun(id, h)
		if err != nil {
			c.log.Error("redis: decode an ended run (its end is not delivered)", "run", id, "error", err)
			continue
		}
		ev := workflow.RunEvent{
			Ref:        workflow.RunRef{RunID: workflow.RunID(id), ScheduledID: workflow.ScheduledID(r.sid)},
			Workflow:   wf,
			Status:     r.status,
			Err:        r.runError(),
			Labels:     r.labels,
			Tenant:     r.tenant,
			FinishedAt: r.finished,
		}
		if r.status == workflow.StatusFailed {
			ev.Step = r.failedStep
		}
		for _, fn := range obs {
			c.observe(ctx, fn, ev)
		}
		if err := ackScript.Run(ctx, c.rdb, c.keys(), c.base, wf, id, until).Err(); err != nil {
			return classify(err)
		}
	}
	return nil
}

// observe calls fn, recovering a panic so an observer can never change a
// run's status or stop delivery.
func (c *Client) observe(ctx context.Context, fn workflow.RunFinishedFunc, ev workflow.RunEvent) {
	defer func() {
		if p := recover(); p != nil {
			c.log.Error("redis: run-finished observer panicked", "run", ev.Ref.RunID, "panic", fmt.Sprint(p))
		}
	}()
	fn(ctx, ev)
}

// storedError is a failed run's error rebuilt from its stored message.
type storedError string

func (e storedError) Error() string { return string(e) }

// discardedError is a discarded run's error rebuilt from its stored
// message: it matches workflow.ErrDiscarded and workflow.ErrCancelled, as
// workflow.DiscardedError does before it is stored.
type discardedError string

func (e discardedError) Error() string { return string(e) }

// Is makes errors.Is match both sentinels a discard carries.
func (e discardedError) Is(target error) bool {
	return target == workflow.ErrDiscarded || target == workflow.ErrCancelled
}

// runError is a finished run's Err as observers see it, rebuilt from its
// stored status and message. A failed run's error is a
// *workflow.RunError naming the run, workflow, failed step and its last
// attempt, around the stored message (a ForceFail's rebuilt to match
// workflow.ErrForced again).
func (r *stored) runError() error {
	msg := r.err
	switch r.status {
	case workflow.StatusFailed:
		var cause error = storedError(msg)
		if r.forced {
			rest := strings.TrimPrefix(msg, workflow.ErrForced.Error())
			cause = workflow.ForcedFailure(strings.TrimPrefix(rest, ": "))
		}
		re := &workflow.RunError{RunID: workflow.RunID(r.id), Workflow: r.wf, Step: r.failedStep, Err: cause}
		if s := r.steps[r.failedStep]; s != nil && r.failedStep != "" {
			re.Attempt = s.N
		}
		return re
	case workflow.StatusDiscarded:
		return discardedError(msg)
	case workflow.StatusCancelled:
		return fmt.Errorf("%w: %s", workflow.ErrCancelled, msg)
	}
	return nil
}

var _ workflow.RunNotifier = (*Client)(nil)
