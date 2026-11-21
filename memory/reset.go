package memory

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/ubgo/workflow"
)

var (
	_ workflow.RunResetter     = (*Client)(nil)
	_ workflow.RunResetterWith = (*Client)(nil)
	_ workflow.RunForker       = (*Client)(nil)
)

// ResetRun re-runs a failed run from step: step and every step downstream
// of it run again, the other steps that succeeded keep their outputs. See
// workflow.RunResetter.
func (c *Client) ResetRun(ctx context.Context, ref workflow.RunRef, step string) (workflow.RunRef, error) {
	return c.ResetRunWith(ctx, ref, step, workflow.ResetOptions{})
}

// ResetRunWith is ResetRun that, with opts.Succeeded, also resets a
// succeeded run in place: it gets workflow.LabelRedo and its chain is not
// handed on again. See workflow.RunResetterWith.
func (c *Client) ResetRunWith(ctx context.Context, ref workflow.RunRef, step string, opts workflow.ResetOptions) (workflow.RunRef, error) {
	if step == "" {
		return workflow.RunRef{}, fmt.Errorf("%w: ResetRun needs a step", workflow.ErrInvalid)
	}
	return c.retryFrom(ctx, ref, step, opts)
}

// ForkRun starts a new run from a finished one, at step, with the outputs
// of the steps it keeps; see workflow.RunForker. The fork is a run like
// any other: concurrency and rate limits apply to it, retention forgets
// it, and the original may be forgotten first without touching it. A
// fork's idempotency key held by an unfinished run returns that run's
// RunRef and starts nothing, as Enqueue does.
func (c *Client) ForkRun(ctx context.Context, ref workflow.RunRef, step string, opts workflow.ForkOptions) (workflow.RunRef, error) {
	if err := ctx.Err(); err != nil {
		return workflow.RunRef{}, err
	}
	if err := workflow.CheckFork(step, opts); err != nil {
		return workflow.RunRef{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.usableLocked(); err != nil {
		return workflow.RunRef{}, err
	}
	old, err := c.lookupLocked(ref)
	if err != nil {
		return workflow.RunRef{}, err
	}
	if !workflow.ForkStatusOK(old.status) {
		return workflow.RunRef{}, fmt.Errorf("%w: run %s is %s; only a finished run can be forked", workflow.ErrWrongState, old.id, old.status)
	}
	rerun := workflow.StepsFrom(old.reg.wf, step)
	if rerun == nil {
		return workflow.RunRef{}, fmt.Errorf("%w: workflow %s has no step %q", workflow.ErrInvalid, old.reg.wf.Name, step)
	}
	job := old.job
	job.Labels = workflow.ForkLabels(old.job.Labels, opts.Labels, old.id, step, workflow.ForkRedo(old.status, old.job.Labels))
	job.IdempotencyKey = opts.IdempotencyKey
	job.Chain = nil
	job.RunAt = time.Time{}
	if q := workflow.EffectiveQueue(old.reg.wf, job); !c.servesLocked(q) {
		return workflow.RunRef{}, fmt.Errorf("%w: queue %q is not served by this client (WithQueues); the fork would never start", workflow.ErrInvalid, q)
	}
	c.pruneLocked()
	key := job.IdempotencyKey
	switch old.reg.wf.Idempotency {
	case workflow.IdempotencyOff:
		key = ""
	case workflow.IdempotencyRequired:
		if key == "" {
			return workflow.RunRef{}, fmt.Errorf("%w: workflow %q requires an IdempotencyKey (ForkOptions.IdempotencyKey)", workflow.ErrInvalid, old.reg.wf.Name)
		}
	}
	ik := idemKey{workflow: old.reg.wf.Name, key: key}
	if key != "" {
		if holder, ok := c.idem[ik]; ok {
			workflow.MarkDuplicate(ctx)
			return holder.ref(), nil
		}
	}
	r := c.newRunLocked(old.reg, job)
	for name, out := range old.outputs {
		if !slices.Contains(rerun, name) {
			r.outputs[name] = slices.Clone(out)
		}
	}
	if key != "" {
		r.idem = &ik
		c.idem[ik] = r
	}
	out := c.startRunLocked(r)
	workflow.MarkInserted(ctx)
	c.emitLocked(r, workflow.LifecycleInserted, "", 0, nil)
	return out, nil
}
