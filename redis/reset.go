package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/ubgo/workflow"
)

var (
	_ workflow.RunResetter     = (*Client)(nil)
	_ workflow.RunResetterWith = (*Client)(nil)
	_ workflow.RunForker       = (*Client)(nil)
)

// ResetRun re-runs a failed run from step: step and every step downstream
// of it start again with the steps that had not succeeded, the other steps
// keep their outputs (workflow.RunResetter). The client must have the
// run's workflow registered to know which steps are downstream; otherwise
// it returns an error wrapping ErrInvalid.
//
//ds:def id=redis-client-resetrun-ax2tev3t owner=@khanakia stability=stable
func (c *Client) ResetRun(ctx context.Context, ref workflow.RunRef, step string) (workflow.RunRef, error) {
	return c.ResetRunWith(ctx, ref, step, workflow.ResetOptions{})
}

// ResetRunWith is ResetRun that, with opts.Succeeded, also resets a
// succeeded run in place (workflow.RunResetterWith): the same script as
// Retry checks the status, so a run that moved meanwhile is not touched,
// and also writes workflow.LabelRedo into the run's labels and clears its
// stored result. The chain was handed on, and cleared, by the script that
// ended the run succeeded, so it is not handed on again; a succeeded run
// that still holds a chain (its handoff failed) is refused with
// ErrWrongState.
//
//ds:def id=redis-client-resetrunwith-wmvvxtaj owner=@khanakia stability=stable
func (c *Client) ResetRunWith(ctx context.Context, ref workflow.RunRef, step string, opts workflow.ResetOptions) (workflow.RunRef, error) {
	if step == "" {
		return workflow.RunRef{}, fmt.Errorf("%w: ResetRun needs a step", workflow.ErrInvalid)
	}
	return c.retryFrom(ctx, ref, step, opts)
}

// ForkRun starts a new run from a finished one at step (workflow.RunForker):
// the original's workflow, payload, tenant, Job.Queue, priority, job retry
// policy and workflow version, labels from workflow.ForkLabels, no chain,
// no start time, and opts.IdempotencyKey only (under the workflow's
// Idempotency policy, as Enqueue applies it: a key held by an unfinished
// run returns that run's RunRef and starts nothing). The steps the
// original succeeded in that are not step or downstream of it are written
// done with the original's outputs, so the fork resumes after them as a
// retried run does. One script checks that the original is still the
// finished run that was read, creates the fork through the same create
// helper as Enqueue (run index, floors, ready or pending set, idempotency
// hash) and writes its steps. The client must have the workflow
// registered to know which steps are downstream; otherwise it returns an
// error wrapping ErrInvalid.
//
//ds:def id=redis-client-forkrun-bvcdjaks owner=@khanakia stability=stable
func (c *Client) ForkRun(ctx context.Context, ref workflow.RunRef, step string, opts workflow.ForkOptions) (workflow.RunRef, error) {
	if err := ctx.Err(); err != nil {
		return workflow.RunRef{}, err
	}
	if err := workflow.CheckFork(step, opts); err != nil {
		return workflow.RunRef{}, err
	}
	if err := c.usable(); err != nil {
		return workflow.RunRef{}, err
	}
	id, err := runID(ref)
	if err != nil {
		return workflow.RunRef{}, err
	}
	r, err := c.load(ctx, id, ref)
	if err != nil {
		return workflow.RunRef{}, err
	}
	if !workflow.ForkStatusOK(r.status) {
		return workflow.RunRef{}, fmt.Errorf("%w: run %+v is %s; only a finished run can be forked", workflow.ErrWrongState, ref, r.status)
	}
	wf, known := c.registered(r.wf)
	if !known {
		return workflow.RunRef{}, fmt.Errorf("%w: ForkRun: workflow %s is not registered on this client, so its steps are unknown", workflow.ErrInvalid, r.wf)
	}
	rerun := workflow.StepsFrom(wf, step)
	if rerun == nil {
		return workflow.RunRef{}, fmt.Errorf("%w: workflow %s has no step %q", workflow.ErrInvalid, r.wf, step)
	}
	job := workflow.Job{Workflow: r.wf, Payload: r.payload, Priority: r.priority, Queue: r.jobQueue, Tenant: r.tenant, Retry: r.retry,
		IdempotencyKey: opts.IdempotencyKey,
		Labels:         workflow.ForkLabels(r.labels, opts.Labels, workflow.RunID(id), step, workflow.ForkRedo(r.status, r.labels))}
	field, err := c.idemFieldFor(job)
	if err != nil {
		return workflow.RunRef{}, fmt.Errorf("%w (ForkOptions.IdempotencyKey)", err)
	}
	kept := map[string]*stepRec{}
	for name, rec := range r.steps {
		if rec.Done && !slices.Contains(rerun, name) {
			kept[name] = &stepRec{Done: true, Out: rec.Out}
		}
	}
	steps, err := json.Marshal(kept)
	if err != nil {
		return workflow.RunRef{}, fmt.Errorf("redis: encode fork steps: %w", err)
	}
	now := c.clk.Now()
	fid := c.newID()
	args, err := c.createArgsAt(fid, job, field, false, now, r.version)
	if err != nil {
		return workflow.RunRef{}, err
	}
	res, err := forkScript.Run(ctx, c.rdb, c.keys(), append([]any{c.base, id, r.finishedRaw, string(steps), field}, args...)...).Slice()
	if err != nil {
		return workflow.RunRef{}, fmt.Errorf("redis: fork: %w", classify(err))
	}
	switch str(res[0]) {
	case "new":
	case "dup":
		workflow.MarkDuplicate(ctx)
		return refOf(str(res[1]), workflow.Status(str(res[2])), str(res[3])), nil
	case "notfound":
		return workflow.RunRef{}, fmt.Errorf("%w: run %+v", workflow.ErrNotFound, ref)
	default:
		return workflow.RunRef{}, fmt.Errorf("%w: run %+v is %s or changed since it was read; only a finished run can be forked", workflow.ErrWrongState, ref, str(res[1]))
	}
	workflow.MarkInserted(ctx)
	c.wakeAll()
	c.pokeDispatcher()
	out := workflow.RunRef{RunID: workflow.RunID(fid)}
	c.emit(workflow.LifecycleEvent{Kind: workflow.LifecycleInserted, Ref: out, Workflow: job.Workflow, Queue: workflow.EffectiveQueue(wf, job),
		Tenant: job.Tenant, Status: workflow.StatusQueued, Labels: job.Labels, At: now})
	return out, nil
}
