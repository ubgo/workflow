package memory

import (
	"context"

	"github.com/ubgo/workflow"
)

// coalesceLocked answers an enqueue of a workflow with Workflow.Coalesce
// whose key ik is held by holder: a holder still in the queue, or the run
// already pending behind an admitted holder, takes the enqueue in (with
// CoalesceKeepLast, its payload and priority); otherwise a new run is
// stored pending behind the holder, scheduled with no timer until the
// holder ends (promoteNextLocked).
func (c *Client) coalesceLocked(ctx context.Context, reg *registered, job workflow.Job, ik idemKey, holder *run) workflow.RunRef {
	if !holder.admitted {
		c.foldLocked(reg, holder, job)
		workflow.MarkDuplicate(ctx)
		return holder.ref()
	}
	if p, ok := c.next[ik]; ok {
		c.foldLocked(reg, p, job)
		workflow.MarkDuplicate(ctx)
		return p.ref()
	}
	r := c.newRunLocked(reg, job)
	r.runAt = job.RunAt
	r.sid = workflow.ScheduledID(c.nextID("delayed"))
	r.status = workflow.StatusScheduled
	c.delayed[r.sid] = r
	if c.next == nil {
		c.next = map[idemKey]*run{}
	}
	c.next[ik] = r
	workflow.MarkInserted(ctx)
	c.emitLocked(r, workflow.LifecycleInserted, "", 0, nil)
	return r.ref()
}

// foldLocked folds job into r, a run still in the queue: under
// CoalesceKeepLast r takes job's payload and priority, and its concurrency
// and rate-limit keys are evaluated again from them, so admission (which
// it has not passed yet) weighs the keys of the payload it will run with.
// A run waiting for admission may fit under its new keys, so admission is
// tried again.
func (c *Client) foldLocked(reg *registered, r *run, job workflow.Job) {
	if !reg.wf.Coalesce.KeepsLast() || r.admitted || r.status.IsTerminal() {
		return
	}
	r.job.Payload, r.job.Priority = job.Payload, job.Priority
	r.evalKeys()
	c.admitLocked()
}

// keyReleasedLocked runs when r ends, after its idempotency key was
// released: it forgets r's checkpoints and its place as a pending
// coalesced run, and starts the run pending behind r's key, which then
// holds the key, at the later of now and its own RunAt.
func (c *Client) keyReleasedLocked(r *run) {
	c.dropCheckpointsLocked(r)
	if len(c.next) == 0 {
		return
	}
	if k := (idemKey{workflow: r.reg.wf.Name, key: r.job.IdempotencyKey}); c.next[k] == r {
		delete(c.next, k)
	}
	if r.idem == nil {
		return
	}
	ik := *r.idem
	p, ok := c.next[ik]
	if _, held := c.idem[ik]; !ok || held || c.stopped {
		return
	}
	delete(c.next, ik)
	p.idem = &ik
	c.idem[ik] = p
	c.armDelayLocked(p, p.job.RunAt.Sub(c.clk.Now()))
}
