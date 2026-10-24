package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/ubgo/workflow"
)

// WithEnqueueOnly makes a client that writes work for other clients to run
// (workflow.CapEnqueueOnly): a web node enqueues, worker nodes over the
// same tables run. Enqueue, EnqueueMany, EnqueueTx, Status, Cancel and
// Signal work without Start, and Start returns nil and starts nothing: no
// worker, dispatcher, cron firing or run-end delivery runs on it. Stop makes
// later calls return ErrNotStarted and closes Done.
//
// Register is optional. A registered workflow's jobs are checked and keyed
// as on any client (workflow idempotency, concurrency and rate keys are
// evaluated here) and, for a workflow with no concurrency or rate policy,
// admitted at once. A job of a workflow not registered here passes only
// the checks that need no definition (workflow.CheckJob); its
// workflow-level idempotency policy does not apply (Job.IdempotencyKey
// still does), and its concurrency key, rate keys and default queue are
// evaluated by the first worker that admits it.
//
// With a Notifier (WithNotifier) each enqueue notifies the workers before
// it returns, since there is no background sender on this client.
func WithEnqueueOnly() Option {
	return func(cl *Client) { cl.enqueueOnly = true }
}

// enqueueReg is the registration an enqueue of workflow name uses: the
// registered one, or on an enqueue-only client a definition-free stand-in
// (registered.stub) whose keys the admitting worker evaluates.
func (c *Client) enqueueReg(name string) (*registered, error) {
	if reg := c.reg(name); reg != nil {
		return reg, nil
	}
	if c.enqueueOnly {
		return &registered{wf: workflow.Workflow{Name: name}, stub: true}, nil
	}
	return nil, fmt.Errorf("%w: workflow %q is not registered", workflow.ErrNotFound, name)
}

// admitEnqueued admits reg's pending runs after an enqueue, unless this
// client cannot: a stand-in has no steps to write, and an enqueue-only
// client holds no admission lock rows (Start never ran prepare), so a
// workflow with a policy is admitted by its workers.
func (c *Client) admitEnqueued(ctx context.Context, reg *registered) error {
	if reg.stub || (c.enqueueOnly && (reg.wf.Concurrency != nil || len(reg.wf.RateLimits) > 0)) {
		return nil
	}
	return c.admit(ctx, reg)
}

// unevaluatedKeys is the rate_keys value of a run enqueued without its
// workflow's definition: its bucket, rate keys and default queue are
// written by the first worker that admits it (evaluateKeys).
const unevaluatedKeys = ""

// evaluateKeys fills in, in x, the concurrency bucket, rate keys and
// default queue of run r, enqueued by an enqueue-only client that did not
// register its workflow. Both updates are conditional on the marker, so a
// second worker doing the same finds nothing to change.
func (c *Client) evaluateKeys(ctx context.Context, tx *sql.Tx, reg *registered, r *runRow) error {
	if r.rateKeys != unevaluatedKeys {
		return nil
	}
	job, err := r.job()
	if err != nil {
		c.log.Error("sqlstore: run keys evaluated without labels", "run", r.id, "error", err)
	}
	bucket, rk, err := runKeys(reg, job)
	if err != nil {
		return err
	}
	if _, err := c.st.exec(ctx, tx, "UPDATE {runs} SET bucket = ?, rate_keys = ? WHERE id = ? AND rate_keys = ?",
		bucket, rk, r.id, unevaluatedKeys); err != nil {
		return err
	}
	if _, err := c.st.exec(ctx, tx, "UPDATE {run_ext} SET queue = ? WHERE run_id = ? AND queue = ''",
		workflow.EffectiveQueue(reg.wf, workflow.Job{}), r.id); err != nil {
		return err
	}
	r.bucket, r.rateKeys = bucket, rk
	return nil
}

// runKeys evaluates reg's concurrency key and rate-limit keys for job: the
// bucket and the rate keys as stored (a JSON list, one per limit).
func runKeys(reg *registered, job workflow.Job) (string, string, error) {
	bucket := ""
	if conc := reg.wf.Concurrency; conc != nil && conc.Key.Func != nil {
		bucket = conc.Key.Func(job)
	}
	keys := make([]string, len(reg.wf.RateLimits))
	for i, rl := range reg.wf.RateLimits {
		if rl.Key.Func != nil {
			keys[i] = rl.Key.Func(job)
		}
	}
	rk, err := json.Marshal(keys)
	if err != nil {
		return "", "", fmt.Errorf("sqlstore: encode rate keys: %w", err)
	}
	return bucket, string(rk), nil
}
