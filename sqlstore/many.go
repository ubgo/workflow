package sqlstore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/ubgo/workflow"
)

// EnqueueMany starts jobs in one transaction (batch.ManyEnqueuer, which
// batch.EnqueueMany prefers): every run is written or none is, so a crash
// or an error never leaves half a bulk enqueue behind. It returns one
// RunRef per job, in order. Each job is checked as Enqueue checks it
// before anything is written; an error returns nil refs and starts
// nothing. A job whose idempotency key an unfinished run holds (one
// already in the table, or an earlier job of this call) gets that run's
// RunRef and starts nothing, as Enqueue does. Enqueue middleware of a
// wrapping client does not run, since the call bypasses the wrapper.
func (c *Client) EnqueueMany(ctx context.Context, jobs []workflow.Job) ([]workflow.RunRef, error) {
	if len(jobs) == 0 {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.usable(); err != nil {
		return nil, err
	}
	regs := make([]*registered, len(jobs))
	keys := make([]string, len(jobs))
	for i, job := range jobs {
		reg, err := c.enqueueReg(job.Workflow)
		if err != nil {
			return nil, fmt.Errorf("jobs[%d]: %w", i, err)
		}
		if err := workflow.CheckJobLabels(ctx, job); err != nil {
			return nil, fmt.Errorf("jobs[%d]: %w", i, err)
		}
		if err := c.checkJob(reg, job); err != nil {
			return nil, fmt.Errorf("jobs[%d]: %w", i, err)
		}
		key, err := idemKey(reg, job)
		if err != nil {
			return nil, fmt.Errorf("jobs[%d]: %w", i, err)
		}
		regs[i], keys[i] = reg, key
	}
	var refs []workflow.RunRef
	var dup []bool // jobs answered with an idempotency holder's run
	var pending map[string]*registered
	var out txOut
	var err error
	// A key another transaction takes between this one's holder read and
	// its insert fails the insert on the unique index; the whole
	// transaction is then tried again and finds the holder.
	for range maxIdemAttempts {
		err = c.st.tx(ctx, func(tx *sql.Tx) error {
			refs, pending, out = make([]workflow.RunRef, len(jobs)), map[string]*registered{}, txOut{}
			dup = make([]bool, len(jobs))
			for i, job := range jobs {
				if keys[i] != "" {
					holder, err := c.loadHolder(ctx, tx, regs[i].wf.Name, keys[i])
					if err != nil {
						return err
					}
					if holder != nil {
						refs[i] = holder.ref()
						dup[i] = true
						continue
					}
				}
				row, err := c.insertRun(ctx, tx, regs[i], job, keys[i], "")
				if err != nil {
					return err
				}
				if err := c.admitInline(ctx, tx, regs[i], row, &out); err != nil {
					return err
				}
				refs[i] = row.ref()
				if row.status == runPending {
					pending[regs[i].wf.Name] = regs[i]
				}
			}
			return nil
		})
		if err == nil || !c.st.isUnique(err) || ctx.Err() != nil {
			break
		}
	}
	if err != nil {
		return nil, err
	}
	for _, reg := range pending {
		if err := c.admitEnqueued(ctx, reg); err != nil {
			return refs, err
		}
	}
	c.afterTx(ctx, out)
	c.nudge()
	// One outcome per call: the first mark wins, so it reports the first job.
	if dup[0] {
		workflow.MarkDuplicate(ctx)
	} else {
		workflow.MarkInserted(ctx)
	}
	return refs, nil
}

// loadHolder reads the unfinished run of workflow holding key in tx; nil
// when none does.
func (c *Client) loadHolder(ctx context.Context, tx *sql.Tx, wf, key string) (*runRow, error) {
	var r runRow
	err := c.st.one(ctx, tx, "SELECT "+runCols+" FROM {runs} WHERE workflow = ? AND idem_active = ?", []any{wf, key}, r.targets()...)
	if isNoRows(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("sqlstore: read idempotency holder: %w", err)
	}
	return &r, nil
}
