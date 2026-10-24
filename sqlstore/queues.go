package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"sort"

	"github.com/ubgo/workflow"
)

// queueOf is the SQL expression for the queue of the step row aliased st:
// its run's run_ext row, or the default queue for a run written before
// migration 003 (which has no row).
const queueOf = "COALESCE((SELECT rq.queue FROM {run_ext} rq WHERE rq.run_id = st.run_id), '" + workflow.DefaultQueue + "')"

// queueOfAs is queueOf for the step row aliased alias.
func queueOfAs(alias string) string {
	return "COALESCE((SELECT rq.queue FROM {run_ext} rq WHERE rq.run_id = " + alias + ".run_id), '" + workflow.DefaultQueue + "')"
}

// notPaused is the predicate that keeps the step row aliased st out of a
// claim while a pause holds its run: a pause of everything, of its
// workflow or of its queue. It is part of the claim statement itself, so a
// Pause that committed before the claim began is obeyed on every node.
const notPaused = "NOT EXISTS (SELECT 1 FROM {pauses} p WHERE p.kind = '" + pauseAll + "'" +
	" OR (p.kind = '" + pauseWorkflow + "' AND p.name = st.workflow)" +
	" OR (p.kind = '" + pauseQueue + "' AND p.name = " + queueOf + "))"

// queueTiers groups qs by Priority, highest first, each tier's queues by
// name, and returns the names as IN-list arguments.
func queueTiers(qs []workflow.QueueConfig) ([][]workflow.QueueConfig, []any) {
	sorted := append([]workflow.QueueConfig(nil), qs...)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].Priority != sorted[j].Priority {
			return sorted[i].Priority > sorted[j].Priority
		}
		return sorted[i].Name < sorted[j].Name
	})
	var tiers [][]workflow.QueueConfig
	var served []any
	for i, q := range sorted {
		if i == 0 || q.Priority != sorted[i-1].Priority {
			tiers = append(tiers, nil)
		}
		tiers[len(tiers)-1] = append(tiers[len(tiers)-1], q)
		served = append(served, q.Name)
	}
	return tiers, served
}

// capped reports whether any served queue has a Workers cap, which is when
// claims must be serialised (claimMu). The caps change under c.mu
// (SetWorkers).
func (c *Client) capped() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, q := range c.queues {
		if q.Workers > 0 {
			return true
		}
	}
	return false
}

// openQueues returns the queues of tier that have room for one more
// attempt on this client.
func (c *Client) openQueues(tier []workflow.QueueConfig) []any {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []any
	for _, q := range tier {
		if q.Workers == 0 || c.running[q.Name] < q.Workers {
			out = append(out, q.Name)
		}
	}
	return out
}

// countRunning moves the per-queue count of attempts executing here.
func (c *Client) countRunning(queue string, delta int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.running[queue] += delta
	if c.running[queue] <= 0 {
		delete(c.running, queue)
	}
}

// retryDoc is a workflow.RetryPolicy as stored in run_ext.retry. Only the
// data fields: workflow.ValidateJob refuses a Job.Retry with NoRetryOn or
// DelayFor, because a function cannot be stored. Durations are
// nanoseconds.
type retryDoc struct {
	Max       int     `json:"max"`
	Initial   int64   `json:"initial"`
	Factor    float64 `json:"factor"`
	MaxDelay  int64   `json:"max_delay"`
	Jitter    float64 `json:"jitter"`
	Until     int64   `json:"until"`
	MaxErrors int     `json:"max_errors"`

	// DiscardOnExhaust is RetryPolicy.DiscardOnExhaust. It lives in this JSON
	// document, not a column, so it needs no migration: a document written
	// before it existed decodes to false, the default.
	DiscardOnExhaust bool `json:"discard_on_exhaust,omitempty"`
}

func encodeRetry(p *workflow.RetryPolicy) (any, error) {
	if p == nil {
		return nil, nil
	}
	b, err := json.Marshal(toRetryDoc(p))
	if err != nil {
		return nil, fmt.Errorf("sqlstore: encode Job.Retry: %w", err)
	}
	return string(b), nil
}

func decodeRetry(s sql.NullString) (*workflow.RetryPolicy, error) {
	if !s.Valid || s.String == "" {
		return nil, nil
	}
	var d retryDoc
	if err := json.Unmarshal([]byte(s.String), &d); err != nil {
		return nil, fmt.Errorf("sqlstore: stored Job.Retry: %w", err)
	}
	return fromRetryDoc(d), nil
}

// insertRunExt writes run id's run_ext row in tx: its queue, its key and
// its retry override.
func (c *Client) insertRunExt(ctx context.Context, tx *sql.Tx, reg *registered, job workflow.Job, key, id string) error {
	retry, err := encodeRetry(job.Retry)
	if err != nil {
		return err
	}
	var idem any
	if key != "" {
		idem = key
	}
	_, err = c.st.exec(ctx, tx, "INSERT INTO {run_ext} (run_id, queue, idem_key, retry, started_at, discarded) VALUES (?, ?, ?, ?, 0, 0)",
		id, runQueue(reg, job), idem, retry)
	return err
}

// checkJob is what Enqueue and EnqueueTx refuse before writing: a job of
// the wrong shape, or one needing what this driver lacks.
func (c *Client) checkJob(reg *registered, job workflow.Job) error {
	if err := workflow.ValidateJob(job); err != nil {
		return err
	}
	if err := workflow.CheckPayload(job, c.payloadLimit, "driver"); err != nil {
		return err
	}
	if err := workflow.CheckJob(caps, job); err != nil {
		return err
	}
	return workflow.CheckJobQueue(caps, reg.wf, job)
}

// runQueue is the queue stored for a new run: workflow.EffectiveQueue, or
// for a stand-in (an enqueue-only client without the workflow's
// definition) the job's own queue, empty when it names none, which the
// admitting worker replaces with the workflow's (evaluateKeys).
func runQueue(reg *registered, job workflow.Job) string {
	if reg.stub {
		return job.Queue
	}
	return workflow.EffectiveQueue(reg.wf, job)
}

// draw is one uniform value in [0, 1) from the jitter source
// (WithJitterSource), for weighted queue picks.
func (c *Client) draw() float64 {
	if c.rnd != nil {
		return c.rnd()
	}
	return rand.Float64()
}
