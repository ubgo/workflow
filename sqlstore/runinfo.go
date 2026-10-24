package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/workflow"
)

// Migration 008's tables: per-run facts the queue's own statements never
// read (tenant, result, progress, the nodes that ran it), the node of each
// recorded attempt error, and the node registry.
const (
	tblRunInfo      = "run_info"
	tblAttemptNode  = "attempt_err_node"
	tblNodes        = "nodes"
	defaultNodesTTL = time.Hour // how long a silent node stays listed
)

// runInfoDefs is the tables of migration 008. New tables, not columns, for
// the reason adminDefs gives.
func runInfoDefs(t sqlTypes) []tableDef {
	return []tableDef{{stem: tblRunInfo, lines: []string{
		// One row per run, written with it (backfilled by 008 for older
		// runs), so later writes are updates.
		"run_id " + t.id + " NOT NULL PRIMARY KEY",
		// tenant is Job.Tenant; empty: none.
		"tenant " + t.name + " NOT NULL",
		// output is workflow.RunOutput, written when the run succeeds;
		// has_output tells a stored empty output from none.
		"output " + t.blob,
		"has_output " + t.int + " NOT NULL",
		// progress is the latest workflow.Progress as JSON; NULL: none.
		"progress " + t.text,
		// nodes is a JSON array of the node ids that claimed an attempt of
		// the run, in first-claim order (RunInfo.AttemptedBy).
		"nodes " + t.text + " NOT NULL",
		// deadline is when Workflow.RunTimeout fails the run, Unix
		// nanoseconds on the scheduling clock, set at admission; 0: none.
		"deadline " + t.int + " NOT NULL",
		// timeout names the deadline that failed the run (timeout.go);
		// empty when it did not fail on one.
		"timeout " + t.short + " NOT NULL",
	}, indexes: []index{{"tenant", "tenant"}}}, {stem: tblAttemptNode, lines: []string{
		// The node that ran each attempt recorded in attempt_errors.
		"run_id " + t.id + " NOT NULL",
		"step " + t.name + " NOT NULL",
		"attempt " + t.int + " NOT NULL",
		"node " + t.name + " NOT NULL",
		"PRIMARY KEY (run_id, step, attempt)",
	}}, {stem: tblStepCancel, lines: []string{
		// One row per running step CancelStep asked to stop, deleted when
		// the attempt's outcome is written.
		"step_id " + t.id + " NOT NULL PRIMARY KEY",
	}}, scheduleSpecDefs(t)[0], {stem: tblNodes, lines: []string{
		// One row per running client (WithNodeID), refreshed by its
		// dispatcher; rows silent for an hour are pruned.
		"id " + t.name + " NOT NULL PRIMARY KEY",
		"mode " + t.short + " NOT NULL",
		// queues is a JSON array of the queue names served; empty: all.
		"queues " + t.text + " NOT NULL",
		"workers " + t.int + " NOT NULL",
		"running " + t.int + " NOT NULL",
		"started_at " + t.int + " NOT NULL",
		"last_seen " + t.int + " NOT NULL",
	}}}
}

// backfillRunInfo gives every run written before 008 its run_info row.
// Safe to run twice: it inserts only the missing rows.
const backfillRunInfo = "INSERT INTO {run_info} (run_id, tenant, output, has_output, progress, nodes, deadline, timeout)" +
	" SELECT id, '', NULL, 0, NULL, '[]', 0, '' FROM {runs} WHERE id NOT IN (SELECT run_id FROM {run_info})"

// insertRunInfo writes run id's run_info row in tx.
func (c *Client) insertRunInfo(ctx context.Context, tx *sql.Tx, job workflow.Job, id string) error {
	_, err := c.st.exec(ctx, tx, "INSERT INTO {run_info} (run_id, tenant, output, has_output, progress, nodes, deadline, timeout) VALUES (?, ?, NULL, 0, NULL, '[]', 0, '')",
		id, job.Tenant)
	return err
}

// infoExtra is what Runs reads from run_info for one run.
type infoExtra struct {
	tenant    sql.NullString
	output    []byte
	hasOutput sql.NullInt64
	progress  sql.NullString
	nodes     sql.NullString
}

// fill copies the run_info facts into info.
func (x infoExtra) fill(c *Client, info *workflow.RunInfo) {
	info.Tenant = x.tenant.String
	if x.hasOutput.Int64 == 1 {
		info.Output = append([]byte{}, x.output...)
	}
	if x.progress.Valid {
		var p workflow.Progress
		if err := json.Unmarshal([]byte(x.progress.String), &p); err == nil {
			info.Progress = &p
		} else {
			c.log.Error("sqlstore: unreadable stored progress", "run", info.Ref.RunID, "error", err)
		}
	}
	if x.nodes.Valid && x.nodes.String != "" {
		var nodes []string
		if err := json.Unmarshal([]byte(x.nodes.String), &nodes); err == nil && len(nodes) > 0 {
			info.AttemptedBy = nodes
		}
	}
}

// storeOutput writes the result of run id, which just succeeded in tx.
func (c *Client) storeOutput(ctx context.Context, tx *sql.Tx, id string, out []byte) error {
	_, err := c.st.exec(ctx, tx, "UPDATE {run_info} SET output = ?, has_output = 1 WHERE run_id = ?", out, id)
	return err
}

// clearRunResult forgets run id's result and progress, for a run that
// starts over (Retry).
func (c *Client) clearRunResult(ctx context.Context, tx *sql.Tx, id string) error {
	_, err := c.st.exec(ctx, tx, "UPDATE {run_info} SET output = NULL, has_output = 0, progress = NULL, deadline = 0, timeout = '' WHERE run_id = ?", id)
	return err
}

// Result returns the output of a succeeded run (workflow.ResultReader):
// workflow.RunOutput of its final steps, stored when it succeeded. A run
// that has not succeeded returns ErrWrongState; an unknown one ErrNotFound.
func (c *Client) Result(ctx context.Context, ref workflow.RunRef) ([]byte, error) {
	r, err := c.lookup(ctx, ref)
	if err != nil {
		return nil, err
	}
	if r.status != runSucceeded {
		return nil, fmt.Errorf("%w: run %s is %s, not succeeded", workflow.ErrWrongState, r.id, runStatus(r.status, nil))
	}
	var out []byte
	var has int64
	err = c.st.one(ctx, c.st.db, "SELECT output, has_output FROM {run_info} WHERE run_id = ?", []any{r.id}, &out, &has)
	if errors.Is(err, sqlb.ErrNoRows) || (err == nil && has == 0) {
		return nil, nil // succeeded before results were stored
	}
	if err != nil {
		return nil, fmt.Errorf("sqlstore: read the result: %w", err)
	}
	return out, nil
}

// reportProgress is the workflow.ProgressReporter a step of run a sees.
func (c *Client) progressReporter(a *attempt) workflow.ProgressFunc {
	return func(ctx context.Context, p workflow.Progress) error {
		p.Step = a.name
		p.At = c.clk.Now()
		raw, err := json.Marshal(p)
		if err != nil {
			return fmt.Errorf("sqlstore: encode progress: %w", err)
		}
		if _, err := c.st.exec(ctx, c.st.db, "UPDATE {run_info} SET progress = ? WHERE run_id = ?", string(raw), a.runID); err != nil {
			return err
		}
		c.emit(ctx, workflow.LifecycleEvent{Kind: workflow.LifecycleProgress, Ref: workflow.RunRef{RunID: workflow.RunID(a.runID)},
			Workflow: a.workflow, Queue: a.queue, Tenant: a.tenant, Step: a.name, Attempt: int(a.attempts), Progress: &p})
		return nil
	}
}

// Reschedule moves a scheduled run's start to at (workflow.RunRescheduler).
// The RunRef, payload, labels and key stay; RunInfo.RunAt becomes at; a
// zero or past at starts it now. A run that is not scheduled returns
// ErrWrongState, an unknown one ErrNotFound.
func (c *Client) Reschedule(ctx context.Context, ref workflow.RunRef, at time.Time) (workflow.RunRef, error) {
	r, err := c.lookup(ctx, ref)
	if err != nil {
		return workflow.RunRef{}, err
	}
	now := c.nowNanos()
	when := at.UnixNano()
	if at.IsZero() || when < now {
		when = now
	}
	n, err := c.st.execN(ctx, c.st.db, "UPDATE {runs} SET run_at = ? WHERE id = ? AND status = ?", when, r.id, runScheduled)
	if err != nil {
		return workflow.RunRef{}, err
	}
	if n == 0 {
		return workflow.RunRef{}, fmt.Errorf("%w: run %s is not scheduled", workflow.ErrWrongState, r.id)
	}
	c.nudge()
	return r.ref(), nil
}

// dueFilter is the SQL half of RunQuery.DueFrom and DueTo: runs started
// with a RunAt (a scheduled id) due in [from, to).
func dueFilter(q workflow.RunQuery) (string, []any) {
	if q.DueFrom.IsZero() && q.DueTo.IsZero() {
		return "", nil
	}
	where, args := " AND r.scheduled_id IS NOT NULL", []any(nil)
	if !q.DueFrom.IsZero() {
		where += " AND r.run_at >= ?"
		args = append(args, q.DueFrom.UnixNano())
	}
	if !q.DueTo.IsZero() {
		where += " AND r.run_at < ?"
		args = append(args, q.DueTo.UnixNano())
	}
	return where, args
}

// addRunNode appends this client's node to the run's AttemptedBy list in
// tx (the claim), unless it is there already.
func (c *Client) addRunNode(ctx context.Context, tx *sql.Tx, runID, raw string) error {
	var nodes []string
	if raw != "" {
		if err := json.Unmarshal([]byte(raw), &nodes); err != nil {
			c.log.Error("sqlstore: unreadable run nodes; starting the list again", "run", runID, "error", err)
			nodes = nil
		}
	}
	if slices.Contains(nodes, c.nodeID) {
		return nil
	}
	enc, err := json.Marshal(append(nodes, c.nodeID))
	if err != nil {
		return err
	}
	_, err = c.st.exec(ctx, tx, "UPDATE {run_info} SET nodes = ? WHERE run_id = ?", string(enc), runID)
	return err
}
