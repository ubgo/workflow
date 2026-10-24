package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
)

// Retention says how long finished runs are kept before Prune deletes them.
// It is opt-in: without WithRetention nothing is ever deleted, because
// deleting history is a decision only the app can make.
type Retention struct {
	// Succeeded, Failed and Cancelled are how long a run that ended with
	// that status is kept after it finished. Zero keeps it forever. A
	// discarded run is kept as long as a cancelled one (Cancelled), as it
	// was when discards were stored as cancelled.
	Succeeded, Failed, Cancelled time.Duration

	// KeepLast caps how many finished runs (succeeded, failed,
	// cancelled and discarded together) each workflow keeps: past the newest KeepLast by
	// finish time, older runs are deleted even when their status's
	// duration above would keep them. Both rules apply: a run goes when either says so. The guards
	// below still hold, so a run they keep stays and still counts toward
	// the N. Zero (the default) sets no count limit.
	KeepLast int

	// PerWorkflow replaces the durations and KeepLast above for the named
	// workflows, whole: a workflow listed here uses only its own entry.
	PerWorkflow map[string]Retention

	// Undelivered also deletes runs whose end was never delivered to an
	// OnRunFinished observer. Without it, a cluster where no client
	// registers an observer deletes nothing at all: Prune reports those runs
	// in PruneResult.HeldUndelivered and the background prune logs a
	// warning naming both fixes. Leave it false when any client of the cluster
	// registers an observer, or an end it has not seen yet is lost; set it
	// when none does, or no run would ever be deleted (an end nobody
	// observes is never marked delivered).
	Undelivered bool
}

// FinishedRun is one run Prune is about to delete, as the archive hook
// sees it.
type FinishedRun struct {
	Ref        workflow.RunRef
	Workflow   string
	Status     workflow.Status
	Labels     map[string]string
	Payload    []byte
	Err        string // the stored error text; empty when it succeeded
	CreatedAt  time.Time
	FinishedAt time.Time
	// Outputs holds each step's stored output by step name; a step that
	// never succeeded is absent.
	Outputs map[string][]byte
}

// ArchiveFunc receives each chunk of runs before it is deleted. Returning
// an error keeps the chunk (and stops the prune), so history can be copied
// to cold storage first and is never lost to a failed copy.
type ArchiveFunc func(ctx context.Context, runs []FinishedRun) error

// Defaults of the retention options.
const (
	// defaultPruneInterval is how often a client with WithRetention tries to
	// prune. Retention is measured in hours or days, so an hourly pass keeps
	// the tables near their target without adding load.
	defaultPruneInterval = time.Hour
	// defaultPruneChunk is how many runs one delete transaction removes
	// (WithPruneChunk), so a prune holds its row locks briefly and never
	// blocks the queue for long, however much history has piled up.
	defaultPruneChunk = 100
	// pruneLockName is the lock row whose lease picks the one client of a
	// cluster that prunes in a given interval.
	pruneLockName = "prune"
)

// WithRetention turns on deletion of finished runs per r (see Retention).
func WithRetention(r Retention) Option {
	return func(cl *Client) { cl.retention = &r }
}

// WithPruneInterval sets how often the background prune runs. Values of
// zero or less are ignored.
func WithPruneInterval(d time.Duration) Option {
	return func(cl *Client) {
		if d > 0 {
			cl.pruneEvery = d
		}
	}
}

// WithPruneChunk sets how many finished runs one Prune transaction deletes
// (and the archive hook receives at once); default 100. Smaller chunks hold
// row locks for less time; larger ones finish a big backlog in fewer
// round trips. Values below 1 are ignored.
func WithPruneChunk(n int) Option {
	return func(cl *Client) {
		if n >= 1 {
			cl.pruneChunk = n
		}
	}
}

// WithoutAutoPrune turns off the background prune; the app calls Prune
// from its own scheduler instead.
func WithoutAutoPrune() Option { return func(cl *Client) { cl.noAutoPrune = true } }

// WithArchive sets the hook each chunk of runs passes through before it is
// deleted.
func WithArchive(fn ArchiveFunc) Option { return func(cl *Client) { cl.archive = fn } }

// pruneLoop prunes every pruneEvery on whichever client wins the prune
// lock's lease for that interval, so N clients do not all prune at once.
func (c *Client) pruneLoop() {
	defer c.wg.Done()
	for {
		t := c.leaseClk.NewTimer(c.pruneEvery)
		select {
		case <-c.ctx.Done():
			t.Stop()
			return
		case <-t.C():
		}
		now := c.leaseNanos()
		n, err := c.st.execN(c.ctx, c.st.db, "UPDATE {locks} SET n = ? WHERE name = ? AND n < ?", now+int64(c.pruneEvery), pruneLockName, now-int64(c.leaseSkew))
		if err != nil || n == 0 {
			if err != nil && c.ctx.Err() == nil {
				c.log.Error("sqlstore: take the prune lock", "error", err)
			}
			continue
		}
		res, err := c.Prune(c.ctx)
		if err != nil && c.ctx.Err() == nil {
			c.log.Error("sqlstore: prune finished runs", "error", err)
		}
		if res.HeldUndelivered > 0 {
			c.log.Warn("sqlstore: retention is holding back finished runs whose end was never delivered; "+
				"register an OnRunFinished observer on a client, or set Retention.Undelivered if no client observes run ends",
				"held", res.HeldUndelivered, "deleted", res.Deleted)
		}
	}
}

// pruneRule is one (workflows, status, cutoff) selection.
type pruneRule struct {
	where string
	args  []any
}

// terminalIn is the IN list of the four terminal run states, in the order
// terminalArgs gives them.
const terminalIn = "status IN (?, ?, ?, ?)"

func terminalArgs() []any { return []any{runSucceeded, runFailed, runCancelled, runDiscarded} }

// pruneRules turns the retention into selections of deletable runs: per
// status, by age on the scheduling clock; per workflow with a KeepLast,
// everything that finished before its KeepLast-th newest run. Each rule's
// cutoff is fixed when Prune starts, so the delete can repeat the rule.
func (c *Client) pruneRules(ctx context.Context) ([]pruneRule, error) {
	r := c.retention
	now := c.nowNanos()
	var out []pruneRule
	add := func(scope string, scopeArgs []any, keep Retention) {
		for _, s := range []struct {
			status string
			keep   time.Duration
		}{{runSucceeded, keep.Succeeded}, {runFailed, keep.Failed}, {runCancelled, keep.Cancelled}, {runDiscarded, keep.Cancelled}} {
			if s.keep <= 0 {
				continue
			}
			args := append(append([]any(nil), scopeArgs...), s.status, now-int64(s.keep))
			out = append(out, pruneRule{where: scope + " status = ? AND finished_at <= ?", args: args})
		}
	}
	var listed []any
	for name, keep := range r.PerWorkflow {
		add("workflow = ? AND", []any{name}, keep)
		listed = append(listed, name)
		if keep.KeepLast > 0 {
			rule, ok, err := c.keepLastRule(ctx, name, keep.KeepLast)
			if err != nil {
				return nil, err
			}
			if ok {
				out = append(out, rule)
			}
		}
	}
	if len(listed) == 0 {
		add("", nil, *r)
	} else {
		add("workflow NOT IN ("+inList(len(listed))+") AND", listed, *r)
	}
	if r.KeepLast > 0 {
		q := "SELECT DISTINCT workflow FROM {runs} WHERE " + terminalIn
		args := terminalArgs()
		if len(listed) > 0 {
			q += " AND workflow NOT IN (" + inList(len(listed)) + ")"
			args = append(args, listed...)
		}
		var names []string
		var name string
		if err := c.st.each(ctx, c.st.db, q, args, func() error { names = append(names, name); return nil }, &name); err != nil {
			return nil, fmt.Errorf("sqlstore: read workflows with finished runs: %w", err)
		}
		for _, name := range names {
			rule, ok, err := c.keepLastRule(ctx, name, r.KeepLast)
			if err != nil {
				return nil, err
			}
			if ok {
				out = append(out, rule)
			}
		}
	}
	return out, nil
}

// keepLastRule selects the finished runs of workflowName older than its
// keep-th newest (by finish time, then by seq, so ties are cut exactly).
// ok is false when the workflow has keep or fewer finished runs.
func (c *Client) keepLastRule(ctx context.Context, workflowName string, keep int) (pruneRule, bool, error) {
	var at, seq int64
	err := c.st.one(ctx, c.st.db, "SELECT finished_at, seq FROM {runs} WHERE workflow = ? AND "+terminalIn+
		" ORDER BY finished_at DESC, seq DESC LIMIT 1 OFFSET ?",
		append(append([]any{workflowName}, terminalArgs()...), keep), &at, &seq)
	if errors.Is(err, sqlb.ErrNoRows) {
		return pruneRule{}, false, nil
	}
	if err != nil {
		return pruneRule{}, false, fmt.Errorf("sqlstore: find the keep-last cutoff of %q: %w", workflowName, err)
	}
	return pruneRule{
		where: "workflow = ? AND " + terminalIn + " AND (finished_at < ? OR (finished_at = ? AND seq <= ?))",
		args:  append(append([]any{workflowName}, terminalArgs()...), at, at, seq),
	}, true, nil
}

// guards are the conditions every deleted run meets, whatever the rule:
// it holds no idempotency key and, unless Retention.Undelivered, its end
// was delivered. The batch guard is checked in Go (batchOpen).
func (c *Client) guards() string {
	g := "idem_active IS NULL"
	if !c.retention.Undelivered {
		g += " AND notified = 1"
	}
	return g
}

// PruneResult is what one Prune did. The Held counts are runs old enough
// to delete that a guard kept, so a setup that can never delete anything
// shows up as a number rather than as silence.
type PruneResult struct {
	// Deleted is how many runs were deleted (with their steps).
	Deleted int
	// HeldUndelivered counts runs whose end was never delivered to an
	// OnRunFinished observer. When it keeps growing, no client of the
	// cluster registers an observer: register one, or set
	// Retention.Undelivered.
	HeldUndelivered int
	// HeldIdempotency counts runs still holding an idempotency key. A run
	// that both an age rule and KeepLast select is counted once per rule.
	HeldIdempotency int
	// HeldBatch counts runs of a batch in BatchStore that is still open
	// (State.Open).
	HeldBatch int
}

// Prune deletes the finished runs the retention allows, in chunks of
// WithPruneChunk, each through the archive hook first, and reports what it
// deleted and what each guard held back. Each run's steps and
// consumed-signal markers go with it. It never deletes a run that still
// holds an idempotency key, whose end is not delivered (unless
// Retention.Undelivered), or that belongs to a batch in this client's
// BatchStore that is still open (State.Open). Without WithRetention it deletes
// nothing. It needs Migrate, not Start, so an app can run it from its own
// scheduler (with WithoutAutoPrune).
func (c *Client) Prune(ctx context.Context) (PruneResult, error) {
	var res PruneResult
	if c.retention == nil {
		return res, nil
	}
	rules, err := c.pruneRules(ctx)
	if err != nil {
		return res, err
	}
	for _, rule := range rules {
		if err := c.countHeld(ctx, rule, &res); err != nil {
			return res, err
		}
		skip := map[string]bool{} // runs a guard kept, so the next chunk moves on
		for {
			runs, err := c.pruneCandidates(ctx, rule, skip, &res.HeldBatch)
			if err != nil {
				return res, err
			}
			if len(runs) == 0 {
				break
			}
			n, err := c.deleteChunk(ctx, rule, runs)
			res.Deleted += n
			if err != nil {
				return res, err
			}
			if n == 0 { // every run changed since it was read: move past them
				for _, fr := range runs {
					skip[string(fr.Ref.RunID)] = true
				}
			}
		}
	}
	return res, nil
}

// countHeld adds the runs rule selects that the SQL guards keep: those
// holding an idempotency key, and those whose end is undelivered.
func (c *Client) countHeld(ctx context.Context, rule pruneRule, res *PruneResult) error {
	var n int64
	q := "SELECT COUNT(*) FROM {runs} WHERE " + rule.where + " AND idem_active IS NOT NULL"
	if err := c.st.one(ctx, c.st.db, q, rule.args, &n); err != nil {
		return fmt.Errorf("sqlstore: count runs holding a key: %w", err)
	}
	res.HeldIdempotency += int(n)
	if c.retention.Undelivered {
		return nil
	}
	q = "SELECT COUNT(*) FROM {runs} WHERE " + rule.where + " AND idem_active IS NULL AND notified = 0"
	if err := c.st.one(ctx, c.st.db, q, rule.args, &n); err != nil {
		return fmt.Errorf("sqlstore: count undelivered runs: %w", err)
	}
	res.HeldUndelivered += int(n)
	return nil
}

// pruneCandidates reads the next chunk of deletable runs for rule. Runs
// in skip (kept by the batch guard, or unchanged by a delete) are passed
// over; a run of an unfinished batch is added to skip.
func (c *Client) pruneCandidates(ctx context.Context, rule pruneRule, skip map[string]bool, heldBatch *int) ([]FinishedRun, error) {
	for {
		limit := c.pruneChunk + len(skip)
		var read []FinishedRun
		var r runRow
		var created int64
		q := "SELECT " + runCols + ", created_at FROM {runs} WHERE " + rule.where + " AND " + c.guards() + " ORDER BY seq LIMIT ?"
		err := c.st.each(ctx, c.st.db, q, append(append([]any(nil), rule.args...), limit), func() error {
			labels, err := r.labelMap()
			if err != nil {
				c.log.Error("sqlstore: prune reads a run without labels", "run", r.id, "error", err)
			}
			read = append(read, FinishedRun{
				Ref:        workflow.RunRef{RunID: workflow.RunID(r.id), ScheduledID: workflow.ScheduledID(r.sid.String)},
				Workflow:   r.workflow,
				Status:     workflow.Status(r.status),
				Labels:     labels,
				Payload:    append([]byte(nil), r.payload...),
				Err:        r.errMsg.String,
				CreatedAt:  unixTime(created),
				FinishedAt: unixTime(r.finishedAt),
			})
			return nil
		}, append(r.targets(), &created)...)
		if err != nil {
			return nil, fmt.Errorf("sqlstore: read prunable runs: %w", err)
		}
		var kept []FinishedRun
		for _, fr := range read {
			id := string(fr.Ref.RunID)
			if skip[id] || len(kept) == c.pruneChunk {
				continue
			}
			open, err := c.batchOpen(ctx, fr.Labels)
			if err != nil {
				return nil, err
			}
			if open {
				skip[id] = true
				*heldBatch++
				continue
			}
			kept = append(kept, fr)
		}
		if len(kept) > 0 || len(read) < limit {
			return kept, nil // a chunk, or the table has nothing more to offer
		}
	}
}

// batchOpen reports whether labels name a job of a batch kept in this
// client's BatchStore that is still open (State.Open). A batch kept in
// another store cannot be seen here, which is why the package doc tells apps with
// retention on to keep batches in Client.BatchStore.
func (c *Client) batchOpen(ctx context.Context, labels map[string]string) (bool, error) {
	id := labels[batch.LabelBatch]
	if id == "" {
		return false, nil
	}
	st, err := c.BatchStore().Get(ctx, batch.ID(id))
	if err != nil {
		if errors.Is(err, workflow.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return !st.Finished, nil
}

// deleteChunk archives runs and then deletes them with their steps and
// consumed markers in one transaction. The delete repeats the rule and the
// guards, so a run that changed since it was read (its key taken again, its
// end not yet delivered) is left alone.
func (c *Client) deleteChunk(ctx context.Context, rule pruneRule, runs []FinishedRun) (int, error) {
	ids := make([]any, len(runs))
	idx := map[string]int{}
	for i, fr := range runs {
		ids[i] = string(fr.Ref.RunID)
		idx[string(fr.Ref.RunID)] = i
	}
	if c.archive != nil {
		var runID, name string
		var out []byte
		err := c.st.each(ctx, c.st.db, "SELECT run_id, name, out_data FROM {steps} WHERE status = ? AND run_id IN ("+inList(len(ids))+")",
			append([]any{stepSucceeded}, ids...), func() error {
				fr := &runs[idx[runID]]
				if fr.Outputs == nil {
					fr.Outputs = map[string][]byte{}
				}
				fr.Outputs[name] = append([]byte(nil), out...)
				return nil
			}, &runID, &name, &out)
		if err != nil {
			return 0, fmt.Errorf("sqlstore: read step outputs to archive: %w", err)
		}
		if err := c.archive(ctx, runs); err != nil {
			return 0, fmt.Errorf("sqlstore: archive hook refused a chunk of %d runs (kept): %w", len(runs), err)
		}
	}
	var deleted int64
	err := c.st.tx(ctx, func(tx *sql.Tx) error {
		deleted = 0
		in := "(" + inList(len(ids)) + ")"
		cond := "id IN " + in + " AND " + rule.where + " AND " + c.guards()
		args := append(append([]any(nil), ids...), rule.args...)
		if c.archiveTable {
			moved, err := c.moveToArchive(ctx, tx, cond, args)
			if err != nil || len(moved) == 0 {
				return err
			}
			// Exactly the runs copied: they are locked, so the rule still
			// holds for each.
			cond, args = "id IN ("+inList(len(moved))+")", moved
		}
		n, err := c.st.execN(ctx, tx, "DELETE FROM {runs} WHERE "+cond, args...)
		if err != nil {
			return err
		}
		deleted = n
		return c.deleteOrphans(ctx, tx, ids)
	})
	if err != nil {
		return 0, fmt.Errorf("sqlstore: delete a chunk of finished runs: %w", err)
	}
	return int(deleted), nil
}

// deleteOrphans deletes, in tx, the rows that hang off the runs ids once
// those runs are gone: step attempt counts, steps, consumed-signal markers,
// run_ext rows, label index rows, attempt error history and captured output. A run among ids that still exists (a guard kept it) is
// left whole.
func (c *Client) deleteOrphans(ctx context.Context, tx *sql.Tx, ids []any) error {
	in := "(" + inList(len(ids)) + ")"
	gone := " AND run_id NOT IN (SELECT id FROM {runs})"
	for _, q := range []string{
		"DELETE FROM {step_ext} WHERE step_id IN (SELECT id FROM {steps} WHERE run_id IN " + in + gone + ")",
		"DELETE FROM {step_err} WHERE step_id IN (SELECT id FROM {steps} WHERE run_id IN " + in + gone + ")",
		"DELETE FROM {step_cancel} WHERE step_id IN (SELECT id FROM {steps} WHERE run_id IN " + in + gone + ")",
		"DELETE FROM {steps} WHERE run_id IN " + in + gone,
		"DELETE FROM {consumed} WHERE run_id IN " + in + gone,
		"DELETE FROM {run_ext} WHERE run_id IN " + in + gone,
		"DELETE FROM {run_labels} WHERE run_id IN " + in + gone,
		"DELETE FROM {attempt_errors} WHERE run_id IN " + in + gone,
		"DELETE FROM {attempt_err_node} WHERE run_id IN " + in + gone,
		"DELETE FROM {run_info} WHERE run_id IN " + in + gone,
		"DELETE FROM {outputs} WHERE run_id IN " + in + gone,
		"DELETE FROM {run_forced} WHERE run_id IN " + in + gone,
		// A chain waiting for a handoff (next_wf set) outlives its run: the
		// next job has not been enqueued yet.
		"DELETE FROM {run_chain} WHERE run_id IN " + in + gone + " AND next_wf = ''",
	} {
		if _, err := c.st.exec(ctx, tx, q, ids...); err != nil {
			return err
		}
	}
	return nil
}
