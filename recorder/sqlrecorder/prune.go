package sqlrecorder

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/recorder"
)

// Prune implements recorder.Pruner with the store's Retention (WithRetention),
// in batches of Retention.BatchSize so a backlog never holds one long
// transaction:
//
//   - Stage two first: finished runs past their Runs age are read in full
//     (steps and events included), handed to Archive when set, and deleted
//     with their steps, events and labels. An Archive error stops the prune
//     with that batch untouched.
//   - Then stage one: finished runs past their Steps age lose their step
//     and event rows; the run row stays.
//
// Each rule is applied to its workflows: a PerWorkflow entry to its name,
// the default rule to every workflow without one. A zero age keeps that
// stage. Unfinished runs are never touched.
func (s *Store) Prune(ctx context.Context) (recorder.PruneResult, error) {
	var res recorder.PruneResult
	if s.retention.IsZero() {
		return res, nil
	}
	now := s.clk.Now()
	named := slices.Sorted(maps.Keys(s.retention.PerWorkflow))
	type scope struct {
		rule recorder.RetentionRule
		// workflow is the one workflow of a PerWorkflow rule; empty for the
		// default rule, which covers every workflow not in named.
		workflow string
	}
	scopes := []scope{{rule: s.retention.RetentionRule}}
	for _, wf := range named {
		scopes = append(scopes, scope{rule: s.retention.PerWorkflow[wf], workflow: wf})
	}
	for _, sc := range scopes {
		if sc.rule.Runs > 0 {
			n, archived, err := s.pruneRuns(ctx, now.Add(-sc.rule.Runs), sc.workflow, named)
			res.RunsDeleted += n
			res.Archived += archived
			if err != nil {
				return res, err
			}
		}
	}
	for _, sc := range scopes {
		if sc.rule.Steps > 0 {
			n, err := s.pruneSteps(ctx, now.Add(-sc.rule.Steps), sc.workflow, named)
			res.StepsCleared += n
			if err != nil {
				return res, err
			}
		}
	}
	return res, nil
}

// scopeWhere is the condition picking finished runs ended at or before
// cutoff, of workflow wf (or, for wf "", of every workflow not in named).
func (s *Store) scopeWhere(a *args, cutoff time.Time, wf string, named []string) string {
	col := func(c string) string { return "r." + s.d.quote(c) }
	conds := []string{
		col("status") + " IN " + in(a, terminalStatuses),
		col("finished_at") + " <> 0",
		col("finished_at") + " <= " + a.add(nanos(cutoff)),
	}
	switch {
	case wf != "":
		conds = append(conds, col("workflow")+" = "+a.add(wf))
	case len(named) > 0:
		conds = append(conds, col("workflow")+" NOT IN "+in(a, named))
	}
	return strings.Join(conds, " AND ")
}

// pruneRuns archives and deletes, batch by batch, the runs scopeWhere
// picks. It returns how many it deleted and archived.
func (s *Store) pruneRuns(ctx context.Context, cutoff time.Time, wf string, named []string) (deleted, archived int, err error) {
	batch := s.retention.BatchSize()
	for {
		a := &args{d: s.d}
		stmt := "SELECT r." + s.d.quote("run_key") + ", r." + s.d.quote("run_id") + " FROM " + s.table(tableRuns) + " r WHERE " +
			s.scopeWhere(a, cutoff, wf, named) + " ORDER BY r." + s.d.quote("finished_at") + ", r." + s.d.quote("run_key") + " LIMIT " + fmt.Sprint(batch)
		victims, err := s.keys(ctx, stmt, a.vals)
		if err != nil || len(victims) == 0 {
			return deleted, archived, err
		}
		if s.retention.Archive != nil {
			recs := make([]recorder.RunRecord, 0, len(victims))
			for _, v := range victims {
				rec, err := s.Run(ctx, v.ref)
				if err != nil {
					return deleted, archived, err
				}
				recs = append(recs, rec)
			}
			if err := s.retention.Archive(ctx, recs); err != nil {
				return deleted, archived, fmt.Errorf("sqlrecorder: archive: %w", err)
			}
			archived += len(recs)
		}
		err = s.inTx(ctx, func(tx *sql.Tx) error {
			for _, v := range victims {
				if err := s.deleteRun(ctx, tx, v); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return deleted, archived, fmt.Errorf("sqlrecorder: prune runs: %w", err)
		}
		deleted += len(victims)
	}
}

// pruneSteps drops, batch by batch, the step and event rows of the runs
// scopeWhere picks that still have steps. It returns how many runs it
// cleared.
func (s *Store) pruneSteps(ctx context.Context, cutoff time.Time, wf string, named []string) (int, error) {
	batch := s.retention.BatchSize()
	cleared := 0
	q := s.d.quote
	for {
		a := &args{d: s.d}
		stmt := "SELECT r." + q("run_key") + ", r." + q("run_id") + " FROM " + s.table(tableRuns) + " r WHERE " +
			s.scopeWhere(a, cutoff, wf, named) + " AND r." + q("run_id") + " IS NOT NULL AND EXISTS (SELECT 1 FROM " + s.table(tableSteps) +
			" st WHERE st." + q("run_id") + " = r." + q("run_id") + ") ORDER BY r." + q("finished_at") + ", r." + q("run_key") + " LIMIT " + fmt.Sprint(batch)
		victims, err := s.keys(ctx, stmt, a.vals)
		if err != nil || len(victims) == 0 {
			return cleared, err
		}
		err = s.inTx(ctx, func(tx *sql.Tx) error {
			for _, v := range victims {
				if err := s.deleteSteps(ctx, tx, v.ref.RunID); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return cleared, fmt.Errorf("sqlrecorder: prune steps: %w", err)
		}
		cleared += len(victims)
	}
}

// victim is one run a prune picked.
type victim struct {
	key string
	ref workflow.RunRef
}

// keys runs a (run_key, run_id) query.
func (s *Store) keys(ctx context.Context, stmt string, vals []any) ([]victim, error) {
	rows, err := s.db.QueryContext(ctx, stmt, vals...)
	if err != nil {
		return nil, fmt.Errorf("sqlrecorder: prune scan: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []victim
	for rows.Next() {
		var v victim
		var runID sql.NullString
		if err := rows.Scan(&v.key, &runID); err != nil {
			return nil, err
		}
		v.ref = workflow.RunRef{RunID: workflow.RunID(runID.String)}
		if v.ref.RunID.IsZero() {
			// A run known only by its ScheduledID: read it by key.
			v.ref = workflow.RunRef{ScheduledID: workflow.ScheduledID(strings.TrimPrefix(v.key, "s:"))}
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// deleteSteps removes a run's step and event rows.
func (s *Store) deleteSteps(ctx context.Context, tx *sql.Tx, runID workflow.RunID) error {
	for _, t := range []string{tableEvents, tableSteps} {
		a := &args{d: s.d}
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+s.table(t)+" WHERE "+s.d.quote("run_id")+" = "+a.add(string(runID)), a.vals...); err != nil {
			return err
		}
	}
	return nil
}

// deleteRun removes a run's row, labels, steps and events.
func (s *Store) deleteRun(ctx context.Context, tx *sql.Tx, v victim) error {
	if !v.ref.RunID.IsZero() {
		if err := s.deleteSteps(ctx, tx, v.ref.RunID); err != nil {
			return err
		}
	}
	if err := s.writeLabels(ctx, tx, v.key, "", nil); err != nil {
		return err
	}
	a := &args{d: s.d}
	_, err := tx.ExecContext(ctx, "DELETE FROM "+s.table(tableRuns)+" WHERE "+s.d.quote("run_key")+" = "+a.add(v.key), a.vals...)
	return err
}
