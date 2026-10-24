package sqlstore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/ubgo/workflow"
)

// WithErrorHistory bounds RunInfo.Errors, the history of failed attempts
// each run keeps (workflow.CapErrorHistory): the newest n, oldest first.
// Without it a run keeps workflow.DefaultErrorHistory; n <= 0 turns the
// history off (nothing is written, and rows written before stay until the
// run is deleted). Every failed attempt is recorded, retried, final or
// discarded; an attempt cut short by a clean stop is not a failure and is
// not recorded.
func WithErrorHistory(n int) Option {
	return func(cl *Client) { cl.errHistory = n }
}

// addAttemptError appends a's failure to its run's error history in tx and
// drops the entries past the bound, so a run retried for a week keeps a
// fixed number of rows.
func (c *Client) addAttemptError(ctx context.Context, tx *sql.Tx, a *attempt, herr error) error {
	if c.errHistory <= 0 {
		return nil
	}
	e := workflow.NewAttemptError(a.name, int(a.attempts), herr, c.clk.Now())
	if _, err := c.st.exec(ctx, tx, "INSERT INTO {attempt_errors} (run_id, step, attempt, err, stack, at) VALUES (?, ?, ?, ?, ?, ?)",
		a.runID, e.Step, e.Attempt, e.Error, e.Stack, e.At.UnixNano()); err != nil {
		return fmt.Errorf("sqlstore: record an attempt error: %w", err)
	}
	if _, err := c.st.exec(ctx, tx, "DELETE FROM {attempt_err_node} WHERE run_id = ? AND step = ? AND attempt = ?", a.runID, e.Step, e.Attempt); err != nil {
		return err
	}
	if _, err := c.st.exec(ctx, tx, "INSERT INTO {attempt_err_node} (run_id, step, attempt, node) VALUES (?, ?, ?, ?)",
		a.runID, e.Step, e.Attempt, c.nodeID); err != nil {
		return fmt.Errorf("sqlstore: record an attempt's node: %w", err)
	}
	var cut int64
	err := c.st.one(ctx, tx, "SELECT id FROM {attempt_errors} WHERE run_id = ? ORDER BY id DESC LIMIT 1 OFFSET ?", []any{a.runID, c.errHistory}, &cut)
	if isNoRows(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("sqlstore: read the error history bound: %w", err)
	}
	_, err = c.st.exec(ctx, tx, "DELETE FROM {attempt_errors} WHERE run_id = ? AND id <= ?", a.runID, cut)
	return err
}

// readAttemptErrors fills RunInfo.Errors of infos, oldest first, in one
// statement.
func (c *Client) readAttemptErrors(ctx context.Context, infos []workflow.RunInfo, ids []any) error {
	at := make(map[string]int, len(infos))
	for i, info := range infos {
		at[string(info.Ref.RunID)] = i
	}
	var runID, step, msg, stack string
	var node sql.NullString
	var attempt, when int64
	err := c.st.each(ctx, c.st.db, "SELECT e.run_id, e.step, e.attempt, e.err, e.stack, e.at, n.node FROM {attempt_errors} e"+
		" LEFT JOIN {attempt_err_node} n ON n.run_id = e.run_id AND n.step = e.step AND n.attempt = e.attempt"+
		" WHERE e.run_id IN ("+inList(len(ids))+") ORDER BY e.run_id, e.id",
		ids, func() error {
			i, ok := at[runID]
			if !ok {
				return nil
			}
			infos[i].Errors = append(infos[i].Errors, workflow.AttemptError{Step: step, Attempt: int(attempt), Error: msg, Stack: stack,
				At: unixTime(when), Node: node.String})
			return nil
		}, &runID, &step, &attempt, &msg, &stack, &when, &node)
	if err != nil {
		return fmt.Errorf("sqlstore: read error history: %w", err)
	}
	return nil
}
