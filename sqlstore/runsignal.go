package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/ubgo/workflow"
)

var _ workflow.RunSignalReader = (*Client)(nil)

// PendingSignals lists the signals addressed to run (workflow.RunKey)
// with one of names that the run may still take: rows of the signals
// table above the run's sig_floor with no consumed row for the run,
// oldest first (workflow.RunSignalReader). The ID is the signal row's id.
// It reads the (name, ev_key) index, so no migration was needed. A
// finished or unknown run has none.
func (c *Client) PendingSignals(ctx context.Context, run workflow.RunID, names []string, limit int) ([]workflow.RunSignal, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.usable(); err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return nil, nil
	}
	row, err := c.loadRun(ctx, c.st.db, "id", string(run))
	if errors.Is(err, workflow.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("sqlstore: pending signals: %w", err)
	}
	if !openRun(row.status) {
		return nil, nil
	}
	q := `SELECT id, name, payload FROM {signals} WHERE ev_key = ? AND name IN (?` + strings.Repeat(", ?", len(names)-1) + `)
		AND id > ? AND id NOT IN (SELECT signal_id FROM {consumed} WHERE run_id = ?) ORDER BY id`
	args := []any{workflow.RunKey(run)}
	for _, n := range names {
		args = append(args, n)
	}
	args = append(args, row.sigFloor, row.id)
	if limit > 0 {
		q += " LIMIT " + strconv.Itoa(limit)
	}
	var out []workflow.RunSignal
	var id int64
	var sig workflow.RunSignal
	err = c.st.each(ctx, c.st.db, q, args, func() error {
		sig.ID = strconv.FormatInt(id, 10)
		out = append(out, sig)
		sig = workflow.RunSignal{}
		return nil
	}, &id, &sig.Name, &sig.Payload)
	if err != nil {
		return nil, fmt.Errorf("sqlstore: pending signals: %w", err)
	}
	return out, nil
}

// TakeSignal marks the signal id taken by run with a consumed row, the
// record a wait step taking it writes (workflow.RunSignalReader). The
// run's row is locked first (touch), so a take and a wait step's
// delivery of the same signal never both win.
func (c *Client) TakeSignal(ctx context.Context, run workflow.RunID, id string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := c.usable(); err != nil {
		return false, err
	}
	sigID, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return false, fmt.Errorf("%w: signal %q", workflow.ErrNotFound, id)
	}
	took := false
	err = c.st.tx(ctx, func(tx *sql.Tx) error {
		took = false
		row, err := c.touch(ctx, tx, string(run))
		if err != nil {
			return err
		}
		var n int64
		err = c.st.one(ctx, tx, "SELECT COUNT(*) FROM {signals} WHERE id = ? AND ev_key = ? AND id > ?",
			[]any{sigID, workflow.RunKey(run), row.sigFloor}, &n)
		if err != nil {
			return err
		}
		if n == 0 || !openRun(row.status) {
			return fmt.Errorf("%w: signal %q for run %s", workflow.ErrNotFound, id, run)
		}
		err = c.st.one(ctx, tx, "SELECT COUNT(*) FROM {consumed} WHERE run_id = ? AND signal_id = ?", []any{row.id, sigID}, &n)
		if err != nil || n > 0 {
			return err
		}
		if _, err := c.st.exec(ctx, tx, "INSERT INTO {consumed} (run_id, signal_id) VALUES (?, ?)", row.id, sigID); err != nil {
			return err
		}
		took = true
		return nil
	})
	if err != nil {
		if errors.Is(err, workflow.ErrNotFound) {
			return false, err
		}
		return false, fmt.Errorf("sqlstore: take signal: %w", err)
	}
	return took, nil
}

// openRun reports whether a run in status may still take signals.
func openRun(status string) bool {
	switch status {
	case runScheduled, runPending, runActive, runFailing:
		return true
	}
	return false
}
