package sqlstore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/ubgo/workflow"
)

var _ workflow.TransientSignaller = (*Client)(nil)

// SignalWaiting resumes the runs whose wait step is suspended on ev's name
// and key now, and keeps ev for no one else (workflow.TransientSignaller):
// no signal row is written, so a run that begins its wait a moment later,
// on any client, never sees ev. Like the delivery of a kept signal, it
// resumes runs of the workflows this client registered; a run of a
// workflow only another client registered is not resumed, so call it on a
// worker client.
//
//ds:def id=sql-client-signalwaiting-8ssy9v7j owner=@khanakia stability=stable
func (c *Client) SignalWaiting(ctx context.Context, ev workflow.Event) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := c.usable(); err != nil {
		return 0, err
	}
	if len(c.names) == 0 {
		return 0, nil
	}
	type cand struct{ runID, step string }
	var cands []cand
	var cd cand
	err := c.st.each(ctx, c.st.db, `SELECT st.run_id, st.name FROM {steps} st JOIN {runs} r ON r.id = st.run_id
		WHERE st.status = ? AND st.wait_event = ? AND st.wait_key = ? AND st.`+c.inNames()+` AND r.status = ?
		ORDER BY r.seq`,
		append(append([]any{stepWaiting, ev.Name, ev.Key}, c.nameArgs()...), runActive),
		func() error { cands = append(cands, cd); return nil }, &cd.runID, &cd.step)
	if err != nil {
		return 0, fmt.Errorf("sqlstore: find waiting steps: %w", err)
	}
	n := 0
	for _, cand := range cands {
		var o txOut
		took := false
		err := c.st.tx(ctx, func(tx *sql.Tx) error {
			o, took = txOut{}, false
			row, err := c.touch(ctx, tx, cand.runID)
			if err != nil || row.status != runActive {
				return err
			}
			reg := c.reg(row.workflow)
			if reg == nil {
				return nil
			}
			var st, event, key string
			if err := c.st.one(ctx, tx, "SELECT status, wait_event, wait_key FROM {steps} WHERE run_id = ? AND name = ?",
				[]any{cand.runID, cand.step}, &st, &event, &key); err != nil {
				return err
			}
			if st != stepWaiting || event != ev.Name || key != ev.Key {
				return nil // resumed or timed out since the query
			}
			took = true
			return c.completeStep(ctx, tx, reg, row, cand.step, ev.Payload, &o)
		})
		if err != nil {
			return n, err
		}
		c.afterTx(ctx, o)
		if took {
			n++
		}
	}
	if n > 0 {
		c.nudge()
	}
	return n, nil
}
