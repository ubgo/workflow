package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ubgo/workflow"
)

// Schedule overlap (workflow.OverlapSkip), the last fire of each schedule
// (ScheduleInfo.Last) and running a schedule now (ScheduleTriggerer) share
// one row per schedule in schedule_last (migration 006). Every fire, tick
// or trigger, moves last_fire and last_run there in the transaction that
// enqueues the run. Under OverlapSkip that move is a compare-and-set on
// the last_run the overlap decision read, so a tick and a trigger deciding
// at once on two nodes cannot both start a run: the second one's
// transaction finds the row moved and rolls back.

// errLastMoved rolls back a fire whose schedule another fire moved on
// since its overlap decision.
var errLastMoved = errors.New("sqlstore: the schedule fired elsewhere since the overlap decision")

// ensureScheduleLast inserts schedule id's schedule_last row when it is
// missing (a schedule written by a client older than 006 after the
// backfill ran). Same shape as ensureScheduleExt: the update comes first
// so the transaction opens with a write.
func (c *Client) ensureScheduleLast(ctx context.Context, tx *sql.Tx, id string) error {
	n, err := c.st.execN(ctx, tx, "UPDATE {schedule_last} SET schedule_id = schedule_id WHERE schedule_id = ?", id)
	if err != nil || n > 0 {
		return err
	}
	var have int64
	if err := c.st.one(ctx, tx, "SELECT COUNT(*) FROM {schedule_last} WHERE schedule_id = ?", []any{id}, &have); err != nil {
		return err
	}
	if have > 0 {
		return nil // MySQL counts an update that changes nothing as zero rows
	}
	_, err = c.st.exec(ctx, tx, "INSERT INTO {schedule_last} (schedule_id, overlap, last_fire, last_run) VALUES (?, '', 0, '')", id)
	return err
}

// writeOverlap stores p as schedule id's overlap policy in tx, keeping its
// last fire.
func (c *Client) writeOverlap(ctx context.Context, tx *sql.Tx, id string, p workflow.OverlapPolicy) error {
	if err := c.ensureScheduleLast(ctx, tx, id); err != nil {
		return err
	}
	if p == workflow.OverlapAllow {
		p = "" // one spelling of the default in the table
	}
	_, err := c.st.exec(ctx, tx, "UPDATE {schedule_last} SET overlap = ? WHERE schedule_id = ?", string(p), id)
	return err
}

// moveLast records a fire of schedule id at fire (Unix nanoseconds) that
// enqueued run, in tx. Under OverlapSkip it is a compare-and-set on prev,
// the last_run the overlap decision read, and returns errLastMoved when
// another fire moved it first.
func (c *Client) moveLast(ctx context.Context, tx *sql.Tx, d dueSchedule, fire int64, run string) error {
	if err := c.ensureScheduleLast(ctx, tx, d.id); err != nil {
		return err
	}
	q, args := "UPDATE {schedule_last} SET last_fire = ?, last_run = ? WHERE schedule_id = ?", []any{fire, run, d.id}
	if d.overlap == workflow.OverlapSkip {
		q, args = q+" AND last_run = ?", append(args, d.lastRun)
	}
	n, err := c.st.execN(ctx, tx, q, args...)
	if err != nil {
		return err
	}
	if n == 0 {
		return errLastMoved
	}
	return nil
}

// prevStatus is the status of a schedule's last run for an overlap
// decision: "" when it never fired or the run is gone (deleted or pruned),
// which does not block.
func (c *Client) prevStatus(ctx context.Context, runID string) (workflow.Status, error) {
	if runID == "" {
		return "", nil
	}
	st, err := c.Status(ctx, workflow.RunRef{RunID: workflow.RunID(runID)})
	if errors.Is(err, workflow.ErrNotFound) {
		return "", nil
	}
	return st, err
}

// overlapBlocks decides whether d's overlap policy holds a fire back now,
// and returns the previous run's status for the refusal.
func (c *Client) overlapBlocks(ctx context.Context, d dueSchedule) (bool, workflow.Status, error) {
	if d.overlap != workflow.OverlapSkip {
		return false, "", nil
	}
	st, err := c.prevStatus(ctx, d.lastRun)
	if err != nil {
		return false, "", fmt.Errorf("sqlstore: read the previous run of schedule %s: %w", d.id, err)
	}
	return d.overlap.Blocks(st), st, nil
}

// TriggerSchedule runs schedule id now (workflow.ScheduleTriggerer): one
// run with the schedule's input and labels plus workflow.TriggeredLabels.
// It obeys the schedule's overlap policy (an unfinished previous run
// refuses it with an error matching workflow.ErrOverlap, enqueueing
// nothing), ignores its windows and Workflow.ScheduleWhen, works while the
// schedule is paused, becomes the schedule's last fire and leaves its next
// fire time as it was. An unknown or unscheduled id returns ErrNotFound,
// and so does a schedule whose workflow is not registered on this client.
// Tick observers hear a TickFired with Manual set.
func (c *Client) TriggerSchedule(ctx context.Context, id workflow.ScheduleID) (workflow.RunRef, error) {
	if err := ctx.Err(); err != nil {
		return workflow.RunRef{}, err
	}
	if err := c.usable(); err != nil {
		return workflow.RunRef{}, err
	}
	for range c.st.txAttempts {
		d, err := c.loadSchedule(ctx, string(id))
		if err != nil {
			return workflow.RunRef{}, err
		}
		reg := c.reg(d.workflow)
		if reg == nil {
			return workflow.RunRef{}, fmt.Errorf("%w: schedule %s: workflow %q is not registered on this client", workflow.ErrNotFound, id, d.workflow)
		}
		blocked, st, err := c.overlapBlocks(ctx, d)
		if err != nil {
			return workflow.RunRef{}, err
		}
		if blocked {
			return workflow.RunRef{}, workflow.OverlapRefusal(id, workflow.RunRef{RunID: workflow.RunID(d.lastRun)}, st)
		}
		var labels map[string]string
		if err := json.Unmarshal([]byte(d.labels), &labels); err != nil {
			c.log.Error("sqlstore: stored schedule labels do not decode", "schedule", d.id, "error", err)
		}
		runID, err := c.st.newID(IDRun)
		if err != nil {
			return workflow.RunRef{}, err
		}
		now := c.clk.Now()
		var ref workflow.RunRef
		err = c.st.tx(ctx, func(tx *sql.Tx) error {
			if err := c.moveLast(ctx, tx, d, now.UnixNano(), runID); err != nil {
				return err
			}
			job := workflow.Job{Workflow: d.workflow, Payload: d.input, Labels: workflow.TriggeredLabelsAt(labels, id, d.tickName(), c.clk.Now()), Tenant: d.tenant}
			row, err := c.insertRun(ctx, tx, reg, job, "", runID)
			if err != nil {
				return err
			}
			ref = row.ref()
			return nil
		})
		if errors.Is(err, errLastMoved) || c.st.isUnique(err) {
			continue // another fire landed first: decide again on the new last run
		}
		if err != nil {
			return workflow.RunRef{}, err
		}
		if err := c.admit(ctx, reg); err != nil {
			return ref, err
		}
		c.nudge()
		loc, lerr := decodeLocation(d.loc)
		if lerr != nil {
			loc = now.Location()
		}
		c.emitTicks(ctx, []workflow.Tick{{ScheduleID: id, Name: d.tickName(), Workflow: d.workflow, Declared: d.declared,
			At: now.In(loc), Location: loc, Labels: labels, Outcome: workflow.TickFired, Run: ref, Manual: true}})
		return ref, nil
	}
	return workflow.RunRef{}, fmt.Errorf("sqlstore: schedule %s kept firing elsewhere while it was triggered; try again", id)
}

// loadSchedule reads one live schedule; an unknown or unscheduled id is
// ErrNotFound.
func (c *Client) loadSchedule(ctx context.Context, id string) (dueSchedule, error) {
	var r scheduleScan
	err := c.st.one(ctx, c.st.db, "SELECT "+scheduleCols+scheduleFrom+" WHERE s.id = ? AND s.active = 1", []any{id}, r.targets()...)
	if isNoRows(err) {
		return dueSchedule{}, fmt.Errorf("%w: schedule %s", workflow.ErrNotFound, id)
	}
	if err != nil {
		return dueSchedule{}, fmt.Errorf("sqlstore: read schedule %s: %w", id, err)
	}
	return r.row(c.log.Error), nil
}
