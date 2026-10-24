package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/workflow"
)

// The kinds of row in the pauses table. The first three are Pauser's
// targets; pauseSchedules is SchedulePauser's "every schedule", which also
// holds schedules made after the pause.
const (
	pauseAll       = "all"
	pauseWorkflow  = "workflow"
	pauseQueue     = "queue"
	pauseSchedules = "schedules"
)

// pauseRow is the (kind, name) row of a Pauser target.
func pauseRow(t workflow.PauseTarget) (kind, name string) {
	switch {
	case t.All:
		return pauseAll, ""
	case t.Workflow != "":
		return pauseWorkflow, t.Workflow
	}
	return pauseQueue, t.Queue
}

// Pause holds the runs t targets (workflow.Pauser): from the next claim on,
// on every client of this database, no step attempt of a held run starts.
// The pause is a row, so it survives restarts; Pause on a paused target is
// a no-op. It works before Start too.
func (c *Client) Pause(ctx context.Context, t workflow.PauseTarget) error {
	if err := workflow.CheckPause(caps, t); err != nil {
		return err
	}
	kind, name := pauseRow(t)
	_, err := c.st.exec(ctx, c.st.db, "INSERT INTO {pauses} (kind, name) VALUES (?, ?)", kind, name)
	if err != nil && !c.st.isUnique(err) {
		return err
	}
	return nil
}

// Resume removes the pause of t; Resume(workflow.PauseAll()) removes every
// Pauser pause. Resuming a target that is not paused is a no-op. Held runs
// are claimed at once on this client and within the poll interval on the
// others.
func (c *Client) Resume(ctx context.Context, t workflow.PauseTarget) error {
	if err := workflow.CheckPause(caps, t); err != nil {
		return err
	}
	var err error
	if t.All {
		_, err = c.st.exec(ctx, c.st.db, "DELETE FROM {pauses} WHERE kind IN (?, ?, ?)", pauseAll, pauseWorkflow, pauseQueue)
	} else {
		kind, name := pauseRow(t)
		_, err = c.st.exec(ctx, c.st.db, "DELETE FROM {pauses} WHERE kind = ? AND name = ?", kind, name)
	}
	if err != nil {
		return err
	}
	c.nudge()
	return nil
}

// Paused lists the Pauser targets in force: All first, then workflows,
// then queues, each by name.
func (c *Client) Paused(ctx context.Context) ([]workflow.PauseTarget, error) {
	var out []workflow.PauseTarget
	var kind, name string
	err := c.st.each(ctx, c.st.db, "SELECT kind, name FROM {pauses} WHERE kind IN (?, ?, ?)", []any{pauseAll, pauseWorkflow, pauseQueue},
		func() error {
			switch kind {
			case pauseAll:
				out = append(out, workflow.PauseAll())
			case pauseWorkflow:
				out = append(out, workflow.PauseWorkflow(name))
			case pauseQueue:
				out = append(out, workflow.PauseQueue(name))
			}
			return nil
		}, &kind, &name)
	if err != nil {
		return nil, fmt.Errorf("sqlstore: read pauses: %w", err)
	}
	rank := func(t workflow.PauseTarget) (int, string) {
		switch {
		case t.All:
			return 0, ""
		case t.Workflow != "":
			return 1, t.Workflow
		}
		return 2, t.Queue
	}
	sort.Slice(out, func(i, j int) bool {
		ri, ni := rank(out[i])
		rj, nj := rank(out[j])
		if ri != rj {
			return ri < rj
		}
		return ni < nj
	})
	return out, nil
}

// PauseSchedule stops a schedule, or with All every schedule (including
// ones made later), from firing without removing it
// (workflow.SchedulePauser). Its due ticks are reported as TickSkipped
// with TickReasonPaused, once each, by the node that sees them. The pause
// is a row, so every node obeys it and it survives restarts. An unknown ID
// returns ErrNotFound.
func (c *Client) PauseSchedule(ctx context.Context, t workflow.ScheduleTarget) error {
	return c.setSchedulePause(ctx, t, true)
}

// ResumeSchedule lets a paused schedule fire again; with All it clears
// every schedule pause. The ticks that fell due during the pause
// never fire: they are reported skipped (if no dispatcher pass did so
// already) and the schedule resumes at its next tick. Resuming a
// schedule that is not paused is a no-op; an unknown ID returns
// ErrNotFound.
func (c *Client) ResumeSchedule(ctx context.Context, t workflow.ScheduleTarget) error {
	return c.setSchedulePause(ctx, t, false)
}

func (c *Client) setSchedulePause(ctx context.Context, t workflow.ScheduleTarget, paused bool) error {
	if err := workflow.ValidateScheduleTarget(t); err != nil {
		return err
	}
	if !paused {
		if err := c.skipPausedTicks(ctx, t); err != nil {
			return err
		}
	}
	if t.All {
		var err error
		if paused {
			_, err = c.st.exec(ctx, c.st.db, "INSERT INTO {pauses} (kind, name) VALUES (?, '')", pauseSchedules)
			if c.st.isUnique(err) {
				err = nil
			}
		} else {
			err = c.st.tx(ctx, func(tx *sql.Tx) error {
				if _, err := c.st.exec(ctx, tx, "DELETE FROM {pauses} WHERE kind = ?", pauseSchedules); err != nil {
					return err
				}
				_, err := c.st.exec(ctx, tx, "UPDATE {schedule_ext} SET paused = 0")
				return err
			})
		}
		if err == nil {
			c.nudge()
		}
		return err
	}
	id := string(t.ID)
	var n int64
	if err := c.st.one(ctx, c.st.db, "SELECT COUNT(*) FROM {schedules} WHERE id = ?", []any{id}, &n); err != nil {
		return fmt.Errorf("sqlstore: read schedule: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("%w: schedule %q", workflow.ErrNotFound, id)
	}
	err := c.withScheduleExt(ctx, id, func(tx *sql.Tx) error {
		_, err := c.st.exec(ctx, tx, "UPDATE {schedule_ext} SET paused = ? WHERE schedule_id = ?", flag(paused), id)
		return err
	})
	if err == nil {
		c.nudge()
	}
	return err
}

// withScheduleExt runs fn in a transaction in which schedule id has a
// schedule_ext row (inserted with the defaults when missing). Two nodes
// inserting it at once: the loser's transaction is tried again and finds
// the row.
func (c *Client) withScheduleExt(ctx context.Context, id string, fn func(tx *sql.Tx) error) error {
	var err error
	for range c.st.txAttempts {
		err = c.st.tx(ctx, func(tx *sql.Tx) error {
			if err := c.ensureScheduleExt(ctx, tx, id); err != nil {
				return err
			}
			return fn(tx)
		})
		if !c.st.isUnique(err) || ctx.Err() != nil {
			return err
		}
	}
	return err
}

// ensureScheduleExt inserts schedule id's schedule_ext row when it is
// missing. The update comes first so the transaction opens with a write
// (SQLite then takes its write lock up front).
func (c *Client) ensureScheduleExt(ctx context.Context, tx *sql.Tx, id string) error {
	n, err := c.st.execN(ctx, tx, "UPDATE {schedule_ext} SET schedule_id = schedule_id WHERE schedule_id = ?", id)
	if err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	var have int64
	if err := c.st.one(ctx, tx, "SELECT COUNT(*) FROM {schedule_ext} WHERE schedule_id = ?", []any{id}, &have); err != nil {
		return err
	}
	if have > 0 {
		// MySQL reports zero rows changed for an update that changes no
		// value; the row is there.
		return nil
	}
	_, err = c.st.exec(ctx, tx, "INSERT INTO {schedule_ext} (schedule_id, windows, paused) VALUES (?, '', 0)", id)
	return err
}

// schedulesPaused reports whether SchedulePauser's All pause is in force.
func (c *Client) schedulesPaused(ctx context.Context) (bool, error) {
	var n int64
	err := c.st.one(ctx, c.st.db, "SELECT COUNT(*) FROM {pauses} WHERE kind = ?", []any{pauseSchedules}, &n)
	if errors.Is(err, sqlb.ErrNoRows) {
		return false, nil
	}
	return n > 0, err
}

// skipPausedTicks moves every schedule t resumes past the ticks that fell
// due while it was paused (reportPaused), before the pause is cleared, so
// a resume that lands before a dispatcher pass saw those ticks cannot fire
// them. A schedule still held by another pause (All) is left to it.
func (c *Client) skipPausedTicks(ctx context.Context, t workflow.ScheduleTarget) error {
	allPaused, err := c.schedulesPaused(ctx)
	if err != nil {
		return err
	}
	where, args := " AND x.paused = 1", []any{}
	switch {
	case t.All && allPaused:
		where = ""
	case !t.All && allPaused:
		return nil // still paused by All
	}
	if !t.All {
		where += " AND s.id = ?"
		args = append(args, string(t.ID))
	}
	now := c.clk.Now()
	var list []dueSchedule
	var r scheduleScan
	err = c.st.each(ctx, c.st.db, "SELECT "+scheduleCols+scheduleFrom+" WHERE s.active = 1 AND s.next_fire <= ?"+where,
		append([]any{now.UnixNano()}, args...), func() error { list = append(list, r.row(c.log.Error)); return nil }, r.targets()...)
	if err != nil {
		return fmt.Errorf("sqlstore: read paused schedules: %w", err)
	}
	for _, d := range list {
		if err := c.reportPaused(ctx, d, now); err != nil {
			return err
		}
	}
	return nil
}
