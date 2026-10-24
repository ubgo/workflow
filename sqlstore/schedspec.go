package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/cron"
)

// tblScheduleSpec is migration 008's table of the schedule settings with a
// non-default value: a seconds field, an active period, a run history
// bound. A schedule without a row uses the defaults.
const tblScheduleSpec = "schedule_spec"

// scheduleSpecDefs is the schedule_spec table of migration 008.
func scheduleSpecDefs(t sqlTypes) []tableDef {
	return []tableDef{{stem: tblScheduleSpec, lines: []string{
		"schedule_id " + t.id + " NOT NULL PRIMARY KEY",
		// seconds is 1 when the expression has a seconds field first
		// (Schedule.Seconds, parsed with cron.ParseSeconds).
		"seconds " + t.int + " NOT NULL",
		// active_from and active_until are Schedule.Active, Unix
		// nanoseconds; 0: unbounded on that side.
		"active_from " + t.int + " NOT NULL",
		"active_until " + t.int + " NOT NULL",
		// keep_runs is Schedule.KeepRuns; 0 keeps every run.
		"keep_runs " + t.int + " NOT NULL",
	}}}
}

// scheduleSpec is one schedule's schedule_spec settings.
type scheduleSpec struct {
	seconds bool
	active  workflow.Active
	keep    int
}

func (s scheduleSpec) isZero() bool { return !s.seconds && s.active.IsZero() && s.keep == 0 }

// specOf is a Schedule's settings; declaredSpec a workflow's declared
// crons'.
func specOf(s workflow.Schedule) scheduleSpec {
	return scheduleSpec{seconds: s.Seconds, active: s.Active, keep: s.KeepRuns}
}

func declaredSpec(wf workflow.Workflow) scheduleSpec {
	return scheduleSpec{seconds: wf.CronSeconds, active: wf.CronActive, keep: wf.CronKeepRuns}
}

// writeSpec stores spec as schedule id's settings in tx: no row for the
// defaults. The delete comes first so the transaction opens with a write.
func (c *Client) writeSpec(ctx context.Context, tx *sql.Tx, id string, spec scheduleSpec) error {
	if _, err := c.st.exec(ctx, tx, "DELETE FROM {schedule_spec} WHERE schedule_id = ?", id); err != nil {
		return err
	}
	if spec.isZero() {
		return nil
	}
	sec := 0
	if spec.seconds {
		sec = 1
	}
	_, err := c.st.exec(ctx, tx, "INSERT INTO {schedule_spec} (schedule_id, seconds, active_from, active_until, keep_runs) VALUES (?, ?, ?, ?, ?)",
		id, sec, nanosOrZero(spec.active.From), nanosOrZero(spec.active.Until), spec.keep)
	return err
}

func nanosOrZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func timeOrZero(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return unixTime(n)
}

// parseFor parses expr as spec says: with the seconds field
// (cron.ParseSeconds) or with the client's parser, then limited to the
// active period.
func (c *Client) parseFor(expr string, spec scheduleSpec) (cron.Schedule, error) {
	var s cron.Schedule
	var err error
	if spec.seconds {
		s, err = cron.ParseSeconds(expr)
	} else {
		s, err = c.parseCron(expr)
	}
	if err != nil {
		return nil, err
	}
	if s == nil {
		return nil, fmt.Errorf("cron parser returned no schedule for %q", expr)
	}
	if spec.active.IsZero() {
		return s, nil
	}
	return activeSchedule{Schedule: s, active: spec.active}, nil
}

// activeSchedule limits a cron schedule to an Active period, so the
// firing, the misfire plan and Schedules see only ticks inside it.
type activeSchedule struct {
	cron.Schedule
	active workflow.Active
}

// Next is the first tick after `after` inside the period.
func (a activeSchedule) Next(after time.Time, loc *time.Location) time.Time {
	return workflow.NextActive(a.Schedule, after, loc, a.active)
}

// Between keeps the ticks inside the period.
func (a activeSchedule) Between(from, to time.Time, loc *time.Location, limit int) []time.Time {
	if f := a.active.From; !f.IsZero() && from.Before(f) {
		from = f.Add(-time.Nanosecond)
	}
	return slices.DeleteFunc(a.Schedule.Between(from, to, loc, limit), func(t time.Time) bool { return !a.active.Contains(t) })
}

// keepRuns deletes the finished runs schedule d fired beyond its newest
// KeepRuns, as Delete would; runs still going and runs it did not fire are
// never touched. Found through the label index (workflow.LabelSchedule).
func (c *Client) keepRuns(ctx context.Context, d dueSchedule) error {
	if d.spec.keep <= 0 {
		return nil
	}
	var ids []string
	var id string
	err := c.st.each(ctx, c.st.db, "SELECT r.id FROM {runs} r JOIN {run_labels} l ON l.run_id = r.id WHERE l.lkey = ? AND l.lval = ? AND r."+terminalIn+
		" ORDER BY r.finished_at DESC, r.seq DESC", append([]any{workflow.LabelSchedule, d.id}, terminalArgs()...),
		func() error { ids = append(ids, id); return nil }, &id)
	if err != nil {
		return fmt.Errorf("sqlstore: read a schedule's finished runs: %w", err)
	}
	for _, id := range ids[min(d.spec.keep, len(ids)):] {
		if err := c.Delete(ctx, workflow.RunRef{RunID: workflow.RunID(id)}); err != nil && !isNotFoundOrState(err) {
			return err
		}
	}
	return nil
}

// isNotFoundOrState reports a run that is already gone or no longer
// finished (retried meanwhile): nothing for keepRuns to do.
func isNotFoundOrState(err error) bool {
	return errors.Is(err, workflow.ErrNotFound) || errors.Is(err, workflow.ErrWrongState)
}

// trimHistories applies KeepRuns to every schedule of this client's
// workflows that sets it, for runs that finished after their schedule's
// last fire (a fire trims at once; housekeeping catches the rest).
func (c *Client) trimHistories(ctx context.Context) error {
	var list []dueSchedule
	var id string
	var keep int64
	err := c.st.each(ctx, c.st.db, "SELECT p.schedule_id, p.keep_runs FROM {schedule_spec} p JOIN {schedules} s ON s.id = p.schedule_id WHERE s."+c.inNames()+
		" AND p.keep_runs > 0", c.nameArgs(), func() error {
		list = append(list, dueSchedule{id: id, spec: scheduleSpec{keep: int(keep)}})
		return nil
	}, &id, &keep)
	if err != nil {
		return err
	}
	for _, d := range list {
		if err := c.keepRuns(ctx, d); err != nil {
			return err
		}
	}
	return nil
}

// keepAfterEnd applies KeepRuns of schedule id once a run it fired has
// ended, so the history holds the bound without waiting for the next fire.
func (c *Client) keepAfterEnd(ctx context.Context, id string) {
	var keep int64
	err := c.st.one(ctx, c.st.db, "SELECT keep_runs FROM {schedule_spec} WHERE schedule_id = ?", []any{id}, &keep)
	if errors.Is(err, sqlb.ErrNoRows) || (err == nil && keep <= 0) {
		return
	}
	if err == nil {
		err = c.keepRuns(ctx, dueSchedule{id: id, spec: scheduleSpec{keep: int(keep)}})
	}
	if err != nil && ctx.Err() == nil {
		c.log.Error("sqlstore: trim a schedule's run history (KeepRuns)", "schedule", id, "error", err)
	}
}

// scheduleLabel is the schedule that fired the run (workflow.LabelSchedule),
// or "" for a run nobody scheduled.
func (r *runRow) scheduleLabel() string {
	if !strings.Contains(r.labels, workflow.LabelSchedule) {
		return ""
	}
	m, err := r.labelMap()
	if err != nil {
		return ""
	}
	return m[workflow.LabelSchedule]
}
