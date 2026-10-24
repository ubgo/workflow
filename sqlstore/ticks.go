package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/ubgo/workflow"
)

// OnTick registers fn to observe every schedule tick this client fires,
// skips or drops (workflow.TickNotifier): TickFired with the run, TickSkipped
// for a tick a window, ScheduleWhen or a schedule pause held back, and
// TickMisfired for the ticks a late wake dropped. Each tick is reported by
// the one node that fired or skipped it, after its transaction committed;
// a crash between the commit and the call loses the report (best effort,
// like RunNotifier's contract allows). Register before Start.
func (c *Client) OnTick(fn workflow.TickObserver) error {
	if fn == nil {
		return fmt.Errorf("%w: OnTick needs a function", workflow.ErrInvalid)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return workflow.ErrAlreadyStarted
	}
	c.tickObservers = append(c.tickObservers, fn)
	return nil
}

// emitTicks calls every tick observer for each tick in order, recovering
// panics.
func (c *Client) emitTicks(ctx context.Context, ticks []workflow.Tick) {
	c.mu.Lock()
	obs := c.tickObservers
	c.mu.Unlock()
	for _, t := range ticks {
		for _, fn := range obs {
			c.observeTick(ctx, fn, t)
		}
	}
}

func (c *Client) observeTick(ctx context.Context, fn workflow.TickObserver, t workflow.Tick) {
	defer func() {
		if p := recover(); p != nil {
			c.log.Error("sqlstore: tick observer panicked", "schedule", t.ScheduleID, "panic", fmt.Sprint(p))
		}
	}()
	fn(ctx, t)
}

// writeWindows stores w as schedule id's windows in tx (an empty string
// for none).
func (c *Client) writeWindows(ctx context.Context, tx *sql.Tx, id string, w workflow.Windows) error {
	doc := ""
	if !w.IsZero() {
		b, err := json.Marshal(w)
		if err != nil {
			return fmt.Errorf("sqlstore: encode windows: %w", err)
		}
		doc = string(b)
	}
	if err := c.ensureScheduleExt(ctx, tx, id); err != nil {
		return err
	}
	_, err := c.st.exec(ctx, tx, "UPDATE {schedule_ext} SET windows = ? WHERE schedule_id = ?", doc, id)
	return err
}

// Schedules lists the live schedules of every workflow in this database,
// declared and made with Schedule, sorted by Workflow, Name, then ID
// (workflow.ScheduleLister). An unscheduled schedule, or a declared one
// whose workflow dropped the expression, is not listed. Next is the
// stored next fire time; Overlap and Last (the last fire, tick or trigger,
// its run and that run's status now) come from schedule_last.
func (c *Client) Schedules(ctx context.Context) ([]workflow.ScheduleInfo, error) {
	allPaused, err := c.schedulesPaused(ctx)
	if err != nil {
		return nil, err
	}
	var out []workflow.ScheduleInfo
	var r scheduleScan
	err = c.st.each(ctx, c.st.db, "SELECT "+scheduleCols+scheduleFrom+" WHERE s.active = 1", nil, func() error {
		d := r.row(c.log.Error)
		loc, lerr := decodeLocation(d.loc)
		if lerr != nil {
			c.log.Error("sqlstore: stored schedule zone does not load", "schedule", d.id, "error", lerr)
			loc = time.UTC
		}
		var labels map[string]string
		if err := json.Unmarshal([]byte(d.labels), &labels); err != nil {
			c.log.Error("sqlstore: stored schedule labels do not decode", "schedule", d.id, "error", err)
		}
		if d.declared {
			labels = nil // declared crons carry no labels of their own
		}
		info := workflow.ScheduleInfo{
			ID: workflow.ScheduleID(d.id), Name: d.tickName(), Workflow: d.workflow, Cron: d.expr, Location: loc,
			Declared: d.declared, Paused: allPaused || d.paused, Next: unixTime(d.next).In(loc),
			Misfire: d.misfire, Windows: d.windows, Labels: labels, Overlap: d.overlap,
			Seconds: d.spec.seconds, Active: d.spec.active, KeepRuns: d.spec.keep, Tenant: d.tenant,
		}
		if d.lastFire > 0 {
			info.Last.Fire = unixTime(d.lastFire).In(loc)
		}
		if d.lastRun != "" {
			info.Last.Run = workflow.RunRef{RunID: workflow.RunID(d.lastRun)}
		}
		out = append(out, info)
		return nil
	}, r.targets()...)
	if err != nil {
		return nil, fmt.Errorf("sqlstore: list schedules: %w", err)
	}
	// Last.Status is read after the scan, which holds a connection open.
	for i := range out {
		if out[i].Last.Run.RunID.IsZero() {
			continue
		}
		st, err := c.prevStatus(ctx, string(out[i].Last.Run.RunID))
		if err != nil {
			return nil, fmt.Errorf("sqlstore: list schedules: last run status: %w", err)
		}
		out[i].Last.Status = st
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Workflow != b.Workflow {
			return a.Workflow < b.Workflow
		}
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		return a.ID < b.ID
	})
	return out, nil
}
