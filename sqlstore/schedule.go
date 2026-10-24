package sqlstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/workflow"
)

// fixedZonePrefix marks a stored location that is a fixed offset rather
// than a zone name: "fixed:<seconds east of UTC>:<name>".
const fixedZonePrefix = "fixed:"

// declaredID is the stable id of a cron expression declared on a workflow,
// the same on every node and across restarts, so a declaration is one row
// however many clients register it and its ticks dedupe on that row. A
// declaration in a zone other than UTC (Workflow.CronLocation) hashes the
// stored zone too, so changing the zone replaces the row; a UTC one keeps
// the id it had before zones existed.
func declaredID(workflowName, expr, loc string) string {
	key := workflowName + "\x00" + expr
	if loc != "UTC" {
		key += "\x00" + loc
	}
	sum := sha256.Sum256([]byte(key))
	return "cron_" + hex.EncodeToString(sum[:16])
}

// syncDeclared makes the stored declared schedules of reg match its Cron
// list, CronLocation and Misfire: expressions no longer declared are
// removed, new ones added, and every one's misfire policy rewritten. A
// declaration that already exists keeps its next fire time, so a restart
// neither skips nor repeats a tick.
func (c *Client) syncDeclared(ctx context.Context, reg *registered) error {
	zone := cronLocation(reg.wf)
	loc, err := encodeLocation(zone)
	if err != nil {
		return err
	}
	want := map[string]string{}
	for _, expr := range reg.crons {
		want[declaredID(reg.wf.Name, expr, loc)] = expr
	}
	var have []string
	var id string
	if err := c.st.each(ctx, c.st.db, "SELECT id FROM {schedules} WHERE workflow = ? AND declared = 1", []any{reg.wf.Name},
		func() error { have = append(have, id); return nil }, &id); err != nil {
		return fmt.Errorf("sqlstore: read declared schedules: %w", err)
	}
	for _, h := range have {
		if _, ok := want[h]; ok {
			err := c.withScheduleExt(ctx, h, func(tx *sql.Tx) error {
				if err := c.writeMisfire(ctx, tx, h, reg.wf.Misfire); err != nil {
					return err
				}
				if err := c.writeOverlap(ctx, tx, h, reg.wf.CronOverlap); err != nil {
					return err
				}
				if err := c.writeSpec(ctx, tx, h, declaredSpec(reg.wf)); err != nil {
					return err
				}
				return c.writeWindows(ctx, tx, h, reg.wf.CronWindows)
			})
			if err != nil {
				return err
			}
			delete(want, h)
			continue
		}
		err := c.st.tx(ctx, func(tx *sql.Tx) error {
			if _, err := c.st.exec(ctx, tx, "DELETE FROM {schedules} WHERE id = ?", h); err != nil {
				return err
			}
			if _, err := c.st.exec(ctx, tx, "DELETE FROM {schedule_ext} WHERE schedule_id = ?", h); err != nil {
				return err
			}
			if _, err := c.st.exec(ctx, tx, "DELETE FROM {schedule_last} WHERE schedule_id = ?", h); err != nil {
				return err
			}
			if _, err := c.st.exec(ctx, tx, "DELETE FROM {schedule_spec} WHERE schedule_id = ?", h); err != nil {
				return err
			}
			_, err := c.st.exec(ctx, tx, "DELETE FROM {schedule_misfire} WHERE schedule_id = ?", h)
			return err
		})
		if err != nil {
			return err
		}
	}
	now := c.clk.Now()
	for id, expr := range want {
		s, err := c.parseFor(expr, declaredSpec(reg.wf)) // parsed at Register already
		if err != nil {
			return fmt.Errorf("%w: workflow %q cron %q: %w", workflow.ErrInvalid, reg.wf.Name, expr, err)
		}
		next := s.Next(now, zone)
		if next.IsZero() {
			continue
		}
		err = c.st.tx(ctx, func(tx *sql.Tx) error {
			if err := c.writeMisfire(ctx, tx, id, reg.wf.Misfire); err != nil {
				return err
			}
			if err := c.writeWindows(ctx, tx, id, reg.wf.CronWindows); err != nil {
				return err
			}
			if err := c.writeOverlap(ctx, tx, id, reg.wf.CronOverlap); err != nil {
				return err
			}
			if err := c.writeSpec(ctx, tx, id, declaredSpec(reg.wf)); err != nil {
				return err
			}
			_, err := c.st.exec(ctx, tx, `INSERT INTO {schedules} (id, workflow, name, cron, location, labels, next_fire, active, declared)
				VALUES (?, ?, ?, ?, ?, '{}', ?, 1, 1)`, id, reg.wf.Name, reg.wf.Name, expr, loc, next.UnixNano())
			return err
		})
		if err != nil && !c.st.isUnique(err) { // another client declared it first
			return err
		}
	}
	return nil
}

// writeMisfire stores m as schedule id's misfire policy in tx: no row for
// the default policy, one row otherwise. The delete comes first so the
// transaction opens with a write (SQLite then takes its write lock up front
// instead of failing a read-to-write upgrade).
func (c *Client) writeMisfire(ctx context.Context, tx *sql.Tx, id string, m workflow.Misfire) error {
	if _, err := c.st.exec(ctx, tx, "DELETE FROM {schedule_misfire} WHERE schedule_id = ?", id); err != nil {
		return err
	}
	if m.IsZero() {
		return nil
	}
	_, err := c.st.exec(ctx, tx, "INSERT INTO {schedule_misfire} (schedule_id, policy, within_ns, max_fires) VALUES (?, ?, ?, ?)",
		id, string(m.Policy), int64(m.Within), m.Max)
	return err
}

// namedScheduleID is the stable id of a Client.Schedule with a Name: the
// same workflow and name give the same row on every node and every boot,
// so its primary key is what makes Schedule an upsert by name.
func namedScheduleID(workflowName, name string) string {
	sum := sha256.Sum256([]byte("named\x00" + workflowName + "\x00" + name))
	return string(IDSchedule) + "_" + hex.EncodeToString(sum[:16])
}

// Schedule registers a recurring trigger, stored so every client running
// the workflow fires it and each tick fires once. Its Location must be
// loadable by name on every node (time.LoadLocation) or be a fixed offset;
// time.Local is refused because it differs from machine to machine. Its
// Misfire policy is stored with it (the zero policy is this driver's
// default: see the package doc).
//
// A schedule with a Name is upserted by (Workflow, Name): calling Schedule
// again with the same name, on any node or after a reboot, returns the same
// ScheduleID and updates that one row instead of adding a second, so an app
// can declare its schedules at every boot. The update replaces the cron,
// zone, input, labels and misfire policy and makes the schedule active
// again (after an Unschedule too); when the cron and zone are unchanged and
// the schedule was active, its next fire time is kept, so a reboot neither
// skips nor repeats a tick. Two nodes declaring one name at once end with
// one row. A schedule without a Name is a new schedule on every call, with
// an ID from the ID generator. Rows made by a version before named upserts
// keep firing until they are Unscheduled.
func (c *Client) Schedule(ctx context.Context, s workflow.Schedule) (workflow.ScheduleID, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := workflow.ValidateSchedule(s); err != nil {
		return "", err
	}
	if err := workflow.CheckScheduleCaps(caps, s); err != nil {
		return "", err
	}
	parsed, err := c.parseFor(s.Cron, specOf(s))
	if err != nil {
		return "", fmt.Errorf("%w: schedule for %q: %w", workflow.ErrInvalid, s.Workflow, err)
	}
	if err := c.usable(); err != nil {
		return "", err
	}
	if c.reg(s.Workflow) == nil {
		return "", fmt.Errorf("%w: workflow %q is not registered", workflow.ErrNotFound, s.Workflow)
	}
	loc, err := encodeLocation(s.Location)
	if err != nil {
		return "", err
	}
	labels, err := json.Marshal(nonNil(s.Labels))
	if err != nil {
		return "", fmt.Errorf("sqlstore: encode labels: %w", err)
	}
	row := scheduleRow{s: s, loc: loc, labels: string(labels)}
	if next := parsed.Next(c.clk.Now(), s.Location); !next.IsZero() {
		row.next, row.active = next.UnixNano(), 1
	}
	if s.Name == "" {
		if row.id, err = c.st.newID(IDSchedule); err != nil {
			return "", err
		}
		err = c.st.tx(ctx, func(tx *sql.Tx) error {
			if err := c.writeMisfire(ctx, tx, row.id, s.Misfire); err != nil {
				return err
			}
			if err := c.writeWindows(ctx, tx, row.id, s.Windows); err != nil {
				return err
			}
			if err := c.writeOverlap(ctx, tx, row.id, s.Overlap); err != nil {
				return err
			}
			if err := c.writeSpec(ctx, tx, row.id, specOf(s)); err != nil {
				return err
			}
			if err := c.writeTenant(ctx, tx, row.id, s.Tenant); err != nil {
				return err
			}
			return c.insertSchedule(ctx, tx, row)
		})
	} else {
		row.id = namedScheduleID(s.Workflow, s.Name)
		err = c.upsertNamed(ctx, row)
	}
	if err != nil {
		return "", err
	}
	c.nudge()
	return workflow.ScheduleID(row.id), nil
}

// scheduleRow is what Schedule writes.
type scheduleRow struct {
	id     string
	s      workflow.Schedule
	loc    string // encodeLocation(s.Location)
	labels string // s.Labels as JSON
	next   int64  // first fire time; 0 with active 0 when it never fires
	active int
}

func (c *Client) insertSchedule(ctx context.Context, tx *sql.Tx, r scheduleRow) error {
	_, err := c.st.exec(ctx, tx, `INSERT INTO {schedules} (id, workflow, name, cron, location, in_data, labels, next_fire, active, declared)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)`, r.id, r.s.Workflow, r.s.Name, r.s.Cron, r.loc, r.s.Input, r.labels, r.next, r.active)
	return err
}

// upsertNamed is Schedule's upsert by name, in one transaction with the
// misfire row. Two nodes inserting one name at once: the second fails on
// the primary key and its transaction is tried again, finding the row.
func (c *Client) upsertNamed(ctx context.Context, r scheduleRow) error {
	var err error
	for range c.st.txAttempts {
		err = c.st.tx(ctx, func(tx *sql.Tx) error {
			if err := c.writeMisfire(ctx, tx, r.id, r.s.Misfire); err != nil {
				return err
			}
			if err := c.writeWindows(ctx, tx, r.id, r.s.Windows); err != nil {
				return err
			}
			if err := c.writeOverlap(ctx, tx, r.id, r.s.Overlap); err != nil {
				return err
			}
			if err := c.writeSpec(ctx, tx, r.id, specOf(r.s)); err != nil {
				return err
			}
			if err := c.writeTenant(ctx, tx, r.id, r.s.Tenant); err != nil {
				return err
			}
			var have int64
			if err := c.st.one(ctx, tx, "SELECT COUNT(*) FROM {schedules} WHERE id = ? AND declared = 0", []any{r.id}, &have); err != nil {
				return fmt.Errorf("sqlstore: read schedule %q: %w", r.id, err)
			}
			if have == 0 {
				return c.insertSchedule(ctx, tx, r)
			}
			return c.updateNamed(ctx, tx, r)
		})
		if !c.st.isUnique(err) || ctx.Err() != nil {
			return err
		}
	}
	return err
}

// updateNamed is the update half of upsertNamed. next_fire and active are
// assigned first, because MySQL evaluates SET left to right with the new
// values and both CASEs must compare the stored cron, zone and active flag:
// an unchanged, active schedule keeps its next fire time and stays active;
// anything else takes the newly computed ones.
func (c *Client) updateNamed(ctx context.Context, tx *sql.Tx, r scheduleRow) error {
	same := "cron = ? AND location = ? AND active = 1"
	_, err := c.st.exec(ctx, tx, `UPDATE {schedules} SET
		next_fire = CASE WHEN `+same+` THEN next_fire ELSE ? END,
		active = CASE WHEN `+same+` THEN 1 ELSE ? END,
		cron = ?, location = ?, in_data = ?, labels = ?
		WHERE id = ? AND declared = 0`,
		r.s.Cron, r.loc, r.next, r.s.Cron, r.loc, r.active, r.s.Cron, r.loc, r.s.Input, r.labels, r.id)
	return err
}

// Unschedule stops a schedule. An empty id, or one already removed, is a
// no-op; an id never issued returns ErrNotFound. The row stays, inactive,
// so the second call can tell "removed" from "never existed".
func (c *Client) Unschedule(ctx context.Context, id workflow.ScheduleID) error {
	if id.IsZero() {
		return nil
	}
	var active int64
	err := c.st.one(ctx, c.st.db, "SELECT active FROM {schedules} WHERE id = ? AND declared = 0", []any{string(id)}, &active)
	if errors.Is(err, sqlb.ErrNoRows) {
		return fmt.Errorf("%w: schedule %q", workflow.ErrNotFound, id)
	}
	if err != nil {
		return fmt.Errorf("sqlstore: read schedule: %w", err)
	}
	_, err = c.st.exec(ctx, c.st.db, "UPDATE {schedules} SET active = 0 WHERE id = ?", string(id))
	return err
}

// dueSchedule is one schedule row fireSchedules found due.
type dueSchedule struct {
	id, workflow, name, expr, loc, labels string
	declared                              bool
	input                                 []byte
	next                                  int64
	misfire                               workflow.Misfire
	windows                               workflow.Windows
	paused                                bool
	// overlap, lastFire and lastRun are the schedule_last row (zero for a
	// schedule that has none yet).
	overlap  workflow.OverlapPolicy
	lastFire int64
	lastRun  string
	// spec is the schedule_spec row (zero: the defaults).
	spec scheduleSpec
	// tenant is the schedule_tenant row: Schedule.Tenant, "" for none.
	tenant string
}

// scheduleCols is the column list fireSchedules and Schedules scan:
// schedules s, schedule_misfire m, schedule_ext x and schedule_last l.
const scheduleCols = "s.id, s.workflow, s.name, s.cron, s.location, s.in_data, s.labels, s.next_fire, s.declared, s.active," +
	" m.policy, m.within_ns, m.max_fires, x.windows, x.paused, l.overlap, l.last_fire, l.last_run," +
	" p.seconds, p.active_from, p.active_until, p.keep_runs, tn.tenant"

const scheduleFrom = " FROM {schedules} s LEFT JOIN {schedule_misfire} m ON m.schedule_id = s.id LEFT JOIN {schedule_ext} x ON x.schedule_id = s.id" +
	" LEFT JOIN {schedule_last} l ON l.schedule_id = s.id LEFT JOIN {schedule_spec} p ON p.schedule_id = s.id" +
	" LEFT JOIN {schedule_tenant} tn ON tn.schedule_id = s.id"

// scheduleScan reads one row of scheduleCols.
type scheduleScan struct {
	d                       dueSchedule
	declared, active        int64
	policy, windows         sql.NullString
	within, maxFires, pause sql.NullInt64
	overlap, lastRun        sql.NullString
	lastFire                sql.NullInt64
	seconds, from, until    sql.NullInt64
	keep                    sql.NullInt64
	tenant                  sql.NullString
}

func (r *scheduleScan) targets() []any {
	return []any{&r.d.id, &r.d.workflow, &r.d.name, &r.d.expr, &r.d.loc, &r.d.input, &r.d.labels, &r.d.next, &r.declared, &r.active,
		&r.policy, &r.within, &r.maxFires, &r.windows, &r.pause, &r.overlap, &r.lastFire, &r.lastRun,
		&r.seconds, &r.from, &r.until, &r.keep, &r.tenant}
}

// row copies the scan into a dueSchedule the caller may keep.
func (r *scheduleScan) row(log func(string, ...any)) dueSchedule {
	d := r.d
	d.input = append([]byte(nil), r.d.input...)
	d.declared = r.declared == 1
	if r.policy.Valid {
		d.misfire = workflow.Misfire{Policy: workflow.MisfirePolicy(r.policy.String), Within: time.Duration(r.within.Int64), Max: int(r.maxFires.Int64)}
	}
	if r.windows.Valid && r.windows.String != "" {
		if err := json.Unmarshal([]byte(r.windows.String), &d.windows); err != nil {
			log("sqlstore: stored schedule windows do not decode", "schedule", d.id, "error", err)
		}
	}
	d.paused = r.pause.Int64 == 1
	d.overlap, d.lastFire, d.lastRun = workflow.OverlapPolicy(r.overlap.String), r.lastFire.Int64, r.lastRun.String
	d.spec = scheduleSpec{seconds: r.seconds.Int64 == 1, active: workflow.Active{From: timeOrZero(r.from.Int64), Until: timeOrZero(r.until.Int64)},
		keep: int(r.keep.Int64)}
	d.tenant = r.tenant.String
	return d
}

// tickName is Tick.Name and ScheduleInfo.Name: the Schedule.Name, or the
// expression of a declared cron.
func (d dueSchedule) tickName() string {
	if d.declared {
		return d.expr
	}
	return d.name
}

// fireSchedules fires every due schedule of this client's workflows. The
// tick row (schedule, fire time) is inserted first in the firing
// transaction: its primary key is the once-per-tick guarantee, because a
// second client firing the same tick fails that insert and rolls back
// before enqueueing anything; the move of next_fire is conditional on the
// value read, which guards a firing that inserts no tick (a skipped
// misfire). Under the default misfire policy a tick missed while no client
// ran fires once when one returns and earlier missed ticks are not
// replayed; a schedule with a workflow.Misfire policy fires what
// Misfire.Due returns for the ticks from its stored next fire time to now.
// A paused schedule (SchedulePauser) does not fire: its due ticks are
// reported as skipped and its next fire time moves past them, so they
// never fire, not even on resume.
func (c *Client) fireSchedules(ctx context.Context) error {
	now := c.clk.Now()
	allPaused, err := c.schedulesPaused(ctx)
	if err != nil {
		return err
	}
	var list []dueSchedule
	var r scheduleScan
	collect := func() error { list = append(list, r.row(c.log.Error)); return nil }
	if !allPaused {
		err = c.st.each(ctx, c.st.db, "SELECT "+scheduleCols+scheduleFrom+" WHERE s."+c.inNames()+
			" AND s.active = 1 AND s.next_fire <= ? AND COALESCE(x.paused, 0) = 0 ORDER BY s.next_fire LIMIT ?",
			c.nameArgs(now.UnixNano(), c.fetch), collect, r.targets()...)
		if err != nil {
			return err
		}
	}
	pausedWhere := " AND x.paused = 1"
	if allPaused {
		pausedWhere = ""
	}
	var paused []dueSchedule
	collect = func() error { paused = append(paused, r.row(c.log.Error)); return nil }
	err = c.st.each(ctx, c.st.db, "SELECT "+scheduleCols+scheduleFrom+" WHERE s."+c.inNames()+
		" AND s.active = 1 AND s.next_fire <= ?"+pausedWhere+" ORDER BY s.next_fire LIMIT ?",
		c.nameArgs(now.UnixNano(), c.fetch), collect, r.targets()...)
	if err != nil {
		return err
	}
	for _, d := range paused {
		if err := c.reportPaused(ctx, d, now); err != nil {
			c.log.Error("sqlstore: report a paused schedule's ticks", "schedule", d.id, "error", err)
		}
	}
	fired := map[string]*registered{}
	for _, d := range list {
		ok, err := c.fireOne(ctx, d, now)
		if err != nil {
			c.log.Error("sqlstore: cron fire could not enqueue", "workflow", d.workflow, "cron", d.expr, "error", err)
			continue
		}
		if ok {
			fired[d.workflow] = c.reg(d.workflow)
		}
	}
	for _, reg := range fired {
		if err := c.admit(ctx, reg); err != nil {
			return err
		}
	}
	if len(fired) > 0 {
		c.nudge()
	}
	return nil
}

// errTickTaken rolls back a firing whose schedule another client moved on
// first.
var errTickTaken = errors.New("sqlstore: tick fired by another client")

// tickDecision is what fireOne decided for one due tick before its
// transaction: fire it, or skip it with a reason.
type tickDecision struct {
	at     int64
	runID  string // set when it fires
	reason string // TickReason* when it is skipped
}

// fireOne fires the ticks of one due schedule and moves it to its next
// fire time, in one transaction. It reports whether it enqueued anything;
// a tick another client fired is neither an error nor a fire. Windows and
// ScheduleWhen are decided before the transaction, on this node, and a
// skipped tick still gets its tick row, so one node reports it. Tick
// observers hear of every outcome after the commit.
func (c *Client) fireOne(ctx context.Context, d dueSchedule, now time.Time) (bool, error) {
	reg := c.reg(d.workflow)
	sched, err := c.parseFor(d.expr, d.spec)
	if err != nil {
		return false, fmt.Errorf("stored schedule %s does not parse: %w", d.id, err)
	}
	loc, err := decodeLocation(d.loc)
	if err != nil {
		return false, fmt.Errorf("stored schedule %s zone %q does not load: %w", d.id, d.loc, err)
	}
	var labels map[string]string
	if err := json.Unmarshal([]byte(d.labels), &labels); err != nil {
		c.log.Error("sqlstore: stored schedule labels do not decode", "schedule", d.id, "error", err)
	}
	// The default fires the oldest due tick once under its own fire time;
	// a policy fires what Misfire.Due picks from [next_fire, now].
	last := time.Unix(0, d.next-1)
	var fire []time.Time
	if d.misfire.IsZero() {
		fire = []time.Time{unixTime(d.next)}
	} else {
		fire = d.misfire.Due(sched, last, now, loc)
	}
	if !d.spec.active.IsZero() {
		// A next fire time stored before the period changed may lie
		// outside it: such a tick never fires.
		fire = slices.DeleteFunc(fire, func(t time.Time) bool { return !d.spec.active.Contains(t) })
	}
	missed, missedAt := dropped(sched, last, now, loc, fire)
	windows := d.windows
	if d.declared && reg != nil {
		windows = reg.wf.CronWindows
	}
	base := workflow.Tick{ScheduleID: workflow.ScheduleID(d.id), Name: d.tickName(), Workflow: d.workflow, Declared: d.declared,
		Location: loc, Labels: labels}
	// Under OverlapSkip an unfinished previous run holds every tick back,
	// and so does a run this firing starts: of several due ticks at most
	// one fires.
	blocked, _, err := c.overlapBlocks(ctx, d)
	if err != nil {
		return false, err
	}
	decisions := make([]tickDecision, len(fire))
	for i, t := range fire {
		decisions[i].at = t.UnixNano()
		tick := base
		tick.At = t.In(loc)
		switch {
		case !windows.IsZero() && !windows.Allows(t, loc):
			decisions[i].reason = workflow.TickReasonWindow
		case blocked:
			decisions[i].reason = workflow.TickReasonOverlap
		case reg != nil && reg.wf.ScheduleWhen != nil && !c.askWhen(ctx, reg.wf.ScheduleWhen, tick):
			decisions[i].reason = workflow.TickReasonWhen
		default:
			if decisions[i].runID, err = c.st.newID(IDRun); err != nil {
				return false, err
			}
			blocked = d.overlap == workflow.OverlapSkip
		}
	}
	next := sched.Next(now, loc)
	refs := make([]workflow.RunRef, len(decisions))
	err = c.st.tx(ctx, func(tx *sql.Tx) error {
		q, args := "UPDATE {schedules} SET next_fire = ? WHERE id = ? AND next_fire = ?", []any{next.UnixNano(), d.id, d.next}
		if next.IsZero() {
			q, args = "UPDATE {schedules} SET active = 0 WHERE id = ? AND next_fire = ?", args[1:]
		}
		n, err := c.st.execN(ctx, tx, q, args...)
		if err != nil {
			return err
		}
		if n == 0 {
			return errTickTaken
		}
		for i, dec := range decisions {
			if _, err := c.st.exec(ctx, tx, "INSERT INTO {ticks} (schedule_id, fire_at, run_id) VALUES (?, ?, ?)", d.id, dec.at, dec.runID); err != nil {
				return err
			}
			if dec.runID == "" {
				continue
			}
			runLabels := workflow.FiredLabelsAt(labels, workflow.ScheduleID(d.id), d.tickName(), unixTime(dec.at))
			row, err := c.insertRun(ctx, tx, reg, workflow.Job{Workflow: d.workflow, Payload: d.input, Labels: runLabels, Tenant: d.tenant}, "", dec.runID)
			if err != nil {
				return err
			}
			refs[i] = row.ref()
			if err := c.moveLast(ctx, tx, d, dec.at, dec.runID); err != nil {
				return err
			}
			d.lastRun = dec.runID // the next tick of this firing compares against it
		}
		return nil
	})
	if errors.Is(err, errTickTaken) || errors.Is(err, errLastMoved) || c.st.isUnique(err) {
		return false, nil // another client fired this tick
	}
	var ticks []workflow.Tick
	if missed > 0 && err == nil {
		t := base
		t.Outcome, t.At, t.Missed = workflow.TickMisfired, missedAt.In(loc), missed
		ticks = append(ticks, t)
	}
	enqueued := false
	for i, dec := range decisions {
		t := base
		t.At = unixTime(dec.at).In(loc)
		switch {
		case dec.runID == "" && err == nil:
			t.Outcome, t.Reason = workflow.TickSkipped, dec.reason
		case dec.runID == "":
			continue // the skip was not recorded; the next pass decides again
		default:
			t.Outcome, t.Run, t.Err = workflow.TickFired, refs[i], err
			enqueued = enqueued || err == nil
		}
		ticks = append(ticks, t)
	}
	c.emitTicks(ctx, ticks)
	if enqueued {
		if kerr := c.keepRuns(ctx, d); kerr != nil {
			c.log.Error("sqlstore: trim a schedule's run history (KeepRuns)", "schedule", d.id, "error", kerr)
		}
	}
	return enqueued, err
}

// askWhen calls a ScheduleWhen, treating a panic as "no" so one bad
// predicate cannot stop the dispatcher.
func (c *Client) askWhen(ctx context.Context, when func(context.Context, workflow.Tick) bool, t workflow.Tick) (ok bool) {
	defer func() {
		if p := recover(); p != nil {
			c.log.Error("sqlstore: ScheduleWhen panicked; the tick is skipped", "schedule", t.ScheduleID, "panic", fmt.Sprint(p))
			ok = false
		}
	}()
	return when(ctx, t)
}

// maxDroppedScan caps how many ticks dropped counts per firing, so a
// frequent cron after a long outage costs bounded time; the count is then
// a lower bound.
const maxDroppedScan = 1 << 16

// dropped counts the ticks of src in (last, now] that fire does not hold,
// and returns the newest of them.
func dropped(src workflow.TickSource, last, now time.Time, loc *time.Location, fire []time.Time) (int, time.Time) {
	fired := make(map[int64]bool, len(fire))
	for _, t := range fire {
		fired[t.UnixNano()] = true
	}
	n := 0
	var newest time.Time
	from := last
	for scanned := 0; scanned < maxDroppedScan; {
		chunk := src.Between(from, now, loc, claimBatch)
		for _, t := range chunk {
			if !fired[t.UnixNano()] {
				n++
				newest = t
			}
		}
		scanned += len(chunk)
		if len(chunk) < claimBatch || !chunk[len(chunk)-1].After(from) {
			break
		}
		from = chunk[len(chunk)-1]
	}
	return n, newest
}

// reportPaused reports the due ticks of a paused schedule as TickSkipped
// with TickReasonPaused and moves its next fire time past now, so those
// ticks never fire, not even on resume (workflow.SchedulePauser). The move
// is a compare-and-set on the next fire time read, so across the cluster
// only the node whose update lands reports them. At most claimBatch ticks
// are reported per move; older ones of a very long pause are skipped
// unreported.
func (c *Client) reportPaused(ctx context.Context, d dueSchedule, now time.Time) error {
	sched, err := c.parseFor(d.expr, d.spec)
	if err != nil {
		return err
	}
	loc, err := decodeLocation(d.loc)
	if err != nil {
		return err
	}
	due := sched.Between(time.Unix(0, d.next-1), now, loc, claimBatch)
	next := sched.Next(now, loc)
	q, args := "UPDATE {schedules} SET next_fire = ? WHERE id = ? AND next_fire = ?", []any{next.UnixNano(), d.id, d.next}
	if next.IsZero() {
		q, args = "UPDATE {schedules} SET active = 0 WHERE id = ? AND next_fire = ?", args[1:]
	}
	n, err := c.st.execN(ctx, c.st.db, q, args...)
	if err != nil || n == 0 {
		return err
	}
	var labels map[string]string
	if err := json.Unmarshal([]byte(d.labels), &labels); err != nil {
		c.log.Error("sqlstore: stored schedule labels do not decode", "schedule", d.id, "error", err)
	}
	ticks := make([]workflow.Tick, len(due))
	for i, t := range due {
		ticks[i] = workflow.Tick{ScheduleID: workflow.ScheduleID(d.id), Name: d.tickName(), Workflow: d.workflow, Declared: d.declared,
			At: t.In(loc), Location: loc, Outcome: workflow.TickSkipped, Reason: workflow.TickReasonPaused, Labels: labels}
	}
	c.emitTicks(ctx, ticks)
	return nil
}

// zoneProbes are the instants encodeLocation compares offsets at: the two
// solstice months of several years, enough to see any daylight-saving rule.
var zoneProbes = func() []time.Time {
	var out []time.Time
	for y := 2020; y <= 2030; y++ {
		out = append(out, time.Date(y, 1, 15, 12, 0, 0, 0, time.UTC), time.Date(y, 7, 15, 12, 0, 0, 0, time.UTC))
	}
	return out
}()

// encodeLocation stores a zone so every node evaluates it the same: by
// name when time.LoadLocation gives the same offsets back, else as a fixed
// offset when it has one. Anything else is refused rather than shifted.
func encodeLocation(loc *time.Location) (string, error) {
	name := loc.String()
	if loc == time.Local || name == "Local" {
		return "", workflow.Unsupported("Schedule.Location", "time.Local differs from machine to machine; pass a named zone")
	}
	if l, err := time.LoadLocation(name); err == nil && sameOffsets(l, loc) {
		return name, nil
	}
	_, off := zoneProbes[0].In(loc).Zone()
	for _, t := range zoneProbes {
		if _, o := t.In(loc).Zone(); o != off {
			return "", workflow.Unsupported("Schedule.Location", "zone "+name+" is neither loadable by name nor a fixed offset")
		}
	}
	return fixedZonePrefix + strconv.Itoa(off) + ":" + name, nil
}

func decodeLocation(s string) (*time.Location, error) {
	if rest, ok := strings.CutPrefix(s, fixedZonePrefix); ok {
		off, name, ok := strings.Cut(rest, ":")
		secs, err := strconv.Atoi(off)
		if !ok || err != nil {
			return nil, fmt.Errorf("sqlstore: stored zone %q is malformed", s)
		}
		return time.FixedZone(name, secs), nil
	}
	return time.LoadLocation(s)
}

func sameOffsets(a, b *time.Location) bool {
	for _, t := range zoneProbes {
		_, oa := t.In(a).Zone()
		_, ob := t.In(b).Zone()
		if oa != ob {
			return false
		}
	}
	return true
}
