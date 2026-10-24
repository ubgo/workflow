package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/cron"
)

// declaredPrefix starts the ScheduleID of a Workflow.Cron entry; the rest
// is the workflow name and the entry's index, so every client that
// registers the workflow writes the same schedule.
const declaredPrefix = "cron:"

func declaredID(wf string, i int) string { return declaredPrefix + wf + ":" + strconv.Itoa(i) }

// spec is what decides a schedule's fire times. A schedule written again
// with the same spec keeps its last and next fire times.
func spec(expr string, loc *time.Location, m workflow.Misfire, seconds bool, act workflow.Active) string {
	b, _ := json.Marshal(m) // a struct of plain fields always encodes
	out := expr + "\n" + loc.String() + "\n" + string(b)
	if seconds || !act.IsZero() {
		out += "\n" + strconv.FormatBool(seconds) + "\n" + encodeActive(act)
	}
	return out
}

// encodeActive is how an Active period is stored: its bounds in Unix
// nanoseconds, 0 for an open end ("" for none).
func encodeActive(a workflow.Active) string {
	if a.IsZero() {
		return ""
	}
	ns := func(t time.Time) int64 {
		if t.IsZero() {
			return 0
		}
		return t.UnixNano()
	}
	return strconv.FormatInt(ns(a.From), 10) + ":" + strconv.FormatInt(ns(a.Until), 10)
}

// decodeActive reads a stored Active period.
func decodeActive(s string) workflow.Active {
	from, until, ok := strings.Cut(s, ":")
	if !ok {
		return workflow.Active{}
	}
	return workflow.Active{From: nanos(from), Until: nanos(until)}
}

// tickSource is a schedule's fire times: the cron expression, limited to
// its Active period (workflow.NextActive), so misfire catch-up never
// reaches across the time before From or after Until.
type tickSource struct {
	s   cron.Schedule
	act workflow.Active
}

func (t tickSource) Next(after time.Time, loc *time.Location) time.Time {
	return workflow.NextActive(t.s, after, loc, t.act)
}

func (t tickSource) Between(from, to time.Time, loc *time.Location, limit int) []time.Time {
	if !t.act.From.IsZero() && from.Before(t.act.From) {
		from = t.act.From.Add(-time.Nanosecond)
	}
	if !t.act.Until.IsZero() && !to.Before(t.act.Until) {
		to = t.act.Until.Add(-time.Nanosecond)
	}
	if !to.After(from) {
		return nil
	}
	return t.s.Between(from, to, loc, limit)
}

func locName(loc *time.Location) string {
	if workflow.IsUTC(loc) {
		return "UTC"
	}
	return loc.String()
}

// fixedPrefix marks a stored fixed-offset zone ("fixed:<offset seconds>:<name>").
const fixedPrefix = "fixed:"

// encodeLoc is how a zone is stored so every client can rebuild it: its
// IANA name when the time zone database knows it, or for a fixed-offset
// zone (time.FixedZone) its offset and name. Any other zone (built from
// custom zone data) is refused, since another client could not rebuild it.
func encodeLoc(loc *time.Location) (string, error) {
	name := locName(loc)
	if _, err := time.LoadLocation(name); err == nil {
		return name, nil
	}
	// A fixed zone has one offset all year; probe two seasons to tell.
	_, jan := time.Date(2026, 1, 1, 0, 0, 0, 0, loc).Zone()
	_, jul := time.Date(2026, 7, 1, 0, 0, 0, 0, loc).Zone()
	if jan != jul {
		return "", fmt.Errorf("%w: zone %q is neither in the time zone database nor a fixed offset, so other clients could not evaluate it", workflow.ErrInvalid, name)
	}
	return fixedPrefix + strconv.Itoa(jan) + ":" + name, nil
}

// decodeLoc rebuilds a zone stored by encodeLoc.
func decodeLoc(s string) (*time.Location, error) {
	if rest, ok := cutPrefix(s, fixedPrefix); ok {
		off, name, found := strings.Cut(rest, ":")
		secs, err := strconv.Atoi(off)
		if !found || err != nil {
			return nil, fmt.Errorf("stored zone %q is malformed", s)
		}
		return time.FixedZone(name, secs), nil
	}
	return time.LoadLocation(s)
}

// effectiveMisfire is the policy applied: the schedule's own, or for the
// zero policy this driver's default, fire the newest missed tick once with
// no age limit (what the memory and SQL drivers do).
func effectiveMisfire(m workflow.Misfire) workflow.Misfire {
	if m.IsZero() {
		return workflow.Misfire{Policy: workflow.MisfireFireOnce}
	}
	return m
}

// Schedule writes a recurring trigger. With a Name it is an upsert by
// (Workflow, Name): the same ScheduleID on every client and boot, and an
// unchanged expression, zone and misfire policy keep their next fire time.
// Any client of the prefix may fire it; the fire is a compare-and-set on
// the schedule's last catch-up time, so each tick runs once.
func (c *Client) Schedule(ctx context.Context, s workflow.Schedule) (workflow.ScheduleID, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := c.usable(); err != nil {
		return "", err
	}
	if err := workflow.ValidateSchedule(s); err != nil {
		return "", err
	}
	if err := workflow.CheckScheduleCaps(caps, s); err != nil {
		return "", err
	}
	c.mu.Lock()
	_, ok := c.wfs[s.Workflow]
	c.mu.Unlock()
	if !ok {
		return "", fmt.Errorf("%w: workflow %q is not registered", workflow.ErrNotFound, s.Workflow)
	}
	src, err := c.parseCronFor(s.Cron, s.Seconds)
	if err != nil {
		return "", fmt.Errorf("%w: schedule for workflow %q: %w", workflow.ErrInvalid, s.Workflow, err)
	}
	field := ""
	if s.Name != "" {
		field = s.Workflow + "\n" + s.Name
	}
	id, err := c.writeSchedule(ctx, schedWrite{id: "sch-" + c.newID(), field: field, name: s.Name, wf: s.Workflow, expr: s.Cron,
		src: src, loc: s.Location, input: s.Input, labels: s.Labels, misfire: s.Misfire, overlap: s.Overlap,
		seconds: s.Seconds, active: s.Active, keep: s.KeepRuns, windows: s.Windows, tenant: s.Tenant})
	if err != nil {
		return "", err
	}
	c.pokeDispatcher()
	return workflow.ScheduleID(id), nil
}

// schedWrite is one schedule as written: a Client.Schedule or a declared
// Workflow.Cron entry.
type schedWrite struct {
	id, field, name, wf, expr string
	src                       cron.Schedule
	loc                       *time.Location
	input                     []byte
	labels                    map[string]string
	misfire                   workflow.Misfire
	overlap                   workflow.OverlapPolicy
	declared                  bool
	seconds                   bool
	active                    workflow.Active
	keep                      int
	windows                   workflow.Windows
	tenant                    string // Schedule.Tenant, the tenant of every run it fires; none for a declared cron
}

// writeSchedule runs the upsert script and returns the schedule's ID.
func (c *Client) writeSchedule(ctx context.Context, w schedWrite) (string, error) {
	loc, labels, m, expr := w.loc, w.labels, w.misfire, w.expr
	if loc == nil {
		loc = time.UTC
	}
	lb := "{}"
	if len(labels) > 0 {
		b, err := json.Marshal(labels)
		if err != nil {
			return "", fmt.Errorf("redis: encode schedule labels: %w", err)
		}
		lb = string(b)
	}
	mb, _ := json.Marshal(m) // plain fields always encode
	now := c.clk.Now()
	next := tickSource{w.src, w.active}.Next(now, loc)
	if next.IsZero() {
		return "", fmt.Errorf("%w: cron %q never fires (in its Active period)", workflow.ErrInvalid, expr)
	}
	locStored, err := encodeLoc(loc)
	if err != nil {
		return "", err
	}
	declared := ""
	if w.declared {
		declared = "1"
	}
	sec, win := "", ""
	if w.seconds {
		sec = "1"
	}
	if !w.windows.IsZero() {
		b, err := json.Marshal(w.windows)
		if err != nil {
			return "", fmt.Errorf("redis: encode schedule windows: %w", err)
		}
		win = string(b)
	}
	got, err := scheduleScript.Run(ctx, c.rdb, c.keys(), c.base, w.id, w.field, spec(expr, loc, m, w.seconds, w.active), w.wf, expr,
		locStored, string(w.input), lb, string(mb), now.UnixMilli(), next.UnixMilli(), w.name, declared, string(w.overlap),
		sec, encodeActive(w.active), w.keep, win, w.tenant).Text()
	if err != nil {
		return "", fmt.Errorf("redis: write schedule: %w", classify(err))
	}
	return got, nil
}

// Unschedule removes a schedule. An empty id is a no-op.
func (c *Client) Unschedule(ctx context.Context, id workflow.ScheduleID) error {
	if id.IsZero() {
		return nil
	}
	if err := unscheduleScript.Run(ctx, c.rdb, c.keys(), c.base, string(id)).Err(); err != nil {
		return fmt.Errorf("redis: unschedule: %w", classify(err))
	}
	return nil
}

// declareCrons writes every registered workflow's Workflow.Cron entries
// as schedules and removes entries a shorter Cron list no longer has.
func (c *Client) declareCrons(ctx context.Context) error {
	c.mu.Lock()
	regs := make([]*registered, 0, len(c.wfs))
	for _, r := range c.wfs {
		regs = append(regs, r)
	}
	c.mu.Unlock()
	for _, reg := range regs {
		for i, src := range reg.crons {
			if _, err := c.writeSchedule(ctx, schedWrite{id: declaredID(reg.wf.Name, i), name: reg.wf.Cron[i], wf: reg.wf.Name,
				expr: reg.wf.Cron[i], src: src, loc: reg.wf.CronLocation, misfire: reg.wf.Misfire,
				overlap: reg.wf.CronOverlap, declared: true, seconds: reg.wf.CronSeconds, active: reg.wf.CronActive,
				keep: reg.wf.CronKeepRuns, windows: reg.wf.CronWindows}); err != nil {
				return err
			}
		}
		for i := len(reg.crons); ; i++ {
			n, err := c.rdb.Exists(ctx, c.base+":sched:"+declaredID(reg.wf.Name, i)).Result()
			if err != nil {
				return fmt.Errorf("redis: read declared schedules: %w", classify(err))
			}
			if n == 0 {
				break
			}
			if err := c.Unschedule(ctx, workflow.ScheduleID(declaredID(reg.wf.Name, i))); err != nil {
				return err
			}
		}
	}
	return nil
}

// dispatch fires due schedule ticks and wakes the workers when a due time
// on the scheduling clock comes. It arms one timer on that clock for the
// earliest due run or schedule, and also wakes every poll interval (wall
// time) and on a poke, so work another client made due is seen too.
func (c *Client) dispatch() {
	defer c.wg.Done()
	for {
		wake := c.pass()
		var due <-chan time.Time
		var stopDue func() bool
		if !wake.IsZero() {
			t := c.clk.NewTimer(wake.Sub(c.clk.Now()))
			due, stopDue = t.C(), t.Stop
		}
		poll := c.leaseClk.NewTimer(c.poll)
		select {
		case <-c.ctx.Done():
		case <-due:
			c.wakeAll()
		case <-poll.C():
		case <-c.dispCh:
		}
		poll.Stop()
		if stopDue != nil {
			stopDue()
		}
		if c.ctx.Err() != nil {
			return
		}
	}
}

// pass fires every due schedule and returns the earliest future due time
// of a run or schedule (zero when none).
func (c *Client) pass() time.Time {
	if c.isCrashed() {
		return time.Time{}
	}
	ctx := c.ctx
	now := c.clk.Now()
	var ids []string
	if c.mode != workflow.NodeWorker {
		var err error
		ids, err = c.rdb.ZRangeByScore(ctx, c.base+":scheds", &goredis.ZRangeBy{Min: "-inf", Max: strconv.FormatInt(now.UnixMilli(), 10), Count: 100}).Result()
		if err != nil {
			if ctx.Err() == nil {
				c.log.Error("redis: read due schedules", "error", classify(err))
			}
			return time.Time{}
		}
	}
	for _, id := range ids {
		if err := c.fire(ctx, id, now); err != nil && ctx.Err() == nil {
			c.log.Error("redis: fire a schedule", "schedule", id, "error", err)
		}
	}
	if err := expireScript.Run(ctx, c.rdb, c.keys(), c.base, now.UnixMilli()).Err(); err != nil && ctx.Err() == nil {
		c.log.Error("redis: time out runs past their RunTimeout", "error", classify(err))
	}
	wake := c.admitAll(ctx)
	if !wake.IsZero() && !wake.After(now) {
		wake = now.Add(time.Millisecond)
	}
	c.housekeep(ctx)
	if err := keepScript.Run(ctx, c.rdb, c.keys(), c.base).Err(); err != nil && ctx.Err() == nil {
		c.log.Error("redis: forget runs past a schedule's KeepRuns", "error", classify(err))
	}
	keys := []string{c.base + ":due", c.base + ":rto"}
	if c.mode != workflow.NodeWorker { // a worker node leaves schedules to another node
		keys = append(keys, c.base+":scheds")
	}
	for _, key := range keys {
		zs, err := c.rdb.ZRangeWithScores(ctx, key, 0, 0).Result()
		if err != nil || len(zs) == 0 {
			continue
		}
		at := time.UnixMilli(int64(zs[0].Score))
		if !at.After(now) {
			at = now.Add(time.Millisecond) // due already: look again shortly
			c.wakeAll()
		}
		if wake.IsZero() || at.Before(wake) {
			wake = at
		}
	}
	return wake
}

// housekeep runs the housekeeping script at most every housekeepEvery on
// the lease clock.
func (c *Client) housekeep(ctx context.Context) {
	now := c.leaseClk.Now()
	c.mu.Lock()
	due := c.lastHK.IsZero() || now.Sub(c.lastHK) >= housekeepEvery
	if due {
		c.lastHK = now
	}
	names := c.names
	c.mu.Unlock()
	if !due {
		return
	}
	args := []any{c.base}
	for _, n := range names {
		args = append(args, n)
	}
	if err := housekeepScript.Run(ctx, c.rdb, c.keys(), args...).Err(); err != nil && ctx.Err() == nil {
		c.log.Error("redis: housekeeping", "error", classify(err))
	}
}

// fire fires schedule id's due ticks under its misfire policy. The
// compare-and-set in fireScript makes one client fire each tick.
func (c *Client) fire(ctx context.Context, id string, now time.Time) error {
	h, err := c.rdb.HGetAll(ctx, c.base+":sched:"+id).Result()
	if err != nil {
		return classify(err)
	}
	if len(h) == 0 {
		return c.rdb.ZRem(ctx, c.base+":scheds", id).Err() // removed meanwhile
	}
	c.mu.Lock()
	reg := c.wfs[h["wf"]]
	c.mu.Unlock()
	if reg == nil {
		return nil // its ScheduleWhen and windows are code only a client that registered it has
	}
	info, input, err := c.decodeSchedule(id, h)
	if err != nil {
		return err
	}
	if !info.Paused {
		if info.Paused, err = c.allSchedulesPaused(ctx); err != nil {
			return err
		}
	}
	cs, err := c.parseCronFor(h["cron"], info.Seconds)
	if err != nil {
		return fmt.Errorf("stored cron %q: %w", h["cron"], err)
	}
	src, loc := tickSource{cs, info.Active}, info.Location
	lastMS, _ := strconv.ParseInt(h["last"], 10, 64)
	last := time.UnixMilli(lastMS)
	next := src.Next(now, loc)
	base := workflow.Tick{ScheduleID: workflow.ScheduleID(id), Name: info.Name, Workflow: info.Workflow, Declared: info.Declared,
		Location: loc, Labels: info.Labels}

	// Decide each due tick on this node; the script below moves the
	// schedule from the last value read, so only one node acts on a tick
	// and reports it.
	type candidate struct {
		at  time.Time
		id  string
		run []any
	}
	var (
		skipped   []workflow.Tick
		cands     []candidate
		missed    int
		missedAt  time.Time
		pausedAll bool
	)
	if info.Paused {
		pausedAll = true
		for _, t := range src.Between(last, now, loc, admitBatch) {
			tk := base
			tk.At, tk.Outcome, tk.Reason = t.In(loc), workflow.TickSkipped, workflow.TickReasonPaused
			skipped = append(skipped, tk)
		}
	} else {
		policy := effectiveMisfire(info.Misfire)
		fire := policy.Due(src, last, now, loc)
		missed, missedAt = droppedTicks(src, last, now, loc, fire)
		for _, t := range fire {
			tk := base
			tk.At = t.In(loc)
			switch {
			case !info.Windows.IsZero() && !info.Windows.Allows(t, loc):
				tk.Outcome, tk.Reason = workflow.TickSkipped, workflow.TickReasonWindow
				skipped = append(skipped, tk)
			case reg.wf.ScheduleWhen != nil && !c.askWhen(ctx, reg.wf.ScheduleWhen, tk):
				tk.Outcome, tk.Reason = workflow.TickSkipped, workflow.TickReasonWhen
				skipped = append(skipped, tk)
			default:
				runID := c.newID()
				fired := workflow.FiredLabelsAt(info.Labels, workflow.ScheduleID(id), info.Name, t)
				a, err := c.createArgs(runID, workflow.Job{Workflow: info.Workflow, Payload: input, Labels: fired, Tenant: info.Tenant}, "", false, now)
				if err != nil {
					return err
				}
				cands = append(cands, candidate{at: t, id: runID, run: a})
			}
		}
	}
	args := []any{c.base, id, h["last"], now.UnixMilli(), next.UnixMilli(), len(cands)}
	for _, cd := range cands {
		args = append(append(args, cd.at.UnixMilli()), cd.run...)
	}
	if next.IsZero() {
		c.log.Warn("redis: cron schedule never fires again", "schedule", id, "cron", h["cron"])
		args[4] = int64(1<<53 - 1) // park it at the end of the index
	}
	res, err := fireScript.Run(ctx, c.rdb, c.keys(), args...).Int64Slice()
	if err != nil {
		return classify(err)
	}
	if len(res) != 2 || res[0] == 0 {
		return nil // another client fired these ticks
	}
	var ticks []workflow.Tick
	if missed > 0 && !pausedAll {
		tk := base
		tk.Outcome, tk.At, tk.Missed = workflow.TickMisfired, missedAt.In(loc), missed
		ticks = append(ticks, tk)
	}
	ticks = append(ticks, skipped...)
	for i, cd := range cands {
		tk := base
		tk.At = cd.at.In(loc)
		if int64(i) < res[1] {
			tk.Outcome, tk.Run = workflow.TickFired, workflow.RunRef{RunID: workflow.RunID(cd.id)}
		} else {
			tk.Outcome, tk.Reason = workflow.TickSkipped, workflow.TickReasonOverlap
		}
		ticks = append(ticks, tk)
	}
	sort.SliceStable(ticks, func(i, j int) bool { return ticks[i].At.Before(ticks[j].At) })
	if res[1] > 0 {
		c.wakeAll()
	}
	c.emitTicks(ctx, ticks)
	return nil
}

// allSchedulesPaused reports whether PauseSchedule(All) is in force.
func (c *Client) allSchedulesPaused(ctx context.Context) (bool, error) {
	n, err := c.rdb.Exists(ctx, c.base+":schedpaused").Result()
	if err != nil {
		return false, classify(err)
	}
	return n == 1, nil
}

// askWhen calls a ScheduleWhen, treating a panic as "no" so one bad
// predicate cannot stop the dispatcher.
func (c *Client) askWhen(ctx context.Context, when func(context.Context, workflow.Tick) bool, t workflow.Tick) (ok bool) {
	defer func() {
		if p := recover(); p != nil {
			c.log.Error("redis: ScheduleWhen panicked; the tick is skipped", "schedule", t.ScheduleID, "panic", fmt.Sprint(p))
			ok = false
		}
	}()
	return when(ctx, t)
}

// maxDroppedScan caps how many ticks droppedTicks counts per firing, so a
// frequent cron after a long outage costs bounded time; the count is then
// a lower bound.
const maxDroppedScan = 1 << 16

// droppedTicks counts the ticks of src in (last, now] that fire does not
// hold, and returns the newest of them.
func droppedTicks(src workflow.TickSource, last, now time.Time, loc *time.Location, fire []time.Time) (int, time.Time) {
	fired := make(map[int64]bool, len(fire))
	for _, t := range fire {
		fired[t.UnixNano()] = true
	}
	n := 0
	var newest time.Time
	from := last
	for scanned := 0; scanned < maxDroppedScan; {
		chunk := src.Between(from, now, loc, admitBatch)
		for _, t := range chunk {
			if !fired[t.UnixNano()] {
				n++
				newest = t
			}
		}
		scanned += len(chunk)
		if len(chunk) < admitBatch || !chunk[len(chunk)-1].After(from) {
			break
		}
		from = chunk[len(chunk)-1]
	}
	return n, newest
}

// OnTick registers fn to observe every tick this client fires, skips or
// drops (workflow.TickNotifier): TickFired with the run, TickSkipped for a
// tick a window, ScheduleWhen, a schedule pause or OverlapSkip held back,
// TickMisfired for ticks a late wake dropped, and TickFired with Manual for
// a TriggerSchedule. Each tick is reported by the one client whose script
// moved the schedule, after it ran; a crash in between loses the report.
// Register before Start.
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
			func() {
				defer func() {
					if p := recover(); p != nil {
						c.log.Error("redis: tick observer panicked", "schedule", t.ScheduleID, "panic", fmt.Sprint(p))
					}
				}()
				fn(ctx, t)
			}()
		}
	}
}

// PauseSchedule stops a schedule, or every schedule (All, including ones
// made later), from firing until ResumeSchedule: its due ticks are
// reported TickSkipped with TickReasonPaused and never fire, not even on
// resume. The pause is stored, so every client obeys it. An unknown ID is
// ErrNotFound (workflow.SchedulePauser).
func (c *Client) PauseSchedule(ctx context.Context, t workflow.ScheduleTarget) error {
	return c.setSchedulePause(ctx, t, true)
}

// ResumeSchedule lets a paused schedule fire again from its next tick;
// with All it clears every schedule pause.
func (c *Client) ResumeSchedule(ctx context.Context, t workflow.ScheduleTarget) error {
	return c.setSchedulePause(ctx, t, false)
}

func (c *Client) setSchedulePause(ctx context.Context, t workflow.ScheduleTarget, pause bool) error {
	if err := workflow.ValidateScheduleTarget(t); err != nil {
		return err
	}
	res, err := schedPauseScript.Run(ctx, c.rdb, c.keys(), c.base, string(t.ID), t.All, pause).Text()
	if err != nil {
		return fmt.Errorf("redis: pause schedule: %w", classify(err))
	}
	if res == "notfound" {
		return fmt.Errorf("%w: schedule %q", workflow.ErrNotFound, t.ID)
	}
	return nil
}

// decodeSchedule turns a schedule hash into a ScheduleInfo, its input
// and its cron source.
func (c *Client) decodeSchedule(id string, h map[string]string) (workflow.ScheduleInfo, []byte, error) {
	info := workflow.ScheduleInfo{ID: workflow.ScheduleID(id), Name: h["sname"], Workflow: h["wf"], Cron: h["cron"],
		Declared: h["declared"] == "1", Overlap: workflow.OverlapPolicy(h["overlap"])}
	loc, err := decodeLoc(h["loc"])
	if err != nil {
		return info, nil, fmt.Errorf("redis: schedule %s: stored zone %q: %w", id, h["loc"], err)
	}
	info.Location = loc
	if err := json.Unmarshal([]byte(h["misfire"]), &info.Misfire); err != nil {
		return info, nil, fmt.Errorf("redis: schedule %s: stored misfire policy: %w", id, err)
	}
	if s := h["labels"]; s != "" && s != "{}" {
		if err := json.Unmarshal([]byte(s), &info.Labels); err != nil {
			return info, nil, fmt.Errorf("redis: schedule %s: stored labels: %w", id, err)
		}
	}
	info.Seconds, info.Active = h["sec"] == "1", decodeActive(h["act"])
	info.Paused = h["paused"] == "1"
	if w := h["win"]; w != "" {
		if err := json.Unmarshal([]byte(w), &info.Windows); err != nil {
			return info, nil, fmt.Errorf("redis: schedule %s: stored windows: %w", id, err)
		}
	}
	info.KeepRuns, _ = strconv.Atoi(h["keep"])
	info.Tenant = h["tn"]
	if src, err := c.parseCronFor(h["cron"], info.Seconds); err == nil {
		info.Next = tickSource{src, info.Active}.Next(c.clk.Now(), loc)
	}
	if ms, _ := strconv.ParseInt(h["lfire"], 10, 64); ms != 0 {
		info.Last.Fire = time.UnixMilli(ms).In(loc)
	}
	var input []byte
	if h["input"] != "" {
		input = []byte(h["input"])
	}
	return info, input, nil
}

// Schedules lists every schedule of the prefix, declared and made at run
// time, sorted by Workflow, then Name, then ID, with Last filled from the
// schedule's stored last fire and its run's current status ("" once the
// run is gone).
func (c *Client) Schedules(ctx context.Context) ([]workflow.ScheduleInfo, error) {
	ids, err := c.rdb.ZRange(ctx, c.base+":scheds", 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("redis: list schedules: %w", classify(err))
	}
	all, err := c.allSchedulesPaused(ctx)
	if err != nil {
		return nil, fmt.Errorf("redis: list schedules: %w", err)
	}
	out := make([]workflow.ScheduleInfo, 0, len(ids))
	for _, id := range ids {
		h, err := c.rdb.HGetAll(ctx, c.base+":sched:"+id).Result()
		if err != nil {
			return nil, fmt.Errorf("redis: read schedule: %w", classify(err))
		}
		if len(h) == 0 {
			continue // removed meanwhile
		}
		info, _, err := c.decodeSchedule(id, h)
		if err != nil {
			return nil, err
		}
		info.Paused = info.Paused || all
		if lr := h["lrun"]; lr != "" {
			st, _ := c.rdb.HMGet(ctx, c.base+":run:"+lr, "status", "sid").Result()
			status, sid := "", ""
			if len(st) == 2 {
				status, sid = str(st[0]), str(st[1])
			}
			info.Last.Run = refOf(lr, workflow.Status(status), sid)
			info.Last.Status = workflow.Status(status)
		}
		out = append(out, info)
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

// TriggerSchedule enqueues one run of schedule id now, with its input and
// labels plus workflow.TriggeredLabels, and records it as the schedule's
// last fire; the next cron fire is unchanged. Under OverlapSkip it
// enqueues nothing while the schedule's last run is unfinished and
// returns an error matching workflow.ErrOverlap; the check and the
// enqueue are one script.
func (c *Client) TriggerSchedule(ctx context.Context, id workflow.ScheduleID) (workflow.RunRef, error) {
	if err := c.usable(); err != nil {
		return workflow.RunRef{}, err
	}
	h, err := c.rdb.HGetAll(ctx, c.base+":sched:"+string(id)).Result()
	if err != nil {
		return workflow.RunRef{}, fmt.Errorf("redis: read schedule: %w", classify(err))
	}
	if len(h) == 0 {
		return workflow.RunRef{}, fmt.Errorf("%w: schedule %q", workflow.ErrNotFound, id)
	}
	info, input, err := c.decodeSchedule(string(id), h)
	if err != nil {
		return workflow.RunRef{}, err
	}
	now := c.clk.Now()
	runID := c.newID()
	labels := workflow.TriggeredLabelsAt(info.Labels, id, info.Name, now)
	job := workflow.Job{Workflow: info.Workflow, Payload: input, Labels: labels, Tenant: info.Tenant}
	args, err := c.createArgs(runID, job, "", false, now)
	if err != nil {
		return workflow.RunRef{}, err
	}
	res, err := triggerScript.Run(ctx, c.rdb, c.keys(), append([]any{c.base, string(id), now.UnixMilli()}, args...)...).Slice()
	if err != nil {
		return workflow.RunRef{}, fmt.Errorf("redis: trigger schedule: %w", classify(err))
	}
	switch str(res[0]) {
	case "notfound":
		return workflow.RunRef{}, fmt.Errorf("%w: schedule %q", workflow.ErrNotFound, id)
	case "overlap":
		return workflow.RunRef{}, workflow.OverlapRefusal(id, refOf(str(res[1]), workflow.Status(str(res[2])), str(res[3])), workflow.Status(str(res[2])))
	}
	c.wakeAll()
	ref := workflow.RunRef{RunID: workflow.RunID(runID)}
	c.emitTicks(ctx, []workflow.Tick{{ScheduleID: id, Name: info.Name, Workflow: info.Workflow, Declared: info.Declared,
		At: now.In(info.Location), Location: info.Location, Outcome: workflow.TickFired, Run: ref, Labels: info.Labels, Manual: true}})
	return ref, nil
}

var (
	_ workflow.ScheduleLister    = (*Client)(nil)
	_ workflow.ScheduleTriggerer = (*Client)(nil)
	_ workflow.TickNotifier      = (*Client)(nil)
	_ workflow.SchedulePauser    = (*Client)(nil)
)
