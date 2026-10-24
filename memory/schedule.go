package memory

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/cron"
)

// schedule is one recurring trigger: a Workflow.Cron entry or a
// Client.Schedule call. Its goroutine fires until halt or shutdown.
// Fields below the line may change while it runs (an upsert, a pause) and
// are guarded by Client.mu.
type schedule struct {
	id       workflow.ScheduleID
	s        cron.Schedule
	loc      *time.Location
	workflow string
	declared bool
	stop     chan struct{} // closed by halt

	// guarded by Client.mu
	name    string // Schedule.Name, or the expression of a declared cron
	expr    string // the cron expression as given (name for a declared cron)
	misfire workflow.Misfire
	windows workflow.Windows
	input   []byte
	labels  map[string]string
	halted  bool
	paused  bool
	overlap workflow.OverlapPolicy
	last    workflow.ScheduleLast // Fire and Run of the last fire; Status is filled when listed
	seconds bool                  // Schedule.Seconds: parsed with cron.ParseSeconds
	active  workflow.Active       // Schedule.Active: already applied to s through activeSchedule
	keep    int                   // Schedule.KeepRuns; zero keeps every run
	tenant  string                // Schedule.Tenant: Job.Tenant of every fired run
	fired   []*run                // the runs it fired, oldest first, while KeepRuns is set
}

// activeSchedule limits a cron schedule to an Active period, so the loop,
// the misfire plan and Schedules all see only ticks inside it.
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

// trackFiredLocked ties the run s just fired to s, for KeepRuns. Callers
// hold Client.mu.
func (c *Client) trackFiredLocked(s *schedule, ref workflow.RunRef) {
	if s.keep <= 0 {
		return
	}
	r, err := c.lookupLocked(ref)
	if err != nil || r.sched != nil {
		return // an idempotent duplicate is already tracked
	}
	r.sched = s
	s.fired = append(s.fired, r)
}

// bounded applies an Active period to a parsed schedule.
func bounded(s cron.Schedule, a workflow.Active) cron.Schedule {
	if a.IsZero() {
		return s
	}
	return activeSchedule{Schedule: s, active: a}
}

// keepLocked forgets the oldest finished runs s fired beyond its KeepRuns,
// as Delete would. Callers hold Client.mu.
func (c *Client) keepLocked(s *schedule) {
	if s.keep <= 0 {
		return
	}
	s.fired = slices.DeleteFunc(s.fired, func(r *run) bool { return r.forgotten })
	finished := 0
	for _, r := range s.fired {
		if r.status.IsTerminal() {
			finished++
		}
	}
	for i := 0; finished > s.keep && i < len(s.fired); {
		r := s.fired[i]
		if !r.status.IsTerminal() {
			i++
			continue
		}
		c.dropLocked(r)
		c.forgetFinishedLocked(r)
		r.forgotten = true
		s.fired = slices.Delete(s.fired, i, i+1)
		finished--
	}
}

// halt stops the schedule's goroutine. Callers hold Client.mu.
func (s *schedule) halt() {
	if !s.halted {
		s.halted = true
		close(s.stop)
	}
}

// declaredID is the stable ScheduleID of Cron[i] of a workflow.
func declaredID(wf string, i int) workflow.ScheduleID {
	return workflow.ScheduleID("cron:" + wf + ":" + strconv.Itoa(i))
}

// effectiveMisfire is the policy the loop applies: the schedule's own, or
// for the zero policy this driver's default, fire the newest missed tick
// once with no age limit.
func effectiveMisfire(m workflow.Misfire) workflow.Misfire {
	if m.IsZero() {
		return workflow.Misfire{Policy: workflow.MisfireFireOnce}
	}
	return m
}

// pausedLocked reports whether s is paused, by itself or by PauseSchedule
// with All.
func (c *Client) schedPausedLocked(s *schedule) bool { return s.paused || c.schedPaused }

// startSchedule runs s until it is halted or the client stops. Callers
// hold Client.mu.
//
// The loop keeps two instants: last, when it last caught up, and seen,
// the newest tick it has armed a timer for. On each wake it asks the
// misfire policy (workflow.Misfire.Plan) which ticks in (last, now] to
// fire; each fired tick then passes the schedule's windows and the
// workflow's ScheduleWhen, or is reported skipped. While the schedule is
// paused, each wake reports its tick skipped and moves last past it, so
// a tick that fell due during a pause never fires, not even on resume:
// a pause means "not now", not "later".
func (c *Client) startSchedule(s *schedule) {
	c.wg.Add(1)
	last := c.clk.Now()
	seen := last
	go func() {
		defer c.wg.Done()
		for {
			next := s.s.Next(seen, s.loc)
			if next.IsZero() {
				c.log.Warn("memory: cron schedule never fires again", "workflow", s.workflow, "cron", s.s.String())
				return
			}
			// Read the clock as late as possible: a delay from an
			// earlier reading lands past next whenever the clock moved
			// in between (a fake clock advanced by a test), and the tick
			// would fire late.
			t := c.clk.NewTimer(next.Sub(c.clk.Now()))
			select {
			case <-t.C():
			case <-s.stop:
				t.Stop()
				return
			case <-c.ctx.Done():
				t.Stop()
				return
			}
			c.mu.Lock()
			if s.halted {
				c.mu.Unlock()
				return
			}
			now := c.clk.Now()
			if c.schedPausedLocked(s) {
				// Every tick up to now is the pause's: report the newest
				// and catch up past them all, so none fires on resume.
				c.queueTickLocked(s.tick(workflow.LastTick(s.s, last, now, s.loc), workflow.TickSkipped, workflow.TickReasonPaused))
				last, seen = now, now
				c.mu.Unlock()
				continue
			}
			fire, dropped := effectiveMisfire(s.misfire).Plan(s.s, last, now, s.loc)
			if dropped > 0 {
				newest := now
				if len(fire) > 0 {
					newest = fire[0].Add(-time.Nanosecond)
				}
				mt := s.tick(workflow.LastTick(s.s, last, newest, s.loc), workflow.TickMisfired, "")
				mt.Missed = dropped
				c.queueTickLocked(mt)
			}
			last, seen = now, now
			windows, labels, input := s.windows, s.labels, s.input
			reg := c.wfs[s.workflow]
			c.mu.Unlock()
			c.fire(s, reg, fire, windows, labels, input)
		}
	}()
}

// tick builds a Tick of s. Callers hold Client.mu.
func (s *schedule) tick(at time.Time, outcome workflow.TickOutcome, reason string) workflow.Tick {
	return workflow.Tick{
		ScheduleID: s.id,
		Name:       s.name,
		Workflow:   s.workflow,
		Declared:   s.declared,
		At:         at,
		Location:   s.loc,
		Outcome:    outcome,
		Reason:     reason,
		Labels:     s.labels,
	}
}

// fire decides and enqueues the ticks a wake made due. Windows and
// ScheduleWhen are evaluated without the lock, since ScheduleWhen is app
// code that may call back into the client.
func (c *Client) fire(s *schedule, reg *registered, ticks []time.Time, windows workflow.Windows, labels map[string]string, input []byte) {
	for _, at := range ticks {
		c.mu.Lock()
		tk := s.tick(at, workflow.TickFired, "")
		c.mu.Unlock()
		reason := ""
		switch {
		case !windows.Allows(at, s.loc):
			reason = workflow.TickReasonWindow
		case reg != nil && reg.wf.ScheduleWhen != nil && !c.askWhen(reg.wf.ScheduleWhen, tk):
			reason = workflow.TickReasonWhen
		}
		c.mu.Lock()
		if s.halted {
			c.mu.Unlock()
			return
		}
		if reason == "" && s.overlap.Blocks(c.lastStatusLocked(s)) {
			reason = workflow.TickReasonOverlap
		}
		if reason != "" {
			tk.Outcome, tk.Reason = workflow.TickSkipped, reason
			c.queueTickLocked(tk)
			c.mu.Unlock()
			continue
		}
		job := workflow.Job{Workflow: s.workflow, Payload: input, Labels: workflow.FiredLabelsAt(labels, s.id, s.name, at), Tenant: s.tenant}
		ref, err := c.enqueueLocked(context.Background(), job)
		if err == nil {
			s.last = workflow.ScheduleLast{Fire: at, Run: ref}
			c.trackFiredLocked(s, ref)
		}
		tk.Run, tk.Err = ref, err
		c.queueTickLocked(tk)
		c.mu.Unlock()
		if err != nil {
			c.log.Error("memory: cron fire could not enqueue", "workflow", s.workflow, "cron", s.s.String(), "error", err)
		}
	}
}

// askWhen calls a ScheduleWhen, treating a panic as "skip" so app code
// cannot kill the schedule.
func (c *Client) askWhen(when func(context.Context, workflow.Tick) bool, tk workflow.Tick) (ok bool) {
	defer func() {
		if p := recover(); p != nil {
			c.log.Error("memory: ScheduleWhen panicked; the tick is skipped", "workflow", tk.Workflow, "schedule", tk.ScheduleID, "panic", p)
			ok = false
		}
	}()
	return when(c.ctx, tk)
}

// Schedule registers a recurring trigger evaluated in s.Location on the
// injected clock. It needs Start first and a registered workflow.
//
// A schedule with a Name is upserted by (Workflow, Name), as on the SQL
// driver: calling Schedule again with the same name returns the same
// ScheduleID and replaces the schedule (re-activating it after an
// Unschedule) instead of adding a second. A replaced schedule computes its
// next tick from the moment of the call, so an unchanged schedule keeps
// its next fire time. Without a Name every call adds a schedule.
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
	parsed, err := c.parseCronFor(s.Cron, s.Seconds)
	if err != nil {
		return "", fmt.Errorf("%w: schedule for %q: %w", workflow.ErrInvalid, s.Workflow, err)
	}
	parsed = bounded(parsed, s.Active)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.usableLocked(); err != nil {
		return "", err
	}
	if _, ok := c.wfs[s.Workflow]; !ok {
		return "", fmt.Errorf("%w: workflow %q is not registered", workflow.ErrNotFound, s.Workflow)
	}
	var id workflow.ScheduleID
	paused := false
	var last workflow.ScheduleLast
	if s.Name != "" {
		for oid, old := range c.scheds {
			if !old.declared && old.workflow == s.Workflow && old.name == s.Name {
				id, paused, last = oid, old.paused && !old.halted, old.last
				old.halt()
				break
			}
		}
	}
	if id.IsZero() {
		id = workflow.ScheduleID(c.nextID("schedule"))
	}
	name := s.Name
	sc := &schedule{
		id:       id,
		name:     name,
		expr:     s.Cron,
		s:        parsed,
		loc:      s.Location,
		misfire:  s.Misfire,
		windows:  s.Windows,
		workflow: s.Workflow,
		input:    s.Input,
		labels:   maps.Clone(s.Labels),
		paused:   paused,
		overlap:  s.Overlap,
		last:     last,
		stop:     make(chan struct{}),
		seconds:  s.Seconds,
		active:   s.Active,
		keep:     s.KeepRuns,
		tenant:   s.Tenant,
	}
	c.scheds[id] = sc
	c.startSchedule(sc)
	return id, nil
}

// Unschedule stops a schedule. An empty id, or one already removed, is a
// no-op; an id this client never issued returns ErrNotFound. A declared
// cron is part of its workflow and is refused (ErrInvalid): remove it from
// Workflow.Cron instead, or pause it.
func (c *Client) Unschedule(_ context.Context, id workflow.ScheduleID) error {
	if id.IsZero() {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	sc, ok := c.scheds[id]
	if !ok {
		return fmt.Errorf("%w: schedule %q", workflow.ErrNotFound, id)
	}
	if sc.declared {
		return fmt.Errorf("%w: schedule %q is declared by Workflow.Cron of %q; remove it there or PauseSchedule it", workflow.ErrInvalid, id, sc.workflow)
	}
	sc.halt() // the entry stays as a tombstone so a second call is a no-op
	return nil
}

// Schedules lists the live schedules, declared and run-time, sorted by
// workflow, name and ID, each with its next fire time in its zone (zero
// while paused).
func (c *Client) Schedules(ctx context.Context) ([]workflow.ScheduleInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clk.Now()
	var out []workflow.ScheduleInfo
	for _, s := range c.scheds {
		if s.halted {
			continue
		}
		info := workflow.ScheduleInfo{
			ID:       s.id,
			Name:     s.name,
			Workflow: s.workflow,
			Cron:     s.expr,
			Location: s.loc,
			Declared: s.declared,
			Paused:   c.schedPausedLocked(s),
			Misfire:  s.misfire,
			Windows:  s.windows,
			Labels:   maps.Clone(s.labels),
			Overlap:  s.overlap,
			Last:     c.lastLocked(s),
			Seconds:  s.seconds,
			Active:   s.active,
			KeepRuns: s.keep,
			Tenant:   s.tenant,
		}
		if info.Cron == "" {
			info.Cron = s.name
		}
		if !info.Paused {
			if next := s.s.Next(now, s.loc); !next.IsZero() {
				info.Next = next.In(s.loc)
			}
		}
		out = append(out, info)
	}
	slices.SortFunc(out, func(a, b workflow.ScheduleInfo) int {
		return cmp.Or(cmp.Compare(a.Workflow, b.Workflow), cmp.Compare(a.Name, b.Name), cmp.Compare(a.ID, b.ID))
	})
	return out, nil
}

// PauseSchedule stops a schedule, or every schedule, from firing. See
// workflow.SchedulePauser: a tick that falls due during the pause never
// fires, not even on resume.
func (c *Client) PauseSchedule(_ context.Context, t workflow.ScheduleTarget) error {
	return c.setSchedulePause(t, true)
}

// ResumeSchedule lets a paused schedule, or every schedule, fire again
// from its next tick.
func (c *Client) ResumeSchedule(_ context.Context, t workflow.ScheduleTarget) error {
	return c.setSchedulePause(t, false)
}

func (c *Client) setSchedulePause(t workflow.ScheduleTarget, pause bool) error {
	if err := workflow.ValidateScheduleTarget(t); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.usableLocked(); err != nil {
		return err
	}
	if t.All {
		c.schedPaused = pause
		if !pause {
			for _, s := range c.scheds {
				s.paused = false
			}
		}
		return nil
	}
	s, ok := c.scheds[t.ID]
	if !ok || s.halted {
		return fmt.Errorf("%w: schedule %q", workflow.ErrNotFound, t.ID)
	}
	s.paused = pause
	return nil
}

// lastStatusLocked is the status of s's last run, or "" when s never
// fired or the run has been forgotten (retention, Delete). Callers hold
// Client.mu.
func (c *Client) lastStatusLocked(s *schedule) workflow.Status {
	if s.last.Run.IsZero() {
		return ""
	}
	r, err := c.lookupLocked(s.last.Run)
	if err != nil {
		return ""
	}
	return r.statusLocked()
}

// lastLocked is s's last-fire bookkeeping with the run's current status.
// Callers hold Client.mu.
func (c *Client) lastLocked(s *schedule) workflow.ScheduleLast {
	l := s.last
	l.Status = c.lastStatusLocked(s)
	return l
}

// TriggerSchedule runs schedule id now with its input and labels (see
// workflow.ScheduleTriggerer): it obeys the overlap policy, ignores
// windows, ScheduleWhen and a pause, and becomes the schedule's last fire.
func (c *Client) TriggerSchedule(ctx context.Context, id workflow.ScheduleID) (workflow.RunRef, error) {
	if err := ctx.Err(); err != nil {
		return workflow.RunRef{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.usableLocked(); err != nil {
		return workflow.RunRef{}, err
	}
	s, ok := c.scheds[id]
	if !ok || s.halted {
		return workflow.RunRef{}, fmt.Errorf("%w: schedule %q", workflow.ErrNotFound, id)
	}
	if st := c.lastStatusLocked(s); s.overlap.Blocks(st) {
		return workflow.RunRef{}, workflow.OverlapRefusal(id, s.last.Run, st)
	}
	now := c.clk.Now()
	job := workflow.Job{Workflow: s.workflow, Payload: s.input, Labels: workflow.TriggeredLabelsAt(s.labels, s.id, s.name, now), Tenant: s.tenant}
	ref, err := c.enqueueLocked(ctx, job)
	if err == nil {
		c.trackFiredLocked(s, ref)
	}
	tk := s.tick(now, workflow.TickFired, "")
	tk.Manual, tk.Run, tk.Err = true, ref, err
	c.queueTickLocked(tk)
	if err != nil {
		return workflow.RunRef{}, err
	}
	s.last = workflow.ScheduleLast{Fire: now, Run: ref}
	return ref, nil
}
