package regional

import (
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/ubgo/workflow"
)

var (
	_ workflow.RunQuerier        = (*Client)(nil)
	_ workflow.RunRetrier        = (*Client)(nil)
	_ workflow.RunDeleter        = (*Client)(nil)
	_ workflow.BulkCanceller     = (*Client)(nil)
	_ workflow.Pauser            = (*Client)(nil)
	_ workflow.SchedulePauser    = (*Client)(nil)
	_ workflow.ScheduleLister    = (*Client)(nil)
	_ workflow.ScheduleTriggerer = (*Client)(nil)
	_ workflow.TickNotifier      = (*Client)(nil)
	_ workflow.Drainer           = (*Client)(nil)
	_ workflow.ReportingDrainer  = (*Client)(nil)
	_ workflow.StopCauser        = (*Client)(nil)
	_ workflow.RunForcer         = (*Client)(nil)
	_ workflow.RunRescheduler    = (*Client)(nil)
	_ workflow.StepCanceller     = (*Client)(nil)
	_ workflow.ResultReader      = (*Client)(nil)
	_ workflow.NodeLister        = (*Client)(nil)
	_ workflow.HealthChecker     = (*Client)(nil)
	_ workflow.LifecycleNotifier = (*Client)(nil)
	_ workflow.PayloadLimiter    = (*Client)(nil)
	_ workflow.OpProber          = (*Client)(nil)
)

// need refuses a call when Capabilities lacks caps, so a method never
// half-works on some regions.
func (c *Client) need(field string, caps workflow.Caps) error {
	if missing := c.Capabilities().Missing(caps); missing != 0 {
		return &workflow.UnsupportedError{Field: field, Missing: missing, Reason: "one of the regions cannot do it"}
	}
	return nil
}

// every returns each region as I, or an UnsupportedError naming caps when
// Capabilities lacks them or a region does not implement I.
func every[I any](c *Client, field string, caps workflow.Caps) (map[string]I, error) {
	if err := c.need(field, caps); err != nil {
		return nil, err
	}
	out := make(map[string]I, len(c.names))
	for _, name := range c.names {
		i, ok := c.regions[name].(I)
		if !ok {
			return nil, &workflow.UnsupportedError{Field: field, Missing: caps, Reason: "region " + name + " does not implement it"}
		}
		out[name] = i
	}
	return out, nil
}

// onRegion is onRef for an optional interface: it refuses with an
// UnsupportedError naming caps when Capabilities lacks them or the
// region holding the run does not implement I, then calls fn.
func onRegion[I any](c *Client, field string, caps workflow.Caps, ref workflow.RunRef, fn func(name string, i I, inner workflow.RunRef) error) error {
	if err := c.need(field, caps); err != nil {
		return err
	}
	return c.onRef(ref, func(name string, rc workflow.Client, inner workflow.RunRef) error {
		i, ok := rc.(I)
		if !ok {
			return &workflow.UnsupportedError{Field: field, Missing: caps, Reason: "region " + name + " does not implement it"}
		}
		return fn(name, i, inner)
	})
}

// joinRegions names each region's error, in name order, or returns nil
// when every one is nil.
func joinRegions(op string, names []string, errs map[string]error) error {
	var out []error
	for _, name := range names {
		if err := errs[name]; err != nil {
			out = append(out, fmt.Errorf("regional: %s %s: %w", op, name, err))
		}
	}
	return errors.Join(out...)
}

// Runs lists the runs of every region as one feed, newest enqueue first
// (ties broken by RunID descending), with every Ref prefixed by its
// region like the refs Enqueue returns. Each page holds up to the query's
// limit; the cursor records where each region's feed stands, so paging
// merges them without gaps or repeats as long as each region's own
// paging is stable. A Ref in q asks only the region it names (a ref
// naming none matches no run); a LabelSchedule filter asks only the
// region holding that schedule, with its own ID. Needs CapRunQuery.
func (c *Client) Runs(ctx context.Context, q workflow.RunQuery) (workflow.RunPage, error) {
	qs, err := every[workflow.RunQuerier](c, "Runs", workflow.CapRunQuery)
	if err != nil {
		return workflow.RunPage{}, err
	}
	if err := workflow.ValidateRunQuery(q); err != nil {
		return workflow.RunPage{}, err
	}
	if err := workflow.CheckRunQuery(c.Capabilities(), q); err != nil {
		return workflow.RunPage{}, err
	}
	cur, err := c.decodeCursor(q.Cursor)
	if err != nil {
		return workflow.RunPage{}, err
	}
	only := "" // with a Ref set, the one region asked
	if !q.Ref.IsZero() {
		var ok bool
		only, q.Ref, ok = decodeRef(q.Ref)
		if !ok {
			only = "\x00" // names no region: no feed is asked
		}
	}
	feeds := make([]*feed, 0, len(c.names))
	for _, name := range c.names {
		fd := &feed{region: name, src: qs[name], pos: cur[name]}
		var holds bool
		fd.q, holds = regionQuery(q, name)
		if !holds || (only != "" && only != name) {
			fd.pos = feedPos{Done: true}
		}
		feeds = append(feeds, fd)
	}
	limit := q.PageLimit()
	for _, fd := range feeds {
		if err := fd.load(ctx, limit); err != nil {
			return workflow.RunPage{}, fmt.Errorf("regional: %s: %w", fd.region, err)
		}
	}
	var page workflow.RunPage
	for len(page.Runs) < limit {
		var pick *feed
		for _, fd := range feeds {
			if len(fd.buf) > 0 && (pick == nil || newer(fd.head(), pick.head())) {
				pick = fd
			}
		}
		if pick == nil {
			break
		}
		page.Runs = append(page.Runs, pick.head())
		if err := pick.take(ctx, limit); err != nil {
			return workflow.RunPage{}, fmt.Errorf("regional: %s: %w", pick.region, err)
		}
	}
	next := runsCursor{}
	more := false
	for _, fd := range feeds {
		next[fd.region] = fd.pos
		more = more || fd.more()
	}
	if more {
		page.Next = encodeCursor(next)
	}
	return page, nil
}

// feedPos is where one region's feed stands: the inner cursor of the page
// being read and how many of its runs were already handed out. Done is
// set once the region has nothing more.
type feedPos struct {
	Cursor string `json:"c,omitempty"`
	Skip   int    `json:"k,omitempty"`
	Done   bool   `json:"d,omitempty"`
}

// runsCursor is each region's position, by region name.
type runsCursor map[string]feedPos

func encodeCursor(c runsCursor) string {
	b, _ := json.Marshal(c) // a map of strings, ints and bools always marshals
	return base64.RawURLEncoding.EncodeToString(b)
}

// decodeCursor reads a cursor Runs made. A cursor naming a region this
// client lacks, or a negative skip, was not made by this client.
func (c *Client) decodeCursor(s string) (runsCursor, error) {
	cur := runsCursor{}
	if s == "" {
		return cur, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err == nil {
		err = json.Unmarshal(b, &cur)
	}
	for name, pos := range cur {
		if _, ok := c.regions[name]; !ok || pos.Skip < 0 {
			err = errors.New("bad position")
		}
	}
	if err != nil {
		return nil, fmt.Errorf("%w: regional: Runs cursor was not made by this client", workflow.ErrInvalid)
	}
	return cur, nil
}

// feed is one region's runs, read page by page.
type feed struct {
	region string
	q      workflow.RunQuery // the query as this region sees it (regionQuery)
	src    workflow.RunQuerier
	pos    feedPos
	buf    []workflow.RunInfo // the unread runs of the current page, refs prefixed
	next   string             // the inner cursor after the current page
}

// load reads the page at pos and drops the runs already handed out,
// moving on to later pages while the current one has nothing left.
func (fd *feed) load(ctx context.Context, limit int) error {
	for !fd.pos.Done {
		q := fd.q
		q.Cursor, q.Limit = fd.pos.Cursor, limit
		page, err := fd.src.Runs(ctx, q)
		if err != nil {
			return err
		}
		fd.next = page.Next
		runs := page.Runs[min(fd.pos.Skip, len(page.Runs)):]
		fd.buf = make([]workflow.RunInfo, len(runs))
		for i, r := range runs {
			r = r.Clone()
			r.Ref = encodeRef(fd.region, r.Ref)
			r.Labels = regionLabels(r.Labels, fd.region)
			fd.buf[i] = r
		}
		if len(fd.buf) > 0 {
			return nil
		}
		fd.advance()
	}
	return nil
}

// advance moves pos to the next inner page, or marks the feed done.
func (fd *feed) advance() {
	if fd.next == "" {
		fd.pos = feedPos{Done: true}
		return
	}
	fd.pos = feedPos{Cursor: fd.next}
}

func (fd *feed) head() workflow.RunInfo { return fd.buf[0] }

// take consumes the head run and refills from the next inner page when
// the current one is used up.
func (fd *feed) take(ctx context.Context, limit int) error {
	fd.buf = fd.buf[1:]
	fd.pos.Skip++
	if len(fd.buf) > 0 {
		return nil
	}
	fd.advance()
	return fd.load(ctx, limit)
}

// more reports whether the feed has runs not yet handed out.
func (fd *feed) more() bool { return len(fd.buf) > 0 || !fd.pos.Done }

// newer orders a merged feed: later enqueue first, then RunID descending.
func newer(a, b workflow.RunInfo) bool {
	if c := a.EnqueuedAt.Compare(b.EnqueuedAt); c != 0 {
		return c > 0
	}
	return a.Ref.RunID > b.Ref.RunID
}

// Retry re-runs a failed run in the region its ref names and returns the
// ref prefixed the same way. Needs CapRetryRun.
func (c *Client) Retry(ctx context.Context, ref workflow.RunRef) (workflow.RunRef, error) {
	var out workflow.RunRef
	err := onRegion(c, "Retry", workflow.CapRetryRun, ref, func(name string, r workflow.RunRetrier, inner workflow.RunRef) error {
		got, err := r.Retry(ctx, inner)
		out = encodeRef(name, got)
		return err
	})
	if err != nil {
		return workflow.RunRef{}, err
	}
	return out, nil
}

// Delete forgets a terminal run in the region its ref names. Needs
// CapDeleteRun.
func (c *Client) Delete(ctx context.Context, ref workflow.RunRef) error {
	return onRegion(c, "Delete", workflow.CapDeleteRun, ref, func(_ string, d workflow.RunDeleter, inner workflow.RunRef) error {
		return d.Delete(ctx, inner)
	})
}

// ForceComplete force-completes the run in the region its ref names.
// Needs CapForceRun.
func (c *Client) ForceComplete(ctx context.Context, ref workflow.RunRef) error {
	return onRegion(c, "ForceComplete", workflow.CapForceRun, ref, func(_ string, f workflow.RunForcer, inner workflow.RunRef) error {
		return f.ForceComplete(ctx, inner)
	})
}

// ForceFail force-fails the run in the region its ref names. Needs
// CapForceRun.
func (c *Client) ForceFail(ctx context.Context, ref workflow.RunRef, reason string) error {
	return onRegion(c, "ForceFail", workflow.CapForceRun, ref, func(_ string, f workflow.RunForcer, inner workflow.RunRef) error {
		return f.ForceFail(ctx, inner, reason)
	})
}

// Reschedule moves a delayed run in the region its ref names and returns
// the ref prefixed the same way.
func (c *Client) Reschedule(ctx context.Context, ref workflow.RunRef, at time.Time) (workflow.RunRef, error) {
	var out workflow.RunRef
	err := onRegion(c, "Reschedule", 0, ref, func(name string, r workflow.RunRescheduler, inner workflow.RunRef) error {
		got, err := r.Reschedule(ctx, inner, at)
		out = encodeRef(name, got)
		return err
	})
	if err != nil {
		return workflow.RunRef{}, err
	}
	return out, nil
}

// CancelStep withdraws a step of the run in the region its ref names.
func (c *Client) CancelStep(ctx context.Context, ref workflow.RunRef, step string) error {
	return onRegion(c, "CancelStep", 0, ref, func(_ string, s workflow.StepCanceller, inner workflow.RunRef) error {
		return s.CancelStep(ctx, inner, step)
	})
}

// Result reads the result from the region the ref names. Needs CapResult.
func (c *Client) Result(ctx context.Context, ref workflow.RunRef) ([]byte, error) {
	var out []byte
	err := onRegion(c, "Result", workflow.CapResult, ref, func(_ string, r workflow.ResultReader, inner workflow.RunRef) error {
		var err error
		out, err = r.Result(ctx, inner)
		return err
	})
	return out, err
}

// CancelAll cancels the matching runs in every region, since runs of one
// workflow may sit in any of them, and returns the total. When a region
// fails, the others' cancellations stand and the error names the failing
// one. A LabelSchedule filter asks only the region holding the schedule.
// Needs CapCancelAll.
func (c *Client) CancelAll(ctx context.Context, q workflow.RunQuery) (int, error) {
	cs, err := every[workflow.BulkCanceller](c, "CancelAll", workflow.CapCancelAll)
	if err != nil {
		return 0, err
	}
	if err := workflow.ValidateCancelQuery(q); err != nil {
		return 0, err
	}
	if err := workflow.CheckRunQuery(c.Capabilities(), q); err != nil {
		return 0, err
	}
	total := 0
	errs := map[string]error{}
	for _, name := range c.names {
		rq, ok := regionQuery(q, name)
		if !ok {
			continue
		}
		n, err := cs[name].CancelAll(ctx, rq)
		total += n
		errs[name] = err
	}
	return total, joinRegions("CancelAll", c.names, errs)
}

// Pause holds matching runs in every region, since the runs a target
// names may sit in any of them. Every region is tried; when one refuses,
// the others' pauses stand and the error says which. Needs the
// capability t needs.
func (c *Client) Pause(ctx context.Context, t workflow.PauseTarget) error {
	return c.pauseAll(ctx, "Pause", t, workflow.Pauser.Pause)
}

// Resume removes t in every region.
func (c *Client) Resume(ctx context.Context, t workflow.PauseTarget) error {
	return c.pauseAll(ctx, "Resume", t, workflow.Pauser.Resume)
}

func (c *Client) pauseAll(ctx context.Context, field string, t workflow.PauseTarget, op func(workflow.Pauser, context.Context, workflow.PauseTarget) error) error {
	if err := workflow.CheckPause(c.Capabilities(), t); err != nil {
		return err
	}
	ps, err := every[workflow.Pauser](c, field, workflow.RequiresPause(t))
	if err != nil {
		return err
	}
	errs := map[string]error{}
	for _, name := range c.names {
		errs[name] = op(ps[name], ctx, t)
	}
	return joinRegions(field, c.names, errs)
}

// Paused returns the targets paused in any region, without repeats, in
// the order Pauser documents. A target paused in some regions only (a
// half-applied Pause) is listed, so it can be seen and resumed.
func (c *Client) Paused(ctx context.Context) ([]workflow.PauseTarget, error) {
	ps, err := every[workflow.Pauser](c, "Paused", workflow.CapPauseWorkflow)
	if err != nil {
		return nil, err
	}
	set := map[workflow.PauseTarget]struct{}{}
	errs := map[string]error{}
	for _, name := range c.names {
		ts, err := ps[name].Paused(ctx)
		errs[name] = err
		for _, t := range ts {
			set[t] = struct{}{}
		}
	}
	if err := joinRegions("Paused", c.names, errs); err != nil {
		return nil, err
	}
	out := slices.Collect(maps.Keys(set))
	rank := func(t workflow.PauseTarget) int {
		switch {
		case t.All:
			return 0
		case t.Workflow != "":
			return 1
		}
		return 2
	}
	slices.SortFunc(out, func(a, b workflow.PauseTarget) int {
		return cmp.Or(cmp.Compare(rank(a), rank(b)), cmp.Compare(a.Workflow, b.Workflow), cmp.Compare(a.Queue, b.Queue))
	})
	return out, nil
}

// PauseSchedule pauses the schedule t.ID names in its region, or every
// schedule of every region with t.All. Needs CapSchedulePause.
func (c *Client) PauseSchedule(ctx context.Context, t workflow.ScheduleTarget) error {
	return c.onSchedules(ctx, "PauseSchedule", t, workflow.SchedulePauser.PauseSchedule)
}

// ResumeSchedule resumes what PauseSchedule paused.
func (c *Client) ResumeSchedule(ctx context.Context, t workflow.ScheduleTarget) error {
	return c.onSchedules(ctx, "ResumeSchedule", t, workflow.SchedulePauser.ResumeSchedule)
}

func (c *Client) onSchedules(ctx context.Context, field string, t workflow.ScheduleTarget, op func(workflow.SchedulePauser, context.Context, workflow.ScheduleTarget) error) error {
	sps, err := every[workflow.SchedulePauser](c, field, workflow.CapSchedulePause)
	if err != nil {
		return err
	}
	if err := workflow.ValidateScheduleTarget(t); err != nil {
		return err
	}
	if !t.All {
		name, _, inner, err := c.scheduleRegion(t.ID)
		if err != nil {
			return err
		}
		return op(sps[name], ctx, workflow.ScheduleTarget{ID: inner})
	}
	errs := map[string]error{}
	for _, name := range c.names {
		errs[name] = op(sps[name], ctx, t)
	}
	return joinRegions(field, c.names, errs)
}

// Schedules lists every region's schedules, each ID prefixed by its
// region, sorted by Workflow, then Name, then ID. Needs CapScheduleList.
func (c *Client) Schedules(ctx context.Context) ([]workflow.ScheduleInfo, error) {
	ls, err := every[workflow.ScheduleLister](c, "Schedules", workflow.CapScheduleList)
	if err != nil {
		return nil, err
	}
	var out []workflow.ScheduleInfo
	for _, name := range c.names {
		infos, err := ls[name].Schedules(ctx)
		if err != nil {
			return nil, fmt.Errorf("regional: %s: %w", name, err)
		}
		for _, info := range infos {
			info.ID = encodeScheduleID(name, info.ID)
			if !info.Last.Run.IsZero() {
				info.Last.Run = encodeRef(name, info.Last.Run)
			}
			out = append(out, info)
		}
	}
	slices.SortStableFunc(out, func(a, b workflow.ScheduleInfo) int {
		return cmp.Or(cmp.Compare(a.Workflow, b.Workflow), cmp.Compare(a.Name, b.Name), cmp.Compare(a.ID, b.ID))
	})
	return out, nil
}

// TriggerSchedule runs a schedule now in the region its ID names and
// returns the run's ref prefixed by that region. Needs
// CapScheduleTrigger.
func (c *Client) TriggerSchedule(ctx context.Context, id workflow.ScheduleID) (workflow.RunRef, error) {
	ts, err := every[workflow.ScheduleTriggerer](c, "TriggerSchedule", workflow.CapScheduleTrigger)
	if err != nil {
		return workflow.RunRef{}, err
	}
	name, _, inner, err := c.scheduleRegion(id)
	if err != nil {
		return workflow.RunRef{}, err
	}
	ref, err := ts[name].TriggerSchedule(ctx, inner)
	if err != nil {
		return workflow.RunRef{}, err
	}
	return encodeRef(name, ref), nil
}

// OnTick registers fn in every region, with each tick's ScheduleID and
// Run prefixed by its region. Needs CapTickEvents. Register before
// Start.
func (c *Client) OnTick(fn workflow.TickObserver) error {
	tns, err := every[workflow.TickNotifier](c, "OnTick", workflow.CapTickEvents)
	if err != nil {
		return err
	}
	for _, name := range c.names {
		region := name
		err := tns[name].OnTick(func(ctx context.Context, t workflow.Tick) {
			t.ScheduleID = encodeScheduleID(region, t.ScheduleID)
			if !t.Run.IsZero() {
				t.Run = encodeRef(region, t.Run)
			}
			fn(ctx, t)
		})
		if err != nil {
			return fmt.Errorf("regional: %s: %w", name, err)
		}
	}
	return nil
}

// Drain drains every region at once and returns when all have stopped or
// ctx ends; the error joins each region's. Needs CapDrain.
func (c *Client) Drain(ctx context.Context) error {
	ds, err := every[workflow.Drainer](c, "Drain", workflow.CapDrain)
	if err != nil {
		return err
	}
	errs := c.parallel(func(name string) error { return ds[name].Drain(ctx) })
	return joinRegions("Drain", c.names, errs)
}

// DrainWithReport drains every region at once and merges their reports;
// each cut run's ID carries its region's prefix. Needs CapDrainReport.
func (c *Client) DrainWithReport(ctx context.Context) (workflow.DrainReport, error) {
	ds, err := every[workflow.ReportingDrainer](c, "DrainWithReport", workflow.CapDrainReport)
	if err != nil {
		return workflow.DrainReport{}, err
	}
	var mu sync.Mutex
	reps := map[string]workflow.DrainReport{}
	errs := c.parallel(func(name string) error {
		rep, err := ds[name].DrainWithReport(ctx)
		mu.Lock()
		reps[name] = rep
		mu.Unlock()
		return err
	})
	var merged workflow.DrainReport
	for _, name := range c.names {
		rep := reps[name]
		merged.FinishedRuns += rep.FinishedRuns
		merged.FinishedSteps += rep.FinishedSteps
		for _, cut := range rep.Cut {
			cut.RunID = encodeRef(name, workflow.RunRef{RunID: cut.RunID}).RunID
			merged.Cut = append(merged.Cut, cut)
		}
	}
	return merged, joinRegions("DrainWithReport", c.names, errs)
}

// parallel runs fn once per region at once and returns each error by
// region name.
func (c *Client) parallel(fn func(name string) error) map[string]error {
	var wg sync.WaitGroup
	var mu sync.Mutex
	errs := make(map[string]error, len(c.names))
	for _, name := range c.names {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := fn(name)
			mu.Lock()
			errs[name] = err
			mu.Unlock()
		}()
	}
	wg.Wait()
	return errs
}

// Done is closed once every region has stopped. Without CapDrain it
// returns a nil channel, which never closes, as workflow.Forward does.
func (c *Client) Done() <-chan struct{} {
	ds, err := every[workflow.Drainer](c, "Done", workflow.CapDrain)
	if err != nil {
		return nil
	}
	c.doneOnce.Do(func() {
		c.done = make(chan struct{})
		go func() {
			for _, name := range c.names {
				<-ds[name].Done()
			}
			close(c.done)
		}()
	})
	return c.done
}

// StopCause joins each region's StopCause, each named by its region, so a
// recycle limit reached in any region reads as workflow.ErrRecycle. It is
// nil while no region stopped itself.
func (c *Client) StopCause() error {
	errs := map[string]error{}
	for _, name := range c.names {
		if sc, ok := c.regions[name].(workflow.StopCauser); ok {
			errs[name] = sc.StopCause()
		}
	}
	return joinRegions("StopCause", c.names, errs)
}

// Supports reports an operation as done only when every region does it,
// since a run, or a node, may sit in any of them.
func (c *Client) Supports(op workflow.Op) bool {
	for _, name := range c.names {
		if !workflow.Supports(c.regions[name], op) {
			return false
		}
	}
	return true
}

// Nodes lists every region's nodes, in region name order. A node ID is
// the region's own; give each region's nodes distinct IDs to tell them
// apart. Every region must implement workflow.NodeLister.
func (c *Client) Nodes(ctx context.Context) ([]workflow.NodeInfo, error) {
	var out []workflow.NodeInfo
	errs := map[string]error{}
	for _, name := range c.names {
		nl, ok := c.regions[name].(workflow.NodeLister)
		if !ok {
			return nil, workflow.Unsupported("Nodes", "every region must implement workflow.NodeLister")
		}
		ns, err := nl.Nodes(ctx)
		out = append(out, ns...)
		errs[name] = err
	}
	return out, joinRegions("Nodes", c.names, errs)
}

// Health is live while every region is live and ready while every region
// is ready: a job the router sends to a region that is not ready fails,
// so the client as a whole is ready only when each region is. Each check
// is named after its region.
func (c *Client) Health(ctx context.Context) workflow.HealthReport {
	out := workflow.HealthReport{Live: true, Ready: true}
	for _, name := range c.names {
		rep := workflow.CheckHealth(ctx, c.regions[name])
		out.Live = out.Live && rep.Live
		out.Ready = out.Ready && rep.Ready
		for _, ch := range rep.Checks {
			ch.Name = name + " " + ch.Name
			out.Checks = append(out.Checks, ch)
		}
	}
	return out
}

// OnLifecycle registers fn in every region, with each event's Ref and
// schedule label prefixed like the refs this client hands out. Every
// region must implement workflow.LifecycleNotifier. Register before
// Start.
func (c *Client) OnLifecycle(fn workflow.LifecycleFunc) error {
	ls := make(map[string]workflow.LifecycleNotifier, len(c.names))
	for _, name := range c.names {
		l, ok := c.regions[name].(workflow.LifecycleNotifier)
		if !ok {
			return workflow.Unsupported("OnLifecycle", "every region must implement workflow.LifecycleNotifier")
		}
		ls[name] = l
	}
	if c.isStarted() {
		return workflow.ErrAlreadyStarted
	}
	for _, name := range c.names {
		region := name
		err := ls[name].OnLifecycle(func(ctx context.Context, ev workflow.LifecycleEvent) {
			ev.Ref = encodeRef(region, ev.Ref)
			ev.Labels = regionLabels(ev.Labels, region)
			fn(ctx, ev)
		})
		if err != nil {
			if errors.Is(err, workflow.ErrUnsupported) {
				return err
			}
			return fmt.Errorf("regional: %s: %w", name, err)
		}
	}
	return nil
}

// PayloadLimit is the smallest of the regions' hard caps (a region
// without one counts as no cap), since a job may land in any region.
// Zero means no region has a cap.
func (c *Client) PayloadLimit() int {
	limit := 0
	for _, name := range c.names {
		if l, ok := c.regions[name].(workflow.PayloadLimiter); ok {
			if n := l.PayloadLimit(); n > 0 && (limit == 0 || n < limit) {
				limit = n
			}
		}
	}
	return limit
}
