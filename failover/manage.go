package failover

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

	"github.com/ubgo/workflow"
)

// The capabilities of the schedule family. Schedules live on the primary
// unless WithSchedulesOnBoth, so like CapCron these are the primary's
// alone in that case: the secondary never fires a tick to condition,
// report, list or pause.
const scheduleCaps = workflow.CapCron | workflow.CapScheduleWhen | workflow.CapScheduleWindow |
	workflow.CapSchedulePause | workflow.CapScheduleList | workflow.CapTickEvents |
	workflow.CapOverlapSkip | workflow.CapScheduleTrigger | workflow.CapScheduleHistory |
	workflow.CapCronSeconds | workflow.CapScheduleActive | workflow.CapScheduleKeep | workflow.CapTickTime

var (
	_ workflow.RunQuerier       = (*Client)(nil)
	_ workflow.RunRetrier       = (*Client)(nil)
	_ workflow.RunDeleter       = (*Client)(nil)
	_ workflow.BulkCanceller    = (*Client)(nil)
	_ workflow.Pauser           = (*Client)(nil)
	_ workflow.SchedulePauser   = (*Client)(nil)
	_ workflow.ScheduleLister   = (*Client)(nil)
	_ workflow.TickNotifier     = (*Client)(nil)
	_ workflow.Drainer          = (*Client)(nil)
	_ workflow.ReportingDrainer = (*Client)(nil)
	_ workflow.StopCauser       = (*Client)(nil)
)

// need refuses a call when Capabilities lacks c, so a method never
// half-works on one backend.
func (f *Client) need(field string, c workflow.Caps) error {
	if missing := f.Capabilities().Missing(c); missing != 0 {
		return &workflow.UnsupportedError{Field: field, Missing: missing, Reason: "one of the failover backends cannot do it"}
	}
	return nil
}

// both returns the two backends as I, or an UnsupportedError naming c
// when either does not implement it.
func both[I any](f *Client, field string, c workflow.Caps) (p, s I, err error) {
	if err := f.need(field, c); err != nil {
		return p, s, err
	}
	p, pok := f.primary.(I)
	s, sok := f.secondary.(I)
	if !pok || !sok {
		return p, s, &workflow.UnsupportedError{Field: field, Missing: c, Reason: "a failover backend does not implement it"}
	}
	return p, s, nil
}

// scheduleBackends returns the backends that hold schedules, as I: the
// primary, and the secondary under WithSchedulesOnBoth.
func scheduleBackends[I any](f *Client, field string, c workflow.Caps) (map[side]I, error) {
	if err := f.need(field, c); err != nil {
		return nil, err
	}
	out := map[side]I{}
	sides := []side{primarySide}
	if f.schedBoth {
		sides = append(sides, secondarySide)
	}
	for _, sd := range sides {
		i, ok := f.backend(sd).(I)
		if !ok {
			return nil, &workflow.UnsupportedError{Field: field, Missing: c, Reason: "the " + sd.String() + " backend does not implement it"}
		}
		out[sd] = i
	}
	return out, nil
}

// Runs lists the runs of both backends as one feed, newest enqueue first
// (ties broken by RunID descending), with every Ref prefixed by its
// backend like the refs Enqueue returns. Each page holds up to the
// query's limit; the cursor records where each backend's feed stands, so
// paging merges the two without gaps or repeats as long as each backend's
// own paging is stable. A fired run's LabelSchedule is prefixed like the
// schedule IDs this client hands out, and a LabelSchedule filter in q is
// turned back into each backend's own ID (a backend not holding that
// schedule is not asked). Needs CapRunQuery on both.
func (f *Client) Runs(ctx context.Context, q workflow.RunQuery) (workflow.RunPage, error) {
	pq, sq, err := both[workflow.RunQuerier](f, "Runs", workflow.CapRunQuery)
	if err != nil {
		return workflow.RunPage{}, err
	}
	if err := workflow.ValidateRunQuery(q); err != nil {
		return workflow.RunPage{}, err
	}
	if err := workflow.CheckRunQuery(f.Capabilities(), q); err != nil {
		return workflow.RunPage{}, err
	}
	cur, err := decodeCursor(q.Cursor)
	if err != nil {
		return workflow.RunPage{}, err
	}
	feeds := []*feed{
		{side: primarySide, src: pq, pos: cur.P},
		{side: secondarySide, src: sq, pos: cur.S},
	}
	// A Ref names one backend by its prefix: only that backend is asked,
	// with the driver's own ref, and the run comes back re-prefixed by
	// load. A ref this client did not make (no prefix, or two IDs naming
	// different backends) decodes to unknownSide, so no feed is asked and
	// it matches no run, as an unknown ref does on a driver.
	if !q.Ref.RunID.IsZero() || !q.Ref.ScheduledID.IsZero() {
		sd, inner, _ := decodeRef(q.Ref)
		q.Ref = inner
		for _, fd := range feeds {
			if fd.side != sd {
				fd.pos = feedPos{Done: true}
			}
		}
	}
	limit := q.PageLimit()
	for _, fd := range feeds {
		var holds bool
		if fd.q, holds = sideQuery(q, fd.side); !holds {
			fd.pos = feedPos{Done: true}
		}
		if err := fd.load(ctx, fd.q, limit); err != nil {
			return workflow.RunPage{}, fmt.Errorf("failover: %s: %w", fd.side, err)
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
		if err := pick.take(ctx, pick.q, limit); err != nil {
			return workflow.RunPage{}, fmt.Errorf("failover: %s: %w", pick.side, err)
		}
	}
	if feeds[0].more() || feeds[1].more() {
		page.Next = encodeCursor(runsCursor{P: feeds[0].pos, S: feeds[1].pos})
	}
	return page, nil
}

// feedPos is where one backend's feed stands: the inner cursor of the
// page being read and how many of its runs were already handed out. Done
// is set once the backend has nothing more.
type feedPos struct {
	Cursor string `json:"c,omitempty"`
	Skip   int    `json:"k,omitempty"`
	Done   bool   `json:"d,omitempty"`
}

type runsCursor struct {
	P feedPos `json:"p"`
	S feedPos `json:"s"`
}

func encodeCursor(c runsCursor) string {
	b, _ := json.Marshal(c) // a struct of strings, ints and bools always marshals
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(s string) (runsCursor, error) {
	var c runsCursor
	if s == "" {
		return c, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err == nil {
		err = json.Unmarshal(b, &c)
	}
	if err != nil || c.P.Skip < 0 || c.S.Skip < 0 {
		return runsCursor{}, fmt.Errorf("%w: failover: Runs cursor was not made by this client", workflow.ErrInvalid)
	}
	return c, nil
}

// feed is one backend's runs, read page by page.
type feed struct {
	side side
	q    workflow.RunQuery // the query as this backend sees it (sideQuery)
	src  workflow.RunQuerier
	pos  feedPos
	buf  []workflow.RunInfo // the unread runs of the current page, refs prefixed
	next string             // the inner cursor after the current page
}

// load reads the page at pos and drops the runs already handed out,
// moving on to later pages while the current one has nothing left.
func (fd *feed) load(ctx context.Context, q workflow.RunQuery, limit int) error {
	for !fd.pos.Done {
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
			r.Ref = encodeRef(fd.side, r.Ref)
			r.Labels = sideLabels(r.Labels, fd.side)
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
func (fd *feed) take(ctx context.Context, q workflow.RunQuery, limit int) error {
	fd.buf = fd.buf[1:]
	fd.pos.Skip++
	if len(fd.buf) > 0 {
		return nil
	}
	fd.advance()
	return fd.load(ctx, q, limit)
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

// Retry re-runs a failed run on the backend its ref names, and returns
// the ref prefixed the same way. Needs CapRetryRun on both.
func (f *Client) Retry(ctx context.Context, ref workflow.RunRef) (workflow.RunRef, error) {
	if err := f.need("Retry", workflow.CapRetryRun); err != nil {
		return workflow.RunRef{}, err
	}
	var out workflow.RunRef
	err := f.onSide(ref, func(sd side, c workflow.Client, inner workflow.RunRef) error {
		r, ok := c.(workflow.RunRetrier)
		if !ok {
			return &workflow.UnsupportedError{Field: "Retry", Missing: workflow.CapRetryRun, Reason: "the " + sd.String() + " backend does not implement it"}
		}
		got, err := r.Retry(ctx, inner)
		out = encodeRef(sd, got)
		return err
	})
	if err != nil {
		return workflow.RunRef{}, err
	}
	return out, nil
}

// Delete forgets a terminal run on the backend its ref names. Needs
// CapDeleteRun on both.
func (f *Client) Delete(ctx context.Context, ref workflow.RunRef) error {
	if err := f.need("Delete", workflow.CapDeleteRun); err != nil {
		return err
	}
	return f.onSide(ref, func(sd side, c workflow.Client, inner workflow.RunRef) error {
		d, ok := c.(workflow.RunDeleter)
		if !ok {
			return &workflow.UnsupportedError{Field: "Delete", Missing: workflow.CapDeleteRun, Reason: "the " + sd.String() + " backend does not implement it"}
		}
		return d.Delete(ctx, inner)
	})
}

// onSide is onRef with the side the call went to, for results that must
// be prefixed again.
func (f *Client) onSide(ref workflow.RunRef, fn func(side, workflow.Client, workflow.RunRef) error) error {
	s, _, ok := decodeRef(ref)
	if !ok || s != unknownSide {
		return f.onRef(ref, func(c workflow.Client, inner workflow.RunRef) error { return fn(s, c, inner) })
	}
	// An unprefixed ref: onRef tries the primary, then the secondary;
	// record which one answered.
	tried := primarySide
	return f.onRef(ref, func(c workflow.Client, inner workflow.RunRef) error {
		sd := tried
		tried = secondarySide
		return fn(sd, c, inner)
	})
}

// CancelAll cancels the matching runs on both backends, since runs of one
// workflow may sit on either, and returns the total. When one backend
// fails, the other's cancellations stand and the error names the failing
// one. A LabelSchedule filter is translated as in Runs. Needs
// CapCancelAll on both.
func (f *Client) CancelAll(ctx context.Context, q workflow.RunQuery) (int, error) {
	pc, sc, err := both[workflow.BulkCanceller](f, "CancelAll", workflow.CapCancelAll)
	if err != nil {
		return 0, err
	}
	if err := workflow.ValidateCancelQuery(q); err != nil {
		return 0, err
	}
	if err := workflow.CheckRunQuery(f.Capabilities(), q); err != nil {
		return 0, err
	}
	var pn, sn int
	var perr, serr error
	if pq, ok := sideQuery(q, primarySide); ok {
		pn, perr = pc.CancelAll(ctx, pq)
	}
	if sq, ok := sideQuery(q, secondarySide); ok {
		sn, serr = sc.CancelAll(ctx, sq)
	}
	return pn + sn, joinSides("CancelAll", perr, serr)
}

// Pause holds matching runs on both backends, since the runs a target
// names may sit on either. It is applied to the primary, then the
// secondary; when the secondary refuses, the primary's pause stands and
// the error says so. Needs the capability t needs on both.
func (f *Client) Pause(ctx context.Context, t workflow.PauseTarget) error {
	pp, sp, err := f.pausers(t)
	if err != nil {
		return err
	}
	return joinSides("Pause", pp.Pause(ctx, t), sp.Pause(ctx, t))
}

// Resume removes t on both backends.
func (f *Client) Resume(ctx context.Context, t workflow.PauseTarget) error {
	pp, sp, err := f.pausers(t)
	if err != nil {
		return err
	}
	return joinSides("Resume", pp.Resume(ctx, t), sp.Resume(ctx, t))
}

func (f *Client) pausers(t workflow.PauseTarget) (p, s workflow.Pauser, err error) {
	if err := workflow.CheckPause(f.Capabilities(), t); err != nil {
		return nil, nil, err
	}
	return both[workflow.Pauser](f, "Pause", workflow.RequiresPause(t))
}

// Paused returns the targets paused on either backend, without repeats,
// in the order Pauser documents. A target paused on one backend only (a
// half-applied Pause) is listed, so it can be seen and resumed.
func (f *Client) Paused(ctx context.Context) ([]workflow.PauseTarget, error) {
	pp, sp, err := both[workflow.Pauser](f, "Paused", workflow.CapPauseWorkflow)
	if err != nil {
		return nil, err
	}
	pt, perr := pp.Paused(ctx)
	st, serr := sp.Paused(ctx)
	if err := joinSides("Paused", perr, serr); err != nil {
		return nil, err
	}
	set := map[workflow.PauseTarget]struct{}{}
	for _, t := range append(pt, st...) {
		set[t] = struct{}{}
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

// PauseSchedule pauses the schedule t names on every backend that holds
// it (decoded from the ScheduleID, as Unschedule does), or every schedule
// with t.All. Needs CapSchedulePause, which is the primary's alone unless
// WithSchedulesOnBoth.
func (f *Client) PauseSchedule(ctx context.Context, t workflow.ScheduleTarget) error {
	return f.onSchedules(ctx, "PauseSchedule", t, func(ctx context.Context, sp workflow.SchedulePauser, t workflow.ScheduleTarget) error {
		return sp.PauseSchedule(ctx, t)
	})
}

// ResumeSchedule resumes what PauseSchedule paused.
func (f *Client) ResumeSchedule(ctx context.Context, t workflow.ScheduleTarget) error {
	return f.onSchedules(ctx, "ResumeSchedule", t, func(ctx context.Context, sp workflow.SchedulePauser, t workflow.ScheduleTarget) error {
		return sp.ResumeSchedule(ctx, t)
	})
}

func (f *Client) onSchedules(ctx context.Context, field string, t workflow.ScheduleTarget, fn func(context.Context, workflow.SchedulePauser, workflow.ScheduleTarget) error) error {
	backends, err := scheduleBackends[workflow.SchedulePauser](f, field, workflow.CapSchedulePause)
	if err != nil {
		return err
	}
	if err := workflow.ValidateScheduleTarget(t); err != nil {
		return err
	}
	ids := map[side]workflow.ScheduleID{}
	if !t.All {
		ids[primarySide], ids[secondarySide] = decodeScheduleID(t.ID)
	}
	errs := map[side]error{}
	applied := 0
	for sd, sp := range backends {
		target := t
		if !t.All {
			if ids[sd].IsZero() {
				continue // the schedule has no copy there
			}
			target.ID = ids[sd]
		}
		applied++
		errs[sd] = fn(ctx, sp, target)
	}
	if applied == 0 {
		return fmt.Errorf("%w: failover: schedule %q is on no backend this client schedules on", workflow.ErrNotFound, t.ID)
	}
	return joinSides(field, errs[primarySide], errs[secondarySide])
}

// Schedules lists the schedules of the primary, and of the secondary
// under WithSchedulesOnBoth, with each ID prefixed by its backend. Under
// WithSchedulesOnBoth a schedule therefore appears twice, once per copy,
// and pausing one ID pauses that copy only; the ScheduleID Schedule
// returned covers both. Sorted by Workflow, then Name, primary first.
// Needs CapScheduleList.
func (f *Client) Schedules(ctx context.Context) ([]workflow.ScheduleInfo, error) {
	backends, err := scheduleBackends[workflow.ScheduleLister](f, "Schedules", workflow.CapScheduleList)
	if err != nil {
		return nil, err
	}
	var out []workflow.ScheduleInfo
	for _, sd := range []side{primarySide, secondarySide} {
		l, ok := backends[sd]
		if !ok {
			continue
		}
		infos, err := l.Schedules(ctx)
		if err != nil {
			return nil, fmt.Errorf("failover: %s: %w", sd, err)
		}
		for _, info := range infos {
			info.ID = encodeScheduleID(sd, info.ID)
			if !info.Last.Run.IsZero() {
				info.Last.Run = encodeRef(sd, info.Last.Run)
			}
			out = append(out, info)
		}
	}
	slices.SortStableFunc(out, func(a, b workflow.ScheduleInfo) int {
		return cmp.Or(cmp.Compare(a.Workflow, b.Workflow), cmp.Compare(a.Name, b.Name))
	})
	return out, nil
}

// OnTick registers fn on the primary, and on the secondary under
// WithSchedulesOnBoth, with each tick's ScheduleID and Run prefixed by its
// backend. Needs CapTickEvents. Register before Start.
func (f *Client) OnTick(fn workflow.TickObserver) error {
	backends, err := scheduleBackends[workflow.TickNotifier](f, "OnTick", workflow.CapTickEvents)
	if err != nil {
		return err
	}
	for _, sd := range []side{primarySide, secondarySide} {
		tn, ok := backends[sd]
		if !ok {
			continue
		}
		if err := tn.OnTick(sideTick(fn, sd)); err != nil {
			return fmt.Errorf("failover: %s: %w", sd, err)
		}
	}
	return nil
}

func sideTick(fn workflow.TickObserver, sd side) workflow.TickObserver {
	return func(ctx context.Context, t workflow.Tick) {
		t.ScheduleID = encodeScheduleID(sd, t.ScheduleID)
		if !t.Run.RunID.IsZero() || !t.Run.ScheduledID.IsZero() {
			t.Run = encodeRef(sd, t.Run)
		}
		fn(ctx, t)
	}
}

// Drain drains both backends at once and returns when both have stopped
// or ctx ends; the error joins each backend's. Needs CapDrain on both.
func (f *Client) Drain(ctx context.Context) error {
	pd, sd, err := both[workflow.Drainer](f, "Drain", workflow.CapDrain)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	var perr, serr error
	wg.Add(2)
	go func() { defer wg.Done(); perr = pd.Drain(ctx) }()
	go func() { defer wg.Done(); serr = sd.Drain(ctx) }()
	wg.Wait()
	return joinSides("Drain", perr, serr)
}

// DrainWithReport drains both backends at once and merges their reports;
// each cut run's ID carries its backend's prefix, as every other ID the
// failover client returns. Needs CapDrainReport on both.
func (f *Client) DrainWithReport(ctx context.Context) (workflow.DrainReport, error) {
	pd, sd, err := both[workflow.ReportingDrainer](f, "DrainWithReport", workflow.CapDrainReport)
	if err != nil {
		return workflow.DrainReport{}, err
	}
	var wg sync.WaitGroup
	var prep, srep workflow.DrainReport
	var perr, serr error
	wg.Add(2)
	go func() { defer wg.Done(); prep, perr = pd.DrainWithReport(ctx) }()
	go func() { defer wg.Done(); srep, serr = sd.DrainWithReport(ctx) }()
	wg.Wait()
	merged := workflow.DrainReport{
		FinishedRuns:  prep.FinishedRuns + srep.FinishedRuns,
		FinishedSteps: prep.FinishedSteps + srep.FinishedSteps,
	}
	for _, c := range prep.Cut {
		c.RunID = encodeRef(primarySide, workflow.RunRef{RunID: c.RunID}).RunID
		merged.Cut = append(merged.Cut, c)
	}
	for _, c := range srep.Cut {
		c.RunID = encodeRef(secondarySide, workflow.RunRef{RunID: c.RunID}).RunID
		merged.Cut = append(merged.Cut, c)
	}
	return merged, joinSides("DrainWithReport", perr, serr)
}

// Done is closed once both backends have stopped. Without CapDrain on both
// it returns a nil channel, which never closes, as workflow.Forward does.
func (f *Client) Done() <-chan struct{} {
	if f.need("Done", workflow.CapDrain) != nil {
		return nil
	}
	pd, pok := f.primary.(workflow.Drainer)
	sd, sok := f.secondary.(workflow.Drainer)
	if !pok || !sok {
		return nil
	}
	f.doneOnce.Do(func() {
		f.done = make(chan struct{})
		go func() {
			<-pd.Done()
			<-sd.Done()
			close(f.done)
		}()
	})
	return f.done
}

// StopCause joins each backend's StopCause, each named by its side, so a
// recycle limit reached on either backend reads as workflow.ErrRecycle.
// It is nil while neither stopped itself. Done still closes only once
// both backends stopped: give both the same recycle limits, or drain the
// other side when one recycles.
func (f *Client) StopCause() error {
	cause := func(c workflow.Client) error {
		if sc, ok := c.(workflow.StopCauser); ok {
			return sc.StopCause()
		}
		return nil
	}
	return joinSides("StopCause", cause(f.primary), cause(f.secondary))
}

// joinSides names each backend's error, or returns nil when both are nil.
func joinSides(op string, perr, serr error) error {
	var errs []error
	if perr != nil {
		errs = append(errs, fmt.Errorf("failover: %s primary: %w", op, perr))
	}
	if serr != nil {
		errs = append(errs, fmt.Errorf("failover: %s secondary: %w", op, serr))
	}
	return errors.Join(errs...)
}
