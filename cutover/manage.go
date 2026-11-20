package cutover

import (
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/ubgo/workflow"
)

var (
	_ workflow.RunQuerier         = (*Client)(nil)
	_ workflow.RunRetrier         = (*Client)(nil)
	_ workflow.RunDeleter         = (*Client)(nil)
	_ workflow.BulkCanceller      = (*Client)(nil)
	_ workflow.Pauser             = (*Client)(nil)
	_ workflow.SchedulePauser     = (*Client)(nil)
	_ workflow.ScheduleLister     = (*Client)(nil)
	_ workflow.Drainer            = (*Client)(nil)
	_ workflow.NodeLister         = (*Client)(nil)
	_ workflow.HealthChecker      = (*Client)(nil)
	_ workflow.RunEditor          = (*Client)(nil)
	_ workflow.RunRescheduler     = (*Client)(nil)
	_ workflow.StepCanceller      = (*Client)(nil)
	_ workflow.RunResetter        = (*Client)(nil)
	_ workflow.TransientSignaller = (*Client)(nil)
	_ workflow.OpProber           = (*Client)(nil)
)

// both returns the two backends as I, or an UnsupportedError naming caps
// when Capabilities lacks it or either backend does not implement I.
func both[I any](c *Client, field string, caps workflow.Caps) (o, n I, err error) {
	if err := c.need(field, caps); err != nil {
		return o, n, err
	}
	o, ook := c.old.(I)
	n, nok := c.new.(I)
	if !ook || !nok {
		return o, n, &workflow.UnsupportedError{Field: field, Missing: caps, Reason: "a cutover backend does not implement it"}
	}
	return o, n, nil
}

// Runs lists the runs of both backends as one feed, newest enqueue first
// (ties broken by RunID descending), every Ref prefixed by its backend.
// The cursor records where each backend's feed stands, so paging merges
// the two without gaps or repeats as long as each backend's own paging is
// stable. A Ref filter asks only the backend it names (an unprefixed one
// asks both); a LabelSchedule filter holding a prefixed ID asks only that
// backend, with its own ID. Needs CapRunQuery on both.
func (c *Client) Runs(ctx context.Context, q workflow.RunQuery) (workflow.RunPage, error) {
	oq, nq, err := both[workflow.RunQuerier](c, "Runs", workflow.CapRunQuery)
	if err != nil {
		return workflow.RunPage{}, err
	}
	if err := workflow.ValidateRunQuery(q); err != nil {
		return workflow.RunPage{}, err
	}
	if err := workflow.CheckRunQuery(c.Capabilities(), q); err != nil {
		return workflow.RunPage{}, err
	}
	cur, err := decodeCursor(q.Cursor)
	if err != nil {
		return workflow.RunPage{}, err
	}
	feeds := []*feed{{side: SideOld, src: oq, pos: cur.O}, {side: SideNew, src: nq, pos: cur.N}}
	if !q.Ref.RunID.IsZero() || !q.Ref.ScheduledID.IsZero() {
		sd, inner, ok := decodeRef(q.Ref)
		q.Ref = inner
		for _, fd := range feeds {
			if !ok || (sd != "" && fd.side != sd) {
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
		if err := fd.load(ctx, limit); err != nil {
			return workflow.RunPage{}, fmt.Errorf("cutover: %s: %w", fd.side, err)
		}
	}
	var page workflow.RunPage
	for len(page.Runs) < limit {
		var pick *feed
		for _, fd := range feeds {
			if len(fd.buf) > 0 && (pick == nil || newer(fd.buf[0], pick.buf[0])) {
				pick = fd
			}
		}
		if pick == nil {
			break
		}
		page.Runs = append(page.Runs, pick.buf[0])
		if err := pick.take(ctx, limit); err != nil {
			return workflow.RunPage{}, fmt.Errorf("cutover: %s: %w", pick.side, err)
		}
	}
	if feeds[0].more() || feeds[1].more() {
		page.Next = encodeCursor(runsCursor{O: feeds[0].pos, N: feeds[1].pos})
	}
	return page, nil
}

// feedPos is where one backend's feed stands: the inner cursor of the page
// being read and how many of its runs were already handed out. Done is set
// once the backend has nothing more.
type feedPos struct {
	Cursor string `json:"c,omitempty"`
	Skip   int    `json:"k,omitempty"`
	Done   bool   `json:"d,omitempty"`
}

type runsCursor struct {
	O feedPos `json:"o"`
	N feedPos `json:"n"`
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
	if err != nil || c.O.Skip < 0 || c.N.Skip < 0 {
		return runsCursor{}, fmt.Errorf("%w: cutover: Runs cursor was not made by this client", workflow.ErrInvalid)
	}
	return c, nil
}

// feed is one backend's runs, read page by page.
type feed struct {
	side Side
	q    workflow.RunQuery
	src  workflow.RunQuerier
	pos  feedPos
	buf  []workflow.RunInfo // unread runs of the current page, refs prefixed
	next string             // the inner cursor after the current page
}

// load reads the page at pos and drops the runs already handed out,
// moving on while the current page has nothing left.
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

func (fd *feed) advance() {
	if fd.next == "" {
		fd.pos = feedPos{Done: true}
		return
	}
	fd.pos = feedPos{Cursor: fd.next}
}

// take consumes the head run and refills from the next inner page when the
// current one is used up.
func (fd *feed) take(ctx context.Context, limit int) error {
	fd.buf = fd.buf[1:]
	fd.pos.Skip++
	if len(fd.buf) > 0 {
		return nil
	}
	fd.advance()
	return fd.load(ctx, limit)
}

func (fd *feed) more() bool { return len(fd.buf) > 0 || !fd.pos.Done }

// newer orders the merged feed: later enqueue first, then RunID descending.
func newer(a, b workflow.RunInfo) bool {
	if d := a.EnqueuedAt.Compare(b.EnqueuedAt); d != 0 {
		return d > 0
	}
	return a.Ref.RunID > b.Ref.RunID
}

// Retry re-runs a failed run on the backend its ref names (a run on the
// old backend is retried there) and returns its ref with that backend's
// prefix. Needs CapRetryRun on both.
func (c *Client) Retry(ctx context.Context, ref workflow.RunRef) (workflow.RunRef, error) {
	if err := c.need("Retry", workflow.CapRetryRun); err != nil {
		return workflow.RunRef{}, err
	}
	var out workflow.RunRef
	err := c.onRef(ref, func(s Side, b workflow.Client, inner workflow.RunRef) error {
		r, ok := b.(workflow.RunRetrier)
		if !ok {
			return &workflow.UnsupportedError{Field: "Retry", Missing: workflow.CapRetryRun, Reason: "the " + string(s) + " backend does not implement it"}
		}
		got, err := r.Retry(ctx, inner)
		if err == nil {
			out = encodeRef(s, got)
		}
		return err
	})
	return out, err
}

// Delete forgets a terminal run on the backend its ref names. Needs
// CapDeleteRun on both.
func (c *Client) Delete(ctx context.Context, ref workflow.RunRef) error {
	if err := c.need("Delete", workflow.CapDeleteRun); err != nil {
		return err
	}
	return c.onRef(ref, func(s Side, b workflow.Client, inner workflow.RunRef) error {
		d, ok := b.(workflow.RunDeleter)
		if !ok {
			return &workflow.UnsupportedError{Field: "Delete", Missing: workflow.CapDeleteRun, Reason: "the " + string(s) + " backend does not implement it"}
		}
		return d.Delete(ctx, inner)
	})
}

// CancelAll cancels the matching runs on both backends and returns the
// total. When one fails, the other's cancellations stand and the error
// names the failing one. Needs CapCancelAll on both.
func (c *Client) CancelAll(ctx context.Context, q workflow.RunQuery) (int, error) {
	oc, nc, err := both[workflow.BulkCanceller](c, "CancelAll", workflow.CapCancelAll)
	if err != nil {
		return 0, err
	}
	if err := workflow.ValidateCancelQuery(q); err != nil {
		return 0, err
	}
	if err := workflow.CheckRunQuery(c.Capabilities(), q); err != nil {
		return 0, err
	}
	var on, nn int
	var oerr, nerr error
	if sq, ok := sideQuery(q, SideOld); ok {
		on, oerr = oc.CancelAll(ctx, sq)
	}
	if sq, ok := sideQuery(q, SideNew); ok {
		nn, nerr = nc.CancelAll(ctx, sq)
	}
	return on + nn, joinSides("CancelAll", oerr, nerr)
}

// Pause holds matching runs on both backends. Needs the capability t needs
// on both.
func (c *Client) Pause(ctx context.Context, t workflow.PauseTarget) error {
	op, np, err := c.pausers(t)
	if err != nil {
		return err
	}
	return joinSides("Pause", op.Pause(ctx, t), np.Pause(ctx, t))
}

// Resume removes t on both backends.
func (c *Client) Resume(ctx context.Context, t workflow.PauseTarget) error {
	op, np, err := c.pausers(t)
	if err != nil {
		return err
	}
	return joinSides("Resume", op.Resume(ctx, t), np.Resume(ctx, t))
}

func (c *Client) pausers(t workflow.PauseTarget) (o, n workflow.Pauser, err error) {
	if err := workflow.CheckPause(c.Capabilities(), t); err != nil {
		return nil, nil, err
	}
	return both[workflow.Pauser](c, "Pause", workflow.RequiresPause(t))
}

// Paused returns the targets paused on either backend, without repeats:
// all first, then workflows, then queues.
func (c *Client) Paused(ctx context.Context) ([]workflow.PauseTarget, error) {
	op, np, err := both[workflow.Pauser](c, "Paused", workflow.CapPauseWorkflow)
	if err != nil {
		return nil, err
	}
	ot, oerr := op.Paused(ctx)
	nt, nerr := np.Paused(ctx)
	if err := joinSides("Paused", oerr, nerr); err != nil {
		return nil, err
	}
	set := map[workflow.PauseTarget]struct{}{}
	for _, t := range append(ot, nt...) {
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

// PauseSchedule pauses the schedule t names on the backend its ID names
// (an unprefixed ID on the old backend), or every schedule of every
// backend implementing SchedulePauser with t.All. Needs CapSchedulePause.
func (c *Client) PauseSchedule(ctx context.Context, t workflow.ScheduleTarget) error {
	return c.onSchedule(ctx, "PauseSchedule", t, func(ctx context.Context, sp workflow.SchedulePauser, t workflow.ScheduleTarget) error {
		return sp.PauseSchedule(ctx, t)
	})
}

// ResumeSchedule resumes what PauseSchedule paused.
func (c *Client) ResumeSchedule(ctx context.Context, t workflow.ScheduleTarget) error {
	return c.onSchedule(ctx, "ResumeSchedule", t, func(ctx context.Context, sp workflow.SchedulePauser, t workflow.ScheduleTarget) error {
		return sp.ResumeSchedule(ctx, t)
	})
}

func (c *Client) onSchedule(ctx context.Context, field string, t workflow.ScheduleTarget, fn func(context.Context, workflow.SchedulePauser, workflow.ScheduleTarget) error) error {
	if err := c.need(field, workflow.CapSchedulePause); err != nil {
		return err
	}
	if err := workflow.ValidateScheduleTarget(t); err != nil {
		return err
	}
	if t.All {
		errs := map[Side]error{}
		for _, s := range []Side{SideOld, SideNew} {
			if sp, ok := c.backend(s).(workflow.SchedulePauser); ok {
				errs[s] = fn(ctx, sp, t)
			}
		}
		return joinSides(field, errs[SideOld], errs[SideNew])
	}
	s, inner := splitID(string(t.ID))
	if s == "" {
		s = SideOld
	}
	sp, ok := c.backend(s).(workflow.SchedulePauser)
	if !ok {
		return &workflow.UnsupportedError{Field: field, Missing: workflow.CapSchedulePause, Reason: "the " + string(s) + " backend does not implement it"}
	}
	return fn(ctx, sp, workflow.ScheduleTarget{ID: workflow.ScheduleID(inner)})
}

// Schedules lists the schedules of both backends that implement
// ScheduleLister, each ID prefixed by its backend, sorted by Workflow,
// then Name, old first: the old backend's are the ones still to move
// (MoveSchedules). Needs CapScheduleList (the schedule backend's).
func (c *Client) Schedules(ctx context.Context) ([]workflow.ScheduleInfo, error) {
	if err := c.need("Schedules", workflow.CapScheduleList); err != nil {
		return nil, err
	}
	var out []workflow.ScheduleInfo
	for _, s := range []Side{SideOld, SideNew} {
		l, ok := c.backend(s).(workflow.ScheduleLister)
		if !ok {
			continue
		}
		infos, err := l.Schedules(ctx)
		if err != nil {
			return nil, fmt.Errorf("cutover: %s: %w", s, err)
		}
		for _, info := range infos {
			info.ID = encodeScheduleID(s, info.ID)
			if !info.Last.Run.IsZero() {
				info.Last.Run = encodeRef(s, info.Last.Run)
			}
			out = append(out, info)
		}
	}
	slices.SortStableFunc(out, func(a, b workflow.ScheduleInfo) int {
		return cmp.Or(cmp.Compare(a.Workflow, b.Workflow), cmp.Compare(a.Name, b.Name))
	})
	return out, nil
}

// Drain drains both backends at once and returns when both have stopped
// or ctx ends. Needs CapDrain on both.
func (c *Client) Drain(ctx context.Context) error {
	od, nd, err := both[workflow.Drainer](c, "Drain", workflow.CapDrain)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	var oerr, nerr error
	wg.Add(2)
	go func() { defer wg.Done(); oerr = od.Drain(ctx) }()
	go func() { defer wg.Done(); nerr = nd.Drain(ctx) }()
	wg.Wait()
	return joinSides("Drain", oerr, nerr)
}

// Done is closed once both backends have stopped. Without CapDrain on
// both it returns a nil channel, which never closes.
func (c *Client) Done() <-chan struct{} {
	od, nd, err := both[workflow.Drainer](c, "Done", workflow.CapDrain)
	if err != nil {
		return nil
	}
	c.doneOnce.Do(func() {
		c.done = make(chan struct{})
		go func() {
			<-od.Done()
			<-nd.Done()
			close(c.done)
		}()
	})
	return c.done
}

// Nodes lists the nodes of both backends, old first. A node ID is the
// backend's own; the same process appears twice when it runs workers for
// both. Both must implement workflow.NodeLister.
func (c *Client) Nodes(ctx context.Context) ([]workflow.NodeInfo, error) {
	on, ook := c.old.(workflow.NodeLister)
	nn, nok := c.new.(workflow.NodeLister)
	if !ook || !nok {
		return nil, workflow.Unsupported("Nodes", "both cutover backends must implement workflow.NodeLister")
	}
	o, oerr := on.Nodes(ctx)
	n, nerr := nn.Nodes(ctx)
	return append(o, n...), joinSides("Nodes", oerr, nerr)
}

// Health is live while both backends are live, since the old one still
// runs work, and ready while the new one is ready, since every Enqueue
// goes there. Each check is named after its backend.
func (c *Client) Health(ctx context.Context) workflow.HealthReport {
	o, n := workflow.CheckHealth(ctx, c.old), workflow.CheckHealth(ctx, c.new)
	out := workflow.HealthReport{Live: o.Live && n.Live, Ready: n.Ready}
	for _, s := range []struct {
		side Side
		rep  workflow.HealthReport
	}{{SideOld, o}, {SideNew, n}} {
		for _, ch := range s.rep.Checks {
			ch.Name = string(s.side) + " " + ch.Name
			out.Checks = append(out.Checks, ch)
		}
	}
	return out
}

// Reschedule moves a delayed run on the backend its ref names and returns
// its ref with that backend's prefix.
func (c *Client) Reschedule(ctx context.Context, ref workflow.RunRef, at time.Time) (workflow.RunRef, error) {
	var out workflow.RunRef
	err := c.onRef(ref, func(s Side, b workflow.Client, inner workflow.RunRef) error {
		r, ok := b.(workflow.RunRescheduler)
		if !ok {
			return workflow.Unsupported("Reschedule", "the "+string(s)+" backend does not implement it")
		}
		got, err := r.Reschedule(ctx, inner, at)
		if err == nil {
			out = encodeRef(s, got)
		}
		return err
	})
	return out, err
}

// CancelStep withdraws a step of the run on the backend its ref names.
func (c *Client) CancelStep(ctx context.Context, ref workflow.RunRef, step string) error {
	return c.onRef(ref, func(s Side, b workflow.Client, inner workflow.RunRef) error {
		sc, ok := b.(workflow.StepCanceller)
		if !ok {
			return workflow.Unsupported("CancelStep", "the "+string(s)+" backend does not implement it")
		}
		return sc.CancelStep(ctx, inner, step)
	})
}

// EditRun edits a scheduled run on the backend its ref names and returns
// its ref with that backend's prefix.
func (c *Client) EditRun(ctx context.Context, ref workflow.RunRef, edit workflow.JobEdit) (workflow.RunRef, error) {
	var out workflow.RunRef
	err := c.onRef(ref, func(s Side, b workflow.Client, inner workflow.RunRef) error {
		e, ok := b.(workflow.RunEditor)
		if !ok {
			return workflow.Unsupported("EditRun", "the "+string(s)+" backend does not implement it")
		}
		got, err := e.EditRun(ctx, inner, edit)
		if err == nil {
			out = encodeRef(s, got)
		}
		return err
	})
	return out, err
}

// ResetRun re-runs a failed run from step on the backend its ref names (a
// run on the old backend is reset there) and returns its ref with that
// backend's prefix.
func (c *Client) ResetRun(ctx context.Context, ref workflow.RunRef, step string) (workflow.RunRef, error) {
	var out workflow.RunRef
	err := c.onRef(ref, func(s Side, b workflow.Client, inner workflow.RunRef) error {
		r, ok := b.(workflow.RunResetter)
		if !ok {
			return workflow.Unsupported("ResetRun", "the "+string(s)+" backend does not implement it")
		}
		got, err := r.ResetRun(ctx, inner, step)
		if err == nil {
			out = encodeRef(s, got)
		}
		return err
	})
	return out, err
}

// SignalWaiting delivers ev to the runs waiting for it now on both
// backends, since a waiting run may sit on either, and returns how many
// it resumed in all. Both must implement workflow.TransientSignaller;
// when one does not, nothing is delivered. When one fails, the other's
// deliveries stand and the error names the failing one.
func (c *Client) SignalWaiting(ctx context.Context, ev workflow.Event) (int, error) {
	ot, ook := c.old.(workflow.TransientSignaller)
	nt, nok := c.new.(workflow.TransientSignaller)
	if !ook || !nok {
		return 0, workflow.Unsupported("SignalWaiting", "both cutover backends must implement workflow.TransientSignaller")
	}
	on, oerr := ot.SignalWaiting(ctx, ev)
	nn, nerr := nt.SignalWaiting(ctx, ev)
	return on + nn, joinSides("SignalWaiting", oerr, nerr)
}

// Supports reports an operation this client implements as done only when
// both backends do it, since a run, or a node, may sit on either; the
// one it does not implement (OnLifecycle) is false.
func (c *Client) Supports(op workflow.Op) bool {
	switch op {
	case workflow.OpReschedule, workflow.OpCancelStep, workflow.OpEditRun, workflow.OpResetRun, workflow.OpResetSucceeded, workflow.OpForkRun,
		workflow.OpSignalWaiting, workflow.OpNodes:
		return workflow.Supports(c.old, op) && workflow.Supports(c.new, op)
	}
	return false
}
