package memory

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/ubgo/workflow"
)

// infoLocked is r as a RunQuerier reports it.
func (r *run) infoLocked() workflow.RunInfo {
	info := workflow.RunInfo{
		Ref:        r.ref(),
		Workflow:   r.reg.wf.Name,
		Queue:      r.queue,
		Status:     r.statusLocked(),
		FailedStep: r.failedStep,
		Labels:     r.job.Labels,
		Payload:    r.job.Payload,
		Priority:   r.job.Priority,
		EnqueuedAt: r.enqueuedAt,
		StartedAt:  r.startedAt,
		FinishedAt: r.finishedAt,
		Attempts:   r.attempts,
		Errors:     slices.Clone(r.errs),

		Tenant:      r.job.Tenant,
		RunAt:       r.runAt,
		Output:      r.output,
		Progress:    r.progress,
		AttemptedBy: r.attemptedBy,
	}
	if r.err != nil && r.status != workflow.StatusSucceeded {
		info.Error = r.err.Error()
	}
	if info.Status == workflow.StatusRetrying {
		for _, w := range r.retrying {
			if info.NextAttemptAt.IsZero() || w.at.Before(info.NextAttemptAt) {
				info.NextAttempt, info.NextAttemptAt = w.attempt, w.at
				if w.err != nil {
					info.Error = w.err.Error()
				}
			}
		}
	}
	return info.Clone()
}

// allRunsLocked is every run the client knows, delayed ones included,
// once each.
func (c *Client) allRunsLocked() []*run {
	out := make([]*run, 0, len(c.runs)+len(c.delayed))
	for _, r := range c.runs {
		out = append(out, r)
	}
	for _, r := range c.delayed {
		if r.id.IsZero() { // a fired delayed run is in c.runs too
			out = append(out, r)
		}
	}
	return out
}

// newestFirst orders runs by enqueue time, then by enqueue sequence, both
// descending: the order Runs pages through.
func newestFirst(a, b *run) int {
	return cmp.Or(b.enqueuedAt.Compare(a.enqueuedAt), cmp.Compare(b.seq, a.seq))
}

// cursor is the position after one run in newestFirst order, encoded as
// "<enqueue unix nanos>.<seq>".
func cursorOf(r *run) string {
	return strconv.FormatInt(r.enqueuedAt.UnixNano(), 10) + "." + strconv.FormatUint(r.seq, 10)
}

func parseCursor(s string) (nanos int64, seq uint64, err error) {
	a, b, ok := strings.Cut(s, ".")
	if ok {
		nanos, err = strconv.ParseInt(a, 10, 64)
		if err == nil {
			seq, err = strconv.ParseUint(b, 10, 64)
		}
	}
	if !ok || err != nil {
		return 0, 0, fmt.Errorf("%w: run query cursor %q was not issued by this driver", workflow.ErrInvalid, s)
	}
	return nanos, seq, nil
}

// Runs lists runs matching q, newest enqueue first. The cursor is a
// position, not an offset, so runs enqueued between pages do not shift
// the next page.
func (c *Client) Runs(ctx context.Context, q workflow.RunQuery) (workflow.RunPage, error) {
	if err := ctx.Err(); err != nil {
		return workflow.RunPage{}, err
	}
	if err := workflow.ValidateRunQuery(q); err != nil {
		return workflow.RunPage{}, err
	}
	if err := workflow.CheckRunQuery(caps, q); err != nil {
		return workflow.RunPage{}, err
	}
	var afterNanos int64
	var afterSeq uint64
	if q.Cursor != "" {
		var err error
		if afterNanos, afterSeq, err = parseCursor(q.Cursor); err != nil {
			return workflow.RunPage{}, err
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneLocked()
	runs := c.allRunsLocked()
	slices.SortFunc(runs, newestFirst)
	limit := q.PageLimit()
	var page workflow.RunPage
	var lastOnPage *run
	for _, r := range runs {
		if q.Cursor != "" {
			n := r.enqueuedAt.UnixNano()
			if n > afterNanos || (n == afterNanos && r.seq >= afterSeq) {
				continue // at or before the cursor
			}
		}
		info := r.infoLocked()
		if !q.Match(info) {
			continue
		}
		if len(page.Runs) == limit { // a further match exists: there is a next page
			page.Next = cursorOf(lastOnPage)
			break
		}
		page.Runs = append(page.Runs, info)
		lastOnPage = r
	}
	return page, nil
}

// Retry re-runs a failed run from the step that failed; see
// workflow.RunRetrier. The run keeps its RunRef. Its concurrency and rate
// limits apply again, as for a new run.
func (c *Client) Retry(ctx context.Context, ref workflow.RunRef) (workflow.RunRef, error) {
	if err := ctx.Err(); err != nil {
		return workflow.RunRef{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.usableLocked(); err != nil {
		return workflow.RunRef{}, err
	}
	old, err := c.lookupLocked(ref)
	if err != nil {
		return workflow.RunRef{}, err
	}
	if old.status != workflow.StatusFailed {
		return workflow.RunRef{}, fmt.Errorf("%w: run %s is %s; only a failed run can be retried", workflow.ErrWrongState, old.id, old.status)
	}
	if old.idem != nil {
		if holder, held := c.idem[*old.idem]; held && holder != old {
			return workflow.RunRef{}, fmt.Errorf("%w: run %s holds the key %q", workflow.ErrDuplicate, holder.id, old.idem.key)
		}
	}
	c.forgetFinishedLocked(old)
	r := c.newRunLocked(old.reg, old.job)
	r.id, r.sid, r.seq = old.id, old.sid, old.seq // the same run, seen by signals as enqueued when it was
	r.enqueuedAt, r.startedAt, r.attempts = old.enqueuedAt, old.startedAt, old.attempts
	r.outputs = maps.Clone(old.outputs)
	r.consumed = maps.Clone(old.consumed)
	r.chain = old.chain // edits of the steps that succeeded before the failure
	r.errs = old.errs
	if old.idem != nil {
		k := *old.idem
		r.idem = &k
		c.idem[k] = r
	}
	if !r.sid.IsZero() {
		c.delayed[r.sid] = r
	}
	c.readyLocked(r) // stores it under its RunID, replacing the failed one
	return r.ref(), nil
}

// Delete forgets a terminal run; see workflow.RunDeleter.
func (c *Client) Delete(ctx context.Context, ref workflow.RunRef) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	r, err := c.lookupLocked(ref)
	if err != nil {
		return err
	}
	if !r.status.IsTerminal() {
		return fmt.Errorf("%w: run %s is %s; only a finished run can be deleted", workflow.ErrWrongState, r.id, r.status)
	}
	c.dropLocked(r)
	c.forgetFinishedLocked(r)
	return nil
}

// dropLocked removes r from the lookup maps, where they still point at
// it (a retried run's ID points at its successor).
func (c *Client) dropLocked(r *run) {
	if !r.id.IsZero() && c.runs[r.id] == r {
		delete(c.runs, r.id)
	}
	if !r.sid.IsZero() && c.delayed[r.sid] == r {
		delete(c.delayed, r.sid)
	}
}

// forgetFinishedLocked takes r out of the retention list.
func (c *Client) forgetFinishedLocked(r *run) {
	c.finished = slices.DeleteFunc(c.finished, func(f *run) bool { return f == r })
}

// pruneLocked forgets the oldest finished runs while they are past
// RunRetention.MaxAge or there are more than MaxRuns of them. finished is
// in finish order, so the oldest are at the front and the scan stops at
// the first run both limits keep.
func (c *Client) pruneLocked() {
	ret := c.retention
	if ret.MaxAge == 0 && ret.MaxRuns == 0 {
		return
	}
	now := c.clk.Now()
	n := 0
	for n < len(c.finished) {
		r := c.finished[n]
		overCount := ret.MaxRuns > 0 && len(c.finished)-n > ret.MaxRuns
		overAge := ret.MaxAge > 0 && now.Sub(r.finishedAt) > ret.MaxAge
		if !overCount && !overAge {
			break
		}
		c.dropLocked(r)
		n++
	}
	if n > 0 {
		clear(c.finished[:n])
		c.finished = c.finished[n:]
	}
}

// CancelAll cancels every run matching q; see workflow.BulkCanceller.
func (c *Client) CancelAll(ctx context.Context, q workflow.RunQuery) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := workflow.ValidateCancelQuery(q); err != nil {
		return 0, err
	}
	if err := workflow.CheckRunQuery(caps, q); err != nil {
		return 0, err
	}
	q.Statuses = workflow.CancelStatuses(q)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.usableLocked(); err != nil {
		return 0, err
	}
	n := 0
	for _, r := range c.allRunsLocked() {
		if r.status.IsTerminal() || !q.Match(r.infoLocked()) {
			continue
		}
		c.cancelLocked(r, fmt.Errorf("%w: CancelAll", workflow.ErrCancelled))
		n++
	}
	c.admitLocked()
	return n, nil
}

// Pause holds the runs t targets; see workflow.Pauser.
func (c *Client) Pause(ctx context.Context, t workflow.PauseTarget) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := workflow.CheckPause(caps, t); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pauses[t] = true
	return nil
}

// Resume lifts a pause; Resume(PauseAll()) lifts every pause.
func (c *Client) Resume(ctx context.Context, t workflow.PauseTarget) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := workflow.CheckPause(caps, t); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if t.All {
		clear(c.pauses)
	} else {
		delete(c.pauses, t)
	}
	c.taskCond.Broadcast()
	return nil
}

// Paused lists the pauses in force: All first, then workflows, then
// queues, each by name.
func (c *Client) Paused(ctx context.Context) ([]workflow.PauseTarget, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out := slices.Collect(maps.Keys(c.pauses))
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
		return cmp.Or(cmp.Compare(rank(a), rank(b)), cmp.Compare(a.Workflow+a.Queue, b.Workflow+b.Queue))
	})
	return out, nil
}

// Drain stops claiming step attempts, waits for the running ones until
// ctx ends, then shuts the client down; see workflow.Drainer. Unfinished
// runs become StatusCancelled, since nothing in memory outlives the
// client. It is DrainWithReport without the report.
func (c *Client) Drain(ctx context.Context) error {
	_, err := c.DrainWithReport(ctx)
	return err
}

// Done is closed once the client has fully stopped: after Drain, after
// the Start ctx was cancelled, or after WithStopWhenIdle fired.
func (c *Client) Done() <-chan struct{} { return c.done }

// checkIdleLocked arms the stop-when-idle timer when the client is
// started, idle and configured for it. The timer acts only if no run was
// enqueued since it was armed (idleEpoch) and the client is still idle.
func (c *Client) checkIdleLocked() {
	if !c.stopWhenIdle || !c.started || c.stopped || c.live > 0 {
		return
	}
	epoch := c.idleEpoch
	t := c.clk.NewTimer(c.idleGrace)
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		select {
		case <-t.C():
		case <-c.ctx.Done():
			t.Stop()
			return
		}
		c.mu.Lock()
		idle := c.idleEpoch == epoch && c.live == 0 && !c.stopped
		c.mu.Unlock()
		if idle {
			go func() { _ = c.Drain(context.Background()) }() // not in wg: Drain waits for wg
		}
	}()
}
