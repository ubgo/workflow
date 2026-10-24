package sqlstore

import (
	"context"
	"fmt"

	"github.com/ubgo/workflow"
)

// Drain stops the client gracefully (workflow.Drainer), as before a
// deploy: workers stop claiming at once, Drain waits until every
// attempt this client claimed has run and its outcome is written, or until
// ctx ends, then shuts the client down as cancelling the Start ctx would.
// An attempt cut off by ctx is handed back as ready and not charged an
// attempt (CapShutdownRequeue), so another client runs it at once; runs
// stay in the database. It returns nil when every attempt finished and
// ctx's error otherwise. Calling it again, or after the Start ctx ended,
// waits for the same shutdown; before Start it returns ErrNotStarted. It is
// DrainWithReport without the report.
func (c *Client) Drain(ctx context.Context) error {
	_, err := c.DrainWithReport(ctx)
	return err
}

// DrainWithReport is Drain that also returns what the drain finished and
// cut off (workflow.ReportingDrainer). Every cut-off attempt has the
// disposition DrainRequeued. The report is fixed when the deadline ends,
// before the client's context is cancelled. A second call returns the first
// call's report.
func (c *Client) DrainWithReport(ctx context.Context) (workflow.DrainReport, error) {
	c.mu.Lock()
	if !c.started || c.cancel == nil {
		c.mu.Unlock()
		return workflow.DrainReport{}, fmt.Errorf("%w: Drain before Start", workflow.ErrNotStarted)
	}
	c.drainTrack.StartDrain()
	c.draining.Store(true)
	var wait chan struct{}
	if c.inflight > 0 {
		if c.drained == nil {
			c.drained = make(chan struct{})
		}
		wait = c.drained
	}
	cancel := c.cancel
	c.mu.Unlock()

	var ctxErr error
	if wait != nil {
		select {
		case <-wait:
		case <-ctx.Done():
			ctxErr = ctx.Err()
		}
	}
	c.drainTrack.Freeze(workflow.DrainRequeued)
	cancel()
	c.shutdown()
	<-c.done
	report := c.drainTrack.Seal()
	return report, workflow.DrainOutcome(report, ctxErr)
}

// Done is closed once the client has fully stopped: after Drain, after the
// Start ctx was cancelled or Stop was called, or after WithStopWhenIdle
// fired. Before Start it is open.
func (c *Client) Done() <-chan struct{} { return c.done }

// beginAttempt and endAttempt bracket one claim and, when it lands, the
// attempt up to its written outcome, so Drain knows when the last one is
// done. beginAttempt refuses once Drain began: the check and the count
// share c.mu with Drain, so no claim slips in after Drain read the count.
func (c *Client) beginAttempt() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.draining.Load() {
		return false
	}
	c.inflight++
	return true
}

func (c *Client) endAttempt() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inflight--
	if c.inflight == 0 && c.drained != nil {
		close(c.drained)
		c.drained = nil
	}
}

// checkIdle is WithStopWhenIdle's test, run on every dispatcher pass: once
// no unfinished run of this client's workflows (on its served queues, when
// WithQueues is set) has existed for idleGrace on the scheduling clock, it
// drains the client. It returns when the grace ends (Unix nanoseconds; 0:
// not idle, or already draining), so the dispatcher wakes for it.
func (c *Client) checkIdle(ctx context.Context) (int64, error) {
	if c.idleGrace <= 0 || c.draining.Load() {
		return 0, nil
	}
	q := "SELECT COUNT(*) FROM {runs} r WHERE r." + c.inNames() + " AND r.status IN (?, ?, ?, ?)"
	args := c.nameArgs(runScheduled, runPending, runActive, runFailing)
	if len(c.served) > 0 {
		q += " AND COALESCE((SELECT x.queue FROM {run_ext} x WHERE x.run_id = r.id), '" + workflow.DefaultQueue + "') IN (" + inList(len(c.served)) + ")"
		args = append(args, c.served...)
	}
	var n int64
	if err := c.st.one(ctx, c.st.db, q, args, &n); err != nil {
		return 0, err
	}
	now := c.clk.Now()
	if n > 0 {
		c.idleSince = 0
		return 0, nil
	}
	if c.idleSince == 0 {
		c.idleSince = now.UnixNano()
	}
	deadline := c.idleSince + int64(c.idleGrace)
	if now.UnixNano() < deadline {
		return deadline, nil
	}
	c.draining.Store(true)
	// Drain waits for the dispatcher to exit, so it runs on its own
	// goroutine; Done reports the end.
	go func() {
		if err := c.Drain(context.WithoutCancel(ctx)); err != nil {
			c.log.Error("sqlstore: stop when idle", "error", err)
		}
	}()
	return 0, nil
}
