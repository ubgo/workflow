package memory

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ubgo/workflow"
)

// ErrRunAllStalled is returned by RunAll when runs are left that nothing
// it can hurry will move: a paused workflow or queue, a rate limit or
// timeout waiting on a fake clock, a concurrency slot held by a run that
// waits for an event.
var ErrRunAllStalled = errors.New("memory: RunAll stalled")

// runAllPoll is how often RunAll looks at the runs, and runAllStall how
// many looks without progress make it give up. These are real-time waits
// for the client's goroutines to catch up, not schedule logic, so they
// use the wall clock even under a fake clock.
const (
	runAllPoll  = time.Millisecond
	runAllStall = 500
)

// RunAllReport counts what became of the runs RunAll covered: the runs
// unfinished when it was called and the runs enqueued while it ran
// (chained jobs, children, cron fires).
type RunAllReport struct {
	Succeeded, Failed, Cancelled, Discarded int

	// Waiting counts runs left suspended on a wait step: an event they
	// need has not been signalled, which RunAll cannot do for them.
	Waiting int
}

// RunAll runs everything now, for a test: delayed starts begin at once,
// retry backoffs, snoozes and sleep steps end at once, and it returns
// when no run it covers can move further (finished, or waiting for an
// event). Timeouts are not hurried: a step timeout, wait deadline,
// heartbeat or RunTimeout still follows the client's clock. It needs
// Start. When runs are left that it cannot move (paused, rate-limited on
// a fake clock), it returns the report with an error wrapping
// ErrRunAllStalled; ctx ending returns ctx's error.
func (c *Client) RunAll(ctx context.Context) (RunAllReport, error) {
	c.mu.Lock()
	if err := c.usableLocked(); err != nil {
		c.mu.Unlock()
		return RunAllReport{}, err
	}
	startSeq := c.seq
	covered := map[*run]bool{}
	for _, r := range c.allRunsLocked() {
		if !r.status.IsTerminal() {
			covered[r] = true
		}
	}
	c.mu.Unlock()

	still, lastProgress := 0, -1
	for {
		c.mu.Lock()
		busy, progress := c.runAllStateLocked(covered, startSeq)
		close(c.hurry)
		c.hurry = make(chan struct{})
		var rep RunAllReport
		if !busy || still >= runAllStall {
			rep = c.runAllReportLocked(covered, startSeq)
		}
		c.mu.Unlock()
		switch {
		case !busy:
			return rep, nil
		case still >= runAllStall:
			return rep, fmt.Errorf("%w: runs are left that only the clock, a resume or a free slot can move", ErrRunAllStalled)
		}
		if progress != lastProgress {
			still, lastProgress = 0, progress
		} else {
			still++
		}
		t := time.NewTimer(runAllPoll)
		select {
		case <-ctx.Done():
			t.Stop()
			return RunAllReport{}, ctx.Err()
		case <-t.C:
		}
	}
}

// runAllStateLocked reports whether a covered run can still move, and a
// progress counter that changes whenever any run moves (an attempt made, a
// run finished). Callers hold c.mu.
func (c *Client) runAllStateLocked(covered map[*run]bool, startSeq uint64) (busy bool, progress int) {
	for _, r := range c.allRunsLocked() {
		if !covered[r] && r.seq <= startSeq {
			continue
		}
		progress += r.attempts
		if r.status.IsTerminal() {
			progress += 1 << 20
			continue
		}
		if st := r.statusLocked(); st != workflow.StatusWaiting || r.suspended > r.awaiting {
			busy = true
		}
	}
	return busy, progress
}

// runAllReportLocked counts the covered runs by outcome. Callers hold c.mu.
func (c *Client) runAllReportLocked(covered map[*run]bool, startSeq uint64) RunAllReport {
	var rep RunAllReport
	for _, r := range c.allRunsLocked() {
		if !covered[r] && r.seq <= startSeq {
			continue
		}
		switch r.statusLocked() {
		case workflow.StatusSucceeded:
			rep.Succeeded++
		case workflow.StatusFailed:
			rep.Failed++
		case workflow.StatusCancelled:
			rep.Cancelled++
		case workflow.StatusDiscarded:
			rep.Discarded++
		case workflow.StatusWaiting:
			rep.Waiting++
		}
	}
	return rep
}
