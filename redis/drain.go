package redis

import (
	"context"
	"fmt"

	"github.com/ubgo/workflow"
)

var _ workflow.ReportingDrainer = (*Client)(nil)

// Drain stops claiming runs, waits until the runs being executed finish
// or ctx ends, then stops the client as cancelling the Start ctx would.
// Runs still executing when ctx ends are handed back, so another client
// resumes them; the cut-off attempt is not charged an attempt
// (CapShutdownRequeue). See workflow.Drainer. It is DrainWithReport
// without the report.
func (c *Client) Drain(ctx context.Context) error {
	_, err := c.DrainWithReport(ctx)
	return err
}

// DrainWithReport is Drain that also returns what the drain finished and
// cut off (workflow.ReportingDrainer). A cut-off attempt is one whose
// handler was still running when ctx ended; its disposition is
// DrainRequeued. The report is fixed before the client's context is
// cancelled, and a second call returns the first call's report.
func (c *Client) DrainWithReport(ctx context.Context) (workflow.DrainReport, error) {
	c.mu.Lock()
	if !c.started {
		c.mu.Unlock()
		return workflow.DrainReport{}, fmt.Errorf("%w: Drain before Start", workflow.ErrNotStarted)
	}
	c.drainTrack.StartDrain()
	c.draining = true
	if c.idle == nil {
		c.idle = make(chan struct{})
		if c.inflight == 0 {
			close(c.idle)
		}
	}
	idle, cancel := c.idle, c.cancel
	c.mu.Unlock()
	c.wakeAll()
	var ctxErr error
	select {
	case <-idle:
	case <-ctx.Done():
		ctxErr = ctx.Err()
	case <-c.done:
	}
	c.drainTrack.Freeze(workflow.DrainRequeued)
	cancel()
	<-c.done
	report := c.drainTrack.Seal()
	return report, workflow.DrainOutcome(report, ctxErr)
}
