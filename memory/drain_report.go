package memory

import (
	"context"
	"fmt"

	"github.com/ubgo/workflow"
)

// DrainWithReport is Drain that also returns what the drain finished and
// what it cut off (workflow.ReportingDrainer). Every cut-off attempt has
// the disposition DrainCancelled: nothing in memory outlives the client,
// so its run becomes StatusCancelled. The report is fixed when the
// deadline ends, before the client's context is cancelled, so attempts
// the shutdown itself interrupts are listed as cut and never as finished.
// A second call, or one after the client stopped, returns the first
// call's report.
func (c *Client) DrainWithReport(ctx context.Context) (workflow.DrainReport, error) {
	c.mu.Lock()
	if !c.started {
		c.mu.Unlock()
		return workflow.DrainReport{}, fmt.Errorf("%w: Drain before Start", workflow.ErrNotStarted)
	}
	c.drainTrack.StartDrain()
	if c.drained == nil {
		c.drained = make(chan struct{})
		c.draining = true
		if c.settledLocked() || c.stopped {
			closeOnce(c.drained)
		}
	}
	drained := c.drained
	c.mu.Unlock()
	var ctxErr error
	select {
	case <-drained:
	case <-ctx.Done():
		ctxErr = ctx.Err()
	}
	c.drainTrack.Freeze(workflow.DrainCancelled)
	c.Stop()
	report := c.drainTrack.Seal()
	return report, workflow.DrainOutcome(report, ctxErr)
}
