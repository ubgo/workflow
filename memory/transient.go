package memory

import (
	"context"

	"github.com/ubgo/workflow"
)

var _ workflow.TransientSignaller = (*Client)(nil)

// SignalWaiting resumes the runs whose wait step is suspended on ev's name
// and key now, and keeps ev for no one else (workflow.TransientSignaller).
func (c *Client) SignalWaiting(ctx context.Context, ev workflow.Event) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.usableLocked(); err != nil {
		return 0, err
	}
	c.seq++
	seq := c.seq
	k := eventKey{name: ev.Name, key: ev.Key}
	n := 0
	for _, w := range c.waiters[k] {
		select {
		case w.ch <- ev.Payload:
			w.r.markConsumedLocked(seq)
			w.deliverLocked()
			n++
		default: // already delivered or timed out
		}
	}
	delete(c.waiters, k)
	return n, nil
}
