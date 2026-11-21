package memory

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strconv"

	"github.com/ubgo/workflow"
)

var _ workflow.RunSignalReader = (*Client)(nil)

// PendingSignals lists the signals addressed to run (workflow.RunKey)
// with one of names that the run may still take: kept, sent after its
// enqueue and not taken by it, oldest first (workflow.RunSignalReader).
// The ID is the signal's sequence number.
func (c *Client) PendingSignals(ctx context.Context, run workflow.RunID, names []string, limit int) ([]workflow.RunSignal, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.runs[run]
	if !ok || r.status.IsTerminal() {
		return nil, nil
	}
	type found struct {
		seq uint64
		sig workflow.RunSignal
	}
	var all []found
	for _, name := range names {
		for _, sg := range c.signals[eventKey{name: name, key: workflow.RunKey(run)}] {
			if sg.seq > r.seq && !r.consumed[sg.seq] {
				all = append(all, found{sg.seq, workflow.RunSignal{ID: strconv.FormatUint(sg.seq, 10), Name: name, Payload: sg.payload}})
			}
		}
	}
	slices.SortFunc(all, func(a, b found) int { return cmp.Compare(a.seq, b.seq) })
	if limit > 0 && len(all) > limit {
		all = all[:limit]
	}
	out := make([]workflow.RunSignal, len(all))
	for i, f := range all {
		out[i] = f.sig
	}
	return out, nil
}

// TakeSignal marks the signal id taken by run, as a wait step taking it
// would (workflow.RunSignalReader).
func (c *Client) TakeSignal(ctx context.Context, run workflow.RunID, id string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	seq, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		return false, fmt.Errorf("%w: signal %q", workflow.ErrNotFound, id)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.runs[run]
	if !ok || seq <= r.seq || !c.addressedLocked(run, seq) {
		return false, fmt.Errorf("%w: signal %q for run %s", workflow.ErrNotFound, id, run)
	}
	if r.consumed[seq] {
		return false, nil
	}
	r.markConsumedLocked(seq)
	return true, nil
}

// addressedLocked reports whether signal seq is kept under run's address.
func (c *Client) addressedLocked(run workflow.RunID, seq uint64) bool {
	key := workflow.RunKey(run)
	for k, sgs := range c.signals {
		if k.key != key {
			continue
		}
		if slices.ContainsFunc(sgs, func(sg signal) bool { return sg.seq == seq }) {
			return true
		}
	}
	return false
}
