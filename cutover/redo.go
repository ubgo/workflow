package cutover

import (
	"context"

	"github.com/ubgo/workflow"
)

var (
	_ workflow.RunResetterWith = (*Client)(nil)
	_ workflow.RunForker       = (*Client)(nil)
)

// ResetRunWith resets a run (a succeeded one too, with opts.Succeeded) on
// the backend its ref names (a run on the old backend is reset there) and
// returns its ref with that backend's prefix.
func (c *Client) ResetRunWith(ctx context.Context, ref workflow.RunRef, step string, opts workflow.ResetOptions) (workflow.RunRef, error) {
	var out workflow.RunRef
	err := c.onRef(ref, func(s Side, b workflow.Client, inner workflow.RunRef) error {
		r, ok := b.(workflow.RunResetterWith)
		if !ok {
			return workflow.Unsupported("ResetRunWith", "the "+string(s)+" backend does not implement it")
		}
		got, err := r.ResetRunWith(ctx, inner, step, opts)
		if err == nil {
			out = encodeRef(s, got)
		}
		return err
	})
	return out, err
}

// ForkRun forks a finished run on the backend its ref names: a fork
// needs the original's step outputs, which only that backend holds, so a
// fork of a run on the old backend is a new run there too. It returns the
// fork's ref with that backend's prefix.
func (c *Client) ForkRun(ctx context.Context, ref workflow.RunRef, step string, opts workflow.ForkOptions) (workflow.RunRef, error) {
	var out workflow.RunRef
	err := c.onRef(ref, func(s Side, b workflow.Client, inner workflow.RunRef) error {
		r, ok := b.(workflow.RunForker)
		if !ok {
			return workflow.Unsupported("ForkRun", "the "+string(s)+" backend does not implement it")
		}
		got, err := r.ForkRun(ctx, inner, step, opts)
		if err == nil {
			out = encodeRef(s, got)
		}
		return err
	})
	return out, err
}
