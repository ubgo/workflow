package failover

import (
	"context"

	"github.com/ubgo/workflow"
)

var (
	_ workflow.RunResetterWith = (*Client)(nil)
	_ workflow.RunForker       = (*Client)(nil)
)

// ResetRunWith resets a run (a succeeded one too, with opts.Succeeded) on
// the backend its ref names, and prefixes the ref it returns.
func (f *Client) ResetRunWith(ctx context.Context, ref workflow.RunRef, step string, opts workflow.ResetOptions) (workflow.RunRef, error) {
	var out workflow.RunRef
	err := f.onSide(ref, func(sd side, c workflow.Client, inner workflow.RunRef) error {
		r, ok := c.(workflow.RunResetterWith)
		if !ok {
			return workflow.Unsupported("ResetRunWith", "the "+sd.String()+" backend does not implement it")
		}
		got, err := r.ResetRunWith(ctx, inner, step, opts)
		out = encodeRef(sd, got)
		return err
	})
	if err != nil {
		return workflow.RunRef{}, err
	}
	return out, nil
}

// ForkRun forks a finished run on the backend its ref names (the fork
// lives beside its original) and prefixes the fork's ref. The fork's
// workflow.LabelForkOf holds the original's RunID as that backend knows
// it, without the prefix.
func (f *Client) ForkRun(ctx context.Context, ref workflow.RunRef, step string, opts workflow.ForkOptions) (workflow.RunRef, error) {
	var out workflow.RunRef
	err := f.onSide(ref, func(sd side, c workflow.Client, inner workflow.RunRef) error {
		r, ok := c.(workflow.RunForker)
		if !ok {
			return workflow.Unsupported("ForkRun", "the "+sd.String()+" backend does not implement it")
		}
		got, err := r.ForkRun(ctx, inner, step, opts)
		out = encodeRef(sd, got)
		return err
	})
	if err != nil {
		return workflow.RunRef{}, err
	}
	return out, nil
}
