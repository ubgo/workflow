package regional

import (
	"context"

	"github.com/ubgo/workflow"
)

var (
	_ workflow.RunResetterWith = (*Client)(nil)
	_ workflow.RunForker       = (*Client)(nil)
)

// ResetRunWith resets a run (a succeeded one too, with opts.Succeeded) in
// the region its ref names and returns the ref prefixed the same way.
func (c *Client) ResetRunWith(ctx context.Context, ref workflow.RunRef, step string, opts workflow.ResetOptions) (workflow.RunRef, error) {
	var out workflow.RunRef
	err := onRegion(c, "ResetRunWith", workflow.Caps{}, ref, func(name string, r workflow.RunResetterWith, inner workflow.RunRef) error {
		got, err := r.ResetRunWith(ctx, inner, step, opts)
		out = encodeRef(name, got)
		return err
	})
	if err != nil {
		return workflow.RunRef{}, err
	}
	return out, nil
}

// ForkRun forks a finished run in the region its ref names (the fork
// stays in that region, with the original's tenant and labels, so the
// Router is not asked again) and returns the fork's ref with the region's
// prefix.
func (c *Client) ForkRun(ctx context.Context, ref workflow.RunRef, step string, opts workflow.ForkOptions) (workflow.RunRef, error) {
	var out workflow.RunRef
	err := onRegion(c, "ForkRun", workflow.Caps{}, ref, func(name string, r workflow.RunForker, inner workflow.RunRef) error {
		got, err := r.ForkRun(ctx, inner, step, opts)
		out = encodeRef(name, got)
		return err
	})
	if err != nil {
		return workflow.RunRef{}, err
	}
	return out, nil
}
