package middleware

import (
	"context"

	"github.com/ubgo/workflow"
)

// mapChain returns a copy of jobs with fn applied to every job, nested
// chains included, so enqueue middleware that rewrites a job (sealing its
// payload, claiming a big one) rewrites the jobs a driver enqueues later
// from Job.Chain too; without it they would reach their steps unsealed.
func mapChain(jobs []workflow.Job, fn func(workflow.Job) (workflow.Job, error)) ([]workflow.Job, error) {
	if len(jobs) == 0 {
		return jobs, nil
	}
	out := make([]workflow.Job, len(jobs))
	for i, j := range jobs {
		mapped, err := fn(j)
		if err != nil {
			return nil, err
		}
		if mapped.Chain, err = mapChain(j.Chain, fn); err != nil {
			return nil, err
		}
		out[i] = mapped
	}
	return out, nil
}

// mappingEditor rewrites the jobs a step adds to its chain with fn before
// handing them to the driver's editor, for the same reason as mapChain.
type mappingEditor struct {
	inner workflow.ChainEditor
	fn    func(workflow.Job) (workflow.Job, error)
}

func (m mappingEditor) Prepend(jobs ...workflow.Job) error {
	mapped, err := mapChain(jobs, m.fn)
	if err != nil {
		return err
	}
	return m.inner.Prepend(mapped...)
}

func (m mappingEditor) Append(jobs ...workflow.Job) error {
	mapped, err := mapChain(jobs, m.fn)
	if err != nil {
		return err
	}
	return m.inner.Append(mapped...)
}

// withMappedChain returns ctx with its ChainEditor, if any, wrapped by fn.
func withMappedChain(ctx context.Context, fn func(workflow.Job) (workflow.Job, error)) context.Context {
	ed, ok := workflow.ChainEditorFrom(ctx)
	if !ok || ed == nil {
		return ctx
	}
	return workflow.WithChainEditor(ctx, mappingEditor{inner: ed, fn: fn})
}
