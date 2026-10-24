package middleware

import (
	"context"

	"github.com/ubgo/workflow"
)

// Chain composes step middleware into one. The first argument is the
// outermost: Chain(a, b)(h) runs a, then b, then h, the order the list is
// read in. Nil entries are skipped, so a caller can build the list with
// optional pieces. An empty chain returns the handler unchanged.
func Chain(mws ...workflow.Middleware) workflow.Middleware {
	return func(next workflow.Handler) workflow.Handler {
		for i := len(mws) - 1; i >= 0; i-- {
			if mws[i] != nil {
				next = mws[i](next)
			}
		}
		return next
	}
}

// ChainEnqueue composes enqueue middleware with the same ordering rule as
// Chain: the first argument sees the job first.
func ChainEnqueue(mws ...workflow.EnqueueMiddleware) workflow.EnqueueMiddleware {
	return func(next workflow.EnqueueFunc) workflow.EnqueueFunc {
		for i := len(mws) - 1; i >= 0; i-- {
			if mws[i] != nil {
				next = mws[i](next)
			}
		}
		return next
	}
}

// WrapClient returns a Client whose Enqueue runs through mws before
// reaching c, so enqueue middleware works on any driver without the driver
// knowing. Every other method, Capabilities included, is c's own: the
// wrapper never claims or hides a capability. It also implements
// workflow.RunNotifier by delegating to c (see forwardRunFinished), so
// stacking wrappers never hides the run-finished hook from code such as
// UniqueUntilFinished. The optional interfaces of the run query, pause,
// schedule and drain families (workflow.RunQuerier, Pauser,
// ScheduleLister, TickNotifier, Drainer and the rest) are forwarded to c
// through workflow.Forward, and refused with an UnsupportedError when c
// lacks one, so a wrapper never hides them either.
func WrapClient(c workflow.Client, mws ...workflow.EnqueueMiddleware) workflow.Client {
	return &wrappedClient{Client: c, Forward: workflow.Forward{Inner: c}, enqueue: ChainEnqueue(mws...)(c.Enqueue)}
}

type wrappedClient struct {
	workflow.Client
	workflow.Forward
	enqueue workflow.EnqueueFunc
}

func (w *wrappedClient) Enqueue(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
	return w.enqueue(ctx, job)
}

func (w *wrappedClient) OnRunFinished(fn workflow.RunFinishedFunc) error {
	return forwardRunFinished(w.Client, fn)
}

// forwardRunFinished registers fn on c when c implements
// workflow.RunNotifier. A wrapper's static type always has OnRunFinished,
// so the honest answer for an inner client without the hook is an
// UnsupportedError naming CapRunFinished, never a silent no-op that would
// let a caller believe its observer is registered.
func forwardRunFinished(c workflow.Client, fn workflow.RunFinishedFunc) error {
	n, ok := c.(workflow.RunNotifier)
	if !ok {
		return &workflow.UnsupportedError{Field: "OnRunFinished", Missing: workflow.CapRunFinished, Reason: "the wrapped client does not implement workflow.RunNotifier"}
	}
	return n.OnRunFinished(fn)
}

var (
	_ workflow.RunNotifier    = (*wrappedClient)(nil)
	_ workflow.RunNotifier    = (*codecClient)(nil)
	_ workflow.RunQuerier     = (*wrappedClient)(nil)
	_ workflow.Pauser         = (*wrappedClient)(nil)
	_ workflow.ScheduleLister = (*wrappedClient)(nil)
	_ workflow.TickNotifier   = (*codecClient)(nil)
	_ workflow.Drainer        = (*codecClient)(nil)
)
