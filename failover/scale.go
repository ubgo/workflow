package failover

import (
	"context"

	"github.com/ubgo/workflow"
)

// primaryOnlyCaps are the capabilities the failover client takes from the
// primary alone; see Capabilities.
const primaryOnlyCaps = workflow.CapClosure | workflow.CapWorkerScale

var (
	_ workflow.WorkerScaler    = (*Client)(nil)
	_ workflow.ClosureEnqueuer = (*Client)(nil)
)

// primaryAs returns the primary as I, or an UnsupportedError naming c
// when it lacks the capability or the interface.
func primaryAs[I any](f *Client, field string, c workflow.Caps) (I, error) {
	var zero I
	if err := f.need(field, c); err != nil {
		return zero, err
	}
	i, ok := f.primary.(I)
	if !ok {
		return zero, &workflow.UnsupportedError{Field: field, Missing: c, Reason: "the failover primary does not implement it"}
	}
	return i, nil
}

// EnqueueClosure runs c on the primary only, with no failover: a closure
// has no idempotency key, so an ambiguous primary failure retried on the
// secondary could run it twice. The RunRef carries the primary prefix.
func (f *Client) EnqueueClosure(ctx context.Context, c workflow.ClosureJob) (workflow.RunRef, error) {
	ce, err := primaryAs[workflow.ClosureEnqueuer](f, "EnqueueClosure", workflow.CapClosure)
	if err != nil {
		return workflow.RunRef{}, err
	}
	ref, err := ce.EnqueueClosure(ctx, c)
	if err != nil {
		return workflow.RunRef{}, err
	}
	return encodeRef(primarySide, ref), nil
}

// QueueLoads reports the primary's queue loads.
func (f *Client) QueueLoads(ctx context.Context) ([]workflow.QueueLoad, error) {
	s, err := primaryAs[workflow.WorkerScaler](f, "QueueLoads", workflow.CapWorkerScale)
	if err != nil {
		return nil, err
	}
	return s.QueueLoads(ctx)
}

// SetWorkers sets a queue's worker count on the primary.
func (f *Client) SetWorkers(ctx context.Context, queue string, n int) error {
	s, err := primaryAs[workflow.WorkerScaler](f, "SetWorkers", workflow.CapWorkerScale)
	if err != nil {
		return err
	}
	return s.SetWorkers(ctx, queue, n)
}
