package workflow

import "context"

// EnqueueOutcome is what Enqueue did beyond the RunRef it returned. A
// caller asks for it with WithEnqueueOutcome; drivers and enqueue
// middleware fill it with MarkDuplicate and MarkInserted, so it passes
// through any stack of wrappers without changing Enqueue's signature.
type EnqueueOutcome struct {
	// Reported is true when something below the caller reported the
	// outcome. False means the client cannot tell (a driver without the
	// report), so Duplicate false is "unknown", not "new".
	Reported bool

	// Duplicate is true when no new run was started: the RunRef is an
	// existing run's (an idempotency key or a unique lock held by an
	// unfinished run).
	Duplicate bool
}

type outcomeKey struct{}

// WithEnqueueOutcome returns a ctx that collects the outcome of the
// Enqueue it is passed to, and the outcome to read after the call:
//
//	ctx, out := workflow.WithEnqueueOutcome(ctx)
//	ref, err := client.Enqueue(ctx, job)
//	if err == nil && out.Duplicate { ... }
//
// Use a fresh one per Enqueue: an Enqueue of a chain or a batch made with
// the same ctx reports its first job only.
func WithEnqueueOutcome(ctx context.Context) (context.Context, *EnqueueOutcome) {
	out := &EnqueueOutcome{}
	return context.WithValue(ctx, outcomeKey{}, out), out
}

// MarkDuplicate records that the Enqueue handling ctx returned an
// existing run. Drivers call it when an idempotency key returns the
// holder's run; middleware.Unique calls it when it returns the holder
// instead of ErrDuplicate. It does nothing when the caller did not ask.
func MarkDuplicate(ctx context.Context) {
	if out, ok := ctx.Value(outcomeKey{}).(*EnqueueOutcome); ok && !out.Reported {
		out.Reported, out.Duplicate = true, true
	}
}

// MarkInserted records that the Enqueue handling ctx started a new run.
// Drivers call it once the run is stored. The first mark wins, so a
// middleware that marked a duplicate is not overwritten.
func MarkInserted(ctx context.Context) {
	if out, ok := ctx.Value(outcomeKey{}).(*EnqueueOutcome); ok && !out.Reported {
		out.Reported = true
	}
}
