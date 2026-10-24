package workflow

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
)

// MaxChainJobs bounds Job.Chain, counting every job after the first, so a
// chain stored with a run (one database row on a durable driver) stays a
// size a driver can load and copy on every step. A step that edits its
// chain past it gets an ErrInvalid.
const MaxChainJobs = 1000

// validateChain checks job.Chain: at most MaxChainJobs jobs in all, each
// passing ValidateJob. Nested chains (a chained job with its own Chain)
// count towards the same bound, because NextInChain flattens them.
func validateChain(job Job) error {
	if n := chainLen(job.Chain); n > MaxChainJobs {
		return invalidf("job for %q: Job.Chain holds %d jobs, more than %d", job.Workflow, n, MaxChainJobs)
	}
	for i, next := range job.Chain {
		if err := ValidateJob(next); err != nil {
			return fmt.Errorf("job for %q: Job.Chain[%d]: %w", job.Workflow, i, err)
		}
	}
	return nil
}

// chainLen counts the jobs of chain, nested chains included.
func chainLen(chain []Job) int {
	n := len(chain)
	for _, j := range chain {
		n += chainLen(j.Chain)
	}
	return n
}

// CloneJob returns a deep copy of j (Payload, Labels, Retry and Chain), so
// a driver or a recorder can keep a job without sharing the caller's
// slices and maps.
func CloneJob(j Job) Job {
	j.Payload = bytes.Clone(j.Payload)
	j.Labels = maps.Clone(j.Labels)
	if j.Retry != nil {
		r := *j.Retry
		j.Retry = &r
	}
	if j.Chain != nil {
		chain := make([]Job, len(j.Chain))
		for i, next := range j.Chain {
			chain[i] = CloneJob(next)
		}
		j.Chain = chain
	}
	return j
}

// NextInChain is the job a driver enqueues when a run with this chain
// succeeds: chain[0], carrying its own Chain followed by chain[1:], so
// the rest of the chain travels with it. It returns false for an empty
// chain. Every driver calls it, so a chain flattens the same way
// everywhere. The result shares nothing with chain.
func NextInChain(chain []Job) (Job, bool) {
	if len(chain) == 0 {
		return Job{}, false
	}
	next := CloneJob(chain[0])
	for _, rest := range chain[1:] {
		next.Chain = append(next.Chain, CloneJob(rest))
	}
	return next, true
}

// ChainEditor edits the rest of the chain of the run a step belongs to.
// A driver with CapChain sets one on every step attempt's ctx with
// WithChainEditor; steps reach it through PrependToChain and
// AppendToChain.
//
// Edits belong to the attempt: a driver applies them only when the
// attempt succeeds, so a step that fails and is retried does not add its
// jobs twice. ChainEdits implements this for drivers.
type ChainEditor interface {
	// Prepend puts jobs in front of the rest of the chain, in the order
	// given, so jobs[0] runs next once this run succeeds.
	Prepend(jobs ...Job) error
	// Append puts jobs at the end of the chain, in the order given.
	Append(jobs ...Job) error
}

// chainEditorKey is the unexported context key for a ChainEditor.
type chainEditorKey struct{}

// WithChainEditor returns a ctx carrying ed. Drivers call it before
// invoking a step handler.
func WithChainEditor(ctx context.Context, ed ChainEditor) context.Context {
	return context.WithValue(ctx, chainEditorKey{}, ed)
}

// ChainEditorFrom returns the ChainEditor a driver set on ctx, and false
// when ctx is not a step's context or the driver lacks CapChain.
func ChainEditorFrom(ctx context.Context) (ChainEditor, bool) {
	ed, ok := ctx.Value(chainEditorKey{}).(ChainEditor)
	return ed, ok
}

// PrependToChain puts jobs in front of the rest of the chain of the run
// whose step ctx is, so jobs[0] runs next once the run succeeds. Several
// calls stack: the jobs of the latest call run first. It applies only if
// the step attempt succeeds. Outside a step, or on a driver without
// CapChain (an engine whose run shape is fixed at registration), it
// returns an UnsupportedError; start a child run instead there. A job
// that fails ValidateJob or the driver's CheckJob is refused at once, and
// so is one carrying a reserved label that ctx does not allow
// (CheckJobLabels): a chained job reaches the driver's internal enqueue,
// so this is where an app's labels are checked.
func PrependToChain(ctx context.Context, jobs ...Job) error {
	ed, ok := ChainEditorFrom(ctx)
	if !ok {
		return noChain("PrependToChain")
	}
	if err := checkEditLabels(ctx, jobs); err != nil {
		return err
	}
	return ed.Prepend(jobs...)
}

// checkEditLabels is CheckJobLabels for each job a step adds to its chain,
// with the step's ctx, so an owner's AllowReservedLabels still applies.
func checkEditLabels(ctx context.Context, jobs []Job) error {
	for _, j := range jobs {
		if err := CheckJobLabels(ctx, j); err != nil {
			return err
		}
	}
	return nil
}

// AppendToChain puts jobs at the end of the chain of the run whose step
// ctx is, in the order given. It applies only if the step attempt
// succeeds, and is refused as PrependToChain is.
func AppendToChain(ctx context.Context, jobs ...Job) error {
	ed, ok := ChainEditorFrom(ctx)
	if !ok {
		return noChain("AppendToChain")
	}
	if err := checkEditLabels(ctx, jobs); err != nil {
		return err
	}
	return ed.Append(jobs...)
}

func noChain(field string) error {
	return &UnsupportedError{Field: field, Missing: CapChain,
		Reason: "ctx is not the ctx of a step on a driver that edits chains"}
}

// ChainEdits is the ChainEditor drivers use: it checks and buffers the
// edits of one step attempt (the reserved-label check, which needs the
// step's ctx, happens in PrependToChain and AppendToChain before it), and Apply produces the run's new chain once
// the attempt succeeded. Make one per attempt with NewChainEdits. It is
// safe for concurrent use, since a step may fan out goroutines.
type ChainEdits struct {
	have    Caps
	base    int
	mu      sync.Mutex
	prepend []Job // already in run order
	append  []Job
}

// NewChainEdits returns the editor of one attempt of a run whose stored
// chain is chain, checking each edit against have with CheckJob and the
// edited chain against MaxChainJobs. chain is only counted, not kept.
func NewChainEdits(have Caps, chain []Job) *ChainEdits {
	return &ChainEdits{have: have, base: chainLen(chain)}
}

// check validates jobs and returns deep copies, or the first refusal.
func (e *ChainEdits) check(jobs []Job) ([]Job, error) {
	out := make([]Job, len(jobs))
	for i, j := range jobs {
		if err := ValidateJob(j); err != nil {
			return nil, err
		}
		if err := CheckJob(e.have, j); err != nil {
			return nil, err
		}
		out[i] = CloneJob(j)
	}
	if n := e.base + chainLen(e.prepend) + chainLen(e.append) + chainLen(out); n > MaxChainJobs {
		return nil, invalidf("chain edit: the chain would hold %d jobs, more than %d", n, MaxChainJobs)
	}
	return out, nil
}

// Prepend implements ChainEditor.
func (e *ChainEdits) Prepend(jobs ...Job) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	cp, err := e.check(jobs)
	if err != nil {
		return err
	}
	e.prepend = append(cp, e.prepend...)
	return nil
}

// Append implements ChainEditor.
func (e *ChainEdits) Append(jobs ...Job) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	cp, err := e.check(jobs)
	if err != nil {
		return err
	}
	e.append = append(e.append, cp...)
	return nil
}

// Changed reports whether any edit was made.
func (e *ChainEdits) Changed() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.prepend)+len(e.append) > 0
}

// Apply returns chain with the buffered edits applied: the prepended
// jobs, then chain, then the appended jobs. chain is not modified.
func (e *ChainEdits) Apply(chain []Job) []Job {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := slices.Concat(e.prepend, chain, e.append)
	for i := range out {
		out[i] = CloneJob(out[i])
	}
	return out
}
