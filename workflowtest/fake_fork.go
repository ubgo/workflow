package workflowtest

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/ubgo/workflow"
)

// FakeFork is one successful ForkRun call.
type FakeFork struct {
	// From is the forked run, Ref the fork.
	From, Ref workflow.RunRef
	Step      string
	Opts      workflow.ForkOptions
}

// ForkRun records a fork of a recorded finished run from step as a new
// recorded run (StatusQueued, its own RunID) with the job a driver would
// start: the original's workflow, payload and tenant, labels through
// workflow.ForkLabels, opts.IdempotencyKey and no chain, as
// workflow.RunForker does. An empty step or a reserved label in opts is
// workflow.ErrInvalid, as is a step the run's workflow does not have; a
// run that has not finished is workflow.ErrWrongState; a fork key held by
// an unfinished recorded run returns that run's ref; a ref the Fake did
// not issue goes to the pass-through client, or is ErrNotFound without
// one. After FakeLike(c) with a c that cannot fork it is an
// UnsupportedError. Use Finish to end a run first.
func (f *Fake) ForkRun(ctx context.Context, ref workflow.RunRef, step string, opts workflow.ForkOptions) (workflow.RunRef, error) {
	if err := ctx.Err(); err != nil {
		return workflow.RunRef{}, err
	}
	if !f.does(workflow.OpForkRun) {
		return workflow.RunRef{}, workflow.Unsupported("ForkRun", "the client the Fake is like does not implement it")
	}
	if err := workflow.CheckFork(step, opts); err != nil {
		return workflow.RunRef{}, err
	}
	f.mu.Lock()
	orig, ok := f.byRef[ref]
	if !ok {
		f.mu.Unlock()
		if f.inner != nil {
			return f.Forward.ForkRun(ctx, ref, step, opts)
		}
		return workflow.RunRef{}, fmt.Errorf("%w: run %+v", workflow.ErrNotFound, ref)
	}
	defer f.mu.Unlock()
	wf := f.wfs[orig.job.Workflow]
	if workflow.StepsFrom(wf, step) == nil {
		return workflow.RunRef{}, fmt.Errorf("%w: workflow %q has no step %q", workflow.ErrInvalid, orig.job.Workflow, step)
	}
	if !workflow.ForkStatusOK(orig.status) {
		return workflow.RunRef{}, fmt.Errorf("%w: run %+v is %s; only a finished run can be forked", workflow.ErrWrongState, ref, orig.status)
	}
	job := copyJob(orig.job)
	job.Labels = workflow.ForkLabels(orig.job.Labels, opts.Labels, orig.ref.RunID, step, workflow.ForkRedo(orig.status, orig.job.Labels))
	job.IdempotencyKey = opts.IdempotencyKey
	job.Chain = nil
	job.RunAt = time.Time{}
	key := job.IdempotencyKey
	switch wf.Idempotency {
	case workflow.IdempotencyOff:
		key = ""
	case workflow.IdempotencyRequired:
		if key == "" {
			return workflow.RunRef{}, fmt.Errorf("%w: workflow %q requires an IdempotencyKey (ForkOptions.IdempotencyKey)", workflow.ErrInvalid, wf.Name)
		}
	}
	r := &fakeRun{job: job, status: workflow.StatusQueued}
	if key != "" {
		r.key = [2]string{wf.Name, key}
		if holder, ok := f.idem[r.key]; ok {
			workflow.MarkDuplicate(ctx)
			return holder.ref, nil
		}
		f.idem[r.key] = r
	}
	r.ref.RunID = workflow.RunID("fake-run-" + strconv.Itoa(len(f.runs)+1))
	f.runs = append(f.runs, r)
	f.byRef[r.ref] = r
	f.forks = append(f.forks, FakeFork{From: ref, Ref: r.ref, Step: step, Opts: opts})
	return r.ref, nil
}

// Forks returns every successful ForkRun call, in order.
func (f *Fake) Forks() []FakeFork {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.forks)
}

// AssertForked fails t unless ref was forked from step, and returns the
// fork's ref (zero when it was not).
func (f *Fake) AssertForked(t testing.TB, ref workflow.RunRef, step string) workflow.RunRef {
	t.Helper()
	forks := f.Forks()
	for _, fk := range forks {
		if fk.From == ref && fk.Step == step {
			return fk.Ref
		}
	}
	t.Errorf("run %+v was not forked from step %q; forks: %+v", ref, step, forks)
	return workflow.RunRef{}
}
