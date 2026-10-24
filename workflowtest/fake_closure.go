package workflowtest

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/ubgo/workflow"
)

var _ workflow.ClosureEnqueuer = (*Fake)(nil)

// EnqueueClosure records c without running it, as a run of
// workflow.ClosureWorkflow labelled workflow.LabelClosure, so Status,
// Cancel, Finish and Jobs see it like any recorded run. It refuses what
// a real driver refuses: before Start ErrNotStarted, without CapClosure
// (FakeCaps of a durable driver) an UnsupportedError, an invalid closure
// ErrInvalid. Closures are never passed through. RunClosures runs the
// recorded ones when a test wants their effect.
func (f *Fake) EnqueueClosure(ctx context.Context, c workflow.ClosureJob) (workflow.RunRef, error) {
	if err := ctx.Err(); err != nil {
		return workflow.RunRef{}, err
	}
	if !f.caps.Has(workflow.CapClosure) {
		return workflow.RunRef{}, &workflow.UnsupportedError{Field: "EnqueueClosure", Missing: workflow.CapClosure,
			Reason: "the Fake claims the capabilities of a driver that cannot run closures"}
	}
	if err := workflow.ValidateClosure(c); err != nil {
		return workflow.RunRef{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.started {
		return workflow.RunRef{}, workflow.ErrNotStarted
	}
	labels := maps.Clone(c.Labels)
	if labels == nil {
		labels = map[string]string{}
	}
	labels[workflow.LabelClosure] = c.Name
	c.Labels = maps.Clone(c.Labels)
	f.closures = append(f.closures, c)
	r := &fakeRun{
		job:    workflow.Job{Workflow: workflow.ClosureWorkflow, Labels: labels, Priority: c.Priority, Queue: c.Queue, RunAt: c.RunAt},
		status: workflow.StatusQueued,
	}
	n := strconv.Itoa(len(f.runs) + 1)
	if c.RunAt.IsZero() {
		r.ref.RunID = workflow.RunID("fake-run-" + n)
	} else {
		r.ref.ScheduledID = workflow.ScheduledID("fake-delayed-" + n)
		r.status = workflow.StatusScheduled
	}
	f.runs = append(f.runs, r)
	f.byRef[r.ref] = r
	return r.ref, nil
}

// Closures returns the recorded closures, in enqueue order, optionally
// only those with one of names.
func (f *Fake) Closures(names ...string) []workflow.ClosureJob {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []workflow.ClosureJob
	for _, c := range f.closures {
		if len(names) == 0 || slices.Contains(names, c.Name) {
			out = append(out, c)
		}
	}
	return out
}

// AssertClosureEnqueued fails t unless a closure named name was enqueued.
func (f *Fake) AssertClosureEnqueued(t testing.TB, name string) {
	t.Helper()
	if len(f.Closures(name)) > 0 {
		return
	}
	var got []string
	for _, c := range f.Closures() {
		got = append(got, c.Name)
	}
	t.Errorf("no closure %q was enqueued; closures: [%s]", name, strings.Join(got, ", "))
}

// RunClosures calls every recorded closure's Fn once, in enqueue order,
// with ctx, and returns their errors joined (each naming its closure).
// It runs no retries and does not change the recorded runs' statuses; it
// is for a test that asserts on what the closures do.
func (f *Fake) RunClosures(ctx context.Context) error {
	var errs []error
	for _, c := range f.Closures() {
		if err := c.Fn(ctx); err != nil {
			errs = append(errs, fmt.Errorf("closure %q: %w", c.Name, err))
		}
	}
	return errors.Join(errs...)
}
