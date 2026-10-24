package workflowtest

import (
	"cmp"
	"context"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/ubgo/workflow"
)

// The run operations the Fake records itself (Retry, Delete, Pause,
// Resume, Paused) instead of forwarding through workflow.Forward.
var (
	_ workflow.RunRetrier = (*Fake)(nil)
	_ workflow.RunDeleter = (*Fake)(nil)
	_ workflow.Pauser     = (*Fake)(nil)
)

// Retry records a retry of a recorded failed run and moves it back to
// StatusQueued, as workflow.RunRetrier does: the run keeps its RunRef and
// takes its idempotency key again. It needs CapRetryRun. A run that is not
// StatusFailed is workflow.ErrWrongState, a key another unfinished run
// holds is workflow.ErrDuplicate, and a ref the Fake did not issue goes to
// the pass-through client, or is ErrNotFound without one. Use Finish to
// fail a run first.
func (f *Fake) Retry(ctx context.Context, ref workflow.RunRef) (workflow.RunRef, error) {
	if err := ctx.Err(); err != nil {
		return workflow.RunRef{}, err
	}
	if !f.caps.Has(workflow.CapRetryRun) {
		return workflow.RunRef{}, &workflow.UnsupportedError{Field: "Retry", Missing: workflow.CapRetryRun}
	}
	f.mu.Lock()
	r, ok := f.byRef[ref]
	if !ok {
		f.mu.Unlock()
		if f.inner != nil {
			return f.Forward.Retry(ctx, ref)
		}
		return workflow.RunRef{}, fmt.Errorf("%w: run %+v", workflow.ErrNotFound, ref)
	}
	defer f.mu.Unlock()
	if r.status != workflow.StatusFailed {
		return workflow.RunRef{}, fmt.Errorf("%w: run %+v is %s; only a failed run can be retried", workflow.ErrWrongState, ref, r.status)
	}
	if r.key != ([2]string{}) {
		if holder, held := f.idem[r.key]; held && holder != r {
			return workflow.RunRef{}, fmt.Errorf("%w: run %+v holds the key %q", workflow.ErrDuplicate, holder.ref, r.key[1])
		}
		f.idem[r.key] = r
	}
	r.status = workflow.StatusQueued
	f.retries = append(f.retries, ref)
	return r.ref, nil
}

// Delete forgets a recorded terminal run, as workflow.RunDeleter does:
// Status then returns ErrNotFound. The job stays in Jobs and Enqueued,
// since it was enqueued. It needs CapDeleteRun. A run that is not
// terminal is workflow.ErrWrongState, and a ref the Fake did not issue
// goes to the pass-through client, or is ErrNotFound without one.
func (f *Fake) Delete(ctx context.Context, ref workflow.RunRef) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !f.caps.Has(workflow.CapDeleteRun) {
		return &workflow.UnsupportedError{Field: "Delete", Missing: workflow.CapDeleteRun}
	}
	f.mu.Lock()
	r, ok := f.byRef[ref]
	if !ok {
		f.mu.Unlock()
		if f.inner != nil {
			return f.Forward.Delete(ctx, ref)
		}
		return fmt.Errorf("%w: run %+v", workflow.ErrNotFound, ref)
	}
	defer f.mu.Unlock()
	if !r.status.IsTerminal() {
		return fmt.Errorf("%w: run %+v is %s; only a finished run can be deleted", workflow.ErrWrongState, ref, r.status)
	}
	delete(f.byRef, ref)
	f.deletes = append(f.deletes, ref)
	return nil
}

// Pause records t as paused. It checks t as a driver does
// (workflow.CheckPause: CapPauseWorkflow for a workflow or All,
// CapPauseQueue for a queue). The Fake runs nothing, so a pause changes
// no status; it is there to be asserted (AssertPaused) and listed
// (Paused). A pause of a passed-through workflow (FakeExcept, FakeOnly)
// goes to the real client instead and is not recorded.
func (f *Fake) Pause(ctx context.Context, t workflow.PauseTarget) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := workflow.CheckPause(f.caps, t); err != nil {
		return err
	}
	if t.Workflow != "" && f.passes(t.Workflow) {
		return f.Forward.Pause(ctx, t)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.paused[t] = true
	return nil
}

// Resume removes the pause t, or every pause for workflow.PauseAll(), as
// workflow.Pauser does; resuming a target that is not paused is a no-op.
// A passed-through workflow's target goes to the real client.
func (f *Fake) Resume(ctx context.Context, t workflow.PauseTarget) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := workflow.CheckPause(f.caps, t); err != nil {
		return err
	}
	if t.Workflow != "" && f.passes(t.Workflow) {
		return f.Forward.Resume(ctx, t)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if t.All {
		clear(f.paused)
		return nil
	}
	delete(f.paused, t)
	return nil
}

// Paused lists the recorded pauses in force in workflow.Pauser's order:
// All first, then workflows, then queues, each by name.
func (f *Fake) Paused(ctx context.Context) ([]workflow.PauseTarget, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]workflow.PauseTarget, 0, len(f.paused))
	for t := range f.paused {
		out = append(out, t)
	}
	sortPauses(out)
	return out, nil
}

// sortPauses orders targets as workflow.Pauser.Paused lists them.
func sortPauses(ts []workflow.PauseTarget) {
	rank := func(t workflow.PauseTarget) int {
		switch {
		case t.All:
			return 0
		case t.Workflow != "":
			return 1
		}
		return 2
	}
	slices.SortFunc(ts, func(a, b workflow.PauseTarget) int {
		return cmp.Or(cmp.Compare(rank(a), rank(b)), cmp.Compare(a.Workflow, b.Workflow), cmp.Compare(a.Queue, b.Queue))
	})
}

// Retries returns every ref passed to a successful Retry, in order.
func (f *Fake) Retries() []workflow.RunRef {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.retries)
}

// Deletes returns every ref passed to a successful Delete, in order.
func (f *Fake) Deletes() []workflow.RunRef {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.deletes)
}

// AssertRetried fails t unless ref was retried.
func (f *Fake) AssertRetried(t testing.TB, ref workflow.RunRef) {
	t.Helper()
	assertRef(t, "retried", ref, f.Retries())
}

// AssertDeleted fails t unless ref was deleted.
func (f *Fake) AssertDeleted(t testing.TB, ref workflow.RunRef) {
	t.Helper()
	assertRef(t, "deleted", ref, f.Deletes())
}

// AssertPaused fails t unless target is paused now.
func (f *Fake) AssertPaused(t testing.TB, target workflow.PauseTarget) {
	t.Helper()
	ps, _ := f.Paused(context.Background())
	assertPause(t, target, ps, true)
}

// AssertNotPaused fails t if target is paused now.
func (f *Fake) AssertNotPaused(t testing.TB, target workflow.PauseTarget) {
	t.Helper()
	ps, _ := f.Paused(context.Background())
	assertPause(t, target, ps, false)
}

// AssertEnqueuedOn fails t unless a job of workflow wf matching every m
// was enqueued to run on queue: its effective queue (workflow.EffectiveQueue:
// Job.Queue, else the registered workflow's Queue, else
// workflow.DefaultQueue).
func (f *Fake) AssertEnqueuedOn(t testing.TB, queue, wf string, m ...JobMatcher) {
	t.Helper()
	def, _ := f.Workflow(wf)
	assertOn(t, queue, wf, def, f.Enqueued(wf, m...), m, f.recorded())
}

// HasQueue matches a job whose Job.Queue is q as given. An empty Job.Queue
// (the workflow's queue) matches only HasQueue(""); use AssertEnqueuedOn
// to match the queue a run actually lands on.
func HasQueue(q string) JobMatcher {
	return Where("queue "+strconv.Quote(q), func(j workflow.Job) bool { return j.Queue == q })
}

// HasRetry matches a job whose Job.Retry override equals p (Max, Backoff,
// Until, MaxErrors, DiscardOnExhaust; a job's override carries no functions).
func HasRetry(p workflow.RetryPolicy) JobMatcher {
	return Where(fmt.Sprintf("retry %+v", p), func(j workflow.Job) bool {
		return j.Retry != nil && reflect.DeepEqual(*j.Retry, p)
	})
}

// assertRef fails t unless ref is in refs.
func assertRef(t testing.TB, what string, ref workflow.RunRef, refs []workflow.RunRef) {
	t.Helper()
	if !slices.Contains(refs, ref) {
		t.Errorf("run %+v was not %s; %s: %v", ref, what, what, refs)
	}
}

// assertPause fails t unless target's presence in ps is want.
func assertPause(t testing.TB, target workflow.PauseTarget, ps []workflow.PauseTarget, want bool) {
	t.Helper()
	if slices.Contains(ps, target) == want {
		return
	}
	verb := "is not paused"
	if !want {
		verb = "is paused, want not"
	}
	t.Errorf("%s %s; paused: [%s]", describePause(target), verb, describePauses(ps))
}

func describePause(t workflow.PauseTarget) string {
	switch {
	case t.All:
		return "everything"
	case t.Workflow != "":
		return "workflow " + strconv.Quote(t.Workflow)
	}
	return "queue " + strconv.Quote(t.Queue)
}

func describePauses(ps []workflow.PauseTarget) string {
	parts := make([]string, len(ps))
	for i, p := range ps {
		parts[i] = describePause(p)
	}
	return strings.Join(parts, ", ")
}

// assertOn fails t unless a job in jobs runs on queue under def.
func assertOn(t testing.TB, queue, wf string, def workflow.Workflow, jobs []workflow.Job, m []JobMatcher, recorded string) {
	t.Helper()
	var seen []string
	for _, j := range jobs {
		q := workflow.EffectiveQueue(def, j)
		if q == queue {
			return
		}
		seen = append(seen, q)
	}
	t.Errorf("workflow %q%s was not enqueued on queue %q (queues seen: %v); %s", wf, describe(m), queue, seen, recorded)
}
