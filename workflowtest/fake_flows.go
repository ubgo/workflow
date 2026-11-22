package workflowtest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/ubgo/workflow"
)

// The optional operations the Fake records itself although they carry no
// capability bit: it does them unless FakeLike copied a client that does
// not (workflow.Supports).
//
//ds:def id=workflowtest-fakeops-pwnskjhn owner=@khanakia stability=stable
var fakeOps = []workflow.Op{workflow.OpResetRun, workflow.OpResetSucceeded, workflow.OpForkRun, workflow.OpSignalWaiting}

var (
	_ workflow.RunResetter        = (*Fake)(nil)
	_ workflow.RunResetterWith    = (*Fake)(nil)
	_ workflow.RunForker          = (*Fake)(nil)
	_ workflow.TransientSignaller = (*Fake)(nil)
	_ workflow.OpProber           = (*Fake)(nil)
)

// does reports whether the Fake does op (see fakeOps).
func (f *Fake) does(op workflow.Op) bool { return f.ops == nil || f.ops[op] }

// Supports answers for the operations the Fake records (ResetRun,
// ResetRunWith, ForkRun, SignalWaiting) itself, and for the others as workflow.Forward does: by
// the pass-through client, false without one.
func (f *Fake) Supports(op workflow.Op) bool {
	if slices.Contains(fakeOps, op) {
		return f.does(op)
	}
	return f.Forward.Supports(op)
}

// FakeReset is one successful ResetRun or ResetRunWith call.
//
//ds:def id=workflowtest-fakereset-dw7kj5kq owner=@khanakia stability=api
type FakeReset struct {
	Ref  workflow.RunRef
	Step string

	// Succeeded is ResetOptions.Succeeded of a ResetRunWith call that
	// reset a succeeded run.
	Succeeded bool
}

// ResetRun records a reset of a recorded failed run from step and moves
// it back to StatusQueued under its own RunRef, as workflow.RunResetter
// does. A step the run's registered workflow does not have is
// workflow.ErrInvalid, a run that is not StatusFailed is
// workflow.ErrWrongState, a key another unfinished run holds is
// workflow.ErrDuplicate, and a ref the Fake did not issue goes to the
// pass-through client, or is ErrNotFound without one. After
// FakeLike(c) with a c that has no ResetRun it is an UnsupportedError. Use
// Finish to fail a run first.
func (f *Fake) ResetRun(ctx context.Context, ref workflow.RunRef, step string) (workflow.RunRef, error) {
	return f.ResetRunWith(ctx, ref, step, workflow.ResetOptions{})
}

// ResetRunWith is ResetRun that, with opts.Succeeded, also resets a
// recorded succeeded run, which then carries workflow.LabelRedo, as
// workflow.RunResetterWith does. After FakeLike(c) with a c that does
// not reset succeeded runs (workflow.OpResetSucceeded), such a call is an
// UnsupportedError.
//
//ds:def id=workflowtest-fake-resetrunwith-6smy4tj4 owner=@khanakia stability=stable
func (f *Fake) ResetRunWith(ctx context.Context, ref workflow.RunRef, step string, opts workflow.ResetOptions) (workflow.RunRef, error) {
	if err := ctx.Err(); err != nil {
		return workflow.RunRef{}, err
	}
	if !f.does(workflow.OpResetRun) {
		return workflow.RunRef{}, workflow.Unsupported("ResetRun", "the client the Fake is like does not implement it")
	}
	if opts.Succeeded && !f.does(workflow.OpResetSucceeded) {
		return workflow.RunRef{}, workflow.Unsupported("ResetRunWith", "the client the Fake is like does not reset a succeeded run")
	}
	f.mu.Lock()
	r, ok := f.byRef[ref]
	if !ok {
		f.mu.Unlock()
		if f.inner != nil {
			if opts == (workflow.ResetOptions{}) {
				return f.Forward.ResetRun(ctx, ref, step)
			}
			return f.Forward.ResetRunWith(ctx, ref, step, opts)
		}
		return workflow.RunRef{}, fmt.Errorf("%w: run %+v", workflow.ErrNotFound, ref)
	}
	defer f.mu.Unlock()
	if workflow.StepsFrom(f.wfs[r.job.Workflow], step) == nil {
		return workflow.RunRef{}, fmt.Errorf("%w: workflow %q has no step %q", workflow.ErrInvalid, r.job.Workflow, step)
	}
	if !workflow.ResetStatusOK(r.status, opts) {
		return workflow.RunRef{}, fmt.Errorf("%w: run %+v is %s; it cannot be reset with %+v", workflow.ErrWrongState, ref, r.status, opts)
	}
	redo := r.status == workflow.StatusSucceeded
	if r.key != ([2]string{}) {
		if holder, held := f.idem[r.key]; held && holder != r {
			return workflow.RunRef{}, fmt.Errorf("%w: run %+v holds the key %q", workflow.ErrDuplicate, holder.ref, r.key[1])
		}
		f.idem[r.key] = r
	}
	r.status = workflow.StatusQueued
	if redo {
		r.job.Labels = maps.Clone(r.job.Labels)
		if r.job.Labels == nil {
			r.job.Labels = map[string]string{}
		}
		r.job.Labels[workflow.LabelRedo] = "true"
	}
	f.resets = append(f.resets, FakeReset{Ref: ref, Step: step, Succeeded: redo})
	return r.ref, nil
}

// SignalWaiting records ev and returns how many runs it resumed: none of
// the Fake's own, since nothing waits on it, plus what the pass-through
// client resumed when there is one (which refuses when that client has no
// SignalWaiting). After FakeLike(c) with a c that has no SignalWaiting it
// is an UnsupportedError and nothing is recorded.
//
//ds:def id=workflowtest-fake-signalwaiting-84da3r4b owner=@khanakia stability=stable
func (f *Fake) SignalWaiting(ctx context.Context, ev workflow.Event) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if !f.does(workflow.OpSignalWaiting) {
		return 0, workflow.Unsupported("SignalWaiting", "the client the Fake is like does not implement it")
	}
	f.mu.Lock()
	if !f.started {
		f.mu.Unlock()
		return 0, workflow.ErrNotStarted
	}
	rec := ev
	rec.Payload = bytes.Clone(ev.Payload)
	f.waiting = append(f.waiting, rec)
	f.mu.Unlock()
	if f.inner != nil {
		return f.Forward.SignalWaiting(ctx, ev)
	}
	return 0, nil
}

// Resets returns every successful ResetRun call, in order.
func (f *Fake) Resets() []FakeReset {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.resets)
}

// SignalsWaiting returns every SignalWaiting event, in order. Signal's
// events are in Signals.
//
//ds:def id=workflowtest-fake-signalswaiting-haecqnnh owner=@khanakia stability=stable
func (f *Fake) SignalsWaiting() []workflow.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.waiting)
}

// AssertReset fails t unless ref was reset from step (a failed or a
// succeeded run).
//
//ds:def id=workflowtest-fake-assertreset-2gjbkxqc owner=@khanakia stability=stable
func (f *Fake) AssertReset(t testing.TB, ref workflow.RunRef, step string) {
	t.Helper()
	resets := f.Resets()
	if slices.ContainsFunc(resets, func(r FakeReset) bool { return r.Ref == ref && r.Step == step }) {
		return
	}
	t.Errorf("run %+v was not reset from step %q; resets: %+v", ref, step, resets)
}

// AssertSignalledWaiting fails t unless an event named name with key key
// went to SignalWaiting. An event sent with Signal does not count.
//
//ds:def id=workflowtest-fake-assertsignalledwaiting-x9ecyaxg owner=@khanakia stability=stable
func (f *Fake) AssertSignalledWaiting(t testing.TB, name, key string) {
	t.Helper()
	var all []string
	for _, ev := range f.SignalsWaiting() {
		if ev.Name == name && ev.Key == key {
			return
		}
		all = append(all, ev.Name+"/"+ev.Key)
	}
	t.Errorf("event %q with key %q was not sent to SignalWaiting; sent: [%s]", name, key, strings.Join(all, ", "))
}

// Promises returns the Fake's promise store, one per Fake. Hand it and the
// Fake to the code under test (workflow.ResolvePromise(ctx, fake,
// fake.Promises(), id, v)), then assert with AssertPromiseResolved,
// AssertPromiseRejected and AssertPromisePending, which read this store
// and the signals the settlement sent (the Fake must be started for
// those).
//
//ds:def id=workflowtest-fake-promises-yqrgjhnb owner=@khanakia stability=stable
func (f *Fake) Promises() *workflow.MemoryPromises {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.promises == nil {
		f.promises = workflow.NewMemoryPromises()
	}
	return f.promises
}

// AssertPromiseResolved fails t unless promise id was resolved in
// Promises and its settlement signalled (workflow.PromiseEvent with key
// id) through the Fake. A non-nil value must equal the resolved value.
//
//ds:def id=workflowtest-fake-assertpromiseresolved-wy83merk owner=@khanakia stability=stable
func (f *Fake) AssertPromiseResolved(t testing.TB, id string, value []byte) {
	t.Helper()
	v, settled, err := workflow.PromiseResult(context.Background(), f.Promises(), id)
	switch {
	case !settled:
		t.Errorf("promise %q is not settled in the Fake's Promises store", id)
		return
	case err != nil:
		t.Errorf("promise %q was not resolved: %v", id, err)
		return
	case value != nil && !bytes.Equal(v, value):
		t.Errorf("promise %q resolved with %q, want %q", id, v, value)
		return
	}
	f.assertPromiseSignalled(t, id)
}

// AssertPromiseRejected fails t unless promise id was rejected in Promises
// and its settlement signalled through the Fake. A non-empty reason must
// equal the rejection's reason.
//
//ds:def id=workflowtest-fake-assertpromiserejected-9btpkcnz owner=@khanakia stability=stable
func (f *Fake) AssertPromiseRejected(t testing.TB, id, reason string) {
	t.Helper()
	_, settled, err := workflow.PromiseResult(context.Background(), f.Promises(), id)
	var rej *workflow.PromiseRejectedError
	switch {
	case !settled:
		t.Errorf("promise %q is not settled in the Fake's Promises store", id)
		return
	case !errors.As(err, &rej):
		t.Errorf("promise %q was not rejected: %v", id, err)
		return
	case reason != "" && rej.Reason != reason:
		t.Errorf("promise %q rejected with %q, want %q", id, rej.Reason, reason)
		return
	}
	f.assertPromiseSignalled(t, id)
}

// AssertPromisePending fails t if promise id is settled in Promises.
//
//ds:def id=workflowtest-fake-assertpromisepending-56tykqm3 owner=@khanakia stability=stable
func (f *Fake) AssertPromisePending(t testing.TB, id string) {
	t.Helper()
	if _, settled, _ := workflow.PromiseResult(context.Background(), f.Promises(), id); settled {
		t.Errorf("promise %q is settled, want it pending", id)
	}
}

//ds:def id=workflowtest-fake-assertpromisesignalled-hvhde9wj owner=@khanakia stability=stable
func (f *Fake) assertPromiseSignalled(t testing.TB, id string) {
	t.Helper()
	for _, ev := range f.Signals() {
		if ev.Name == workflow.PromiseEvent && ev.Key == id {
			return
		}
	}
	t.Errorf("promise %q is settled but its signal (%s) did not go through the Fake: was the settlement given another client?", id, workflow.PromiseEvent)
}

// FakeObjectCall is one recorded call of a virtual object
// (workflow.CallObject): the job enqueued for it, read back.
//
//ds:def id=workflowtest-fakeobjectcall-hpe9wvp3 owner=@khanakia stability=api
type FakeObjectCall struct {
	Key    string
	Method string
	Args   []byte
	Job    workflow.Job
}

// ObjectCalls returns the recorded calls of the virtual object named
// object, in order. The Fake runs none of them; register the object's
// workflow (workflow.DefineObject) so CallObject's Enqueue is accepted.
//
//ds:def id=workflowtest-fake-objectcalls-d36grba2 owner=@khanakia stability=stable
func (f *Fake) ObjectCalls(object string) []FakeObjectCall {
	var out []FakeObjectCall
	for _, j := range f.Enqueued(object) {
		key, ok := j.Labels[workflow.LabelObjectKey]
		if !ok {
			continue
		}
		var call struct {
			Method string `json:"method"`
			Args   []byte `json:"args,omitempty"`
		}
		if err := json.Unmarshal(j.Payload, &call); err != nil {
			continue
		}
		out = append(out, FakeObjectCall{Key: key, Method: call.Method, Args: call.Args, Job: j})
	}
	return out
}

// AssertObjectCalled fails t unless method of object was called for key.
//
//ds:def id=workflowtest-fake-assertobjectcalled-kmbmnk46 owner=@khanakia stability=stable
func (f *Fake) AssertObjectCalled(t testing.TB, object, key, method string) {
	t.Helper()
	calls := f.ObjectCalls(object)
	var all []string
	for _, c := range calls {
		if c.Key == key && c.Method == method {
			return
		}
		all = append(all, c.Key+"."+c.Method)
	}
	t.Errorf("object %q: %s was not called for key %q; calls: [%s]", object, method, key, strings.Join(all, ", "))
}

// AssertObjectNotCalled fails t if any method of object was called for
// key.
//
//ds:def id=workflowtest-fake-assertobjectnotcalled-saufgrh2 owner=@khanakia stability=stable
func (f *Fake) AssertObjectNotCalled(t testing.TB, object, key string) {
	t.Helper()
	for _, c := range f.ObjectCalls(object) {
		if c.Key == key {
			t.Errorf("object %q: %s was called for key %q", object, c.Method, key)
			return
		}
	}
}
