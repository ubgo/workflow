// Command fake-forks tests the app code behind a support console with
// workflowtest.Fake and no worker: forking a finished order run, resetting
// a failed one in place, waking only the runs waiting now
// (SignalWaiting), settling a refund promise, calling a virtual object and
// dispatching a batch. Every call is recorded by the Fake and read back:
// Forks, Resets, SignalsWaiting, Promises, ObjectCalls and Batches.
//
// It is the program behind docs/testing/fake.md: the guide copies its
// regions (marked "snippet:") and task docs:snippets fails when they
// drift. main_test.go asserts the same calls with the Fake's Assert*
// helpers.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
	"github.com/ubgo/workflow/workflowtest"
)

// Workflow, step, object, event and label names the app and its tests
// share, so a typo is a compile error rather than an assertion that
// silently looks for the wrong thing.
const (
	orderWorkflow  = "order"
	reshipWorkflow = "reship"

	stepCharge = "charge"
	stepPack   = "pack"
	stepShip   = "ship"

	cartObject  = "cart"
	methodClear = "clear"

	// managerOnline is the event support sends when a manager is
	// available, so approval steps waiting now can go on; a run that
	// starts waiting later must not see it, which is what SignalWaiting
	// is for.
	managerOnline = "manager-online"

	// labelBy marks who started a fork, so the run list shows support's
	// redos apart from the original runs.
	labelBy      = "by"
	labelSupport = "support"
)

// approved is the value a refund promise is resolved with.
var approved = []byte(`"approved"`)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "fake-forks:", err)
		os.Exit(1)
	}
}

// snippet:app
// Desk is app code behind a support console. It depends on the
// workflow.Client interface and a workflow.PromiseStore, never on a
// driver, so a test hands it a Fake (and the Fake's promise store). The
// optional operations are reached through the interface a driver
// implements, as production code reaches them.
type Desk struct {
	Jobs     workflow.Client
	Promises workflow.PromiseStore
	Batches  *batch.Manager
}

// Redo starts a new run from a finished one at step, keeping the outputs
// of the steps before it, so support can rerun shipping without charging
// the card again.
func (d *Desk) Redo(ctx context.Context, ref workflow.RunRef, step string) (workflow.RunRef, error) {
	f, ok := d.Jobs.(workflow.RunForker)
	if !ok {
		return workflow.RunRef{}, workflow.Unsupported("ForkRun", "the client is not a workflow.RunForker")
	}
	return f.ForkRun(ctx, ref, step, workflow.ForkOptions{Labels: map[string]string{labelBy: labelSupport}})
}

// Retry re-runs a failed run in place from step: same RunID, the steps
// before step keep their outputs.
func (d *Desk) Retry(ctx context.Context, ref workflow.RunRef, step string) error {
	r, ok := d.Jobs.(workflow.RunResetter)
	if !ok {
		return workflow.Unsupported("ResetRun", "the client is not a workflow.RunResetter")
	}
	_, err := r.ResetRun(ctx, ref, step)
	return err
}

// ManagerOnline wakes the approval steps of team that wait now; the event
// is not kept for runs that start waiting later.
func (d *Desk) ManagerOnline(ctx context.Context, team string) (int, error) {
	s, ok := d.Jobs.(workflow.TransientSignaller)
	if !ok {
		return 0, workflow.Unsupported("SignalWaiting", "the client is not a workflow.TransientSignaller")
	}
	return s.SignalWaiting(ctx, workflow.Event{Name: managerOnline, Key: team})
}

// Refund settles the refund promise id: resolved when approved, rejected
// with reason otherwise. The run awaiting it resumes either way.
func (d *Desk) Refund(ctx context.Context, id string, ok bool, reason string) error {
	if ok {
		return workflow.ResolvePromise(ctx, d.Jobs, d.Promises, id, approved)
	}
	return workflow.RejectPromise(ctx, d.Jobs, d.Promises, id, reason)
}

// ClearCart calls the cart object's clear method for user.
func (d *Desk) ClearCart(ctx context.Context, user string) error {
	_, err := workflow.CallObject(ctx, d.Jobs, cartObject, user, methodClear, nil, workflow.Job{})
	return err
}

// Reship sends every order again as one batch, so support sees the
// progress of the whole set.
func (d *Desk) Reship(ctx context.Context, orders ...string) (batch.ID, error) {
	b := batch.Batch{Name: "reship"}
	for _, o := range orders {
		b.Jobs = append(b.Jobs, workflow.Job{Workflow: reshipWorkflow, Payload: []byte(`"` + o + `"`)})
	}
	return d.Batches.Dispatch(ctx, b)
}

// snippet:end

// noop is a step body; the Fake runs nothing, so it is never called.
func noop(context.Context, []byte) ([]byte, error) { return nil, nil }

// snippet:setup
// newFake returns a started Fake with the app's workflows registered,
// and a Desk on it. The cart object is registered from DefineObject, so
// CallObject's Enqueue is accepted and read back by ObjectCalls.
func newFake() (*workflowtest.Fake, *Desk, error) {
	fake := workflowtest.NewFake()
	order := workflow.Workflow{Name: orderWorkflow, Steps: []workflow.Step{
		{Name: stepCharge, Run: noop},
		{Name: stepPack, After: []string{stepCharge}, Run: noop},
		{Name: stepShip, After: []string{stepPack}, Run: noop},
	}}
	reship := workflow.Workflow{Name: reshipWorkflow, Steps: []workflow.Step{{Name: stepShip, Run: noop}}}
	cart, err := workflow.DefineObject(workflow.ObjectSpec[[]string]{
		Name: cartObject,
		Methods: map[string]workflow.ObjectMethod[[]string]{
			methodClear: func(context.Context, []string, []byte) ([]string, []byte, error) { return nil, nil, nil },
		},
		Store: workflow.NewMemoryJournal(),
	})
	if err != nil {
		return nil, nil, err
	}
	for _, wf := range []workflow.Workflow{order, reship, cart} {
		if err := fake.Register(wf); err != nil {
			return nil, nil, err
		}
	}
	if err := fake.Start(context.Background()); err != nil {
		return nil, nil, err
	}
	desk := &Desk{Jobs: fake, Promises: fake.Promises(), Batches: batch.New(fake, batch.NewMemoryStore())}
	return fake, desk, nil
}

// snippet:end

// run is main without the process exit, writing to w so the test checks
// every line.
func run(ctx context.Context, w io.Writer) error {
	fake, desk, err := newFake()
	if err != nil {
		return err
	}

	// snippet:records
	order, err := fake.Enqueue(ctx, workflow.Job{Workflow: orderWorkflow, Payload: []byte(`"order-7"`)})
	if err != nil {
		return err
	}
	// Recorded runs never finish on their own: Finish says how they ended.
	if err := fake.Finish(order, workflow.StatusFailed); err != nil {
		return err
	}
	if err := desk.Retry(ctx, order, stepPack); err != nil {
		return err
	}
	for _, r := range fake.Resets() { // []workflowtest.FakeReset
		fmt.Fprintf(w, "reset %s from %s (succeeded run: %t)\n", r.Ref.RunID, r.Step, r.Succeeded)
	}

	if err := fake.Finish(order, workflow.StatusSucceeded); err != nil {
		return err
	}
	if _, err := desk.Redo(ctx, order, stepShip); err != nil {
		return err
	}
	for _, f := range fake.Forks() { // []workflowtest.FakeFork
		job := fake.Enqueued(orderWorkflow, workflowtest.HasLabel(workflow.LabelForkOf, f.From.RunID.String()))[0]
		fmt.Fprintf(w, "fork %s of %s from %s by=%s redo=%s\n", f.Ref.RunID, f.From.RunID, f.Step, f.Opts.Labels[labelBy], job.Labels[workflow.LabelRedo])
	}

	if _, err := desk.ManagerOnline(ctx, "refunds"); err != nil {
		return err
	}
	for _, ev := range fake.SignalsWaiting() {
		fmt.Fprintf(w, "signal-waiting %s/%s (Signal calls: %d)\n", ev.Name, ev.Key, len(fake.Signals()))
	}

	if err := desk.Refund(ctx, "refund-7", true, ""); err != nil {
		return err
	}
	if err := desk.Refund(ctx, "refund-8", false, "used item"); err != nil {
		return err
	}
	for _, id := range []string{"refund-7", "refund-8", "refund-9"} {
		v, settled, err := workflow.PromiseResult(ctx, fake.Promises(), id)
		fmt.Fprintf(w, "promise %s settled=%t value=%s err=%v\n", id, settled, v, err)
	}

	if err := desk.ClearCart(ctx, "ada"); err != nil {
		return err
	}
	for _, c := range fake.ObjectCalls(cartObject) { // []workflowtest.FakeObjectCall
		fmt.Fprintf(w, "object %s key=%s method=%s\n", c.Job.Workflow, c.Key, c.Method)
	}

	if _, err := desk.Reship(ctx, "order-7", "order-8"); err != nil {
		return err
	}
	for _, b := range fake.Batches() { // []workflowtest.FakeBatch
		var first string
		if err := json.Unmarshal(b.Jobs[0].Payload, &first); err != nil {
			return err
		}
		fmt.Fprintf(w, "batch of %d %s jobs, first %s\n", len(b.Jobs), b.Jobs[0].Workflow, first)
	}
	// snippet:end
	return nil
}
