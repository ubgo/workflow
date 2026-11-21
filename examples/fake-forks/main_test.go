package main

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/workflowtest"
)

// want is the program's whole output, the one its README and guide show.
const want = `reset fake-run-1 from pack (succeeded run: false)
fork fake-run-2 of fake-run-1 from ship by=support redo=true
signal-waiting manager-online/refunds (Signal calls: 0)
promise refund-7 settled=true value="approved" err=<nil>
promise refund-8 settled=true value= err=workflow: promise "refund-8" rejected: used item
promise refund-9 settled=false value= err=<nil>
object cart key=ada method=clear
batch of 2 reship jobs, first order-7
`

// TestRun runs the program and checks every line it prints.
func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}

// snippet:asserts
// TestSupportDesk checks every call the Desk makes with the Fake's
// assertions; no worker runs and nothing waits.
func TestSupportDesk(t *testing.T) {
	ctx := context.Background()
	fake, desk, err := newFake()
	if err != nil {
		t.Fatal(err)
	}
	order, err := fake.Enqueue(ctx, workflow.Job{Workflow: orderWorkflow})
	if err != nil {
		t.Fatal(err)
	}

	// Reset needs a failed run, fork a finished one: Finish moves the
	// recorded run as a driver would.
	if err := fake.Finish(order, workflow.StatusFailed); err != nil {
		t.Fatal(err)
	}
	if err := desk.Retry(ctx, order, stepPack); err != nil {
		t.Fatal(err)
	}
	fake.AssertReset(t, order, stepPack)
	if err := fake.Finish(order, workflow.StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	if _, err := desk.Redo(ctx, order, stepShip); err != nil {
		t.Fatal(err)
	}
	fork := fake.AssertForked(t, order, stepShip) // the fork's RunRef
	if st, _ := fake.Status(ctx, fork); st != workflow.StatusQueued {
		t.Errorf("fork status = %s, want queued", st)
	}

	if _, err := desk.ManagerOnline(ctx, "refunds"); err != nil {
		t.Fatal(err)
	}
	fake.AssertSignalledWaiting(t, managerOnline, "refunds")

	if err := desk.Refund(ctx, "refund-7", true, ""); err != nil {
		t.Fatal(err)
	}
	if err := desk.Refund(ctx, "refund-8", false, "used item"); err != nil {
		t.Fatal(err)
	}
	fake.AssertPromiseResolved(t, "refund-7", approved)
	fake.AssertPromiseRejected(t, "refund-8", "used item")
	fake.AssertPromisePending(t, "refund-9")

	if err := desk.ClearCart(ctx, "ada"); err != nil {
		t.Fatal(err)
	}
	fake.AssertObjectCalled(t, cartObject, "ada", methodClear)
	fake.AssertObjectNotCalled(t, cartObject, "grace")

	if _, err := desk.Reship(ctx, "order-7", "order-8"); err != nil {
		t.Fatal(err)
	}
	fake.AssertBatched(t,
		workflowtest.BatchSize(2), // the two orders Reship was given
		workflowtest.BatchHasJob(reshipWorkflow, workflowtest.HasPayloadJSON("order-8")),
		workflowtest.BatchWhere("no labels of the app's own", func(b workflowtest.FakeBatch) bool {
			return b.Jobs[0].Labels[labelBy] == ""
		}),
	)
}

// snippet:end

// snippet:refusals
// TestRefusedLikeADriver shows the Fake refusing what a driver refuses:
// an unfinished run cannot be forked, a run that has not failed cannot be
// reset, and a step the workflow does not have is invalid.
func TestRefusedLikeADriver(t *testing.T) {
	ctx := context.Background()
	fake, desk, err := newFake()
	if err != nil {
		t.Fatal(err)
	}
	order, err := fake.Enqueue(ctx, workflow.Job{Workflow: orderWorkflow})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := desk.Redo(ctx, order, stepShip); !errors.Is(err, workflow.ErrWrongState) {
		t.Errorf("fork of a queued run = %v, want ErrWrongState", err)
	}
	if err := desk.Retry(ctx, order, stepPack); !errors.Is(err, workflow.ErrWrongState) {
		t.Errorf("reset of a queued run = %v, want ErrWrongState", err)
	}
	if err := fake.Finish(order, workflow.StatusFailed); err != nil {
		t.Fatal(err)
	}
	if err := desk.Retry(ctx, order, "refund"); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("reset from an unknown step = %v, want ErrInvalid", err)
	}
	if len(fake.Forks())+len(fake.Resets()) != 0 {
		t.Errorf("refused calls were recorded: forks %v, resets %v", fake.Forks(), fake.Resets())
	}
}

// snippet:end
