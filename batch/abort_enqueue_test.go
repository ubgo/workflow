package batch_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
)

// landsThenFails is a memory client (CancelAll kept) whose Enqueue of the
// job at index failAt lands on the engine and still returns an error, as a
// timeout after the engine accepted the job does. It is not a
// GroupRunner, so Dispatch takes the one-Enqueue-per-job path.
type landsThenFails struct {
	workflow.Client
	calls  int
	failAt int
	landed workflow.RunRef
}

func (l *landsThenFails) Enqueue(ctx context.Context, j workflow.Job) (workflow.RunRef, error) {
	ref, err := l.Client.Enqueue(ctx, j)
	l.calls++
	if l.calls-1 == l.failAt && err == nil {
		l.landed = ref
		return workflow.RunRef{}, errors.New("enqueue timed out after the engine accepted it")
	}
	return ref, err
}

func (l *landsThenFails) CancelAll(ctx context.Context, q workflow.RunQuery) (int, error) {
	return l.Client.(workflow.BulkCanceller).CancelAll(ctx, q)
}

// TestAbortAfterLandedEnqueueCancelsIt: on the per-job path an Enqueue
// that returned an error may still have started its run. Dispatch cannot
// know its ref, so it cancels the batch's runs by their LabelBatch label
// when the client has CapCancelAll, instead of recording the job
// cancelled while the run goes on unseen.
func TestAbortAfterLandedEnqueueCancelsIt(t *testing.T) {
	c := startHolding(t)
	l := &landsThenFails{Client: c, failAt: 1}
	m := batch.New(l, batch.NewMemoryStore(), batch.WithCompletionSource())
	_, err := m.Dispatch(context.Background(), batch.Batch{Jobs: []workflow.Job{{Workflow: "hold"}, {Workflow: "hold"}, {Workflow: "hold"}}})
	if err == nil {
		t.Fatal("Dispatch succeeded although an Enqueue failed")
	}
	var ue *workflow.UnsupportedError
	if errors.As(err, &ue) {
		t.Fatalf("Dispatch error %v; want no UnsupportedError on a client with CapCancelAll", err)
	}
	if l.landed.IsZero() {
		t.Fatal("the failing Enqueue did not land; the test proves nothing")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, serr := c.Status(context.Background(), l.landed)
		if serr != nil {
			t.Fatal(serr)
		}
		if st == workflow.StatusCancelled {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the run whose Enqueue errored but landed is %s after the aborted dispatch, want cancelled (error: %v)", st, err)
		}
		time.Sleep(time.Millisecond)
	}
}
