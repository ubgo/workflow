package batch_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
	"github.com/ubgo/workflow/memory"
)

// runPartial starts the first two jobs and then fails without refs (the
// GroupRunner interface allows nil refs on an error), as a bulk trigger
// cut off part-way would.
func runPartial(ctx context.Context, c workflow.Client, jobs []workflow.Job, started *[]workflow.RunRef) ([]workflow.RunRef, error) {
	for _, j := range jobs[:2] {
		ref, err := c.Enqueue(ctx, j)
		if err != nil {
			return nil, err
		}
		*started = append(*started, ref)
	}
	return nil, errors.New("engine dropped the rest of the group")
}

// partialGroup is a memory client (CancelAll kept) with that RunGroup.
type partialGroup struct {
	*memory.Client
	started []workflow.RunRef
}

func (g *partialGroup) RunGroup(ctx context.Context, _ string, jobs []workflow.Job) ([]workflow.RunRef, error) {
	return runPartial(ctx, g.Client, jobs, &g.started)
}

// partialNoBulk is the same RunGroup on a client without CancelAll and
// CapCancelAll.
type partialNoBulk struct {
	noBulkCancel
	started []workflow.RunRef
}

func (g *partialNoBulk) RunGroup(ctx context.Context, _ string, jobs []workflow.Job) ([]workflow.RunRef, error) {
	return runPartial(ctx, g.noBulkCancel, jobs, &g.started)
}

// noBulkCancel hides the memory client's CancelAll and CapCancelAll.
type noBulkCancel struct{ workflow.Client }

func (n noBulkCancel) Capabilities() workflow.Caps {
	return n.Client.Capabilities().Without(workflow.CapCancelAll)
}

// startHolding starts a memory client whose "hold" workflow runs until
// cancelled.
func startHolding(t *testing.T) *memory.Client {
	t.Helper()
	c := memory.New()
	if err := c.Register(workflow.Workflow{Name: "hold", Steps: []workflow.Step{{Name: "wait", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return c
}

// TestAbortAfterPartialGroupCancelsStartedChildren: RunGroup fails after
// starting two of three jobs and returns no refs. The batch cannot place
// those children on its jobs, so it cancels them by their batch label
// (CapCancelAll) instead of recording every job cancelled while the
// started ones run on unseen.
func TestAbortAfterPartialGroupCancelsStartedChildren(t *testing.T) {
	c := startHolding(t)
	g := &partialGroup{Client: c}
	m := batch.New(g, batch.NewMemoryStore(), batch.WithCompletionSource())
	_, err := m.Dispatch(context.Background(), batch.Batch{Jobs: []workflow.Job{{Workflow: "hold"}, {Workflow: "hold"}, {Workflow: "hold"}}})
	if err == nil {
		t.Fatal("Dispatch succeeded although the group failed")
	}
	if len(g.started) != 2 {
		t.Fatalf("%d children started, want 2", len(g.started))
	}
	for i, ref := range g.started {
		deadline := time.Now().Add(5 * time.Second)
		for {
			st, serr := c.Status(context.Background(), ref)
			if serr != nil {
				t.Fatal(serr)
			}
			if st == workflow.StatusCancelled {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("started child %d is %s after the aborted dispatch, want cancelled (error: %v)", i, st, err)
			}
			time.Sleep(time.Millisecond)
		}
	}
}

// TestAbortAfterPartialGroupSaysWhenItCannotCancel: on a client without
// CapCancelAll the started children cannot be found, and Dispatch's error
// says so with an UnsupportedError instead of hiding it.
func TestAbortAfterPartialGroupSaysWhenItCannotCancel(t *testing.T) {
	c := startHolding(t)
	g := &partialNoBulk{noBulkCancel: noBulkCancel{c}}
	m := batch.New(g, batch.NewMemoryStore(), batch.WithCompletionSource())
	_, err := m.Dispatch(context.Background(), batch.Batch{Jobs: []workflow.Job{{Workflow: "hold"}, {Workflow: "hold"}, {Workflow: "hold"}}})
	var ue *workflow.UnsupportedError
	if !errors.As(err, &ue) || !ue.Missing.Has(workflow.CapCancelAll) {
		t.Fatalf("Dispatch error %v; want an UnsupportedError naming CapCancelAll", err)
	}
}

// TestAbortCancelsByLabelOnlyWhenRefsAreUnknown: a group that reports a
// ref per job (zero for a job that did not start) and the one-Enqueue-
// per-job path both know every started child, so an abort cancels by ref
// and asks nothing of CancelAll: on a client without it there is no
// UnsupportedError.
func TestAbortCancelsByLabelOnlyWhenRefsAreUnknown(t *testing.T) {
	var ue *workflow.UnsupportedError
	positional := &groupStub{retryStub: newRetryStub(), err: errors.New("third job refused"), refs: func(jobs []workflow.Job) []workflow.RunRef {
		refs := allRefs(jobs)
		refs[2] = workflow.RunRef{}
		return refs
	}}
	_, err := batch.New(positional, batch.NewMemoryStore()).Dispatch(context.Background(), batch.Batch{Jobs: []workflow.Job{{Workflow: "a"}, {Workflow: "a"}, {Workflow: "a"}}})
	if err == nil || errors.As(err, &ue) {
		t.Fatalf("positional refs: Dispatch error %v; want the group error and no UnsupportedError", err)
	}
	if len(positional.cancelled) != 2 {
		t.Fatalf("positional refs: %d started children cancelled by ref, want 2", len(positional.cancelled))
	}
	perJob := newStub(workflow.Caps{})
	perJob.failAt = 2
	_, err = batch.New(perJob, batch.NewMemoryStore()).Dispatch(context.Background(), batch.Batch{Jobs: []workflow.Job{{Workflow: "a"}, {Workflow: "a"}, {Workflow: "a"}}})
	if err == nil || errors.As(err, &ue) {
		t.Fatalf("per-job path: Dispatch error %v; want the enqueue error and no UnsupportedError", err)
	}
}

// partialNoStates is partialGroup on a driver without CapRunStates
// (Hatchet): its CancelAll refuses StatusRetrying, a status it lacks.
type partialNoStates struct{ *partialGroup }

func (g partialNoStates) Capabilities() workflow.Caps {
	return g.partialGroup.Capabilities().Without(workflow.CapRunStates)
}

func (g partialNoStates) CancelAll(ctx context.Context, q workflow.RunQuery) (int, error) {
	if slices.Contains(q.Statuses, workflow.StatusRetrying) {
		return 0, &workflow.UnsupportedError{Field: "RunQuery.Statuses", Missing: workflow.CapRunStates, Reason: "no retrying status"}
	}
	return g.partialGroup.CancelAll(ctx, q)
}

// TestAbortAfterPartialGroupWithoutRunStates: the label cancel asks only
// for statuses the client has, so a driver without StatusRetrying cancels
// the started children instead of refusing.
func TestAbortAfterPartialGroupWithoutRunStates(t *testing.T) {
	g := partialNoStates{&partialGroup{Client: startHolding(t)}}
	_, err := batch.New(g, batch.NewMemoryStore(), batch.WithCompletionSource()).Dispatch(context.Background(), batch.Batch{Jobs: []workflow.Job{{Workflow: "hold"}, {Workflow: "hold"}, {Workflow: "hold"}}})
	var ue *workflow.UnsupportedError
	if err == nil || errors.As(err, &ue) {
		t.Fatalf("Dispatch error %v; want the group error only", err)
	}
}
