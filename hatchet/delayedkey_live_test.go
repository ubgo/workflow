package hatchet_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/hatchet"
)

// TestDelayedKeyLive proves a delayed start with an idempotency key on the
// engine: Enqueue takes it (Hatchet's scheduled runs take no key, so the
// run starts at once and its start gate sleeps until RunAt), the run reads
// StatusScheduled with its RunAt and no StartedAt while it waits, a second
// Enqueue with the key returns the same RunRef, a scheduled-only run query
// lists it, its step starts no earlier than RunAt and soon after it, and a
// waiting one cannot be rescheduled (UnsupportedError) but is cancelled
// without its step running, which frees its key.
// A workflow with a Concurrency refuses the pair. A run started at once
// still holds its key while its step runs, and an immediate Enqueue with
// a waiting delayed start's key returns that start.
func TestDelayedKeyLive(t *testing.T) {
	skipWithoutEngine(t)
	prefix := "wfdelaykey_" + uuid.NewString()[:8]
	var mu sync.Mutex
	started := map[string]time.Time{}
	step := func(ctx context.Context, in []byte) ([]byte, error) {
		mu.Lock()
		started[string(in)] = time.Now()
		mu.Unlock()
		return in, nil
	}
	wf := workflow.Workflow{Name: prefix, Steps: []workflow.Step{{Name: "work", Run: step}}}
	conc := workflow.Workflow{Name: prefix + "_conc", Concurrency: &workflow.Concurrency{MaxRuns: 1, Key: workflow.KeyExpr(`"all"`)}, Steps: []workflow.Step{{Name: "work", Run: step}}}
	c, _ := startLive(t, hatchet.Config{WorkerName: prefix}, wf, conc)
	ctx := context.Background()

	runAt := time.Now().Add(8 * time.Second)
	job := workflow.Job{Workflow: wf.Name, Payload: []byte(`"soon"`), RunAt: runAt, IdempotencyKey: prefix + "-soon"}
	ref, err := c.Enqueue(ctx, job)
	if err != nil {
		t.Fatalf("Enqueue of a delayed start with a key = %v", err)
	}
	if ref.RunID.IsZero() {
		t.Fatalf("RunRef = %+v, want the RunID of the run started at once", ref)
	}
	waitStatus(t, c, ref, workflow.StatusScheduled)
	page, err := c.Runs(ctx, workflow.RunQuery{Ref: ref})
	if err != nil || len(page.Runs) != 1 {
		t.Fatalf("Runs(ref) = %+v, %v", page, err)
	}
	info := page.Runs[0]
	if info.Status != workflow.StatusScheduled || info.RunAt.UnixMilli() != runAt.UnixMilli() || !info.StartedAt.IsZero() || info.Attempts != 0 {
		t.Errorf("RunInfo while waiting = status %s, RunAt %v, StartedAt %v, attempts %d; want scheduled, %v, zero, 0", info.Status, info.RunAt, info.StartedAt, info.Attempts, runAt)
	}
	octx, outcome := workflow.WithEnqueueOutcome(ctx)
	dup, err := c.Enqueue(octx, job)
	if err != nil || dup != ref {
		t.Fatalf("second Enqueue with the key = %+v, %v; want %+v", dup, err, ref)
	}
	if !outcome.Reported || !outcome.Duplicate {
		t.Errorf("second Enqueue with the key: outcome %+v, want a duplicate", outcome)
	}
	listed, err := c.Runs(ctx, workflow.RunQuery{Workflow: wf.Name, Statuses: []workflow.Status{workflow.StatusScheduled}})
	if err != nil || len(listed.Runs) != 1 || listed.Runs[0].Ref.RunID != ref.RunID {
		t.Fatalf("Runs(scheduled) = %+v, %v; want the waiting run", listed.Runs, err)
	}
	waitStatus(t, c, ref, workflow.StatusSucceeded)
	mu.Lock()
	at := started[`"soon"`]
	mu.Unlock()
	if at.Before(runAt) {
		t.Errorf("the step started %s before RunAt", runAt.Sub(at))
	}
	if late := at.Sub(runAt); late > 5*time.Second {
		t.Errorf("the step started %s after RunAt, want within 5s", late)
	}
	page, err = c.Runs(ctx, workflow.RunQuery{Ref: ref})
	if err != nil || len(page.Runs) != 1 || page.Runs[0].StartedAt.Before(runAt.Add(-time.Second)) {
		t.Errorf("RunInfo after the run = %+v, %v; want StartedAt at or after RunAt", page.Runs, err)
	}

	far := workflow.Job{Workflow: wf.Name, Payload: []byte(`"far"`), RunAt: time.Now().Add(time.Hour), IdempotencyKey: prefix + "-far"}
	farRef, err := c.Enqueue(ctx, far)
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, farRef, workflow.StatusScheduled)
	var ue *workflow.UnsupportedError
	if _, err := c.Reschedule(ctx, farRef, time.Now().Add(2*time.Hour)); !errors.As(err, &ue) || ue.Field != "Reschedule" {
		t.Errorf("Reschedule of a waiting keyed delayed start: err = %v, want UnsupportedError naming Reschedule", err)
	}
	if err := c.Cancel(ctx, farRef); err != nil {
		t.Fatalf("Cancel of a waiting delayed start = %v", err)
	}
	waitStatus(t, c, farRef, workflow.StatusCancelled)
	again, err := c.Enqueue(ctx, far)
	if err != nil || again == farRef {
		t.Errorf("Enqueue with the cancelled run's key = %+v, %v; want a new run (the key is held only while a run is unfinished)", again, err)
	} else if err := c.Cancel(ctx, again); err != nil {
		t.Errorf("Cancel = %v", err)
	}
	mu.Lock()
	_, ran := started[`"far"`]
	mu.Unlock()
	if ran {
		t.Error("the cancelled delayed start ran its step")
	}

	_, err = c.Enqueue(ctx, workflow.Job{Workflow: conc.Name, RunAt: time.Now().Add(time.Hour), IdempotencyKey: prefix + "-conc"})
	if !errors.As(err, &ue) || ue.Field != "Job.IdempotencyKey" {
		t.Errorf("a keyed delayed start of a workflow with a Concurrency: err = %v, want UnsupportedError naming Job.IdempotencyKey", err)
	}

	// A run started at once keeps its key while its step runs: the
	// start gate lives only on the delayed variant, because the engine
	// releases a key when the first task created with the run ends.
	release := make(chan struct{})
	holding := make(chan struct{}, 1)
	hold := workflow.Workflow{Name: prefix + "_hold", Steps: []workflow.Step{{Name: "work", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		holding <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return in, nil
	}}}}
	c3, _ := startLive(t, hatchet.Config{WorkerName: prefix + "_hold"}, hold)
	holdRef, err := c3.Enqueue(ctx, workflow.Job{Workflow: hold.Name, IdempotencyKey: prefix + "-hold"})
	if err != nil {
		t.Fatal(err)
	}
	<-holding
	waitStatus(t, c3, holdRef, workflow.StatusRunning)
	if again, err := c3.Enqueue(ctx, workflow.Job{Workflow: hold.Name, IdempotencyKey: prefix + "-hold"}); err != nil || again != holdRef {
		t.Errorf("Enqueue with the key of a running run = %+v, %v; want %+v (the key held while the run is unfinished)", again, err, holdRef)
	}
	// An immediate Enqueue with a waiting delayed start's key returns
	// that start: the variant shares the workflow's keys.
	waitingRef, err := c3.Enqueue(ctx, workflow.Job{Workflow: hold.Name, RunAt: time.Now().Add(time.Hour), IdempotencyKey: prefix + "-wait"})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c3, waitingRef, workflow.StatusScheduled)
	if now, err := c3.Enqueue(ctx, workflow.Job{Workflow: hold.Name, IdempotencyKey: prefix + "-wait"}); err != nil || now != waitingRef {
		t.Errorf("immediate Enqueue with a waiting delayed start's key = %+v, %v; want %+v", now, err, waitingRef)
	}
	close(release)
	if err := c3.Cancel(ctx, waitingRef); err != nil {
		t.Errorf("Cancel = %v", err)
	}
	waitStatus(t, c3, holdRef, workflow.StatusSucceeded)
}
