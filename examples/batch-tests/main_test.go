package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/workflowtest"
)

// TestRun runs the program and checks the lines its README shows.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if want := "notified ada\npublish enqueued\n"; out.String() != want {
		t.Errorf("output:\n%s\nwant:\n%s", out.String(), want)
	}
}

func noop(context.Context, []byte) ([]byte, error) { return nil, nil }

func resizeOK(context.Context, string) error { return nil }

// snippet:batched
func TestUploadDispatchesOneBatch(t *testing.T) {
	ctx := context.Background()
	fake := workflowtest.NewFake()
	if err := Register(fake, resizeOK, noop); err != nil {
		t.Fatal(err)
	}
	if err := fake.Start(ctx); err != nil {
		t.Fatal(err)
	}
	g := &Gallery{Jobs: fake, Batches: batch.New(fake, batch.NewMemoryStore())}

	fake.AssertNothingBatched(t)
	if _, err := g.Upload(ctx, "ada", "a.png", "b.png", "c.png"); err != nil {
		t.Fatal(err)
	}
	fake.AssertBatchCount(t, 1)
	fake.AssertBatched(t,
		workflowtest.BatchSize(3),
		workflowtest.BatchHasJob("resize", workflowtest.HasPayload([]byte("b.png"))),
	)
	fake.AssertNotEnqueued(t, "notify") // nothing ran, so the batch has not finished
}

// snippet:end

// snippet:chained
func TestPublishRunsTheChain(t *testing.T) {
	ctx := context.Background()
	fake := workflowtest.NewFake()
	if err := Register(fake, resizeOK, noop); err != nil {
		t.Fatal(err)
	}
	if err := fake.Start(ctx); err != nil {
		t.Fatal(err)
	}
	g := &Gallery{Jobs: fake}
	if err := g.Publish(ctx, "summer"); err != nil {
		t.Fatal(err)
	}
	fake.AssertChained(t, "publish", "watermark", "cdn", "announce")
}

// snippet:end

// snippet:passthrough
// TestUploadNotifiesWhenResizesFinish runs the resize jobs for real on
// the memory driver and fakes everything else, so the test sees the
// batch's Then callback recorded once the real jobs ended.
func TestUploadNotifiesWhenResizesFinish(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fake := workflowtest.NewFake(workflowtest.FakeExcept(memory.New(), "resize"))
	if err := Register(fake, resizeOK, noop); err != nil {
		t.Fatal(err)
	}
	// The Fake reports no run ends, so the Manager polls Status, which
	// the Fake answers from the memory driver for the resize runs.
	m := batch.New(fake, batch.NewMemoryStore(), batch.WithStatusPoll(10*time.Millisecond))
	if err := m.Attach(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fake.Start(ctx); err != nil {
		t.Fatal(err)
	}
	g := &Gallery{Jobs: fake, Batches: m}
	if _, err := g.Upload(ctx, "ada", "a.png", "b.png"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(fake.Enqueued("notify")) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	fake.AssertEnqueued(t, "notify", workflowtest.HasLabel("owner", "ada"))
	fake.AssertNotEnqueued(t, "resize") // passed through, so not recorded
}

// snippet:end

// snippet:runstep
func TestResizeStepOutcomes(t *testing.T) {
	failWith := func(err error) func(context.Context, string) error {
		return func(context.Context, string) error { return err }
	}
	workflowtest.RunStep(ResizeStep(resizeOK), []byte("a.png")).AssertSucceeded(t)
	workflowtest.RunStep(ResizeStep(failWith(ErrRateLimited)), []byte("a.png")).AssertReleased(t, 30*time.Second)
	workflowtest.RunStep(ResizeStep(failWith(ErrBadImage)), []byte("a.txt")).AssertPermanent(t)
	workflowtest.RunStep(ResizeStep(failWith(errors.New("connection reset"))), []byte("a.png")).AssertFailed(t)
}

// snippet:end

// TestResizeStepSeesItsAttempt shows StepInfoOf, for a handler that reads
// its attempt.
func TestResizeStepSeesItsAttempt(t *testing.T) {
	var attempt int
	h := func(ctx context.Context, _ []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		attempt = info.Attempt
		return nil, nil
	}
	workflowtest.RunStep(h, nil, workflowtest.StepInfoOf(workflow.StepInfo{Workflow: "resize", Step: "run", RunID: "r1", Attempt: 3})).AssertSucceeded(t)
	if attempt != 3 {
		t.Errorf("attempt = %d, want 3", attempt)
	}
}

// snippet:ops-test
func TestOperations(t *testing.T) {
	ctx := context.Background()
	fake := workflowtest.NewFake()
	if err := Register(fake, resizeOK, noop); err != nil {
		t.Fatal(err)
	}
	if err := fake.Start(ctx); err != nil {
		t.Fatal(err)
	}
	g := &Gallery{Jobs: fake}
	admin := &Admin{Jobs: fake}

	// A reprocess overrides the workflow's queue and its retry budget.
	ref, err := g.Reprocess(ctx, "a.png")
	if err != nil {
		t.Fatal(err)
	}
	fake.AssertEnqueuedOn(t, "bulk", "resize", workflowtest.HasRetry(workflow.RetryPolicy{Max: 10}))

	if err := admin.StartMaintenance(ctx); err != nil {
		t.Fatal(err)
	}
	fake.AssertPaused(t, workflow.PauseQueue("images"))

	// The Fake runs nothing: Finish models the run failing.
	if err := fake.Finish(ref, workflow.StatusFailed); err != nil {
		t.Fatal(err)
	}
	if err := admin.Requeue(ctx, ref); err != nil {
		t.Fatal(err)
	}
	fake.AssertRetried(t, ref)

	if err := fake.Finish(ref, workflow.StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	if err := admin.Forget(ctx, ref); err != nil {
		t.Fatal(err)
	}
	fake.AssertDeleted(t, ref)
}

// snippet:end

// TestUploadsRunOnTheImagesQueue checks the workflow's own queue.
func TestUploadsRunOnTheImagesQueue(t *testing.T) {
	ctx := context.Background()
	fake := workflowtest.NewFake()
	if err := Register(fake, resizeOK, noop); err != nil {
		t.Fatal(err)
	}
	if err := fake.Start(ctx); err != nil {
		t.Fatal(err)
	}
	g := &Gallery{Jobs: fake, Batches: batch.New(fake, batch.NewMemoryStore())}
	if _, err := g.Upload(ctx, "ada", "a.png"); err != nil {
		t.Fatal(err)
	}
	fake.AssertEnqueuedOn(t, "images", "resize")
}
