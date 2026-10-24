// Command batch-tests holds app code that dispatches batches, enqueues a
// chain, overrides a job's queue and retry budget, pauses, retries and
// forgets runs, and defines a step that releases on rate limits; its
// main_test.go tests that code with workflowtest.Fake (AssertBatched,
// AssertChained, AssertEnqueuedOn, AssertPaused, AssertRetried,
// AssertDeleted, pass-through with FakeExcept) and workflowtest.RunStep,
// without running a worker. main runs the same code on the in-memory
// driver.
//
// It is the program behind docs/testing/batches-and-chains.md,
// docs/testing/operations.md and docs/testing/run-step.md: the guides copy its regions (marked
// "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
	"github.com/ubgo/workflow/memory"
)

// rateLimitWait is how long the resize step asks to wait after the image
// service answers 429, as its Retry-After header would say.
const rateLimitWait = 30 * time.Second

// Errors the image service returns.
var (
	ErrRateLimited = errors.New("image service: too many requests")
	ErrBadImage    = errors.New("image service: not an image")
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "batch-tests:", err)
		os.Exit(1)
	}
}

// snippet:app
// Gallery is app code: it depends on workflow.Client and a batch.Manager,
// never on a driver, so a test can hand it a Fake.
type Gallery struct {
	Jobs    workflow.Client
	Batches *batch.Manager
}

// Upload resizes every file in one batch and notifies the owner when all
// are done.
func (g *Gallery) Upload(ctx context.Context, owner string, files ...string) (batch.ID, error) {
	b := batch.Batch{Name: "upload", Then: "notify", Labels: map[string]string{"owner": owner}}
	for _, f := range files {
		b.Jobs = append(b.Jobs, workflow.Job{Workflow: "resize", Payload: []byte(f)})
	}
	return g.Batches.Dispatch(ctx, b)
}

// Publish runs the publish chain for an album.
func (g *Gallery) Publish(ctx context.Context, album string) error {
	_, err := g.Jobs.Enqueue(ctx, workflow.Job{Workflow: "publish", Payload: []byte(album)})
	return err
}

// PublishWorkflow is a chain: watermark, then upload to the CDN, then
// announce.
func PublishWorkflow(step workflow.Handler) workflow.Workflow {
	return workflow.Workflow{Name: "publish", Steps: []workflow.Step{
		{Name: "watermark", Run: step},
		{Name: "cdn", After: []string{"watermark"}, Run: step},
		{Name: "announce", After: []string{"cdn"}, Run: step},
	}}
}

// snippet:end

// snippet:ops
// imagesQueue is where resize jobs run, so operators can pause image work
// without pausing anything else.
const imagesQueue = "images"

// reprocessRetries is the retry budget of a manual reprocess, higher than
// the workflow's own because an operator is waiting for it to succeed.
const reprocessRetries = 10

// Reprocess resizes one file again on the bulk queue with a larger retry
// budget than an upload gets.
func (g *Gallery) Reprocess(ctx context.Context, file string) (workflow.RunRef, error) {
	return g.Jobs.Enqueue(ctx, workflow.Job{
		Workflow: "resize",
		Payload:  []byte(file),
		Queue:    "bulk",
		Retry:    &workflow.RetryPolicy{Max: reprocessRetries},
	})
}

// Admin is operator code: maintenance pauses image work, a failed run is
// retried from its failed step, and a finished run is forgotten. Each
// call needs an optional client interface, so a driver without it fails
// loudly with workflow.ErrUnsupported.
type Admin struct{ Jobs workflow.Client }

// StartMaintenance pauses the images queue; runs on it wait, nothing is
// cancelled.
func (a *Admin) StartMaintenance(ctx context.Context) error {
	p, ok := a.Jobs.(workflow.Pauser)
	if !ok {
		return workflow.Unsupported("Pause", "the client is not a workflow.Pauser")
	}
	return p.Pause(ctx, workflow.PauseQueue(imagesQueue))
}

// Requeue retries a failed run in place, from its failed step; the run
// keeps its RunRef.
func (a *Admin) Requeue(ctx context.Context, ref workflow.RunRef) error {
	r, ok := a.Jobs.(workflow.RunRetrier)
	if !ok {
		return workflow.Unsupported("Retry", "the client is not a workflow.RunRetrier")
	}
	_, err := r.Retry(ctx, ref)
	return err
}

// Forget deletes a finished run's record.
func (a *Admin) Forget(ctx context.Context, ref workflow.RunRef) error {
	d, ok := a.Jobs.(workflow.RunDeleter)
	if !ok {
		return workflow.Unsupported("Delete", "the client is not a workflow.RunDeleter")
	}
	return d.Delete(ctx, ref)
}

// snippet:end

// snippet:step
// ResizeStep calls the image service. A 429 releases the job for
// rateLimitWait, a file that is not an image fails it for good, and any
// other error is retried on the workflow's backoff.
func ResizeStep(resize func(ctx context.Context, file string) error) workflow.Handler {
	return func(ctx context.Context, in []byte) ([]byte, error) {
		err := resize(ctx, string(in))
		switch {
		case errors.Is(err, ErrRateLimited):
			return nil, workflow.RetryAfter(err, rateLimitWait)
		case errors.Is(err, ErrBadImage):
			return nil, workflow.Permanent(err)
		}
		return nil, err
	}
}

// snippet:end

// Register registers the app's workflows on c with step as the body of
// every step except resize, which runs resize.
func Register(c workflow.Client, resize func(context.Context, string) error, step workflow.Handler) error {
	for _, wf := range []workflow.Workflow{
		{Name: "resize", Queue: imagesQueue, Steps: []workflow.Step{{Name: "run", Run: ResizeStep(resize)}}},
		{Name: "notify", Steps: []workflow.Step{{Name: "run", Run: step}}},
		PublishWorkflow(step),
	} {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	return nil
}

// run is main without the process exit: the app on the memory driver.
func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	c := memory.New()
	notified := make(chan string, 1)
	notify := func(ctx context.Context, _ []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		if info.Workflow == "notify" {
			notified <- info.Labels["owner"]
		}
		return nil, nil
	}
	if err := Register(c, func(context.Context, string) error { return nil }, notify); err != nil {
		return err
	}
	m := batch.New(c, batch.NewMemoryStore(), batch.WithManualCallbackRetry())
	if err := m.Attach(ctx); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	g := &Gallery{Jobs: c, Batches: m}
	if _, err := g.Upload(ctx, "ada", "a.png", "b.png"); err != nil {
		return err
	}
	fmt.Fprintln(out, "notified", <-notified)
	if err := g.Publish(ctx, "summer"); err != nil {
		return err
	}
	fmt.Fprintln(out, "publish enqueued")
	return nil
}
