# Test queues, retries and pauses

App and operator code does more than enqueue: it sends a job to a named queue, gives one run a larger retry budget, pauses a queue during maintenance, retries a failed run and forgets a finished one. `workflowtest.Fake` validates and records each of these, with assertions to check them, so the code is tested without a worker.

## Before you start

- App code that takes a `workflow.Client` and reaches the optional interfaces (`workflow.Pauser`, `workflow.RunRetrier`, `workflow.RunDeleter`) by type assertion, so the Fake can stand in for a driver.
- The runnable program for this page is [examples/batch-tests](../../examples/batch-tests/).

The app code under test reprocesses a file on the `bulk` queue with ten retries, and gives operators three calls:

<!-- snippet: examples/batch-tests/main.go#ops -->
```go
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
```

## Step 1: assert the queue and the retry budget

`fake.AssertEnqueuedOn(t, queue, wf, matchers...)` passes when a job of wf matching every matcher runs on queue. The queue is the effective one (`workflow.EffectiveQueue`): `Job.Queue`, else the registered workflow's `Queue`, else `workflow.DefaultQueue`, so a job that names no queue is checked against its workflow's.

Two job matchers cover the new job fields:

| Matcher | Matches a job |
|---|---|
| `HasQueue(q)` | whose `Job.Queue` is q as given (empty means the workflow's queue) |
| `HasRetry(policy)` | whose `Job.Retry` override equals policy |

The Fake also refuses these fields as a driver does: `workflow.ValidateJob` (a bad queue name, a `Job.Retry` that carries functions), `workflow.CheckJob` (a field whose capability the Fake was built without, such as `CapJobRetry`), and `workflow.CheckJobQueue` (a queue other than the workflow's without `CapQueueOverride`).

## Step 2: assert pauses, retries and deletes

The Fake implements `Pauser`, `RunRetrier` and `RunDeleter` itself and records each call:

<!-- snippet: examples/batch-tests/main_test.go#ops-test -->
```go
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
```

| Call | What the Fake does | Assertion |
|---|---|---|
| `Pause(t)`, `Resume(t)` | checks the target (`workflow.CheckPause`) and records it; `Resume(PauseAll())` clears every pause; `Paused` lists them in the driver order | `AssertPaused(t, target)`, `AssertNotPaused(t, target)` |
| `Retry(ref)` | needs a run that `Finish` moved to failed (else `ErrWrongState`), takes its idempotency key again (`ErrDuplicate` when another run holds it) and puts it back to queued under the same ref | `AssertRetried(t, ref)`, `Retries()` |
| `Delete(ref)` | needs a finished run (else `ErrWrongState`); `Status` then returns `ErrNotFound`, while the job stays in `Jobs` | `AssertDeleted(t, ref)`, `Deletes()` |

Nothing runs on the Fake, so a pause changes no status; it is recorded to be asserted. A pause of a workflow passed through with `FakeExcept` or `FakeOnly`, and a retry or delete of a run the Fake did not create, go to the real client instead.

The [Recorder](recorder.md) has the same assertions over a real client: its `Retry` and `Delete` forward and record each call that succeeded, and its `AssertPaused` reads the real client's `Paused`.

## Try it

```sh
cd examples
go test -v -run 'Operations|ImagesQueue' ./batch-tests
```

```text
=== RUN   TestOperations
--- PASS: TestOperations (0.00s)
=== RUN   TestUploadsRunOnTheImagesQueue
--- PASS: TestUploadsRunOnTheImagesQueue (0.00s)
PASS
ok  	github.com/ubgo/workflow/examples/batch-tests	0.008s
```

## Pitfalls

- **Capabilities follow the production driver.** A default Fake claims what the memory driver claims. Build it with `FakeLike(productionClient)` so a test fails when the code pauses a queue or overrides a queue on a driver that cannot (Hatchet has no `CapPauseQueue`, `CapQueueOverride` or `CapJobRetry`).
- **`FakeLike` drops what the Fake does not serve.** The run query, bulk cancel, schedule pause, schedule listing, tick and drain capabilities are never claimed, because the Fake answers those interfaces only by forwarding to a pass-through client; test that code on the memory driver.
- **`HasQueue("")` is not the default queue.** It matches a job that named no queue; use `AssertEnqueuedOn` with `workflow.DefaultQueue` to check where the run lands.
- **Retry needs a failed run.** The Fake never fails a run by itself; call `Finish(ref, workflow.StatusFailed)` first, as the example does.

## Related

- [Test app code with the Fake](fake.md): the Fake's refusals, job assertions and matchers.
- [Test batches, chains and a subset of workflows](batches-and-chains.md): batch and chain assertions, pass-through.
- [Test one step](run-step.md): `RunStep` for a handler's outcome.
- [Record a real client](recorder.md): the same assertions over a driver that runs the jobs.
- Example: [examples/batch-tests](../../examples/batch-tests/).
