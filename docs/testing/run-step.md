# Test one step

A step handler decides how its attempt ends: success, an ordinary failure the retry policy retries, a release that asks for a retry after a set delay ([`workflow.RetryAfter`](ds:block?id=retryafter-xck6cdyv)), a permanent failure that is never retried ([`workflow.Permanent`](ds:block?id=permanent-9e6vjm6s)), or a discard that drops the run (`workflow.Discard`). [`workflowtest.RunStep`](ds:block?id=workflowtest-runstep-zsng78k5) calls a handler once, with no client and no driver, and returns that outcome with checks for each case.

## Before you start

- A step handler you can build without a client, such as a function that returns a [`workflow.Handler`](ds:block?id=handler-82jhj4f7) from its dependencies.
- The runnable program for this page is [examples/batch-tests](../../examples/batch-tests/).

The handler under test calls an image service; a 429 releases the job for 30 seconds and a file that is not an image fails it for good:

<!-- snippet: examples/batch-tests/main.go#step -->
```go
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
```

## Step 1: run the handler and check the outcome

`RunStep(handler, input, opts...)` returns a [`workflowtest.StepOutcome`](ds:block?id=workflowtest-stepoutcome-753swc5f) with the handler's `Output` and `Err`:

<!-- snippet: examples/batch-tests/main_test.go#runstep -->
```go
func TestResizeStepOutcomes(t *testing.T) {
	failWith := func(err error) func(context.Context, string) error {
		return func(context.Context, string) error { return err }
	}
	workflowtest.RunStep(ResizeStep(resizeOK), []byte("a.png")).AssertSucceeded(t)
	workflowtest.RunStep(ResizeStep(failWith(ErrRateLimited)), []byte("a.png")).AssertReleased(t, 30*time.Second)
	workflowtest.RunStep(ResizeStep(failWith(ErrBadImage)), []byte("a.txt")).AssertPermanent(t)
	workflowtest.RunStep(ResizeStep(failWith(errors.New("connection reset"))), []byte("a.png")).AssertFailed(t)
}
```

| Check | Passes when the attempt |
|---|---|
| [`AssertSucceeded(t)`](ds:block?id=workflowtest-stepoutcome-assertsucceeded-u77vv7gv) | returned no error |
| [`AssertFailed(t)`](ds:block?id=workflowtest-stepoutcome-assertfailed-73snx492) | returned an error that is neither permanent nor a release: one the retry policy retries |
| [`AssertPermanent(t)`](ds:block?id=workflowtest-stepoutcome-assertpermanent-kj8e4262) | returned `workflow.Permanent(err)`: no retry |
| [`AssertDiscarded(t)`](ds:block?id=workflowtest-stepoutcome-assertdiscarded-kqacfetq) | returned `workflow.Discard(err)`: drop the run, which ends discarded (cancelled without `CapRunStates`) rather than failed on a driver with `CapDiscard` ([`workflow.EndStatus`](ds:block?id=endstatus-d3uc8s8n)) |
| [`AssertReleased(t, d)`](ds:block?id=workflowtest-stepoutcome-assertreleased-nzcxqexr) | returned `workflow.RetryAfter(err, d)` with exactly d |

The same questions answer without failing the test: `Succeeded()`, [`Failed()`](ds:block?id=workflowtest-stepoutcome-failed-jd76dhf7) (any error), `Permanent()`, `Discarded()` and [`Released()`](ds:block?id=workflowtest-stepoutcome-released-zjsf6mru), which returns the delay and whether there was one. `Permanent` and `Discard` win over [`RetryAfter`](ds:block?id=retryafter-xck6cdyv), as on every driver: `RetryAfter(Permanent(err), d)` is permanent, not released.

## Step 2: give the handler its context

The handler runs on a ctx carrying `workflow.StepInfo`: by default ([`RunStep`](ds:block?id=workflowtest-runstep-zsng78k5)) workflow `test`, step `step`, run `runstep`, attempt 1. [`StepInfoOf(info)`](ds:block?id=workflowtest-stepinfoof-ex2r47gt) sets it, for a handler that reads its attempt, run or labels; [`StepContext(ctx)`](ds:block?id=workflowtest-stepcontext-qvktt9hm) sets the parent ctx; and [`StepMiddleware(mw...)`](ds:block?id=workflowtest-stepmiddleware-3gea9j4h) wraps the handler in the middleware a driver would, outermost first.

```go
workflowtest.RunStep(h, nil, workflowtest.StepInfoOf(workflow.StepInfo{Workflow: "resize", Step: "run", RunID: "r1", Attempt: 3}))
```

A handler that edits its [chain](../workflows/chains.md) with `PrependToChain` or `AppendToChain` gets an editor on its ctx. [`StepChain(jobs...)`](ds:block?id=workflowtest-stepchain-b2p6dt3z) sets the chain the run already holds, [`StepCaps(caps)`](ds:block?id=workflowtest-stepcaps-4qqyy6u2) sets the capabilities the edits are checked against (the default includes `CapChain`; leave it out to see the refusal a driver without chains gives), and the outcome reports the chain after the attempt in `StepOutcome.Chain`, with [`ChainNames()`](ds:block?id=workflowtest-stepoutcome-chainnames-7zn22fv7) listing the workflows in the order they would run. A failed attempt leaves the chain unchanged, as a driver drops a failed attempt's edits: [`RunStep`](ds:block?id=workflowtest-runstep-zsng78k5) applies them only when the attempt succeeds.

<!-- snippet: examples/chain-edits/main_test.go#runstep-chain -->
```go
func TestScanEditsTheChain(t *testing.T) {
	scan := func(ctx context.Context, _ []byte) ([]byte, error) {
		if err := workflow.PrependToChain(ctx, workflow.Job{Workflow: "import-file"}); err != nil {
			return nil, err
		}
		return nil, workflow.AppendToChain(ctx, workflow.Job{Workflow: "reindex"})
	}
	out := workflowtest.RunStep(scan, nil, workflowtest.StepChain(workflow.Job{Workflow: "notify"}))
	out.AssertSucceeded(t)
	if got := strings.Join(out.ChainNames(), " -> "); got != "import-file -> notify -> reindex" {
		t.Errorf("chain after the step: %s", got)
	}
}
```

## Try it

```sh
cd examples
go test -v -run ResizeStep ./batch-tests
```

```text
=== RUN   TestResizeStepOutcomes
--- PASS: TestResizeStepOutcomes (0.00s)
=== RUN   TestResizeStepSeesItsAttempt
--- PASS: TestResizeStepSeesItsAttempt (0.00s)
PASS
ok  	github.com/ubgo/workflow/examples/batch-tests	0.030s
```

## Pitfalls

- **A release is honoured only under a policy that reads it.** A workflow's retry policy must set `DelayFor:` [`workflow.RetryAfterDelay`](ds:block?id=retryafterdelay-pgsc6bvn) (and the driver must have `CapRetryDelay`) for the delay to apply; under any other policy the release retries on the normal backoff ([`workflow.RetryAfter`](ds:block?id=retryafter-xck6cdyv)). `RunStep` checks what the handler asked for, not what a driver will do.
- **One attempt, no retries.** [`RunStep`](ds:block?id=workflowtest-runstep-zsng78k5) never retries; to test a whole workflow with its retries, run it on the memory driver.
- **Panics are not recovered.** A panic in the handler fails the test with its stack ([`RunStep`](ds:block?id=workflowtest-runstep-zsng78k5)), so add the recovery middleware ([`middleware.Recover`](ds:block?id=mw-recover-a7qbjc9u)) with `StepMiddleware` to test what a driver with it would do.

## Related

- [Test batches, chains and a subset of workflows](batches-and-chains.md): the Fake's batch and chain assertions.
- [Test app code with the Fake](fake.md): assert what app code enqueued.
- [Retries](../jobs/retries.md): retry policies, `RetryAfter` and `Permanent`.
- Example: [examples/batch-tests](../../examples/batch-tests/).
