# Record a real client

The [Fake](fake.md) runs nothing, which is what most app tests want. Some tests need the jobs to really run (an integration test, code that relies on the run-finished hook) and still want to check what was enqueued. `workflowtest.NewRecorder(inner)` wraps a real client: every call goes to `inner`, and each enqueue `inner` accepted is recorded on the way back.

## Before you start

- A real client to wrap, usually the memory driver in tests. The Recorder forwards `Register`, `Start`, `Status`, `Cancel`, `Signal`, `Schedule`, `Unschedule` and `Capabilities` to it, and forwards `OnRunFinished` when `inner` implements `workflow.RunNotifier`.
- The runnable program for this page is [examples/app-tests](../../examples/app-tests/).

## Record a real client

Use the Recorder wherever the app takes a `workflow.Client`: register and start through it, run the code, then assert. The jobs run on the memory driver; the Recorder only notes them:

<!-- snippet: examples/app-tests/main_test.go#recorder -->
```go
func TestSignUpOnARealDriver(t *testing.T) {
	ctx := t.Context()
	// The jobs really run on the memory driver; the Recorder notes each
	// enqueue the driver accepted.
	rec := workflowtest.NewRecorder(memory.New())
	if err := startOn(ctx, rec); err != nil {
		t.Fatal(err)
	}
	app := &Accounts{Jobs: rec, Clock: clock.Real{}}
	if _, err := app.SignUp(ctx, "ada@example.com"); err != nil {
		t.Fatal(err)
	}

	rec.AssertEnqueued(t, "welcome-email", 1) // exactly once
	rec.AssertEnqueuedWith(t, "reminder", func(j workflow.Job) bool { return j.IdempotencyKey == "reminder:ada@example.com" })
	rec.AssertNotEnqueued(t, "nudge")
}
```

The Recorder has its own assertions, simpler than the Fake's:

| Method | Fails unless |
|---|---|
| `AssertEnqueued(t, wf, n)` | Workflow `wf` was enqueued exactly `n` times |
| `AssertEnqueuedWith(t, wf, match)` | At least one job of `wf` satisfies `match`, a `func(workflow.Job) bool` |
| `AssertNotEnqueued(t, wf)` | `wf` was not enqueued at all |
| `AssertNothingEnqueued(t)` | Nothing was enqueued |

`Jobs()` returns a copy of every recorded job, in enqueue order. Jobs are copied when recorded, so a caller reusing its `Job` or `Labels` map cannot change what was recorded.

The Recorder has the Fake's batch and chain assertions too (`Batches`, `AssertBatched`, `AssertBatchCount`, `AssertNothingBatched`, `AssertChained`), described on [Test batches, chains and a subset of workflows](batches-and-chains.md), and `Workflow(name)` returns a registered definition. Its `Retry` and `Delete` record each call the real client accepted, for `AssertRetried` and `AssertDeleted`, and `AssertPaused` reads the real client's pauses ([Test queues, retries and pauses](operations.md)).

## Fake or Recorder

| | `NewFake` | `NewRecorder(inner)` |
|---|---|---|
| Runs the jobs | no | yes, on `inner` |
| Refuses what production refuses | yes, with `FakeLike(prod)` | yes, because `inner` is a real driver |
| Assertions | the full set: matchers, schedules, signals, cancels | the four above, plus `Jobs()` |
| Speed | instant | as fast as `inner`; pair the memory driver with a [fake clock](fake-clock.md) to skip delays and backoffs |
| Use it for | unit tests of code that enqueues | integration tests where a step's effect, a run end or a batch callback must really happen |

If a test only asks "did my handler enqueue the right job?", the Fake is simpler and faster. Reach for the Recorder when the next assertion depends on the job having run.

## Where it sits in a middleware stack

The Recorder records the job exactly as it was passed to its own `Enqueue`. Where you put it decides what you see:

- `workflowtest.NewRecorder(middleware.WrapClient(mem, mws...))` records the job as the app sent it, before any enqueue middleware changed it. Use this to test the app's own calls.
- `middleware.WrapClient(workflowtest.NewRecorder(mem), mws...)` records the job after the middleware ran (a `Route` rule's new workflow name and labels, a codec's encoded payload). Use this to test the middleware.

In both shapes a job that the middleware or the driver refused is not recorded.

## Try it

```sh
cd examples
go test ./app-tests/ -run TestSignUpOnARealDriver -v
```

```text
=== RUN   TestSignUpOnARealDriver
--- PASS: TestSignUpOnARealDriver (0.00s)
PASS
```

## Pitfalls

- A job the inner client refused is not recorded: it was not enqueued. A job deduplicated onto an existing run by its key is recorded, because the inner client accepted the call.
- `OnRunFinished` on a Recorder over a client without `workflow.RunNotifier` returns an `UnsupportedError` rather than registering an observer that would never be called.
- `NewRecorder(nil)` is the older standalone fake. It claims every capability and refuses nothing, so it tests the double rather than the app; it is kept only for compatibility. Use [`NewFake`](fake.md) for a client that runs nothing.

## Related

- [Test app code with the Fake](fake.md).
- [Drive time in tests](fake-clock.md).
- [The memory driver](../drivers/memory.md): the usual inner client in tests.
- Example: [examples/app-tests](../../examples/app-tests/).
