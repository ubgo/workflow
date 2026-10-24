# batch-tests

App code that uploads files as one batch, publishes an album through a three-step chain, reprocesses a file on another queue with a larger retry budget, lets an operator pause the images queue, retry a failed run and forget a finished one, and resizes images with a step that releases on rate limits, with the tests that check it without a worker: `Fake.AssertBatched` and `AssertBatchCount` on the batch, `Fake.AssertChained` on the chain, `AssertEnqueuedOn`, `AssertPaused`, `AssertRetried` and `AssertDeleted` on the operator code, `FakeExcept` running the resize jobs for real on the memory driver while the batch's `Then` callback is faked, and `workflowtest.RunStep` on the step's success, release, permanent failure and ordinary failure.

```sh
cd examples
go run ./batch-tests
go test -v ./batch-tests
```

`go run` runs the app on the in-memory driver:

```text
notified ada
publish enqueued
```

The guides for this program are [Test batches, chains and a subset of workflows](../../docs/testing/batches-and-chains.md), [Test queues, retries and pauses](../../docs/testing/operations.md) and [Test one step](../../docs/testing/run-step.md).
