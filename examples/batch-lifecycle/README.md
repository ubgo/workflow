# batch-lifecycle

What a batch can do after `Dispatch`, on the in-memory driver: `Before` and `Progress` callbacks, a job that adds two more jobs to its own batch with `Manager.Add`, `Manager.RetryFailed` on a batch whose flaky job failed once, a workflow whose `AwaitStep` dispatches a batch and resumes when it ends, `batch.EnqueueMany` for jobs without a batch, and `Manager.Prune` of the closed batches two days later on a fake clock.

```sh
cd examples
go run ./batch-lifecycle
```

```text
-- before and progress
before b1 total=3
finally b1 total=3 succeeded=3 failed=0
progress b1 total=3
progress b1 total=3
progress b1 total=3
-- a job adds jobs to its batch
finally b2 total=3 succeeded=3 failed=0
then b2 total=3 succeeded=3 failed=0
-- retry the failed jobs
catch b3 total=2 failed=1
finally b3 total=2 succeeded=1 failed=1
retried 1
finally b3 total=2 succeeded=2 failed=0
then b3 total=2 succeeded=2 failed=0
-- a workflow waits for a batch
import done: 4 of 4 succeeded
-- many jobs without a batch
started 3 runs
-- prune
pruned 4
```

Callbacks are separate runs that may finish in any order, so the program sorts each group of lines. The `before` and `progress` lines print only the total, because the other counts in their payload depend on which jobs had ended when each was enqueued.

The guides for this program are [Change a batch after dispatch](../../docs/batches/lifecycle.md) and [Wait for a batch in a workflow](../../docs/batches/await.md).
