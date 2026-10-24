# run-states

The two statuses for runs between and after attempts, on the in-memory driver: `StatusRetrying` while a failed step waits on its backoff, with the next attempt's number and time in `RunInfo`, and `StatusDiscarded` for a run a step dropped with `workflow.Discard`. It also shows that a run stopped by `MaxErrors` still ends failed, how a batch counts a discarded job by default and with `DiscardedAs`, and a status switch with a default branch.

```sh
cd examples
go run ./run-states
```

```text
memory declares CapRunStates: true
sync-invoices: retrying, attempt 2 at 1m0s, last error "upstream 503"
after the backoff: succeeded
ship-order: discarded, terminal true, ErrDiscarded true, ErrCancelled true
admin discarded: ship-order, "workflow: run cancelled: workflow: run discarded: step \"ship\": order o-9 no longer exists"
call-partner with MaxErrors 2: failed
batch with DiscardedAs "": batch-catch ran; succeeded 1, failed 1, discarded 1
batch with DiscardedAs "succeeded": batch-then ran; succeeded 2, failed 0, discarded 1
  retrying: waiting to retry
  discarded: dropped on purpose
  failed: needs a look
  succeeded: done
```

The `sync-invoices` step fails once with a one-minute backoff; while the fake clock stands still the run reads `retrying`, and moving the clock a minute lets attempt 2 run and succeed. The discarded run's error still matches `ErrCancelled`, so code written for the older behaviour keeps working. Only `Discard` (or a policy that sets `DiscardOnExhaust`, see [discard-exhausted](../discard-exhausted)) ends a run discarded: `call-partner` gives up through `MaxErrors` under a plain policy and ends `failed`, where an operator sees it.

The memory and SQL drivers declare `CapRunStates`. Hatchet does not: a run in backoff reads `queued` there, and `Discard` acts as `Permanent`.

The guide that copies this program is [run states](../../docs/jobs/run-states.md). `go test ./run-states` checks every line.
