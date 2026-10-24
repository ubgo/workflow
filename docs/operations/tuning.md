# Tuning

The defaults fit most apps. This page covers the knobs worth turning when they do not: the SQL driver's clean-up cadence, and the Hatchet driver's limits for big batches and slow engines. The SQL driver's everyday settings (workers, polling, leases, transaction retries) are in [The SQL driver](../drivers/sqlstore.md#workers-and-polling), and how long finished runs stay is in [Retention](retention.md).

## Before you start

- A [SQL driver](../drivers/sqlstore.md) or [Hatchet driver](../drivers/hatchet.md) client running with the defaults, and a reason to change one (a table growing, a batch failing to start, a slow engine).

## Housekeeping

The SQL driver's dispatcher cleans up every `WithHousekeepingEvery` passes (default 50), whatever the retention: consumed signals and their markers, signals no unfinished run can take, fired cron tick rows older than `WithTickRetention` (default 24 hours), expired uniqueness locks and debounce bursts whose run has started.

<!-- snippet: examples/retention/main.go#housekeeping -->
```go
return []sqlstore.Option{
	// A tick row stops a second node firing the same cron tick; keep it
	// well above the longest pause a node can have (default 24h).
	sqlstore.WithTickRetention(48 * time.Hour),
	// Clean up every 20 dispatcher passes instead of 50: for a database
	// where signals or unique locks pile up fast.
	sqlstore.WithHousekeepingEvery(20),
}
```

- **`WithHousekeepingEvery(passes)`**: lower it when signals or unique locks pile up faster than every 50 passes; raise it to spend fewer queries on a quiet database. Values below 1 are ignored.
- **`WithTickRetention(d)`**: a tick row is what stops a second node firing the same tick, which only matters while a node may still see that tick due. Keep it well above the longest pause a node can have between reading a schedule and firing it (a long GC pause, a frozen VM). Values of zero or less are ignored.

Finished runs are not housekeeping: they have their own cadence, `WithPruneInterval` ([Retention](retention.md#prune-on-your-own-schedule)).

## Hatchet tunings

A batch on the Hatchet driver starts its jobs as bulk triggers, or as one coordinator run that starts them as children ([Native groups](../batches/native-groups.md)). These fields size those calls:

<!-- snippet: examples/hatchet-worker/main.go#tunings -->
```go
return hatchet.Config{
	// Bulk triggers (batch.GroupRunner): at most this many runs and this
	// much JSON per RunMany call. The defaults (1000 runs, 2 MiB) are also
	// the maximum; lower them for an engine with a smaller gRPC limit.
	GroupChunkRuns:  500,
	GroupChunkBytes: 1 << 20,

	// One coordinator run per batch, which starts every job as a child.
	BatchCoordinator: true,
	// One coordinator attempt; also how long a coordinator on a dead worker
	// waits before the engine retries it (default 5 minutes).
	BatchCoordinatorTimeout: 10 * time.Minute,
	// Must fit Hatchet's Factor^n seconds: Initial equal to Factor seconds,
	// no Jitter, no DelayFor, no NoRetryOn (default: 10 retries, capped 2m).
	BatchCoordinatorRetry: &workflow.RetryPolicy{Max: 20, Backoff: workflow.Backoff{
		Initial: 2 * time.Second, Factor: 2, Max: 2 * time.Minute,
	}},
	// A batch whose input is bigger starts as bulk triggers instead
	// (default 2 MiB; raise it only with the engine's message limit).
	BatchCoordinatorMaxInput: 1 << 20,
}
```

| Field | What it bounds | Default | Limit |
|---|---|---|---|
| `GroupChunkRuns` | Runs per bulk trigger (`RunGroup` and the coordinator's child starts) | 1000 (`DefaultGroupChunkRuns`) | 1000: the SDK splits a larger call itself and then cannot say which runs failed |
| `GroupChunkBytes` | JSON input and metadata per bulk trigger; lower it for an engine whose gRPC message limit is under 4 MiB | 2 MiB (`DefaultGroupChunkBytes`) | 2 MiB, under the SDK's 3 MiB split |
| `BatchCoordinatorTimeout` | One coordinator attempt; also how long a coordinator on a dead worker waits before the engine retries it. Keep it near the time starting your largest batch takes | 5 min | none |
| `BatchCoordinatorRetry` | The coordinator's retry policy. A retry starts the children again with the same child keys, and the engine returns the existing child for a key it has seen, so no job runs twice | `DefaultBatchCoordinatorRetry()`: 10 retries at 2^n seconds capped at 2 minutes, about ten minutes of engine outage | Must fit Factor^n seconds; `NoRetryOn` must be nil |
| `BatchCoordinatorMaxInput` | JSON input and metadata one coordinator trigger carries; a larger batch starts as bulk triggers | 2 MiB (`DefaultBatchCoordinatorMaxInput`) | the engine's gRPC message limit |

A single job larger than `GroupChunkBytes` goes in a call of its own. `Start` refuses a chunk size over its limit with an error matching `workflow.ErrInvalid`, and a coordinator retry policy Hatchet cannot express, before it connects.

For a slow engine, two fields outside batches matter:

- **`RegisterTimeout`** (default 30 seconds) bounds `Start`'s wait for the engine to list the worker; a cold pod may need longer.
- **`FailedSettle`** (default 5 seconds, `hatchet.DefaultFailedSettle`) is how long after a failed attempt a FAILED read of a run with retries left still counts as between attempts for `Status`, `Runs`, `Retry`, the run-finished hook and `BatchSource`. Raise it on a slow engine whose retries take longer to show.
- **`VisibilityLag`** (default 5 seconds) is how long the engine's read side may trail a write. `Status` retries a not-found answer this long; it also sizes the run-finished notifier's list overlap and how long a batch waits after its coordinator ended.

## Try it

```sh
cd examples
go run ./retention
go run ./hatchet-worker
```

`retention` builds its clients with the housekeeping options above; its output is in [Retention › Try it](retention.md#try-it). `hatchet-worker` runs its live part with the tunings above when `HATCHET_CLIENT_TOKEN` is set, and shows the `GroupChunkRuns` refusal either way:

```text
GroupChunkRuns 5000: ErrInvalid=true
  workflow: invalid spec: hatchet driver: GroupChunkRuns 5000 is over the SDK's bulk limit of 1000
```

## Pitfalls

- A `WithTickRetention` shorter than a node's longest pause lets that node fire a tick another node already fired.
- `BatchCoordinatorTimeout` too short fails a coordinator that is still starting a big batch; too long makes a coordinator on a dead worker wait that long before its retry.
- Raising `BatchCoordinatorMaxInput` past the engine's gRPC message limit makes coordinator triggers fail; raise both together.
- Every process sharing a batch store should set `BatchCoordinator` alike, so a resumed dispatch finds the coordinator.

## Related

- [The SQL driver](../drivers/sqlstore.md) and [the Hatchet driver](../drivers/hatchet.md)
- [Retention](retention.md)
- [Native groups](../batches/native-groups.md)
- Examples: [retention](../../examples/retention/), [hatchet-worker](../../examples/hatchet-worker/)
