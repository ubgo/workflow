# Custom stores

Every feature that keeps state outside a run (unique locks, debounce bursts, circuit-breaker counts, claim-check blobs, batch progress) keeps it behind a small interface, with an in-process implementation as the default. The default holds only inside one process; a store every process shares (a SQL table, Redis, S3) makes the feature hold across the fleet. This guide lists the interfaces and the guarantee each one must keep, and writes a unique-lock store over any key-value service.

## Before you start

- You use the feature whose state you are moving: [unique jobs](../reliability/unique-jobs.md#unique-for-a-time), [debounce](../reliability/debounce.md#debounce-a-burst), [throttling](../middleware/step-middleware.md#throttle-a-failing-dependency), [claim check](../reliability/claim-check.md#move-big-payloads-out-of-the-queue) or [batches](../batches/batches.md#dispatch-a-batch).

## Bring your own store

| Interface | Methods | Must guarantee | Built in | Plug in with |
|---|---|---|---|---|
| `middleware.UniqueStore` | `Acquire(ctx, key, owner, ttl) (bool, error)`, `Release(ctx, key, owner) error` | `Acquire` is atomic (check-and-take in one step); `Release` frees the key only for its owner, and a free or foreign key is a no-op | `middleware.MemoryUnique` (one process); `sqlstore.UniqueStore` (every process on one database, usable without the SQL driver through `sqlstore.NewUniqueStore`) | `middleware.Unique(by, store, ttl)`, `middleware.UniqueUntilFinished(c, by, store, ttl)` |
| `middleware.DebounceStore` | `Get(ctx, key)`, `Swap(ctx, key, burst)` | `Swap` replaces a key's burst and returns the one it replaced in one atomic step, so every replaced run is cancelled by exactly one process; `Get` is a hint and may race | `middleware.MemoryDebounce`; `sqlstore.DebounceStore` (`Client.DebounceStore`, or standalone `NewDebounceStore`) | `middleware.WithDebounceStore(store)` on `middleware.Debounce` |
| `middleware.CircuitStore` | `IsOpen(ctx, key)`, `RecordError(ctx, key, policy)`, `Reset(ctx, key)` | `RecordError` counts in one atomic step (an increment, not a read and a write) and opens the circuit for `policy.Cooldown` once `policy.MaxErrors` errors fall in `policy.Window` | `middleware.MemoryCircuit` | `ThrottleConfig.Store` on `middleware.ThrottleErrors` |
| `middleware.BlobStore` | `Put`, `Get`, `Delete` | `Get` of a missing key wraps `middleware.ErrBlobNotFound`; `Delete` of a missing key is a no-op; every process that enqueues or runs steps reaches the same blobs | `middleware.MemoryBlobStore` | `middleware.NewClaimCheck(store, threshold)` |
| `middleware.BlobPrefixDeleter` (optional upgrade of `BlobStore`) | `DeletePrefix(ctx, prefix)` | removes every blob under the prefix | `MemoryBlobStore` has it | with it, `ClaimCheck.CleanupOnFinish` also deletes a run's step outputs; without it, only the payload |
| `batch.Store` | `Create`, `Get`, `Update(ctx, id, fn)`, `Unfinished` | `Update` applies `fn` and saves atomically; an unknown ID wraps `workflow.ErrNotFound`; `Unfinished` lists open batches cheaply | `batch.MemoryStore`; `sqlstore.Client.BatchStore()` | `batch.New(client, store)` |

Failures are designed to fail closed: a unique store error fails the enqueue, a debounce store error fails the enqueue (and cancels the new run if `Swap` failed), a circuit store `IsOpen` error throttles the call. A store never makes a feature quietly stop working.

The same pattern runs through the other replaceable parts, each an interface or a function type with the shipped behaviour as the default: the cron dialect (`cron.Parser` via `memory.WithCronParser`, `sqlstore.WithCronParser`), the database error classifier (`sqlstore.WithErrorClassifier`), run and schedule IDs (`sqlstore.WithIDGenerator`), batch IDs (`batch.WithIDFunc`), what counts as an outage for failover (`failover.WithClassifier`), and payload codecs (`middleware.Codec`).

### Step 1 — Write the store over the service you run

A unique store needs two operations a key-value service has: "set if absent, with expiry" and "delete if the value is still mine". Redis has both (`SET key value NX PX ttl`, and a compare-and-delete script); a SQL table with a primary key has both too. The owner is a token `Unique` draws per enqueue, which is what keeps a late run-finished event for an old run from freeing a newer job's lock.

<!-- snippet: examples/custom-plugin/store.go#store -->
```go
// KV is the slice of a key-value service a unique store needs: an atomic
// "set if absent, with expiry" and a "delete if the value is still mine".
// Redis gives both (SET key value NX PX ttl, and a compare-and-delete
// script); so does a SQL table with a primary key.
type KV interface {
	SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error)
	DeleteIfEquals(ctx context.Context, key, value string) error
}

// kvUnique is a middleware.UniqueStore over any KV. Acquire is one atomic
// call, so two processes enqueueing the same key at once cannot both win,
// and Release deletes only the owner's own lock, so a late run-finished
// event for an old run never frees a newer job's lock.
type kvUnique struct {
	kv     KV
	prefix string
}

var _ middleware.UniqueStore = kvUnique{}

func (s kvUnique) Acquire(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	return s.kv.SetNX(ctx, s.prefix+key, owner, ttl)
}

func (s kvUnique) Release(ctx context.Context, key, owner string) error {
	return s.kv.DeleteIfEquals(ctx, s.prefix+key, owner)
}
```

The example's `memKV` stands in for the service, with a mutex where Redis would be atomic.

### Step 2 — Plug it in

<!-- snippet: examples/custom-plugin/plugin.go#enqueue -->
```go
// EnqueueMiddleware makes workflow.Use run these on every Enqueue through
// the client it returns. The first entry sees the job first.
func (p *tenancy) EnqueueMiddleware() []workflow.EnqueueMiddleware {
	byTenant := func(j workflow.Job) string { return j.Labels["tenant"] + "/" + j.Workflow }
	return []workflow.EnqueueMiddleware{
		requireLabel("tenant"),
		middleware.Unique(byTenant, p.unique, time.Hour),
	}
}

// requireLabel refuses a job without the label, before it reaches the
// driver.
func requireLabel(key string) workflow.EnqueueMiddleware {
	return func(next workflow.EnqueueFunc) workflow.EnqueueFunc {
		return func(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
			if job.Labels[key] == "" {
				return workflow.RunRef{}, fmt.Errorf("%w: job for %q has no %q label", workflow.ErrInvalid, job.Workflow, key)
			}
			return next(ctx, job)
		}
	}
}
```

### Step 3 — Test the guarantee, not the happy path

The guarantee that matters is atomicity under contention and owner-checked release. The example's test starts 50 goroutines acquiring one key and expects exactly one winner, then checks that an expired lock is taken over and that a release by another owner frees nothing.

## Try it

```sh
cd examples
go test ./custom-plugin -run TestKVUniqueOneWinner -v
```

```text
=== RUN   TestKVUniqueOneWinner
--- PASS: TestKVUniqueOneWinner (0.00s)
PASS
```

## Pitfalls

- A "get, then set" store is not atomic: two enqueues at once both see the key free and both pass. Use the service's own conditional write.
- A `Release` that deletes the key whatever its value lets a duplicate run-finished event for an old run free the lock a newer job took since.
- An in-process store (the `Memory*` defaults) holds only inside one process. With several worker or API processes, every one of them enqueueing, uniqueness and debouncing need a shared store to mean anything.
- A store keeps its own time. Expiry measured by the service's clock and by your processes' clocks can differ; keep TTLs well above clock skew.
- The SQL stores need their tables: `sqlstore.Client.Migrate` creates the unique and debounce tables; a standalone `NewUniqueStore` or `NewDebounceStore` has its own `Migrate` ([Each store's migration list](../operations/migrations.md#each-stores-migration-list)).

## Related

- [A unique store shared by every process](../reliability/unique-jobs.md#a-store-shared-by-every-process)
- [Debounce state shared by every process](../reliability/debounce.md#a-store-shared-by-every-process)
- [Where batch state lives](../batches/durability.md#where-batch-state-lives)
- [Write a plugin](writing-a-plugin.md#write-a-plugin)
- Example: [custom-plugin](../../examples/custom-plugin/)
