# Custom stores

Every feature that keeps state outside a run (unique locks, debounce bursts, circuit-breaker counts, claim-check blobs, batch progress) keeps it behind a small interface, with an in-process implementation as the default. The default holds only inside one process; a store every process shares (a SQL table, Redis, S3) makes the feature hold across the fleet. This guide lists the interfaces and the guarantee each one must keep, and writes a unique-lock store over any key-value service.

## Before you start

- You use the feature whose state you are moving: [unique jobs](../reliability/unique-jobs.md#unique-for-a-time), [debounce](../reliability/debounce.md#debounce-a-burst), [throttling](../middleware/step-middleware.md#throttle-a-failing-dependency), [claim check](../reliability/claim-check.md#move-big-payloads-out-of-the-queue) or [batches](../batches/batches.md#dispatch-a-batch).

## Bring your own store

| Interface | Methods | Must guarantee | Built in | Plug in with |
|---|---|---|---|---|
| [`middleware.UniqueStore`](ds:block?id=mw-uniquestore-udbnz7hp) | `Acquire(ctx, key, owner, ttl) (bool, error)`, `Release(ctx, key, owner) error` | `Acquire` is atomic (check-and-take in one step); `Release` frees the key only for its owner, and a free or foreign key is a no-op | [`middleware.MemoryUnique`](ds:block?id=mw-memoryunique-uwj4x9gs) (one process); [`sqlstore.UniqueStore`](ds:block?id=sql-uniquestore-u4xwy2j4) (every process on one database, usable without the SQL driver through [`sqlstore.NewUniqueStore`](ds:block?id=sql-newuniquestore-65hb2yyu)) | [`middleware.Unique(by, store, ttl)`](ds:block?id=mw-unique-pgzepgu6), [`middleware.UniqueUntilFinished(c, by, store, ttl)`](ds:block?id=mw-uniqueuntilfinished-kgv65kyf) |
| [`middleware.DebounceStore`](ds:block?id=mw-debouncestore-gp9rakek) | `Get(ctx, key)`, `Swap(ctx, key, burst)` | `Swap` replaces a key's burst and returns the one it replaced in one atomic step, so every replaced run is cancelled by exactly one process; `Get` is a hint and may race | [`middleware.MemoryDebounce`](ds:block?id=mw-memorydebounce-abd56ahw); [`sqlstore.DebounceStore`](ds:block?id=sql-debouncestore-jsm48sbx) (`Client.DebounceStore`, or standalone [`NewDebounceStore`](ds:block?id=sql-newdebouncestore-4ymc7ecv)) | [`middleware.WithDebounceStore(store)`](ds:block?id=mw-withdebouncestore-5r4vkz95) on [`middleware.Debounce`](ds:block?id=mw-debounce-5g2ska6s) |
| [`middleware.CircuitStore`](ds:block?id=mw-circuitstore-cxtscnjk) | `IsOpen(ctx, key)`, `RecordError(ctx, key, policy)`, `Reset(ctx, key)` | `RecordError` counts in one atomic step (an increment, not a read and a write) and opens the circuit for `policy.Cooldown` once `policy.MaxErrors` errors fall in `policy.Window` | [`middleware.MemoryCircuit`](ds:block?id=mw-memorycircuit-uveb9h55) | [`ThrottleConfig.Store`](ds:block?id=mw-throttleconfig-m3a2t8jn) on [`middleware.ThrottleErrors`](ds:block?id=mw-throttleerrors-fvbczfxu) |
| [`middleware.BlobStore`](ds:block?id=mw-blobstore-qhytdhk9) | `Put`, `Get`, `Delete` | `Get` of a missing key wraps [`middleware.ErrBlobNotFound`](ds:block?id=mw-errblobnotfound-ezj4fu6t); `Delete` of a missing key is a no-op; every process that enqueues or runs steps reaches the same blobs | [`middleware.MemoryBlobStore`](ds:block?id=mw-memoryblobstore-aygfdxck) | [`middleware.NewClaimCheck(store, threshold)`](ds:block?id=mw-newclaimcheck-vbjnm6gt) |
| [`middleware.BlobPrefixDeleter`](ds:block?id=mw-blobprefixdeleter-sre4kb37) (optional upgrade of `BlobStore`) | `DeletePrefix(ctx, prefix)` | removes every blob under the prefix | `MemoryBlobStore` has it | with it, `ClaimCheck.CleanupOnFinish` also deletes a run's step outputs; without it, only the payload |
| [`batch.Store`](ds:block?id=batch-store-vp7qhnme) | `Create`, `Get`, `Update(ctx, id, fn)`, `Unfinished` | `Update` applies `fn` and saves atomically; an unknown ID wraps `workflow.ErrNotFound`; `Unfinished` lists open batches cheaply | [`batch.MemoryStore`](ds:block?id=batch-memorystore-hjud4syz); [`sqlstore.Client.BatchStore()`](ds:block?id=sql-client-batchstore-m5rnk82w) | [`batch.New(client, store)`](ds:block?id=batch-new-geazwpek) |

Failures are designed to fail closed: a unique store error fails the enqueue ([`Unique`](ds:block?id=mw-unique-pgzepgu6)), a debounce store error fails the enqueue (and cancels the new run if `Swap` failed: [`DebounceStore`](ds:block?id=mw-debouncestore-gp9rakek)), a circuit store `IsOpen` error throttles the call ([`CircuitStore`](ds:block?id=mw-circuitstore-cxtscnjk)). A store never makes a feature quietly stop working.

The same pattern runs through the other replaceable parts, each an interface or a function type with the shipped behaviour as the default: the cron dialect ([`cron.Parser`](ds:block?id=cron-parser-8g5hqgru) via [`memory.WithCronParser`](ds:block?id=memory-withcronparser-jnqd5hex), [`sqlstore.WithCronParser`](ds:block?id=sql-withcronparser-dqejw86q)), the database error classifier ([`sqlstore.WithErrorClassifier`](ds:block?id=sql-witherrorclassifier-jjq8my6v)), run and schedule IDs ([`sqlstore.WithIDGenerator`](ds:block?id=sql-withidgenerator-jnvdknyn)), batch IDs ([`batch.WithIDFunc`](ds:block?id=batch-withidfunc-a9ccxauz)), what counts as an outage for failover ([`failover.WithClassifier`](ds:block?id=failover-withclassifier-ghwaub6c)), and payload codecs ([`middleware.Codec`](ds:block?id=mw-codec-zax9ezsu)).

### Step 1 — Write the store over the service you run

A unique store needs two operations a key-value service has: "set if absent, with expiry" and "delete if the value is still mine". Redis has both (`SET key value NX PX ttl`, and a compare-and-delete script); a SQL table with a primary key has both too. The owner is a token [`Unique`](ds:block?id=mw-unique-pgzepgu6) draws per enqueue, which is what keeps a late run-finished event for an old run from freeing a newer job's lock.

<!-- snippet: examples/custom-plugin/store.go#store -->
```go
// KV is the slice of a key-value service a unique store needs: an atomic
// "set if absent, with expiry" and a "delete if the value is still mine".
// Redis gives both (SET key value NX PX ttl, and a compare-and-delete
// script); so does a SQL table with a primary key.
type KV interface {
	// SetNX stores value under key for ttl unless a live value is there,
	// and reports whether it did.
	SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error)
	// DeleteIfEquals deletes key only while it still holds value.
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

// kvUnique must keep satisfying the interface middleware.Unique takes.
var _ middleware.UniqueStore = kvUnique{}

// Acquire implements middleware.UniqueStore: one atomic SetNX of the
// prefixed key with the owner as its value.
func (s kvUnique) Acquire(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	return s.kv.SetNX(ctx, s.prefix+key, owner, ttl)
}

// Release implements middleware.UniqueStore: it deletes the lock only if
// owner still holds it.
func (s kvUnique) Release(ctx context.Context, key, owner string) error {
	return s.kv.DeleteIfEquals(ctx, s.prefix+key, owner)
}
```

The example's `memKV` stands in for the service, with a mutex where Redis would be atomic.

### Step 2 — Plug it in

Hand the store to [`middleware.Unique`](ds:block?id=mw-unique-pgzepgu6): it takes the lock before the job reaches the driver, refuses the job with `workflow.ErrDuplicate` while the key is held, and releases the lock at once when the driver refuses the enqueue, so a retry can pass. Here the plugin's enqueue middleware does it:

<!-- snippet: examples/custom-plugin/plugin.go#enqueue -->
```go
// EnqueueMiddleware makes workflow.Use run these on every Enqueue through
// the client it returns. The first entry sees the job first.
func (p *tenancy) EnqueueMiddleware() []workflow.EnqueueMiddleware {
	// byTenant is the uniqueness key: one queued job per tenant and workflow.
	byTenant := func(j workflow.Job) string { return j.Labels["tenant"] + "/" + j.Workflow }
	return []workflow.EnqueueMiddleware{
		requireLabel("tenant"),
		middleware.Unique(byTenant, p.unique, time.Hour), // TTL: a lock whose release was lost frees itself
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
- A [`UniqueStore`](ds:block?id=mw-uniquestore-udbnz7hp) `Release` that deletes the key whatever its value lets a duplicate run-finished event for an old run free the lock a newer job took since.
- An in-process store (the `Memory*` defaults, such as [`MemoryUnique`](ds:block?id=mw-memoryunique-uwj4x9gs)) holds only inside one process. With several worker or API processes, every one of them enqueueing, uniqueness and debouncing need a shared store to mean anything.
- A store keeps its own time. Expiry measured by the service's clock and by your processes' clocks can differ; keep TTLs well above clock skew.
- The SQL stores need their tables: [`sqlstore.Client.Migrate`](ds:block?id=sql-client-migrate-vdt8uy3d) creates the unique and debounce tables; a standalone [`NewUniqueStore`](ds:block?id=sql-newuniquestore-65hb2yyu) or [`NewDebounceStore`](ds:block?id=sql-newdebouncestore-4ymc7ecv) has its own `Migrate` ([Each store's migration list](../operations/migrations.md#each-stores-migration-list)).

## Related

- [A unique store shared by every process](../reliability/unique-jobs.md#a-store-shared-by-every-process)
- [Debounce state shared by every process](../reliability/debounce.md#a-store-shared-by-every-process)
- [Where batch state lives](../batches/durability.md#where-batch-state-lives)
- [Write a plugin](writing-a-plugin.md#write-a-plugin)
- Example: [custom-plugin](../../examples/custom-plugin/)
