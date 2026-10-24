# Unique jobs

A unique job refuses a second job with the same key while the first one holds a lock: one export per user at a time, one invoice run per account, one reindex per product. `middleware.Unique` is enqueue middleware, so it works on every driver; the lock lives in a `UniqueStore` you choose, and how far the guarantee reaches (one process, or every process sharing a database) is exactly how far that store reaches. The lock lasts for a time you choose, or until the run finishes, and the key is yours to build or the workflow and its arguments ([Unique by workflow and arguments](#unique-by-workflow-and-arguments)). It can count only while the holder waits to start ([Unique while the run waits to start](#unique-while-the-run-waits-to-start)), and a duplicate can get the existing run back instead of an error ([Return the existing run](#return-the-existing-run)).

Uniqueness is not idempotency. A job's `IdempotencyKey` makes a duplicate enqueue return the run that already holds the key, with no error ([idempotency](../jobs/idempotency.md)); `Unique` refuses the duplicate with an error wrapping `workflow.ErrDuplicate`, so the caller learns that the work is already queued and can say so.

## Before you start

- A client (any driver) and the `middleware` package. A store shared by several processes needs the `sqlstore` module and a database, but not the SQL driver: the lock table works beside Hatchet or the memory driver.
- A key function that returns the uniqueness key for a job, or `""` for a job that is not unique.
- The runnable program for this page is [examples/unique-jobs](../../examples/unique-jobs/).

## Unique for a time

`middleware.Unique(by, store, ttl)` takes the key from `by`, tries to acquire it in the store for `ttl`, and passes the job on only if it got the lock. A second job with the same key inside the `ttl` is refused with an error wrapping `workflow.ErrDuplicate`, and nothing reaches the driver. Wrap the client with `middleware.WrapClient` so every `Enqueue` goes through it; the wrapper keeps the driver's own capabilities.

The example keys exports by the `user` label and uses `middleware.NewMemoryUnique` on a fake clock, so the hour passes at once:

<!-- snippet: examples/unique-jobs/main.go#unique -->
```go
clk := clock.NewFake(time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC))
locks := middleware.NewMemoryUnique(clk)
c := middleware.WrapClient(mc, middleware.Unique(byUser, locks, time.Hour))

job := workflow.Job{Workflow: "export", Labels: map[string]string{"user": "u-7"}}
_, err1 := c.Enqueue(ctx, job)
_, err2 := c.Enqueue(ctx, job) // same user within the hour
clk.Advance(time.Hour)
_, err3 := c.Enqueue(ctx, job) // the lock has expired
```

The lock lasts `ttl` from the enqueue whatever the run does: it may outlive a fast run or end while a slow one is still working. Use plain `Unique` when the driver has no run-finished hook, or when "at most one per hour" is the rule you want.

Every job that passes gets two labels: `middleware.UniqueKeyLabel` (`ubgo.workflow/unique-key`) with the key and `middleware.UniqueOwnerLabel` (`ubgo.workflow/unique-owner`) with a random token drawn per enqueue. The token is the lock's owner, so only that run's release can free it.

`UniqueStore` is the seam for your own store (Redis, another table): `Acquire(ctx, key, owner, ttl)` must check and take the key in one atomic step, and `Release(ctx, key, owner)` frees it only for its owner and is a no-op otherwise. `MemoryUnique` holds inside one process.

## Unique until the run finishes

What an app usually wants is a lock that is freed as soon as the run ends, succeeded, failed or cancelled, with the `ttl` kept as a safety net. `middleware.UniqueUntilFinished(c, by, store, ttl)` registers a run-finished observer on `c` that reads the two labels from the `RunEvent` and releases the lock, then returns the same middleware as `Unique`. It registers an observer, so call it before `c.Start`:

<!-- snippet: examples/unique-jobs/main.go#until-finished -->
```go
locks := middleware.NewMemoryUnique(nil)
// Before Start: the release is a run-finished observer on mc.
unique, err := middleware.UniqueUntilFinished(mc, byUser, locks, 24*time.Hour)
if err != nil {
	return err
}
c := middleware.WrapClient(mc, unique)
```

While the first export runs, a second one for the same user is refused; once the run has finished, the next one is accepted. The release reads only the labels on the event, so it works in any process and after a restart, without a table in memory.

## Unique by workflow and arguments

Often the key is simply "the same job": the same workflow with the same arguments. `middleware.UniqueByArgs(opts...)` returns that key function, ready for `Unique`, `UniqueUntilFinished` and `UniqueUntilStarted`. The key is `args:` plus the workflow name plus the SHA-256 of the canonical payload: a JSON payload is decoded and encoded again, so key order and spacing do not matter (numbers keep their literal text, so `1` and `1.0` differ); any other payload is hashed as its bytes.

<!-- snippet: examples/unique-jobs/main.go#by-args -->
```go
// Only the account and the month count: a different requested_by is
// still the same report.
key := middleware.UniqueByArgs(middleware.ArgsFields("account", "month"))
c := middleware.WrapClient(mc, middleware.Unique(key, middleware.NewMemoryUnique(nil), time.Hour))

_, err1 := c.Enqueue(ctx, workflow.Job{Workflow: "report", Payload: []byte(`{"account":"a-1","month":"2026-01","requested_by":"ann"}`)})
// Same fields in another order, asked by someone else: the same key.
_, err2 := c.Enqueue(ctx, workflow.Job{Workflow: "report", Payload: []byte(`{"requested_by":"bob","month":"2026-01","account":"a-1"}`)})
_, err3 := c.Enqueue(ctx, workflow.Job{Workflow: "report", Payload: []byte(`{"account":"a-2","month":"2026-01"}`)})
```

The options choose what counts:

| Option | What it adds or changes |
|---|---|
| `ArgsFields(names...)` | Only these top-level fields of a JSON object count; a payload that is not an object counts whole, so the key gets narrower, never wider |
| `ArgsQueue()` | `Job.Queue` counts, so the same arguments on two queues are two jobs |
| `ArgsLabels(keys...)` | The values of these labels count (a missing label counts as absent) |
| `ArgsKeyPrefix(p)` | Replaces `args:` (`middleware.DefaultArgsKeyPrefix`), to keep these locks apart from your own keys in one store |
| `ArgsTagged[T]()` | Only the fields of `T` tagged `unique:"true"` count, by their JSON names: `ArgsFields` with the list kept on the payload type ([below](#fields-tagged-on-the-payload-type)) |

### Fields tagged on the payload type

`ArgsFields` lists the fields that count as strings at the call site, apart from the type that defines them; a renamed field silently stops counting. `middleware.ArgsTagged[T]()` reads the list from struct tags instead, so the rule lives on the payload type next to the fields it names:

<!-- snippet: examples/unique-states/main.go#tagged -->
```go
// SyncArgs is the payload of "sync-shop". Only the shop decides whether two
// syncs are the same job; the cursor does not.
type SyncArgs struct {
	ShopID string `json:"shop_id" unique:"true"`
	Cursor string `json:"cursor"`
}
```

`middleware.UniqueByArgs(middleware.ArgsTagged[SyncArgs]())` then keys a job by its workflow and its `shop_id` alone, so two syncs of one shop with different cursors are the same job. A field's name is its `json` tag (the part before a comma), else its Go name, as `encoding/json` does; unexported fields are skipped. A `T` that is not a struct, or has no tagged field, makes the whole payload count, the same as no option. The tag name is `middleware.UniqueTag`.

## Unique while the run waits to start

Plain `Unique` treats a key as taken whatever its holder is doing. Often the rule is narrower: "no second sync of this shop while one is waiting to start", because once a sync is running, a new request may carry changes it will not see, and should be let in. `middleware.UniqueWith(middleware.UniqueConfig{...})` is `Unique` with that choice. `States` lists the holder's statuses that make a new job a duplicate; when the key is held by a run in none of them, or by a run the client no longer knows, the new job takes the lock over:

<!-- snippet: examples/unique-states/main.go#states -->
```go
waiting, err := middleware.UniqueWith(middleware.UniqueConfig{
	By:     middleware.UniqueByArgs(middleware.ArgsTagged[SyncArgs]()),
	Store:  middleware.NewMemoryUnique(clk),
	TTL:    24 * time.Hour, // the safety net
	States: []workflow.Status{workflow.StatusScheduled, workflow.StatusQueued},
	Client: mc, // asked for the holder's status
})
if err != nil {
	return err
}
c := middleware.WrapClient(mc, waiting)
```

```text
unique while waiting to start:
  s-1 in an hour: accepted
  s-1 now, other cursor, first one scheduled: refused (ErrDuplicate)
  s-2 now: accepted
  started {"shop_id":"s-2","cursor":""}
  started {"shop_id":"s-1","cursor":"a"}
  s-1 again, first one running: accepted
  started {"shop_id":"s-1","cursor":"c"}
```

The first sync of `s-1` is delayed an hour, so it is `scheduled` and a second one is refused. Once it is running, a third is accepted. Adding `StatusSucceeded` to `States` keeps a key closed after success until the `TTL` ends ("at most one successful sync per day"); an empty `States` means every status counts, which is plain `Unique`.

`UniqueWith` looks the holder up, so it needs two things beyond `Unique`: a `Client` to ask the holder's `Status` (any client; the example passes the driver), and a store that remembers which run holds each key, the optional `middleware.UniqueHolders` interface. `MemoryUnique`, the Redis driver's unique store and `sqlstore.UniqueStore` (the table `<prefix>unique_holders`) implement it. `UniqueWith` refuses a configuration it cannot honour with `ErrInvalid`, at construction: a missing `By`, `Store` or `TTL`, an unknown status, `States` without a `Client`, or `States` or `ReturnExisting` on a store without `UniqueHolders`. `TTL` still bounds every lock, whatever the holder's state.

## Return the existing run

A refused duplicate is an error the caller has to handle. Often the caller only wants "the run for this export", new or not, the way an idempotency key behaves. `UniqueConfig.ReturnExisting` answers a duplicate with the holder's `RunRef` and a nil error. To tell a new run from an existing one, ask for the enqueue's outcome with `workflow.WithEnqueueOutcome`:

<!-- snippet: examples/unique-states/main.go#return-existing -->
```go
existing, err := middleware.UniqueWith(middleware.UniqueConfig{
	By:             func(j workflow.Job) string { return "export:" + j.Labels["user"] },
	Store:          middleware.NewMemoryUnique(clk),
	TTL:            time.Hour,
	ReturnExisting: true, // a duplicate gets the holder's RunRef and no error
})
if err != nil {
	return err
}
exports := middleware.WrapClient(mc, existing)
job := workflow.Job{Workflow: "export", Labels: map[string]string{"user": "u-7"}}
var first workflow.RunRef
for _, try := range []string{"first", "second"} {
	ctx, outcome := workflow.WithEnqueueOutcome(ctx) // a fresh one per Enqueue
	ref, err := exports.Enqueue(ctx, job)
	if err != nil {
		return err
	}
	if first.IsZero() {
		first = ref
	}
	fmt.Fprintf(out, "  %s: same run %t, duplicate %t (reported %t)\n", try, ref == first, outcome.Duplicate, outcome.Reported)
}
```

```text
return the existing run:
  first: same run true, duplicate false (reported true)
  second: same run true, duplicate true (reported true)
```

`WithEnqueueOutcome(ctx)` returns a ctx to pass to `Enqueue` and a `*workflow.EnqueueOutcome` to read after it. `Duplicate` is true when no new run was started: the ref is an existing run's. `Reported` says whether anything below the caller reported at all: false means the client cannot tell, so `Duplicate` false is "unknown", not "new". The drivers report it themselves for an idempotency key, with no middleware involved:

<!-- snippet: examples/unique-states/main.go#idempotency-outcome -->
```go
keyed := workflow.Job{Workflow: "export", IdempotencyKey: "export:u-8"}
for _, try := range []string{"first", "second"} {
	ctx, outcome := workflow.WithEnqueueOutcome(ctx)
	if _, err := mc.Enqueue(ctx, keyed); err != nil { // an idempotency key, no middleware
		return err
	}
	fmt.Fprintf(out, "  key %s: duplicate %t\n", try, outcome.Duplicate)
}
```

```text
  key first: duplicate false
  key second: duplicate true
```

Drivers call `workflow.MarkInserted` once a new run is stored and `workflow.MarkDuplicate` when an idempotency key returns the holder's run; `UniqueWith` calls `MarkDuplicate` when it returns the holder. The first mark wins. The memory, Redis, SQL and Hatchet drivers report outcomes. When the holder's run is not known yet (a race with its own enqueue), `ReturnExisting` still returns `ErrDuplicate`.

## A store shared by every process

`MemoryUnique` makes the lock hold in one process: two servers would each accept one job. `sqlstore.UniqueStore` keeps the locks as rows of `<prefix>unique_locks` (`wf_unique_locks` by default), so the lock holds across every process that shares the database. Its semantics are `MemoryUnique`'s exactly, and `Acquire` is atomic across processes: one upsert on Postgres and SQLite, and on MySQL a delete of the expired row followed by an insert that the primary key lets only one process win.

It needs no running SQL driver. `sqlstore.NewUniqueStore(db, dialect, opts...)` builds it over any `*sql.DB`, and its `Migrate` creates the lock table and nothing else, which is how an app on Hatchet gets durable uniqueness. An app on the SQL driver takes `Client.UniqueStore()` instead; `Client.Migrate` already creates the table. Of the sqlstore options, `WithTablePrefix` and `WithClock` apply.

In the example two connection pools on one SQLite file stand in for two servers. Each builds its own store and its own client, as a separate process would:

<!-- snippet: examples/unique-jobs/main.go#sql-store -->
```go
db, err := driversqlite.Open(path)
if err != nil {
	return nil, nil, nil, err
}
// No sqlstore queue needed: the lock table works beside any driver.
locks, err := sqlstore.NewUniqueStore(db, sqlb.SQLiteDialect{}, sqlstore.WithClock(clk))
if err != nil {
	return nil, nil, nil, err
}
if err := locks.Migrate(ctx); err != nil { // creates wf_unique_locks only
	return nil, nil, nil, err
}
mc := memory.New()
c := middleware.WrapClient(mc, middleware.Unique(byUser, locks, 10*time.Minute))
```

Process A's export is accepted and process B's, for the same user, is refused. Expired rows never block an `Acquire`; they only take space. A running `sqlstore.Client` deletes them on its dispatcher's housekeeping, and an app with no SQL client calls `PruneExpired` from its own scheduler:

<!-- snippet: examples/unique-jobs/main.go#prune -->
```go
clk.Advance(10 * time.Minute)
n, err := locksA.PruneExpired(ctx) // a running sqlstore.Client does this itself
```

## Try it

```sh
cd examples
go run ./unique-jobs
```

```text
-- Unique for a time
first: accepted
second, same hour: refused (ErrDuplicate)
third, an hour later: accepted
-- Unique until the run finishes
first: accepted
second, while the first runs: refused (ErrDuplicate)
export ran
run finished: succeeded key label: u-7
third, after it finished: accepted
export ran
-- Unique by workflow and arguments
a-1, ann: accepted
a-1, bob, keys reordered: refused (ErrDuplicate)
a-2: accepted
key: args:report:7dee8b416858...
-- A store shared by every process
process A: accepted
process B, same user: refused (ErrDuplicate)
expired locks pruned: 1
process B, after expiry: accepted
```

## Pitfalls

- A `ttl` of zero or less fails every keyed enqueue with `workflow.ErrInvalid`, rather than holding a lock forever.
- A job for which the key function returns `""` is not unique and passes straight through.
- `Acquire` on a held key reports false whoever asks, the holder included: the same caller enqueuing twice is refused too.
- If the driver refuses the enqueue, `Unique` releases the lock at once, so a retry can pass.
- `UniqueUntilFinished` after `Start` returns `workflow.ErrAlreadyStarted`. On a client that does not implement `workflow.RunNotifier` it returns an `UnsupportedError` naming `CapRunFinished`; use `Unique` there and live with the `ttl`.
- On a driver without `CapRunFinished` (Hatchet delivers run ends best effort) a lost event leaves the lock held until the `ttl` runs out. Pick a `ttl` longer than the longest run, but short enough that a lost event costs little.
- `Unique` owns `ubgo.workflow/unique-key` and `ubgo.workflow/unique-owner`. A job arriving with either, or any other label under `ubgo.workflow/` except the claim check's, is refused with `workflow.ErrInvalid`, so a caller cannot release someone else's lock. Use one `Unique` per middleware chain; a second one would refuse the first one's labels.
- A job enqueued straight on the driver, bypassing the middleware, is not checked, and so is a run written with `sqlstore.Client.EnqueueTx`, which runs no enqueue middleware. Route those through the [outbox](outbox.md) when they must be unique.
- **Place `UniqueByArgs` before a codec that changes the payload on every call.** It hashes the payload the middleware sees; after an encrypting codec two identical jobs have different bytes and never match.
- **A field missing from the payload is not the same as a field set to `null`** under `ArgsFields`; send the field consistently.
- **Use a fresh `WithEnqueueOutcome` per `Enqueue`.** The first mark wins, so a ctx reused for a second enqueue keeps the first one's answer; an `Enqueue` of a chain or a batch with one ctx reports its first job only.
- **`States` costs a `Status` call per refused enqueue.** The holder is looked up only when the key is held, but each lookup reaches the backend.
- **A takeover does not stop the holder.** With `States`, a running holder keeps running beside the job that took its key over; `States` decides whether a second run may exist, not which one wins.
- `MemoryUnique` holds per process. With several servers, use the SQL store (or your own shared `UniqueStore`).

## Related

- [Debounce a burst](debounce.md): keep the last job of a burst instead of the first.
- [Idempotency keys](../jobs/idempotency.md): fold a duplicate into the existing run instead of refusing it.
- [Enqueue middleware](../middleware/enqueue-middleware.md): how `WrapClient` and middleware order work.
- [React when a run finishes](../observability/run-finished.md): the hook `UniqueUntilFinished` uses.
- [Reserved names](../reference/names.md#labels): every label the library writes.
- Example: [examples/unique-jobs](../../examples/unique-jobs/).
- Example: [examples/unique-states](../../examples/unique-states/) (unique by state, tagged fields, return the existing run).
