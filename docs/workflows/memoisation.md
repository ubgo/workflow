# Memoise work inside a step

A step is retried from the top. When it charges a card and then fails to send the receipt, the retry charges the card again, unless the charge is remembered. `workflow.Memo` remembers it: it runs a named piece of work once per run, stores the result in a journal, and on any later attempt of the same step returns the stored result without running the work. `MapEach`, `Loop` and `ProcessChunks` are built on it, so a list, a loop or an export resumes at what did not finish instead of starting over, and `SetRunData` uses the same journal to keep a value for the whole run.

## Before you start

- Every helper on this page needs the journal: the `workflow.Journal` plugin on the client (or the `workflow.WithJournal` step middleware). Without it they return `workflow.ErrNoJournal`, marked permanent.
- The plugin forgets a run's entries when the run finishes, so the client must implement `workflow.RunNotifier`; `workflow.Use` refuses the plugin otherwise. The memory, SQL and Redis drivers do; Hatchet's run-finished hook is best effort, so a missed end can leave entries behind.
- Pick a journal store. `workflow.MemoryJournal` is in-process: it holds across retries of a step in one process, not across a crash, and not when the retry lands on another worker process. The Redis driver ships a durable one, `redis.NewJournalStore(rdb, prefix, ttl)` or `Client.JournalStore()`, which every worker shares. The SQL driver ships one too, `sqlstore.NewJournalStore(db, dialect)` or `Client.JournalStore()`, in the table `<prefix>journal`, created by `Client.Migrate` (or by `JournalStore.Migrate` for an app with no running client). Any other is your own `JournalStore` ([below](#write-your-own-journal-store)).
- The example is [`examples/flow-memo`](../../examples/flow-memo). Every workflow in it fails once on purpose. Run it with `go run ./flow-memo` from `examples/`.

## Turn on the journal

Wrap the client with the plugin before `Register` and `Start`, and register workflows on the client `Use` returns, so their steps get the journal:

<!-- snippet: examples/flow-memo/main.go#client -->
```go
journal := workflow.NewMemoryJournal()
c, err := workflow.Use(memory.New(), workflow.Journal(journal))
if err != nil {
	return err
}
```

`workflow.Journal(store, opts...)` puts the store on every step's context and forgets a run's entries when it ends. By default every finished run is forgotten, failed ones too, so a journal never grows without bound. `workflow.JournalKeepFailed()` keeps the entries of a failed run, so a later retry of that run through `RunRetrier` (which keeps its `RunRef`) resumes from its memoised work; your app then forgets those runs itself with `JournalStore.Forget`. Use `workflow.WithJournal(store)` alone, as step middleware, only when something else forgets runs.

## Memoise a call with Memo

`workflow.Memo[T](ctx, name, fn)` runs `fn` the first time and stores its result as JSON under `name`; every later attempt of the same step of the same run gets that result back and `fn` is not called:

<!-- snippet: examples/flow-memo/main.go#memo -->
```go
return workflow.Workflow{
	Name:  "bill",
	Retry: workflow.RetryPolicy{Max: 2},
	Steps: []workflow.Step{{Name: "charge", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		chargeID, err := workflow.Memo(ctx, "charge-card", func(ctx context.Context) (string, error) {
			calls.add("charge")
			return "ch_1", nil // the payment provider's charge ID
		})
		if err != nil {
			return nil, err
		}
		if calls.add("mail") == 1 {
			return nil, errors.New("mail server timed out") // retried; the charge is not repeated
		}
		return []byte(chargeID), nil
	}}},
}
```

The rules a caller keeps:

- **Names are stable across attempts and unique within one attempt.** A name used twice in one attempt of a step is refused (permanent, `ErrInvalid`), because the second call would silently get the first call's result. Names are scoped to the step, so two steps may use the same name.
- **An error is not stored.** When `fn` fails, nothing is saved and the next attempt calls `fn` again.
- **The result is JSON.** A stored value that no longer decodes into `T` (the type changed between deploys) is a permanent error.
- **At least once, not exactly once.** If the process dies after `fn` returns and before the save, or the save fails, `fn` runs again on the next attempt. Work that must never repeat, such as a charge, also carries an idempotency key the remote side honours.

`Memo` is safe to call from several goroutines of one step, with different names.

## Run a call per item with MapEach

`workflow.MapEach(ctx, name, items, fn, opts...)` calls `fn` once per item and returns the results in the order of `items`, for a number of parallel pieces known only when the step runs. Each item is memoised under `name` and its index, so a retried step runs again only the items that did not succeed:

<!-- snippet: examples/flow-memo/main.go#map -->
```go
sizes, err := workflow.MapEach(ctx, "resize", images, func(ctx context.Context, i int, image string) (string, error) {
	if calls.add("resize "+image) == 1 && image == "b.png" {
		return "", errors.New("image service busy") // only this item runs again
	}
	return image + "@2x", nil
}, workflow.MapConcurrency(2))
```

Every item runs even when others fail, so successes are kept for the next attempt; the error is then the join of every failed item's error, each labelled with its index, and the results are nil. A permanent error from any item makes the joined error permanent. `workflow.MapConcurrency(n)` runs at most `n` items at once; the default is one at a time. When the context ends, items not yet begun are skipped with its error.

Items are matched to their stored results by index, so keep their order stable across attempts. To run each item as a run of its own, with its own retries and worker and visible in run listings, use [child runs](children.md) instead.

## Loop until done with Loop

`workflow.Loop(ctx, name, init, limit, body)` calls `body` with the iteration number and the current state until `body` reports done, and returns the final state. Each iteration's state is memoised, so a retried step replays the finished iterations from the journal, without calling `body`, and resumes at the first one that did not finish:

<!-- snippet: examples/flow-memo/main.go#loop -->
```go
cursor, err := workflow.Loop(ctx, "pages", 0, 10, func(ctx context.Context, i int, cursor int) (int, bool, error) {
	if calls.add(fmt.Sprintf("page %d", cursor)) == 1 && cursor == 2 {
		return cursor, false, errors.New("API returned 502") // pages 0 and 1 are not fetched again
	}
	next := cursor + 1
	return next, next == 4, nil // done after page 3
})
```

`limit` is required and at least 1, so a loop is never unbounded by accident; reaching it without done returns `workflow.ErrLoopLimit`, marked permanent. A body error is returned as it is, and that iteration runs again on the next attempt. The state is stored as JSON after every iteration, so keep it small: a cursor or a counter, not the data.

`Loop` repeats code inside one step. A run's graph of steps is fixed when it starts, so to repeat a block of steps, enqueue a new run of the workflow from its last step.

## Process rows in chunks with progress

`workflow.ProcessChunks(ctx, name, opts, fn)` runs an export or import in chunks of `Size` items: it calls `fn` with each chunk's offset and size until `fn` handles fewer items than the size, reports progress after every chunk, and returns the number of items handled. Each chunk is a `Loop` iteration, so a retried step resumes at the first chunk that did not finish:

<!-- snippet: examples/flow-memo/main.go#chunks -->
```go
n, err := workflow.ProcessChunks(ctx, "rows", workflow.ChunkOptions{Total: 25, Size: 10},
	func(ctx context.Context, offset, size int64) (int64, error) {
		if calls.add(fmt.Sprintf("chunk %d", offset)) == 1 && offset == 10 {
			return 0, errors.New("disk full") // chunk 0 is not written again
		}
		return min(size, 25-offset), nil // rows written; fewer than size ends the loop
	})
```

`ChunkOptions.Total` is the item count when known, which turns progress into a percentage; zero means unknown. `MaxChunks` bounds the loop; it defaults to `Total/Size + 1` and is required when `Total` is zero, so a source that never runs dry cannot keep the step busy for ever. Reaching it is a permanent `ErrLoopLimit`.

Each report is a `workflow.Progress` with a percentage, a message such as `"20 of 25 items"`, and a `workflow.ChunkProgress` as JSON in `Data`. Read the latest one back from the run:

<!-- snippet: examples/flow-memo/main.go#progress -->
```go
page, err := c.(workflow.RunQuerier).Runs(ctx, workflow.RunQuery{Ref: ref, Limit: 1})
if err != nil {
	return err
}
p := page.Runs[0].Progress // the latest report: Percent, Message, and ChunkProgress as JSON in Data
var cp workflow.ChunkProgress
if err := json.Unmarshal(p.Data, &cp); err != nil {
	return err
}
fmt.Fprintf(out, "  progress %.0f%% %q, done %d of %d\n", p.Percent, p.Message, cp.Done, cp.Total)
```

The step asked for progress, so a driver that keeps none is not silently ignored: the first report's `UnsupportedError` fails the step, marked permanent. The memory, SQL and Redis drivers keep progress; Hatchet does not.

## Share a value across steps

A workflow has three places to keep data. A step's own input and output are passed along the graph by the [StepInput rule](steps.md#steps-that-depend-on-other-steps). Data from outside the run arrives through a wait step and `Client.Signal`, or through an event published with the bus's `events.WithSignal` option ([wait for an event](signals.md), [publish and subscribe](../events/publish-subscribe.md)). Between the two sits run data: `workflow.SetRunData(ctx, name, v)` stores `v` for the whole run, and `workflow.RunData[T](ctx, name)` reads it from any later step or attempt, without threading it through the steps in between:

<!-- snippet: examples/flow-memo/main.go#rundata -->
```go
{Name: "lookup", Run: func(ctx context.Context, in []byte) ([]byte, error) {
	err := workflow.SetRunData(ctx, "plan", "pro") // visible to every later step of this run
	return []byte(`"account created"`), err
}},
{Name: "provision", After: []string{"lookup"}, Run: func(ctx context.Context, in []byte) ([]byte, error) {
	return []byte(`"workspace ready"`), nil // does not pass the plan on
}},
{Name: "welcome", After: []string{"provision"}, Run: func(ctx context.Context, in []byte) ([]byte, error) {
	plan, ok, err := workflow.RunData[string](ctx, "plan")
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, workflow.Permanent(errors.New("no plan was set"))
	}
	return json.Marshal("welcome mail for the " + plan + " plan")
}},
```

`RunData` returns false when nothing was stored under the name. A later `SetRunData` of the same name replaces the value; two parallel steps setting one name leave whichever wrote last, so give them their own names. Run data writes through the journal, so it is as durable as the `JournalStore` and is forgotten with the run's journal when the run finishes.

## Write your own journal store

`workflow.JournalStore` has three methods: `Load(ctx, run, key)`, `Save(ctx, run, key, val)` and `Forget(ctx, run)`. Entries are keyed by run and by a key the helpers build from the step name and your name. A store must be safe for concurrent use, return a copy (or a value it never changes again) from `Load`, and replace an existing key on `Save`; forgetting a run with no entries is not an error. A store on the database your workers share makes memoised work survive a crash and a retry on another worker, which `MemoryJournal` cannot. The Redis driver's `redis.JournalStore` is one: a hash per run, so whichever worker resumes the run sees its entries. `redis.NewJournalStore(rdb, prefix, ttl)` takes a Redis client, a key prefix and a TTL that expires a run's hash that long after its last save, a safety net for runs whose `Forget` never comes (zero keeps entries until `Forget`); `Client.JournalStore()` builds one over the driver's own Redis and prefix, with no expiry. `MemoryJournal.Runs()` lists the runs that hold entries, for tests that check finished runs were forgotten.

## Try it

```sh
cd examples
go run ./flow-memo
```

```text
memo:
  result ch_1, charge ran 1 time(s), step ran 2 time(s)
map:
  result ["a.png@2x","b.png@2x","c.png@2x"]
  calls: a.png 1, b.png 2, c.png 1
loop:
  final cursor 4
  fetches: page 0 1, page 1 1, page 2 2, page 3 1
chunks:
  rows 25; calls: offset 0 1, offset 10 2, offset 20 1
  progress 100% "25 of 25 items", done 25 of 25
run data:
  "welcome mail for the pro plan"
choice:
  steps: load, route.decide, route.auto, route.review, route, notify
  40: "refunded 40 automatically"
  250: "refund of 250 sent to review"
no journal:
  ErrNoJournal=true permanent=true
journal:
  runs holding entries after every run finished: 0
```

Each step ran twice, and in each the work that had already succeeded ran once: the charge, images `a` and `c`, pages 0 and 1, and the chunk at offset 0.

## Pitfalls

- **`MemoryJournal` is in-process.** A retry on another worker process, or after a crash, finds no entries and does the work again. Use the Redis journal store, or a durable `JournalStore` of your own, when that matters.
- **Memoisation is at least once.** A crash between the work and the save repeats the work; pair a charge or an email with the remote side's idempotency key.
- **Change a memoised type with care.** A stored value that no longer decodes fails the step permanently; rename the memo when its result type changes.
- **Keep names stable and items in order.** A name built from something that changes between attempts (a timestamp, a random ID) never finds its entry, and `MapEach` matches results by index.
- **A failed run's journal is forgotten by default.** Pass `JournalKeepFailed` to resume a failed run retried by an operator from its memoised work.
- **`ProcessChunks` needs a driver that keeps progress**, and `fn` must be idempotent per chunk (an upsert, a file part named by its offset), since a crash after a chunk and before the save runs it again.

## Related

- [Workflows and steps](steps.md), [Branching](branching.md), [Child runs](children.md)
- [Retries](../jobs/retries.md), [Manage runs](../jobs/manage-runs.md) (retrying a failed run)
- [Plugins](../concepts/plugins.md)
- Example: [examples/flow-memo](../../examples/flow-memo/)
