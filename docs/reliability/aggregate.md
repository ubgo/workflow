# Aggregation and chunked fan-out

Two problems with the same shape: the number of runs does not match the work. Aggregation turns many small jobs into few runs: forty comments on a post in ten minutes become one digest mail per user instead of forty mails. Chunked fan-out turns one job into very many, safely: an announcement to a million recipients is enqueued a chunk at a time, under a rate limit, and a failure halfway resumes at the chunk that failed without mailing anyone twice.

`middleware.Aggregate` collects the payloads of an aggregated workflow's jobs in an `AggregateStore` and enqueues one flush run per group and window, whose step gets every collected item at once. `batch.EnqueueChunked` pages recipients inside a step and starts each chunk with `EnqueueMany`, resuming through the step's journal.

## Before you start

- Aggregation needs a client with `CapDelayedStart` (the memory, SQL, Redis and Hatchet drivers all have it) and the [Journal plugin](../workflows/memoisation.md) on the worker, so a retried flush step gets back the items it took.
- Where the items live decides the reach: `middleware.NewMemoryAggregate()` holds them in one process; `sqlstore.AggregateStore` (a table pair in your database) and `redis.AggregateStore` (one list per group) let every process enqueue into the same groups and every worker flush them.
- Chunked fan-out runs inside a step on any driver with the Journal plugin, and needs `CapIdempotencyKey` on the client the jobs go to (memory, SQL, Redis, Hatchet).
- The runnable program for this page is [examples/aggregate-digest](../../examples/aggregate-digest/).

## Aggregate jobs into one run per window

`middleware.AggregatePlugin(c, store, cfg)` bundles both halves for `workflow.Use`: the enqueue middleware (`Aggregate`) and the step middleware that hands the flush run its items as a JSON array (`AggregateInput`). Put it after the Journal plugin in the same `Use` call. `AggregateConfig` names the aggregated workflows, the `Group` a job joins (here the user; `""` sends the job as an ordinary run), the `Window` and the `MaxItems` cap:

<!-- snippet: examples/aggregate-digest/main.go#aggregate -->
```go
inner := memory.New()
store := middleware.NewMemoryAggregate() // sqlstore.AggregateStore or redis.AggregateStore across processes
agg, err := middleware.AggregatePlugin(inner, store, middleware.AggregateConfig{
	Workflows: []string{"comment-digest"},
	Group:     func(j workflow.Job) string { return j.Labels["user"] },
	Window:    50 * time.Millisecond, // one digest per user per window
	MaxItems:  3,                     // or sooner, once three comments are waiting
})
if err != nil {
	return err
}
// The Journal plugin comes first: a retried flush step gets back the
// comments it took.
c, err := workflow.Use(inner, workflow.Journal(workflow.NewMemoryJournal()), agg)
if err != nil {
	return err
}
```

The jobs are enqueued as usual. None becomes a run of its own: its payload is appended to its group, and a flush run is enqueued only when the group needs one:

<!-- snippet: examples/aggregate-digest/main.go#enqueue -->
```go
posts := []comment{{"alice", "c1"}, {"bob", "c1"}, {"alice", "c2"}, {"alice", "c3"}, {"alice", "c4"}, {"bob", "c2"}, {"alice", "c5"}}
for _, cm := range posts {
	payload, _ := json.Marshal(cm)
	_, err := c.Enqueue(ctx, workflow.Job{Workflow: "comment-digest", Payload: payload,
		Labels: map[string]string{"user": cm.User}})
	if err != nil {
		return err
	}
}
```

The flush run is a run of the same workflow. Its step receives the group's items, in the order they were appended, as one JSON array:

<!-- snippet: examples/aggregate-digest/main.go#flush -->
```go
err = c.Register(workflow.Workflow{Name: "comment-digest", Steps: []workflow.Step{{Name: "mail",
	Run: func(ctx context.Context, in []byte) ([]byte, error) {
		var comments []comment // AggregateInput turned the flush's items into a JSON array
		if err := json.Unmarshal(in, &comments); err != nil {
			return nil, workflow.Permanent(err)
		}
		if len(comments) == 0 {
			return nil, nil // an earlier flush took this chunk
		}
		texts := make([]string, len(comments))
		for i, cm := range comments {
			texts[i] = cm.Text
		}
		box.send(fmt.Sprintf("digest for %s: %s", comments[0].User, strings.Join(texts, ", ")))
		return nil, nil
	}}}})
```

Seven comments become three mails: Alice's third comment fills a chunk, so her first digest goes out at once; her last two and Bob's two go out when their windows close.

### When a flush run is enqueued

- The job that opens a group (its first item since the group was empty) enqueues a flush for the window's end: `Window` after the group's oldest pending item.
- With `MaxItems` set, the job that fills a chunk (the group's count reaches a multiple of `MaxItems`) enqueues a flush for now, and the job that opens the next chunk enqueues one for the window's end. A flush takes at most `MaxItems` items.
- Every other job enqueues nothing and returns a zero `RunRef` with a nil error: its item rides with the pending flush.

So there is one flush per window and chunk, never one per job, and always at least as many flushes pending as the stored items need. A flush that finds its chunk already taken by an earlier one gets an empty array and should return early. Items appended after a flush took its chunk go to the next flush.

### No item lost or delivered twice

`AggregateStore` has two methods: `Append(ctx, group, item, now)` returns the group's count and the time of its oldest pending item, and `Take(ctx, group, max)` removes and returns up to `max` of the oldest items. Both are atomic per group, so every item comes out of exactly one `Take`. The SQL store locks the group's row before reading or writing its items (the same statements on SQLite, Postgres and MySQL, no `RETURNING` needed); the Redis store runs each as one Lua script. `workflowtest.AggregateStoreContract` is the test every store passes, appenders and takers racing on one group.

The flush step takes its items once: `AggregatedItems` (which `AggregateInput` calls) keeps what it took as run data in the journal, so a retried step, a later step of the same run, or a run retried with `RunRetrier` gets the same items back rather than taking another chunk. Read the items yourself with `middleware.AggregatedItems(ctx, store)` when they are not JSON (`AggregateConfig.RawItems`) or when you want them as bytes.

### The SQL and Redis stores

`Client.AggregateStore()` on the SQL driver, or `sqlstore.NewAggregateStore(db, dialect, opts...)` over any `*sql.DB` (an app on Hatchet with a database), returns the durable store. Its two tables have a migration list of their own (owner `ubgo/workflow/sqlstore/aggregate`, `001_aggregate`): call the store's `Migrate`, which `Client.Migrate` does not do for you, or apply `AggregateStore.MigrationScripts()` in your own pipeline. A group's row outlives its items, so a busy group is locked by an update rather than re-inserted; `PruneEmpty` deletes the rows of empty groups now and then. In Redis, `redis.NewAggregateStore(rdb, prefix)` or `Client.AggregateStore()`.

## Fan out to many recipients in chunks

`batch.EnqueueChunked(ctx, c, name, opts, page)` runs inside a step. It calls `page` with each chunk's offset and size, enqueues the jobs it returns with `EnqueueMany` (one transaction on the SQL driver, one bulk request on Hatchet), and ends when a page returns fewer jobs than the chunk size. It is built on `workflow.ProcessChunks`, so each finished chunk is recorded in the journal and progress is reported after every chunk:

<!-- snippet: examples/aggregate-digest/main.go#chunked -->
```go
wf := workflow.Workflow{Name: "announce", Retry: workflow.RetryPolicy{Max: 3}, Steps: []workflow.Step{{Name: "send-all",
	Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		n, err := batch.EnqueueChunked(ctx, c, "recipients",
			batch.ChunkedOptions{ChunkOptions: workflow.ChunkOptions{Size: 10, Total: int64(recipients)}},
			func(ctx context.Context, offset, size int64) ([]workflow.Job, error) {
				mu.Lock()
				paged[offset]++
				first := paged[offset] == 1
				mu.Unlock()
				if offset == 10 && first {
					return nil, errors.New("recipient database timed out") // the step is retried from this chunk
				}
				var jobs []workflow.Job
				for i := offset; i < min(offset+size, int64(recipients)); i++ {
					jobs = append(jobs, workflow.Job{Workflow: "announce-one", Payload: fmt.Appendf(nil, "user-%02d", i)})
				}
				return jobs, nil
			})
		if err != nil {
			return nil, err
		}
		return json.Marshal(n)
	}}}}
```

The second page fails once, the step is retried, and it resumes at that page: the first chunk is not read or sent again, and every recipient gets one mail.

Each job gets an `IdempotencyKey` made from the run, the name and the job's index (or `ChunkedOptions.Key`), which covers the one window the journal cannot: a crash after a chunk was enqueued and before the journal recorded it. The retry sends that chunk again and the keys fold each job into its run from the first send, while that run is unfinished.

`ChunkedOptions.Limit` takes a `workflow.UpstreamLimit` and draws one unit per job before each chunk is enqueued, so the fan-out goes out at the rate the mail provider allows. A chunk that does not fit fails the step with the limit's retry-after error (or snoozes it, with `ChunkedOptions.Snooze` on a driver with `CapSnooze`), and the retry resumes at that chunk. Give the step's workflow `RetryPolicy{DelayFor: workflow.RetryAfterDelay}` so the retry waits for room, and enough attempts.

## Try it

```sh
cd examples
go run ./aggregate-digest
```

```text
-- Aggregate comments into digests
7 comments became 3 mails:
  digest for alice: c1, c2, c3
  digest for alice: c4, c5
  digest for bob: c1, c2
-- Fan an announcement out in chunks
enqueued 25 jobs; 25 mails to 25 distinct recipients
pages read: offset 0 1x, offset 10 2x, offset 20 1x
```

## Pitfalls

- `Aggregate` returns an `UnsupportedError` (matching `workflow.ErrUnsupported`) on a client without `CapDelayedStart`; a config without workflows or a positive `Window`, or a negative `MaxItems`, is `workflow.ErrInvalid`.
- A job of an aggregated workflow may not carry an `IdempotencyKey` or a `Chain` (`ErrInvalid`): the flush run is not that job's run. Its `RunAt` is replaced; its `Queue`, `Tenant`, `Priority`, `Retry` and labels are copied onto the flush run it triggers.
- Payloads must be JSON (`ErrInvalid` before anything is stored), since the step gets them as a JSON array; set `RawItems` and read them with `AggregatedItems` for other bytes.
- If the item is stored and its flush run cannot be enqueued, the error matches `middleware.ErrFlushNotEnqueued`. The item is kept and the group's next job in this process enqueues a flush at once. Do not enqueue the same job again: its payload is already in the group.
- Items are stored as the payload reaches `Aggregate`. A payload codec or claim check that runs before it in the enqueue chain stores sealed or referenced payloads, which are not JSON: put `Aggregate` first, or set `RawItems` and open the items in the step.
- Every process enqueuing a group must use the same `MaxItems`: the trigger rule counts on it.
- Call `AggregatedItems` from one step at a time. Two steps of one run taking in parallel would each take a chunk.
- A crash between a flush's take and the journal's save loses that chunk; with a durable `JournalStore` the window is two store calls wide. A flush run that fails for good keeps its items only in its journal: keep failed journals (`workflow.JournalKeepFailed`) to retry it with them.
- `EnqueueChunked` returns a Permanent `UnsupportedError` without `CapIdempotencyKey` on the client, and a Permanent `ErrInvalid` outside a step (no `StepInfo`) unless `ChunkedOptions.Key` is set, or for a page that returns more jobs than the chunk size.
- An idempotency key holds only while its run is unfinished. A recipient whose run already finished before a chunk was sent again gets a second message: keep the job's own handler idempotent, or add long-lived dedup such as [unique jobs](unique-jobs.md), when that matters.
- `ChunkedOptions.Size` must not exceed the limit's capacity, or every chunk is refused (Permanent).

## Related

- [Debounce](debounce.md): keep the last job of a burst instead of all of them.
- [Memoise work inside a step](../workflows/memoisation.md): the journal, `ProcessChunks` and run data both helpers are built on.
- [Batches](../batches/batches.md): many jobs as one unit with callbacks, and `EnqueueMany`.
- Example: [examples/aggregate-digest](../../examples/aggregate-digest/).
