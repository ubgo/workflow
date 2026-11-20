# aggregate-digest

Two ways to change how many runs a burst of work becomes, on the in-memory driver. Aggregation: seven comments become three digest mails, one per user and window (or sooner, once three are waiting), with `middleware.AggregatePlugin` collecting the payloads in a `MemoryAggregate` and one flush run taking them. Chunked fan-out: an announcement goes to 25 recipients ten at a time with `batch.EnqueueChunked`; the second page fails once, and the retried step resumes there without mailing anyone twice.

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

## The code

The plugin comes after the Journal plugin, so a retried flush gets back the items it took:

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

The flush run's step gets the group's items as one JSON array:

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

The fan-out pages recipients inside a step and enqueues a chunk at a time:

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

The guide that copies this program is [aggregation and chunked fan-out](../../docs/reliability/aggregate.md). `go test ./aggregate-digest` checks every line.
