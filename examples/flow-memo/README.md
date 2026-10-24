# flow-memo

The flow helpers that keep work inside a step from repeating when the step is retried, on the in-memory driver: `Memo` around a card charge, `MapEach` over a list of images, `Loop` over a paged API, `ProcessChunks` for an export with progress, run-scoped data with `SetRunData` and `RunData`, and an exclusive choice built with `Choice`. Each workflow fails once on purpose, so the output shows what ran again and what did not.

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

## The code

The Journal plugin turns memoisation on; every helper below needs it:

<!-- snippet: examples/flow-memo/main.go#client -->
```go
journal := workflow.NewMemoryJournal()
c, err := workflow.Use(memory.New(), workflow.Journal(journal))
if err != nil {
	return err
}
```

A charge memoised under a name: the step fails after it, is retried, and the charge is not made again:

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

One memoised call per item; only the item that failed runs again:

<!-- snippet: examples/flow-memo/main.go#map -->
```go
sizes, err := workflow.MapEach(ctx, "resize", images, func(ctx context.Context, i int, image string) (string, error) {
	if calls.add("resize "+image) == 1 && image == "b.png" {
		return "", errors.New("image service busy") // only this item runs again
	}
	return image + "@2x", nil
}, workflow.MapConcurrency(2))
```

A loop whose finished iterations replay from the journal:

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

An export in chunks of ten, with progress after every chunk, read back from the run:

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

A value set by the first step and read by the third:

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

A choice between two branches, expanded into ordinary steps:

<!-- snippet: examples/flow-memo/main.go#choice -->
```go
steps, err := workflow.Choice(workflow.ChoiceSpec{
	Name:  "route",
	After: []string{"load"},
	Decide: func(ctx context.Context, in []byte) (string, error) {
		var amount int
		if err := json.Unmarshal(in, &amount); err != nil {
			return "", workflow.Permanent(err)
		}
		if amount < 100 {
			return "auto", nil
		}
		return "review", nil
	},
	Branches: []workflow.Branch{
		{Name: "auto", Run: func(ctx context.Context, in []byte) ([]byte, error) {
			return []byte(`"refunded ` + string(in) + ` automatically"`), nil
		}},
		{Name: "review", Run: func(ctx context.Context, in []byte) ([]byte, error) {
			return []byte(`"refund of ` + string(in) + ` sent to review"`), nil
		}},
	},
})
if err != nil {
	return workflow.Workflow{}, err
}
load := workflow.Step{Name: "load", Run: func(ctx context.Context, in []byte) ([]byte, error) {
	return in, nil // the refund amount, from the payload
}}
notify := workflow.Step{Name: "notify", After: []string{"route"}, Run: func(ctx context.Context, in []byte) ([]byte, error) {
	return in, nil // the chosen branch's output, unchanged
}}
wf := workflow.Workflow{Name: "refund", Steps: append(append([]workflow.Step{load}, steps...), notify)}
```

The guides that copy this program are [memoise work inside a step](../../docs/workflows/memoisation.md) and [branching](../../docs/workflows/branching.md). `go test ./flow-memo` checks every line.
