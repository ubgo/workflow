# flow-stages

Step-layout helpers on the in-memory driver: `workflow.Stages` runs groups of parallel steps one group after another, `workflow.When` runs a step only on a condition and otherwise passes its input on, `workflow.Milestone` holds a step until the run reaches a state, and `middleware.SoftTimeout` warns a long step before its hard timeout so it can stop with its progress saved.

```sh
cd examples
go run ./flow-stages
```

```text
stages:
  merge after both fetches: true
  publishing after merge: true
  run result: {"publish-web":"publish-web","publish-feed":"publish-feed"}
when:
  "icon.png" -> "icon.png"
  "poster-large.png" -> resized "poster-large.png"
milestone:
  before payment the run has ended: false
  after payment: {"pay":null,"ship":"shipped"}
soft timeout:
  step returned before its hard timeout: true
```

## The code

Three stages, the first and last with two parallel steps:

<!-- snippet: examples/flow-stages/main.go#stages -->
```go
steps, err := workflow.Stages(
	[]workflow.Step{{Name: "fetch-prices", Run: ran.step("fetch-prices")}, {Name: "fetch-stock", Run: ran.step("fetch-stock")}},
	[]workflow.Step{{Name: "merge", Run: ran.step("merge")}},
	[]workflow.Step{{Name: "publish-web", Run: ran.step("publish-web")}, {Name: "publish-feed", Run: ran.step("publish-feed")}},
)
if err != nil {
	return err
}
```

A step that resizes only large images and passes small ones on unchanged:

<!-- snippet: examples/flow-stages/main.go#when -->
```go
resize := func(_ context.Context, in []byte) ([]byte, error) {
	return append([]byte("resized "), in...), nil
}
isLarge := func(_ context.Context, in []byte) (bool, error) {
	return len(in) > 10, nil
}
images := workflow.Workflow{Name: "images", Steps: []workflow.Step{
	{Name: "resize", Run: workflow.When(isLarge, resize)},
	{Name: "store", After: []string{"resize"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
		return in, nil
	}},
}}
```

A step enabled only once the run's state is `paid`:

<!-- snippet: examples/flow-stages/main.go#milestone -->
```go
order := workflow.Workflow{Name: "order", Steps: []workflow.Step{
	{Name: "pay", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		<-release // the payment provider answers
		return nil, workflow.SetRunData(ctx, "state", "paid")
	}},
	{Name: "ship", Run: workflow.Milestone("state", "paid", func(context.Context, []byte) ([]byte, error) {
		return []byte(`"shipped"`), nil
	}, workflow.MilestoneOptions{Poll: 5 * time.Millisecond})},
}}
```

A long step that stops at its soft deadline, keeping its progress for the next attempt:

<!-- snippet: examples/flow-stages/main.go#soft -->
```go
soft := middleware.SoftTimeout(middleware.SoftTimeoutConfig{After: 20 * time.Millisecond})
export := workflow.Workflow{Name: "export", Steps: []workflow.Step{{
	Name:    "rows",
	Timeout: 50 * time.Millisecond,
	Run: soft(func(ctx context.Context, _ []byte) ([]byte, error) {
		done, err := workflow.Memo(ctx, "progress", func(context.Context) (int, error) { return 0, nil })
		if err != nil {
			return nil, err
		}
		for done < 1_000_000 {
			select {
			case <-middleware.SoftDone(ctx):
				return json.Marshal(fmt.Sprintf("stopped early, %d rows saved for the next attempt", done))
			case <-time.After(time.Millisecond):
				done += 1000
			}
		}
		return json.Marshal("all rows")
	}),
}}}
```

The guide is [docs/workflows/stages.md](../../docs/workflows/stages.md).
