# flow-joins

Joins over child runs on the in-memory driver. `workflow.JoinChildren` asks three price mirrors and continues on the first answer, cancelling the others, then asks four and continues once two have answered. `workflow.MapRuns` runs one sync run per product from a paged source, three at a time, and tolerates one failed product. Three parallel branches of an order each start their own notification run (a multi-merge).

```sh
cd examples
go run ./flow-joins
```

```text
discriminator:
  first answer: "eu: 42"
  mirrors: 1 succeeded, 2 cancelled
partial join:
  two of four answered, winners: [0,2]
distributed map:
  started 10, succeeded 9, failed 1 (item 4)
multi-merge:
  one notification per branch: packed, paid, shipped
```

## The code

The first child to succeed wins, the rest are cancelled:

<!-- snippet: examples/flow-joins/main.go#first -->
```go
res, err := workflow.JoinChildren(ctx, c, "mirrors", mirrors("slow", "eu", "slow"), workflow.JoinSpec{
	Need: 1,
	Rest: workflow.JoinCancelRest,
	Poll: 5 * time.Millisecond, // how often the join re-reads its children
})
if err != nil {
	return nil, err // a Snooze while no mirror has answered yet
}
return res.Outputs[0], nil
```

Two of four:

<!-- snippet: examples/flow-joins/main.go#quorum -->
```go
res, err := workflow.JoinChildren(ctx, c, "mirrors", mirrors("eu", "down", "us", "slow"), workflow.JoinSpec{
	Need: 2, // two answers make a quorum
	Rest: workflow.JoinCancelRest,
	Poll: 5 * time.Millisecond, // how often the join re-reads its children
})
if err != nil {
	return nil, err
}
return json.Marshal(res.Winners)
```

One child run per item of a paged source:

<!-- snippet: examples/flow-joins/main.go#map -->
```go
res, err := workflow.MapRuns(ctx, c, "products", products, func(_ int, id int) (workflow.Job, error) {
	return workflow.Job{Workflow: "product", Payload: []byte(strconv.Itoa(id))}, nil
}, workflow.MapRunsOptions{InFlight: 3, MaxFailures: 1, Poll: 5 * time.Millisecond}) // 3 children at a time, 1 failure allowed
if err != nil {
	return nil, err
}
return json.Marshal(res)
```

Each branch starts its own downstream run:

<!-- snippet: examples/flow-joins/main.go#multi-merge -->
```go
// branch is one parallel step that starts its own downstream run.
branch := func(name string) workflow.Step {
	return workflow.Step{Name: name, Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		_, err := workflow.Spawn(ctx, c, "downstream", []workflow.Job{{Workflow: "notify", Payload: []byte(`"` + name + `"`)}})
		return nil, err
	}}
}
```

Cancelling every child of a step is in [flow-cancel](../flow-cancel).

The guide is [docs/workflows/joins.md](../../docs/workflows/joins.md).
