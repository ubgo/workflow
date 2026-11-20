# flow-joins

Joins over child runs on the in-memory driver. `workflow.JoinChildren` asks three price mirrors and continues on the first answer, cancelling the others, then asks four and continues once two have answered. `workflow.MapRuns` runs one sync run per product from a paged source, three at a time, and tolerates one failed product.

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
```

## The code

The first child to succeed wins, the rest are cancelled:

<!-- snippet: examples/flow-joins/main.go#first -->
```go
res, err := workflow.JoinChildren(ctx, c, "mirrors", mirrors("slow", "eu", "slow"), workflow.JoinSpec{
	Need: 1,
	Rest: workflow.JoinCancelRest,
	Poll: 5 * time.Millisecond,
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
	Need: 2,
	Rest: workflow.JoinCancelRest,
	Poll: 5 * time.Millisecond,
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
}, workflow.MapRunsOptions{InFlight: 3, MaxFailures: 1, Poll: 5 * time.Millisecond})
if err != nil {
	return nil, err
}
return json.Marshal(res)
```

The guide is [docs/workflows/joins.md](../../docs/workflows/joins.md).
