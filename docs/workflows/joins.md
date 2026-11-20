# Joins and distributed maps

A step that starts child runs does not always need every one of them. A price lookup asks three mirrors and takes the first answer; a quorum write continues once two of three replicas confirm; a catalogue sync starts a run per product, a million of them, and should not give up because three products have bad data. `workflow.JoinChildren` waits for some of a step's children and decides what becomes of the rest, and `workflow.MapRuns` runs a child per item of a source too large to hold in memory. Both wait without holding a worker and survive a retried or restarted step.

## Before you start

- Both build on [child runs](children.md): the client needs the Journal plugin, and `workflow.CapIdempotencyKey` so a retried step never starts a child twice.
- They wait by snoozing the parent's step, so the parent's driver needs `workflow.CapSnooze` (memory, SQL and Redis declare it; Hatchet does not, and both refuse there with a permanent `*workflow.UnsupportedError` before starting any child).
- Reading the winners' results needs `workflow.CapResult`; cancelling the rest needs `workflow.CapCancel`; completing it needs `workflow.CapForceRun`. Each is checked before any child starts.
- The example is [`examples/flow-joins`](../../examples/flow-joins). Run it with `go run ./flow-joins` from `examples/`.

## Continue on the first child to succeed

`workflow.JoinChildren(ctx, client, name, jobs, spec)` starts `jobs` as children of the calling step, like `AwaitChildren`, and passes once `spec.Need` of them have succeeded. With `Need: 1` it is a discriminator: the first child to succeed wins.

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

The `workflow.JoinResult` lists every child started (`Refs`), the indexes of the children that counted (`Winners`) and their results (`Outputs`). While the join is undecided it returns `workflow.Snooze(spec.Poll)`, so return its error from the step as it is. Once decided, the decision is kept in the run's journal, so a retried step gets the same winners.

## Continue on N of M

`Need: n` is a partial join: the step continues once n children have succeeded. `Need: 0` waits for every child, as `AwaitChildren` does.

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

When more than `Need` children have succeeded by the time the join looks, the winners are picked in job order, not in the order they finished: a join cannot see the order of two ends between two looks. Keep `Poll` short when the first finisher matters.

When the children can no longer reach `Need` (too many failed), the join fails the step with a permanent `*workflow.JoinFailedError` naming each child that did not succeed and how it ended.

## Decide what happens to the rest

`spec.Rest` says what becomes of the children still running once the join is decided:

| `Rest` | The children still running | Pattern |
|---|---|---|
| `workflow.JoinKeepRest` (the zero value) | keep running; their ends are ignored | structured discriminator, structured partial join |
| `workflow.JoinCancelRest` | are cancelled | cancelling discriminator, cancelling partial join |
| `workflow.JoinCompleteRest` | are forced to succeed without running their remaining steps (`RunForcer.ForceComplete`) | complete a multiple-instance task |

A join that fails cancels the children still running with either `JoinCancelRest` or `JoinCompleteRest`, so a failed join leaves nothing behind.

## Add children while waiting

`spec.More` lets a join grow while it waits (a dynamic partial join): it is called on every look and returns every job added so far beyond `jobs`, and `closed` once no more will come. It must be append-only, since each job is started once, keyed by its index. While `More` is open the join waits rather than fail.

## Cancel every child of a step

`workflow.CancelChildren(ctx, client, runID, step)` cancels every unfinished child that a step of a run spawned (an empty step means every step of the run), from inside the run or from an operator's tool. It finds them by the parent labels `Spawn` writes (`workflow.ChildLabels` builds the same query), so the children's client needs `workflow.CapCancelAll`.

## Run downstream work once per arriving branch

A step's `After` joins its parents: it runs once, after all of them. When each branch should start the downstream work on its own as it arrives (a multi-merge: "paid", "packed" and "shipped" each send a notification), let each branch start it as a child run with `Spawn` instead of listing it as a later step:

```go
branch := func(name string) workflow.Step {
	return workflow.Step{Name: name, Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		_, err := workflow.Spawn(ctx, c, "downstream", []workflow.Job{{Workflow: "notify", Payload: []byte(`"` + name + `"`)}})
		return nil, err
	}}
}
```

Each branch starts one run, and a retried branch never starts it twice. This multiplies runs by design, so use it only when every arrival really needs its own downstream run.

## Run a child per item of a large source

`workflow.MapRuns(ctx, client, name, page, job, opts)` reads items a page at a time from `page`, turns each into a child job with `job`, and keeps at most `opts.InFlight` children running (ten by default):

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

Each look retires the children that ended, starts new ones up to `InFlight` and journals its progress after every child it starts, so a step retried after a crash continues where it left off; each child carries an idempotency key made from the run, the step, `name` and the item's index, so a crash between an enqueue and the save starts no item twice while its run is unfinished. `MaxFailures` is how many items may fail (`workflow.MapUnlimitedFailures` for any number); past it, the children still running are cancelled and the step fails with a permanent `*workflow.MapRunsError`. The `workflow.MapRunsResult` counts started, succeeded and failed items and lists the first `KeepFailed` failures (a hundred by default) with their index.

## Pitfalls

- `page` must return the same items for the same cursor: a look after a crash may ask for a page again.
- Items fetched but not yet started are kept in the journal as JSON between looks. Keep pages small.
- A join's winners are decided in job order between two looks, not by finish time.
- `JoinKeepRest` leaves children running after the parent moved on. They still use workers and can still fail; use `JoinCancelRest` when their work is no longer wanted.
