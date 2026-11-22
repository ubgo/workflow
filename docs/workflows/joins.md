# Joins and distributed maps

A step that starts child runs does not always need every one of them. A price lookup asks three mirrors and takes the first answer; a quorum write continues once two of three replicas confirm; a catalogue sync starts a run per product, a million of them, and should not give up because three products have bad data. [`workflow.JoinChildren`](ds:block?id=joinchildren-a95h7rs8) waits for some of a step's children and decides what becomes of the rest, and [`workflow.MapRuns`](ds:block?id=mapruns-etngp9jc) runs a child per item of a source too large to hold in memory. Both wait without holding a worker and survive a retried or restarted step.

## Before you start

- Both build on [child runs](children.md): the client needs the Journal plugin ([`workflow.Journal`](ds:block?id=journal-pr52wnjp)), and [`workflow.CapIdempotencyKey`](ds:block?id=capidempotencykey-5wcqqwy8) so a retried step never starts a child twice ([`Spawn`](ds:block?id=spawn-jrs3a39f)).
- They wait by snoozing the parent's step, so the parent's driver needs [`workflow.CapSnooze`](ds:block?id=capsnooze-pffu58xc) ([memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-caps-yu37ef2j) and [Redis](ds:block?id=redis-caps-bfgdst9t) declare it; [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z) does not, and both refuse there with a permanent `*workflow.UnsupportedError` before starting any child: [`checkJoin`](ds:block?id=checkjoin-5zxqmnan), [`MapRuns`](ds:block?id=mapruns-etngp9jc)).
- Reading the winners' results needs [`workflow.CapResult`](ds:block?id=capresult-9dusbjaz); cancelling the rest needs [`workflow.CapCancel`](ds:block?id=capcancel-73j3f5s3); completing it needs [`workflow.CapForceRun`](ds:block?id=capforcerun-ybb9dzyt). Each is checked before any child starts ([`checkJoin`](ds:block?id=checkjoin-5zxqmnan)).
- [`CancelChildren`](ds:block?id=cancelchildren-hh6njpmy) needs [`workflow.CapCancelAll`](ds:block?id=capcancelall-7enkwcsq) on the children's client.
- The examples are [`examples/flow-joins`](../../examples/flow-joins) (joins, maps, multi-merge) and [`examples/flow-cancel`](../../examples/flow-cancel) (cancel every child of a step). Run them with `go run ./flow-joins` and `go run ./flow-cancel` from `examples/`.

## Continue on the first child to succeed

[`workflow.JoinChildren(ctx, client, name, jobs, spec)`](ds:block?id=joinchildren-a95h7rs8) starts `jobs` as children of the calling step, like `AwaitChildren`, and passes once [`spec.Need`](ds:block?id=joinspec-p4n46erj) of them have succeeded. With `Need: 1` it is a discriminator: the first child to succeed wins.

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

The [`workflow.JoinResult`](ds:block?id=joinresult-uucbtd8w) lists every child started (`Refs`), the indexes of the children that counted (`Winners`) and their results (`Outputs`). While the join is undecided it returns `workflow.Snooze(spec.Poll)`, so return its error from the step as it is. Once decided, the decision is kept in the run's journal, so a retried step gets the same winners ([`JoinChildren`](ds:block?id=joinchildren-a95h7rs8)).

## Continue on N of M

`Need: n` is a partial join: the step continues once n children have succeeded. `Need: 0` waits for every child, as `AwaitChildren` does ([`JoinSpec`](ds:block?id=joinspec-p4n46erj)).

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

When more than `Need` children have succeeded by the time the join looks, the winners are picked in job order, not in the order they finished ([`JoinChildren`](ds:block?id=joinchildren-a95h7rs8)): a join cannot see the order of two ends between two looks. Keep `Poll` short when the first finisher matters.

When the children can no longer reach `Need` (too many failed), the join fails the step with a permanent [`*workflow.JoinFailedError`](ds:block?id=joinfailederror-r52ajzv8) naming each child that did not succeed and how it ended.

## Decide what happens to the rest

`spec.Rest` says what becomes of the children still running once the join is decided:

| `Rest` | The children still running | Pattern |
|---|---|---|
| [`workflow.JoinKeepRest`](ds:block?id=joinkeeprest-88sa9sg4) (the zero value) | keep running; their ends are ignored | structured discriminator, structured partial join |
| [`workflow.JoinCancelRest`](ds:block?id=joincancelrest-j4ju6z9r) | are cancelled | cancelling discriminator, cancelling partial join |
| [`workflow.JoinCompleteRest`](ds:block?id=joincompleterest-ae3vwxd7) | are forced to succeed without running their remaining steps (`RunForcer.ForceComplete`) | complete a multiple-instance task |

A join that fails cancels the children still running with either `JoinCancelRest` or `JoinCompleteRest` ([`JoinChildren`](ds:block?id=joinchildren-a95h7rs8), [`settleRest`](ds:block?id=settlerest-53yzjzvw)), so a failed join leaves nothing behind.

## Add children while waiting

[`spec.More`](ds:block?id=joinspec-p4n46erj) lets a join grow while it waits (a dynamic partial join): it is called on every look and returns every job added so far beyond `jobs`, and `closed` once no more will come. It must be append-only, since each job is started once, keyed by its index. While `More` is open the join waits rather than fail.

## Cancel every child of a step

A step that spawned children with `Spawn` (or `JoinChildren` with `JoinKeepRest`) moves on and leaves them running. When their work is no longer wanted (the order was cancelled, the album deleted), [`workflow.CancelChildren(ctx, client, runID, step)`](ds:block?id=cancelchildren-hh6njpmy) cancels every unfinished child that `step` of the run spawned and returns how many it cancelled. Call it from a later step of the run (with `workflow.StepInfoFrom(ctx)` for the run ID) or from an operator's tool that holds the parent's ref. The example is [`examples/flow-cancel`](../../examples/flow-cancel): an album spawns three renders from its `render-all` step and succeeds while they run.

<!-- snippet: examples/flow-cancel/main.go#cancel-children -->
```go
n, err := workflow.CancelChildren(ctx, c, ref.RunID, "render-all")
if err != nil {
	return err
}
fmt.Fprintf(w, "  cancelled %d children\n", n)
```

An empty `step` cancels the children of every step of the run. The call finds the children by the parent labels `Spawn` wrote and cancels them with one `BulkCanceller.CancelAll`, so the children's client needs [`workflow.CapCancelAll`](ds:block?id=capcancelall-7enkwcsq) and must filter by those labels and by every unfinished status (the [memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-caps-yu37ef2j), [Redis](ds:block?id=redis-caps-bfgdst9t) and [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z) drivers do); a client without it is refused with an `*workflow.UnsupportedError` before anything is cancelled, and a zero run ID with an error matching `workflow.ErrInvalid`. Only children that are scheduled (a delayed start), queued, running, waiting or retrying count ([`unfinishedStatuses`](ds:block?id=unfinishedstatuses-uvmyv6mp)): a second call returns 0. The retrying status is asked for only on a client with [`workflow.CapRunStates`](ds:block?id=caprunstates-qugshrv4); elsewhere a child in retry backoff reads queued or running and is cancelled as one.

[`workflow.ChildLabels(runID, step)`](ds:block?id=childlabels-se7ep9y5) builds the same label filter (`LabelParentRun`, plus `LabelParentStep` when `step` is not empty), so a `RunQuery` lists the children, for a report or to cancel only some of them:

<!-- snippet: examples/flow-cancel/main.go#child-labels -->
```go
page, err := c.(workflow.RunQuerier).Runs(ctx, workflow.RunQuery{
	Labels: workflow.ChildLabels(ref.RunID, "render-all"),
})
if err != nil {
	return err
}
for _, r := range page.Runs {
	fmt.Fprintf(w, "  child %s of step %s: %s\n", r.Payload, r.Labels[workflow.LabelParentStep], r.Status)
}
```

## Run downstream work once per arriving branch

A step's `After` joins its parents: it runs once, after all of them. When each branch should start the downstream work on its own as it arrives (a multi-merge: "paid", "packed" and "shipped" each send a notification), let each branch start it as a child run with [`Spawn`](ds:block?id=spawn-jrs3a39f) instead of listing it as a later step:

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

Each branch starts one run, and a retried branch never starts it twice: in the example the three branches of `order-events` start three `notify` runs, in whatever order the branches finish. This multiplies runs by design, so use it only when every arrival really needs its own downstream run.

## Run a child per item of a large source

[`workflow.MapRuns(ctx, client, name, page, job, opts)`](ds:block?id=mapruns-etngp9jc) reads items a page at a time from `page`, turns each into a child job with `job`, and keeps at most [`opts.InFlight`](ds:block?id=maprunsoptions-mg733853) children running ([`DefaultMapInFlight`](ds:block?id=defaultmapinflight-nngs8xvm) by default):

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

Each look retires the children that ended, starts new ones up to `InFlight` and journals its progress after every child it starts, so a step retried after a crash continues where it left off; each child carries an idempotency key made from the run, the step, `name` and the item's index, so a crash between an enqueue and the save starts no item twice while its run is unfinished. `MaxFailures` is how many items may fail ([`workflow.MapUnlimitedFailures`](ds:block?id=mapunlimitedfailures-s5r92k9m) for any number); past it, the children still running are cancelled and the step fails with a permanent [`*workflow.MapRunsError`](ds:block?id=maprunserror-6agjduvk). The [`workflow.MapRunsResult`](ds:block?id=maprunsresult-jus3jnwu) counts started, succeeded and failed items and lists the first `KeepFailed` failures ([`DefaultMapKeepFailed`](ds:block?id=defaultmapkeepfailed-pszz2thj) by default) with their index.

## Try it

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

The slow mirrors were still running when the first answer won, and were cancelled. Cancelling every child of a step is in the second program:

```sh
go run ./flow-cancel
```

```text
cancel every child of a step:
  album succeeded, 3 renders running
  cancelled 3 children
  child 3 of step render-all: cancelled
  child 2 of step render-all: cancelled
  child 1 of step render-all: cancelled
  cancel again: 0 children (only unfinished ones count)
withdraw several steps:
  quote succeeded with "cached feed-eu + cached feed-us"
  withdraw again: ErrWrongState true, both steps named true
deliver only to runs waiting now:
  before any run waits: resumed 0
  the run started later is still waiting: the first nudge was not kept
  while it waits: resumed 1
  run succeeded
```

The run listing is newest first, so the children print in reverse order.

## Pitfalls

- `page` must return the same items for the same cursor: a look after a crash may ask for a page again ([`MapRuns`](ds:block?id=mapruns-etngp9jc)).
- Items fetched but not yet started are kept in the journal as JSON between looks ([`MapRuns`](ds:block?id=mapruns-etngp9jc)). Keep pages small.
- A join's winners are decided in job order between two looks, not by finish time ([`JoinChildren`](ds:block?id=joinchildren-a95h7rs8)).
- [`JoinKeepRest`](ds:block?id=joinkeeprest-88sa9sg4) leaves children running after the parent moved on. They still use workers and can still fail; use `JoinCancelRest` when their work is no longer wanted.
- [`CancelChildren`](ds:block?id=cancelchildren-hh6njpmy) finds children by their parent labels only ([`ChildLabels`](ds:block?id=childlabels-se7ep9y5)). Runs a step enqueued with plain `Enqueue` carry no such labels and are not cancelled; start them with `Spawn`.
- `CancelChildren` cancels children; it does not cancel the parent. Cancelling a parent does not cancel its children either ([child runs](children.md#pitfalls)), so call `CancelChildren` from the tool that cancels the parent.
- The children's client must declare [`CapCancelAll`](ds:block?id=capcancelall-7enkwcsq). When children run on another backend than the parent, pass that backend's client.

## Related

- [Child runs](children.md): `Spawn` and `AwaitChildren`, which joins build on
- [Memoise work inside a step](memoisation.md#turn-on-the-journal): the journal that keeps a join's decision
- [Promises, updates and signal helpers](promises.md#withdraw-several-steps-of-a-run): withdraw steps of one run instead of whole child runs
- [Manage runs](../jobs/manage-runs.md): cancel, list and bulk-cancel runs
- Examples: [examples/flow-joins](../../examples/flow-joins/), [examples/flow-cancel](../../examples/flow-cancel/)
