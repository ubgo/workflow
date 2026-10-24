# Child runs

A step sometimes needs to start more work than it should do itself: a sign-up that sends an email and a text, an album that resizes each of its images. Started as child runs, each piece has its own retries, runs on any free worker and shows in run listings. `workflow.Spawn` starts children from inside a step and moves on; `workflow.AwaitChildren` starts them and gives the step their results once every one has ended, without holding a worker while they run. A retried step never starts its children twice.

## Before you start

- Both helpers memoise the children they started, so the client needs the [Journal plugin](memoisation.md#turn-on-the-journal).
- `Spawn` gives each child an idempotency key and needs `workflow.CapIdempotencyKey`, which every driver declares; a client without it is refused unless you pass `workflow.SpawnAllowDuplicates()`.
- `AwaitChildren` reads the children's results, so the children's client needs `workflow.CapResult` (it implements `workflow.ResultReader`). It waits by snoozing the parent's step, so the parent's driver needs `workflow.CapSnooze`; without it `AwaitChildren` refuses with a permanent `*workflow.UnsupportedError` (field `AwaitChild`, missing `CapSnooze`) before starting any child, because every look at an unfinished child would spend an attempt of the parent's step. The memory and Redis drivers declare `CapSnooze`; Hatchet does not; the SQL driver declares it too.
- The example is [`examples/flow-children`](../../examples/flow-children). Run it with `go run ./flow-children` from `examples/`.

## Start children with Spawn

`workflow.Spawn(ctx, client, name, jobs, opts...)` enqueues each job as a run of its own and returns their `RunRef`s in order, without waiting for them. A step has no client of its own, so pass the one the app enqueues with, reached through a closure; another client spawns onto another backend.

<!-- snippet: examples/flow-children/main.go#spawn -->
```go
refs, err := workflow.Spawn(ctx, c, "notify", []workflow.Job{
	{Workflow: "notify", Payload: []byte(`"email"`)},
	{Workflow: "notify", Payload: []byte(`"sms"`)},
})
if err != nil {
	return nil, err
}
```

A retried step does not start its children again: each child's `RunRef` is memoised under `name` and the job's index. A job without an `IdempotencyKey` gets one derived from the run, the step, `name` and the index, which also covers a crash between the enqueue and the journal's save; a job that already has a key keeps it. `SpawnAllowDuplicates` lets `Spawn` run on a client without `CapIdempotencyKey`, accepting that such a crash can start a child twice.

Every child carries two labels under the reserved internal prefix `ubgo.workflow/`: `workflow.LabelParentRun`, the parent's run ID, and `workflow.LabelParentStep`, the step that spawned it. `Spawn` sets them itself, overwriting any value the job carried. A driver's `Enqueue` refuses app labels under a reserved prefix (`workflow.CheckJobLabels`), and `Spawn` allows only its own two keys, so on a driver that enforces `CheckJobLabels` these labels cannot arrive by accident or from untrusted input; like every reserved label they are a guard, not a sandbox. A run listing finds a run's children by them:

<!-- snippet: examples/flow-children/main.go#children -->
```go
page, err := c.(workflow.RunQuerier).Runs(ctx, workflow.RunQuery{Workflow: "notify"})
if err != nil {
	return err
}
for _, r := range page.Runs {
	fmt.Fprintf(out, "  child of %s, parent run matches: %v\n",
		r.Labels[workflow.LabelParentStep], r.Labels[workflow.LabelParentRun] == ref.RunID.String())
}
```

## Wait for children with AwaitChildren

`workflow.AwaitChildren(ctx, client, name, jobs, opts)` spawns the jobs, and while any child is unfinished it returns `workflow.Snooze(opts.Poll)`, which puts the parent's step back to ask again later. Once every child has ended, it returns their results in the order of `jobs`. `workflow.AwaitChild` is the same for one job.

<!-- snippet: examples/flow-children/main.go#await -->
```go
jobs := make([]workflow.Job, len(images))
for i, img := range images {
	jobs[i] = workflow.Job{Workflow: "resize", Payload: []byte(`"` + img + `"`)}
}
outs, err := workflow.AwaitChildren(ctx, c, "resize", jobs, workflow.ChildOptions{Poll: 5 * time.Millisecond})
if err != nil {
	return nil, err // a Snooze while children run; a failed child fails this step
}
names := make([]string, len(outs))
for i, o := range outs {
	if err := json.Unmarshal(o, &names[i]); err != nil {
		return nil, workflow.Permanent(err)
	}
}
return json.Marshal(names)
```

Return its error as it is: a snooze must reach the driver to put the step back. `ChildOptions.Poll` is the wait between looks, `workflow.DefaultChildPoll` (5 seconds) when zero; `ChildOptions.Spawn` passes options to `Spawn`. `ChildOptions.Parent` names the parent's client when the children run on another backend, so the `CapSnooze` check looks at the parent's driver; nil means the client the children run on, the usual case of one backend.

The first child that did not succeed fails the parent's step with a permanent `*workflow.RunFailedError` (asking again would see the same end), and the parent's run fails with it:

<!-- snippet: examples/flow-children/main.go#failed -->
```go
ref, err = c.Enqueue(ctx, workflow.Job{Workflow: "album", Payload: []byte(`["a.png","bad.png"]`)})
if err != nil {
	return err
}
_, err = workflow.AwaitResult(ctx, c, ref, await)
var failed *workflow.RunFailedError
if errors.As(err, &failed) {
	fmt.Fprintf(out, "  a failed child fails the parent: %s\n", failed.Status)
	fmt.Fprintf(out, "  parent error names the child: %v\n", strings.Contains(failed.Err, `await child "resize" #1`))
}
```

To wait for a run from outside a step, from a request handler or a command-line tool, use `workflow.AwaitResult`, which holds the caller instead.

## Try it

```sh
cd examples
go run ./flow-children
```

```text
spawn:
  parent finished, children started: 2
  child of welcome, parent run matches: true
  child of welcome, parent run matches: true
  notifications sent: 2
await children:
  results in job order: ["a@2x.png","b@2x.png","c@2x.png"]
  parent step attempts spent: 1
  a failed child fails the parent: failed
  parent error names the child: true
```

The parent looked at its children several times while they ran, and spent one attempt.

## Pitfalls

- **The parent's driver needs `CapSnooze`.** `AwaitChildren` refuses a parent driver without it rather than spend an attempt on every look, so on Hatchet use `Spawn` and a later step or a [batch](../batches/await.md) instead. When the children run on another backend, set `ChildOptions.Parent`, or the check looks at the children's client.
- **Keep the job list stable across attempts.** Children are memoised by index, so a list built in a different order on a retry pairs stored runs with the wrong jobs.
- **A key you set yourself is used as it is.** Two parents spawning a job with the same `IdempotencyKey` share one child while it is unfinished.
- **An in-process journal does not cross workers.** With `MemoryJournal`, a retry on another worker finds no memoised `RunRef`s, though the derived idempotency key still stops a second child while the first is unfinished; use a shared store such as the Redis driver's ([memoisation](memoisation.md#turn-on-the-journal)).
- **Children are not cancelled with the parent.** Cancel them by their refs, or find them by `LabelParentRun`.

## Related

- [Memoise work inside a step](memoisation.md), [Branching](branching.md)
- [Chains of jobs](chains.md), [Batches](../batches/batches.md), [Wait for a batch in a workflow](../batches/await.md)
- [Idempotency keys](../jobs/idempotency.md), [Manage runs](../jobs/manage-runs.md)
- Example: [examples/flow-children](../../examples/flow-children/)
