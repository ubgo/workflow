# Groups, early ends and guards

Small helpers that keep a growing workflow manageable: group steps under one name, end a run early on purpose, stop a runaway recursion, catch a deploy that changed the steps under running runs, and keep payloads and results typed. None of them changes how a step runs; each guards one way a workflow goes wrong as it grows: a graph too wide to read, work done for nothing, a step that enqueues itself forever, a rename that strands runs already queued, a payload of the wrong shape.

## Before you start

- Every helper here runs on every driver. `EndRun` needs the Journal plugin and the `EndRuns` plugin; `RecursionLimit` and `ShapeGuard` are plugins passed to `workflow.Use`.
- The example is [`examples/flow-patterns`](../../examples/flow-patterns). Run it with `go run ./flow-patterns` from `examples/`.

## Set up the client

<!-- snippet: examples/flow-patterns/main.go#client -->
```go
c, err := workflow.Use(memory.New(),
	workflow.Journal(workflow.NewMemoryJournal()), // Saga, Poll and EndRun keep their state here
	workflow.EndRuns(),         // honours EndRun
	workflow.RecursionLimit(3), // a step's own enqueues nest at most 3 deep
)
if err != nil {
	return err
}
```

## Step groups

<!-- snippet: examples/flow-patterns/main.go#group -->
```go
group, err := workflow.Group("import", nil, []workflow.Step{
	{Name: "fetch", Run: out("rows")},
	{Name: "clean", After: []string{"fetch"}, Run: out("120 clean rows")},
	{Name: "count", After: []string{"fetch"}, Run: out("3 rejected")},
})
if err != nil {
	return workflow.Workflow{}, err
}
report := workflow.Step{Name: "report", After: []string{"import"}, Run: func(ctx context.Context, in []byte) ([]byte, error) {
	return in, nil // {"clean": ..., "count": ...}: the group's last steps, by their names inside it
}}
wf := workflow.Workflow{Name: "import", Steps: append(group, report)}
```

`workflow.Group(name, after, steps)` renames each step `<name>/<step>`, rewires `After` inside the group, makes the steps with no parent inside the group wait on `after`, and adds a join named `name` that waits on the group's last steps and carries their output (one last step: its output; several: an object keyed by their names inside the group). Later steps put the group's name in `After`. A whole sub-workflow with its own run and retries is a [child run](children.md) instead.

## End a run early

<!-- snippet: examples/flow-patterns/main.go#endrun -->
```go
lookup := step("lookup", nil, func(ctx context.Context, in []byte) ([]byte, error) {
	if strings.Contains(string(in), "blocked") {
		if err := workflow.EndRun(ctx); err != nil { // the steps below are skipped
			return nil, err
		}
		return []byte(`"blocked, nothing to do"`), nil
	}
	return in, nil
})
```

`workflow.EndRun(ctx)` ends the run early and successfully: every step that has not started is skipped, steps already running finish. It is a decision, not an abort; `Cancel` is the abort.

## Limit recursion

<!-- snippet: examples/flow-patterns/main.go#recursion -->
```go
_, err := c.Enqueue(ctx, workflow.Job{Workflow: "walk"}) // a child of this run, one level deeper
if errors.Is(err, workflow.ErrDepth) {
	did.add("depth " + depth + " stopped: limit reached")
	return nil, nil
}
```

`workflow.RecursionLimit(max)` writes `LabelDepth` on every job enqueued from inside a step, one deeper than the run whose step enqueued it, and refuses a job past `max` with `ErrDepth` (it matches `ErrInvalid`). A job enqueued outside any step has depth 0.

## Catch changed steps under running runs

`workflow.ShapeGuard(wfs...)` stamps each job with `LabelShape`, the `Shape` of its workflow (a hash of every step's name, kind and `After`, independent of their order in `Steps`), and checks it at every step: a run enqueued under a different set of steps fails with `ErrShapeChanged` (`Permanent`) before its handler runs, instead of resuming against steps it was not written for. The example plays a rolling deploy in which `save` was renamed `store`: a worker on the new build runs v2, while an API server still on the old build enqueues under v1.

<!-- snippet: examples/flow-patterns/main.go#shape -->
```go
worker, err := workflow.Use(backend, workflow.ShapeGuard(v2)) // the new deploy: "save" renamed "store"
if err != nil {
	return err
}
if err := worker.Register(v2); err != nil {
	return err
}
if err := worker.Start(ctx); err != nil {
	return err
}
api, err := workflow.Use(backend, workflow.ShapeGuard(v1)) // an API server still on the old build
if err != nil {
	return err
}
ref, err := api.Enqueue(ctx, workflow.Job{Workflow: "sync"}) // stamped with Shape(v1)
if err != nil {
	return err
}
_, err = workflow.AwaitResult(ctx, worker, ref, await)
msg := fmt.Sprint(err) // ErrShapeChanged, Permanent, before any handler ran
fmt.Println("  run from the old build failed:", msg[max(strings.Index(msg, "was enqueued"), 0):])
```

Runs without the label (enqueued before the guard, or fired by a driver's own scheduler, which runs no enqueue middleware) pass, and so do workflows not passed to `ShapeGuard`. Handler code, retries and timeouts are not part of the shape, so a deploy that only changes code passes. Give a changed definition a new workflow name and drain the old one; the guard tells you when a deploy forgot to.

## Typed from enqueue to result

`workflow.EnqueueOf(ctx, c, name, v, edit)` encodes `v` as the payload (`JobOf`), lets `edit` adjust the job (labels, key, `RunAt`, queue) and enqueues it; `workflow.AwaitResultOf[T](ctx, c, ref, opts)` waits for the run and decodes its result as `T`. With a [`Define`](steps.md#typed-handlers-with-define) handler on the other side, the workflow is typed end to end:

<!-- snippet: examples/flow-patterns/main.go#typed -->
```go
ref, err := workflow.EnqueueOf(ctx, c, "price", quote{SKU: "pen", Qty: 4}, func(j *workflow.Job) {
	j.Labels = map[string]string{"customer": "c-7"} // edit the job before it is sent
})
if err != nil {
	return err
}
p, err := workflow.AwaitResultOf[priced](ctx, c, ref, await)
if err != nil {
	return err
}
fmt.Printf("  %s costs %.2f\n", p.SKU, p.Total)
```

Results need `CapResult`, as `AwaitResult` does. A result that does not decode into `T` is an error naming the run; a run that did not succeed returns `AwaitResult`'s error.

## Try it

```sh
cd examples
go run ./flow-patterns
```

Among its output, these lines are this page's: the group's join carries both of its last steps, a blocked address ends the run early and successfully after `lookup`, the recursion stops at depth 3, the typed run returns a decoded struct, and the run enqueued by the old build is refused with both shapes named.

```text
group:
  {"clean":"120 clean rows","count":"3 rejected"}
end run:
  status succeeded, ran: lookup
recursion:
  depth 0, depth 1, depth 2, depth 3 stopped: limit reached
typed:
  pen costs 10.00
shape guard:
  shape v1 ee4f66355fdd42d5, v2 2572197af55e2410
  run from the old build failed: was enqueued with steps ee4f66355fdd42d5, now 2572197af55e2410 (step "fetch")
```

## Pitfalls

- **A step skipped by `EndRun` gives its children empty input;** they are skipped too, since the run has ended. `EndRun` without the `EndRuns` plugin on the client records the end but skips nothing.
- **`RecursionLimit` counts every enqueue made from inside a step,** follow-up jobs and `Spawn` children included, not only a workflow enqueuing itself. A job past the limit is refused with `ErrDepth`, which matches `ErrInvalid`; handle it in the step as the example does, or the step's attempt fails.
- **`ShapeGuard` must know the definitions on both sides.** Pass every guarded workflow to it on the enqueuing side and the worker side; a workflow the guard does not know is neither stamped nor checked.
- **Group step names change.** Steps inside a group are named `<group>/<step>`, so run history, `ResetRun` and step-level tools use the prefixed names.

## Related

- [Workflows and steps](steps.md), [Child runs](children.md) (a sub-workflow with its own run and retries)
- [Memoise work inside a step](memoisation.md): the journal `EndRun` keeps its state in
- [Typed payloads](../jobs/payloads.md), [Results](../jobs/results.md)
- [Rolling deploys](../operations/deploy.md)
- Example: [examples/flow-patterns](../../examples/flow-patterns/)
