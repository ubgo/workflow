# flow-cancel

Withdrawing work that is no longer wanted, on the in-memory driver. An album spawns three renders and moves on; `workflow.CancelChildren` cancels every child of its `render-all` step in one call, and `workflow.ChildLabels` lists them by their parent labels. A quote asks two price feeds that hang; `workflow.CancelSteps` withdraws both, each falls back to a cached price, and the run goes on to combine them. A `workflow.TransientSignaller` nudge reaches nobody before a run waits and is not kept, then resumes the run once it waits. Last, an order waits for "paid and packed, or cancelled": a child run holds the two waits and answers the order's race with `ship`, and when the customer cancels first, the `cancel` branch withdraws the half-met child with `CancelChildren`.

```sh
cd examples
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
wait for all of, or any of:
  A-1 (order.paid + order.packed): shipped; child succeeded
  A-2 (order.paid): cancelled, 1 child withdrawn; child cancelled
```

## The code

Cancel every unfinished child a step spawned, from inside the run or from an operator's tool:

<!-- snippet: examples/flow-cancel/main.go#cancel-children -->
```go
n, err := workflow.CancelChildren(ctx, c, ref.RunID, "render-all")
if err != nil {
	return err
}
fmt.Fprintf(w, "  cancelled %d children\n", n)
```

List them by the labels `Spawn` wrote:

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

Withdraw several steps of one run; a handler that sees `workflow.ErrStepCancelled` returns a fallback and the run continues:

<!-- snippet: examples/flow-cancel/main.go#cancel-steps -->
```go
if err := workflow.CancelSteps(ctx, c, ref, "feed-eu", "feed-us"); err != nil {
	return err
}
out, err := workflow.AwaitResult(ctx, c, ref, await)
if err != nil {
	return err
}
fmt.Fprintf(w, "  quote succeeded with %s\n", out)
```

Nudge only the runs waiting now:

<!-- snippet: examples/flow-cancel/main.go#signal-waiting -->
```go
n, err = c.(workflow.TransientSignaller).SignalWaiting(ctx, workflow.Event{Name: "cache.refresh", Key: "catalogue"})
if err != nil {
	return err
}
fmt.Fprintf(w, "  while it waits: resumed %d\n", n)
```

Wait for "all of" in a child run and "any of" in the parent's race; the cancel branch withdraws the child:

<!-- snippet: examples/flow-cancel/main.go#mix-race -->
```go
watch := workflow.Step{Name: watchStep, Run: func(ctx context.Context, _ []byte) ([]byte, error) {
	info, _ := workflow.StepInfoFrom(ctx)
	_, err := workflow.Spawn(ctx, c, "ready", []workflow.Job{{
		Workflow: "order.ready",
		Labels:   map[string]string{orderLabel: info.Labels[orderLabel]},
	}})
	return nil, err
}}
race, err := workflow.Race(workflow.RaceSpec{
	Name: "decided", After: []string{watchStep}, Event: decidedEvent, KeyFrom: byOrder,
	Branches: []workflow.Branch{
		{Name: "ship", Run: func(context.Context, []byte) ([]byte, error) {
			return json.Marshal("shipped")
		}},
		{Name: "cancel", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			info, _ := workflow.StepInfoFrom(ctx)
			n, err := workflow.CancelChildren(ctx, c, info.RunID, watchStep) // the half-met "all of"
			if err != nil {
				return nil, err
			}
			return json.Marshal(fmt.Sprintf("cancelled, %d child withdrawn", n))
		}},
	},
})
if err != nil {
	return workflow.Workflow{}, err
}
```

The guides are [joins](../../docs/workflows/joins.md#cancel-every-child-of-a-step), [promises](../../docs/workflows/promises.md#withdraw-several-steps-of-a-run) [signals](../../docs/workflows/signals.md#deliver-only-to-runs-waiting-now) and [multi-choice and races](../../docs/workflows/multi-choice-and-race.md#wait-for-all-of-any-of-or-a-mix).
