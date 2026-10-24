# Branching

A refund under 100 is approved automatically; a bigger one goes to review. A run's graph of steps is fixed when it starts, so a branch picked at run time cannot add or remove steps. `workflow.Choice` builds an exclusive choice out of ordinary steps instead: a decide step picks one branch, every branch step starts, the branches not picked return at once without doing their work, and a join step carries the chosen branch's output on. Because the result is plain steps, it runs on every driver with no capability beyond what the steps themselves need.

## Before you start

- No capability or plugin is needed; `Choice` lives in the core package.
- Steps and `After` are explained on [workflows and steps](steps.md).
- The example is [`examples/flow-memo`](../../examples/flow-memo). Run it with `go run ./flow-memo` from `examples/`.

## Build a choice

`workflow.Choice(spec)` returns the steps of a `workflow.ChoiceSpec`; append them to `Workflow.Steps`. `Decide` returns the name of the branch to run, and each `workflow.Branch` is an ordinary step body with its own `Retry` and `Timeout`:

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

The spec expands into these steps, which is what a run listing shows:

| Step | After | What it does |
|---|---|---|
| `<name>.decide` | the spec's `After` | Calls `Decide` with the input `After` gives it, and records the branch and that input |
| `<name>.<branch>`, one per branch | `<name>.decide` | Runs the branch's `Run` with the decide step's input when it is the branch picked; otherwise returns at once without calling `Run` |
| `<name>` | every branch step | Returns the chosen branch's output unchanged |

Later steps list the choice's own name in `After`, as they would any step, and receive the chosen branch's output. The decide suffix is the constant `workflow.ChoiceDecideSuffix`.

## Errors

`Choice` returns an error wrapping `workflow.ErrInvalid` when the spec has no `Name`, no `Decide` or no branches, when a branch has no `Name` or `Run`, or when a branch name repeats. At run time, a `Decide` that returns a name that is not a branch fails the run (permanent, `ErrInvalid`); any other error from `Decide` retries the decide step under the workflow's policy.

## Try it

```sh
cd examples
go run ./flow-memo
```

The choice's part, the expanded steps and one run on each branch:

```text
choice:
  steps: load, route.decide, route.auto, route.review, route, notify
  40: "refunded 40 automatically"
  250: "refund of 250 sent to review"
```

## Pitfalls

- **Every branch step shows as succeeded.** The branches not picked succeed without doing anything; the one that did the work is the one whose `Run` was called. Read the decide step's output, or log in the branch, to see which ran.
- **Only one branch runs.** For several paths at once, give steps the same `After` and join them; for one call per item of a list, use [MapEach](memoisation.md#run-a-call-per-item-with-mapeach) or [child runs](children.md).
- **The branch gets the decide step's input, not its output.** Put what the branch needs in the input of the choice, through the steps in its `After`.
- **Branch names are part of step names.** Renaming a branch renames its step, which run listings and step-level retries see.

## Related

- [Workflows and steps](steps.md), [Memoise work inside a step](memoisation.md), [Child runs](children.md)
- Example: [examples/flow-memo](../../examples/flow-memo/)
