# Stages, conditions and milestones

Most workflows are a few groups of steps: fetch two feeds in parallel, merge them, publish to two places. Some steps should run only when the data calls for it, some only while the run is in a given state, and some run long enough that they should be warned before their timeout kills them. These helpers build all of that out of ordinary steps and middleware, so they run on every driver.

## Before you start

- `Stages` and `When` need nothing beyond the core package.
- `Milestone` reads [run data](memoisation.md), so the client needs the Journal plugin; it waits by snoozing, which needs `workflow.CapSnooze` (memory, SQL and Redis declare it; Hatchet does not).
- `middleware.SoftTimeout` is step middleware and works on every driver.
- Steps and `After` are explained on [workflows and steps](steps.md); branching between several paths is [branching](branching.md).
- The example is [`examples/flow-stages`](../../examples/flow-stages). Run it with `go run ./flow-stages` from `examples/`.

## Run steps in stages

`workflow.Stages(stages...)` takes a list of stages, each a list of steps. Every step of a stage runs in parallel, and a stage starts once every step of the stage before it has ended: `Stages` adds every step name of the previous stage to each step's `After`, keeping the `After` entries the step already had. Append the result to `Workflow.Steps`.

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

A step of a later stage receives its input the way any step does ([`StepInput`](steps.md)): the output of the single step before it, or a JSON object keyed by step name when the stage before has several steps. `Stages` refuses an empty stage or a step without a name with an error wrapping `workflow.ErrInvalid`; `Register` validates the rest.

## Run a step only on a condition

`workflow.When(cond, handler)` returns a step body that calls `handler` only when `cond` reports true for the step's input. Otherwise it returns the input unchanged, so the steps after it still run and receive what the skipped step received, as data flowing past a node that had nothing to do:

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

`cond` runs on every attempt, so it must decide from the input and the ctx alone, never from the wall clock, or a retry could decide differently. An error from `cond` is the step's error and the retry policy applies. Use `When` to skip one step; use [`Choice`](branching.md) to pick one of several paths; use `middleware.Skip` to drop a step's output instead of passing its input on.

## Enable a step only in a given state

A milestone is a step that may run only while the run is in a given state. `workflow.Milestone(name, want, handler, opts)` reads the run data `name` (written by another step with `workflow.SetRunData`) and calls `handler` only while it equals `want`. Until then the step snoozes for `MilestoneOptions.Poll` (five seconds by default) and looks again, without spending an attempt:

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

With `MilestoneOptions{Skip: true}` the step passes its input on at once instead of waiting, as `When` does. The stored value is decoded into the type of `want` and compared with `reflect.DeepEqual`; a value that does not decode is a permanent error.

## Run tasks one at a time in any order

A set of tasks that must each run once and never two at once, in whatever order (interleaved routing), is `workflow.MapEach` with `workflow.MapConcurrency(1)`: each item runs once, one after the other, and a retry runs only the items that did not succeed (see [memoisation](memoisation.md)). For tasks that are runs of their own, give them one `Concurrency` key with `MaxRuns: 1`.

## Local work inside a step

Engines that dispatch every activity to a worker offer a "local activity": short work run in the orchestrating process with its result recorded. In this library every step body already runs in the worker's process, and `workflow.Memo` records the result of a piece of it in the run's journal, so a retried step skips it. `Memo` is the local activity; see [memoisation](memoisation.md).

## Warn a long step before its timeout

`middleware.SoftTimeout(cfg)` gives each step attempt a soft deadline before its hard one. When it passes, the channel `middleware.SoftDone(ctx)` returns is closed and `cfg.OnSoft`, when set, is told; the attempt itself keeps running. A handler that watches it can save its progress (`Memo`, `SetRunData`) and return before the hard timeout throws its partial work away:

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

`After` is the soft deadline for every step; `For` returns one per step (zero means none for that step). `middleware.SoftExpired(ctx)` reports the same thing for a handler that checks between units of work. Outside `SoftTimeout`, `SoftDone` returns nil, which blocks for ever in a `select`, so a handler can watch it unconditionally. `middleware.Stuck` reports long attempts to an outside callback; `SoftTimeout` tells the attempt itself.

## Try it

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

`merge` started only after both fetches ended and both publishes only after `merge`; the run's result is the object of its two last steps. The short file name skipped `resize` and reached `store` unchanged. `ship` waited, without failing, until `pay` set the state, and the export returned its partial count before the 50 ms timeout.

## Pitfalls

- `When` passes on the step's input, not its parents' outputs one by one: a step with several parents passes on the JSON object keyed by their names, so its children see a different shape than if it had run. Give a conditional step one parent when its output shape matters.
- A `Milestone` waits by snoozing. On a driver without `workflow.CapSnooze` every look spends an attempt, so the step can fail before the state arrives; use `Skip` or a generous retry policy there.
- Drivers time `Step.Timeout` on their own clock, not as a ctx deadline, so `SoftTimeout` cannot read the hard timeout. Set `After` or `For` below the step's `Timeout`, or the soft deadline never comes first.
- `Stages` only sets `After`. A failed step in one stage stops the later stages the way any failed parent does.

## Related

- [Workflows and steps](steps.md): steps, `After` and step input.
- [Branching](branching.md): pick one of several paths with `Choice`.
- [Memoise work inside a step](memoisation.md): `Memo`, `MapEach` and run data, which `Milestone` reads.
- [Trigger rules](trigger-rules.md): steps that run when their parents fail or are skipped.
- [Step middleware](../middleware/step-middleware.md): `SoftTimeout` beside the other step wrappers.
- [Timeouts](../jobs/timeouts.md): the hard `Step.Timeout`.
- Example: [examples/flow-stages](../../examples/flow-stages/)
