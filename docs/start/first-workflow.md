# Your first workflow

A workflow of four steps on the in-memory driver: a payment step that fails once and is retried, two steps that run in parallel once it succeeds, and a last step that joins their outputs. A second order fails for good, is not retried, and runs the workflow's failure handler. Along the way you meet typed handlers, step inputs, retry policies, `StepInfo`, run status and the run-finished hook.

## Before you start

- The [Quickstart](quickstart.md) runs; this page builds on it.
- Every step's input and output are bytes, JSON by convention. `workflow.Define` does the encoding for you.

## Step 1 — Declare the payload types

The job's payload is the input of the first steps. A step with several parents receives one JSON object keyed by parent name, so its input type has one field per parent.

<!-- snippet: examples/first-workflow/main.go#types -->
```go
// order is the job's payload. Each step's input and output are JSON.
type order struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
}

// joined is the input of a step with several parents: one field per
// parent, named after the parent step.
type joined struct {
	Receipt string `json:"receipt"`
	Ship    string `json:"ship"`
}
```

## Step 2 — Write the workflow

Steps form a graph through `After`. A step with no `After` starts with the run; a list of steps without `After` runs them all in parallel, so you chain them for sequence. What each step receives follows one rule on every driver (`workflow.StepInput`): the payload for a first step, its parent's output unchanged for one parent, and an object keyed by parent name for several.

`Workflow.Retry` is the default retry policy of every step (a step's own `Retry` overrides it, and `workflow.RetriesNone()` turns retries off for one step). Its zero value means no retries. Returning `workflow.Permanent(err)` fails the step at once and skips the remaining attempts. `workflow.StepInfoFrom(ctx)` tells a handler which run, step and attempt it is.

<!-- snippet: examples/first-workflow/main.go#workflow -->
```go
wf := workflow.Workflow{
	Name: "order",
	// Every step may be retried twice, 10ms apart.
	Retry: workflow.RetryPolicy{Max: 2, Backoff: workflow.Backoff{Initial: 10 * time.Millisecond}},
	Steps: []workflow.Step{
		{Name: "charge", Run: workflow.Define(func(ctx context.Context, o order) (order, error) {
			info, _ := workflow.StepInfoFrom(ctx)
			if o.Amount <= 0 {
				// Permanent: retrying the same input can never succeed.
				return o, workflow.Permanent(fmt.Errorf("order %s: nothing to charge", o.ID))
			}
			charges++
			if charges == 1 {
				say("charge %s: attempt %d failed", o.ID, info.Attempt)
				return o, errors.New("card network timeout")
			}
			say("charge %s: attempt %d charged %d", o.ID, info.Attempt, o.Amount)
			return o, nil
		})},
		// receipt and ship both start once charge succeeded, in
		// parallel, each with charge's output as input.
		{Name: "receipt", After: []string{"charge"}, Run: workflow.Define(func(ctx context.Context, o order) (string, error) {
			return "receipt for " + o.ID, nil
		})},
		{Name: "ship", After: []string{"charge"}, Run: workflow.Define(func(ctx context.Context, o order) (string, error) {
			return "parcel for " + o.ID, nil
		})},
		// notify has two parents: its input is a JSON object keyed by
		// their names.
		{Name: "notify", After: []string{"receipt", "ship"}, Run: workflow.Define(func(ctx context.Context, in joined) (struct{}, error) {
			say("notify: %s, %s", in.Receipt, in.Ship)
			return struct{}{}, nil
		})},
	},
	// OnFailure runs once when a run fails for good, with the failing
	// step's input.
	OnFailure: func(ctx context.Context, in []byte) ([]byte, error) {
		say("on failure: %s", in)
		return nil, nil
	},
}
```

`say` writes a line to the program's output; `charges` counts calls to `charge`.

## Step 3 — Register, observe run ends, start

The memory driver implements `workflow.RunNotifier`: an observer registered with `OnRunFinished` before `Start` sees every run that reaches a terminal status (succeeded, failed or cancelled). It is how app code reacts to a run's end without polling. Registering after `Start` returns `workflow.ErrAlreadyStarted`.

<!-- snippet: examples/first-workflow/main.go#start -->
```go
c := memory.New()
ended := make(chan workflow.RunEvent, 2)
// Observers are registered before Start; the memory driver reports
// every run that reaches a terminal status.
if err := c.OnRunFinished(func(ctx context.Context, ev workflow.RunEvent) { ended <- ev }); err != nil {
	return err
}
if err := c.Register(wf); err != nil {
	return err
}
ctx, cancel := context.WithCancel(ctx)
defer cancel() // stops every worker when run returns
if err := c.Start(ctx); err != nil {
	return err
}
```

## Step 4 — Enqueue and read the status

`Job.Labels` carry app metadata (a tenant, a source) to every step through `StepInfo.Labels` and to observers through `RunEvent.Labels`. `Status` reads where a run is; once the observer has seen the end, `Status` reports the same terminal status.

<!-- snippet: examples/first-workflow/main.go#enqueue -->
```go
ref, err := c.Enqueue(ctx, workflow.Job{
	Workflow: "order",
	Payload:  []byte(o),
	Labels:   map[string]string{"tenant": "acme"},
})
if err != nil {
	return err
}
ev := <-ended
st, err := c.Status(ctx, ref)
if err != nil {
	return err
}
say("run %s: %s (status %s, err %v)", ev.Workflow, ev.Status, st, ev.Err)
```

The program enqueues two orders, `A-1` for 30 and `A-2` for 0, one after the other.

## Try it

```sh
cd examples
go run ./first-workflow
```

```text
charge A-1: attempt 1 failed
charge A-1: attempt 2 charged 30
notify: receipt for A-1, parcel for A-1
run order: succeeded (status succeeded, err <nil>)
on failure: {"id":"A-2","amount":0}
run order: failed (status failed, err memory: step "charge" failed after 1 attempt(s): order A-2: nothing to charge)
```

The first charge failed and the retry policy ran it again 10ms later; `notify` received both parents' outputs. The second order's `Permanent` error stopped after one attempt although two retries were allowed, and `OnFailure` ran once with the failing step's input.

## Pitfalls

- Steps without `After` run in parallel, not in list order. Give every step after the first its parents.
- A step's input with several parents is an object keyed by parent name; a parent whose output is empty or missing appears as `null`. Name the input struct's fields after the parents.
- Retries are off by default. A workflow without `Retry` fails at the first error.
- Mark errors that retrying cannot fix with `workflow.Permanent`, or list them in `RetryPolicy.NoRetryOn`; `Define` already marks undecodable input permanent.
- A failed run's `RunEvent.Err` keeps the original error on in-process drivers; an engine driver such as Hatchet rebuilds it from the stored message, so `errors.Is` against your own sentinel is reliable only on in-process drivers.
- `OnRunFinished` is an optional interface: the memory, SQL and Redis drivers declare `CapRunFinished` (every end delivered at least once); the Hatchet driver delivers best-effort only. Observers must tolerate seeing the same run twice.

## Related

- [Steps](../workflows/steps.md#define-a-workflow), [typed handlers](../workflows/steps.md#typed-handlers-with-define), [steps that depend on other steps](../workflows/steps.md#steps-that-depend-on-other-steps), [what a step knows about its run](../workflows/steps.md#what-a-step-knows-about-its-run) and [failure handlers](../workflows/steps.md#run-a-handler-when-a-run-fails).
- [Retry policy](../jobs/retries.md#retry-policy) and [errors that must not retry](../jobs/retries.md#errors-that-must-not-retry).
- [React when a run finishes](../observability/run-finished.md#react-when-a-run-finishes) and [read a run's status](../jobs/cancel-and-status.md#read-a-runs-status).
- Example: [first-workflow](../../examples/first-workflow/).
