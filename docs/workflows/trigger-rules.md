# Trigger rules and exit handlers

A card charge fails because the card was declined. The order should not fail outright: the customer gets a mail asking for another card, the receipt is not sent, and the cart is released whatever happened. By default a step's final failure fails the whole run and every other step is cancelled. `workflow.WithRules` changes that for the steps you name: a tolerated step records its failure instead, and the steps after it run by a trigger rule over what their parents did. Everything is ordinary step code, so it runs on every driver.

## Before you start

- No capability or plugin is needed; `WithRules` lives in the core package and returns a copy of your workflow.
- Steps and `After` are explained on [workflows and steps](steps.md).
- The example is [`examples/flow-patterns`](../../examples/flow-patterns). Run it with `go run ./flow-patterns` from `examples/`.

## Tolerate a step, route its failure, always clean up

<!-- snippet: examples/flow-patterns/main.go#rules -->
```go
wf := workflow.Workflow{Name: "order", Steps: []workflow.Step{
	step("charge", nil, func(in []byte) ([]byte, error) {
		if string(in) == `"declined"` {
			return nil, workflow.Permanent(errDeclined)
		}
		return []byte(`"ch_1"`), nil
	}),
	step("receipt", []string{"charge"}, func(in []byte) ([]byte, error) { return []byte(`"mailed"`), nil }),
	step("tell-customer", []string{"charge"}, func(in []byte) ([]byte, error) { return []byte(`"asked for another card"`), nil }),
	step("cleanup", nil, func(in []byte) ([]byte, error) { return []byte(`"cart released"`), nil }),
}}
return workflow.WithRules(wf, workflow.RulesSpec{
	Catch: map[string][]workflow.CatchRoute{"charge": {{Kind: "declined", Match: func(err error) bool {
		return errors.Is(err, errDeclined) // errors.Is works: it runs in the failing attempt
	}}}},
	Rules: map[string]workflow.TriggerRule{
		"tell-customer": workflow.RuleOneFailedOf("declined"), // runs only for a decline
	},
	Finally: []string{"cleanup"}, // after every leaf, whatever happened; keeps the run failed on a failure
})
```

- `RulesSpec.Tolerate` lists Run steps whose final failure is recorded instead of failing the run. A step listed in `Catch` is tolerated too. The failure is final when the step's policy (its `Retry`, else the workflow's) would not retry it, or it is `Permanent`; earlier attempts retry as usual. A cancelled, discarded or snoozed attempt passes through untouched.
- `RulesSpec.Catch` gives a failure a kind: the first `CatchRoute` whose `Match` returns true. `Match` runs in the failing attempt on the real error, so `errors.Is` and `errors.As` work. Without a match the kind is `FailureTimeout` for an error matching `ErrTimeout` and `FailureError` otherwise.
- `RulesSpec.Rules` gives a step a `TriggerRule`. A child of a tolerated or ruled step without an entry gets `RuleAllSucceeded`, so a failure or a skip below it propagates as a skip instead of reaching a handler as data.
- `RulesSpec.Finally` lists exit handlers. A Finally step with no `After` is placed after every leaf, every other Run step is tolerated so nothing can stop it, and it runs once all its parents are done. After its handler returns, the run fails with `ErrStepsFailed` when any step before it failed, so the run's status stays honest.

## The rules

| Rule | The step runs when |
|---|---|
| `RuleAllSucceeded` | every parent succeeded (the default below a tolerated or ruled step) |
| `RuleNoneFailed` | no parent failed, directly or through a parent skipped because of a failure; a parent skipped by a rule that simply did not hold is fine |
| `RuleAllDone` | always, once every parent is done |
| `RuleOneFailed` | at least one parent failed |
| `RuleAllFailed` | every parent failed |
| `RuleOneSucceeded` | at least one parent succeeded |
| `RuleOneFailedOf(kind)` | at least one parent failed with this kind |

A step whose rule does not hold does not call its handler; it ends with a skip record that its own children see. A ruled step receives the usual input (one parent's output, or an object keyed by parent), with a failed or skipped parent's output as `null`; `workflow.ParentFailures(ctx)` lists the failures, each a `StepFailure` with the step, its kind and its error message.

## A timer that does not stop the step

`workflow.Boundary(d, onTimer, steps...)` is step middleware: when a step is still running after `d`, `onTimer` is called once in its own goroutine while the step carries on (escalate, page someone, send a reminder). The interrupting form is `Step.Timeout` on a tolerated step with a `RuleOneFailedOf(workflow.FailureTimeout)` branch.

## Pitfalls

- A run whose tolerated steps failed and that has no Finally step ends succeeded: the failure was handled. Add a Finally step to keep it failed.
- The last attempt is judged from the step's or the workflow's policy. A per-dispatch `Job.Retry` is not visible to the step, so a job that overrides the policy should not rely on tolerated steps.
- Only Run steps can be tolerated or ruled. A Wait or Sleep step below a tolerated step is refused (`ErrInvalid`), since it could not skip.
- The error text crosses to the ruled step, not the error value: route on kinds, decided in the failing attempt by `Catch`.
