# Trigger rules and exit handlers

A card charge fails because the card was declined. The order should not fail outright: the customer gets a mail asking for another card, the receipt is not sent, and the cart is released whatever happened. By default a step's final failure fails the whole run and every other step is cancelled. [`workflow.WithRules`](ds:block?id=withrules-q3mcee45) changes that for the steps you name: a tolerated step records its failure instead, and the steps after it run by a trigger rule over what their parents did. Everything is ordinary step code, so it runs on every driver.

Reach for it when a failure is an expected outcome with its own path (a decline, a timeout, a missing record), when cleanup must run whatever happened (release a lease, delete a test environment), or when a step only needs some of its parents to succeed. For a person to be reminded while a step runs on, without stopping it, use the [`Boundary`](ds:block?id=boundary-s3ak3b46) timer at the end of this page.

## Before you start

- No capability or plugin is needed for [`WithRules`](ds:block?id=withrules-q3mcee45); it lives in the core package and returns a copy of your workflow with the rules applied. Setup and teardown with run data ([`SetRunData`](ds:block?id=setrundata-2gsvewmu)) needs the [Journal plugin](memoisation.md#turn-on-the-journal).
- Steps and `After` are explained on [workflows and steps](steps.md).
- The examples are [`examples/flow-patterns`](../../examples/flow-patterns) (tolerate, route, clean up) and [`examples/flow-boundary`](../../examples/flow-boundary) (a deadline routed to a fallback, setup and teardown, the `Boundary` timer). Run them with `go run ./flow-patterns` and `go run ./flow-boundary` from `examples/`.

## Tolerate a step, route its failure, always clean up

List the steps that may fail in `Catch` (or `Tolerate`), give the steps after them a rule, and name the cleanup in `Finally`:

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

- [`RulesSpec.Tolerate`](ds:block?id=rulesspec-n7pbvchr) lists Run steps whose final failure is recorded instead of failing the run. A step listed in `Catch` is tolerated too. The failure is final when the step's policy (its `Retry`, else the workflow's) would not retry it, or it is `Permanent`; earlier attempts retry as usual. A cancelled, discarded or snoozed attempt passes through untouched ([`toleratedHandler`](ds:block?id=toleratedhandler-xnm5kays)).
- `RulesSpec.Catch` gives a failure a kind: the first [`CatchRoute`](ds:block?id=catchroute-f563zdkd) whose `Match` returns true. `Match` runs in the failing attempt on the real error, so `errors.Is` and `errors.As` work. Without a match the kind is [`FailureTimeout`](ds:block?id=failuretimeout-tuhban6c) for an error matching `ErrTimeout` and [`FailureError`](ds:block?id=failureerror-uhnjkhy8) otherwise ([`toleratedHandler`](ds:block?id=toleratedhandler-xnm5kays)).
- `RulesSpec.Rules` gives a step a `TriggerRule`. A child of a tolerated or ruled step without an entry gets [`RuleAllSucceeded`](ds:block?id=ruleallsucceeded-rb23vtac) ([`WithRules`](ds:block?id=withrules-q3mcee45)), so a failure or a skip below it propagates as a skip instead of reaching a handler as data. Here `receipt` has no entry, so a decline skips it.
- `RulesSpec.Finally` lists exit handlers. A Finally step with no `After` is placed after every leaf ([`leavesExcept`](ds:block?id=leavesexcept-2vzduexk)), every other Run step is tolerated so nothing can stop it, and it runs once all its parents are done. After its handler returns, the run fails with [`ErrStepsFailed`](ds:block?id=errstepsfailed-q4yb27zg) when any step before it failed ([`ruledHandler`](ds:block?id=ruledhandler-sxsw3vmv)), so the run's status stays honest.

[`WithRules`](ds:block?id=withrules-q3mcee45) returns an error wrapping `ErrInvalid` when a name is not a step of the workflow, a listed step is not a Run step, a `CatchRoute` has no `Kind` or `Match`, or a rule is unknown.

## Send a missed deadline to a fallback

A tolerated step that gives up at a deadline records the kind `FailureTimeout`, and a step with [`RuleOneFailedOf(workflow.FailureTimeout)`](ds:block?id=ruleonefailedof-fsnzz3ss) takes over. The deadline can be the step's own `Step.Timeout` (on a driver with [`CapRunTimeout`](ds:block?id=capruntimeout-nth9c52n): [memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-p5caps-wggh6esn), [Redis](ds:block?id=redis-caps-bfgdst9t)), or one the step enforces itself by returning an error built with [`workflow.TimeoutError`](ds:block?id=timeouterror-nqztem89), as here:

<!-- snippet: examples/flow-boundary/main.go#interrupt -->
```go
wf := workflow.Workflow{Name: "quote", Steps: []workflow.Step{
	{Name: "price", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		ctx, cancel := context.WithTimeout(ctx, supplierDeadline) // the deadline lives in the step
		defer cancel()
		price, err := askSupplier(ctx)
		if errors.Is(err, context.DeadlineExceeded) {
			// An error matching ErrTimeout gets the kind FailureTimeout.
			return nil, workflow.TimeoutError(workflow.ErrStepTimeout, "supplier did not answer")
		}
		return price, err
	}},
	{Name: "fallback", After: []string{"price"}, Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		f := workflow.ParentFailures(ctx)[0]
		return json.Marshal(fmt.Sprintf("cached price 9.90 (%s failed: %s)", f.Step, f.Kind))
	}},
}}
return workflow.WithRules(wf, workflow.RulesSpec{
	Tolerate: []string{"price"},
	Rules:    map[string]workflow.TriggerRule{"fallback": workflow.RuleOneFailedOf(workflow.FailureTimeout)},
})
```

[`workflow.ParentFailures(ctx)`](ds:block?id=parentfailures-8u89n2hu) lists what the parents of a ruled step recorded, each a [`StepFailure`](ds:block?id=stepfailure-xq2xnjvt) with the step, its kind and its error message. With `Step{Name: "price", Timeout: supplierDeadline, ...}` instead of the deadline inside the step, the route is the same: the attempt's `ctx` is cancelled with a cause matching [`workflow.ErrStepTimeout`](ds:block?id=errsteptimeout-6b727264), and the tolerated step records the timeout as `FailureTimeout`, whatever the handler returns after it ([`toleratedHandler`](ds:block?id=toleratedhandler-xnm5kays)).

## Setup and teardown

A step that acquires something (a test environment, a lease, a temporary bucket) and a Finally step that releases it make a setup and teardown pair: the teardown runs after the work whether it succeeded or failed, and the run still ends failed when the work failed. Pass the resource from setup to teardown as run data, since the teardown's input is its parents' outputs, not the setup's:

<!-- snippet: examples/flow-boundary/main.go#teardown -->
```go
return workflow.WithRules(workflow.Workflow{Name: "e2e", Steps: []workflow.Step{
	{Name: "setup", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		env, err := createEnv(ctx)
		if err != nil {
			return nil, err
		}
		if err := workflow.SetRunData(ctx, "env", env); err != nil {
			return nil, err
		}
		return in, nil // the suite to run
	}},
	{Name: "test", After: []string{"setup"}, Run: runSuite},
	{Name: "teardown", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		env, ok, err := workflow.RunData[string](ctx, "env")
		if err != nil || !ok {
			return nil, err // setup failed before it made one: nothing to release
		}
		return nil, deleteEnv(ctx, env)
	}},
}}, workflow.RulesSpec{Finally: []string{"teardown"}}) // placed after every leaf
```

[`workflow.SetRunData(ctx, name, v)`](ds:block?id=setrundata-2gsvewmu) stores `v` as JSON for the rest of the run and [`workflow.RunData[T](ctx, name)`](ds:block?id=rundata-43fsr5xk) reads it back, with `false` when nothing was stored; both need the Journal plugin on the client. When setup itself fails, the teardown still runs and finds no value. A teardown that must not run twice for one resource can memoise its call with [`workflow.Memo`](ds:block?id=memo-wg47wmpf).

## The rules

| Rule | The step runs when |
|---|---|
| [`RuleAllSucceeded`](ds:block?id=ruleallsucceeded-rb23vtac) | every parent succeeded (the default below a tolerated or ruled step) |
| [`RuleNoneFailed`](ds:block?id=rulenonefailed-sw2t9ffg) | no parent failed, directly or through a parent skipped because of a failure; a parent skipped by a rule that simply did not hold is fine |
| [`RuleAllDone`](ds:block?id=rulealldone-apc5tzgu) | always, once every parent is done |
| [`RuleOneFailed`](ds:block?id=ruleonefailed-epgevkfz) | at least one parent failed |
| [`RuleAllFailed`](ds:block?id=ruleallfailed-rq28tyzz) | every parent failed |
| [`RuleOneSucceeded`](ds:block?id=ruleonesucceeded-7hffk2d3) | at least one parent succeeded |
| [`RuleOneFailedOf(kind)`](ds:block?id=ruleonefailedof-fsnzz3ss) | at least one parent failed with this kind |

A step whose rule does not hold does not call its handler; it ends with a skip record that its own children see ([`ruledHandler`](ds:block?id=ruledhandler-sxsw3vmv)). A ruled step receives the usual input (one parent's output, or an object keyed by parent), with a failed or skipped parent's output as `null`; `workflow.ParentFailures(ctx)` lists the failures.

## A timer that does not stop the step

[`workflow.Boundary(d, onTimer, steps...)`](ds:block?id=boundary-s3ak3b46) is step middleware: when a step is still running after `d`, `onTimer` is called once, in its own goroutine, with the step's context and `StepInfo`, while the step carries on (escalate, page someone, send a reminder). `steps` limits it to those step names; none means every step. Give it to the driver like any step middleware:

<!-- snippet: examples/flow-boundary/main.go#boundary -->
```go
// remind is the boundary timer's callback for a step still running.
remind := func(ctx context.Context, info workflow.StepInfo) {
	// Runs once, in its own goroutine, while the step carries on.
	did.add(fmt.Sprintf("reminder: %s still in %q after %s", info.Labels["claim"], info.Step, remindAfter))
}
c, err := workflow.Use(
	memory.New(memory.WithMiddleware(workflow.Boundary(remindAfter, remind, "review"))),
	workflow.Journal(workflow.NewMemoryJournal()), // SetRunData and RunData keep run data here
)
if err != nil {
	return err
}
```

The timer belongs to one attempt: it starts when the attempt starts and stops when the attempt returns, so a review that ends in time sends nothing. The other drivers take the same middleware ([`sqlstore.WithMiddleware`](ds:block?id=sql-withmiddleware-hkqu69ab), [`redis.WithMiddleware`](ds:block?id=redis-withmiddleware-28r4tn9t), `hatchet.Config.Middleware`). The interrupting form, which stops the step and takes another path, is the [deadline routed to a fallback](#send-a-missed-deadline-to-a-fallback) above.

## Try it

```sh
cd examples
go run ./flow-patterns
go run ./flow-boundary
```

The first lines of `flow-patterns` are this page's: a good card runs the receipt and the cleanup; a declined card skips the receipt, runs the decline branch and the cleanup, and the run ends failed with the charge's error.

```text
rules:
  card "ok": "cart released"
    ran: charge, receipt, cleanup
  card "declined": failed: workflow: steps failed before the exit handler: step "charge": card declined
    ran: charge, tell-customer, cleanup
```

`flow-boundary` prints:

```text
boundary timer:
  quick review: "approved"
    ran: file, review
  slow review: "approved"
    ran: file, reminder: claim-slow still in "review" after 20ms, review
interrupting deadline:
  "cached price 9.90 (price failed: timeout)"
setup and teardown:
  green suite: run succeeded
    ran: create env-42, test, delete env-42
  broken suite: run failed
    ran: create env-42, test, delete env-42
```

The reminder lands while the slow review is still running, and the review still returns its result. The broken suite's environment is deleted too, and its run still ends failed.

## Pitfalls

- **A handled failure ends the run succeeded.** A run whose tolerated steps failed and that has no Finally step ends succeeded (a tolerated failure is a record, not an error: [`toleratedHandler`](ds:block?id=toleratedhandler-xnm5kays)). Add a Finally step to keep it failed, or have the handling step return a `Permanent` error.
- **On Hatchet, `Step.Timeout` is enforced by the worker.** Hatchet has no run deadline (no `CapRunTimeout`), but its worker cancels an attempt at `Step.Timeout` with `workflow.ErrStepTimeout` ([`callTimed`](ds:block?id=hatchet-client-calltimed-xjgb6ju6)), keeps the handshake, and gives the engine's own task timeout a [5-second margin](ds:block?id=hatchet-enginetimeoutmargin-nwx47exf), so a tolerated step records its timeout as `FailureTimeout` there too. Only a handler that ignores its context past that margin is ended by the engine, with the engine's error. See [Hatchet limitations](../drivers/hatchet.md#limitations).
- **The last attempt is judged from the step's or the workflow's policy** ([`WithRules`](ds:block?id=withrules-q3mcee45)). A per-dispatch `Job.Retry` is not visible to the step, so a job that overrides the policy should not rely on tolerated steps.
- **Only Run steps can be tolerated or ruled.** A Wait or Sleep step below a tolerated step is refused (`ErrInvalid`, [`WithRules`](ds:block?id=withrules-q3mcee45)), since it could not skip.
- **The error text crosses to the ruled step, not the error value.** Route on kinds, decided in the failing attempt by `Catch`; [`StepFailure.Error`](ds:block?id=stepfailure-xq2xnjvt) is only the message.
- **`Boundary` is not durable.** Its timer is an in-process `time.AfterFunc` on the worker running the attempt ([`Boundary`](ds:block?id=boundary-s3ak3b46)): a worker that dies takes the timer with it, and a retried step starts a new timer, so a step that fails and retries can remind once per attempt. `onTimer` gets the step's context, which is cancelled once the attempt ends, so hand anything slow to a job of its own.
- **Parallel steps setting one run data name overwrite each other.** Give each its own name; the last write wins ([`SetRunData`](ds:block?id=setrundata-2gsvewmu)).

## Related

- [Workflows and steps](steps.md), [Branching](branching.md), [Sagas](sagas.md) (compensation inside a step or across steps)
- [Memoise work inside a step](memoisation.md#share-a-value-across-steps): run data and the Journal plugin
- [Timeouts](../jobs/timeouts.md), [Retries](../jobs/retries.md), [Step middleware](../middleware/step-middleware.md)
- [Polling and approvals](polling-and-approval.md): an approval's timeout tolerated for an escalation branch
- Examples: [examples/flow-patterns](../../examples/flow-patterns/), [examples/flow-boundary](../../examples/flow-boundary/)
