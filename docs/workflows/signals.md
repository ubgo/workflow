# Wait for an event

Many processes stop and wait for the outside world: an order waits for its payment, an expense for an approval, a sign-up for an email click. A wait step suspends the run, holding no worker, until your app sends a matching event with `Client.Signal`, or until a deadline passes ([`WaitSpec`](ds:block?id=waitspec-9t4kcwbt)). The event's payload becomes the wait step's output ([`Event`](ds:block?id=event-vwnku48b)), so the next step reads what arrived.

## Before you start

- A driver with [`workflow.CapWaitForEvent`](ds:block?id=capwaitforevent-2km3dkzy). The [memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-caps-yu37ef2j), [Redis](ds:block?id=redis-caps-bfgdst9t) and [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z) drivers have it; whether a wait survives a restart is [`CapDurable`](ds:block?id=capdurable-yhjnwexa) (SQL, Redis and Hatchet yes, memory no).
- Workflows and steps: [steps](steps.md).
- The example is [`examples/signals-and-sleep`](../../examples/signals-and-sleep). It runs on the memory driver with a fake clock. Run it with `go run ./signals-and-sleep` from `examples/`. The transient nudge is in [`examples/flow-cancel`](../../examples/flow-cancel) (`go run ./flow-cancel`).

## Wait for an event

A wait step sets `Step.Wait` to a [`workflow.WaitSpec`](ds:block?id=waitspec-9t4kcwbt) naming the `Event` to wait for. In the order workflow, `await-payment` waits for `payment.succeeded` after `reserve`, and `ship` reads the payment the event carried (through a [sleep](sleep.md), which passes its input on):

<!-- snippet: examples/signals-and-sleep/main.go#order -->
```go
orderFlow := workflow.Workflow{
	Name: "order",
	Steps: []workflow.Step{
		{
			Name: "reserve",
			Run: workflow.Define(func(ctx context.Context, in order) (order, error) {
				return in, nil // reserve stock, pass the order on
			}),
		},
		{
			Name:  "await-payment",
			After: []string{"reserve"},
			Wait: &workflow.WaitSpec{
				Event: "payment.succeeded",
				// Each run waits for the payment of its own order.
				KeyFrom: workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["order"] }),
			},
		},
		{
			Name:  "cool-off",
			After: []string{"await-payment"},
			Sleep: 24 * time.Hour, // holds no worker; passes its input on
		},
		{
			Name:  "ship",
			After: []string{"cool-off"},
			Run: workflow.Define(func(ctx context.Context, p payment) (string, error) {
				lines <- fmt.Sprintf("ship %s (paid %d) at %s", p.Order, p.Paid, clk.Now().Format(layout))
				return "shipped", nil
			}),
		},
	},
}
```

Your app delivers the event with `Client.Signal`, typically from the payment provider's webhook handler. A [`workflow.Event`](ds:block?id=event-vwnku48b) has a `Name`, a `Key` and a `Payload`:

<!-- snippet: examples/signals-and-sleep/main.go#signal -->
```go
err := c.Signal(ctx, workflow.Event{
	Name:    "payment.succeeded",
	Key:     "A-2", // resumes only the run waiting on A-2
	Payload: []byte(`{"Order":"A-2","Paid":40}`),
})
if err != nil {
	return err
}
```

The contract every driver keeps, checked by the shared conformance suite ([the wait cases](ds:block?id=workflowtest-waitcases-qxwsxwkk) and [`RunSignalFlows`](ds:block?id=workflowtest-runsignalflows-x6z59d2p)):

- **An event is kept from the run's enqueue.** A run whose wait step begins after the `Signal` still takes it ([tested](ds:block?id=workflowtest-waitcases-qxwsxwkk)), so a fast webhook that arrives before the run reaches its wait is not lost.
- **An event sent before a run was enqueued never resumes that run** ([same case](ds:block?id=workflowtest-waitcases-qxwsxwkk)).
- **One run never takes the same event twice**, so two wait steps on the same name and key need two events (the memory driver's [`consumeLocked`](ds:block?id=memory-client-consumelocked-c6c5ddtp) marks each taken).
- **One event resumes every run waiting with that name and key**, not only the first ([the broadcast flow](ds:block?id=workflowtest-runsignalflows-x6z59d2p)).
- **A signal nobody waits for is not an error** ([tested](ds:block?id=workflowtest-waitcases-qxwsxwkk)).

## Match the event to the run

Many runs of one workflow wait on the same event name for different things: "payment succeeded" for *this* order. The key says which. A run's wait matches an event with the same name and the same key, and there are two ways to give the key:

- [`WaitSpec.Key`](ds:block?id=waitspec-9t4kcwbt) is a fixed key, the same for every run of the workflow (the approval example waits for `expense.approved` with key `finance`). Empty matches an event with an empty key.
- `WaitSpec.OwnRun` waits on the run's own ID instead, so a sender that knows the run's ref reaches it with `workflow.SignalRun` and no key is invented: [escalation and signal handlers](escalation.md#signal-one-run-by-its-id).
- `WaitSpec.KeyFrom` derives the key per run from the run's job. The driver evaluates it once, when the wait step begins ([memory](ds:block?id=memory-client-startwait-6hz3d5h6)), against the run's original `Payload` and `Labels` (not the step's input). The order workflow takes it from the `order` label:

<!-- snippet: examples/signals-and-sleep/main.go#enqueue-orders -->
```go
refs := map[string]workflow.RunRef{}
for _, id := range []string{"A-1", "A-2"} {
	ref, err := c.Enqueue(ctx, workflow.Job{
		Workflow: "order",
		Payload:  fmt.Appendf(nil, `{"ID":%q,"Total":40}`, id),
		Labels:   map[string]string{"order": id}, // the key the wait step resolves
	})
	if err != nil {
		return err
	}
	refs[id] = ref
}
```

`KeyFrom` is a `workflow.ConcurrencyKey`, the same type concurrency keys use: `workflow.KeyFunc` for a Go function, `workflow.KeyExpr` for an expression in the engine's language. At most one of `Key` and `KeyFrom` is set; `Register` refuses a wait that sets both with an error matching `workflow.ErrInvalid` ([`validateStep`](ds:block?id=validatestep-x6aebswc)).

[`WaitSpec.ResolveKey(job)`](ds:block?id=waitspec-resolvekey-ey3s9dka) returns the key a run waits on: the function's result for a `KeyFunc`, else the fixed `Key`. It is what a driver calls when the wait step begins, and what a test or a webhook handler can call to learn which key to signal. It returns an error matching `workflow.ErrUnsupported` for a `KeyExpr` (the engine's to evaluate) and for an `OwnRun` wait (the key comes from the run's ID). The program asks both of its wait steps:

<!-- snippet: examples/signals-and-sleep/main.go#resolve-key -->
```go
job := workflow.Job{Workflow: "order", Labels: map[string]string{"order": "A-7"}}
perRun, err := byLabel.ResolveKey(job) // KeyFrom: this run's own key
if err != nil {
	return err
}
shared, err := fixed.ResolveKey(job) // Key: the same for every run
if err != nil {
	return err
}
fmt.Printf("  order A-7 waits on %q, every approval on %q\n", perRun, shared)
byExpr := workflow.WaitSpec{Event: "payment.succeeded", KeyFrom: workflow.KeyExpr("input.ID")}
_, err = byExpr.ResolveKey(job)
fmt.Println("  an expression is the engine's to evaluate:", errors.Is(err, workflow.ErrUnsupported))
```

In the example, both orders wait; the event for `A-2` resumes only `A-2`, and `A-1` waits until its own payment arrives.

## Give up after a deadline

A wait with no limit waits forever. Set one of:

- `WaitSpec.Timeout`: how long the wait lasts from when it begins;
- `WaitSpec.Deadline`: the absolute time it ends.

At most one of the two may be set ([`validateStep`](ds:block?id=validatestep-x6aebswc)). A wait that reaches its limit fails the run with an error matching `context.DeadlineExceeded` ([memory](ds:block?id=memory-client-startwait-6hz3d5h6)); waits are not retried, so the run fails for good and `OnFailure` runs:

<!-- snippet: examples/signals-and-sleep/main.go#approval -->
```go
approval := workflow.Workflow{
	Name: "approval",
	Steps: []workflow.Step{{
		Name: "await-approval",
		Wait: &workflow.WaitSpec{
			Event:   "expense.approved",
			Key:     "finance",      // a fixed key: every run waits for the same one
			Timeout: 72 * time.Hour, // three days, then the run fails
		},
	}},
	// OnFailure runs once when a run fails for good, with the input of
	// the step that failed.
	OnFailure: func(ctx context.Context, in []byte) ([]byte, error) {
		lines <- fmt.Sprintf("on failure: escalate %s at %s", in, clk.Now().Format(layout))
		return nil, nil
	},
}
```

The approval is enqueued on Wednesday at 09:00 (the orders above moved the fake clock two days); with no approval after 72 hours, `OnFailure` escalates it on Saturday at 09:00.

## Deliver only to runs waiting now

A signal is kept, so a run that reaches its wait step after it still takes it. For a hint whose late delivery would be wrong (a "refresh now" nudge, a cache invalidation, a progress poke), use [`workflow.TransientSignaller`](ds:block?id=transientsignaller-cww6hkhu): `SignalWaiting(ctx, event)` resumes the runs whose wait step is suspended on the event's name and key at that moment, returns how many it resumed, and keeps the event for no one else. Zero resumed is not an error. The example is [`examples/flow-cancel`](../../examples/flow-cancel): a nudge sent before any run waits resumes nobody, and a run that starts afterwards keeps waiting; the same nudge sent while the run waits resumes it.

<!-- snippet: examples/flow-cancel/main.go#signal-waiting -->
```go
n, err = c.(workflow.TransientSignaller).SignalWaiting(ctx, workflow.Event{Name: "cache.refresh", Key: "catalogue"})
if err != nil {
	return err
}
fmt.Fprintf(w, "  while it waits: resumed %d\n", n)
```

`TransientSignaller` is an optional interface with no capability bit, so reach it with a type assertion (or `workflow.Supports(c, workflow.OpSignalWaiting)` ([`workflow.OpSignalWaiting`](ds:block?id=opsignalwaiting-z676jv7w))). The [memory](ds:block?id=memory-client-signalwaiting-swh99hk3), [SQL](ds:block?id=sql-client-signalwaiting-8ssy9v7j) and [Redis](ds:block?id=redis-client-signalwaiting-c8a88w75) drivers implement it; `workflow.Use` forwards it, and a wrapper whose inner client lacks it returns an `*workflow.UnsupportedError` ([`Forward.SignalWaiting`](ds:block?id=forward-signalwaiting-swx7yvja)). On the SQL driver it resumes runs of the workflows the calling client registered ([SignalWaiting](ds:block?id=sql-client-signalwaiting-8ssy9v7j)), so call it on a worker client. Use it only where losing the event is harmless; anything that must arrive goes through `Signal`.

## Try it

```sh
cd examples
go run ./signals-and-sleep
```

```text
wait and sleep:
  both orders waiting for payment
  paid A-2 at Mon 09:00: A-2 waiting (sleeping), A-1 waiting
  ship A-2 (paid 40) at Tue 09:00
  A-2 succeeded, A-1 still waiting
  ship A-1 (paid 40) at Wed 09:00
give up after a timeout:
  on failure: escalate "expense 77" at Sat 09:00
  run failed, deadline exceeded true
steps that depend on other steps:
  quote/quote attempt 1: price 120, stock 7
step timeout:
  run failed: memory: step "write" failed after 1 attempt(s): workflow: step attempt timed out: memory: step "write" attempt 1 after 30s: context deadline exceeded
refusals:
  memory: workflow: WaitSpec.KeyFrom.Expr unsupported by this driver: the memory driver evaluates only KeyFunc keys; workflow order, step await-payment
  hatchet: workflow: WaitSpec.KeyFrom.Expr unsupported by this driver: workflow order step await-payment: Hatchet has no API that evaluates a CEL expression to a string for the driver; use workflow.KeyFunc
wait keys:
  order A-7 waits on "A-7", every approval on "finance"
  an expression is the engine's to evaluate: true
call a handler directly:
  typed: "hello Ada"
  empty input: "hello "
  bad input is permanent: true
  outside a step: not called from a step
  inside a step: report/send, run run-1, attempt 2
```

The first two blocks are this page; the others belong to [steps](steps.md). The transient nudge is in the second program, whose `deliver only to runs waiting now` lines are:

```sh
go run ./flow-cancel
```

```text
deliver only to runs waiting now:
  before any run waits: resumed 0
  the run started later is still waiting: the first nudge was not kept
  while it waits: resumed 1
  run succeeded
```

While a run waits, `Status` reports `StatusWaiting`.

## Pitfalls

- **The key form depends on the driver.** The memory, SQL and Redis drivers evaluate only `KeyFunc` and refuse a `KeyExpr` at `Start` ([memory](ds:block?id=memory-exprkeys-mrxv22pq), [SQL](ds:block?id=sql-exprkeys-wxvw57q3), [Redis](ds:block?id=redis-exprkeys-g3qq6r7w)); the Hatchet driver refuses a `KeyExpr` too ([`checkSupported`](ds:block?id=hatchet-checksupported-733uwgsp)) (it has no API that evaluates CEL to a string for the driver) and takes `KeyFunc`, which its worker evaluates when the wait step begins. So `KeyFunc` is the portable form: See [Hatchet limitations](../drivers/hatchet.md#limitations).

<!-- snippet: examples/signals-and-sleep/main.go#refusal -->
```go
byExpr := workflow.Workflow{
	Name: "order",
	Steps: []workflow.Step{{
		Name: "await-payment",
		Wait: &workflow.WaitSpec{Event: "payment.succeeded", KeyFrom: workflow.KeyExpr("input.ID")},
	}},
}
m := memory.New()
if err := m.Register(byExpr); err != nil { // accepted here; Start validates the key
	return err
}
fmt.Println("  memory:", m.Start(ctx))
m.Stop()
fmt.Println("  hatchet:", hatchet.New(hatchet.Config{}).Register(byExpr))
```

- **On the memory driver a wait does not survive a restart.** The run, its wait and any kept events are in the process. Use the SQL driver or Hatchet for a wait that may last days; on Hatchet a wait without a `Timeout` or `Deadline` is ended by the engine after `Config.StepTimeout` ([a day by default](ds:block?id=hatchet-defaultsteptimeout-j3gvwcvw)), so give a longer one a `Timeout` or `Deadline`, which [the driver sizes the step's execution timeout to](ds:block?id=hatchet-waittimeout-cwq36qj9) ([Hatchet limitations](../drivers/hatchet.md#limitations)).
- **Kept events are pruned.** Events older than the oldest unfinished run are dropped, since no run can take them ([memory](ds:block?id=memory-client-prunesignalslocked-ndrrs2vd), [SQL](ds:block?id=sql-client-prune-5kgznfp8)); an event is not a durable message queue.
- **A transient nudge sent a moment too early is lost.** [`SignalWaiting`](ds:block?id=transientsignaller-cww6hkhu) sees only runs already suspended on the wait step; a run still in the step before, or just enqueued, never gets it. That is the point of it, so never use it for an event a run must act on.
- **A wait outputs the event payload.** The step after a wait reads the event, not what the step before the wait returned ([memory](ds:block?id=memory-client-startwait-6hz3d5h6)). Carry what you need through the job's payload or labels, or have the event include it.
- **Hatchet has no transient signals and no signal reader.** The Hatchet driver does not implement `TransientSignaller`, so `SignalWaiting` is not offered, and it keeps no record of the events a run took, so `workflow.OnSignals` is refused at `Use` ([no `RunSignalReader`](ds:block?id=signalsplugin-bindclient-g8nk42vn)). A kept `Signal` works. See [Hatchet limitations](../drivers/hatchet.md#limitations).

## Related

- [Sleep inside a workflow](sleep.md), [steps](steps.md)
- [Escalation and signal handlers](escalation.md): signal one run by its ID, a child signalling its parent, handlers between steps
- [Concurrency keys](../jobs/concurrency.md#limit-runs-per-key), the same key type
- [Promises, updates and signal helpers](promises.md): signals that return an answer, signal-with-start
- Examples: [`examples/signals-and-sleep`](../../examples/signals-and-sleep), [`examples/flow-cancel`](../../examples/flow-cancel)
