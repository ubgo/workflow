# Promises, updates and signal helpers

[Signals](signals.md) resume a run that waits for an event. Some flows need more: a run that waits for a decision an API call makes later (an approval, a manual review), a caller that sends a run a request and needs its answer back, a signal to a run that may not exist yet, a run that loops for months and must not grow without bound. These helpers build each of them from signals, snoozes and a small store, and run on every driver with the capabilities they name.

## Before you start

- Promises and updates keep their values in a [`workflow.PromiseStore`](ds:block?id=promisestore-fpf8x3m4). [`workflow.NewMemoryPromises()`](ds:block?id=newmemorypromises-capatjkk) is the in-process store; [`sqlstore.Client.PromiseStore()`](ds:block?id=sql-client-promisestore-fw6wxfdm) (or [`sqlstore.NewPromiseStore`](ds:block?id=sql-newpromisestore-cpmkmayx), rows in the journal table) and [`redis.Client.PromiseStore()`](ds:block?id=redis-client-promisestore-r626cn6b) (or [`redis.NewPromiseStore`](ds:block?id=redis-newpromisestore-v66xmp89), one `SET NX` key per promise, with an optional TTL) are durable and shared by every process on that database. The caller and the run must use the same store.
- [`AwaitPromise`](ds:block?id=awaitpromise-338jz9js) waits by snoozing, so it needs [`workflow.CapSnooze`](ds:block?id=capsnooze-pffu58xc) ([memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-caps-yu37ef2j) and [Redis](ds:block?id=redis-caps-bfgdst9t)). [`Update`](ds:block?id=update-khsw5ev5) and [`SignalWithStart`](ds:block?id=signalwithstart-hk5sr9sg) need [`workflow.CapWaitForEvent`](ds:block?id=capwaitforevent-2km3dkzy); `SignalWithStart` and [`ContinueAsNew`](ds:block?id=continueasnew-9dkhw3cb) need [`workflow.CapIdempotencyKey`](ds:block?id=capidempotencykey-5wcqqwy8).
- [`CancelSteps`](ds:block?id=cancelsteps-9p2f2na7) needs a client that implements [`workflow.StepCanceller`](ds:block?id=stepcanceller-gp5urxfn) ([memory](ds:block?id=memory-client-cancelstep-cn7kxx27), [SQL](ds:block?id=sql-client-cancelstep-fnqpwd5z) and [Redis](ds:block?id=redis-client-cancelstep-dzgd8reg); no capability bit, so check with a type assertion or [`workflow.Supports`](ds:block?id=supports-4xyjjwvz) with [`workflow.OpCancelStep`](ds:block?id=opcancelstep-x8n8q838)).
- The examples are [`examples/flow-promises`](../../examples/flow-promises) and, for withdrawing steps, [`examples/flow-cancel`](../../examples/flow-cancel). Run them with `go run ./flow-promises` and `go run ./flow-cancel` from `examples/`.

## Wait for a promise

A promise is a value stored once under an id and read by anyone who knows the id. A step waits for it with [`workflow.AwaitPromise(ctx, store, id, poll)`](ds:block?id=awaitpromise-338jz9js), which returns the value once the promise is settled and `workflow.Snooze(poll)` until then ([`DefaultPromisePoll`](ds:block?id=defaultpromisepoll-2vu4hbw2) when `poll` is zero), so the step holds no worker while it waits:

<!-- snippet: examples/flow-promises/main.go#await-promise -->
```go
refund := workflow.Workflow{Name: "refund", Steps: []workflow.Step{{Name: "approval", Run: func(ctx context.Context, in []byte) ([]byte, error) {
	var requestID string
	if err := json.Unmarshal(in, &requestID); err != nil {
		return nil, workflow.Permanent(err)
	}
	return workflow.AwaitPromise(ctx, promises, requestID, 5*time.Millisecond) // snoozes and looks again this often
}}}}
```

Anything that knows the id settles it, from outside the run: an HTTP handler, a webhook, an operator's tool.

<!-- snippet: examples/flow-promises/main.go#resolve -->
```go
if err := workflow.ResolvePromise(ctx, c, promises, "req-42", []byte(`"approved by finance"`)); err != nil {
	return err
}
```

[`workflow.ResolvePromise(ctx, client, store, id, value)`](ds:block?id=resolvepromise-ej2zjk37) stores the value and signals [`workflow.PromiseEvent`](ds:block?id=promiseevent-9gzm4g33) with the id as key ([`settlePromise`](ds:block?id=settlepromise-e3d6pbhr)), so a run waiting in a wait step (`WaitSpec{Event: workflow.PromiseEvent, Key: id}`) resumes at once; pass a nil client to skip the signal. [`workflow.RejectPromise`](ds:block?id=rejectpromise-e6uyyurn) settles it as failed with a reason, and `AwaitPromise` then fails the step with a permanent [`*workflow.PromiseRejectedError`](ds:block?id=promiserejectederror-t8d9km2g). A promise settles once: the [store's `Settle`](ds:block?id=promisestore-fpf8x3m4) is an atomic insert, so of two processes settling one promise at once exactly one wins. Settling again with the same outcome is a no-op (a retried handler is safe); a different outcome returns an error matching [`workflow.ErrPromiseSettled`](ds:block?id=errpromisesettled-cbm2esnm).

Outside a step, [`workflow.WaitPromise(ctx, store, id, opts)`](ds:block?id=waitpromise-4tdkst6u) blocks until the promise settles, and [`workflow.PromiseResult`](ds:block?id=promiseresult-f3bszagg) reads it once. [`workflow.NewPromiseID()`](ds:block?id=newpromiseid-gs7uukr4) makes a random id when the request has none of its own.

## Ask a run and get its answer

[`workflow.Update(ctx, client, store, event, opts)`](ds:block?id=update-qj6esyyk) is a signal that returns a result: it signals `event` with a fresh promise id wrapped around its payload, and waits until the run answers. The run takes it with a wait step, and the step after it wraps its body in [`workflow.UpdateHandler(store, fn)`](ds:block?id=updatehandler-cegfe95e), which calls `fn` with the caller's payload and answers with `fn`'s output:

<!-- snippet: examples/flow-promises/main.go#update-handler -->
```go
cart := workflow.Workflow{Name: "cart", Steps: []workflow.Step{
	{Name: "add", Wait: &workflow.WaitSpec{Event: "cart.add", KeyFrom: cartKey}},
	{Name: "price", After: []string{"add"}, Run: workflow.UpdateHandler(promises, func(_ context.Context, args []byte) ([]byte, error) {
		var item string
		if err := json.Unmarshal(args, &item); err != nil {
			return nil, workflow.Permanent(err)
		}
		return json.Marshal(item + " added, total 3 items")
	})},
}}
```

<!-- snippet: examples/flow-promises/main.go#update -->
```go
updCtx, cancelUpd := context.WithTimeout(ctx, 10*time.Second) // a run that never takes the signal never answers
defer cancelUpd()
answer, err := workflow.Update(updCtx, c, promises, workflow.Event{Name: "cart.add", Key: "cart-8", Payload: []byte(`"hat"`)}, await)
if err != nil {
	return err
}
```

A permanent error from `fn` rejects the update ([`UpdateHandler`](ds:block?id=updatehandler-cegfe95e)) and `Update` returns a `*workflow.PromiseRejectedError`; any other error goes to the step's retry policy while the caller keeps waiting. An input that is not an update (a plain `Signal`) is passed to `fn` as it is and nobody is answered, so one run takes both. Signals are kept from a run's enqueue, so an update sent before the run reaches its wait step is still taken. Give the caller's ctx a deadline: a run that never reaches the wait step never answers. `Update` deletes the reply once read ([`update`](ds:block?id=update-khsw5ev5)).

## Start a run if it is not running, then signal it

[`workflow.SignalWithStart(ctx, client, job, event)`](ds:block?id=signalwithstart-hk5sr9sg) enqueues `job` and then signals `event`. The job needs an `IdempotencyKey`: when a run holding that key is still unfinished, the enqueue returns that run instead of starting a second one. A run just started still takes the signal when it reaches its wait step.

<!-- snippet: examples/flow-promises/main.go#signal-with-start -->
```go
job := workflow.Job{Workflow: "cart", IdempotencyKey: "cart-7", Labels: map[string]string{"cart": "cart-7"}}
first, err := workflow.SignalWithStart(ctx, c, job, workflow.Event{Name: "cart.add", Key: "cart-7", Payload: []byte(`"socks"`)})
if err != nil {
	return err
}
```

## Loop for ever as a chain of runs

A run that loops for months (a subscription renewed monthly, a poller) would keep a growing journal, step history and error history. [`workflow.ContinueAsNew(ctx, client, payload)`](ds:block?id=continueasnew-9dkhw3cb) starts the next run of the same workflow with a new payload, carrying the run's app labels and tenant; return from the step after it and the current run ends as usual:

<!-- snippet: examples/flow-promises/main.go#continue -->
```go
const rounds = 3 // the poller stops after its third round
var client workflow.Client = c
poller := workflow.Workflow{Name: "poller", Steps: []workflow.Step{{Name: "round", Run: func(ctx context.Context, in []byte) ([]byte, error) {
	var round int
	if err := json.Unmarshal(in, &round); err != nil {
		return nil, workflow.Permanent(err)
	}
	// A real poller checks its source here; this one only counts rounds.
	if round < rounds {
		if _, err := workflow.ContinueAsNew(ctx, client, []byte(fmt.Sprint(round+1))); err != nil {
			return nil, err
		}
	}
	return json.Marshal(round)
}}}}
```

The next run's idempotency key is derived from the current run, so a retried step never starts two. It carries [`workflow.LabelContinuedFrom`](ds:block?id=labelcontinuedfrom-njsmcapf) with the run it continues, which a step reads with [`workflow.ContinuedFrom(ctx)`](ds:block?id=continuedfrom-uw5kcqu4). Labels under a reserved prefix (schedule, tick, parent) are not carried.

## Withdraw several steps of a run

Sometimes a part of a run is no longer wanted while the rest still is: two price feeds hang, and the quote should go on with cached prices rather than wait or fail. [`workflow.CancelSteps(ctx, client, ref, steps...)`](ds:block?id=cancelsteps-9p2f2na7) withdraws each listed step of one run through [`workflow.StepCanceller`](ds:block?id=stepcanceller-gp5urxfn), as `CancelStep` does for one, and leaves the rest of the run alone. The example is [`examples/flow-cancel`](../../examples/flow-cancel): a quote runs `feed-eu` and `feed-us` in parallel, both hang, and `combine` runs after both.

Each feed watches its ctx and, when the cause is [`workflow.ErrStepCancelled`](ds:block?id=errstepcancelled-bxbppyku), returns a fallback, so the step succeeds and the run continues:

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

What happens to each step is [`StepCanceller`](ds:block?id=stepcanceller-gp5urxfn)'s contract:

- **A running attempt** has its ctx cancelled with cause `workflow.ErrStepCancelled` (read it with `context.Cause`). An output from the handler means the step succeeded; an error fails the step for good, without retry, with an error matching `ErrStepCancelled`, and the run fails (`OnFailure` runs).
- **A step with no attempt running** (in retry backoff, snoozed, or a suspended wait or sleep) fails for good at once with an error matching `ErrStepCancelled`, which fails the run. Withdraw a wait step only when the run should end.
- **A step that has not started or has ended** answers `workflow.ErrWrongState`, an unknown run or step `workflow.ErrNotFound`.

[`CancelSteps`](ds:block?id=cancelsteps-9p2f2na7) tries every step, even after one fails, and returns their answers joined, each prefixed with its step name, so `errors.Is` finds any of them. No steps at all is an error matching `workflow.ErrInvalid`, and a client without `StepCanceller` is refused with an `*workflow.UnsupportedError`. The [memory](ds:block?id=memory-client-cancelstep-cn7kxx27), [SQL](ds:block?id=sql-client-cancelstep-fnqpwd5z) and [Redis](ds:block?id=redis-client-cancelstep-dzgd8reg) drivers implement it; Hatchet does not.

## Try it

```sh
cd examples
go run ./flow-promises
```

```text
promise:
  before approval the run has ended: false
  after approval: "approved by finance"
  a second, different outcome is refused: true
signal with start and update:
  started and signalled: "socks added, total 3 items"
  update answered: "hat added, total 3 items"
continue as new:
  rounds run as separate runs: 3
```

Withdrawing several steps is in `examples/flow-cancel`; its `withdraw several steps` lines are:

```sh
go run ./flow-cancel
```

```text
withdraw several steps:
  quote succeeded with "cached feed-eu + cached feed-us"
  withdraw again: ErrWrongState true, both steps named true
```

The second call finds the run ended, so both steps answer `ErrWrongState` and both are named in the joined error.

## Pitfalls

- A signal reaches every run waiting with its name and key. With [`Update`](ds:block?id=update-qj6esyyk) the first answer wins and later ones are ignored; with [`SignalWithStart`](ds:block?id=signalwithstart-hk5sr9sg) give each run its own key (`WaitSpec.KeyFrom`) so the signal reaches only the run it started.
- Promises stay in the store until deleted. Delete the ones you settle and nobody reads, or give the [Redis store](ds:block?id=redis-newpromisestore-v66xmp89) a TTL.
- [`AwaitPromise`](ds:block?id=awaitpromise-338jz9js) looks again every `poll`; a wait step on `workflow.PromiseEvent` resumes at the signal instead.
- [`CancelSteps`](ds:block?id=cancelsteps-9p2f2na7) is not atomic. It withdraws the steps one by one; if one of them fails the run (a wait step, or a handler that returns an error), the steps after it answer `ErrWrongState` because the run has ended.
- A handler that ignores its ctx is not interrupted. Withdrawing a running step only cancels its ctx ([`StepCanceller`](ds:block?id=stepcanceller-gp5urxfn)); pass `ctx` to every call and check `context.Cause` to offer a fallback.
- **Hatchet has no promise store.** The Hatchet driver offers no `PromiseStore`, so a promise settled there lives in the process unless you pass the [SQL](ds:block?id=sql-newpromisestore-cpmkmayx) or [Redis](ds:block?id=redis-newpromisestore-v66xmp89) driver's store. See [Hatchet limitations](../drivers/hatchet.md#limitations).

## Related

- [Wait for an event](signals.md): the signals these helpers build on
- [Manage runs › Cancel one step](../jobs/manage-runs.md#cancel-one-step): `CancelStep` and the fallback handler in detail
- [Joins and distributed maps › Cancel every child of a step](joins.md#cancel-every-child-of-a-step): withdraw child runs instead of steps
- [Escalation and signal handlers](escalation.md): signal one run by its ID
- Examples: [examples/flow-promises](../../examples/flow-promises/), [examples/flow-cancel](../../examples/flow-cancel/)
