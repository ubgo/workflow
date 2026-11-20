# Promises, updates and signal helpers

[Signals](signals.md) resume a run that waits for an event. Some flows need more: a run that waits for a decision an API call makes later (an approval, a manual review), a caller that sends a run a request and needs its answer back, a signal to a run that may not exist yet, a run that loops for months and must not grow without bound. These helpers build each of them from signals, snoozes and a small store, and run on every driver with the capabilities they name.

## Before you start

- Promises and updates keep their values in a `workflow.PromiseStore`. `workflow.NewMemoryPromises()` is the in-process store; `sqlstore.Client.PromiseStore()` (or `sqlstore.NewPromiseStore`, rows in the journal table) and `redis.Client.PromiseStore()` (or `redis.NewPromiseStore`, needs Redis 7 or later) are durable and shared by every process on that database. The caller and the run must use the same store.
- `AwaitPromise` waits by snoozing, so it needs `workflow.CapSnooze` (memory, SQL and Redis). `Update` and `SignalWithStart` need `workflow.CapWaitForEvent`; `SignalWithStart` and `ContinueAsNew` need `workflow.CapIdempotencyKey`.
- The example is [`examples/flow-promises`](../../examples/flow-promises). Run it with `go run ./flow-promises` from `examples/`.

## Wait for a promise

A promise is a value stored once under an id and read by anyone who knows the id. A step waits for it with `workflow.AwaitPromise(ctx, store, id, poll)`, which returns the value once the promise is settled and `workflow.Snooze(poll)` until then, so the step holds no worker while it waits:

<!-- snippet: examples/flow-promises/main.go#await-promise -->
```go
refund := workflow.Workflow{Name: "refund", Steps: []workflow.Step{{Name: "approval", Run: func(ctx context.Context, in []byte) ([]byte, error) {
	var requestID string
	if err := json.Unmarshal(in, &requestID); err != nil {
		return nil, workflow.Permanent(err)
	}
	return workflow.AwaitPromise(ctx, promises, requestID, 5*time.Millisecond)
}}}}
```

Anything that knows the id settles it, from outside the run: an HTTP handler, a webhook, an operator's tool.

<!-- snippet: examples/flow-promises/main.go#resolve -->
```go
if err := workflow.ResolvePromise(ctx, c, promises, "req-42", []byte(`"approved by finance"`)); err != nil {
	return err
}
```

`workflow.ResolvePromise(ctx, client, store, id, value)` stores the value and signals `workflow.PromiseEvent` with the id as key, so a run waiting in a wait step (`WaitSpec{Event: workflow.PromiseEvent, Key: id}`) resumes at once; pass a nil client to skip the signal. `workflow.RejectPromise` settles it as failed with a reason, and `AwaitPromise` then fails the step with a permanent `*workflow.PromiseRejectedError`. A promise settles once: the store's `Settle` is an atomic insert, so of two processes settling one promise at once exactly one wins. Settling again with the same outcome is a no-op (a retried handler is safe); a different outcome returns an error matching `workflow.ErrPromiseSettled`.

Outside a step, `workflow.WaitPromise(ctx, store, id, opts)` blocks until the promise settles, and `workflow.PromiseResult` reads it once. `workflow.NewPromiseID()` makes a random id when the request has none of its own.

## Ask a run and get its answer

`workflow.Update(ctx, client, store, event, opts)` is a signal that returns a result: it signals `event` with a fresh promise id wrapped around its payload, and waits until the run answers. The run takes it with a wait step, and the step after it wraps its body in `workflow.UpdateHandler(store, fn)`, which calls `fn` with the caller's payload and answers with `fn`'s output:

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
updCtx, cancelUpd := context.WithTimeout(ctx, 10*time.Second)
defer cancelUpd()
answer, err := workflow.Update(updCtx, c, promises, workflow.Event{Name: "cart.add", Key: "cart-8", Payload: []byte(`"hat"`)}, await)
if err != nil {
	return err
}
```

A permanent error from `fn` rejects the update and `Update` returns a `*workflow.PromiseRejectedError`; any other error goes to the step's retry policy while the caller keeps waiting. An input that is not an update (a plain `Signal`) is passed to `fn` as it is and nobody is answered, so one run takes both. Signals are kept from a run's enqueue, so an update sent before the run reaches its wait step is still taken. Give the caller's ctx a deadline: a run that never reaches the wait step never answers. `Update` deletes the reply once read.

## Start a run if it is not running, then signal it

`workflow.SignalWithStart(ctx, client, job, event)` enqueues `job` and then signals `event`. The job needs an `IdempotencyKey`: when a run holding that key is still unfinished, the enqueue returns that run instead of starting a second one. A run just started still takes the signal when it reaches its wait step.

<!-- snippet: examples/flow-promises/main.go#signal-with-start -->
```go
job := workflow.Job{Workflow: "cart", IdempotencyKey: "cart-7", Labels: map[string]string{"cart": "cart-7"}}
first, err := workflow.SignalWithStart(ctx, c, job, workflow.Event{Name: "cart.add", Key: "cart-7", Payload: []byte(`"socks"`)})
if err != nil {
	return err
}
```

## Loop for ever as a chain of runs

A run that loops for months (a subscription renewed monthly, a poller) would keep a growing journal, step history and error history. `workflow.ContinueAsNew(ctx, client, payload)` starts the next run of the same workflow with a new payload, carrying the run's app labels and tenant; return from the step after it and the current run ends as usual:

<!-- snippet: examples/flow-promises/main.go#continue -->
```go
var client workflow.Client = c
poller := workflow.Workflow{Name: "poller", Steps: []workflow.Step{{Name: "round", Run: func(ctx context.Context, in []byte) ([]byte, error) {
	var round int
	if err := json.Unmarshal(in, &round); err != nil {
		return nil, workflow.Permanent(err)
	}
	// ... one round of polling ...
	if round < 3 {
		if _, err := workflow.ContinueAsNew(ctx, client, []byte(fmt.Sprint(round+1))); err != nil {
			return nil, err
		}
	}
	return json.Marshal(round)
}}}}
```

The next run's idempotency key is derived from the current run, so a retried step never starts two. It carries `workflow.LabelContinuedFrom` with the run it continues, which a step reads with `workflow.ContinuedFrom(ctx)`. Labels under a reserved prefix (schedule, tick, parent) are not carried.

## Withdraw several steps of a run

`workflow.CancelSteps(ctx, client, ref, steps...)` withdraws several steps of one run through `workflow.StepCanceller` (memory, SQL and Redis), as `CancelStep` does for one: a running step's ctx is cancelled with cause `workflow.ErrStepCancelled` and its handler decides what it returns, while the rest of the run goes on. It tries every step and returns their errors joined.

## Pitfalls

- A signal reaches every run waiting with its name and key. With `Update` the first answer wins and later ones are ignored; with `SignalWithStart` give each run its own key (`WaitSpec.KeyFrom`) so the signal reaches only the run it started.
- Promises stay in the store until deleted. Delete the ones you settle and nobody reads, or give the Redis store a TTL.
- `AwaitPromise` looks again every `poll`; a wait step on `workflow.PromiseEvent` resumes at the signal instead.
