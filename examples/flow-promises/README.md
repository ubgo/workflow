# flow-promises

Signal helpers on the in-memory driver: a refund run waits on a promise until an API call resolves it, a cart run is started and signalled in one call and answers an update, and a poller loops as a chain of short runs.

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

## The code

A step that waits for a promise:

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

Resolving it from outside the run:

<!-- snippet: examples/flow-promises/main.go#resolve -->
```go
if err := workflow.ResolvePromise(ctx, c, promises, "req-42", []byte(`"approved by finance"`)); err != nil {
	return err
}
```

A run that answers updates:

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

Start the run if needed, then signal it:

<!-- snippet: examples/flow-promises/main.go#signal-with-start -->
```go
job := workflow.Job{Workflow: "cart", IdempotencyKey: "cart-7", Labels: map[string]string{"cart": "cart-7"}}
first, err := workflow.SignalWithStart(ctx, c, job, workflow.Event{Name: "cart.add", Key: "cart-7", Payload: []byte(`"socks"`)})
if err != nil {
	return err
}
```

Send an update and wait for the answer:

<!-- snippet: examples/flow-promises/main.go#update -->
```go
updCtx, cancelUpd := context.WithTimeout(ctx, 10*time.Second) // a run that never takes the signal never answers
defer cancelUpd()
answer, err := workflow.Update(updCtx, c, promises, workflow.Event{Name: "cart.add", Key: "cart-8", Payload: []byte(`"hat"`)}, await)
if err != nil {
	return err
}
```

A loop that continues as new runs:

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

The guide is [docs/workflows/promises.md](../../docs/workflows/promises.md).
