# Virtual objects

Some work belongs to one entity at a time: a customer's cart, an account's balance, a device's settings. Two calls for the same cart must not run at once, and each call needs the cart as the last call left it. A virtual object gives every key its own serial lane and its own durable state, so the app writes methods instead of a table, a lock and a loader.

## Before you start

- An object is a workflow whose calls are serialised per key with `Concurrency{MaxRuns: 1}`, so the driver needs `workflow.CapConcurrencyKey` (memory, SQL, Redis and Hatchet). `ObjectSpec.Ordered` also needs `workflow.CapKeyFIFO`.
- State lives in a `workflow.JournalStore`: `sqlstore.Client.JournalStore()` or `redis.Client.JournalStore()` so it survives restarts and every worker sees it, `workflow.NewMemoryJournal()` for one process. No new table is needed.
- The example is [`examples/virtual-objects`](../../examples/virtual-objects). Run it with `go run ./virtual-objects` from `examples/`.

## Define an object

`workflow.DefineObject(spec)` returns the workflow that runs an object's calls; register it like any workflow. Each `workflow.ObjectMethod` receives the key's current state (the zero value for a new key) and the call's arguments, and returns the new state and the call's result:

<!-- snippet: examples/virtual-objects/main.go#define -->
```go
// Cart is the state one cart keeps between calls.
type Cart struct {
	Items []string `json:"items"`
}

func cartObject(store workflow.JournalStore) (workflow.Workflow, error) {
	return workflow.DefineObject(workflow.ObjectSpec[Cart]{
		Name:  "cart",
		Store: store,
		Methods: map[string]workflow.ObjectMethod[Cart]{
			"add": func(_ context.Context, c Cart, args []byte) (Cart, []byte, error) {
				var item string
				if err := json.Unmarshal(args, &item); err != nil {
					return c, nil, workflow.Permanent(err)
				}
				c.Items = append(c.Items, item)
				out, err := json.Marshal(len(c.Items))
				return c, out, err
			},
			"clear": func(context.Context, Cart, []byte) (Cart, []byte, error) {
				return Cart{}, nil, nil
			},
		},
	})
}
```

## Call it

`workflow.CallObject(ctx, client, object, key, method, args, job)` enqueues one call and returns its run; `workflow.AwaitResult` gives the method's result. `job` may carry the run's other fields (`IdempotencyKey`, `RunAt`, `Labels`, `Tenant`).

<!-- snippet: examples/virtual-objects/main.go#call -->
```go
var refs []workflow.RunRef
for _, item := range []string{"socks", "hat", "scarf"} {
	args, err := json.Marshal(item)
	if err != nil {
		return err
	}
	ref, err := workflow.CallObject(ctx, c, "cart", "alice", "add", args, workflow.Job{})
	if err != nil {
		return err
	}
	refs = append(refs, ref)
}
```

Calls of one key run one at a time; calls of different keys run in parallel. Without `Ordered`, the calls of a key run in whatever order the driver claims them; with it, in the order they were enqueued.

## Read the state

`workflow.ObjectState[S](ctx, store, object, key)` reads a key's last saved state from outside any call, for a query endpoint or a test; false means the key was never called.

<!-- snippet: examples/virtual-objects/main.go#state -->
```go
alice, _, err := workflow.ObjectState[Cart](ctx, store, "cart", "alice")
if err != nil {
	return err
}
```

## How a call is applied once

A call loads the key's record, runs the method, and saves the new state together with the call's run ID and result. A call retried after its save (the process died before the step's end was recorded) finds its own run ID in the record and returns the saved result without running the method again. A method error leaves the state as it was and goes to the retry policy (`ObjectSpec.Retry`). Each call carries the key in the reserved label `workflow.LabelObjectKey`, set only through `CallObject`.

## Pitfalls

- A slow method holds up every later call of its key. Keep methods short; start long work as a child run from the method's caller instead.
- `ObjectState` reads the last saved state; a call running now may replace it a moment later.
- With `workflow.NewMemoryJournal` the state lives in one process. Use the driver's durable journal for anything that must survive a restart.
