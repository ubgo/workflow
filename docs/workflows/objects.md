# Virtual objects

Some work belongs to one entity at a time: a customer's cart, an account's balance, a device's settings. Two calls for the same cart must not run at once, and each call needs the cart as the last call left it. A virtual object gives every key its own serial lane and its own durable state, so the app writes methods instead of a table, a lock and a loader.

## Before you start

- An object is a workflow whose calls are serialised per key with `Concurrency{MaxRuns: 1}`, so the driver needs `workflow.CapConcurrencyKey` and a key computed in Go: memory, SQL and Redis run objects; Hatchet refuses the workflow at `Register`, since its concurrency keys are engine expressions. `ObjectSpec.Ordered` also needs `workflow.CapKeyFIFO`.
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

// cartObject defines the cart object: its state lives in store and its
// methods add an item or clear the cart.
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

A call loads the key's record, runs the method, and saves the new state together with the call's run ID and result. A call retried after its save (the process died before the step's end was recorded) finds its own run ID in the record and returns the saved result without running the method again. A method error leaves the state as it was and goes to the retry policy (`ObjectSpec.Retry`, the same field as `Workflow.Retry`).

## Find a key's calls

Each call carries the key in the reserved label `workflow.LabelObjectKey`, set only through `CallObject` (it lifts the reserved-prefix guard for that one label with `workflow.AllowReservedLabels`). A run listing filters on it like any label, so an admin page or a test finds every call of one key:

<!-- snippet: examples/virtual-objects/main.go#calls -->
```go
page, err := c.Runs(ctx, workflow.RunQuery{ // c implements workflow.RunQuerier
	Workflow: "cart",
	Labels:   map[string]string{workflow.LabelObjectKey: "alice"}, // set by CallObject
})
if err != nil {
	return err
}
fmt.Println("calls of alice's cart so far:", len(page.Runs))
```

The listing holds runs in every status: here the three `add` calls and the `clear`. The serialisation itself uses the same label as the concurrency key (`Concurrency{Key: LabelObjectKey, MaxRuns: 1}`), which is why a job enqueued with `Enqueue` instead of `CallObject` is not a call: it has no key, and its step fails with a permanent error that says to enqueue it with `CallObject`.

## A method the object does not have

`CallObject` only enqueues; it does not know the object's methods, so a misspelt method is accepted there and fails the call's run with a permanent error that lists the methods the object has:

<!-- snippet: examples/virtual-objects/main.go#unknown-method -->
```go
ref, err = workflow.CallObject(ctx, c, "cart", "alice", "remove", []byte(`"hat"`), workflow.Job{})
if err != nil {
	return err // CallObject does not know the methods; the call's run does
}
_, err = workflow.AwaitResult(ctx, c, ref, await)
var failed *workflow.RunFailedError
if !errors.As(err, &failed) {
	return fmt.Errorf("a call of an unknown method did not fail: %v", err)
}
fmt.Printf("unknown method: the call %s, naming the methods: %v\n",
	failed.Status, strings.Contains(failed.Err, "(methods: [add clear])"))
```

The state is untouched. `DefineObject` itself refuses, with `ErrInvalid`, a spec without a `Name`, `Methods` or a `Store`, and a method with an empty name or a nil function.

## Try it

```sh
cd examples
go run ./virtual-objects
```

```text
alice's cart holds 3 items
bob's cart holds 1 item
after clear, alice's cart holds 0 items
calls of alice's cart so far: 4
unknown method: the call failed, naming the methods: true
```

Alice's three `add` calls ran one after another on the same state, while Bob's ran beside them on his own.

## Pitfalls

- A slow method holds up every later call of its key. Keep methods short; start long work as a child run from the method's caller instead.
- `ObjectState` reads the last saved state; a call running now may replace it a moment later.
- With `workflow.NewMemoryJournal` the state lives in one process. Use the driver's durable journal for anything that must survive a restart.
- **Without `Ordered`, a key's calls may run in any order.** They never overlap, but two calls enqueued back to back can be claimed the other way round. Set `ObjectSpec.Ordered` when the order matters; it needs `workflow.CapKeyFIFO`.
- **Do not fork a call.** `ForkRun` drops `LabelObjectKey` with the other membership labels, so the fork has no key and its step fails permanently. Enqueue a new call with `CallObject` instead; resetting a failed call keeps its labels and works.
- **The state goes through JSON.** It is saved with `encoding/json` after every call, so unexported fields are lost and a stored state that no longer decodes into the type fails the call permanently. Change the state type the way you would change a stored JSON document.

## Related

- [Durable state machines](state-machines.md): a keyed object whose calls are events moving it between states.
- [Concurrency keys](../jobs/concurrency.md): the per-key limit an object is built on.
- [Memoise work inside a step](memoisation.md#turn-on-the-journal): the journal stores that keep object state.
- [Manage runs](../jobs/manage-runs.md#search-by-label): run listings by label.
- Example: [examples/virtual-objects](../../examples/virtual-objects/)
