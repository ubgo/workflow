# virtual-objects

A keyed virtual object on the in-memory driver: one shopping cart per customer, whose calls run one at a time per cart and share the cart's state, kept in the journal store.

```sh
cd examples
go run ./virtual-objects
```

```text
alice's cart holds 3 items
bob's cart holds 1 item
after clear, alice's cart holds 0 items
```

## The code

Define the object, its state and its methods:

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

Call it:

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

Read a key's state from outside:

<!-- snippet: examples/virtual-objects/main.go#state -->
```go
alice, _, err := workflow.ObjectState[Cart](ctx, store, "cart", "alice")
if err != nil {
	return err
}
```

The guide is [docs/workflows/objects.md](../../docs/workflows/objects.md).
