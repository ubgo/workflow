# Plugins

A plugin bundles a feature (multi-tenancy rules, an audit log, unique jobs, tracing) so an app adds it with one call instead of wiring each piece by hand. A plugin can contribute step middleware, enqueue middleware and run-finished observers; `workflow.Use` applies them all to a client and returns the client to use from then on. The core knows nothing about any particular plugin.

## Before you start

- You know the [client](client.md#start-and-stop) lifecycle: plugins are applied before `Register` and `Start`.

## Add a plugin

`workflow.Use(c, plugins...)` takes a client and any number of `workflow.Plugin` values, each of which needs only a `Name() string`. What a plugin contributes is found by type assertion on three optional interfaces ([What a plugin can provide](../extending/writing-a-plugin.md#what-a-plugin-can-provide)):

| Interface | Contributes | Applied to |
|---|---|---|
| `workflow.StepMiddlewareProvider` | `StepMiddleware() []workflow.Middleware` | every step's handler and the `OnFailure` handler of workflows registered through the returned client |
| `workflow.EnqueueMiddlewareProvider` | `EnqueueMiddleware() []workflow.EnqueueMiddleware` | every `Enqueue` through the returned client |
| `workflow.RunFinishedProvider` | `RunFinishedObservers() []workflow.RunFinishedFunc` | registered on the client's `RunNotifier` by `Use` itself |

The [custom-plugin](../../examples/custom-plugin/) example's `tenancy` plugin implements all three. Adding it:

<!-- snippet: examples/custom-plugin/main.go#use -->
```go
base := memory.New()
plugin := &tenancy{log: log, unique: kvUnique{kv: newMemKV(clock.Real{}), prefix: "unique:"}}
c, err := workflow.Use(base, plugin)
if err != nil {
	return err
}
```

Then register and start through the client `Use` returned:

<!-- snippet: examples/custom-plugin/main.go#register -->
```go
// Register and Start through c, the client Use returned, so the
// plugin's step middleware wraps the workflow's steps.
err = c.Register(workflow.Workflow{Name: "invoice", Steps: []workflow.Step{{
	Name: "send",
	Run:  func(ctx context.Context, in []byte) ([]byte, error) { return nil, nil },
}}})
if err != nil {
	return err
}
if err := c.Start(ctx); err != nil {
	return err
}
```

The rules `Use` keeps:

- **Order is the order given.** The first plugin's middleware is outermost (it sees the job, or the step, first), and its observers are registered first. Within a plugin, its list's first entry is outermost.
- **Plugin step middleware runs inside the driver's own.** A driver built with `WithMiddleware` (a codec, say) has already opened the input before a plugin's step middleware sees it.
- **It checks before it changes anything.** A nil plugin or an empty name is `ErrInvalid`; two plugins with one name in one call are `ErrDuplicatePlugin`; a plugin with observers on a client that does not implement `RunNotifier` is an `UnsupportedError` naming `CapRunFinished` and the plugin. All of this is checked before any observer is registered.
- **Observers can only be added before `Start`.** After `Start` the driver refuses them with `ErrAlreadyStarted`, which `Use` returns naming the plugin. Observers registered before a failing one stay registered, since `RunNotifier` has no way to remove one.
- **Nothing is hidden.** The returned client keeps the inner client's `Capabilities` and forwards `RunNotifier`, so wrappers stacked on it still see the run-finished hook. With no plugins, `Use` returns the client unchanged.

## Try it

```sh
cd examples
go run ./custom-plugin
```

```text
no tenant: workflow: invalid spec: job for "invoice" has no "tenant" label
step invoice/send attempt 1 tenant=acme: ok
run run-2 of invoice for tenant=acme: succeeded
second job for acme: workflow: duplicate idempotency key: unique key "acme/invoice" is held
```

The first line is the plugin's enqueue middleware refusing a job without a tenant, the second its step middleware, the third its observer, and the last its unique-jobs middleware holding the tenant's key. The run ID is the memory driver's own.

## Pitfalls

- Registering on the bare driver (`base.Register`) instead of the client `Use` returned skips the plugin's step middleware for that workflow, silently. Keep one client variable after `Use`.
- `ErrDuplicatePlugin` only sees the plugins of one call. Two `Use` calls with the same plugin apply it twice.
- An observer plugin needs a client that implements `RunNotifier`. The memory, SQL and Redis drivers do, with guaranteed delivery (`CapRunFinished`); the Hatchet driver does best-effort only, so a plugin that must see every end needs a fallback there.
- Enqueue middleware does not run on `sqlstore.Client.EnqueueTx`, which writes inside your transaction ([Choose how to enqueue](../choosing/methods.md#enqueue-enqueuetx-or-the-outbox)).

## Related

- [Write a plugin](../extending/writing-a-plugin.md#write-a-plugin)
- [Write step middleware](../extending/writing-middleware.md#write-step-middleware)
- [Wrap every step](../middleware/step-middleware.md#wrap-every-step) and [wrap every enqueue](../middleware/enqueue-middleware.md#wrap-every-enqueue)
- Example: [custom-plugin](../../examples/custom-plugin/)
