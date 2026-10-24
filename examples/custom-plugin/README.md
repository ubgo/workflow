# custom-plugin

A feature bundled as a `workflow.Plugin` and added with one `workflow.Use` call. The `tenancy` plugin in `plugin.go` contributes enqueue middleware (a required `tenant` label, then `middleware.Unique` so a tenant has one queued job per workflow), step middleware that logs every attempt from `workflow.StepInfo`, and a run-finished observer. The uniqueness locks live in `store.go`, a `middleware.UniqueStore` written over a two-call key-value interface, the shape a Redis or SQL-backed store takes.

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

The run ID is the memory driver's own and opaque; the test checks the line around it.

The guides that copy this program: [writing a plugin](../../docs/extending/writing-a-plugin.md), [writing middleware](../../docs/extending/writing-middleware.md), [custom stores](../../docs/extending/custom-stores.md) and [plugins](../../docs/concepts/plugins.md). `main_test.go` runs it, checks that 50 racing acquires of one key give exactly one winner, and that `Use` refuses two plugins with one name.
