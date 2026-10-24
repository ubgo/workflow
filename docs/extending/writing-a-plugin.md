# Write a plugin

A plugin is how you ship a feature that touches several seams at once (what may be enqueued, what happens around every step, what happens when a run ends) as one value an app adds with `workflow.Use`. This guide writes `tenancy`, a plugin that requires a tenant label on every job, keeps one queued job per tenant and workflow, logs every step attempt and every run end. How an app adds a plugin, and the ordering rules, are in [Plugins](../concepts/plugins.md#add-a-plugin).

## Before you start

- You have used [step middleware](../middleware/step-middleware.md#wrap-every-step) or [enqueue middleware](../middleware/enqueue-middleware.md#wrap-every-enqueue); a plugin is a named bundle of them.

## What a plugin can provide

`workflow.Plugin` is the only interface a plugin must satisfy: `Name() string`, non-empty and unique among the plugins of one `Use` call, because it names the plugin in every refusal. Everything else is an optional interface that `Use` finds by type assertion, so a plugin implements only what it uses and a missing contribution is visibly missing, not an empty method that looks like a forgotten one.

| Optional interface | Method | What `Use` does with it |
|---|---|---|
| `workflow.StepMiddlewareProvider` | `StepMiddleware() []workflow.Middleware` | wraps every step's `Run` and the workflow's `OnFailure`, for workflows registered through the returned client, inside the driver's own middleware |
| `workflow.EnqueueMiddlewareProvider` | `EnqueueMiddleware() []workflow.EnqueueMiddleware` | runs on every `Enqueue` through the returned client; the first entry sees the job first |
| `workflow.RunFinishedProvider` | `RunFinishedObservers() []workflow.RunFinishedFunc` | registers each on the client's `RunNotifier`; refuses a client without one (an `UnsupportedError` naming `CapRunFinished`) rather than dropping them |

A plugin that needs a table (a lock store, an audit log) takes the store as a constructor argument and leaves creating it to the app, as every store in the library does ([Bring your own store](custom-stores.md#bring-your-own-store)).

## Write a plugin

### Step 1 — Name it

The plugin is a struct holding what its contributions need: here a log and a unique-lock store.

<!-- snippet: examples/custom-plugin/plugin.go#plugin -->
```go
// tenancy is a plugin: one value an app adds with workflow.Use, which
// requires a tenant label on every job, keeps one queued job per tenant
// and workflow at a time, logs every step attempt and every run end.
// Each contribution is an optional interface it implements; Use finds
// them by type assertion.
type tenancy struct {
	log    *lineLog
	unique middleware.UniqueStore
}

// Name names the plugin in every refusal Use returns.
func (p *tenancy) Name() string { return "tenancy" }
```

### Step 2 — Contribute enqueue middleware

An enqueue middleware takes the next `workflow.EnqueueFunc` and returns one; returning an error stops the job before it reaches the driver. Refuse with an error wrapping `workflow.ErrInvalid` so callers can tell a bad job from an outage. Library middleware composes in: `middleware.Unique` keeps one job per key, with locks in the store the plugin was given.

<!-- snippet: examples/custom-plugin/plugin.go#enqueue -->
```go
// EnqueueMiddleware makes workflow.Use run these on every Enqueue through
// the client it returns. The first entry sees the job first.
func (p *tenancy) EnqueueMiddleware() []workflow.EnqueueMiddleware {
	byTenant := func(j workflow.Job) string { return j.Labels["tenant"] + "/" + j.Workflow }
	return []workflow.EnqueueMiddleware{
		requireLabel("tenant"),
		middleware.Unique(byTenant, p.unique, time.Hour),
	}
}

// requireLabel refuses a job without the label, before it reaches the
// driver.
func requireLabel(key string) workflow.EnqueueMiddleware {
	return func(next workflow.EnqueueFunc) workflow.EnqueueFunc {
		return func(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
			if job.Labels[key] == "" {
				return workflow.RunRef{}, fmt.Errorf("%w: job for %q has no %q label", workflow.ErrInvalid, job.Workflow, key)
			}
			return next(ctx, job)
		}
	}
}
```

### Step 3 — Contribute step middleware

Step middleware is covered in [Write step middleware](writing-middleware.md#write-step-middleware); the plugin returns it from `StepMiddleware`:

<!-- snippet: examples/custom-plugin/plugin.go#step -->
```go
// StepMiddleware wraps every step of every workflow registered through
// the client Use returns.
func (p *tenancy) StepMiddleware() []workflow.Middleware {
	return []workflow.Middleware{logAttempts(p.log)}
}

// logAttempts is step middleware: it reads what the driver put on the
// ctx (workflow.StepInfo), calls the next handler and logs the outcome.
// It never logs the payload, which may hold personal data.
func logAttempts(log *lineLog) workflow.Middleware {
	return func(next workflow.Handler) workflow.Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			info, _ := workflow.StepInfoFrom(ctx)
			out, err := next(ctx, in)
			outcome := "ok"
			if err != nil {
				outcome = "error: " + err.Error()
			}
			log.Printf("step %s/%s attempt %d tenant=%s: %s", info.Workflow, info.Step, info.Attempt, info.Labels["tenant"], outcome)
			return out, err
		}
	}
}
```

### Step 4 — Observe run ends

An observer runs on a driver goroutine after `Status` shows the end. It must return quickly and be idempotent, because delivery is at least once; long work belongs in a new `Enqueue`.

<!-- snippet: examples/custom-plugin/plugin.go#observer -->
```go
// RunFinishedObservers are registered on the driver's RunNotifier by Use,
// so the client must implement it and Use must run before Start.
func (p *tenancy) RunFinishedObservers() []workflow.RunFinishedFunc {
	return []workflow.RunFinishedFunc{func(ctx context.Context, ev workflow.RunEvent) {
		p.log.Printf("run %s of %s for tenant=%s: %s", ev.Ref.RunID, ev.Workflow, ev.Labels["tenant"], ev.Status)
	}}
}
```

### Step 5 — Add it

<!-- snippet: examples/custom-plugin/main.go#use -->
```go
base := memory.New()
plugin := &tenancy{log: log, unique: kvUnique{kv: newMemKV(clock.Real{}), prefix: "unique:"}}
c, err := workflow.Use(base, plugin)
if err != nil {
	return err
}
```

<!-- snippet: examples/custom-plugin/main.go#enqueue-calls -->
```go
_, err = c.Enqueue(ctx, workflow.Job{Workflow: "invoice"})
log.Printf("no tenant: %v", err)

_, err = c.Enqueue(ctx, workflow.Job{Workflow: "invoice", Labels: map[string]string{"tenant": "acme"}})
if err != nil {
	return err
}
<-finished

_, err = c.Enqueue(ctx, workflow.Job{Workflow: "invoice", Labels: map[string]string{"tenant": "acme"}})
log.Printf("second job for acme: %v", err)
```

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

`go test ./custom-plugin` also checks that `Use` refuses the plugin listed twice with `ErrDuplicatePlugin`.

## Pitfalls

- An observer plugin on a client without `RunNotifier` is refused by `Use`. If your plugin can work without run ends (a lock that expires on a TTL, say), offer the observer as a separate plugin or an option, so apps on such a client can still use the rest.
- Observers can only be added before `Start`, so `Use` must run before `Start`.
- Step middleware sees the input after the driver's own middleware ran (a driver-level codec has already decrypted it). Do not log payloads: they may hold personal data.
- `middleware.Unique` owns the `ubgo.workflow/` unique labels and refuses a job that already carries them; one `Unique` per chain ([Reserved prefixes](../concepts/labels.md#reserved-prefixes)).
- Enqueue middleware does not run on `sqlstore.Client.EnqueueTx`. A plugin whose enqueue rules must hold for transactional enqueues needs the outbox in front ([Choose how to enqueue](../choosing/methods.md#enqueue-enqueuetx-or-the-outbox)).

## Related

- [Plugins](../concepts/plugins.md#add-a-plugin)
- [Write step middleware](writing-middleware.md#write-step-middleware)
- [Bring your own store](custom-stores.md#bring-your-own-store)
- [Unique jobs](../reliability/unique-jobs.md#unique-for-a-time)
- Example: [custom-plugin](../../examples/custom-plugin/)
