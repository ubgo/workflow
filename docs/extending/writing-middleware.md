# Write middleware

Cross-cutting behaviour (logging, tracing, metrics, recovery, timeouts, codecs, tenant rules) is middleware, applied the same way on every driver, never a wrapper that would hide what the driver can do. The library ships the common ones in `middleware` and `otel`; this guide writes your own: a step middleware that logs every attempt, and an enqueue middleware that refuses a job without a label.

## Before you start

- You know what [step middleware](../middleware/step-middleware.md#wrap-every-step) and [enqueue middleware](../middleware/enqueue-middleware.md#wrap-every-enqueue) are and how to install them.

## Write step middleware

A `workflow.Middleware` is `func(next workflow.Handler) workflow.Handler`. It runs around every attempt of every step (and around `OnFailure`), so it can read or change the input, time the call, change the error or the output. What it knows about the attempt comes from `workflow.StepInfoFrom(ctx)`, which every driver sets before the middleware chain runs: the workflow, step, run ID, attempt number (1 for the first) and the run's labels.

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

An enqueue middleware has the same shape one level up: `func(next workflow.EnqueueFunc) workflow.EnqueueFunc`, around every `Enqueue`. Return an error wrapping `workflow.ErrInvalid` to refuse a job; `StepInfoFrom` reports false there, since an enqueue is not a step.

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

Three ways to install them, all equivalent in effect:

| Where | Step middleware | Enqueue middleware |
|---|---|---|
| On the driver | `memory.WithMiddleware`, `sqlstore.WithMiddleware`, `hatchet.Config.Middleware` | — |
| Around any client | — | `middleware.WrapClient(c, mws...)`, or `middleware.ChainEnqueue` to compose |
| As a plugin | `StepMiddlewareProvider` | `EnqueueMiddlewareProvider` |

`middleware.Chain(mws...)` composes step middleware into one; in every list, the first entry is the outermost.

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

The first line is `requireLabel`, the second `logAttempts`.

## Pitfalls

- Return the error you were given, or wrap it with `%w`. Replacing it with a new error loses `workflow.Permanent` and `workflow.RetryAfter` marks, and the step is retried when it should not be (or the other way round). To stop retries on purpose, return `workflow.Permanent(err)`; the library's `middleware.FailOn` does that for errors you match.
- Middleware runs once per attempt, not once per run. Count runs in a run-finished observer, attempts here.
- Respect `ctx`: a step whose run was cancelled or timed out sees its context end. Middleware that waits (a rate limiter, a lock) must give up when it does.
- Do not log `in` or `out`: payloads may hold personal data, and the library's own `Logging` never logs them.
- On a durable driver the same attempt may run again on another worker after a crash, so a side effect in middleware (a counter in another store) may happen twice.
- Enqueue middleware wraps `Client.Enqueue` only. It does not see `sqlstore.Client.EnqueueTx`, nor the runs a driver starts on its own for a cron tick, since neither goes through the wrapped `Enqueue`.

## Related

- [Wrap every step](../middleware/step-middleware.md#wrap-every-step), with the shipped step middleware
- [Wrap every enqueue](../middleware/enqueue-middleware.md#wrap-every-enqueue)
- [Write a plugin](writing-a-plugin.md#write-a-plugin)
- Example: [custom-plugin](../../examples/custom-plugin/)
