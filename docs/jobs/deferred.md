# Deferred dispatch

A request handler creates an order and enqueues "send the receipt". Then validation fails further down and the handler answers 422, or a bug panics: the order was never confirmed, but the receipt job is already queued and a customer gets an email for nothing. Deferred dispatch holds the jobs a request enqueues and sends them only when the request succeeds; a failing response or a panic drops them. This page covers the HTTP middleware, using the buffer by hand outside HTTP, and when a database transaction is the better tool.

## Before you start

- Everything here is in the core package (`workflow.Deferred`, `DeferEnqueue`, `DeferHTTP`), standard library only, and works with every driver. No capability is needed: the jobs are held in your process and sent through the client as usual.
- The example is [`examples/deferred-dispatch`](../../examples/deferred-dispatch). Run it with `go run ./deferred-dispatch` from `examples/`.

## Step 1: hold enqueues made under a Deferred

Wrap the client your app enqueues through with the `DeferEnqueue` enqueue middleware:

<!-- snippet: examples/deferred-dispatch/main.go#client -->
```go
// The app enqueues through this client everywhere. Inside a request
// wrapped by DeferHTTP its jobs are held; anywhere else they are sent
// at once.
c := middleware.WrapClient(mem, workflow.DeferEnqueue())
```

When the context of an `Enqueue` carries a `workflow.Deferred`, the middleware hands the job to it and returns a zero `RunRef` and a nil error: the run does not exist yet, so there is no ref. Without a `Deferred` on the context it sends the job at once, so the same client works in requests, in background code and in tests. Enqueue middleware listed after `DeferEnqueue` runs when the job is sent, not when it is held.

## Step 2: wrap your HTTP handler

`workflow.DeferHTTP(next, opts)` gives every request its own `Deferred` on `r.Context()`. After `next` returns it reads the status the handler wrote: below 400 the held jobs are sent in order, otherwise they are dropped. A handler that panics drops them too, and the panic goes on up to your recovery middleware.

<!-- snippet: examples/deferred-dispatch/main.go#handler -->
```go
mux := http.NewServeMux()
mux.HandleFunc("POST /orders/{id}", func(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	// Held, not sent: the run exists only if this request succeeds.
	if _, err := c.Enqueue(r.Context(), workflow.Job{Workflow: "send-receipt", Payload: []byte(id)}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	switch r.URL.Query().Get("then") {
	case "fail":
		http.Error(w, "payment declined", http.StatusPaymentRequired)
	case "panic":
		panic("order handler bug")
	default:
		w.WriteHeader(http.StatusCreated)
	}
})
h := workflow.DeferHTTP(mux, workflow.DeferOptions{})
```

```text
POST /orders/o-1: status 201, panicked false
POST /orders/o-2?then=fail: status 402, panicked false
POST /orders/o-3?then=panic: status 500, panicked true
receipts after the requests: o-1
```

`DeferOptions` has three fields, each optional:

| Field | Default | Use it to |
|---|---|---|
| `Succeeded func(status int) bool` | `workflow.DeferSucceeded`: any status below 400; a handler that writes no status counts as 200 | Treat a 404 as success, or only 2xx |
| `OnError func(*http.Request, error)` | Logs with `Logger` | Count or alert on jobs that failed to send. The response is already written, so the caller cannot be told |
| `Logger *slog.Logger` | `slog.Default()` | Send the default error log somewhere else |

The jobs are sent on the request's context without its cancellation (`context.WithoutCancel`), so a client that hangs up after a successful response does not cancel them. The wrapper's response writer passes `http.Flusher` and `Unwrap` through, so streaming handlers and `http.ResponseController` keep working.

## Without HTTP: flush or discard by hand

A queue consumer, a gRPC handler or a CLI command has the same problem without an HTTP status. Create a `Deferred`, put it on the context, and decide at the end:

<!-- snippet: examples/deferred-dispatch/main.go#manual -->
```go
d := workflow.NewDeferred()
jobCtx := workflow.WithDeferred(ctx, d)
for _, id := range []string{"o-4", "o-5"} {
	if _, err := c.Enqueue(jobCtx, workflow.Job{Workflow: "send-receipt", Payload: []byte(id)}); err != nil {
		return err
	}
}
fmt.Fprintf(out, "held before flush: %d\n", d.Len())
refs, err := d.Flush(ctx) // the work this belongs to succeeded
if err != nil {
	return err
}
```

`Flush(ctx)` sends every held job in the order it was added and returns one `RunRef` per job. It keeps going when one fails: that job's ref is the zero value, and the errors come back joined. `Discard()` drops everything and returns how many jobs it dropped. A `Deferred` is used once: after `Flush` or `Discard`, `Add` returns `workflow.ErrDeferredClosed`.

<!-- snippet: examples/deferred-dispatch/main.go#discard -->
```go
d = workflow.NewDeferred()
if _, err := c.Enqueue(workflow.WithDeferred(ctx, d), workflow.Job{Workflow: "send-receipt", Payload: []byte("o-6")}); err != nil {
	return err
}
dropped := d.Discard() // the work failed: nothing is sent
_, err = c.Enqueue(workflow.WithDeferred(ctx, d), workflow.Job{Workflow: "send-receipt", Payload: []byte("o-7")})
```

`DeferredFrom(ctx)` returns the `Deferred` on a context, for code that wants to know whether its enqueues are being held. `Deferred` is safe to use from several goroutines of one request.

## Deferred dispatch or a transaction

Deferred dispatch answers "send this only if the request succeeded". It is not a transaction: the request can succeed and the process die before the flush, or the flush can fail after the response, and nothing ties the job to your database writes.

| You need | Use |
|---|---|
| The job only if the request succeeded, and losing it on a crash is acceptable | Deferred dispatch |
| The job exactly when a database transaction commits, on the SQL driver | [`EnqueueTx`](../reliability/transactional-enqueue.md) |
| The job exactly when a database transaction commits, on any other driver | The [outbox](../reliability/outbox.md) |

The two combine: inside a request wrapped by `DeferHTTP`, a job written with `EnqueueTx` or the outbox belongs to the transaction, and the jobs that should wait for the response go through the deferred client.

## Pitfalls

- **No `RunRef` while held.** A held enqueue returns the zero `RunRef`. Code that needs the ref at once (to store it, or to return it to the caller) must enqueue on a context without a `Deferred`, or read the refs `Flush` returns.
- **Errors arrive late.** Validation and capability checks run when the job is sent, so a job the driver refuses fails at flush time and reaches `OnError`, not the handler. Validate up front with `workflow.ValidateJob` when the handler should answer 400 instead.
- **Only the wrapped client defers.** Enqueues through the unwrapped client, or through `EnqueueTx`, are sent at once even inside `DeferHTTP`.
- **Not durable.** Held jobs live in the process until the flush. A crash between the response and the flush loses them.
- **A status written by middleware outside `DeferHTTP` is not seen.** Put `DeferHTTP` outside the middleware that turns errors into statuses, so it reads the final status.

## Related

- [Enqueue a job](enqueue.md): the `Job` the deferred client holds.
- [Enqueue middleware](../middleware/enqueue-middleware.md): how `DeferEnqueue` fits into a chain.
- [Transactional enqueue](../reliability/transactional-enqueue.md) and [Outbox](../reliability/outbox.md): when the job must follow a commit.
