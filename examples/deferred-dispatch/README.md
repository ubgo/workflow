# deferred-dispatch

Jobs enqueued during an HTTP request, sent only when the request succeeds: `workflow.DeferHTTP` gives each request a `workflow.Deferred`, and a client wrapped with `workflow.DeferEnqueue()` holds its enqueues there. A handler that answers 201 gets its receipt job; one that answers 402 or panics gets none. Outside HTTP, the same buffer is flushed or discarded by hand.

```sh
cd examples
go run ./deferred-dispatch
```

```text
POST /orders/o-1: status 201, panicked false
POST /orders/o-2?then=fail: status 402, panicked false
POST /orders/o-3?then=panic: status 500, panicked true
receipts after the requests: o-1
held before flush: 2
flushed: 2 runs
discarded: 1, enqueue after discard: ErrDeferredClosed true
receipts at the end: o-1,o-4,o-5
```

Each handler enqueues its receipt before deciding how to answer. `DeferHTTP` reads the status after the handler returns: below 400 it flushes, otherwise it drops; a panic drops the held jobs and goes on up (the program recovers it to print the line). A `Deferred` is single use, so an enqueue after `Discard` returns `ErrDeferredClosed`.

A deferred job is not tied to a database transaction. When the job must exist exactly when a transaction commits, use `EnqueueTx` or the outbox instead.

The guide that copies this program is [deferred dispatch](../../docs/jobs/deferred.md). `go test ./deferred-dispatch` checks every line.
