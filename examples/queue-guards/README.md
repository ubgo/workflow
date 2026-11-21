# queue-guards

The counting functions behind `middleware.Bounded` and `middleware.Quota`, written by the app, on the in-memory driver: a `DepthFunc` that bounds the waiting runs of each workflow (the stock `DepthFromRuns` bounds each queue) and a `CountFunc` that reads a tenant's open uploads from the app's own table. A count that fails fails the enqueue.

```sh
cd examples
go run ./queue-guards
```

```text
globex export 1: accepted
globex export 2: accepted
globex export 3: refused, queue full (workflow/middleware: queue is full: 2 of 2 for workflow "export")
globex report: accepted
acme report: refused, quota (workflow/middleware: tenant quota exceeded: 5 of 5 for workflow "report")
report without a tenant: accepted
globex report, table down: failed (workflow/middleware: Quota count: uploads table unreachable)
```

The guide that copies this program is [job guards](../../docs/jobs/guards.md#bound-a-queue-and-cap-a-tenant).
