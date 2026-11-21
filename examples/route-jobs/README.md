# route-jobs

`middleware.Route` on the in-memory driver: a routing table in enqueue middleware sends bulk invoices to a workflow of their own and labels every job with a cost center, a label chosen at the enqueue site wins over the table, and the caller's labels map is never changed. Each run is read back through `RunQuerier` to show the workflow and labels it was enqueued with.

```sh
cd examples
go run ./route-jobs
```

```text
job invoice map[tier:bulk] -> run of invoice-bulk, cost-center=batch
job invoice map[] -> run of invoice, cost-center=web
job invoice map[cost-center:sales] -> run of invoice, cost-center=sales
the caller's labels are not changed: map[tier:bulk]
```

The guide that copies this program is [enqueue middleware](../../docs/middleware/enqueue-middleware.md#route-jobs).
