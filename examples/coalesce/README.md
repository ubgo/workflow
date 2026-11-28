# coalesce

A search reindex with `Workflow.Coalesce`: enqueued with version v1, then three more times (v2, v3, v4) while v1 runs. The second enqueue stores one run pending behind the running one (it reports `scheduled`), the next two fold into it, and with `CoalesceKeepLast` it starts after v1 with the newest payload: two runs in all.

```sh
cd examples
go run ./coalesce
```

```text
while v1 runs, the pending run is: scheduled
runs: 2 payloads: [v1 v4]
```

The guide is [Coalescing runs](../../docs/reliability/coalesce.md).
