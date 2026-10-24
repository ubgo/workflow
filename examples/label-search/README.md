# label-search

Runs found by their labels on the SQL driver over SQLite: an exact tenant label combined with an order-number prefix (`RunQuery.LabelPrefixes`), and "has a coupon label at all" (an empty prefix). Both are answered from the label index that migration `005_run_labels` adds, so a label search does not read every run. The program then calls `Client.IndexLabels`, the backfill an app runs after applying the migrations with its own tool, twice to show it is safe to repeat, and checks that the Hatchet driver refuses a prefix filter instead of ignoring it.

```sh
cd examples
go run ./label-search
```

```text
label prefix capability: true
acme, order A-*: A-1001 A-1002
has a coupon: A-1002 A-1003
IndexLabels: 4 runs, then 0
hatchet refuses: field RunQuery.LabelPrefixes, missing LabelPrefix
```

The first `IndexLabels` rewrites the rows of the four runs enqueued after `Migrate`'s own pass, with the same rows the enqueue wrote; the second finds nothing past its watermark. The same filters work on the in-memory driver and through the admin API (`label=order=A-*`).

The guide that copies this program is [Manage runs](../../docs/jobs/manage-runs.md#search-by-label). `go test ./label-search` checks every line.
