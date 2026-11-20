# scoped-data

Data shared beyond one run: a folder of runs that belong together (one customer), every run of one workflow, and every run of every workflow. A signup run puts the customer's plan in the customer's folder; order runs read it and add to the customer's total with `UpdateData`, which never loses a concurrent write; each run counts itself in its workflow's scope and in the global scope. It runs first on the in-memory `DataStore`, then on sqlstore's `DataStore` over one SQLite file with two clients, as two processes would hold it, so ten orders split across both add up exactly.

```sh
cd examples
go run ./scoped-data
```

```text
memory:
  order: pro plan, 40 spent so far
  order: pro plan, 100 spent so far
  globex order failed: true
  runs: signup 1, order 2, all 3
sqlite, two clients:
  acme spent 100 over 10 orders; runs counted 11
```

The globex order fails permanently because no signup ran for that customer, so its folder has no plan. The guide is [Memoisation › Share data across runs](../../docs/workflows/memoisation.md#share-data-across-runs).
