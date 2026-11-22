# scoped-data

Data shared beyond one run, in three scopes: a folder of runs that belong together, here one customer ([`ScopeFolder`](ds:block?id=scopefolder-f2h5p7uh), built with [`Folder`](ds:block?id=folder-bacd2p59)), every run of one workflow ([`ScopeWorkflow`](ds:block?id=scopeworkflow-sgm5gs92), built with [`WorkflowScope`](ds:block?id=workflowscope-7wc874f8)), and every run of every workflow ([`ScopeGlobal`](ds:block?id=scopeglobal-8thshyka), built with [`GlobalScope`](ds:block?id=globalscope-9wdg3q2c)). A signup run puts the customer's plan in the customer's folder; order runs read it and add to the customer's total with [`UpdateData`](ds:block?id=updatedata-zc9hugjv), which never loses a concurrent write; each run counts itself in its workflow's scope and in the global scope. It runs first on the in-memory `DataStore`, then on sqlstore's `DataStore` over one SQLite file with two clients, as two processes would hold it, so ten orders split across both add up exactly.

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
