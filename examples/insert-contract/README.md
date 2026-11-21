# insert-contract

SQL driver runs enqueued the way a program in another language would: plain SQL in its own transaction, no `sqlstore` client, exactly as [docs/drivers/sqlstore-insert-contract.md](../../docs/drivers/sqlstore-insert-contract.md) specifies. A Go worker that registers the workflow runs them:

- a run due now, with a label, a tenant, and its queue left empty for the admitting worker to fill in from `Workflow.Queue`;
- a delayed run (`status` `scheduled`, a `run_at` 300 ms ahead), which the worker starts once it is due;
- a keyed run: a second insert with the same idempotency key fails on the unique index and writes nothing, and a Go `Enqueue` with that key returns the inserted run.

It runs on a SQLite file in a temporary directory. The statements use SQLite's `?` placeholders; Postgres takes `$1`, `$2`, ... in the same places.

```sh
cd examples
go run ./insert-contract
```

```text
run_billing_0001 succeeded; its step saw {"month":"2026-11"}
label source=billing finds run_billing_0001: queue reports (filled in at admission), tenant acme
run_billing_0002 (scheduled) succeeded after its run_at
second insert of key report-2027-01: unique violation true
Go Enqueue with the same key returns the inserted run: true
```

## The code

- `statements`: the four statements of the contract.
- `job`, `insert`: one run's rows written in one transaction, with `NULL` for absent values.
- `worker`: the Go client that migrates the tables and registers the workflow.
- `write`: a run due now and a delayed run.
- `key`: a held idempotency key refusing a second insert, and `Enqueue` returning the held run.

`main_test.go` runs the program and checks every line. `sqlstore/enginetest/foreign_insert_test.go` proves the same statements on SQLite, Postgres and MySQL.
