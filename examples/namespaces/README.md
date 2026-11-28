# namespaces

Two environments on one database. A staging and a production client of the SQL driver share one SQLite file, each under its own table prefix (`staging_`, `prod_`), so each has its own 44 tables and its own migration ledger. A run and a schedule created in staging are seen by a second staging client and not at all by production. `sqlstore.New` refuses a prefix that breaks the naming rule and `WithSchema` on SQLite. With `REDIS_URL` set, the same isolation is shown on Redis with two key prefixes.

```sh
cd examples
go run ./namespaces
```

```text
tables: 44 staging_*, 44 prod_*
staging worker: staging run scheduled, 1 schedule(s)
prod: staging run not found, 0 schedule(s)
bad prefix: invalid true
schema on SQLite: unsupported true
REDIS_URL is not set: the Redis part is skipped
```

With `REDIS_URL=redis://127.0.0.1:36379/0` the last line is `redis prod: staging run not found true`.

## The code

- `sql`: `sqlstore.WithTablePrefix`, two namespaces and a second client of one of them.
- `isolated`: `Status` and `Schedules` asked from each client.
- `refused`: a prefix `New` refuses (`workflow.ErrInvalid`) and `WithSchema` on SQLite (`workflow.ErrUnsupported`).
- `redis`: `redis.WithPrefix`, one key prefix per namespace.

`main_test.go` runs the program and checks every line, with or without Redis. The guide [docs/operations/namespaces.md](../../docs/operations/namespaces.md) copies its code from the regions marked `snippet:`.
