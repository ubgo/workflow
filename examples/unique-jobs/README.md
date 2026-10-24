# unique-jobs

Refuse a second job for the same key while the first holds a lock: for a fixed time with `middleware.Unique` and `MemoryUnique` on a fake clock, until the run finishes with `UniqueUntilFinished` on the memory driver, by the workflow and its arguments with `UniqueByArgs`, and across two processes with `sqlstore.UniqueStore` (two connection pools on one SQLite file in a temporary directory). It also prunes expired locks with `PruneExpired`.

```sh
cd examples
go run ./unique-jobs
```

```text
-- Unique for a time
first: accepted
second, same hour: refused (ErrDuplicate)
third, an hour later: accepted
-- Unique until the run finishes
first: accepted
second, while the first runs: refused (ErrDuplicate)
export ran
run finished: succeeded key label: u-7
third, after it finished: accepted
export ran
-- Unique by workflow and arguments
a-1, ann: accepted
a-1, bob, keys reordered: refused (ErrDuplicate)
a-2: accepted
key: args:report:7dee8b416858...
-- A store shared by every process
process A: accepted
process B, same user: refused (ErrDuplicate)
expired locks pruned: 1
process B, after expiry: accepted
```

Nothing to install: SQLite runs in-process. Against the shared local stack, open the same store on Postgres with `driverpgx.Open(dsn)` and `sqlb.PostgresDialect{}` in two processes; the lock table and its semantics are the same.

The guide [Unique jobs](../../docs/reliability/unique-jobs.md) explains each step. `main_test.go` runs the same program and checks every line above.
