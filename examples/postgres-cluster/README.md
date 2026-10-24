# postgres-cluster

Three workers of one queue over one database, the way several processes of a deployed app share it. Thirty jobs are enqueued across the workers and each runs exactly once; an hourly cron tick that all three see due fires once; then every worker shuts down cleanly and `Enqueue` reports `ErrNotStarted`.

The three clients live in one process so the program can count what they did; in production each is its own process running the same start-up. They share one fake scheduling clock so the program can make the cron tick due on demand; production leaves `WithClock` out.

```sh
cd examples
go run ./postgres-cluster
```

```text
30 jobs, 30 ran exactly once
hourly-report fired at 10:00
fired once across 3 workers
stopped; Enqueue now fails with workflow: client not started
```

Without `PG_DSN` it runs on a SQLite file in a temporary directory. To run it on Postgres, set `PG_DSN` to a database you may create tables in. On the local stack (the host's Postgres, the database the driver's own tests use):

```sh
createdb ubgo_workflow_test 2>/dev/null || true
PG_DSN="postgres://$USER@localhost:5432/ubgo_workflow_test?sslmode=disable" go run ./postgres-cluster
```

The program uses a fresh table prefix (`cluster_<random>_`) and drops those tables at the end, so a run leaves nothing behind.

## The code

- `workflows`: a plain job and a workflow with `Cron: []string{"0 * * * *"}`.
- `workers`: `New`, `Migrate`, `Register`, `Start` for each worker; `Migrate` from every process at once is safe.
- `shutdown`: cancel the `Start` context, then `Stop` each client.

`main_test.go` runs the program on SQLite, and on Postgres when `PG_DSN` is set (skipped otherwise). The guide [docs/operations/deploy.md](../../docs/operations/deploy.md) copies its code from the regions marked `snippet:`.
