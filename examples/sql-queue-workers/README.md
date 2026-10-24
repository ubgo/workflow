# sql-queue-workers

Two worker clients over one SQLite file, each serving its own queue, the way an app deploys separate worker processes per queue on the SQL driver. Everything that coordinates them is a row in the database: a queue paused from one client holds the other, and a failed run is listed, retried from its failed step and forgotten from whichever client the operator happens to use.

```sh
cd examples
go run ./sql-queue-workers
```

```text
email while mail is paused: queued
email after resume: succeeded, on queue mail
failed: build-report at step render on queue default
after retry: gather ran 1 time(s), render 2
after delete: not found
mail worker drained: true
```

The SQLite file lives in a temporary directory removed at exit.

## The code

- `send-email` runs on the `mail` queue, `build-report` on the default one. The `mail` client is built with `sqlstore.WithQueues(workflow.QueueConfig{Name: "mail", Workers: 1})`, the `reports` client with the default queue only, so each claims only its own queue's steps.
- `Pause(workflow.PauseQueue("mail"))` on the reports client holds the email on the mail client; `Resume` releases it.
- `Runs` filters failed runs by label, `Retry` re-runs the failed run from `render` (the `gather` step keeps its output and does not run again), and `Delete` forgets it.
- `Drain` stops the mail client gracefully; `Done` closes once it has stopped.

`main_test.go` runs the same program and checks every line it prints. The guide [docs/drivers/sqlstore.md](../../docs/drivers/sqlstore.md) copies its code from the regions marked `snippet:`.
