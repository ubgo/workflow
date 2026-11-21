# sql-options

The SQL driver's everyday options, one at a time, on SQLite files in a temporary directory.

- **Pick a database.** `openDB` returns the pool and the sqlb dialect for SQLite, Postgres or MySQL; the program runs the SQLite branch.
- **Workers and polling.** A client with four workers polls once an hour. A run written with `EnqueueTx` inside the app's transaction starts at once because the program calls `Client.Wake` after the commit.
- **Database errors.** A classifier for a proxy's own error codes falls back to `DefaultErrorClassifier`, which sorts a real SQLite duplicate-key error; `WithTxAttempts` raises the retries of a lost lock race.
- **Other options.** `WithClock`, `WithLogger`, `WithMiddleware` (a middleware that prints each step it wraps) and `WithJitterSource`; `Client.DB` reads the runs table directly.
- **Shared stores on their own.** `NewDataStore` and `NewJournalStore` over a database no driver client runs on, each with its own `Migrate`.

```sh
cd examples
go run ./sql-options
```

```text
run enqueued in a transaction ran before the next poll: true
sqlite duplicate: {UniqueViolation:true Retryable:false Unavailable:false}
proxy BUSY:      {UniqueViolation:false Retryable:true Unavailable:false}
proxy DOWN:      {UniqueViolation:false Retryable:false Unavailable:true}
other:           {UniqueViolation:false Retryable:false Unavailable:false}
middleware: send-receipt/send attempt 1
succeeded runs in wf_runs: 3
data store without a client: cursor 42 (found true, version 1)
journal store without a client: fetch "page-1" (found true)
```

## The code

- `pick`: `openDB`, the dialect for each engine.
- `workers`: `WithWorkers`, `WithPollInterval`.
- `wake`: `EnqueueTx` in the app's transaction, then `Client.Wake`.
- `classifier`, `classify`: an `ErrorClassifier` returning `ErrorClass`, `DefaultErrorClassifier`, `WithErrorClassifier`, `WithTxAttempts`.
- `options`: `WithClock`, `WithLogger`, `WithMiddleware`, `WithJitterSource`.
- `db`: `Client.DB`.
- `stores`: `sqlstore.NewDataStore` and `sqlstore.NewJournalStore` with `WithTablePrefix` and their own `Migrate`.

`main_test.go` runs the program and checks every line. The guide [docs/drivers/sqlstore.md](../../docs/drivers/sqlstore.md) copies its code from the regions marked `snippet:`.
