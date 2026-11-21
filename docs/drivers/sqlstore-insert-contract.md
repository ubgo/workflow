# Enqueue from other languages (sqlstore insert contract)

A billing service in Python, a Rails app or a nightly SQL script needs to start a run that Go workers execute. With the SQL driver the queue is a few tables in the app's database, so the other program does not need a client library or an HTTP endpoint: it inserts the run with plain SQL, in its own transaction, and a Go client that registers the workflow runs it. This page is that contract: which rows to write, what to put in each column, how to wake the workers, and what never to touch.

## Before you start

- A database migrated by `Client.Migrate`, or by the statements `sqlstore.MigrationScripts` returns applied with your own tool ([Migrations](../operations/migrations.md)). The column list below is the frozen schema of migrations `001` to `009`; a later release adds new migrations and tables, never a changed column, so a writer built against this page keeps working.
- At least one Go client (`sqlstore.New(db, dialect, ...)`, started) that registers the workflow, on the same tables: the same table prefix (`WithTablePrefix`, default `wf_`) and, on Postgres, the same schema (`WithSchema`).
- SQLite, Postgres and MySQL 8 follow the same contract. It is proven by `TestForeignInsert` in `sqlstore/enginetest/foreign_insert_test.go` on all three, which inserts with exactly the statements below.
- The runnable writer is [`examples/insert-contract`](../../examples/insert-contract): it writes the rows with `database/sql` and no `sqlstore` call, the way a Python or Ruby program would with its own driver, and a Go worker runs them. [A writer, step by step](#a-writer-step-by-step) walks through it.

## The rows to write

One run is three rows, written in one transaction: `<prefix>runs`, `<prefix>run_ext` and `<prefix>run_info`, plus one `<prefix>run_labels` row per label when the run has labels. Table names carry the prefix (`wf_runs` by default) and, on Postgres under `WithSchema`, the schema (`"jobs"."wf_runs"`). Each `?` is a parameter; use your driver's placeholder (`$1` on Postgres):

```sql
INSERT INTO wf_runs (id, scheduled_id, workflow, payload, labels, idem_active, priority, bucket, rate_keys,
  status, run_at, sig_floor, created_at, finished_at, fail_until, notified, notify_until, ver)
VALUES (?, ?, ?, ?, ?, ?, ?, '', '', ?, ?, (SELECT COALESCE(MAX(id), 0) FROM wf_signals), ?, 0, 0, 0, 0, 0);

INSERT INTO wf_run_ext (run_id, queue, idem_key, retry, started_at, discarded)
VALUES (?, ?, ?, NULL, 0, 0);

INSERT INTO wf_run_info (run_id, tenant, output, has_output, progress, nodes, deadline, timeout)
VALUES (?, ?, NULL, 0, NULL, '[]', 0, '');

-- one per label, only when the run has labels
INSERT INTO wf_run_labels (run_id, lkey, lval) VALUES (?, ?, ?);
```

The values, in parameter order:

| Column | Value |
|---|---|
| `runs.id`, `run_ext.run_id`, `run_info.run_id`, `run_labels.run_id` | The run ID: unique, 1 to 64 bytes. The Go side makes `run_` and 32 random hex characters (`DefaultIDGenerator`); any unique string works, such as a UUIDv7. It is the `RunRef.RunID` Go code passes to `Status`, `Cancel` and `Result`. |
| `scheduled_id` | `NULL` for a run that starts now. For a delayed run, a second unique ID (`sched_` and 32 hex characters), which is the `RunRef.ScheduledID`. |
| `workflow` | The workflow name, exactly as registered in Go. |
| `payload` | The bytes the first step receives as its input, usually UTF-8 JSON; `NULL` for none. Bytes are stored as given: no encoding is added. |
| `labels` | The run's labels as a JSON object of string values, `{}` for none. Never `NULL`. |
| `idem_active` | The idempotency key, or `NULL` for none (see [Idempotency keys](#idempotency-keys)). |
| `priority` | `Job.Priority`: higher goes first; `0` by default. |
| `status` | `pending` for a run that starts now, `scheduled` for a delayed one. |
| `run_at` | Unix nanoseconds (a 64-bit integer): now for a pending run, the start time for a scheduled one. |
| `created_at` | Unix nanoseconds now. |
| `run_ext.queue` | The run's queue, or `''` to have the first worker that admits the run fill in the workflow's queue (`Workflow.Queue`, else `default`). |
| `run_ext.idem_key` | The same key as `idem_active`, or `NULL`. |
| `run_info.tenant` | `Job.Tenant`, or `''`. |
| `run_labels.lkey`, `lval` | One row per label, key and value each cut to 191 characters. Needed only for `RunQuery.Labels` and label-prefix searches to find the run; the labels themselves are read from `runs.labels`. |

The literals in the statements are part of the contract too:

- `bucket = ''` and `rate_keys = ''` mark the run's concurrency key and rate-limit keys as not yet evaluated: the first worker that admits it evaluates them from the workflow's Go definition (`Concurrency.Key`, `RateLimits`), as it does for runs from an enqueue-only client. Never write your own values there.
- `sig_floor` is the highest signal ID when the run is written, read by the subquery: the run takes only signals sent after it (`Signal`'s look-back). Keep the subquery.
- `finished_at`, `fail_until`, `notified`, `notify_until`, `ver`, and in `run_ext`, `run_info` the zeros, `NULL`s and `'[]'` shown, are the state of a run nothing has touched yet. The columns left out (`err`, `fail_step`, `fail_input`, `fail_token`) stay `NULL`.

Times are always Unix nanoseconds in a 64-bit integer, UTC by definition, so no engine's date type or session time zone can shift them. On Postgres compute now with `(extract(epoch from clock_timestamp()) * 1000000000)::bigint`, on MySQL with `CAST(UNIX_TIMESTAMP(NOW(6)) * 1000000000 AS SIGNED)`, on SQLite with `CAST((julianday('now') - 2440587.5) * 86400000000000 AS INTEGER)`, or pass the value from your language's clock.

## Idempotency keys

A key held by an unfinished run of the workflow is unique: the unique index on `runs (workflow, idem_active)` makes a second insert with the same workflow and key fail with a unique violation, and the failed transaction writes nothing. Treat that error as "already enqueued": roll back (on Postgres the failed statement has aborted the transaction anyway) and, if you need the existing run, read it with `SELECT id, scheduled_id FROM wf_runs WHERE workflow = ? AND idem_active = ?`. A Go `Enqueue` or `EnqueueTx` with the same key returns that run, and the key is free again once the run finishes, exactly as for runs enqueued from Go. On Postgres, `ON CONFLICT (workflow, idem_active) DO NOTHING` on the `runs` insert avoids the aborted transaction; skip the other inserts when it inserted nothing.

A workflow-level idempotency policy that derives the key in Go (`Workflow.Idempotency` with a key function) does not run for a foreign insert: compute the key yourself.

## Wake the workers

Nothing has to be notified. Every started client looks for pending runs of its workflows every `WithPollInterval` (one second by default) and promotes a scheduled run once its `run_at` has passed, so a committed run starts within a poll interval.

On Postgres, clients built with `WithNotifier(pgnotify.New(db))` also listen on a channel and look at once when it fires ([Wake workers at once](sqlstore.md#wake-workers-at-once)). Send `SELECT pg_notify('ubgo_workflow_wf_wake', '')` in the same transaction: Postgres delivers it when the transaction commits, never before. The channel is `ubgo_workflow_` + the table prefix + `wake`, or `ubgo_workflow_` + schema + `_` + prefix + `wake` under `WithSchema`; when that is longer than 63 characters it is `ubgo_workflow_` followed by the first 16 bytes, in hex, of the SHA-256 of the long name. A wrong channel name only means the run waits for the next poll.

## A writer, step by step

The program below is Go only so the test suite can run it; every statement in it is plain SQL that a program in any language sends through its own driver. It runs on a SQLite file, so the placeholders are `?`.

### Step 1: start a worker that registers the workflow

The tables must exist and a started client must register the workflow before runs are inserted for it. Here the worker migrates them too; in production that is your deploy's migration step:

<!-- snippet: examples/insert-contract/main.go#worker -->
```go
worker, err := sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithPollInterval(pollInterval))
if err != nil {
	return err
}
if err := worker.Migrate(ctx); err != nil { // or apply sqlstore.MigrationScripts with your own tool
	return err
}
report := workflow.Workflow{Name: "report", Queue: "reports", Steps: []workflow.Step{{Name: "build",
	Run: func(ctx context.Context, in []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		mu.Lock()
		seen[info.RunID] = string(in)
		mu.Unlock()
		return nil, nil
	}}}}
if err := worker.Register(report); err != nil { // register before runs are inserted for it
	return err
}
```

`Queue: "reports"` is the workflow's queue. The runs below leave `run_ext.queue` empty, and the worker that admits them fills it in.

### Step 2: keep the statements

The four statements of [The rows to write](#the-rows-to-write), for the default prefix `wf_`:

<!-- snippet: examples/insert-contract/main.go#statements -->
```go
// The contract's statements for the default table prefix "wf_". Every "?"
// is a parameter; the literals are part of the contract.
const (
	insertRun = `INSERT INTO wf_runs (id, scheduled_id, workflow, payload, labels, idem_active, priority, bucket, rate_keys,
  status, run_at, sig_floor, created_at, finished_at, fail_until, notified, notify_until, ver)
VALUES (?, ?, ?, ?, ?, ?, ?, '', '', ?, ?, (SELECT COALESCE(MAX(id), 0) FROM wf_signals), ?, 0, 0, 0, 0, 0)`
	insertRunExt = `INSERT INTO wf_run_ext (run_id, queue, idem_key, retry, started_at, discarded)
VALUES (?, ?, ?, NULL, 0, 0)`
	insertRunInfo = `INSERT INTO wf_run_info (run_id, tenant, output, has_output, progress, nodes, deadline, timeout)
VALUES (?, ?, NULL, 0, NULL, '[]', 0, '')`
	insertRunLabel = `INSERT INTO wf_run_labels (run_id, lkey, lval) VALUES (?, ?, ?)`
)
```

### Step 3: describe a run and write its rows in one transaction

The other program's view of a run is a handful of values. Absent values go in as SQL `NULL`, labels as a JSON object (`{}` for none), times as Unix nanoseconds:

<!-- snippet: examples/insert-contract/main.go#job -->
```go
// foreignJob is one run as the other program describes it.
type foreignJob struct {
	ID          string // unique, 1 to 64 bytes: the RunRef.RunID Go code uses
	ScheduledID string // a second unique ID for a delayed run, else empty
	Workflow    string // exactly as registered in Go
	Payload     []byte // the first step's input, stored as given
	Labels      map[string]string
	Key         string    // idempotency key, empty for none
	Queue       string    // empty: the admitting worker fills in the workflow's queue
	Tenant      string    // empty for none
	Priority    int       // higher goes first
	RunAt       time.Time // zero: now
}
```

<!-- snippet: examples/insert-contract/main.go#insert -->
```go
// insert writes job's rows in one transaction. A held idempotency key
// fails the runs insert with a unique violation, and the rollback leaves
// nothing behind.
func insert(ctx context.Context, db *sql.DB, job foreignJob) (err error) {
	labels := []byte(`{}`) // a JSON object of string values, never NULL
	if len(job.Labels) > 0 {
		if labels, err = json.Marshal(job.Labels); err != nil {
			return err
		}
	}
	now := time.Now().UnixNano() // Unix nanoseconds: UTC by definition
	status, runAt := statusPending, now
	if !job.RunAt.IsZero() {
		status, runAt = statusScheduled, job.RunAt.UnixNano()
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, tx.Rollback())
		}
	}()
	if _, err := tx.ExecContext(ctx, insertRun, job.ID, nullable(job.ScheduledID), job.Workflow, job.Payload, string(labels),
		nullable(job.Key), job.Priority, status, runAt, now); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, insertRunExt, job.ID, job.Queue, nullable(job.Key)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, insertRunInfo, job.ID, job.Tenant); err != nil {
		return err
	}
	for k, v := range job.Labels { // only for label searches; the labels column is what runs read
		if _, err := tx.ExecContext(ctx, insertRunLabel, job.ID, k, v); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// nullable maps "" to SQL NULL, as the contract wants for absent values.
func nullable(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}
```

### Step 4: insert a run due now and a delayed one

A `pending` run starts within a poll interval; a `scheduled` one once its `run_at` has passed. The delayed run needs a second unique ID for `scheduled_id`:

<!-- snippet: examples/insert-contract/main.go#write -->
```go
now := foreignJob{ID: "run_billing_0001", Workflow: "report", Payload: []byte(`{"month":"2026-11"}`),
	Labels: map[string]string{"source": "billing"}, Tenant: "acme"}
later := foreignJob{ID: "run_billing_0002", ScheduledID: "sched_billing_0002", Workflow: "report",
	Payload: []byte(`{"month":"2026-12"}`), RunAt: time.Now().Add(delay)}
for _, job := range []foreignJob{now, later} {
	if err := insert(ctx, db, job); err != nil {
		return err
	}
}
```

The worker runs both. The label row lets `RunQuery.Labels` find the first, and the run reports the queue the worker filled in and the tenant from `run_info`.

### Step 5: handle a held idempotency key

A second insert with a key an unfinished run holds fails on the unique index; the transaction rolls back and writes nothing. A Go `Enqueue` with the same key returns the run the other program inserted:

<!-- snippet: examples/insert-contract/main.go#key -->
```go
keyed := foreignJob{ID: "run_billing_0003", ScheduledID: "sched_billing_0003", Workflow: "report",
	Key: "report-2027-01", RunAt: time.Now().Add(holdFor)}
if err := insert(ctx, db, keyed); err != nil {
	return err
}
dup := keyed
dup.ID, dup.ScheduledID = "run_billing_0004", "sched_billing_0004"
err = insert(ctx, db, dup)
// A unique violation means "already enqueued"; the transaction wrote nothing.
fmt.Fprintf(w, "second insert of key %s: unique violation %v\n", keyed.Key, sqlstore.DefaultErrorClassifier(err).UniqueViolation)
held, err := worker.Enqueue(ctx, workflow.Job{Workflow: "report", IdempotencyKey: keyed.Key})
if err != nil {
	return err
}
fmt.Fprintf(w, "Go Enqueue with the same key returns the inserted run: %v\n", held.ScheduledID == workflow.ScheduledID(keyed.ScheduledID))
```

## Try it

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

The first line is the contract working end to end: a run no Go code enqueued ran with its payload. The last two are the idempotency key held across both kinds of writer.

## What never to write

The insert above is the whole contract. Everything else belongs to the Go clients, and a foreign write there can break runs other workers are executing:

- Never write `<prefix>steps`: a worker creates the step rows when it admits the run.
- Never insert a run in any status other than `pending` or `scheduled`, and never update or delete an existing run. Cancel, retry, delete and reschedule go through a Go client (`Cancel`, `Retry`, `Delete`, `Reschedule`) or the `admin` HTTP API.
- Never write the lease, failure or notification columns (`lease_*`, `fail_*`, `notified`, `notify_until`), the `locks`, `starts`, `schedules`, `ticks`, `signals` or `consumed` tables, or the `migrations` ledger.
- Never write `bucket` or `rate_keys` (keep `''`) or `run_ext.retry` (keep `NULL`; a per-job retry override is a Go `Job.Retry`).
- Never change the schema: a column added by hand breaks nothing today but the next migration may collide with it.

## Pitfalls

- The checks Go's `Enqueue` runs (`workflow.ValidateJob`, `WithPayloadLimit`, capability checks, enqueue middleware such as unique jobs or debounce) do not run for a foreign insert. A payload the workflow cannot decode fails the step, as it would for a bad payload from Go.
- A run of a workflow no client registers stays pending forever: nothing executes it, and nothing reports it. Register the workflow before inserting runs for it.
- Write all rows in one transaction. A `runs` row without its `run_ext` row reads as the `default` queue (a client serving only named queues never claims it), and one without its `run_info` row never gets a run timeout (`RunTimeout` lives there).
- On MySQL use `utf8mb4` and keep the tables' `utf8mb4_bin` collation: workflow names and keys compare byte for byte.
- Transaction poolers are fine for the insert; only `pgnotify`'s `LISTEN` on the Go side needs a direct connection.
- Write both IDs for a `scheduled` run and `NULL` for `scheduled_id` on a `pending` one, as the table above says: the `scheduled_id` is the `RunRef.ScheduledID` Go code hands to `Status` and `Cancel` before the run starts, and `TestForeignInsert` reads the delayed run's status by it.
- On Postgres a unique violation aborts the transaction: roll back before anything else, or use `ON CONFLICT (workflow, idem_active) DO NOTHING` on the `runs` insert and skip the other three inserts when it inserted nothing ([Idempotency keys](#idempotency-keys)).

## Related

- [The SQL driver](sqlstore.md): the clients that run what you insert.
- [`examples/insert-contract`](../../examples/insert-contract): the writer and the worker on this page, run by the test suite.
- [Transactional enqueue](../reliability/transactional-enqueue.md): the same insert from Go, inside the app's transaction.
- [Database change triggers](../reliability/change-triggers.md): jobs from row changes, written by a trigger into the outbox.
- [Migrations](../operations/migrations.md): `MigrationScripts` for the exact DDL of every table above.
