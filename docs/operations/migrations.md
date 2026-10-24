# Migrations

The SQL driver's schema is a list of numbered migrations. You apply them in one of two ways: `Client.Migrate`, which records each one in a ledger table under the engine's migration lock, or `sqlstore.MigrationScripts`, which hands the same SQL to your own tool (goose, Atlas, Flyway, Liquibase, a DBA review). Either way, nothing is applied behind your back: `Start` never migrates.

## Before you start

- A [SQL driver](../drivers/sqlstore.md) client, or a [UniqueStore or DebounceStore](#each-stores-migration-list) on its own.
- For `WithSchema`, a Postgres schema that already exists: `Migrate` does not create it.

## Run the migrations

Call `Migrate` at deploy time, before `Start`. It is safe to run from every process at once: the ledger applies each migration once, serialised by the engine's own lock (an advisory lock on Postgres, a named lock on MySQL, a lock table on SQLite). A second call is a no-op.

<!-- snippet: examples/migrations-export/main.go#migrate -->
```go
c, err := sqlstore.New(db, sqlb.SQLiteDialect{})
if err != nil {
	return err
}
// Run it at deploy time, before Start, from as many processes as you
// like: the ledger applies each migration once, under the engine's lock.
if err := c.Migrate(ctx); err != nil {
	return err
}
if err := c.Migrate(ctx); err != nil { // a second call is a no-op
	return err
}
```

The ledger is a table named `<prefix>migrations` (`wf_migrations` by default), one row per applied migration, keyed by owner and name. After `Migrate` on an empty database it holds:

```text
ubgo/workflow/sqlstore 001_initial
ubgo/workflow/sqlstore 002_schedule_misfire
ubgo/workflow/sqlstore 003_queues_admin
ubgo/workflow/sqlstore 004_run_states_chain
ubgo/workflow/sqlstore 005_run_labels
ubgo/workflow/sqlstore 006_schedule_history
ubgo/workflow/sqlstore/debounce 001_debounce
ubgo/workflow/sqlstore/output 001_output
ubgo/workflow/sqlstore/unique 001_unique
```

`sqlstore.MigrationOwner` (`ubgo/workflow/sqlstore`) owns the queue's own list, so an app that keeps its own migrations in a ledger table of the same name never collides with these. A released migration is never edited: the ledger stores a checksum, and an edited script is refused on every database that applied it. A schema change is a new migration appended to the list. There are no down migrations.

The deploy order for a release that adds a migration (as `002_schedule_misfire` did for the misfire table, `003_queues_admin` for queues, retry overrides, pauses and schedule windows, `004_run_states_chain` for job chains and the discarded status, `005_run_labels` for the label index, `006_schedule_history` for schedule overlap, last fire and per-attempt errors, `001_debounce` for the debounce table, and `001_output` for captured output):

1. Run `Migrate` with the new code (a release step, an init container, or the first process to start).
2. Then start the new workers. `Start` checks that every table it needs is readable and fails with "table ... is not readable (run Migrate, or apply MigrationScripts, first)" otherwise.
3. Workers of the old release that are already running are not checked again: the version check happens only in `Migrate` and `Start`. One that restarts after step 1 is refused, as the next section explains, so finish the rollout rather than restarting old workers.

## Each store's migration list

The stores that apps use without the queue have their own lists, under their own owners, in the same ledger table and under the same lock:

| Store | Owner constant | Migrations | Applied by |
|---|---|---|---|
| The queue | `sqlstore.MigrationOwner` | `001_initial`, `002_schedule_misfire`, `003_queues_admin`, `004_run_states_chain`, `005_run_labels`, `006_schedule_history` | `Client.Migrate` |
| `UniqueStore` (durable unique jobs) | `sqlstore.UniqueMigrationOwner` | `001_unique` | `Client.Migrate` or `UniqueStore.Migrate` |
| `DebounceStore` (durable debounce) | `sqlstore.DebounceMigrationOwner` | `001_debounce` | `Client.Migrate` or `DebounceStore.Migrate` |
| `OutputStore` (captured step output) | `sqlstore.OutputMigrationOwner` (`ubgo/workflow/sqlstore/output`) | `001_output` | `Client.Migrate` or `OutputStore.Migrate` |
| The transactional outbox | `outbox.MigrationOwner` (`ubgo/workflow/sqlstore/outbox`) | `001_outbox` | `Outbox.Migrate` only |

`Client.Migrate` applies the first four. A store built on its own migrates only its own table, which is how an app running on Hatchet gets durable unique jobs from its database:

<!-- snippet: examples/migrations-export/main.go#unique -->
```go
locks, err := sqlstore.NewUniqueStore(db, sqlb.SQLiteDialect{})
if err != nil {
	return err
}
if err := locks.Migrate(ctx); err != nil { // its own list: only the lock table
	return err
}
```

The ledger then holds one row, `ubgo/workflow/sqlstore/unique 001_unique`. The outbox's list is separate from `Client.Migrate`; call `Outbox.Migrate` ([Outbox](../reliability/outbox.md#write-jobs-inside-your-transaction)).

## Rolling back the code

A database that a newer release migrated is refused, not overwritten: older code could write rows the new schema does not expect. `Migrate` and `Start` both return an error matching `sqlstore.ErrSchemaTooNew`; it is a `*sqlstore.SchemaTooNewError` whose `Unknown` names the migrations this code does not know. Nothing is changed.

<!-- snippet: examples/migrations-export/main.go#too-new -->
```go
err = c.Migrate(ctx)
if tooNew, ok := errors.AsType[*sqlstore.SchemaTooNewError](err); ok {
	// The code is older than the schema: deploy the newer release again
	// rather than run this one. Nothing was changed.
	fmt.Fprintln(out, "refused, unknown migrations:", tooNew.Unknown)
}
```

The check is per owner: a store's migration only counts against code that knows that store's owner. Another store's migration is named with its owner's last path element, such as `unique/002_x`. So a rollback is a roll forward: fix the newer release and deploy it again.

## Ship the SQL through your own tool

When DDL goes through goose, Atlas, Flyway, Liquibase or a DBA review, or the app's database user has no DDL rights, take the SQL from `MigrationScripts(dialect, opts...)`. It returns every migration `Client.Migrate` applies, in order, as `MigrationScript` values: `Owner`, `Name` and the exact `Statements`, with table names resolved for the `WithTablePrefix` and `WithSchema` options you pass (others are ignored). Pass the same table options the app gives `New`.

<!-- snippet: examples/migrations-export/main.go#export -->
```go
scripts, err := sqlstore.MigrationScripts(sqlb.PostgresDialect{},
	sqlstore.WithTablePrefix("jobs_"), // the same table options the app's New gets
)
if err != nil {
	return err
}
if err := os.MkdirAll(dir, 0o755); err != nil {
	return err
}
for i, m := range scripts {
	// One file per migration, in order. Record (Owner, Name) in your tool
	// so a later release's new migrations are the only ones it applies.
	name := fmt.Sprintf("%03d_%s_%s.sql", i+1, strings.ReplaceAll(m.Owner, "/", "_"), m.Name)
	body := "-- owner: " + m.Owner + "\n-- name: " + m.Name + "\n" + strings.Join(m.Statements, ";\n\n") + ";\n"
	if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
		return err
	}
	fmt.Fprintln(out, "wrote", name)
}
```

The text is frozen per release exactly as `Migrate`'s is, and every statement is safe to run twice (`IF NOT EXISTS` forms). `Start` checks the tables, not the ledger, so a schema applied this way starts without `Migrate`:

<!-- snippet: examples/migrations-export/main.go#apply -->
```go
scripts, err := sqlstore.MigrationScripts(sqlb.SQLiteDialect{})
if err != nil {
	return err
}
for _, m := range scripts {
	for _, stmt := range m.Statements {
		if _, err := db.ExecContext(ctx, stmt); err != nil { // each is safe to run twice
			return fmt.Errorf("%s %s: %w", m.Owner, m.Name, err)
		}
	}
}
// Start checks that the tables are readable, not the ledger, so a client
// starts on a schema applied this way without calling Migrate.
c, err := sqlstore.New(db, sqlb.SQLiteDialect{})
if err != nil {
	return err
}
if err := c.Register(workflow.Workflow{Name: "noop", Steps: []workflow.Step{{Name: "only", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}}); err != nil {
	return err
}
if err := c.Start(ctx); err != nil {
	return err
}
c.Stop()
```

`Migrate` is still safe on such a database later: it re-runs each script (a no-op) and records it. A database that holds only one store needs only that store's scripts: filter by `Owner`.

## Table names and schemas

`WithTablePrefix(p)` starts every table name (default `wf_`), so two independent queues can share one database. It must match `^[a-z][a-z0-9_]{0,30}$`; `New` refuses anything else.

`WithSchema(name)` puts every table in a Postgres schema of its own (`"jobs"."wf_runs"`), for apps that keep queue tables apart for grants, backup or ownership, or one schema per tenant, without changing the pool's `search_path`. The schema must exist. The ledger stays in the connection's `search_path`, named `<prefix>migrations` as always, and its rows for this store are owned as `<owner>@<schema>`, so several schemas share one ledger table and keep separate histories. `New` refuses `WithSchema` on SQLite, which has no schemas, and on MySQL, whose schema is the database: point the pool at that database instead.

<!-- snippet: examples/migrations-export/main.go#schema -->
```go
scripts, err := sqlstore.MigrationScripts(sqlb.PostgresDialect{},
	sqlstore.WithTablePrefix("wf_"),
	sqlstore.WithSchema("jobs"), // the schema must exist; Migrate does not create it
)
if err != nil {
	return err
}
fmt.Fprintln(out, "postgres:", firstLine(scripts[0].Statements[0]))
fmt.Fprintln(out, "ledger owner:", scripts[0].Owner)

_, err = sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithSchema("jobs"))
fmt.Fprintln(out, "sqlite:", err)
```

## Try it

```sh
cd examples
go run ./migrations-export
```

```text
ledger after Migrate:
  ubgo/workflow/sqlstore 001_initial
  ubgo/workflow/sqlstore 002_schedule_misfire
  ubgo/workflow/sqlstore 003_queues_admin
  ubgo/workflow/sqlstore 004_run_states_chain
  ubgo/workflow/sqlstore 005_run_labels
  ubgo/workflow/sqlstore 006_schedule_history
  ubgo/workflow/sqlstore 007_leader_archive
  ubgo/workflow/sqlstore 008_run_info_nodes
  ubgo/workflow/sqlstore 009_schedule_tenant
  ubgo/workflow/sqlstore/bucket 001_buckets
  ubgo/workflow/sqlstore/debounce 001_debounce
  ubgo/workflow/sqlstore/journal 001_journal
  ubgo/workflow/sqlstore/output 001_output
  ubgo/workflow/sqlstore/unique 001_unique
  ubgo/workflow/sqlstore/unique 002_unique_holder
wrote 001_ubgo_workflow_sqlstore_001_initial.sql
wrote 002_ubgo_workflow_sqlstore_002_schedule_misfire.sql
wrote 003_ubgo_workflow_sqlstore_003_queues_admin.sql
wrote 004_ubgo_workflow_sqlstore_004_run_states_chain.sql
wrote 005_ubgo_workflow_sqlstore_005_run_labels.sql
wrote 006_ubgo_workflow_sqlstore_006_schedule_history.sql
wrote 007_ubgo_workflow_sqlstore_007_leader_archive.sql
wrote 008_ubgo_workflow_sqlstore_008_run_info_nodes.sql
wrote 009_ubgo_workflow_sqlstore_009_schedule_tenant.sql
wrote 010_ubgo_workflow_sqlstore_unique_001_unique.sql
wrote 011_ubgo_workflow_sqlstore_unique_002_unique_holder.sql
wrote 012_ubgo_workflow_sqlstore_debounce_001_debounce.sql
wrote 013_ubgo_workflow_sqlstore_output_001_output.sql
wrote 014_ubgo_workflow_sqlstore_journal_001_journal.sql
wrote 015_ubgo_workflow_sqlstore_bucket_001_buckets.sql
started on a schema applied without Migrate
ledger after UniqueStore.Migrate:
  ubgo/workflow/sqlstore/unique 001_unique
  ubgo/workflow/sqlstore/unique 002_unique_holder
refused, unknown migrations: [099_from_the_future]
Start refused too: sqlstore: the database schema is newer than this code
postgres: CREATE TABLE IF NOT EXISTS "jobs"."wf_runs"
ledger owner: ubgo/workflow/sqlstore@jobs
sqlite: workflow: WithSchema on sqlite unsupported by this driver: only Postgres has schemas inside a database; on MySQL point the pool at the database
```

The program stands in for a newer release by inserting a `099_from_the_future` row into the ledger.

## Pitfalls

- `Start` never migrates. Forgetting `Migrate` (or the new scripts) after an upgrade fails `Start` on the missing table.
- Never edit an applied migration's SQL in your own tool: a later `Migrate` compares checksums and refuses the database.
- With your own tool, record each `(Owner, Name)` there; the next release adds new migrations to `MigrationScripts`, and only those should run.
- Under `WithSchema`, the ledger is not in that schema; grants for the app user must cover the `search_path` schema's `<prefix>migrations` table too.
- Restarting a worker of the old release after the new `Migrate` fails with `ErrSchemaTooNew`.
- Migration `005_run_labels` creates the label index but cannot fill it in SQL, since the labels are JSON decoded in Go. `Migrate` runs the backfill (`Client.IndexLabels`) after the scripts; with your own tool, call `IndexLabels` once after applying 005, or label searches miss the older runs ([Search by label](../jobs/manage-runs.md#the-label-index-on-the-sql-driver)).

## Related

- [The SQL driver](../drivers/sqlstore.md)
- [Deploy workers](deploy.md)
- [Unique jobs](../reliability/unique-jobs.md#a-store-shared-by-every-process), [Debounce](../reliability/debounce.md#a-store-shared-by-every-process), [Outbox](../reliability/outbox.md)
- Example: [migrations-export](../../examples/migrations-export/)
