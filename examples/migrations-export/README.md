# migrations-export

The two ways the SQL driver's schema reaches a database. `Migrate` applies numbered migrations under a ledger and the engine's lock, and is safe to call twice. `MigrationScripts` hands the same SQL to a tool of your own: the program writes one `.sql` file per migration, the way a team on goose, Atlas, Flyway or a DBA review would, then applies the scripts with plain `Exec` calls and starts a client that never ran `Migrate`.

It also migrates a `UniqueStore` on its own (only the lock table), shows that `Migrate` and `Start` refuse a database a newer release migrated (`ErrSchemaTooNew`), and shows `WithSchema`: schema-qualified DDL on Postgres, refused on SQLite.

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

Everything runs on SQLite files in a temporary directory; the Postgres scripts are only generated, never applied, so no server is needed. The "newer release" is simulated by inserting a `099_from_the_future` row into the ledger.

`main_test.go` runs the program and checks every line. The guide [docs/operations/migrations.md](../../docs/operations/migrations.md) copies its code from the regions marked `snippet:`.
