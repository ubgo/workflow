# Change triggers

A CRM must hear about every change to the `accounts` table. Some changes come from the Go app, but others come from an admin tool in another language, a data migration or a support engineer at a SQL prompt, and none of those call `outbox.Write`. A row trigger catches them all: [`outbox.ChangeTrigger`](ds:block?id=outbox-changetrigger-jvb8ehcp) generates the SQL for a trigger that writes an outbox entry in the same statement that changed the row, so the entry commits or rolls back with the change, and the outbox relay enqueues the job afterwards on any client.

## Before you start

- The outbox (`github.com/ubgo/workflow/sqlstore/outbox`) migrated in the app's database, and a relay to the client that runs the workflow ([Outbox](outbox.md)).
- SQLite, Postgres 13 or later (for `gen_random_uuid`, which [the Postgres function](ds:block?id=outbox-outbox-pgtrigger-49mug8p7) calls), or MySQL 8. On MySQL, creating a trigger needs the `TRIGGER` privilege, and with binary logging on also `SUPER` or `log_bin_trust_function_creators`.
- The runnable program for this page is [examples/change-triggers](../../examples/change-triggers/).

## Database change triggers

Describe the trigger with a [`TriggerSpec`](ds:block?id=outbox-triggerspec-u6e88rcx): the table, the changes it fires on (`OnInsert`, `OnUpdate`, `OnDelete`), the workflow, the columns copied into the job's payload, and optionally a payload field naming the change. [`ChangeTrigger(dialect, spec, opts...)`](ds:block?id=outbox-changetrigger-jvb8ehcp) returns the SQL; apply `Create` in one of your migrations, after the outbox table exists:

<!-- snippet: examples/change-triggers/main.go#trigger -->
```go
trg, err := outbox.ChangeTrigger(sqlb.SQLiteDialect{}, outbox.TriggerSpec{
	Table:    "accounts",
	Ops:      []outbox.ChangeOp{outbox.OnInsert, outbox.OnUpdate, outbox.OnDelete},
	Workflow: "crm.sync",
	Columns:  []string{"id", "email", "plan"}, // never password_hash
	OpField:  "op",
})
if err != nil {
	return err
}
for _, stmt := range trg.Create { // in a real app: one of your migrations
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		return err
	}
}
```

From then on every change writes an entry, whoever makes it. Plain SQL is enough:

<!-- snippet: examples/change-triggers/main.go#changes -->
```go
for _, stmt := range []string{
	"INSERT INTO accounts (id, email, plan, password_hash) VALUES ('a1', 'ana@example.com', 'free', 'x')",
	"UPDATE accounts SET plan = 'pro' WHERE id = 'a1'",
	"DELETE FROM accounts WHERE id = 'a1'",
} {
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		return err
	}
}
```

Each entry is the job `outbox.Write` would write for `workflow.Job{Workflow: "crm.sync", Payload: ...}`: the payload is a JSON object with one field per column (the new row's values on insert and update, the old row's on delete) and, with [`OpField`](ds:block?id=outbox-triggerspec-u6e88rcx), the change as `insert`, `update` or `delete`. The relay delivers them like any other entry:

<!-- snippet: examples/change-triggers/main.go#relay -->
```go
relay, err := ob.NewRelay(mc)
if err != nil {
	return err
}
n, err := relay.Flush(ctx) // a running app calls relay.Start(ctx) instead
if err != nil {
	return err
}
```

What each engine gets:

| Engine | `Create` |
|---|---|
| Postgres | One plpgsql function (`<Name>_fn`, built with `json_build_object`) and one `AFTER ... FOR EACH ROW` trigger for the chosen changes (`AFTER INSERT OR UPDATE OR DELETE` with all three), both from [`pgTrigger`](ds:block?id=outbox-outbox-pgtrigger-49mug8p7). `TableSchema` names the app table's schema. |
| MySQL | One trigger per change (`<Name>_ins`, `_upd`, `_del`) whose body is a single `INSERT` with `JSON_OBJECT` ([`perOpTrigger`](ds:block?id=outbox-outbox-peroptrigger-f3ecak24) with [`mysqlExprs`](ds:block?id=outbox-mysqlexprs-68vfqywh)), so no `DELIMITER` is needed. |
| SQLite | One trigger per change with `json_object` ([`perOpTrigger`](ds:block?id=outbox-outbox-peroptrigger-f3ecak24) with [`sqliteExprs`](ds:block?id=outbox-sqliteexprs-t23t49x7)). |

Every [`Create`](ds:block?id=outbox-triggersql-j7czkbyr) is safe to run again: each `CREATE TRIGGER` is preceded by `DROP TRIGGER IF EXISTS` and the Postgres function is `CREATE OR REPLACE`, so re-running a migration replaces the trigger instead of adding a second. `Drop` removes everything `Create` installed.

The rest of the spec:

- [`KeyColumn`](ds:block?id=outbox-triggerspec-u6e88rcx) makes a column's value the job's idempotency key, so a target honouring keys runs one job per row while that row's run is unfinished; a second change of the row before then is dropped by the target. Without it every change is its own job, keyed by the entry ID.
- `Priority` is the job's priority.
- `Name` is the trigger's name, by default the outbox's table prefix ([`wf_`](ds:block?id=outbox-defaulttableprefix-rjhz5epr) unless set), the table and `_outbox` (`wf_accounts_outbox`, filled in by [`checkTrigger`](ds:block?id=outbox-outbox-checktrigger-54jagn5j)). Set it to put two triggers on one table.
- The outbox options `WithTablePrefix` and `WithSchema` say which outbox table the entries go to, as for `outbox.New`.

## Try it

```sh
cd examples
go run ./change-triggers
```

```text
-- install the trigger
statements: 6
-- change rows with plain SQL
-- relay to the workers
relayed: 3
synced {"id":"a1","email":"ana@example.com","plan":"free","op":"insert"} true
synced {"id":"a1","email":"ana@example.com","plan":"pro","op":"update"} true
synced {"id":"a1","email":"ana@example.com","plan":"pro","op":"delete"} true
```

## Pitfalls

- Name the columns. A payload carries only `Columns`, so a password hash or a token never leaves the table unless you list it. 1 to 50 fields (columns plus `OpField`, [`maxTriggerColumns`](ds:block?id=outbox-maxtriggercolumns-w9exfgnc)), since Postgres's `json_build_object` takes 100 arguments.
- Every name is checked before it reaches the SQL: the table, schema, columns, `OpField`, `KeyColumn` and `Name` must be plain identifiers ([`identPattern`](ds:block?id=outbox-identpattern-xtcg2ch7): `[A-Za-z_][A-Za-z0-9_]*`, at most 63 characters, `Name` at most 59 to leave room for its suffix under [`maxTriggerName`](ds:block?id=outbox-maxtriggername-zjszg8gu)) and are quoted; the workflow name may hold only letters, digits and `_ . : / @ -`, 1 to 191 characters ([`workflowLiteral`](ds:block?id=outbox-workflowliteral-6kg8txr4)). Anything else is refused with `workflow.ErrInvalid`; `TableSchema` on MySQL or SQLite with `workflow.ErrUnsupported` ([`TestChangeTriggerRefusals`](ds:block?id=outbox-testchangetriggerrefusals-28frsyw5)).
- An `UPDATE` fires once per row the statement updates, even when no listed column changed: the trigger is `FOR EACH ROW` with no condition ([`perOpTrigger`](ds:block?id=outbox-outbox-peroptrigger-f3ecak24), [`pgTrigger`](ds:block?id=outbox-outbox-pgtrigger-49mug8p7)). A bulk update of a million rows writes a million entries in the same transaction.
- A binary column cannot go into SQLite's `json_object`: the app's statement fails at change time. Leave such columns out.
- MySQL's `JSON_OBJECT` may order the payload's fields differently from `Columns`; decode the payload as JSON, never compare its text.
- The trigger writes the entry with the database's clock and an ID made by the database (`obx_` and 32 random hex digits, from [`sqliteExprs`](ds:block?id=outbox-sqliteexprs-t23t49x7), [`mysqlExprs`](ds:block?id=outbox-mysqlexprs-68vfqywh) or [`pgTrigger`](ds:block?id=outbox-outbox-pgtrigger-49mug8p7)). Entries carry no labels and no `RunAt` ([`triggerInsert`](ds:block?id=outbox-outbox-triggerinsert-qacdmzs5)).
- Delivery is at least once, as for every outbox entry: the relay deletes an entry only after the target accepted it, so a failed delete sends it again ([`Relay.deliver`](ds:block?id=outbox-relay-deliver-g2n48m2x)). A handler must be idempotent.

## Related

- [Outbox](outbox.md): the table, the relay, retries and dead entries.
- [Transactional enqueue](transactional-enqueue.md): enqueue from Go inside the app's transaction when the queue is this database.
- [Enqueue from other languages](../drivers/sqlstore-insert-contract.md): a program in another language inserting a run directly.
