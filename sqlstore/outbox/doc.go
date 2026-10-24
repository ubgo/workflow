// Package outbox is the transactional outbox for any workflow.Client: an app
// writes a job inside its own database transaction, and a Relay enqueues it
// on the target client (Hatchet, memory, a middleware-wrapped client, a
// sqlstore client in another database) after the transaction commits. A
// rolled-back transaction leaves nothing to relay, so a job is enqueued if
// and only if the app's writes committed.
//
//	ob, _ := outbox.New(db, sqlb.PostgresDialect{})
//	_ = ob.Migrate(ctx)                       // explicit, versioned
//	tx, _ := db.BeginTx(ctx, nil)
//	// ... the app's own writes in tx ...
//	_, _ = ob.Write(ctx, tx, workflow.Job{Workflow: "ship", Payload: p})
//	_ = tx.Commit()
//
//	relay, _ := ob.NewRelay(hatchetClient)
//	_ = relay.Start(ctx)                      // nothing runs before this
//	relay.Notify()                            // optional: relay now, not at the next poll
//
// An app whose queue is the same SQL database does not need this package:
// sqlstore.Client.EnqueueTx writes the run itself inside the transaction.
//
// # Delivery
//
// Delivery is at least once. A relay claims a batch of due entries under a
// lease (FOR UPDATE SKIP LOCKED on Postgres and MySQL, one conditional
// UPDATE on SQLite, so any number of relays over one table never claim one
// entry at once), enqueues each in table order and deletes it once the
// target accepted it. A relay that dies after the target accepted an entry
// but before the delete leaves it to be enqueued again once the lease
// expires. Each enqueue carries an idempotency key, the job's own when it
// has one and the entry's ID otherwise, so a target that honours keys
// (CapIdempotencyKey, under a workflow policy other than IdempotencyOff)
// drops the second enqueue while the first run has not finished; a run that
// already finished runs twice. Handlers must be idempotent, as for any
// queue. NewRelay refuses a target without CapIdempotencyKey, because it
// would refuse every keyed entry; AllowDuplicates accepts one, sending only
// the job's own key, and names the trade-off: a redelivered entry then
// always runs twice.
//
// Order is table order within one relay pass; across relays, and after a
// failed entry is retried, there is no order.
//
// A failed enqueue is retried with backoff (WithBackoff, default 1s doubling
// to 5m). WithMaxAttempts moves an entry that keeps failing to the dead
// status; the default retries forever, because a failure is most often the
// target being down or a workflow not yet registered during a deploy. A
// dead entry is reported to the relay's WithOnDead hook (for an alert),
// listed by Outbox.ListDead (paged, oldest first, optionally per workflow)
// and sent again by Outbox.Requeue, which keeps its ID and so its
// idempotency key.
//
// Enqueue middleware on the target runs at relay time, after the commit,
// which is why this is the way to combine a transactional enqueue with
// middleware (unique, debounce, tracing).
//
// # Time
//
// Due times, backoff and leases come from WithClock (default clock.Real),
// and so does the poll timer. Times are stored as BIGINT Unix nanoseconds.
//
// # Schema
//
// Migrate creates the outbox table through sqlb's contrib/migrate ledger,
// under its own owner (ubgo/workflow/sqlstore/outbox) in the ledger table
// <prefix>migrations: the same ledger and migration lock sqlstore uses for
// the same prefix, but its own list of numbered migrations, because an app
// relaying to Hatchet has no sqlstore tables and must not get them. A
// database a newer release migrated is refused (ErrSchemaTooNew).
// MigrationScripts returns the same migrations as SQL text for teams that
// apply DDL with their own tool; Write and relays need only the table.
//
// WithTablePrefix names the table ("wf_" by default); on Postgres,
// WithSchema puts it in a schema of its own (the ledger then stays in the
// search_path, its rows owned as "<owner>@<schema>"). WithIDGenerator
// replaces the entry ID format ("obx_<32 hex>"); the ID is also the default
// idempotency key, so it must be unique.
package outbox
