# Namespaces

Staging and production share one Postgres server, or two products share one Redis, and their runs, schedules, locks and pauses must never meet. Every driver isolates a namespace through its own configuration, so the library has no separate namespace type: the client is the namespace. Two clients configured apart see none of each other's runs, schedules, signals, pauses or unique locks; two clients configured alike share all of them, which is how a fleet of workers is built.

## Before you start

- Nothing to install; each driver below already has the option.
- The example is [`examples/namespaces`](../../examples/namespaces): a staging and a production client of the SQL driver on one SQLite file, and the same on Redis when `REDIS_URL` is set.

## One namespace per client

| Driver | How to isolate | What is shared otherwise |
|---|---|---|
| memory | One `memory.New()` per namespace | Nothing: every client is its own process-local store |
| sqlstore | `sqlstore.WithTablePrefix("staging_")` per namespace on one database; on Postgres also `sqlstore.WithSchema("staging")` for grants, backups or a schema per tenant | Two clients with the same prefix (and schema) share every run and schedule, which is how a fleet of workers is built |
| Redis | `redis.WithPrefix("staging")` | Two clients with the same prefix share every run and schedule |
| Hatchet | A Hatchet namespace on the SDK client (`HATCHET_CLIENT_NAMESPACE` or the client option) and the same value in `hatchet.Config.Namespace` | Workflows are scoped by the namespace the engine knows them by |

The stores beside a driver take the same options: the durable unique, debounce, journal, bucket and batch stores of sqlstore and Redis live under the client's prefix (or the prefix given to their standalone constructors), so a unique lock in staging never blocks production.

## Step 1 — Give each SQL namespace a table prefix

`sqlstore.WithTablePrefix(p)` replaces the default prefix `wf_` on every table the client creates and reads. Clients with the same prefix are one namespace:

<!-- snippet: examples/namespaces/main.go#sql -->
```go
staging, err := start(ctx, db, sqlstore.WithTablePrefix(stagingPrefix))
if err != nil {
	return err
}
prod, err := start(ctx, db, sqlstore.WithTablePrefix(prodPrefix))
if err != nil {
	return err
}
// A second staging process: same prefix, same namespace.
stagingWorker, err := start(ctx, db, sqlstore.WithTablePrefix(stagingPrefix))
if err != nil {
	return err
}
```

`start` builds the client with `sqlstore.New`, runs `Migrate`, registers the workflow and calls `Start`. Each prefix gets its own full set of tables and its own migration ledger (`<prefix>migrations`), so each namespace migrates on its own and can run a different library version during a rollout.

On Postgres, `sqlstore.WithSchema("staging")` puts the tables in a schema (`"staging"."wf_runs"`) without changing the pool's `search_path`. The schema must exist already (`Migrate` does not create it). Use it when the namespaces need separate grants or backups; combine it with a prefix or not. See [Table names and schemas](migrations.md#table-names-and-schemas).

## Step 2 — Check that the namespaces do not meet

Enqueue a run and create a schedule in staging, then ask the second staging client and the production client about them:

<!-- snippet: examples/namespaces/main.go#isolated -->
```go
ref, err := staging.Enqueue(ctx, workflow.Job{Workflow: "report", RunAt: time.Now().Add(delay)})
if err != nil {
	return err
}
_, err = staging.Schedule(ctx, workflow.Schedule{
	Name: "nightly", Workflow: "report", Cron: "0 2 * * *", Location: time.UTC,
})
if err != nil {
	return err
}
for _, c := range []struct {
	name   string
	client *sqlstore.Client
}{{"staging worker", stagingWorker}, {"prod", prod}} {
	st, err := c.client.Status(ctx, ref)
	if errors.Is(err, workflow.ErrNotFound) {
		st = "not found"
	} else if err != nil {
		return err
	}
	scheds, err := c.client.Schedules(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "%s: staging run %s, %d schedule(s)\n", c.name, st, len(scheds))
}
```

The staging worker sees the run and the schedule; production answers `workflow.ErrNotFound` for the run and lists no schedule. The same holds for signals, pauses, unique locks and the admin API built on either client.

## Step 3 — Do the same on Redis

`redis.WithPrefix(p)` replaces the default key prefix `wf`; keys look like `<prefix>:run:<id>`:

<!-- snippet: examples/namespaces/main.go#redis -->
```go
// newClient builds, registers and starts one Redis client under prefix.
newClient := func(prefix string) (*redis.Client, error) {
	c, err := redis.New(rdb,
		redis.WithPrefix(prefix), // keys look like <prefix>:run:<id>
		redis.WithPollInterval(redisPoll),
	)
	if err != nil {
		return nil, err
	}
	if err := c.Register(report); err != nil {
		return nil, err
	}
	return c, c.Start(ctx)
}
staging, err := newClient("staging-" + run)
if err != nil {
	return err
}
prod, err := newClient("prod-" + run)
if err != nil {
	return err
}
```

The example adds a unique suffix so repeated runs on a shared test server never meet; an app uses a fixed name per environment. On Hatchet, set the namespace on the SDK client and the same value in `hatchet.Config.Namespace`, as the [Hatchet driver](../drivers/hatchet.md#create-the-client) shows: the driver scopes idempotency keys, rate limits and its lookups of a workflow by the name the engine knows each workflow by (the namespace plus the workflow name), so a mismatch breaks them.

## Try it

```sh
cd examples
go run ./namespaces
```

```text
tables: 42 staging_*, 42 prod_*
staging worker: staging run scheduled, 1 schedule(s)
prod: staging run not found, 0 schedule(s)
bad prefix: invalid true
schema on SQLite: unsupported true
REDIS_URL is not set: the Redis part is skipped
```

With a Redis running, `REDIS_URL=redis://127.0.0.1:36379/0 go run ./namespaces` prints the same lines and then `redis prod: staging run not found true` in place of the last one. The two refused settings are the first two pitfalls below:

<!-- snippet: examples/namespaces/main.go#refused -->
```go
_, err := sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithTablePrefix("Staging-"))
fmt.Fprintf(w, "bad prefix: invalid %t\n", errors.Is(err, workflow.ErrInvalid))
_, err = sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithSchema("staging"))
fmt.Fprintf(w, "schema on SQLite: unsupported %t\n", errors.Is(err, workflow.ErrUnsupported))
```

## Pitfalls

- A table prefix must match `^[a-z][a-z0-9_]{0,30}$`: lower case, starting with a letter. `sqlstore.New` refuses anything else with `workflow.ErrInvalid`, so `Staging-` or `prod.` fail at build time, not at the first query. A schema name follows the same pattern.
- `WithSchema` is Postgres only. `sqlstore.New` refuses it on SQLite (no schemas) and on MySQL (the schema is the database: point the pool at another database instead) with `workflow.ErrUnsupported`.
- A namespace is not a tenant. Inside one namespace, `Job.Tenant` (with `CapTenant`) scopes runs, queries and quotas per customer; use a namespace for environments or products that must never share anything, and [tenants](../jobs/tenants.md) for customers of one product.
- The migration ledger of sqlstore is per table prefix (`<prefix>migrations`), so each namespace migrates on its own. Running `Migrate` for staging never touches production's tables.
- Every client of a namespace must use the same prefix (and schema). A worker deployed with a typo in its prefix silently becomes a namespace of its own: it migrates fresh tables and never sees the work. Read the prefix from one config value shared by every process.
- On Redis, every client of one server with the default prefix shares one namespace. Give each app and each test its own prefix.

## Related

- [Migrations](migrations.md#table-names-and-schemas): table names, schemas and the ledger.
- [Tenants](../jobs/tenants.md): customers inside one namespace.
- [SQL driver](../drivers/sqlstore.md), [Redis driver](../drivers/redis.md), [Hatchet driver](../drivers/hatchet.md).
- [Deploy workers](deploy.md): several workers of one namespace.
