# Namespaces

Staging and production share one Postgres server, or two products share one Redis, and their runs, schedules, locks and pauses must never meet. Every driver isolates a namespace through its own configuration, so the library has no separate namespace type: the client is the namespace. Two clients configured apart see none of each other's runs, schedules, signals, pauses or unique locks.

## Before you start

- Nothing to install; each driver below already has the option.

## One namespace per client

| Driver | How to isolate | What is shared otherwise |
|---|---|---|
| memory | One `memory.New()` per namespace | Nothing: every client is its own process-local store |
| sqlstore | `sqlstore.WithTablePrefix("staging_")` per namespace on one database; on Postgres also `sqlstore.WithSchema("staging")` for grants, backups or a schema per tenant | Two clients with the same prefix (and schema) share every run and schedule, which is how a fleet of workers is built |
| Redis | `redis.WithPrefix("staging")` | Two clients with the same prefix share every run and schedule |
| Hatchet | A Hatchet namespace on the SDK client (`HATCHET_CLIENT_NAMESPACE` or the client option) and the same value in `hatchet.Config.Namespace` | Workflows are scoped by the namespace the engine knows them by |

The stores beside a driver take the same options: the durable unique, debounce, journal, bucket and batch stores of sqlstore and Redis live under the client's prefix (or the prefix given to their standalone constructors), so a unique lock in staging never blocks production.

## Pitfalls

- A namespace is not a tenant. Inside one namespace, `Job.Tenant` (with `CapTenant`) scopes runs, queries and quotas per customer; use a namespace for environments or products that must never share anything, and tenants for customers of one product.
- The migration ledger of sqlstore is per table prefix (`<prefix>migrations`), so each namespace migrates on its own.
