# Install

You will have the core module in your Go module, know which of the separate modules (`sqlstore`, `redis`, `hatchet`, `otel`, `sentry`, `admin`, `cli`) you need, and know the Go version each one asks for.

## Before you start

- Go 1.24 or later for the core module, `sqlstore`, `redis`, `admin` and `cli`, Go 1.25 for `otel` and `sentry`, Go 1.26 for `hatchet` (each module's `go.mod` declares it, and `task gomin` vets every module under that version).
- The library is pre-release and **nothing is tagged yet**. The API can still change; pin the pseudo-version you install.

## Step 1 — Add the core module

The core module holds the contract (`workflow.Client`, the specs, capabilities, errors and plugins) and every package that needs only the standard library: `clock`, `cron` (with the opt-in `cron/tzdata`), `memory`, `middleware`, `batch`, `failover`, `events`, `hybrid` and `workflowtest`. It has no `replace` directive, so it resolves as a pseudo-version from `main`:

```sh
go get github.com/ubgo/workflow@latest
```

Check: `go list -m github.com/ubgo/workflow` prints a `v0.0.0-…` pseudo-version, and `go mod graph | grep '^github.com/ubgo/workflow@'` prints nothing: the core module requires no other module (a `deps_test.go` in the repository fails the build if its `go.mod` ever gains a `require` line).

## Pick your modules

Packages that bring a third-party dependency are separate modules, so an app pays for the Hatchet SDK, OpenTelemetry, go-redis, sentry-go or a SQL driver only when it imports them. The `admin` module needs only the standard library and the core, but is separate so an app that never exposes an admin surface never builds it; the `cli` module needs only the standard library and talks to the admin API over HTTP.

| You want | Import | Module |
|---|---|---|
| The contract, specs, `Define`, capabilities, errors, plugins | `github.com/ubgo/workflow` | core |
| A driver for tests, local runs and work you can lose on exit | `github.com/ubgo/workflow/memory` | core |
| Logging, recovery, timeouts, codecs and encryption, unique jobs, debounce, routing, claim check, circuit breaker | `github.com/ubgo/workflow/middleware` | core |
| Batches with Then, Catch and Finally callbacks | `github.com/ubgo/workflow/batch` | core |
| A second backend to fail over to | `github.com/ubgo/workflow/failover` | core |
| A fake client and assertions for app tests; the driver conformance suite | `github.com/ubgo/workflow/workflowtest` | core |
| Injected time, the cron parser, an embedded zone database | `github.com/ubgo/workflow/clock`, `github.com/ubgo/workflow/cron`, `github.com/ubgo/workflow/cron/tzdata` | core |
| Publish and subscribe, signed inbound webhooks | `github.com/ubgo/workflow/events` | core |
| Crons on one client, runs on another | `github.com/ubgo/workflow/hybrid` | core |
| Durable runs on SQLite, Postgres or MySQL 8 | `github.com/ubgo/workflow/sqlstore` | `sqlstore` (needs `github.com/ubgo/sqlb` and a database driver) |
| The transactional outbox in front of any client | `github.com/ubgo/workflow/sqlstore/outbox` | `sqlstore` |
| Durable runs on Redis | `github.com/ubgo/workflow/redis` | `redis` (needs go-redis v9) |
| Hatchet as the engine | `github.com/ubgo/workflow/hatchet` | `hatchet` (needs the Hatchet Go SDK, `github.com/hatchet-dev/hatchet` `sdks/go`) |
| OpenTelemetry spans and metrics | `github.com/ubgo/workflow/otel` | `otel` |
| Failures reported to Sentry | `github.com/ubgo/workflow/sentry` | `sentry` (needs sentry-go) |
| Run and queue operations, a closed-by-default admin JSON API and web UI, queue alerts | `github.com/ubgo/workflow/admin` | `admin` (standard library and the core only) |
| The `workflow` command line client for the admin API | `github.com/ubgo/workflow/cli`, command `github.com/ubgo/workflow/cli/cmd/workflow` | `cli` (standard library only) |

The separate modules resolve this repository's core through a local `replace` until the core is tagged, and a `replace` in a dependency is ignored by your build. Until the first tag, build them from a clone of the repository, where the committed `go.work` wires every module together:

```sh
git clone https://github.com/ubgo/workflow
cd workflow/examples
go run ./quickstart
```

The [examples module](../../examples/) is wired the same way: it imports the separate modules and builds against the working tree.

For `sqlstore`, add a database driver through sqlb's driver modules, which open the pool with the settings each engine needs:

| Engine | Driver module | Dialect |
|---|---|---|
| SQLite | `github.com/ubgo/sqlb/contrib/driver-sqlite` (pure Go, no cgo) | `sqlb.SQLiteDialect{}` |
| Postgres | `github.com/ubgo/sqlb/contrib/driver-pgx` | `sqlb.PostgresDialect{}` |
| MySQL 8 | `github.com/ubgo/sqlb/contrib/driver-mysql` | `sqlb.MySQLDialect{}` |

[Choose a driver](../choosing/drivers.md) says which backend fits which app, and the [SQL driver guide](../drivers/sqlstore.md#pick-a-database) covers each database.

## Go versions

| Module | Go | Why |
|---|---|---|
| `github.com/ubgo/workflow` (core) | 1.24 | the floor the library promises |
| `github.com/ubgo/workflow/sqlstore` | 1.24 | sqlb and its contrib modules ask for no more |
| `github.com/ubgo/workflow/otel` | 1.25 | the OpenTelemetry Go modules it requires (v1.46) declare it |
| `github.com/ubgo/workflow/redis` | 1.24 | go-redis v9 asks for no more |
| `github.com/ubgo/workflow/sentry` | 1.25 | sentry-go declares it |
| `github.com/ubgo/workflow/admin` | 1.24 | it needs only the standard library and the core |
| `github.com/ubgo/workflow/cli` | 1.24 | it needs only the standard library |
| `github.com/ubgo/workflow/hatchet` | 1.26 | the Hatchet Go SDK it requires (v0.107.1) declares it |

The minimums are proven, not claimed: `task gomin` vets each module under the Go version its `go.mod` declares, and `task check` runs it.

## Pitfalls

- `go get github.com/ubgo/workflow/sqlstore@latest` (or any other separate module) before the core is tagged fails to resolve `github.com/ubgo/workflow v0.0.0`: build from a clone with `go.work`, as above.
- The memory driver keeps everything in the process. It declares every capability except `CapDurable`, so nothing refuses it; a queued job is gone when the process exits. Pick a durable driver for work you cannot lose ([Choose a driver](../choosing/drivers.md)).
- `sqlstore` never creates its tables at `Start`. Run `Client.Migrate` from your deploy first ([migrations](../operations/migrations.md#run-the-migrations)).

## Next

- [Quickstart](quickstart.md): one workflow on the memory driver.
- [Your first workflow](first-workflow.md): steps, retries, joins and failure handling.
- [Choosing](../choosing/README.md): which driver and which method for each job.
