# Contributing to ubgo/workflow

## The gate

```sh
task check
```

That runs the gofmt check, `bin:check` (no compiled program in the tree), `go vet` and race tests of every module in `go.work`, then `gomin` (each module vetted under the Go version its `go.mod` declares: core and sqlstore 1.24, otel 1.25, hatchet 1.26) and `docs:drift` (the docsync check, which needs the `ds` CLI). Everything must be green before a change is proposed. `task check` is the real gate: the GitHub Actions workflow in `.github/workflows/test.yml` is `workflow_dispatch` only, because Actions minutes are metered on this repository, so nothing runs automatically on a push or pull request.

The backend suites skip without their server; [Tasks while you work](#tasks-while-you-work) lists the tasks that start one.

## Before you start

- Go 1.26 (the `go.work` version; `task gomin` fetches the older toolchains it needs by itself), [Task](https://taskfile.dev) v3, and Docker for the suites that start a throwaway database, Redis or Hatchet.
- Nothing for locking. The heavy tasks (`test`, `vet`, `check`, `gomin`, `examples:test`, `redis:test`, `sqlstore:test`, `recorder:test` and the live suites) wrap their command in `testlock` when it is on your `PATH`: a machine-wide lock the maintainers use so two suites never run at once on one machine. It is not part of this repository; without it the commands run directly (the Taskfiles' `LOCK` variable).
- The `ds` CLI for `docs:drift` (`go install github.com/ubgo/docsync/cli/cmd/ds@latest`).

## Tasks while you work

`task check` is the gate before a change is proposed. While you work, run the task that covers what you touched; `task --list` prints every task with its description.

### Step 1: format and compile

| Command | What it does | When |
|---|---|---|
| `task fmt` | `gofmt -w .`: formats every Go file in place | After editing Go code |
| `task fmt:check` | Fails and lists every Go file that is not gofmt clean (`gofmt -l .`); changes nothing | Before proposing; `check` runs it first |
| `task bin:check` | Fails and names every file git tracks or would track that is a compiled program (ELF, Mach-O, PE or WebAssembly, by its first bytes): `go build` in a folder with a main package writes one named after the folder. `.gitignore` lists every such folder; a new main folder gets its line there | Before proposing; `check` runs it second |
| `task build` | `go build` of every package of every module in `go.work` | After a change to an exported API, to see every module that uses it still compiles |
| `task vet` | `go vet` over every module in `go.work` | After any Go change; `check` runs it |

A clean tree prints only the command Task runs:

```sh
task fmt:check
```

```text
task: [fmt:check] test -z "$(gofmt -l .)" || (gofmt -l . && exit 1)
```

### Step 2: test what you changed

| Command | What it runs | When |
|---|---|---|
| `go test -race ./<package>/...` | One package's tests, from the module that holds it | While you work on that package |
| `task test` | Race tests of every module in `go.work`, the examples module included; the live suites skip without their server | Before proposing; `check` runs it |
| `task examples:test` | `go vet` and `go test` of the examples module alone: every program under `examples/` and the doc checkers | After changing an example or a doc checker |
| `task sqlstore:test` | The SQL driver's race tests on SQLite (the `sqlstore`, `enginetest`, `pgnotify` and `pgxtx` modules); the Postgres and MySQL cases skip without their DSN | After a change to `sqlstore` |
| `task redis:test` | The Redis driver's race tests against a throwaway Redis 8 from the root `compose.yml` (profile `redis`, 127.0.0.1:36379), each test under its own key prefix, the container removed after the run; with `WFTEST_REDIS_URL` set it uses that Redis and starts nothing | After a change to `redis` |
| `task recorder:test` | Race tests of `recorder`, `recorder/sqlrecorder` and `recorder/e2e` on SQLite, the memory driver and the SQL driver on SQLite; the live cases skip | After a change to the recorder |
| `task recorder:e2e:all` | Every live recorder suite, one service at a time: `recorder:e2e:pg`, `recorder:e2e:mysql`, `recorder:e2e:redis`, `recorder:e2e:hatchet`, each against a throwaway container removed after the run | After a change to the recorder's stores or to a driver the recorder reads |
| `task gomin` | `go vet` of every module, test files included, under the Go version its `go.mod` declares, so a stated minimum is proven | After using a newer language or library feature; `check` runs it |
| `task deps:check` | Fails when a released module's `go.mod` is not tidy without `go.work`, or requires a module none of its library packages import | After adding or removing a dependency; `check` runs it |

The backend suites need their server, so a change to a driver is also run against the real backend:

| Command | What it runs |
|---|---|
| `task sqlstore:test:pg` | sqlstore race tests on SQLite and Postgres (database `ubgo_workflow_test`, each test in its own table prefix, dropped after) |
| `task sqlstore:test:mysql` | sqlstore race tests on SQLite and MySQL 8 (a throwaway server from `sqlstore/compose.yml` on 127.0.0.1:33072, removed after the run, pass or fail) |
| `task hatchet:test:live` | The conformance suite plus the run-finished, batch, batch coordinator and crash live tests against a running Hatchet engine |

### Step 3: check the docs

The guides copy their Go code from the example programs, so a snippet cannot drift from code that compiles and runs. An example marks a region with `// snippet:<name>` and `// snippet:end`; a guide puts `<!-- snippet: examples/<name>/main.go#<region> -->` on the line before the code fence.

| Command | What it does | When |
|---|---|---|
| `task docs:snippets` | Fails and names every marked block in `README.md`, `docs/` and `examples/` that differs from its source region, or whose file or region is missing | After changing an example or a guide; `check` runs it |
| `task docs:snippets:sync` | Rewrites every stale marked block from its source region; still fails on a missing file or region | After changing an example region on purpose. Read the diff: it rewrites every guide that copies the region |
| `task docs:wrap` | Fails on hard-wrapped Markdown: every paragraph, bullet and table row is one physical line | After editing Markdown |
| `task docs:links` | Fails when a relative link or `#anchor` in `README.md`, `docs/` or `examples/` does not resolve | After renaming a heading or moving a page |
| `task docs:words` | Fails when a word from a private list appears in the public Markdown or Go files; skipped when no list is set | Run by the maintainers |
| `task docs:drift` | Fails when code a doc sentence cites changed since the sentence was checked (needs `ds`) | Before proposing; `check` runs it |
| `task docs:def -- <file>#<Symbol> [stable\|api] [--label <label>]` | Gives the declaration a docsync id (a `//ds:def` directive, which godoc hides) and prints it; idempotent, one writer at a time under `flock` (needs `ds`) | When a sentence you write states what library code does: link the name it already uses to that id with a `ds:block` link, as the guides do; `stable` for behaviour, defaults and lists, `api` for a type's or function's shape |

To sync only the files you touched, run the checker on them directly:

```sh
go run ./examples/internal/doccheck/snippets -root . -sync docs/reliability/failover.md examples/failover-observer
```

## The gate in detail

`task check` runs, in order: `fmt:check`, `bin:check`, `vet`, `test`, `gomin`, `deps:check`, `docs:wrap`, `docs:links`, `docs:snippets`, `docs:words`, `docs:drift`. It stops at the first failure.

## The rules a change is reviewed against

**The core imports only the standard library.** The root module may not import a third-party package or any other ubgo package; `deps_test.go` is the check ([`TestGuard_CoreModuleIsStdlibOnly`](ds:block?id=testguard-coremoduleisstdlibonly-pje3bjqj)). Logging, tracing and metrics reach the core through middleware and stdlib interfaces (`log/slog`, `context.Context`, `time`). Anything that needs a dependency is a separate module (`hatchet`, `otel`, `sqlstore`).

**Capabilities are honest.** A driver declares only what it really does, and every declared capability is proven by the shared `workflowtest` conformance suite ([`workflowtest.Run`](ds:block?id=workflowtest-run-2dxkjk9h)) against the real backend. A field a driver cannot honour returns an error wrapping [`workflow.ErrUnsupported`](ds:block?id=errunsupported-wacxcsch) that names it ([`UnsupportedError`](ds:block?id=unsupportederror-46jkqrxt)); nothing is silently dropped or emulated badly. A new refusal is listed in that driver's `doc.go`.

**Scheduling code meets a correctness bar.** Time is injected through `clock.Clock`, never read from `time.Now` in logic. Every schedule is evaluated in an explicit time zone and tested across DST gaps and overlaps. "Runs once across N workers" and "survives a crash mid-step" are claims proven by tests that run several workers against a real database, never by prose.

**Nothing runs until [`Start(ctx)`](ds:block?id=client-start-qges69hj), and everything stops with that ctx.** No package-level scheduler, no global registry that changes behaviour, no cache files.

**No project-specific decisions in the library.** Queue names, tenant layout and the choice of engine belong to the application.

**Every closed-set value is a named constant, and every exported symbol has a doc comment** that says why it exists and what stays true, not what the code does.

**Schema changes are new numbered migrations.** `sqlstore` and `sqlstore/outbox` apply numbered migrations through sqlb's `contrib/migrate` ledger ([`migrations`](ds:block?id=sql-store-migrations-sysbsdpd)); an applied migration is never edited, since the ledger stores each one's checksum.

## Tests

A new exported function or behaviour needs a test that pins it; new logic without one does not ship. A change to a driver's semantics needs a conformance case in `workflowtest`, so every driver is held to it.

Never suppress a diagnostic to make a gate pass. No `//nolint`, no loosened assertion, no skipped test. Fix the cause, or say why the tool is wrong and leave it failing for review.

## Docs

A change to a package, task or contract updates the places that list it: the packages and commands tables in `README.md`, the driver's `doc.go`, the [`Client`](ds:block?id=client-w9gg8dzm) doc comments in `client.go`, and `CHANGELOG.md` under Unreleased.

## Commits

Describe what changed and why. The repository history is read by people deciding whether a behaviour was deliberate.

## Pitfalls

- `task test` and `task check` take minutes (the SQLite engine suite alone takes about five minutes under `-race`); run the package you touched while you work and the gate once before proposing.
- `go build` inside a folder with a main package leaves a program named after the folder; build with `go build -o /dev/null` or `go vet` instead. `.gitignore` lists every main folder, and `bin:check` fails on a compiled program that git would add.
- `task fmt` rewrites files in place; `fmt:check` only reports. A gate failure on `fmt:check` is fixed by `task fmt`, not by hand.
- `docs:snippets:sync` rewrites every block that copies a changed region, in any guide. Renaming or changing the meaning of a region another guide copies changes that guide too: grep `docs/` for `snippet: examples/<name>/` first.
- The live tasks start and remove Docker containers on fixed local ports (Redis 36379, MySQL 33072, Postgres 35432); a server already on that port makes the start fail.
- Without `testlock` on `PATH` two heavy tasks started at once on one machine run side by side and compete for CPU and the fixed container ports; start them one after the other.

## Related

- [README](README.md): what the library is and the packages and commands it ships.
- [Feature index](docs/reference/features.md): every feature with its guide and example, the repository tasks included.
- [Examples](examples/README.md): every runnable program the guides copy from.
- [Security policy](SECURITY.md): how to report a vulnerability.
