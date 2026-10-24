# Contributing to ubgo/workflow

## The gate

```sh
task check
```

That runs the gofmt check, `go vet` and race tests of every module in `go.work`, then `gomin` (each module vetted under the Go version its `go.mod` declares: core and sqlstore 1.24, otel 1.25, hatchet 1.26) and `docs:drift` (the docsync check, which needs the `ds` CLI). Everything must be green before a change is proposed. `task check` is the real gate: the GitHub Actions workflow in `.github/workflows/test.yml` is `workflow_dispatch` only, because Actions minutes are metered on this repository, so nothing runs automatically on a push or pull request.

The backend suites skip without their server, so a change to a driver is also run against the real backend:

| Command | What it runs |
|---|---|
| `task sqlstore:test:pg` | sqlstore race tests on SQLite and Postgres (database `ubgo_workflow_test`, each test in its own table prefix, dropped after) |
| `task sqlstore:test:mysql` | sqlstore race tests on SQLite and MySQL 8 (a throwaway server from `sqlstore/compose.yml` on 127.0.0.1:33072, removed after the run, pass or fail) |
| `task hatchet:test:live` | The conformance suite plus the run-finished, batch, batch coordinator and crash live tests against a running Hatchet engine |

## The rules a change is reviewed against

**The core imports only the standard library.** The root module may not import a third-party package or any other ubgo package; `deps_test.go` is the check. Logging, tracing and metrics reach the core through middleware and stdlib interfaces (`log/slog`, `context.Context`, `time`). Anything that needs a dependency is a separate module (`hatchet`, `otel`, `sqlstore`).

**Capabilities are honest.** A driver declares only what it really does, and every declared capability is proven by the shared `workflowtest` conformance suite against the real backend. A field a driver cannot honour returns an error wrapping `workflow.ErrUnsupported` that names it; nothing is silently dropped or emulated badly. A new refusal is listed in that driver's `doc.go`.

**Scheduling code meets a correctness bar.** Time is injected through `clock.Clock`, never read from `time.Now` in logic. Every schedule is evaluated in an explicit time zone and tested across DST gaps and overlaps. "Runs once across N workers" and "survives a crash mid-step" are claims proven by tests that run several workers against a real database, never by prose.

**Nothing runs until `Start(ctx)`, and everything stops with that ctx.** No package-level scheduler, no global registry that changes behaviour, no cache files.

**No project-specific decisions in the library.** Queue names, tenant layout and the choice of engine belong to the application.

**Every closed-set value is a named constant, and every exported symbol has a doc comment** that says why it exists and what stays true, not what the code does.

**Schema changes are new numbered migrations.** `sqlstore` and `sqlstore/outbox` apply numbered migrations through sqlb's `contrib/migrate` ledger; an applied migration is never edited.

## Tests

A new exported function or behaviour needs a test that pins it; new logic without one does not ship. A change to a driver's semantics needs a conformance case in `workflowtest`, so every driver is held to it.

Never suppress a diagnostic to make a gate pass. No `//nolint`, no loosened assertion, no skipped test. Fix the cause, or say why the tool is wrong and leave it failing for review.

## Docs

A change to a package, task or contract updates the places that list it: the packages and commands tables in `README.md`, the driver's `doc.go`, the `Client` doc comments in `client.go`, and `CHANGELOG.md` under Unreleased.

## Commits

Describe what changed and why. The repository history is read by people deciding whether a behaviour was deliberate.
