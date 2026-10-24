<!-- Thanks for contributing to ubgo/workflow! -->

## What & why

<!-- What does this change and why? Link the issue it closes. -->
Closes #

## Type of change

- [ ] Bug fix
- [ ] New feature
- [ ] Refactor / internal
- [ ] Docs
- [ ] Other:

## Checklist

- [ ] `task check` is green. That is the gofmt check, vet and race tests of every module, the Go minimum per module, and the docs drift check.
- [ ] A driver change was also run against its real backend: `task sqlstore:test:pg`, `task sqlstore:test:mysql` or `task hatchet:test:live`, as it applies.
- [ ] New logic has a test that would fail without it; a change to a driver's semantics has a case in the `workflowtest` conformance suite.
- [ ] The core still imports only the standard library (`deps_test.go`).
- [ ] No capability declared that the conformance suite does not prove; a field a driver cannot honour returns `ErrUnsupported` naming it, and the refusal is listed in the driver's `doc.go`.
- [ ] Time comes from the injected clock, schedules from an explicit time zone.
- [ ] Every new closed-set value is a named constant; every new export has a doc comment saying why it exists and what stays true.
- [ ] No suppressed diagnostics. No `//nolint`, no skipped test, no loosened assertion.
- [ ] README tables, the driver's `doc.go` and CHANGELOG updated for anything user-facing.
- [ ] No unrelated changes or formatting churn.
