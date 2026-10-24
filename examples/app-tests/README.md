# app-tests

App code written against `workflow.Client` and `clock.Clock` (`main.go`), and its tests (`main_test.go`): `workflowtest.Fake` with its assertions and matchers, `Finish` to free an idempotency key, `FakeLike`, `FakeCaps` and `FakeKeyExpr` to refuse what production refuses, `NewRecorder` around the memory driver, and a fake clock driven with `BlockUntil`, `Advance`, `Set` and `Pending`. `go run` runs the app on a Fake and prints what it recorded.

```sh
cd examples
go run ./app-tests
```

```text
enqueued welcome-email key="welcome:ada@example.com" at now
enqueued reminder key="reminder:ada@example.com" at 2026-01-02T09:00:00Z
signalled email-confirmed/ada@example.com
cancelled: 1 run(s)
scheduled digest "0 9 * * MON" team=core
reminder status: cancelled
```

Run the tests:

```sh
cd examples
go test ./app-tests/ -v
```

The guides [fake](../../docs/testing/fake.md), [recorder](../../docs/testing/recorder.md) and [fake clock](../../docs/testing/fake-clock.md) walk through the tests.

The guide [Test app code with the Fake](../../docs/testing/fake.md) explains each step. `TestRun` in `main_test.go` runs the same program and checks every line above.
