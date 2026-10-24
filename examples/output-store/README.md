# output-store

Keep each step attempt's captured output in the database: `sqlstore.OutputStore` as the `middleware.CaptureOutput` sink, read back per run, and wired into the admin API as its output source, so every process and the admin run page see the same history. It runs on a SQLite file in a temporary directory, so nothing needs to be installed.

```sh
cd examples
go run ./output-store
```

```text
run succeeded
fetch attempt 1 error="feed timed out" logged fetching feed: true
fetch attempt 2 error="" logged fetching feed: true
admin sees 2 attempts
```

## The code

<!-- snippet: examples/output-store/main.go#store -->
```go
// A client only to build the store from; the worker below gets its
// middleware. Client.Migrate creates <prefix>outputs too.
setup, err := sqlstore.New(db, sqlb.SQLiteDialect{})
if err != nil {
	return err
}
if err := setup.Migrate(ctx); err != nil {
	return err
}
outputs := setup.OutputStore(sqlstore.OutputLimits{}) // 20 attempts per run, 64 KiB each
c, err := sqlstore.New(db, sqlb.SQLiteDialect{},
	sqlstore.WithPollInterval(20*time.Millisecond),
	sqlstore.WithMiddleware(middleware.CaptureOutput(outputs)),
)
```

<!-- snippet: examples/output-store/main.go#read -->
```go
attempts, err := outputs.RunOutput(ctx, ref.RunID) // oldest first
if err != nil {
	return err
}
for _, a := range attempts {
	fmt.Fprintf(out, "%s attempt %d error=%q logged fetching feed: %t\n", a.Step, a.Attempt, a.Error, strings.Contains(a.Text, "fetching feed"))
}
```

<!-- snippet: examples/output-store/main.go#admin -->
```go
// sqlstore does not import admin: map its rows to admin's, one for one.
src := admin.OutputSourceFunc(func(ctx context.Context, id workflow.RunID) ([]admin.AttemptOutput, error) {
	rows, err := outputs.RunOutput(ctx, id)
	if err != nil {
		return nil, err
	}
	out := make([]admin.AttemptOutput, len(rows))
	for i, r := range rows {
		out[i] = admin.AttemptOutput(r) // the same fields, types and order
	}
	return out, nil
})
adm := admin.New(c, admin.WithOutputs(src))
```

`main_test.go` runs the program and compares its whole output with the block above.

## Guides

- [Task output › Keep output in the database](../../docs/schedules/output.md#keep-output-in-the-database)
