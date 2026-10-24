# Quickstart

One workflow on the in-memory driver, in about thirty lines: you register a workflow whose single step takes a typed payload, start the workers, enqueue a job and see the step run. A one-step workflow is a plain background job; the same code runs on SQLite, Postgres or Hatchet by changing the constructor.

## Before you start

- Go 1.24 or later and the core module ([Install](install.md)). The memory driver needs no database and no server.

## Register a workflow

A `workflow.Workflow` is a name plus steps. `workflow.Define` turns a typed function into a step handler: it decodes the JSON payload into the function's input type and encodes its result, and an input that does not decode fails the step for good (it is marked `workflow.Permanent`, since retrying the same bytes cannot succeed).

<!-- snippet: examples/quickstart/main.go#register -->
```go
c := memory.New()
err := c.Register(workflow.Workflow{
	Name: "greet",
	Steps: []workflow.Step{{
		Name: "say",
		Run: workflow.Define(func(ctx context.Context, in greet) (string, error) {
			done <- "hello " + in.Name
			return "ok", nil
		}),
	}},
})
if err != nil {
	return err
}
```

`Register` validates the workflow on the spot (a name, at least one step, unique step names) and returns an error wrapping `workflow.ErrInvalid` naming what is wrong. `greet` is `struct{ Name string }`.

## Start and enqueue

`memory.New()` opened nothing and started nothing. `Start` starts the workers and returns at once; they live until the context passed to `Start` is cancelled, which is how the app stops the client. `Enqueue` returns a `workflow.RunRef` that points at the run.

<!-- snippet: examples/quickstart/main.go#start -->
```go
ctx, cancel := context.WithCancel(ctx)
defer cancel() // cancelling the Start context stops the workers
if err := c.Start(ctx); err != nil {
	return err
}
ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "greet", Payload: []byte(`{"Name":"Ada"}`)})
if err != nil {
	return err
}
```

## Try it

```sh
cd examples
go run ./quickstart
```

```text
hello Ada
```

## Run it on SQLite

Nothing above is specific to the memory driver except `memory.New()`. To keep runs across a restart, open a SQLite file through sqlb's driver (`driversqlite` is `github.com/ubgo/sqlb/contrib/driver-sqlite`):

<!-- snippet: examples/sqlite-worker/main.go#open -->
```go
db, err := driversqlite.Open(path) // a file; ":memory:" would be lost on exit
if err != nil {
	return err
}
defer db.Close()
```

Then replace `memory.New()` with `sqlstore.New(db, sqlb.SQLiteDialect{})`, and call `c.Migrate(ctx)` once before `Start`: the SQL driver never creates its tables on its own, so a deploy decides when the schema changes ([Migrations](../operations/migrations.md#run-the-migrations)). `Register`, `Start` and `Enqueue` stay exactly as they are. The SQL driver lives in its own module (`github.com/ubgo/workflow/sqlstore`), so the core never pulls in a database driver; [Install](install.md) explains how to add it while the modules are untagged, and [The SQL driver](../drivers/sqlstore.md#create-the-client) covers its options. The [sqlite-worker](../../examples/sqlite-worker/) example runs the same workflow on both drivers.

## What to read next

- More than one step, retries and a failure handler: [Your first workflow](first-workflow.md).
- Which driver for your app: [Choose a driver](../choosing/drivers.md).
- Everything to check before launch: [Going to production](production.md#checklist).
- The whole order, step by step: the [learning path](../README.md#learning-path).

## Pitfalls

- `Enqueue` before `Start` returns `workflow.ErrNotStarted`, and `Register` after `Start` returns `workflow.ErrAlreadyStarted`: register every workflow first, then start once ([The client](../concepts/client.md#start-and-stop)).
- The memory driver keeps runs in the process; anything still queued when the program exits is lost. That is fine for this program and for tests, not for work you cannot lose ([Choose a driver](../choosing/drivers.md)).
- `Start` does not block. A `main` that returns right after `Enqueue` exits before the step runs; the example waits on a channel the step writes to.

## Related

- [Your first workflow](first-workflow.md): several steps, retries, a join and a failure handler.
- [The client](../concepts/client.md): every method of `workflow.Client`.
- Example: [quickstart](../../examples/quickstart/).
