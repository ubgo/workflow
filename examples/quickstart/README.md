# quickstart

One workflow on the in-memory driver: register a one-step workflow whose handler takes a typed payload, start the workers, enqueue a job and print what the step did.

```sh
cd examples
go run ./quickstart
```

```text
hello Ada
```

## The code

Register a workflow with one step; `workflow.Define` turns a typed function into a handler that decodes the JSON payload:

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

Start the workers, then enqueue a job. Cancelling the context passed to `Start` stops the workers:

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

`main_test.go` runs the same program and checks the line it prints.
