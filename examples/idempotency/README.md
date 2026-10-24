# idempotency

One run per idempotency key while the run is unfinished, the key freed once it finishes, the three per-workflow policies (optional, required, off), and a client without idempotency keys refusing a job that carries one. It runs on the in-memory driver; the refusing client is the `workflowtest` Fake told to claim every capability but `CapIdempotencyKey`.

```sh
cd examples
go run ./idempotency
```

```text
second enqueue returned the first run: true
after the run finished, a new run: true
payout without a key: true
ping with the same key twice, two runs: true
no CapIdempotencyKey: workflow: Job.IdempotencyKey unsupported by this driver (missing IdempotencyKey): job for workflow invoice
```

## The code

The three workflows and their policies:

<!-- snippet: examples/idempotency/main.go#policies -->
```go
workflows := []workflow.Workflow{
	{
		Name: "invoice", // Idempotency empty: IdempotencyOptional, a key is honoured when given
		Steps: []workflow.Step{{Name: "send", Run: func(context.Context, []byte) ([]byte, error) {
			<-release // keep the run unfinished until the program says so
			return nil, nil
		}}},
	},
	{
		Name:        "payout",
		Idempotency: workflow.IdempotencyRequired, // an enqueue without a key fails
		Steps:       []workflow.Step{{Name: "pay", Run: noop}},
	},
	{
		Name:        "ping",
		Idempotency: workflow.IdempotencyOff, // keys are ignored: every enqueue is a run
		Steps:       []workflow.Step{{Name: "ping", Run: noop}},
	},
}
```

The same job twice, one run:

<!-- snippet: examples/idempotency/main.go#key -->
```go
job := workflow.Job{Workflow: "invoice", IdempotencyKey: "invoice:order-42"}
first, err := c.Enqueue(ctx, job)
if err != nil {
	return err
}
second, err := c.Enqueue(ctx, job) // a retried HTTP request, a redelivered message
if err != nil {
	return err
}
fmt.Fprintln(out, "second enqueue returned the first run:", second == first)
```

The key is free once the run has finished:

<!-- snippet: examples/idempotency/main.go#released -->
```go
third, err := c.Enqueue(ctx, job) // the first run has finished: the key is free
if err != nil {
	return err
}
fmt.Fprintln(out, "after the run finished, a new run:", third != first)
```

A workflow that requires a key:

<!-- snippet: examples/idempotency/main.go#required -->
```go
_, err = c.Enqueue(ctx, workflow.Job{Workflow: "payout"})
fmt.Fprintln(out, "payout without a key:", errors.Is(err, workflow.ErrInvalid))
```

A workflow that ignores keys:

<!-- snippet: examples/idempotency/main.go#off -->
```go
a, err := c.Enqueue(ctx, workflow.Job{Workflow: "ping", IdempotencyKey: "same"})
if err != nil {
	return err
}
b, err := c.Enqueue(ctx, workflow.Job{Workflow: "ping", IdempotencyKey: "same"})
if err != nil {
	return err
}
fmt.Fprintln(out, "ping with the same key twice, two runs:", a != b)
```

A driver without the capability refuses the key:

<!-- snippet: examples/idempotency/main.go#refusal -->
```go
all := memory.New().Capabilities()
f := workflowtest.NewFake(workflowtest.FakeCaps(all &^ workflow.CapIdempotencyKey))
noop := func(context.Context, []byte) ([]byte, error) { return nil, nil }
if err := f.Register(workflow.Workflow{Name: "invoice", Steps: []workflow.Step{{Name: "send", Run: noop}}}); err != nil {
	return err
}
if err := f.Start(ctx); err != nil {
	return err
}
_, err := f.Enqueue(ctx, workflow.Job{Workflow: "invoice", IdempotencyKey: "invoice:order-42"})
fmt.Fprintln(out, "no CapIdempotencyKey:", err)
```

`main_test.go` runs the program and compares its whole output with the block above.

## Guides

- [Idempotency keys](../../docs/jobs/idempotency.md)
