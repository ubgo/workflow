# concurrency

One run at a time per tenant, what each strategy does to the run over the cap, a rate limit per recipient domain, priorities among waiting runs, and the key form each driver refuses. It runs on the in-memory driver with a fake clock, so the one-minute rate window passes when the program moves the clock; the Hatchet refusal needs no server, because the Hatchet client connects only at `Start`.

```sh
cd examples
go run ./concurrency
```

```text
per key:
  acme-1 running, acme-2 queued, globex-1 running
  acme-1 finished, started: acme-2
drop newest:
  second sync: error <nil>, status cancelled
cancel in progress:
  started preview-2; preview-1 cancelled, preview-2 running
rate limit:
  started mail-1 and mail-2; mail-3 queued
  a minute later, started mail-3
priority:
  password-reset, normal, backfill
refusals:
  memory, KeyExpr: workflow: Workflow.Concurrency.Key.Expr unsupported by this driver: the memory driver evaluates only KeyFunc keys; workflow report
  hatchet, KeyFunc: workflow: Workflow.Concurrency.Key.Func unsupported by this driver: workflow report: Hatchet evaluates concurrency keys on the engine; use workflow.KeyExpr with a CEL expression | unsupported: true
```

## The code

One report per tenant at a time:

<!-- snippet: examples/concurrency/main.go#per-key -->
```go
tenant := workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["tenant"] })
report := workflow.Workflow{
	Name: "report",
	Concurrency: &workflow.Concurrency{
		Key:      tenant, // one bucket per tenant
		MaxRuns:  1,      // one report per tenant at a time
		Strategy: workflow.ConcurrencyQueueNewest,
	},
	Steps: []workflow.Step{{Name: "build", Run: g.handler}},
}
```

Drop the newest run, or cancel the one in progress:

<!-- snippet: examples/concurrency/main.go#strategies -->
```go
sync := workflow.Workflow{
	Name:        "sync",
	Concurrency: &workflow.Concurrency{MaxRuns: 1, Strategy: workflow.ConcurrencyDropNewest},
	Steps:       []workflow.Step{{Name: "pull", Run: g.handler}},
}
preview := workflow.Workflow{
	Name:        "preview",
	Concurrency: &workflow.Concurrency{MaxRuns: 1, Strategy: workflow.ConcurrencyCancelInProgress},
	Steps:       []workflow.Step{{Name: "render", Run: g.handler}},
}
```

A dropped run is created, cancelled, and not an error:

<!-- snippet: examples/concurrency/main.go#dropped -->
```go
s2, err := enqueue("sync", "sync-2", nil) // the bucket is full
fmt.Fprintf(out, "  second sync: error %v, status %s\n", err, status(s2))
```

Two starts per minute per domain:

<!-- snippet: examples/concurrency/main.go#rate-limit -->
```go
mail := workflow.Workflow{
	Name: "mail",
	RateLimits: []workflow.RateLimit{{
		Key:   workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["domain"] }),
		Limit: 2,           // at most two starts
		Per:   time.Minute, // per minute, per recipient domain
	}},
	Steps: []workflow.Step{{Name: "send", Run: g.handler}},
}
```

Three priorities behind one busy worker:

<!-- snippet: examples/concurrency/main.go#priority -->
```go
for _, j := range []workflow.Job{
	{Workflow: "task", Payload: []byte("backfill"), Priority: -10},
	{Workflow: "task", Payload: []byte("normal")}, // Priority 0 is the default
	{Workflow: "task", Payload: []byte("password-reset"), Priority: 10},
} {
	if _, err := c.Enqueue(ctx, j); err != nil {
		return err
	}
}
```

Each driver refuses the key form it cannot evaluate:

<!-- snippet: examples/concurrency/main.go#key-forms -->
```go
byExpr := workflow.Workflow{
	Name:        "report",
	Concurrency: &workflow.Concurrency{Key: workflow.KeyExpr("input.tenant"), MaxRuns: 1},
	Steps:       []workflow.Step{{Name: "build", Run: noop}},
}
byFunc := byExpr
byFunc.Concurrency = &workflow.Concurrency{
	Key:     workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["tenant"] }),
	MaxRuns: 1,
}

m := memory.New()
if err := m.Register(byExpr); err != nil { // accepted here; Start validates the key
	return err
}
err := m.Start(ctx)
fmt.Fprintln(out, "  memory, KeyExpr:", err)
m.Stop()

err = hatchet.New(hatchet.Config{}).Register(byFunc)
fmt.Fprintln(out, "  hatchet, KeyFunc:", err, "| unsupported:", errors.Is(err, workflow.ErrUnsupported))
```

`main_test.go` runs the program and compares its whole output with the block above. The two rate-limited runs that start together are printed sorted, since they start on two workers in either order.

## Guides

- [Concurrency and rate limits](../../docs/jobs/concurrency.md)
- [Enqueue › Priority](../../docs/jobs/enqueue.md#priority)
