# fair-share

Round robin across concurrency keys and weighted shares, on the in-memory driver and on SQLite: one worker is held while tenant acme enqueues four runs and globex and initech enqueue theirs after it; with `ConcurrencyRoundRobin` the tenants take turns instead of waiting behind acme's backlog. With `Concurrency.Weight`, tenant gold (weight 3) gets three turns for each turn of tenant free (weight 1) while both have work waiting. Both drivers print the same order.

```sh
cd examples
go run ./fair-share
```

```text
memory:
  round robin: acme-1 globex-1 initech-1 acme-2 globex-2 acme-3 acme-4
  weights: free-1 gold-1 gold-2 gold-3 gold-4 free-2 gold-5 gold-6 free-3 free-4
sqlite:
  round robin: acme-1 globex-1 initech-1 acme-2 globex-2 acme-3 acme-4
  weights: free-1 gold-1 gold-2 gold-3 gold-4 free-2 gold-5 gold-6 free-3 free-4
```

## The code

Tenants take turns:

<!-- snippet: examples/fair-share/main.go#turns -->
```go
conc := &workflow.Concurrency{
	Key:      tenant,
	MaxRuns:  10,                             // per tenant at once
	Strategy: workflow.ConcurrencyRoundRobin, // tenants take turns for the workers
}
```

Weighted turns:

<!-- snippet: examples/fair-share/main.go#weights -->
```go
conc := &workflow.Concurrency{
	Key:      tenant,
	MaxRuns:  10,
	Strategy: workflow.ConcurrencyRoundRobin,
	// gold gets three turns for each turn of a tenant of weight 1
	Weight: workflow.KeyWeights(map[string]int{"gold": 3}, 1),
}
```

`main_test.go` runs the program and compares its whole output with the block above.

## Guides

- [Concurrency and rate limits: share the workers fairly between keys](../../docs/jobs/concurrency.md#share-the-workers-fairly-between-keys)
