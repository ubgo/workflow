# Concurrency and rate limits

Some work must not run too much at once: one report per tenant, so a big customer cannot take every worker; one sync per account, because two would fight over the same rows; no more than two emails a minute to one domain, because the receiving server throttles. A concurrency limit caps how many runs execute at the same time per key; a rate limit caps how many start per window of time per key. Both hold runs back; neither loses one unless you pick a strategy that drops.

## Before you start

- Concurrency needs `workflow.CapConcurrencyKey` and rate limits `workflow.CapRateLimit` ([`Requires`](ds:block?id=requires-6tcjbcfj)). The [memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-caps-yu37ef2j), [Redis](ds:block?id=redis-caps-bfgdst9t) and [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z) drivers have both, with the key forms noted under [Limit runs per key](#limit-runs-per-key).
- The example is [`examples/concurrency`](../../examples/concurrency). It runs on the memory driver with a fake clock, so a one-minute rate window passes when the program moves the clock. Run it with `go run ./concurrency` from `examples/`.

## Limit runs per key

`Workflow.Concurrency` puts each run in a bucket chosen by `Key`, and lets at most `MaxRuns` runs of one bucket execute at once ([`Concurrency`](ds:block?id=concurrency-h4md62gd)). Each distinct key value is its own bucket, so a cap of one per tenant still runs every tenant in parallel:

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

A key has two forms, and each driver evaluates one of them:

| Form | Built with | Evaluated by |
|---|---|---|
| A Go function of the job | `workflow.KeyFunc(func(workflow.Job) string)` | [memory](ds:block?id=memory-exprkeys-mrxv22pq), [SQL](ds:block?id=sql-exprkeys-wxvw57q3), [Redis](ds:block?id=redis-exprkeys-g3qq6r7w) (in your process) |
| An expression in the engine's language | `workflow.KeyExpr("input.tenant")` | [Hatchet](ds:block?id=hatchet-checksupported-733uwgsp) (CEL, on the engine) |

A zero `Key` puts every run of the workflow in one bucket ([`ConcurrencyKey`](ds:block?id=concurrencykey-hp6bd4qc)), so `MaxRuns: 1` with no key means "one at a time, globally". A driver given the form it cannot evaluate refuses it with an error naming the field, at `Start` on the [memory](ds:block?id=memory-exprkeys-mrxv22pq), [SQL](ds:block?id=sql-exprkeys-wxvw57q3) and [Redis](ds:block?id=redis-exprkeys-g3qq6r7w) drivers and at `Register` on [Hatchet](ds:block?id=hatchet-checksupported-733uwgsp):

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
fmt.Println("  memory, KeyExpr:", err)
m.Stop()

err = hatchet.New(hatchet.Config{}).Register(byFunc)
fmt.Println("  hatchet, KeyFunc:", err, "| unsupported:", errors.Is(err, workflow.ErrUnsupported))
```

## Strategies

[`Concurrency.Strategy`](ds:block?id=concurrency-h4md62gd) says what happens to a run that arrives for a full bucket:

| Strategy | The new run | The running run |
|---|---|---|
| [`ConcurrencyQueueNewest`](ds:block?id=concurrencyqueuenewest-ttfpmmum) (the default) | Waits until the bucket has room | Keeps running |
| [`ConcurrencyRoundRobin`](ds:block?id=concurrencyroundrobin-rd8zthfc) | Waits; buckets take turns | Keeps running |
| [`ConcurrencyDropNewest`](ds:block?id=concurrencydropnewest-djnbu5nc) | Is recorded as cancelled and never runs | Keeps running |
| [`ConcurrencyCancelNewest`](ds:block?id=concurrencycancelnewest-59va9tax) | Is recorded as cancelled and never runs | Keeps running |
| [`ConcurrencyCancelInProgress`](ds:block?id=concurrencycancelinprogress-xwcnbcnw) | Runs | The oldest running run of the bucket is cancelled to make room |

With `ConcurrencyRoundRobin` the keys also take turns for the workers, so a tenant that enqueued a thousand runs does not delay a tenant that enqueued one; see [Share the workers fairly between keys](#share-the-workers-fairly-between-keys).

Use queueing for work that must all happen (reports, imports), dropping for work where one run in flight is enough (a "sync now" button pressed twice), and cancel-in-progress for work where only the latest matters (a preview re-rendered on every keystroke).

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

A dropped run is still created: `Enqueue` returns its `RunRef` and a nil error, and its status is `StatusCancelled` (the conformance case [`newestRefused`](ds:block?id=workflowtest-newestrefused-mx5r7ykq)). An enqueue is never an error because of concurrency, on any driver:

<!-- snippet: examples/concurrency/main.go#dropped -->
```go
s2, err := enqueue("sync", "sync-2", nil) // the bucket is full
fmt.Printf("  second sync: error %v, status %s\n", err, status(s2))
```

## Keep one key's runs in order

Some work must not only run one at a time but in order: the events of one account applied as they happened, the versions of one document published oldest first. [`Concurrency.FIFO`](ds:block?id=concurrency-h4md62gd) promises that the queued runs of one key start in the order they were enqueued. With `MaxRuns: 1` that is a sequence per key: one at a time, in insert order, while other keys run in parallel.

<!-- snippet: examples/queue-weights/main.go#fifo -->
```go
wf := workflow.Workflow{
	Name: "sync-account",
	Concurrency: &workflow.Concurrency{
		Key:     workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["account"] }),
		MaxRuns: 1,    // one sync per account at a time
		FIFO:    true, // and in the order they were enqueued
	},
	Steps: []workflow.Step{{Name: "sync", Run: h}},
}
```

The example holds the account's bucket with one run, enqueues three more for the same account, and lets the first finish:

```text
fifo per key:
  started sync-1
  started sync-2
  started sync-3
```

The rules:

- FIFO needs `workflow.CapKeyFIFO` ([`Requires`](ds:block?id=requires-6tcjbcfj)), and a driver without it refuses the workflow rather than starting runs in another order. The [memory](ds:block?id=memory-caps-yxkuq583), [Redis](ds:block?id=redis-caps-bfgdst9t), [SQL](ds:block?id=sql-p5caps-wggh6esn) and [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z) drivers have it.
- It works only with `ConcurrencyQueueNewest`, the default: the other strategies drop, cancel or rotate runs, so there is no order to keep. `Register` refuses another strategy with `ErrInvalid` ([`validatePolicies`](ds:block?id=validatepolicies-x88m4hwu)).
- `Job.Priority` still goes first: a higher-priority run of the key starts before older ones ([`Concurrency.FIFO`](ds:block?id=concurrency-h4md62gd)). Leave priorities equal on a workflow that must keep order.
- A run holds its slot until it ends, retries and backoffs included, so the next run of the key waits for the previous one to finish, not just for its current attempt.

## Share the workers fairly between keys

`MaxRuns` caps each key, but it does not decide whose run a free worker takes next. With the default strategy that is the oldest run, so a tenant that enqueued a thousand runs a minute ago is served before a tenant that enqueued one now. [`ConcurrencyRoundRobin`](ds:block?id=concurrencyroundrobin-rd8zthfc) makes the keys take turns:

<!-- snippet: examples/fair-share/main.go#turns -->
```go
conc := &workflow.Concurrency{
	Key:      tenant,
	MaxRuns:  10,                             // per tenant at once
	Strategy: workflow.ConcurrencyRoundRobin, // tenants take turns for the workers
}
```

[`Concurrency.Weight`](ds:block?id=concurrency-h4md62gd) gives some keys a bigger share: while keys have work waiting, a key of weight 3 gets three turns for each turn of a key of weight 1. [`workflow.KeyWeights`](ds:block?id=keyweights-uwk8upj9) builds one from a map; any `func(key string) int` works, so the weight can come from the tenant's plan:

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

The example holds one worker while acme enqueues four runs, then globex and initech enqueue theirs; then tenants free (weight 1) and gold (weight 3). Both drivers print the order the worker started them in:

```text
memory:
  round robin: acme-1 globex-1 initech-1 acme-2 globex-2 acme-3 acme-4
  weights: free-1 gold-1 gold-2 gold-3 gold-4 free-2 gold-5 gold-6 free-3 free-4
sqlite:
  round robin: acme-1 globex-1 initech-1 acme-2 globex-2 acme-3 acme-4
  weights: free-1 gold-1 gold-2 gold-3 gold-4 free-2 gold-5 gold-6 free-3 free-4
```

How it works, the same on every driver that has `workflow.CapRoundRobin`: when a run is admitted it gets a turn number from its key, one stride after the key's previous run ([`workflow.FairTurn`](ds:block?id=fairturn-usczqc7a); the stride is shorter for a heavier key), and free workers serve the workflow's waiting runs by turn number. A key that had nothing waiting starts at the current turn, so it gets no credit for the time it was idle and cannot flood the workers when it comes back.

The rules:

- Turns apply inside one workflow and one priority. `Job.Priority` still goes first, and a round-robin workflow keeps its place among other workflows: fairness decides which tenant's run takes the workflow's next place in line, never whether the workflow goes before another one.
- A turn is a run, not a step: the steps of one run share its turn.
- Weights need `workflow.CapKeyWeights` and `ConcurrencyRoundRobin` (`Register` refuses `Weight` with another strategy, `ErrInvalid`: [`validatePolicies`](ds:block?id=validatepolicies-x88m4hwu)). The [memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-p5caps-wggh6esn) and [Redis](ds:block?id=redis-caps-bfgdst9t) drivers have both capabilities; Hatchet serves keys in equal turns with its group round-robin and refuses `Weight` ([`checkSupported`](ds:block?id=hatchet-checksupported-733uwgsp)).
- Shares hold while keys have work waiting. A key with nothing waiting does not hold back the others, and its unused share goes to them.
- A key that is at `MaxRuns` does not block the others' admission either: its waiting runs are passed over until a slot frees (the SQL driver skips full buckets when it reads waiting runs; Redis parks them per bucket).

## Rate limits

`Workflow.RateLimits` caps run starts: at most `Limit` starts per window of length `Per`, per key value. A run held back by a limit stays queued and starts once the window allows it; it is delayed, never dropped ([`RateLimit`](ds:block?id=ratelimit-n8n9g5d8)). Several limits on one workflow must all allow a start.

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

In the example, three emails to `example.com` are enqueued at once: two start, the third stays queued until the program moves the fake clock by a minute.

Engines count windows differently, so the contract every driver keeps is stated for a fixed window, the loosest kind ([`RateLimit`](ds:block?id=ratelimit-n8n9g5d8)). For one key: no span of length `Per` holds more than `2*Limit` starts (a fixed window allows a burst at the end of one window and the start of the next); within one window at most `Limit` start; and `N` starts take at least `(ceil(N/Limit) - 2) * Per`. The memory driver is stricter: it keeps a sliding window ([`rateCheckLocked`](ds:block?id=memory-client-ratechecklocked-ce6n2ccw)), so no span of length `Per` holds more than `Limit` starts.

## Try it

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

The `priority` lines belong to [Enqueue › Priority](enqueue.md#priority).

## Pitfalls

- **One workflow definition cannot serve both key forms.** A workflow keyed with `KeyFunc` runs on the memory, SQL and Redis drivers; on Hatchet it needs `KeyExpr` ([`checkSupported`](ds:block?id=hatchet-checksupported-733uwgsp)). If the same code must run on both, build the `Concurrency` value per driver at start-up. See [Hatchet limitations](../drivers/hatchet.md#limitations).
- **Hatchet maps the strategies onto its own** ([`concurrencyStrategy`](ds:block?id=hatchet-concurrencystrategy-hzvwerpw)). `QueueNewest` and `RoundRobin` both become Hatchet's group round-robin, and `DropNewest` and `CancelNewest` both become its cancel-newest; the observable behaviour (wait, or recorded as cancelled) is the one in the table.
- **Hatchet's rate limits have their own limits** ([`rateLimit`](ds:block?id=hatchet-ratelimit-mvwtadc2)). The Hatchet driver accepts `Per` of exactly a second, minute, hour, day or week, refuses a limit with no `Key` (use ``workflow.KeyExpr(`"all"`)`` for one bucket), and refuses rate limits on a workflow with more than one first step, because Hatchet limits tasks, not runs ([`checkSupported`](ds:block?id=hatchet-checksupported-733uwgsp)). Its window is fixed, refilled when the engine next checks the bucket, so the next run of a full bucket may start up to about 30 seconds after the window ends.
- **A key function runs at enqueue time, on the job** ([`ConcurrencyKey`](ds:block?id=concurrencykey-hp6bd4qc)). It sees `Payload` and `Labels`, not step outputs. Keep it cheap and deterministic: the same job must always land in the same bucket.
- **FIFO orders starts, not ends** ([`Concurrency.FIFO`](ds:block?id=concurrency-h4md62gd)). With `MaxRuns` above 1, runs of one key start in order but can finish in any order; use `MaxRuns: 1` for a strict sequence.
- **A failed run does not stop the sequence** ([`Concurrency.FIFO`](ds:block?id=concurrency-h4md62gd)). The next run of the key starts once the failed one has ended; if later runs depend on earlier ones, check for that in the step.
- **The weight function runs when a run is admitted** ([`Concurrency.Weight`](ds:block?id=concurrency-h4md62gd)). It is called once per run, in the worker that admits it (on SQL and Redis that may be another process than the one that enqueued it), so every client must register the same `Weight`, and it should be fast; a weight change applies to runs admitted after it.
- **Concurrency caps runs, not workers** ([`Concurrency`](ds:block?id=concurrency-h4md62gd)). A capped bucket holds runs queued but uses no worker while it waits; how many steps run at once overall is the driver's worker count (`memory.WithWorkers`, `sqlstore.WithWorkers`, Hatchet `Config.Slots`).

## Related

- [Priority](enqueue.md#priority)
- [Debounce](../reliability/debounce.md) and [unique jobs](../reliability/unique-jobs.md), which decide whether a run is created at all
- [Capabilities](../concepts/capabilities.md)
- Example: [`examples/concurrency`](../../examples/concurrency)
- Example: [`examples/queue-weights`](../../examples/queue-weights) (FIFO per key)
- Example: [`examples/fair-share`](../../examples/fair-share) (round robin and weights)
