# Concurrency and rate limits

Some work must not run too much at once: one report per tenant, so a big customer cannot take every worker; one sync per account, because two would fight over the same rows; no more than two emails a minute to one domain, because the receiving server throttles. A concurrency limit caps how many runs execute at the same time per key; a rate limit caps how many start per window of time per key. Both hold runs back; neither loses one unless you pick a strategy that drops.

## Before you start

- Concurrency needs `workflow.CapConcurrencyKey` and rate limits `workflow.CapRateLimit`. The memory, SQL, Redis and Hatchet drivers have both, with the key forms noted under [Limit runs per key](#limit-runs-per-key).
- The example is [`examples/concurrency`](../../examples/concurrency). It runs on the memory driver with a fake clock, so a one-minute rate window passes when the program moves the clock. Run it with `go run ./concurrency` from `examples/`.

## Limit runs per key

`Workflow.Concurrency` puts each run in a bucket chosen by `Key`, and lets at most `MaxRuns` runs of one bucket execute at once. Each distinct key value is its own bucket, so a cap of one per tenant still runs every tenant in parallel:

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
| A Go function of the job | `workflow.KeyFunc(func(workflow.Job) string)` | memory, SQL, Redis (in your process) |
| An expression in the engine's language | `workflow.KeyExpr("input.tenant")` | Hatchet (CEL, on the engine) |

A zero `Key` puts every run of the workflow in one bucket, so `MaxRuns: 1` with no key means "one at a time, globally". A driver given the form it cannot evaluate refuses it with an error naming the field, at `Start` on the memory and SQL drivers and at `Register` on Hatchet:

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

## Strategies

`Concurrency.Strategy` says what happens to a run that arrives for a full bucket:

| Strategy | The new run | The running run |
|---|---|---|
| `ConcurrencyQueueNewest` (the default) | Waits until the bucket has room | Keeps running |
| `ConcurrencyRoundRobin` | Waits; buckets take turns | Keeps running |
| `ConcurrencyDropNewest` | Is recorded as cancelled and never runs | Keeps running |
| `ConcurrencyCancelNewest` | Is recorded as cancelled and never runs | Keeps running |
| `ConcurrencyCancelInProgress` | Runs | The oldest running run of the bucket is cancelled to make room |

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

A dropped run is still created: `Enqueue` returns its `RunRef` and a nil error, and its status is `StatusCancelled`. An enqueue is never an error because of concurrency, on any driver:

<!-- snippet: examples/concurrency/main.go#dropped -->
```go
s2, err := enqueue("sync", "sync-2", nil) // the bucket is full
fmt.Fprintf(out, "  second sync: error %v, status %s\n", err, status(s2))
```

## Keep one key's runs in order

Some work must not only run one at a time but in order: the events of one account applied as they happened, the versions of one document published oldest first. `Concurrency.FIFO` promises that the queued runs of one key start in the order they were enqueued. With `MaxRuns: 1` that is a sequence per key: one at a time, in insert order, while other keys run in parallel.

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

- FIFO needs `workflow.CapKeyFIFO`, and a driver without it refuses the workflow rather than starting runs in another order. The memory, Redis, SQL and Hatchet drivers have it.
- It works only with `ConcurrencyQueueNewest`, the default: the other strategies drop, cancel or rotate runs, so there is no order to keep. `Register` refuses another strategy with `ErrInvalid`.
- `Job.Priority` still goes first: a higher-priority run of the key starts before older ones. Leave priorities equal on a workflow that must keep order.
- A run holds its slot until it ends, retries and backoffs included, so the next run of the key waits for the previous one to finish, not just for its current attempt.

## Rate limits

`Workflow.RateLimits` caps run starts: at most `Limit` starts per window of length `Per`, per key value. A run held back by a limit stays queued and starts once the window allows it; it is delayed, never dropped. Several limits on one workflow must all allow a start.

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

Engines count windows differently, so the contract every driver keeps is stated for a fixed window, the loosest kind. For one key: no span of length `Per` holds more than `2*Limit` starts (a fixed window allows a burst at the end of one window and the start of the next); within one window at most `Limit` start; and `N` starts take at least `(ceil(N/Limit) - 2) * Per`. The memory driver is stricter: it keeps a sliding window, so no span of length `Per` holds more than `Limit` starts.

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

- **One workflow definition cannot serve both key forms.** A workflow keyed with `KeyFunc` runs on the memory, SQL and Redis drivers; on Hatchet it needs `KeyExpr`. If the same code must run on both, build the `Concurrency` value per driver at start-up.
- **Hatchet maps the strategies onto its own.** `QueueNewest` and `RoundRobin` both become Hatchet's group round-robin, and `DropNewest` and `CancelNewest` both become its cancel-newest; the observable behaviour (wait, or recorded as cancelled) is the one in the table.
- **Hatchet's rate limits have their own limits.** The Hatchet driver accepts `Per` of exactly a second, minute, hour, day or week, refuses a limit with no `Key` (use ``workflow.KeyExpr(`"all"`)`` for one bucket), and refuses rate limits on a workflow with more than one first step, because Hatchet limits tasks, not runs. Its window is fixed, refilled when the engine next checks the bucket, so the next run of a full bucket may start up to about 30 seconds after the window ends.
- **A key function runs at enqueue time, on the job.** It sees `Payload` and `Labels`, not step outputs. Keep it cheap and deterministic: the same job must always land in the same bucket.
- **FIFO orders starts, not ends.** With `MaxRuns` above 1, runs of one key start in order but can finish in any order; use `MaxRuns: 1` for a strict sequence.
- **A failed run does not stop the sequence.** The next run of the key starts once the failed one has ended; if later runs depend on earlier ones, check for that in the step.
- **Concurrency caps runs, not workers.** A capped bucket holds runs queued but uses no worker while it waits; how many steps run at once overall is the driver's worker count (`memory.WithWorkers`, `sqlstore.WithWorkers`, Hatchet `Config.Slots`).

## Related

- [Priority](enqueue.md#priority)
- [Debounce](../reliability/debounce.md) and [unique jobs](../reliability/unique-jobs.md), which decide whether a run is created at all
- [Capabilities](../concepts/capabilities.md)
- Example: [`examples/concurrency`](../../examples/concurrency)
- Example: [`examples/queue-weights`](../../examples/queue-weights) (FIFO per key)
