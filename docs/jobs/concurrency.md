# Concurrency and rate limits

Some work must not run too much at once: one report per tenant, so a big customer cannot take every worker; one sync per account, because two would fight over the same rows; no more than two emails a minute to one domain, because the receiving server throttles. A concurrency limit caps how many runs execute at the same time per key, on one key or on several at once; a rate limit caps how many start per window of time per key. Both hold runs back; neither loses one unless you pick a strategy that drops.

## Before you start

- Concurrency needs `workflow.CapConcurrencyKey`, more keys per workflow `workflow.CapConcurrencyLimits`, and rate limits `workflow.CapRateLimit` ([`Requires`](ds:block?id=requires-6tcjbcfj)). The [memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-caps-yu37ef2j), [Redis](ds:block?id=redis-caps-bfgdst9t) and [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z) drivers have concurrency and rate limits, with the key forms noted under [Limit runs per key](#limit-runs-per-key); only the memory and SQL drivers have more keys per workflow ([Limit runs on several keys](#limit-runs-on-several-keys)).
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
- A run holds its slot until it ends, retries and backoffs included, so the next run of the key waits for the previous one to finish, not just for its current attempt. That is the default hold policy, and the only one FIFO accepts: `Register` refuses `FIFO` with `Hold: HoldWhileRunning` ([`validateHold`](ds:block?id=validatehold-q4m7ze2s)), because a later run of the key would start while an earlier one waits to retry. See [Choose how long a run holds its slot](#choose-how-long-a-run-holds-its-slot).

## Choose how long a run holds its slot

By default a run keeps its concurrency slot from the moment it is admitted until it ends, retry backoffs, snoozes, durable sleeps and waits for an event included ([`HoldWholeRun`](ds:block?id=holdwholerun-p7d2xn4c)). With `MaxRuns: 1` and a step whose API says "come back in an hour", the account is blocked for that hour while nothing executes. [`Concurrency.Hold`](ds:block?id=concurrency-h4md62gd) (and `ConcurrencyLimit.Hold` for each [limit](#limit-runs-on-several-keys)) set to [`HoldWhileRunning`](ds:block?id=holdwhilerunning-r9f5mt6b) gives the slot back while the run waits:

<!-- snippet: examples/concurrency/main.go#release-while-waiting -->
```go
syncAccount := workflow.Workflow{
	Name: "sync-account",
	Concurrency: &workflow.Concurrency{
		Key:     account,
		MaxRuns: 1,                         // one sync per account at a time
		Hold:    workflow.HoldWhileRunning, // but not while one waits to retry
	},
	Steps: []workflow.Step{{Name: "pull", Run: pull, Retry: &workflow.RetryPolicy{
		Max:     3,
		Backoff: workflow.Backoff{Initial: time.Hour}, // the API asks to come back in an hour
	}}},
}
```

The example runs the same two syncs of one account under each policy. acme-1's first call is rate limited, so its retry is an hour away; acme-2 is enqueued during that hour:

```text
release while waiting:
  whole_run: acme-1 retrying in an hour; acme-2 queued
    an hour later: acme-1 succeeded, then acme-2 succeeded
  while_running: acme-1 retrying in an hour; acme-2 succeeded at once
    an hour later: acme-1 succeeded
```

Which to choose:

- **`HoldWholeRun`** (the default, the zero value) when the key protects something the whole run owns: a sequence per key (`FIFO`), a run whose later steps rely on no other run of the key having touched the data since its first step, or a downstream system that must see one run of the key at a time from start to end.
- **`HoldWhileRunning`** when the key only caps work done at once (sessions open against an API, load on a database) and a run that waits holds nothing: a retry backoff, a [snooze](retries.md#snooze-a-step-that-is-not-ready), a durable sleep, a wait for an approval or a webhook. The key's other runs use the time the waiting run does not.

How `HoldWhileRunning` works ([`ConcurrencyHold`](ds:block?id=concurrencyhold-k3v8qw2m)):

- **When the slot is given back.** A run that has nothing executing and nothing waiting for a worker gives back the slot of every key set to `HoldWhileRunning`: every unfinished step is in a retry backoff, snoozed, in a durable sleep, or waiting for an event or signal. A run with any step executing or queued for a worker keeps its slots, so a run whose steps run in parallel holds them while one step executes and another waits in its backoff. Keys left at `HoldWholeRun` stay held.
- **When it is taken back.** Once its parked work is ready again (the backoff, snooze or sleep ends, the event arrives), the run must have its slots back before the ready step runs. It takes every one of its `HoldWhileRunning` slots in one step, only when all of those keys have room; until then the step waits, and the run reports `StatusQueued`. Taking a slot back never exceeds `MaxRuns`, on any number of workers or nodes.
- **Where it waits.** It rejoins admission at its original place: higher `Job.Priority` first, then the order runs were enqueued, as when it was first admitted, not at the back of the line. A run enqueued before it never waits behind it, so it cannot jump ahead unfairly; a run enqueued after it, even one that has waited longer for its first start, goes after it. Back of the line was rejected because a run that retries several times would queue behind the whole backlog of its key on every retry, so under a steady stream of new runs it could take hours to finish what it started; its original place bounds its wait to the runs ahead of it, which all finish or park.
- **Priority.** The original `Job.Priority` still goes first: a parked run of priority 0 ready again waits behind a higher-priority run enqueued later, as any run does.
- **Rate limits.** Taking the slot back is not a new start: `RateLimits` are counted once, at the run's first admission, and do not hold a rejoining run back.
- **Several keys.** The [fairness rule](#limit-runs-on-several-keys) holds for a rejoining run like for a new one: a waiting run is never overtaken on any of its keys by a run behind it, so a rejoining run holds later runs off the keys it waits for, and an earlier waiting run holds it off its keys. Only the keys it gave back count; the `HoldWholeRun` keys it kept are not asked for again.
- **Ending while parked.** A run cancelled while parked, or failed while parked (a wait timed out, a run timeout passed), ends without taking its slots back, so the slots it gave back are not given back twice. Its `OnFailure` handler runs outside the cap.
- **Child runs.** A child run is a run of its own workflow, admitted on its own keys like any run (buckets are scoped to the workflow). A parent whose step waits for the child inside its handler is executing and keeps its slots; a parent that waits for the child durably, in a wait step for the child's signal, is parked and gives its `HoldWhileRunning` slots back.
- **Conflicts.** `Register` refuses, with `ErrInvalid` naming the fields ([`validateHold`](ds:block?id=validatehold-q4m7ze2s)), `Concurrency.Hold: HoldWhileRunning` with `Concurrency.FIFO` (a later run of the key would start while an earlier one is parked, breaking the sequence) and with any `Strategy` but `ConcurrencyQueueNewest` (dropping or cancelling the newest run, cancelling the oldest holder and handing out round-robin turns are decided when a run is first admitted and have no meaning for a run taking its slot back). `FIFO` beside a limit set to `HoldWhileRunning` is accepted: the FIFO key itself is held for the whole run.
- **Drivers.** `HoldWhileRunning` needs `workflow.CapHoldWhileRunning` ([`Requires`](ds:block?id=requires-6tcjbcfj), [`CapHoldWhileRunning`](ds:block?id=capholdwhilerunning-h6t3nb8d)); the zero value and `HoldWholeRun` need nothing new, and [`HoldsWhileRunning`](ds:block?id=holdswhilerunning-w2c8ka5t) tells whether a workflow sets the policy on any key. The [memory](ds:block?id=memory-caps-yxkuq583) and [SQL](ds:block?id=sql-p5caps-wggh6esn) drivers have it (the SQL driver from migration `013_run_hold`). Redis and Hatchet do not, and refuse such a workflow (Redis at `Start`, Hatchet at `Register`, [`checkSupported`](ds:block?id=hatchet-checksupported-733uwgsp)) rather than hold the slot for the whole run.

## Limit runs on several keys

One key is not always enough: a sync may need to stay under two at once per connection, because the connection's API allows two parallel sessions, and one at a time per account, because two syncs of one account would fight over the same rows. [`Workflow.ConcurrencyLimits`](ds:block?id=concurrencylimit-5anrp8rw) adds keys beside `Concurrency`, each with its own `MaxRuns`:

<!-- snippet: examples/concurrency/main.go#several-keys -->
```go
label := func(name string) workflow.ConcurrencyKey {
	return workflow.KeyFunc(func(j workflow.Job) string { return j.Labels[name] })
}
syncAccount := workflow.Workflow{
	Name: "sync-account",
	ConcurrencyLimits: []workflow.ConcurrencyLimit{
		{Key: label("connection"), MaxRuns: 2}, // two syncs per connection
		{Key: label("account"), MaxRuns: 1},    // and one per account
	},
	Steps: []workflow.Step{{Name: "pull", Run: g.handler}},
}
```

The example enqueues acme on `conn-1` and `conn-2` and widgets on `conn-1` and `conn-2`. acme-1 and widgets-1 start (`conn-1` has two slots); acme-2 and widgets-2 wait for their account, and each starts when the run of its account finishes.

The rules ([`ConcurrencyLimit`](ds:block?id=concurrencylimit-5anrp8rw)):

- A run is admitted only when every one of its buckets has room: its `Concurrency` bucket when the workflow has one, and its bucket of each limit. It takes all of them in one step and gives them all back when it ends (succeeded, failed, cancelled or discarded), so a run never holds one key while it waits for another, and two runs cannot deadlock on each other's keys.
- Each limit has its own buckets, scoped to the workflow like `Concurrency`'s: the value `"acme"` under one limit and `"acme"` under another are two buckets.
- A run that finds a bucket full waits; it is never dropped or cancelled for it. So beside a limit, `Concurrency.Strategy` may only be `ConcurrencyQueueNewest`, the default; `Register` refuses another strategy with `ErrInvalid` ([`validateConcurrencyLimits`](ds:block?id=validateconcurrencylimits-dyxqw4nf)). `Concurrency.FIFO` still works.
- Fairness: in admission order (higher `Job.Priority` first, then enqueue order), a waiting run is never overtaken on any of its keys by a run behind it. A run that needs two keys is therefore not starved by a stream of runs that need only one of them. The price is head-of-line waiting: a later run that shares a key with an earlier waiting run waits behind it even when that key has room.
- By default slots are held for the whole run, as with one key: a run in its retry backoff keeps every key until it ends. Each limit has its own `Hold`, so a parked run can give back the keys set to `HoldWhileRunning` and keep the others (see [Choose how long a run holds its slot](#choose-how-long-a-run-holds-its-slot)).
- Limits need `workflow.CapConcurrencyLimits` ([`Requires`](ds:block?id=requires-6tcjbcfj), [`CapConcurrencyLimits`](ds:block?id=capconcurrencylimits-585ybb9u)). The [memory](ds:block?id=memory-caps-yxkuq583) and [SQL](ds:block?id=sql-p5caps-wggh6esn) drivers have it. Redis and Hatchet do not, and refuse such a workflow (Redis at `Start`, Hatchet at `Register`, [`checkSupported`](ds:block?id=hatchet-checksupported-733uwgsp)) rather than enforce one key only; the example prints Hatchet's refusal.

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
several keys:
  started acme-1 and widgets-1; acme-2 queued, widgets-2 queued
  acme-1 finished, started: acme-2
  widgets-1 finished, started: widgets-2
  hatchet: true | missing: ConcurrencyLimits
release while waiting:
  whole_run: acme-1 retrying in an hour; acme-2 queued
    an hour later: acme-1 succeeded, then acme-2 succeeded
  while_running: acme-1 retrying in an hour; acme-2 succeeded at once
    an hour later: acme-1 succeeded
refusals:
  memory, KeyExpr: workflow: Workflow.Concurrency.Key.Expr unsupported by this driver: the memory driver evaluates only KeyFunc keys; workflow report
  hatchet, KeyFunc: workflow: Workflow.Concurrency.Key.Func unsupported by this driver: workflow report: Hatchet evaluates concurrency keys on the engine; use workflow.KeyExpr with a CEL expression | unsupported: true
```

The `priority` lines belong to [Enqueue › Priority](enqueue.md#priority).

## Pitfalls

- **One workflow definition cannot serve both key forms.** A workflow keyed with `KeyFunc` runs on the memory, SQL and Redis drivers; on Hatchet it needs `KeyExpr` ([`checkSupported`](ds:block?id=hatchet-checksupported-733uwgsp)). If the same code must run on both, build the `Concurrency` value per driver at start-up. See [Hatchet limitations](../drivers/hatchet.md#limitations).
- **Hatchet maps the strategies onto its own** ([`concurrencyStrategy`](ds:block?id=hatchet-concurrencystrategy-hzvwerpw)). `QueueNewest` and `RoundRobin` both become Hatchet's group round-robin, and `DropNewest` and `CancelNewest` both become its cancel-newest; the observable behaviour (wait, or recorded as cancelled) is the one in the table.
- **Hatchet's rate limits have their own limits** ([`rateLimit`](ds:block?id=hatchet-ratelimit-mvwtadc2)). The Hatchet driver accepts `Per` of exactly a second, minute, hour, day or week, refuses a limit with no `Key` (use ``workflow.KeyExpr(`"all"`)`` for one bucket), and refuses rate limits on a workflow with more than one first step, because Hatchet limits tasks, not runs ([`checkSupported`](ds:block?id=hatchet-checksupported-733uwgsp)). Its window is fixed, refilled when the engine next checks the bucket, so the next run of a full bucket may start up to about 30 seconds after the window ends.
- **A key function runs at enqueue time, on the job** ([`ConcurrencyKey`](ds:block?id=concurrencykey-hp6bd4qc)). It sees `Payload` and `Labels`, not step outputs. Keep it cheap and deterministic: the same job must always land in the same bucket. On a [coalescing workflow](../reliability/coalesce.md) with `CoalesceKeepLast` it runs again when a later enqueue replaces the payload of a run not yet admitted, so the run is admitted under the keys of the payload it runs with.
- **FIFO orders starts, not ends** ([`Concurrency.FIFO`](ds:block?id=concurrency-h4md62gd)). With `MaxRuns` above 1, runs of one key start in order but can finish in any order; use `MaxRuns: 1` for a strict sequence.
- **A failed run does not stop the sequence** ([`Concurrency.FIFO`](ds:block?id=concurrency-h4md62gd)). The next run of the key starts once the failed one has ended; if later runs depend on earlier ones, check for that in the step.
- **The weight function runs when a run is admitted** ([`Concurrency.Weight`](ds:block?id=concurrency-h4md62gd)). It is called once per run, in the worker that admits it (on SQL and Redis that may be another process than the one that enqueued it), so every client must register the same `Weight`, and it should be fast; a weight change applies to runs admitted after it.
- **Keys shared with a waiting run wait behind it** ([`ConcurrencyLimit`](ds:block?id=concurrencylimit-5anrp8rw)). With `ConcurrencyLimits`, a run that waits for one key also keeps later runs off its other keys, so a run is never starved; if most runs of a key wait on another, busy key, give that key more room or split the workflow.
- **A parked run under `HoldWhileRunning` may find its key taken** ([`ConcurrencyHold`](ds:block?id=concurrencyhold-k3v8qw2m)). When its backoff ends it waits for room like any queued run, so its retry can start later than the backoff says; with `HoldWholeRun` it starts on time but blocks the key meanwhile.
- **Concurrency caps runs, not workers** ([`Concurrency`](ds:block?id=concurrency-h4md62gd)). A capped bucket holds runs queued but uses no worker while it waits; how many steps run at once overall is the driver's worker count (`memory.WithWorkers`, `sqlstore.WithWorkers`, Hatchet `Config.Slots`).

## Related

- [Priority](enqueue.md#priority)
- [Debounce](../reliability/debounce.md) and [unique jobs](../reliability/unique-jobs.md), which decide whether a run is created at all
- [Capabilities](../concepts/capabilities.md)
- Example: [`examples/concurrency`](../../examples/concurrency)
- Example: [`examples/queue-weights`](../../examples/queue-weights) (FIFO per key)
- Example: [`examples/fair-share`](../../examples/fair-share) (round robin and weights)
