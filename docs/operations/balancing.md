# Worker balancing

Fixed worker counts per queue suit a steady load. When one queue backs up (a nightly import, a campaign's emails) while another sits idle, the idle queue's workers do nothing and the busy one falls behind. A balancer moves workers between queues as their load changes, within the bounds you set. This page covers the strategies, bounds, the balance loop that applies them, and writing your own strategy.

## Before you start

- Named queues with a worker count each ([Named queues](../jobs/queues.md)): balancing moves workers between the queues one client serves.
- A driver that implements [`workflow.WorkerScaler`](ds:block?id=workerscaler-2pk27xz7) and declares `workflow.CapWorkerScale`: the [memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-batch2caps-9rm4b4ck) and [Redis](ds:block?id=redis-caps-bfgdst9t) drivers do. [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z) does not: its slots are fixed per worker process, so scale Hatchet by running more worker processes for the busy queue. See [Hatchet limitations](../drivers/hatchet.md#limitations).
- Balancing is off unless your app runs a balance loop ([`workflow.BalanceLoop`](ds:block?id=balanceloop-k9gtfh9v)). Nothing changes for an app that does not.
- The example is [`examples/worker-balancing`](../../examples/worker-balancing). Run it with `go run ./worker-balancing` from `examples/`.

## Step 1: give the client a pool and queues

The balancer sets each queue's worker cap inside the client's pool ([`memory.Client.SetWorkers`](ds:block?id=memory-client-setworkers-bgaqmm3w), [`sqlstore.Client.SetWorkers`](ds:block?id=sql-client-setworkers-ybeypsd4)). Give the pool at least the total the balancer shares, or a queue raised past it cannot run more than the pool:

<!-- snippet: examples/worker-balancing/main.go#client -->
```go
c := memory.New(
	memory.WithWorkers(8), // the pool: at least the balancer's total
	memory.WithQueues(
		workflow.QueueConfig{Name: "emails", Workers: 4},
		workflow.QueueConfig{Name: "imports", Workers: 4},
	),
)
```

[`WorkerScaler`](ds:block?id=workerscaler-2pk27xz7) has two methods. `QueueLoads(ctx)` returns one [`workflow.QueueLoad`](ds:block?id=queueload-xhk3w6np) per configured queue, sorted by name:

| Field | Meaning |
|---|---|
| `Queue` | The queue name |
| `Depth` | Attempts ready and waiting for a worker of this client; runs in retry backoff or delayed are not counted |
| `OldestWait` | How long the oldest of those has waited, on the driver's clock |
| `Workers` | The queue's current cap. A queue with no cap (`Workers: 0`) reports the pool size on the [memory](ds:block?id=memory-client-queueloads-zd7ys928) and [SQL](ds:block?id=sql-client-queueloads-aqfyk7w8) drivers |
| `Running` | Attempts of this queue executing now |

`SetWorkers(ctx, queue, n)` sets one queue's cap while the client runs ([`WorkerScaler`](ds:block?id=workerscaler-2pk27xz7)). `n` below 1 is refused with `workflow.ErrInvalid`, a queue the client was not given with `ErrNotFound`, and a call before `Start` with `ErrNotStarted`. Lowering a cap never interrupts running attempts: they finish, and no new ones start until the queue is under its cap. The loads and counts are this client's own. On the SQL driver `Depth` and `OldestWait` come from the due steps of this client's workflows, an uncapped queue reports the pool size ([`QueueLoads`](ds:block?id=sql-client-queueloads-aqfyk7w8)), and a count set by [`SetWorkers`](ds:block?id=sql-client-setworkers-ybeypsd4) lives in the client, not in the database: a restarted process starts from its `WithQueues` counts again.

## Step 2: pick a strategy

A [`workflow.Balancer`](ds:block?id=balancer-dqvct8jq) turns the loads into the worker count each queue should have. Two are built in:

- [`workflow.ByDepth(cfg)`](ds:block?id=bydepth-t5zsqjmu) shares by backlog: each queue's weight is `Depth + Running`, the work it has in hand.
- [`workflow.ByWait(cfg)`](ds:block?id=bywait-p7xf4vu8) shares by latency: each queue's weight is its `OldestWait` in seconds, so the queue falling furthest behind gets the most help.

<!-- snippet: examples/worker-balancing/main.go#strategies -->
```go
loads := []workflow.QueueLoad{
	{Queue: "emails", Depth: 2, Running: 1, Workers: 4, OldestWait: 3 * time.Second},
	{Queue: "imports", Depth: 40, Running: 4, Workers: 4, OldestWait: 9 * time.Second},
}
cfg := workflow.BalanceConfig{Total: 8} // eight workers to share; each queue keeps at least one
fmt.Printf("by depth: %s\n", show(workflow.ByDepth(cfg).Balance(loads)))
fmt.Printf("by wait:  %s\n", show(workflow.ByWait(cfg).Balance(loads)))
```

```text
by depth: emails 1, imports 7
by wait:  emails 2, imports 6
```

Both work the same way ([`proportional`](ds:block?id=proportional-xkxmwbxv)): every queue gets its minimum first, then the rest of `Total` is shared in proportion to the weight, never past a queue's maximum; what a capped queue cannot take goes to the others. When no queue has any weight the rest is shared evenly, and ties go to the queue whose name sorts first, so the answer is the same every time for the same loads.

## Step 3: set bounds and a step size

[`workflow.BalanceConfig`](ds:block?id=balanceconfig-6gmw877z) holds the budget and its limits:

| Field | Default | Meaning |
|---|---|---|
| `Total` | The sum of the queues' current `Workers` | Workers to share |
| `Bounds map[string]workflow.QueueBounds` | none | `Min` and `Max` per queue |
| `Default QueueBounds` | `Min` 1, `Max` `Total` | Bounds for a queue not in `Bounds` |
| `MaxShift` | no limit | The most one queue's count moves in one round, so counts change gradually while load swings |

A `Min` below 1 counts as 1: every queue keeps a worker, so no queue starves. A zero `Max` means `Total` ([`QueueBounds`](ds:block?id=queuebounds-vufjcscs), [`proportional`](ds:block?id=proportional-xkxmwbxv)).

<!-- snippet: examples/worker-balancing/main.go#bounds -->
```go
bounded := workflow.BalanceConfig{
	Total:    8,
	Bounds:   map[string]workflow.QueueBounds{"emails": {Min: 3}, "imports": {Max: 6}},
	MaxShift: 1, // move at most one worker per queue per round
}
fmt.Printf("bounded:  %s\n", show(workflow.ByDepth(bounded).Balance(loads)))
```

```text
bounded:  emails 3, imports 5
```

## Step 4: run the balance loop

[`workflow.BalanceLoop`](ds:block?id=balanceloop-k9gtfh9v) reads the loads, asks the balancer and sets every count that changed, on an interval:

<!-- snippet: examples/worker-balancing/main.go#loop -->
```go
loop := workflow.BalanceLoop{
	Scaler:   c, // the memory and SQL drivers implement workflow.WorkerScaler
	Balancer: workflow.ByDepth(workflow.BalanceConfig{Total: 8}),
	Every:    10 * time.Second,
	Clock:    balanceClock, // nil means the real clock
}
go func() { _ = loop.Run(ctx) }() // stops when ctx ends
```

```text
before: emails workers 4 running 0 waiting 0; imports workers 4 running 4 waiting 8
after one round: emails workers 1 running 0 waiting 0; imports workers 7 running 7 waiting 5
```

- `Every` defaults to [`workflow.DefaultBalanceEvery`](ds:block?id=defaultbalanceevery-sarudmvt) (5 seconds).
- `Clock` drives the interval; the example uses a fake clock so its round happens when the program says.
- [`Run(ctx)`](ds:block?id=balanceloop-run-vmg7hk7w) blocks until the context ends and then returns nil. Run it on the context you pass to `Start`, so it stops with the client. It refuses at once when `Scaler` or `Balancer` is missing.
- [`Once(ctx)`](ds:block?id=balanceloop-once-xuapjqya) runs one round, for a loop you drive yourself (from a cron schedule, or an admin action). A count it cannot set does not stop the others; the failures come back joined.
- A failed round goes to `OnError`, or is logged with `Logger` (default `slog.Default()`), and the loop goes on ([`Run`](ds:block?id=balanceloop-run-vmg7hk7w)).

## Your own strategy

[`Balancer`](ds:block?id=balancer-dqvct8jq) is one method, `Balance(loads []QueueLoad) map[string]int`, and [`workflow.BalancerFunc`](ds:block?id=balancerfunc-xamyydda) turns a function into one. A queue left out of the answer keeps its count ([`Once`](ds:block?id=balanceloop-once-xuapjqya)). For a different weight with the same minimum, maximum and step rules, use [`workflow.Proportional(cfg, weight)`](ds:block?id=proportional-kqg4wfge): for example, weight by `Depth` times an expected job duration you keep per queue, or give a paying tenant's queue double weight. A strategy can also read signals outside the loads (time of day, a feature flag) since it is your code.

## Try it

```sh
cd examples
go run ./worker-balancing
```

```text
by depth: emails 1, imports 7
by wait:  emails 2, imports 6
bounded:  emails 3, imports 5
before: emails workers 4 running 0 waiting 0; imports workers 4 running 4 waiting 8
after one round: emails workers 1 running 0 waiting 0; imports workers 7 running 7 waiting 5
```

The first three lines are the strategies of Steps 2 and 3 applied to the same loads. The last two are a real memory client: twelve imports arrive while emails sit idle, so before balancing four imports run and eight wait behind the cap of four; after one round of the loop, `ByDepth` has moved three workers from emails to imports, seven imports run and five wait. `main_test.go` in the example checks every line.

## Pitfalls

- **The pool is the ceiling.** On the [memory](ds:block?id=memory-client-setworkers-bgaqmm3w), [SQL](ds:block?id=sql-client-setworkers-ybeypsd4) and [Redis](ds:block?id=redis-client-setworkers-d4dn726u) drivers, a queue's cap lives inside the pool set by `WithWorkers`. A balancer total above the pool size gives caps the pool cannot fill.
- **Per process, not per fleet.** Each client balances its own workers from its own view of the loads ([`WorkerScaler`](ds:block?id=workerscaler-2pk27xz7), [`sqlstore.Client.SetWorkers`](ds:block?id=sql-client-setworkers-ybeypsd4)). Several processes each running a loop on one SQL database balance independently; give each the same config and they move the same way.
- **Only configured queues are balanced.** Queues not given to `WithQueues` are not in the loads and keep sharing the pool ([`memory.Client.QueueLoads`](ds:block?id=memory-client-queueloads-zd7ys928)).
- **Failover balances the primary.** A failover client's [`QueueLoads`](ds:block?id=failover-client-queueloads-udgpt8pz) and `SetWorkers` reach its primary; run a second loop on the secondary client if it serves queues too.
- **Fast swings.** A short `Every` with no [`MaxShift`](ds:block?id=balanceconfig-6gmw877z) can move workers back and forth as one burst arrives and drains. Set `MaxShift`, or a longer interval.

## Related

- [Named queues](../jobs/queues.md): the queues and fixed worker counts this builds on.
- [Admin operations](../admin/operations.md): queue depth and wait for operators, and alerts on them.
- [Tuning](tuning.md): the other knobs.
