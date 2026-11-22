# Heavy steps and worker slots

A worker pool assumes every step attempt costs the same. A video render or a large import needs several times the memory or CPU of a webhook call, and eight renders on an eight-worker pool can take a machine down. `Step.Slots` says how many of the pool's worker slots one attempt of a step holds while it runs ([`workflow.SlotPool`](ds:block?id=slotpool-3rcwu7ku)), so a pool of eight runs a four-slot step beside at most four ordinary ones.

Reach for it when one process runs a mix of light and heavy steps and the heavy ones must not all run at once. When heavy work must never delay light work at all, give it its own queue and client instead (see [Pitfalls](#pitfalls)).

## Before you start

- `Step.Slots` above one needs [`workflow.CapSlotCost`](ds:block?id=capslotcost-9fatdg72) ([`Requires`](ds:block?id=requires-6tcjbcfj)). The memory and SQL drivers declare it ([`memory`](ds:block?id=memory-caps-yxkuq583), [`sqlstore`](ds:block?id=sql-paritycaps-e5y5vqhk)). The Redis driver does not ([`redis`](ds:block?id=redis-caps-bfgdst9t)): its workers claim whole runs and run a run's parallel steps beside them, so a worker is a run, not a step attempt. Hatchet does not either ([`hatchet`](ds:block?id=hatchet-caps-d6x9fv8z); its slots are fixed per worker process). Both refuse such a workflow like any missing capability: Redis at `Start` ([`Client.Start`](ds:block?id=redis-client-start-tmas9yaz)), Hatchet at `Register` ([`Client.Register`](ds:block?id=hatchet-client-register-zg59b2j4)).
- The pool is the client's `WithWorkers` size (`memory.WithWorkers`, `sqlstore.WithWorkers`). Per-queue caps (`QueueConfig.Workers`) and the cluster-wide `QueueConfig.Limit` still count attempts, not slots.
- The example is [`examples/step-slots`](../../examples/step-slots), on the in-memory driver. Run it with `go run ./step-slots` from `examples/`.

## Step 1 — Give a step a cost

Set `Slots` on the heavy step. Here the pool has four slots, the render holds three of them, and the webhook step leaves `Slots` at zero, which counts as one:

<!-- snippet: examples/step-slots/main.go#cost -->
```go
c := memory.New(memory.WithWorkers(poolSize))
render := workflow.Workflow{Name: "render", Steps: []workflow.Step{
	{Name: "encode", Slots: renderSlots, Run: encode}, // holds 3 of the 4 slots while it runs
}}
webhook := workflow.Workflow{Name: "webhook", Steps: []workflow.Step{
	{Name: "deliver", Run: deliver}, // zero means one slot
}}
for _, wf := range []workflow.Workflow{render, webhook} {
	if err := c.Register(wf); err != nil {
		return err
	}
}
```

While a render runs, one webhook attempt fits beside it; the others wait until the render gives its slots back. The program counts the slots its handlers hold:

```text
webhooks that ran beside the render: 1 of 3
most slots held at once: 4 of 4
```

`Slots` applies to run steps only ([`validateStep`](ds:block?id=validatestep-x6aebswc)). [`workflow.StepSlots(step)`](ds:block?id=stepslots-am5mgsbq) reads a step's cost the way the drivers do: `Slots`, with zero counted as one.

## Step 2 — Let Register catch a step that can never start

A step that needs more slots than the client's whole pool could never start, so `Register` refuses its workflow with an error wrapping `workflow.ErrInvalid`. The check is [`workflow.CheckSlots(wf, poolSize)`](ds:block?id=checkslots-3nwgkqa3), which drivers with `CapSlotCost` call at `Register` ([`memory`](ds:block?id=memory-client-register-ngcr2sck), [`sqlstore`](ds:block?id=sql-client-register-buvwt9ee)):

<!-- snippet: examples/step-slots/main.go#refuse -->
```go
huge := workflow.Workflow{Name: "transcode-8k", Steps: []workflow.Step{
	{Name: "transcode", Slots: poolSize + 1, Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }},
}}
err = c.Register(huge)
fmt.Fprintln(w, "register a 5-slot step on a 4-slot pool:", errors.Is(err, workflow.ErrInvalid))
fmt.Fprintln(w, " ", err)
```

```text
register a 5-slot step on a 4-slot pool: true
  workflow: invalid spec: workflow "transcode-8k" step "transcode" needs 5 worker slots and this client has 4
```

The check is per client: a workflow registered on a small worker process and a large one is refused only on the small one, so give every process that registers it a pool big enough.

## Step 3 — Know how attempts wait for slots

An attempt takes its slots after it is picked and gives them back when it ends. On the SQL driver it waits after its claim, with its lease renewed while it waits, so its run reads `running`, and a stop, a lost lease or a cancelled step ends the wait as it ends a running handler ([`Client.execute`](ds:block?id=sql-client-execute-5rxes7u5)). The memory driver takes the slots before the attempt counts as started, with the attempt already counted against its queue's cap so no other worker passes that cap meanwhile; only a stop of the client ends that wait ([`Client.worker`](ds:block?id=memory-client-worker-n9cqe2pc)).

Waiters are served first come, first served ([`SlotPool`](ds:block?id=slotpool-3rcwu7ku)): a heavy attempt at the head of the line is not passed by a stream of light ones, so it is never starved, at the price of light attempts waiting behind it until its slots are free.

## Step 4 — Use the shared pool in a driver of your own

[`workflow.SlotPool`](ds:block?id=slotpool-3rcwu7ku) is that rule as a type, and every driver with `CapSlotCost` uses it between claiming an attempt and running it. A driver of your own that declares the capability does the same: [`NewSlotPool(workers)`](ds:block?id=newslotpool-5td9h9hg), then [`Acquire(ctx, StepSlots(step))`](ds:block?id=slotpool-acquire-rbcbkhj5) before the handler and [`Release`](ds:block?id=slotpool-release-bchqsvcz) after it:

<!-- snippet: examples/step-slots/main.go#pool -->
```go
pool := workflow.NewSlotPool(poolSize)
cost := workflow.StepSlots(render.Steps[0]) // 3; a step with Slots 0 costs 1
if err := pool.Acquire(ctx, cost); err != nil {
	return err
}
fmt.Fprintf(w, "pool: %d of %d in use after one render\n", pool.InUse(), pool.Size())
short, stop := context.WithTimeout(ctx, 10*time.Millisecond) // long enough to show it waits, short for the demo
defer stop()
err = pool.Acquire(short, cost) // a second render must wait for the first
fmt.Fprintln(w, "pool: a second render waits until its context ends:", errors.Is(err, context.DeadlineExceeded))
pool.Release(cost)
fmt.Fprintf(w, "pool: %d in use after Release\n", pool.InUse())
fmt.Fprintln(w, "pool: asking for more than the pool:", errors.Is(pool.Acquire(ctx, poolSize+1), workflow.ErrInvalid))
```

```text
pool: 3 of 4 in use after one render
pool: a second render waits until its context ends: true
pool: 0 in use after Release
pool: asking for more than the pool: true
```

[`Acquire`](ds:block?id=slotpool-acquire-rbcbkhj5) returns the context's cause when the context ends first, and gives the slots back if they were granted at that same moment. Asking for more than `Size` is an error wrapping `ErrInvalid`, since it could never be served. The conformance suite has a case for it, [`workflowtest.RunSlotFlows`](ds:block?id=workflowtest-runslotflows-bndr8cke), which a driver declaring `CapSlotCost` should pass.

## Try it

```sh
cd examples
go run ./step-slots
```

```text
memory declares CapSlotCost: true
webhooks that ran beside the render: 1 of 3
most slots held at once: 4 of 4
register a 5-slot step on a 4-slot pool: true
  workflow: invalid spec: workflow "transcode-8k" step "transcode" needs 5 worker slots and this client has 4
pool: 3 of 4 in use after one render
pool: a second render waits until its context ends: true
pool: 0 in use after Release
pool: asking for more than the pool: true
```

The first line checks the capability. The next two show the pool at work: one of the three webhooks ran beside the three-slot render, and never more than four slots were held. Then `Register` refuses a step bigger than the pool, and the last four lines drive `SlotPool` by hand.

## Pitfalls

- **A heavy step at the head of the line holds back light attempts** until enough slots are free ([`SlotPool.Acquire`](ds:block?id=slotpool-acquire-rbcbkhj5)). Put heavy steps on their own queue and client when light work must never wait behind them.
- **`Slots` counts against one client's pool.** Two clients sharing a store each have their own pool ([`CheckSlots`](ds:block?id=checkslots-3nwgkqa3) is given that client's size), so two worker processes with eight slots each can run four four-slot renders between them.
- **Queue caps count attempts, not slots.** `QueueConfig.Workers: 2` lets two renders of four slots run at once if the pool has eight; size the pool and the cap together.
- **The started event can come before the slots on the SQL driver.** Its `LifecycleStarted` event is sent when the attempt is claimed, before it waits for slots ([`Client.execute`](ds:block?id=sql-client-execute-5rxes7u5)), so a dashboard built on lifecycle events may show a heavy attempt as started while it still waits.
- **Redis and Hatchet refuse the workflow.** A workflow with `Slots` above one needs [`CapSlotCost`](ds:block?id=capslotcost-9fatdg72), which neither declares, so it fails at `Start` on Redis ([`Client.Start`](ds:block?id=redis-client-start-tmas9yaz)) and at `Register` on Hatchet ([`Client.Register`](ds:block?id=hatchet-client-register-zg59b2j4)); keep such workflows on a memory or SQL client. See [Hatchet limitations](../drivers/hatchet.md#limitations).

## Related

- [Concurrency](concurrency.md): limits per key, which cap runs, not slots.
- [Named queues](queues.md): per-queue worker caps and a separate client for heavy work.
- [Capabilities](../concepts/capabilities.md): how a missing capability is refused.
- [Writing a driver](../extending/writing-a-driver.md): what a driver of your own declares and the conformance suite.
