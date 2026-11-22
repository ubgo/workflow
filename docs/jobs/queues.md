# Named queues

A named queue separates kinds of work so one kind cannot starve another: password-reset emails should not wait behind ten thousand newsletter sends, and a slow report should not take every worker. Each run belongs to one queue; workers serve queues with a priority and a cap per queue; and an operator can pause one queue while the rest keep running. This page covers a workflow's default queue, sending one run elsewhere, configuring how workers serve queues, capping a queue across every worker, sharing a worker between queues by weight, pausing a queue, and what each driver supports.

## Before you start

- Queues need `workflow.CapQueue`, and sending one run to a queue other than its workflow's needs `workflow.CapQueueOverride` ([`CheckJobQueue`](ds:block?id=checkjobqueue-nkkxjr77)). The [memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-paritycaps-e5y5vqhk) and [Redis](ds:block?id=redis-caps-bfgdst9t) drivers have both; on [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z) see [Pitfalls](#pitfalls). A cap across every worker needs `workflow.CapQueueLimitGlobal`, which the [memory](ds:block?id=memory-caps-yxkuq583) and [SQL](ds:block?id=sql-caps-yu37ef2j) drivers declare.
- The example is [`examples/named-queues`](../../examples/named-queues). It runs on the memory driver with one worker, so the order the steps run in is exactly the order the queues hand them out. Run it with `go run ./named-queues` from `examples/`.

## Step 1: give a workflow its queue

`Workflow.Queue` is the queue every run of the workflow goes to. Empty means [`workflow.DefaultQueue`](ds:block?id=defaultqueue-4qxqag88) (`"default"`), so code written before queues existed keeps working unchanged.

<!-- snippet: examples/named-queues/main.go#workflows -->
```go
emails := workflow.Workflow{Name: "send-email", Queue: "mail", Steps: []workflow.Step{{Name: "send", Run: step}}}
reports := workflow.Workflow{Name: "build-report", Queue: "reports", Steps: []workflow.Step{{Name: "build", Run: step}}}
```

A queue name is 1 to 64 characters of letters, digits and `.`, `_`, `:` or `-`, so every engine and SQL index accepts it; `Register` refuses anything else with `workflow.ErrInvalid` ([`ValidateQueueName`](ds:block?id=validatequeuename-wv597ytz)). A workflow on a queue other than the default needs `CapQueue` ([`Requires`](ds:block?id=requires-6tcjbcfj)), and a driver without it refuses the workflow at `Start`.

## Step 2: decide how workers serve queues

How many workers serve which queue is a deployment decision, not part of the workflow, so it is a driver option. Every driver takes the same [`workflow.QueueConfig`](ds:block?id=queueconfig-mvvmr2y9) values:

<!-- snippet: examples/named-queues/main.go#client -->
```go
c := memory.New(
	memory.WithWorkers(1),
	memory.WithQueues(
		workflow.QueueConfig{Name: "urgent", Priority: 10}, // drains first
		workflow.QueueConfig{Name: "mail", Priority: 5},
		workflow.QueueConfig{Name: "reports", Workers: 1}, // at most one report at a time
	),
)
```

- `Name` is the queue.
- [`Workers`](ds:block?id=queueconfig-mvvmr2y9) caps how many step attempts of that queue run at once on this client. Zero means no cap beyond the driver's pool size.
- [`Limit`](ds:block?id=queueconfig-mvvmr2y9) caps how many step attempts of that queue run at once across every client sharing the store, not only this one (see [Cap a queue across every worker](#cap-a-queue-across-every-worker)). Zero means no cluster cap.
- [`Priority`](ds:block?id=queueconfig-mvvmr2y9) orders queues when a worker frees up: the ready attempts of a higher queue are always taken before a lower one's. Equal priorities fall back to `Job.Priority`, then arrival.

A client given no [`QueueConfig`](ds:block?id=queueconfig-mvvmr2y9) serves every queue from one pool, as before. A client given some serves only those. On a shared store (the SQL driver, Hatchet) runs of other queues wait for a client that serves them, which is how you run one process per queue. The memory driver is one process, so a run on a queue it does not serve could never start: it refuses such a workflow at [`Start`](ds:block?id=memory-client-start-umc68snk) and such a job at [`Enqueue`](ds:block?id=memory-client-enqueuelocked-z3x2cytk), both with `ErrInvalid`.

## Step 3: send one run elsewhere

`Job.Queue` overrides the workflow's queue for one run. It is fixed at enqueue: retries and later steps of that run stay on it.

<!-- snippet: examples/named-queues/main.go#enqueue -->
```go
refs := make([]workflow.RunRef, 0, 3)
for _, job := range []workflow.Job{
	{Workflow: "build-report", Payload: []byte("monthly")},
	{Workflow: "send-email", Payload: []byte("newsletter")},
	{Workflow: "send-email", Payload: []byte("password-reset"), Queue: "urgent"}, // this one run jumps the line
} {
	ref, err := c.Enqueue(ctx, job)
	if err != nil {
		return err
	}
	refs = append(refs, ref)
}
```

[`workflow.EffectiveQueue`](ds:block?id=effectivequeue-uzpgk6se)`(wf, job)` is the rule every driver applies: `Job.Queue`, else `Workflow.Queue`, else the default. A step reads its run's queue from `workflow.StepInfo.Queue`, and the OpenTelemetry middleware records it on every span and metric.

## Run one step on an accelerator

A queue is also how work reaches the machines that have the hardware for it, such as a GPU. A queue belongs to a run, not to a step, so put the step that needs the accelerator in a workflow of its own on that queue, and call it from the main workflow as a child run. The program is [`examples/accelerator-queue`](../../examples/accelerator-queue): two SQL worker clients share one SQLite database, the way a CPU node and a GPU node share a store.

Each kind of node starts its client with only the queues it serves. The GPU node has one worker for its one card, so the model never shares it:

<!-- snippet: examples/accelerator-queue/main.go#nodes -->
```go
cpu, err := sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithPollInterval(poll),
	sqlstore.WithQueues(workflow.QueueConfig{Name: cpuQueue, Workers: cpuWorkers}))
if err != nil {
	return err
}
gpu, err := sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithPollInterval(poll),
	sqlstore.WithQueues(workflow.QueueConfig{Name: gpuQueue, Workers: gpuWorkers})) // one attempt per card
if err != nil {
	return err
}
```

The step that needs the card is the whole of the `infer` workflow, on queue `gpu`; the `photo` workflow stays on `cpu` and its `label` step waits for `infer` with `workflow.AwaitChild`:

<!-- snippet: examples/accelerator-queue/main.go#workflows -->
```go
infer := workflow.Workflow{Name: "infer", Queue: gpuQueue, Steps: []workflow.Step{{Name: "model", Run: runModel}}}
photo := workflow.Workflow{Name: "photo", Queue: cpuQueue, Steps: []workflow.Step{
	{Name: "resize", Run: resize},
	{Name: "label", After: []string{"resize"}, Run: func(ctx context.Context, in []byte) ([]byte, error) {
		// Spawns infer once (the Journal remembers it), then snoozes
		// until it ends: the CPU worker is free while the GPU works.
		return workflow.AwaitChild(ctx, c, "label", workflow.Job{Workflow: "infer", Payload: in},
			workflow.ChildOptions{Poll: poll})
	}},
}}
```

[`AwaitChild`](ds:block?id=awaitchild-bk9f29f4) spawns the child once and then snoozes the parent step until the child ends, so the CPU worker is free while the GPU works. It needs the Journal plugin (it remembers the spawned child, so a snoozed or retried step never spawns it twice) and `CapSnooze` on the parent's driver. On a shared store, use a durable journal that every CPU node reads, such as the SQL driver's own:

<!-- snippet: examples/accelerator-queue/main.go#journal -->
```go
cpuClient, err := workflow.Use(cpu, workflow.Journal(journal)) // AwaitChild needs the Journal plugin
if err != nil {
	return err
}
```

```text
photo p1: succeeded, result "label of p1"
photo p2: succeeded, result "label of p2"
photo p3: succeeded, result "label of p3"
resize on cpu-node: 3, resize on gpu-node: 0
model on gpu-node: 3, model on cpu-node: 0
model attempts at once on the GPU node: at most 1
```

Every node registers both workflows, so any node can enqueue either, but each claims only the steps of the queues in its `WithQueues`. The same layout works on Redis, whose workers also serve the queues they are given. Hatchet maps each queue to a worker of its own too ([`workerPools`](ds:block?id=hatchet-workerpools-2t59fpep)), but it does not declare `CapSnooze` ([Hatchet](ds:block?id=hatchet-caps-d6x9fv8z)), so [`AwaitChild`](ds:block?id=awaitchildren-ddr2cfgz) refuses there with a permanent `UnsupportedError` rather than spend an attempt on every look. In one process, `memory.WithQueues` with `Workers: 1` on the GPU queue keeps the card to one job at a time while the CPU queue runs several.

## Step 4: pause a queue

`Pause` with [`workflow.PauseQueue`](ds:block?id=pausequeue-xg7ky8xh)`(name)` holds the runs on that queue without cancelling them. No step attempt of a held run is claimed, attempts already running finish, and enqueues are still accepted; the held runs report `queued` until `Resume` ([`Pauser`](ds:block?id=pauser-5p4t8yg6)).

<!-- snippet: examples/named-queues/main.go#pause -->
```go
if err := c.Pause(ctx, workflow.PauseQueue("mail")); err != nil {
	return err
}
held, err := c.Enqueue(ctx, workflow.Job{Workflow: "send-email", Payload: []byte("digest")})
if err != nil {
	return err
}
report, err := c.Enqueue(ctx, workflow.Job{Workflow: "build-report", Payload: []byte("weekly")})
if err != nil {
	return err
}
fmt.Println(<-ran) // the report runs; the paused mail queue holds the digest
if err := waitDone(ctx, c, report); err != nil {
	return err
}
st, err := c.Status(ctx, held)
if err != nil {
	return err
}
fmt.Println("digest while mail is paused:", st)
if err := c.Resume(ctx, workflow.PauseQueue("mail")); err != nil {
	return err
}
```

Pausing a queue needs `workflow.CapPauseQueue`; pausing a workflow or everything needs `workflow.CapPauseWorkflow` ([`PauseTarget`](ds:block?id=pausetarget-2g5zg3um)). Both are covered in [manage runs](manage-runs.md#pause-and-resume).

## Cap a queue across every worker

`Workers` is a cap per client: three processes each serving `imports` with `Workers: 3` run up to nine imports at once. When the thing you protect is shared (a partner API that allows three calls at a time, a database that tolerates three bulk loads), set [`QueueConfig.Limit`](ds:block?id=queueconfig-mvvmr2y9), the queue's ceiling for the whole cluster. The attempts of the queue in flight on every client that shares the store are counted against it, and a worker that finds the queue full leaves the run waiting for the next free slot. The two caps combine: a client runs at most `Workers` attempts of the queue, and the cluster at most `Limit`.

The memory driver is one process, so the process is the whole cluster and `Limit` is a plain cap there:

<!-- snippet: examples/queue-limit/main.go#memory -->
```go
c := memory.New(
	memory.WithWorkers(8),
	memory.WithQueues(workflow.QueueConfig{Name: "imports", Limit: importLimit}),
)
```

On the SQL driver the limit holds across nodes. Declare the same `Limit` on every client that serves the queue:

<!-- snippet: examples/queue-limit/main.go#sql -->
```go
// Two clients over one database: two nodes. Each has four workers, so
// eight attempts could run; Limit lets only three of the queue run at
// once across both.
newNode := func() (*sqlstore.Client, error) {
	db, err := driversqlite.Open(path)
	if err != nil {
		return nil, err
	}
	return sqlstore.New(db, sqlb.SQLiteDialect{},
		sqlstore.WithWorkers(4),
		sqlstore.WithPollInterval(50*time.Millisecond),
		sqlstore.WithQueues(workflow.QueueConfig{Name: "imports", Workers: 4, Limit: importLimit}),
	)
}
```

The example runs nine one-step imports on that queue, first on the memory driver and then on two SQL clients over one SQLite file, each with room for four workers. It counts the attempts running at once and checks the peak:

```text
memory: peak within limit 3: true
memory: peak reached the limit: true
sqlstore: queue limit is cluster-wide: true
sqlstore: peak within limit 3: true
sqlstore: peak reached the limit: true
```

The rules:

- A slot is held from the moment a step attempt is claimed until its outcome is written. A node that dies mid-attempt does not leak its slot ([`limitSQL`](ds:block?id=sql-client-limitsql-ncv3ctgs) on SQL): it frees when that attempt's lease expires and another node can claim the step again.
- [`Limit`](ds:block?id=queueconfig-mvvmr2y9) counts step attempts, not runs. A run with two parallel steps on the queue holds two slots.
- A client that declares no [`Limit`](ds:block?id=queueconfig-mvvmr2y9) for the queue does not enforce one, though its attempts still count toward the others' limit. Give every client that serves the queue the same `QueueConfig`.
- A negative `Limit` fails validation with `ErrInvalid` ([`ValidateQueues`](ds:block?id=validatequeues-7c2dyk85)).
- A driver that cannot count a queue's attempts across clients does not declare `workflow.CapQueueLimitGlobal`, and refuses a non-zero `Limit` at `Start` with an `UnsupportedError` (field `QueueConfig.Limit`) rather than enforce a per-client cap under the name of a cluster one. The [memory](ds:block?id=memory-caps-yxkuq583) and [SQL](ds:block?id=sql-caps-yu37ef2j) drivers declare it; [Redis](ds:block?id=redis-caps-bfgdst9t) and [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z) refuse (see [Pitfalls](#pitfalls)). [`workflow.CheckQueueLimits`](ds:block?id=checkqueuelimits-ehvp2923)`(caps, queues)` is the check a driver calls.

The program is [`examples/queue-limit`](../../examples/queue-limit). Run it with `go run ./queue-limit` from `examples/`.

## Share a worker between queues by weight

Priority is strict: while the higher queue has ready work, the lower one waits. Often the goal is a share instead: exports should get most of the turns, but imports must keep moving. [`QueueConfig.Weight`](ds:block?id=queueconfig-mvvmr2y9) does that among queues of equal `Priority`. When several of them have ready work, a free worker draws one at random in proportion to the weights, so a queue of weight 3 gets about three turns for every one a queue of weight 1 gets, and neither starves:

<!-- snippet: examples/queue-weights/main.go#weights -->
```go
c := memory.New(
	memory.WithWorkers(1),
	memory.WithJitterSource(draw), // the weighted draw uses it; production leaves the default
	memory.WithQueues(
		workflow.QueueConfig{Name: "exports", Weight: 3}, // about three turns in four
		workflow.QueueConfig{Name: "imports", Weight: 1}, // still gets one in four
	),
)
```

The example queues eight exports and eight imports behind one busy worker. Its random source is seeded, so the draws are the same on every run:

```text
weighted (E = export, I = import):
  E1 E2 E3 E4 I1 E5 I2 E6
  I3 E7 E8 I4 I5 I6 I7 I8
```

Of the first eleven turns, while both queues had work, exports took eight and imports three; once the exports ran out, the imports had the worker to themselves.

The rules:

- The draw happens only when at least two queues of the same priority have ready work and at least one of them has a weight ([`weightedLocked`](ds:block?id=memory-client-weightedlocked-q4unsb6m) on memory). With all weights zero (the default) the order stays strict: `Job.Priority`, then arrival.
- A queue with no weight among weighted queues counts as weight 1 ([`PickWeighted`](ds:block?id=pickweighted-utkqpnbx)), and a negative weight fails validation with `ErrInvalid` ([`ValidateQueues`](ds:block?id=validatequeues-7c2dyk85)).
- `Priority` still comes first: weights share turns within one priority level, never across levels.
- The draw comes from the driver's random source, the one it uses for retry jitter (`memory.WithJitterSource`), so a test can pin it as the example does. [`workflow.PickWeighted`](ds:block?id=pickweighted-utkqpnbx)`(weights, r)` is the rule every driver applies; the memory driver lists the queues in name order before drawing ([`weightedLocked`](ds:block?id=memory-client-weightedlocked-q4unsb6m)).

Weights need a driver that orders queues on one worker. The [memory](ds:block?id=memory-client-weightedlocked-q4unsb6m), [Redis](ds:block?id=redis-client-queueargs-nt5jgqdy) and [SQL](ds:block?id=sql-client-claimweighted-vdd4grru) drivers do; Hatchet refuses a non-zero `Weight` ([`checkQueues`](ds:block?id=hatchet-checkqueues-xebdznbs)). A driver that cannot refuses a non-zero `Weight` at `Start`.

## Try it

```sh
cd examples
go run ./named-queues
```

```text
build-report hold on reports
send-email password-reset on urgent
send-email newsletter on mail
build-report monthly on reports
build-report weekly on reports
digest while mail is paused: queued
send-email digest on mail
enqueue to a queue this client does not serve: workflow: invalid spec: queue "sms" is not served by this client (WithQueues); the run would never start
```

The first report holds the only worker while three more runs queue. When it ends, the `urgent` run goes first (priority 10), then `mail` (5), then `reports`.

## Pitfalls

- **Hatchet binds a workflow to the workers that registered it.** The Hatchet driver maps a queue to its own Hatchet worker with `Slots` set to `Workers` ([`workerPools`](ds:block?id=hatchet-workerpools-2t59fpep)), so `Workflow.Queue` and per-queue caps work, but a `Job.Queue` that differs from the workflow's is refused (no `CapQueueOverride`: [`CheckJobQueue`](ds:block?id=checkjobqueue-nkkxjr77)), and so is a non-zero `QueueConfig.Priority` ([`checkQueues`](ds:block?id=hatchet-checkqueues-xebdznbs)), since separate workers cannot be ordered against each other. Check `Capabilities()` before relying on either. `Workers: 0` falls back to `Config.Slots`, and a workflow whose queue this process does not list gets no worker here. A workflow reaches the Hatchet engine only when some worker registers it, so an `Enqueue` of a workflow whose queue no process has ever served fails: the engine does not know it yet. See [Hatchet limitations](../drivers/hatchet.md#limitations).
- **A child on an accelerator queue waits for a node that serves it.** If every GPU node is down, the `infer` children queue and their parents keep snoozing ([`AwaitChild`](ds:block?id=awaitchild-bk9f29f4)); nothing fails until a run timeout or the parent's own limits say so. Alert on the age of the oldest run on the GPU queue.
- **A queue no client serves never runs.** On a shared store nothing warns you: the runs wait ([`QueueConfig`](ds:block?id=queueconfig-mvvmr2y9)). List them with `Runs` filtered by `Queue` (see [manage runs](manage-runs.md)).
- **`Workers` is per client.** Three processes each serving `reports` with `Workers: 1` run up to three reports at once ([`QueueConfig.Workers`](ds:block?id=queueconfig-mvvmr2y9)). A cap across every process is `QueueConfig.Limit` for the whole queue (see [Cap a queue across every worker](#cap-a-queue-across-every-worker)), or a concurrency limit on one workflow (`Workflow.Concurrency`).
- **Redis and Hatchet refuse `Limit`.** A Redis worker claims a run whole, so the driver keeps no count of a queue's in-flight step attempts across clients, and Hatchet has slots per worker and a limit per workflow but nothing for a whole queue. Both refuse a non-zero `Limit` at `Start` ([`CheckQueueLimits`](ds:block?id=checkqueuelimits-ehvp2923), called by [Redis](ds:block?id=redis-caps-bfgdst9t) and [Hatchet](ds:block?id=hatchet-checkqueues-xebdznbs)); use `Workers` for a per-client cap, or `Workflow.Concurrency` for a limit across processes.
- **Priority is strict.** A busy high-priority queue ([`Priority`](ds:block?id=queueconfig-mvvmr2y9)) starves the lower ones for as long as it has ready work; give the queues the same priority and a `Weight` each if the low one must make progress, or give it its own process.
- **A weight is a share of turns, not of time** ([`PickWeighted`](ds:block?id=pickweighted-utkqpnbx)). A queue of weight 1 whose steps take a minute can still hold the worker longer than a queue of weight 3 whose steps take a second.

## Related

- [Manage runs](manage-runs.md): list, retry, delete, cancel in bulk, pause.
- [Concurrency and rate limits](concurrency.md): caps across every worker.
- Example: [examples/queue-limit](../../examples/queue-limit/) (a queue capped across two SQL nodes).
- [Capabilities](../concepts/capabilities.md): `CapQueue`, `CapQueueOverride`, `CapQueueLimitGlobal`, `CapPauseQueue`.
- Example: [examples/queue-weights](../../examples/queue-weights/) (weighted queues).
