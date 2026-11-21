# accelerator-queue

One step of a workflow runs only on the machines with the hardware for it. Two SQL worker clients share one SQLite database, as two kinds of node share a store in production: the CPU node serves queue `cpu` with four workers, the GPU node serves queue `gpu` with one worker per card (`sqlstore.WithQueues`). The `photo` workflow runs on `cpu`; its `label` step calls the `infer` workflow, which lives on `gpu`, as a child run with `workflow.AwaitChild` and waits for its result without holding a CPU worker. The Journal plugin, on the SQL driver's durable journal store, remembers the spawned child so a snoozed step never spawns it twice.

```sh
cd examples
go run ./accelerator-queue
```

```text
photo p1: succeeded, result "label of p1"
photo p2: succeeded, result "label of p2"
photo p3: succeeded, result "label of p3"
resize on cpu-node: 3, resize on gpu-node: 0
model on gpu-node: 3, model on cpu-node: 0
model attempts at once on the GPU node: at most 1
```

The database is a temporary SQLite file, removed when the program ends; nothing else is needed.

The guide that copies this program is [named queues](../../docs/jobs/queues.md#run-one-step-on-an-accelerator).
