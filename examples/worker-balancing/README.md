# worker-balancing

Workers moved between named queues as their load changes. First the two built-in strategies on fixed loads (`workflow.ByDepth` shares workers by backlog, `workflow.ByWait` by how long the oldest job has waited), then per-queue bounds and a cap on how far one round moves, then a `workflow.BalanceLoop` on the in-memory driver that takes three workers from an idle email queue and gives them to a backed-up import queue.

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

Each queue first gets its minimum (one unless set), then the rest of the budget is shared by weight: depth plus running for `ByDepth`, seconds of the oldest wait for `ByWait`. In the bounded line, emails must keep three workers and imports may have at most six, and `MaxShift: 1` stops either queue moving more than one worker from its current four. The loop runs on a fake clock, so its first round happens when the program advances it. Raising the import cap lets three more of its waiting jobs start at once.

Balancing is off unless an app runs a loop. The SQL driver implements the same `workflow.WorkerScaler`; Hatchet does not, because its slots are fixed per worker process.

The guide that copies this program is [worker balancing](../../docs/operations/balancing.md). `go test ./worker-balancing` checks every line.
