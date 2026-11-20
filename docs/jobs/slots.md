# Heavy steps and worker slots

A worker pool assumes every step attempt costs the same. A video render or a large import needs several times the memory or CPU of a webhook call, and eight renders on an eight-worker pool can take a machine down. `Step.Slots` says how many of the pool's worker slots one attempt of a step holds while it runs, so a pool of eight runs a four-slot step beside at most four ordinary ones.

## Before you start

- `Step.Slots` above one needs `workflow.CapSlotCost`. The memory and SQL drivers declare it. The Redis driver does not: its workers claim whole runs and run a run's parallel steps beside them, so a worker is a run, not a step attempt. Hatchet does not either (its slots are fixed per worker process). Both refuse such a workflow at `Start`, like any missing capability.
- The pool is the client's `WithWorkers` size. Per-queue caps (`QueueConfig.Workers`) and the cluster-wide `QueueConfig.Limit` still count attempts, not slots.

## Give a step a cost

```go
wf := workflow.Workflow{Name: "render", Steps: []workflow.Step{
	{Name: "fetch", Run: fetch},
	{Name: "encode", After: []string{"fetch"}, Slots: 4, Run: encode},
}}
```

Zero and one mean one slot. A step that needs more slots than the client's whole pool could never start, so `Register` refuses it with an error wrapping `workflow.ErrInvalid` (`workflow.CheckSlots`).

## How attempts wait for slots

An attempt takes its slots after it is picked (on the SQL driver, after its claim, with its lease renewed while it waits) and gives them back when it ends. Waiters are served first come, first served: a heavy attempt at the head of the line is not passed by a stream of light ones, so it is never starved, at the price of light attempts waiting behind it until its slots are free. An attempt waiting for slots counts as running.

`workflow.SlotPool` is the shared rule (`NewSlotPool`, `Acquire`, `Release`, `InUse`); a driver of your own that declares `CapSlotCost` uses it the same way, and `workflow.StepSlots` reads a step's cost.

## Pitfalls

- A heavy step at the head of the line holds back light attempts until enough slots are free. Put heavy steps on their own queue and client when light work must never wait behind them.
- `Slots` counts against one client's pool. Two clients sharing a store each have their own pool.
