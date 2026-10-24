# Hybrid placement

A common shape: the crons live in the app's code and fire in its own process, while the jobs they start must survive a restart and run on whichever worker is free. Package `hybrid` builds that from two clients: `hybrid.New(scheduler, jobs)` is one `workflow.Client` whose schedules fire on `scheduler` (usually the in-process memory driver) and whose runs live on `jobs` (a durable driver). App code registers, schedules and enqueues on it as on any client.

## Before you start

- Two clients, neither started. The jobs client is the one your runs need: durable, with the capabilities your workflows use. The scheduler client needs `CapCron`.
- `hybrid` is in the core module: `import "github.com/ubgo/workflow/hybrid"`.
- The example is [`examples/hybrid-placement`](../../examples/hybrid-placement). It uses two memory clients so it runs anywhere, the scheduler on a fake clock. Run it with `go run ./hybrid-placement` from `examples/`.

## Split schedules from runs

<!-- snippet: examples/hybrid-placement/main.go#new -->
```go
scheduler := memory.New(memory.WithClock(clk), memory.WithNodeID("sched")) // in process: fires the crons
jobs := memory.New(memory.WithNodeID("jobs"))                              // in an app: sqlstore or redis
c := hybrid.New(scheduler, jobs)

err := c.Register(workflow.Workflow{
	Name:         "nightly-report",
	Cron:         []string{"0 2 * * *"},
	CronLocation: time.UTC,
	Steps: []workflow.Step{{Name: "build", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		ran <- info
		return nil, nil
	}}},
})
if err != nil {
	return err
}
if err := c.Start(ctx); err != nil { // starts jobs, then the scheduler
	return err
}
```

`Register` registers the workflow twice. On the jobs client it goes without its schedule fields (`Cron`, `CronLocation`, `Misfire`, `CronWindows`, `ScheduleWhen`, `CronSeconds`, `CronActive`), so the jobs client never fires it. On the scheduler client it goes as a forwarder of the same name: one step that enqueues the real job onto the jobs client with the fired run's payload, labels (schedule ID, tick time) and tenant. `Start` starts the jobs client first, then the scheduler; cancelling the context stops both.

Everything else goes to the jobs client: `Enqueue`, `Cancel`, `Status`, `Signal`, `OnRunFinished`, and every optional interface (`Runs`, `Retry`, `Delete`, `Pause`, lifecycle events and the rest). `Schedule`, `Unschedule`, `Schedules`, `PauseSchedule` and `ResumeSchedule` go to the scheduler client:

<!-- snippet: examples/hybrid-placement/main.go#runs -->
```go
page, err := c.Runs(ctx, workflow.RunQuery{Workflow: "nightly-report"}) // run queries go to the jobs client
if err != nil {
	return err
}
fmt.Fprintln(out, "runs on the jobs client:", len(page.Runs))
```

## What it can do

`Capabilities()` is the jobs client's, with the schedule family taken from the scheduler client: `CapCron`, `CapCronZone`, the three misfire policies, `CapScheduleWhen`, `CapScheduleWindow`, `CapSchedulePause`, `CapScheduleList`, `CapCronSeconds`, `CapScheduleActive` and `CapTickTime`. Five schedule capabilities are declared by neither, because on the scheduler they would describe the forwarder runs, not the jobs: `CapOverlapSkip`, `CapScheduleKeep`, `CapScheduleHistory`, `CapScheduleTrigger` and `CapTickEvents`. So the split refuses, with an `*workflow.UnsupportedError`:

- `Workflow.CronOverlap` and `Schedule.Overlap` set to `OverlapSkip` (the scheduler sees only its forwarder runs);
- `Workflow.CronKeepRuns` and `Schedule.KeepRuns` (the jobs client keeps the runs: use its retention);
- `TriggerSchedule` (it would return the forwarder's run) and `OnTick` (ticks would name forwarder runs).

<!-- snippet: examples/hybrid-placement/main.go#refused -->
```go
err = c.Register(workflow.Workflow{
	Name: "sync", Cron: []string{"*/5 * * * *"}, CronLocation: time.UTC,
	CronOverlap: workflow.OverlapSkip, // the scheduler would see only its forwarder runs
	Steps:       []workflow.Step{{Name: "run", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}},
})
fmt.Fprintln(out, "CronOverlap refused:", errors.Is(err, workflow.ErrUnsupported))
_, err = c.TriggerSchedule(ctx, "cron:nightly-report:0")
fmt.Fprintln(out, "TriggerSchedule refused:", errors.Is(err, workflow.ErrUnsupported))
```

## Several nodes

Each process runs its own scheduler client, so a schedule fires once per process, not once per cluster. Two ways to live with that:

- **One process schedules.** Only one node builds the hybrid client; the others use the jobs client alone.
- **Every node schedules, and the key absorbs the duplicates.** When the jobs client declares `CapIdempotencyKey`, the forwarder enqueues with the key `hybrid/<schedule ID>/<tick time>`, so every node firing the same tick makes one run while the first is unfinished. A tick fired after that run finished starts another one, so this fits jobs that end well before the next node's clock reaches the tick.

For a schedule that must fire exactly once per cluster, schedule on the durable driver itself (its ticks are decided in the store), or give the scheduler role to one process ([Nodes and health › Scheduler-only and worker-only processes](../operations/nodes.md#scheduler-only-and-worker-only-processes)).

The forwarder step retries up to five times with a backoff from one second to 30 seconds, so a jobs backend that is briefly unreachable does not lose a tick; the same key keeps a retried forwarder from starting the job twice.

## Try it

```sh
cd examples
go run ./hybrid-placement
```

```text
cron: true (scheduler)  run query: true (jobs)  trigger: false
tick 02:00 ran on node "jobs", schedule cron:nightly-report:0
runs on the jobs client: 1
CronOverlap refused: true
TriggerSchedule refused: true
```

The 02:00 tick fires on the scheduler client's fake clock and the step runs on the jobs client, with the schedule's ID and the tick time on the run. `go test ./hybrid-placement` checks every line.

## Pitfalls

- **An in-process scheduler forgets on restart.** Run-time schedules made with `Client.Schedule` on a memory scheduler are lost when the process stops; declare recurring work as `Workflow.Cron` so every boot restores it, or put the schedules on the durable driver.
- **Missed ticks follow the scheduler's misfire policy.** Ticks due while the process was down are caught up (or not) by the memory driver's rules, and only from what it still knows: nothing survives a restart.
- **Without `CapIdempotencyKey` on the jobs client, duplicates are yours.** The forwarder then sends no key, and a retried forwarder or a second node can start the job twice.
- **Overlap control belongs to the jobs side.** Since `OverlapSkip` is refused, use a [unique job](../reliability/unique-jobs.md) or a concurrency key on the workflow to keep runs from overlapping.
- **Two clients, two sets of options.** Middleware, loggers and queues are configured on each underlying client; the scheduler's middleware wraps only the forwarder step.

## Related

- [Choose a driver](../choosing/drivers.md), [The in-memory driver](memory.md), [The SQL driver](sqlstore.md), [The Redis driver](redis.md).
- [Cron schedules](../schedules/cron.md), [Local cron](../schedules/local-cron.md) (per-node functions with no queue at all).
- [Nodes and health](../operations/nodes.md).
- Example: [examples/hybrid-placement](../../examples/hybrid-placement/).
