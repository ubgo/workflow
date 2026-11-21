# Choose a schedule tool

Recurring work raises three questions this page answers: where the schedule is declared (in code, at run time, per process, or as a one-off delay), which process fires each tick when several run (the SQL driver's tick row, an elected leader, a dedicated scheduler node), and what happens to ticks that were missed or went stale (a misfire policy, `LatestOnly`, an overlap rule). Every option links to its guide and to an example that runs it.

## Workflow.Cron, Schedule, LocalCron, hybrid or RunAt

| Option | Use it for | Lifetime and reach | Drivers | Guide | Example |
|---|---|---|---|---|---|
| `Workflow.Cron` (with `CronLocation`, `Misfire`, `CronOverlap`) | Recurring work that is part of the app: the nightly cleanup, the hourly sync | Declared with the workflow; every `Register` restores it, registering again replaces it | every driver with `CapCron` | [Cron on a workflow](../schedules/cron.md#cron-on-a-workflow) | [cron-schedules](../../examples/cron-schedules/) |
| `Client.Schedule` / `Unschedule` | Recurring work created at run time: per tenant, per user, set in your UI | Until `Unschedule` with the stored `ScheduleID`; stored by a durable driver | every driver with `CapCron` | [Schedules at run time](../schedules/cron.md#schedules-at-run-time) | [cron-schedules](../../examples/cron-schedules/) |
| `workflow.NewLocalCron` | Per-process work: warm this node's cache, clean its temp directory | Lives in the process; runs on every node that starts one, no queue, no retries, no catch-up | no driver needed | [Local cron](../schedules/local-cron.md#run-a-function-on-every-node) | [schedule-specs](../../examples/schedule-specs/) |
| `hybrid.New(scheduler, jobs)` | Crons fired in the app's process, jobs stored on a durable driver | The schedule is the scheduler client's (in-process); each run is a durable job | the scheduler needs `CapCron` | [Hybrid placement](../drivers/hybrid.md#split-schedules-from-runs) | [hybrid-placement](../../examples/hybrid-placement/) |
| `Job.RunAt` | One run at one point in time: the trial-end email | One run, then nothing | `CapDelayedStart` (every driver) | [Run a job later](../jobs/delays.md#run-a-job-later) | [delayed-jobs](../../examples/delayed-jobs/) |
| `workflow.Backfill`, `FireAtStartup` | Running a schedule's ticks over a past range, or once at boot (once per day across nodes with `StartupOncePer`) | One run per tick, keyed by schedule and tick | `Backfill`: `CapIdempotencyKey` unless `BackfillAllowDuplicates`; `FireAtStartup`: any client, `StartupOncePer` needs `CapIdempotencyKey` | [Backfill missed ticks](../schedules/backfill.md#backfill-missed-ticks) | [schedule-tools](../../examples/schedule-tools/) |

- If every deployment runs it on the same expression, declare it on the workflow; if it differs per tenant or a user can change it, use `Schedule` and store the returned ID ([Workflow.Cron or Schedule](methods.md#workflowcron-or-schedule)).
- If ten replicas should each run it, use `LocalCron`; if the fleet should run it once, use a schedule on a shared driver.
- If the schedule is a single future moment, use `RunAt`, not a cron with an end date.

## Who fires a tick: the tick row, leader election or a scheduler node

With several processes on one store, each tick must start one run. The drivers decide that themselves; the options below change which process does the firing, not whether a tick fires once.

| Option | How a tick is fired once | When a node dies | Drivers | Guide | Example |
|---|---|---|---|---|---|
| No election (the default) | Every client competes; on the SQL driver the client that moves the schedule's next fire time and inserts the (schedule, fire time) tick row in one transaction wins, and the others enqueue nothing. Redis decides in a script; Hatchet's engine fires | Nothing to hand over: the next client to look fires the tick; it survives a split brain | sqlstore, Redis, Hatchet | [One fire per tick across nodes](../schedules/cron.md#one-fire-per-tick-across-nodes) | [postgres-cluster](../../examples/postgres-cluster/) |
| `sqlstore.WithLeaderElection(node, opts...)` | Only the leader of the role `scheduler` fires ticks and runs the dispatcher's housekeeping; the tick row stays as a second guard | The role moves after `LeaderLease` (15 seconds by default); a clean `Stop` resigns, so the next node takes over at its next campaign. `Term` grows on every handover | sqlstore (migration `007_leader_archive`) | [Elect one fire node](../drivers/sqlstore.md#elect-one-fire-node) | [sql-leader-election](../../examples/sql-leader-election/) |
| `sqlstore.WithElector(e)` | As above, with your own elector (a Kubernetes Lease, a Redis lock): one method, `IsLeader() bool`, asked on every dispatcher pass | As your elector decides | sqlstore | [Elect one fire node](../drivers/sqlstore.md#elect-one-fire-node) | [sql-leader-election](../../examples/sql-leader-election/) |
| `WithSchedulerOnly()` on one node, `WithoutScheduler()` on the workers | The scheduler node fires and admits; the workers run steps and fire nothing | No tick fires while every scheduler node is down; run two, the driver still fires each tick once | sqlstore, Redis | [Scheduler-only and worker-only processes](../operations/nodes.md#scheduler-only-and-worker-only-processes) | [node-health](../../examples/node-health/) |
| `sqlstore.NewLeaderElector(db, dialect, role, node)` | A leader for a role of your own (a nightly export, the single consumer of a partner API), outside cron | `Campaign` once, or `Run(ctx)` every third of the lease; `Resign` hands it on | sqlstore tables | [Elect one fire node](../drivers/sqlstore.md#elect-one-fire-node) | [sql-leader-election](../../examples/sql-leader-election/) |

- **If you have no reason to pick a node**, keep the default. It needs no election, has no handover gap, and the tick row already makes each tick fire once.
- **If the fire itself must happen on one known node** (a job that calls a system that allows one caller, a tick whose `ScheduleWhen` reads node-local state), use `WithLeaderElection` with the same option and a distinct node ID on every node.
- **If you already run an elector**, pass it with `WithElector`; it must answer `IsLeader` from memory, since it is called on every dispatcher pass.
- **If workers should scale to zero without touching the clock**, split the roles with `WithSchedulerOnly` and `WithoutScheduler`; this is about resources, not about firing once.
- **If several nodes' clocks disagree**, add `WithDatabaseClock` and `WithLeaseSkew` before reaching for an election ([Clocks, skew and one fire node](../drivers/sqlstore.md#clocks-skew-and-one-fire-node)).

## Missed and stale ticks: misfire, LatestOnly or overlap

Three tools that are easy to confuse, because each "skips" something. A misfire policy decides which ticks fire after nobody was awake to fire them. `LatestOnly` lets the runs fire and skips the work of every run whose next tick is already due. An overlap rule decides about a tick that falls due while the previous run is still going.

| Option | Decides | What it skips | Drivers | Guide | Example |
|---|---|---|---|---|---|
| `Misfire{Policy: MisfireSkip}` | Which missed ticks fire after a pause | Every tick not reached within `MisfireGrace` (one minute) | memory, sqlstore, Redis; Hatchet refuses every explicit policy | [Misfire policies](../schedules/misfire.md#misfire-policies) | [cron-schedules](../../examples/cron-schedules/) |
| `Misfire{Policy: MisfireFireOnce, Within: d}` (the default of memory, sqlstore and Redis, without `Within`) | As above | Every missed tick but the newest, and that one too when older than `Within` | memory, sqlstore, Redis | [Misfire policies](../schedules/misfire.md#misfire-policies) | [cron-schedules](../../examples/cron-schedules/) |
| `Misfire{Policy: MisfireFireAll, Max: n}` | As above | Nothing up to `Max`; every missed tick fires, oldest first, each with its own `TickAt` | memory, sqlstore, Redis | [Misfire policies](../schedules/misfire.md#misfire-policies) | [cron-schedules](../../examples/cron-schedules/) |
| `middleware.LatestOnly(cfg)` | Whether a fired run still has work to do | The step's work, when a tick after the run's own that the schedule really fires is already due (backfilled and manually triggered runs always work); the step returns its input and succeeds | any driver with `CapTickTime` and a `ScheduleLister` (memory, sqlstore, Redis, Hatchet), or a `Next` function | [Latest-only runs](../schedules/latest-only.md#step-2-wrap-the-step-in-latestonly) | [latest-only](../../examples/latest-only/) |
| `Schedule.Overlap` / `Workflow.CronOverlap` = `OverlapSkip` | A tick that falls due while the previous run is unfinished | That tick | memory, sqlstore, Redis (`CapOverlapSkip`); refused on Hatchet and hybrid | [Skip a tick while the previous run is unfinished](../schedules/overlap.md#skip-a-tick-while-the-previous-run-is-unfinished) | [schedule-overlap](../../examples/schedule-overlap/) |
| `workflow.GuardOverlap` with `OverlapReplace` or `OverlapBufferOne` | As above | The old run (replace) or the newest one beyond a buffer of one | needs `CapTickEvents`, `CapRunQuery`, `CapCancel` (memory, sqlstore, Redis) | [Replace the running run, or buffer one](../schedules/overlap.md#replace-the-running-run-or-buffer-one) | [run-control](../../examples/run-control/) |

- **If every tick is its own period** (a daily report, an hourly billing run), choose a misfire policy and read `StepInfo.TickAt()` for the period; never `LatestOnly`, which would drop periods.
- **If only the newest state matters** (refresh a cache, push the current stock level), use `LatestOnly`. It also covers the case no misfire policy sees: a run that waited in a backlog until after its successor's tick.
- **If the work is slow and two copies must never overlap**, use `OverlapSkip`; if the newest data should win, `GuardOverlap` with `OverlapReplace`.
- **If you want catch-up and freshness together**, combine them: `MisfireFireAll` fires every missed tick so each gets a run with its tick, and `LatestOnly` on the expensive step makes all but the newest return at once. The [latest-only](../../examples/latest-only/) example does exactly that.
- **If you run on Hatchet**, leave `Misfire` zero (a missed tick is dropped) and use `LatestOnly` for backlogs; overlap rules are refused there.

## Pitfalls

- A `Schedule` without a `Name` adds a new schedule on every call. Give run-time schedules a `Name`: every driver then keeps one schedule per (workflow, name) and returns the same `ScheduleID`, so calling it at every boot is safe (Hatchet deletes and recreates the engine trigger when the definition changed, since it cannot edit one in place).
- A worker-only client given `WithLeaderElection` still campaigns for the `scheduler` role. When it wins, no client fires ticks, because a `WithoutScheduler` client never fires, leader or not. Give `WithLeaderElection` only to clients that may fire.
- With leader election, ticks pause while the role moves: after a crash, up to `LeaderLease` plus `WithLeaseSkew`. The next leader then applies the schedule's misfire policy to what it missed.
- Leader election gates cron firing and the dispatcher's housekeeping (consumed signals, silent nodes, schedule histories), not retention: `WithRetention`'s background prune picks one client per interval with its own lock row, whoever leads.
- `LatestOnly` counts a tick as newer only when the schedule would fire it (its windows, `Active` period, pause and overlap rule; its `ScheduleWhen` when you pass it as `LatestOnlyConfig.When`), but a tick the misfire policy dropped while the scheduler was down still counts on a driver without `CapScheduleHistory`. See [the pitfalls](../schedules/latest-only.md#pitfalls).
- A skipped step still succeeds and returns its input; the steps after it run. Put `LatestOnly` on the step that does the expensive work, or on every step.
- "Missed" is measured from what the scheduler knows: on the memory driver from `Start` or the `Schedule` call, on the SQL driver from the schedule's stored next fire time. A paused tick is never a missed tick.
- `LocalCron` drops a tick that falls due while the previous run of that entry is still going, and never catches up after a restart.

## Related

- [Cron schedules](../schedules/cron.md), [Misfire policies](../schedules/misfire.md), [Latest-only runs](../schedules/latest-only.md), [Overlap](../schedules/overlap.md)
- [Choose a driver](drivers.md), [Choose an operations tool](operations.md), [Choose a method](methods.md)
