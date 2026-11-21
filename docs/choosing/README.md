# Choosing

Decision guides for the questions every app answers before it writes code. Each one has decision tables, "if X, then Y" rules, the pitfalls found in the code and tests, and for every option a link to the guide section that explains it and the example that runs it.

| Guide | Use this when |
|---|---|
| [Choose a driver](drivers.md) | You are picking a backend (memory, SQL on SQLite, Postgres or MySQL 8, Redis, Hatchet) or a wrapper in front of it (hybrid, failover, regional, cutover, pools), and need to know what survives a restart and which capabilities each one refuses. |
| [Choose the shape of the work](shapes.md) | You know what must happen but not whether it is a job, a workflow of steps, a chain, child runs, a batch, a schedule, a virtual object, a state machine, a saga or events. |
| [Choose a method](methods.md) | The shape is clear and you are picking options inside it: `Enqueue`, `EnqueueTx` or the outbox; idempotency key, unique or debounce; concurrency or rate limit; `RunAt`, sleep or schedule; retry, snooze or release; which hook reacts to a run's end. |
| [Choose how a run waits](waiting.md) | A run must stop for a time, an event, a person's answer, a promise or a condition it can only poll. |
| [Choose a schedule tool](schedules.md) | Work repeats, and you are deciding where the schedule lives, which process fires each tick (the tick row, leader election, a scheduler node), and what happens to missed or stale ticks (misfire policies, `LatestOnly`, overlap). |
| [Choose an operations tool](operations.md) | You are deciding where finished runs go (retention, the `WithArchive` hook, archive tables, the run recorder) and how a process avoids starting too early (`StartWhen`, readiness probes, trusted clocks). |
| [Choose by kind of app](app-types.md) | You want a starting set of driver, options and middleware for your kind of app: one server, a web fleet with workers, multi-tenant, multi-region, edge or desktop, Hatchet, heavy fan-out, webhooks, an outage-proof enqueue path, a backend move, tests or a library. |

Once the choices are made, [Install](../start/install.md) and the [Quickstart](../start/quickstart.md) get the first workflow running, [Going to production](../start/production.md) is the checklist before launch, and the [feature index](../reference/features.md) lists every feature with its guide and example.

## Quick answers

The decisions most apps make first, with the default that fits most of them. Each links to the section that explains the trade-off.

| If you need | Start with | Why | Read |
|---|---|---|---|
| Durable jobs with nothing new to run | `sqlstore` on a SQLite file | Survives restarts, shares one machine, moves to Postgres by changing the dialect and DSN | [Choose a driver](drivers.md#memory-sql-redis-or-hatchet) |
| Several machines sharing the work | `sqlstore` on Postgres or MySQL 8, or `redis` | Each step claimed once, a cron tick fired once, a crashed worker's step taken over after its lease | [Choose a driver](drivers.md#if-x-then-y) |
| A job that exists only if your transaction commits | `EnqueueTx`, or the outbox when the queue is elsewhere | The run or outbox row is written in your own transaction | [Enqueue, EnqueueTx or the outbox](methods.md#enqueue-enqueuetx-or-the-outbox) |
| One run per request when callers retry | `Job.IdempotencyKey` | Enforced by the driver, no extra store | [Idempotency key, unique or debounce](methods.md#idempotency-key-unique-or-debounce) |
| A fixed multi-step process | One workflow with steps | Outputs flow from step to step; one run, one status | [Choose the shape of the work](shapes.md#job-workflow-chain-children-or-batch) |
| A per-tenant recurring job | `Client.Schedule` with a `Name`, storing the returned ID | Created and removed at run time, unlike `Workflow.Cron` | [Workflow.Cron or Schedule](methods.md#workflowcron-or-schedule) |
| A group of jobs with one outcome | `batch.New(...).Dispatch` | `Then`, `Catch`, `Finally`, progress and cancel | [A batch or plain enqueues](methods.md#a-batch-or-plain-enqueues) |
| A run that waits for a webhook or a person | A wait step and `Client.Signal` | Holds no worker, keeps a signal sent early, gives up at a deadline | [Choose how a run waits](waiting.md#wait-for-the-world-signals-approvals-polls-and-promises) |
| A step that should come back later without failing | `workflow.Snooze(d)` | Spends no attempt and records no error | [Retry, snooze or release](methods.md#retry-snooze-or-release) |
| Crons fired on one chosen node | `sqlstore.WithLeaderElection` | Only the leader fires; the tick row still guards | [Who fires a tick](schedules.md#who-fires-a-tick-the-tick-row-leader-election-or-a-scheduler-node) |
| Only the newest tick's work after a backlog | `middleware.LatestOnly` | Older runs skip their work; a misfire policy decides which ticks fire at all | [Missed and stale ticks](schedules.md#missed-and-stale-ticks-misfire-latestonly-or-overlap) |
| History kept after retention deletes runs | `WithArchiveTable` or a `WithArchive` hook; the run recorder for timelines | Hot tables stay small, history stays readable | [Archive tables, the WithArchive hook or the run recorder](operations.md#archive-tables-the-witharchive-hook-or-the-run-recorder) |
| A device that boots with a wrong clock | `workflow.StartWhen` with `ClockAfter` | Nothing is scheduled before the clock is right | [StartWhen or a readiness probe](operations.md#startwhen-or-a-readiness-probe) |
| Tests of the code that enqueues | `workflowtest.NewFake(workflowtest.FakeLike(prod))` | Runs nothing, refuses what production would | [Choose by kind of app](app-types.md#tests) |

## Pitfalls

- Choosing the memory driver is choosing to lose queued, delayed and sleeping work on exit. Nothing refuses a week-long sleep there, because durability is never inferred from a workflow.
- A feature listed for "every driver" may still be refused for one setting: Hatchet takes concurrency keys only as CEL, crons only in UTC, and no misfire policy. Run `workflow.CheckCaps` or a `FakeLike` test against each target before you commit to one.
- The memory stores for unique jobs and debounce hold state inside one process. As soon as two processes enqueue, pass a shared store.
- Several tools "skip" or "retry later" in different senses: a misfire policy, `LatestOnly` and `OverlapSkip` skip different things, and a snooze, a release and `RetryAfter` differ in whether they spend an attempt. The tables in [Choose a schedule tool](schedules.md) and [Choose a method](methods.md#retry-snooze-or-release) set them side by side.
