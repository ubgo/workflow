# Choosing

Decision guides for the questions every app answers before it writes code. Each one has a decision table, "if X, then Y" rules, the pitfalls found in the code and tests, and for every option a link to the guide section that explains it and the example that runs it.

| Guide | The question it answers |
|---|---|
| [Choose a driver](drivers.md) | Memory, SQL (SQLite, Postgres, MySQL 8) or Hatchet: what survives a restart, how many machines share the work, what you operate, which features each has? |
| [Choose a method](methods.md) | `Enqueue`, `EnqueueTx` or the outbox; `Workflow.Cron` or `Schedule`; a batch or plain enqueues; idempotency key, unique or debounce; concurrency or rate limit; `RunAt`, a sleep step or a schedule; which retry tool; `OnFailure`, `OnRunFinished` or a batch callback? |
| [Choose by kind of app](app-types.md) | Which driver, options and middleware for a single-server app, a SaaS cluster, a Hatchet shop, a desktop app, heavy fan-out, webhooks, an outage-proof enqueue path, app tests or a library? |

Once the choices are made, [Install](../start/install.md) and the [Quickstart](../start/quickstart.md) get the first workflow running, [Going to production](../start/production.md) is the checklist before launch, and the [feature index](../reference/features.md) lists every feature with its guide and example.

## Quick answers

The decisions most apps make first, with the default that fits most of them. Each links to the section that explains the trade-off.

| If you need | Start with | Why | Read |
|---|---|---|---|
| Durable jobs with nothing new to run | `sqlstore` on a SQLite file | Survives restarts, shares one machine, moves to Postgres by changing the dialect and DSN | [Choose a driver](drivers.md#memory-sql-redis-or-hatchet) |
| Several machines sharing the work | `sqlstore` on Postgres or MySQL 8 | Each step claimed once, a cron tick fired once, a crashed worker's step taken over after its lease | [Choose a driver](drivers.md#if-x-then-y) |
| A job that exists only if your transaction commits | `EnqueueTx`, or the outbox when the queue is elsewhere | The run or outbox row is written in your own transaction | [Enqueue, EnqueueTx or the outbox](methods.md#enqueue-enqueuetx-or-the-outbox) |
| One run per request when callers retry | `Job.IdempotencyKey` | Enforced by the driver, no extra store | [Idempotency key, unique or debounce](methods.md#idempotency-key-unique-or-debounce) |
| A per-tenant recurring job | `Client.Schedule`, storing the returned ID | Created and removed at run time, unlike `Workflow.Cron` | [Workflow.Cron or Schedule](methods.md#workflowcron-or-schedule) |
| A group of jobs with one outcome | `batch.New(...).Dispatch` | `Then`, `Catch`, `Finally`, progress and cancel | [A batch or plain enqueues](methods.md#a-batch-or-plain-enqueues) |
| Tests of the code that enqueues | `workflowtest.NewFake(workflowtest.FakeLike(prod))` | Runs nothing, refuses what production would | [Choose by kind of app](app-types.md#the-table) |

## Pitfalls

- Choosing the memory driver is choosing to lose queued, delayed and sleeping work on exit. Nothing refuses a week-long sleep there, because durability is never inferred from a workflow.
- A feature listed for "every driver" may still be refused for one setting: Hatchet takes concurrency keys only as CEL, crons only in UTC, and no misfire policy. Run `workflow.CheckCaps` or a `FakeLike` test against each target before you commit to one.
- The memory stores for unique jobs and debounce hold state inside one process. As soon as two processes enqueue, pass a shared store.
