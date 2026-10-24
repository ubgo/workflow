# Choose by kind of app

A starting set of driver, options and middleware for the kinds of app that use a job library, with the reason for each pick. Each row is a default, not a rule: the [driver guide](drivers.md) and the [method guide](methods.md) explain the trade-offs behind it.

## The table

| Kind of app | Driver | Turn on | Leave off, and why | Guides | Example |
|---|---|---|---|---|---|
| A web app or API on one server | `sqlstore` on a SQLite file | `Migrate` at deploy, `EnqueueTx` for jobs tied to your writes, retries with `Permanent` for bad input, `WithRetention`, `middleware.Logging` and `Recover` | Failover: one machine is one failure domain either way | [SQL driver](../drivers/sqlstore.md#create-the-client), [transactional enqueue](../reliability/transactional-enqueue.md#enqueue-inside-your-transaction), [retention](../operations/retention.md#keep-finished-runs-for-a-while) | [sqlite-worker](../../examples/sqlite-worker/) |
| A SaaS on several machines with Postgres or MySQL | `sqlstore` on that database | Everything above, plus `Workflow.Concurrency` keyed by tenant so one tenant cannot starve the rest, `UniqueUntilFinished` with `sqlstore.UniqueStore` for one sync per account, `Client.Schedule` per tenant for their own reports, the `otel` module | The memory stores (`MemoryUnique`, `MemoryDebounce`): each process would keep its own | [run several workers](../operations/deploy.md#run-several-workers), [concurrency](../jobs/concurrency.md#limit-runs-per-key), [unique jobs](../reliability/unique-jobs.md#a-store-shared-by-every-process), [OpenTelemetry](../observability/opentelemetry.md#add-tracing-and-metrics) | [postgres-cluster](../../examples/postgres-cluster/) |
| A team already running Hatchet | `hatchet` | CEL keys (`workflow.KeyExpr`), batches as native groups, `hatchet.Client.BatchSource`, the outbox if jobs must match your own database's commits | Misfire policies, retry jitter, `DelayFor` and non-UTC crons: the driver refuses them; anything that needs every run end guaranteed (Hatchet delivers best-effort) | [Hatchet driver](../drivers/hatchet.md#what-it-can-do-and-what-it-refuses), [native groups](../batches/native-groups.md#native-group-runs), [outbox](../reliability/outbox.md#write-jobs-inside-your-transaction) | [hatchet-worker](../../examples/hatchet-worker/) |
| A desktop app, CLI or agent on a laptop | `sqlstore` on SQLite for work that must survive a restart; `memory` for work that may not | Durable sleeps and waits (a reminder in three days), `Misfire{Policy: MisfireFireOnce}` so a laptop that slept runs the missed tick once | Several worker processes: SQLite shares one machine | [sleep](../workflows/sleep.md#sleep-inside-a-workflow), [misfire policies](../schedules/misfire.md#misfire-policies) | [sqlite-worker](../../examples/sqlite-worker/) |
| Heavy fan-out: imports, mass email, report generation | `sqlstore` on Postgres, or `hatchet` | `batch` with `Then`, `Catch` and `Finally` and a durable store, `Workflow.RateLimits` for the downstream API, the claim check for big payloads | Plain loops of `Enqueue` when you need to know when the group is done | [batches](../batches/batches.md#dispatch-a-batch), [durable batches](../batches/durability.md#a-durable-store), [rate limits](../jobs/concurrency.md#rate-limits), [claim check](../reliability/claim-check.md#move-big-payloads-out-of-the-queue) | [batches](../../examples/batches/) |
| Webhook receivers and payment flows | any durable driver | `Job.IdempotencyKey` from the provider's event ID, `IdempotencyRequired` on the workflow, wait steps for the reply (`Step.Wait` and `Client.Signal`), payload encryption | Debounce: every event matters | [idempotency keys](../jobs/idempotency.md#idempotency-keys), [wait for an event](../workflows/signals.md#wait-for-an-event), [encrypt payloads](../middleware/codecs.md#encrypt-payloads) | [idempotency](../../examples/idempotency/), [signals-and-sleep](../../examples/signals-and-sleep/) |
| An enqueue path that must keep working through a backend outage | `failover.New(primary, secondary)` over two drivers | Idempotency keys on every job (required), `WithCoolOff`, `WithObserver` to alert | Schedules on both backends unless you want them to fire twice (`WithSchedulesOnBoth` is opt-in) | [failover](../reliability/failover.md#fail-over-to-a-second-backend) | [failover](../../examples/failover/) |
| App tests | `workflowtest.Fake` built with `FakeLike(prodClient)` | Its assertions (`AssertEnqueued`, `AssertScheduled`, ...); `memory` with a fake clock when steps must run | Real time in tests: a test that waits for a backoff is slow and flaky | [the Fake](../testing/fake.md#behave-like-your-production-driver), [fake clock](../testing/fake-clock.md#drive-time-in-tests) | [app-tests](../../examples/app-tests/) |
| A library or framework that offers background jobs | depend on `workflow.Client` only | A `workflow.Plugin` for your rules, the conformance suite for any driver you ship | Choosing a driver for your users | [write a plugin](../extending/writing-a-plugin.md#write-a-plugin), [write a driver](../extending/writing-a-driver.md#run-the-conformance-suite) | [custom-plugin](../../examples/custom-plugin/), [custom-driver](../../examples/custom-driver/) |

## If X, then Y

- If your API runs somewhere that cannot keep goroutines alive (a function platform), run the workers as a separate long-lived process on the same driver. The API side only enqueues: `Enqueue` and `EnqueueTx` need a started client, but `outbox.Outbox.Write` needs only a database transaction, and the relay runs with the workers.
- If one tenant's spike must not delay the others, key concurrency (or rate limits) by tenant: each tenant gets its own `MaxRuns`, and `ConcurrencyRoundRobin` makes the waiting tenants take turns ([Strategies](../jobs/concurrency.md#strategies)).
- If a job carries personal data, seal it with the codec `middleware.Encrypt(key, oldKeys...)` returns, applied with `middleware.WithCodec`; the old keys still decrypt during rotation.
- If a payload can be megabytes, move it out of the queue with the claim check rather than raising database or engine limits.
- If you are not sure which you are, start with `sqlstore` on SQLite: it is durable, needs nothing new, and moving to Postgres later changes the dialect and the DSN.

## Pitfalls

- A web app and its worker processes must register the same workflows: `Enqueue` refuses a workflow the calling client has not registered (`ErrNotFound`).
- `EnqueueTx` needs the job's workflow registered on that client and the client started, like `Enqueue`.
- On Hatchet, a plugin or feature that needs every run end (batch callbacks through the hook alone, `UniqueUntilFinished` without its TTL) needs the fallback the library already adds there: a status poller or the TTL.

## Related

- [Choose a driver](drivers.md)
- [Choose a method](methods.md)
- [Going to production](../start/production.md#checklist)
