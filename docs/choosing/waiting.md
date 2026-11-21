# Choose how a run waits

A run often has to stop and wait: for a time, for an event someone will send, for an answer a person or an API will give, or for something it can only ask about. The library has a tool for each, and they differ in who wakes the run, whether a late event still counts, whether the wait spends retry attempts, and which drivers have them. None of them holds a worker while it waits; never `time.Sleep` in a handler.

## Wait for time: sleep step, SleepUntil, RunAt or snooze

| Option | Use it for | How it waits | Needs | Guide | Example |
|---|---|---|---|---|---|
| `Step.Sleep` (a sleep step) | A pause between two steps of one run: charge now, send the survey three days later | The run suspends; the next step runs after the duration | `CapDurableSleep` (every driver); `CapDurable` for it to survive a restart | [Sleep inside a workflow](../workflows/sleep.md#sleep-inside-a-workflow) | [signals-and-sleep](../../examples/signals-and-sleep/) |
| `workflow.SleepUntil(clk, t)` in a step | Waiting inside a step until an absolute time: renew on the subscription's date | Returns a snooze for the time left, nil once `t` has passed | `CapSnooze` (memory, sqlstore, Redis) | [Sleep until a time](../workflows/polling-and-approval.md#sleep-until-a-time) | [flow-patterns](../../examples/flow-patterns/) |
| `Job.RunAt` | A whole run that must not start before a time | The run is created delayed | `CapDelayedStart` (every driver) | [Run a job later](../jobs/delays.md#run-a-job-later) | [delayed-jobs](../../examples/delayed-jobs/) |
| `workflow.Snooze(d)` | "Not yet" inside a step: the upstream report is not built, the partner's window opens at nine | The step is put back for `d` without spending an attempt; the run reads `waiting` | `CapSnooze` | [Snooze a step that is not ready](../jobs/retries.md#snooze-a-step-that-is-not-ready) | [snooze-heartbeat](../../examples/snooze-heartbeat/) |

## Wait for the world: signals, approvals, polls and promises

| Option | Who wakes the run | A signal sent before the run waits | Needs | Guide | Example |
|---|---|---|---|---|---|
| A wait step (`Step.Wait`) and `Client.Signal(ctx, Event{Name, Key})` | Your app, with an event matched by name and key; `Timeout` or `Deadline` gives up | Kept and taken when the run reaches the wait | `CapWaitForEvent` (every driver) | [Wait for an event](../workflows/signals.md#wait-for-an-event) | [signals-and-sleep](../../examples/signals-and-sleep/) |
| `WaitSpec.OwnRun` and `workflow.SignalRun(ctx, c, ref, name, payload)` | Someone who knows the run's ID (an operator, a child via `SignalParent`) | Kept for that run only | `CapWaitOwnRun` (every driver) | [Signal one run by its ID](../workflows/escalation.md#signal-one-run-by-its-id) | [escalation](../../examples/escalation/) |
| `workflow.OnSignals` handlers | As above, but the run keeps going: a handler runs before each step attempt | Taken before the run's next step attempt | a `RunSignalReader` (memory, sqlstore, Redis); refused on Hatchet | [Handle signals between steps](../workflows/escalation.md#handle-signals-between-steps) | [escalation](../../examples/escalation/) |
| `TransientSignaller.SignalWaiting(ctx, ev)` | A hint whose late delivery would be wrong (refresh now, invalidate a cache) | Dropped: only runs suspended on the wait at that moment resume | memory, sqlstore, Redis (`Supports(c, OpSignalWaiting)`) | [Deliver only to runs waiting now](../workflows/signals.md#deliver-only-to-runs-waiting-now) | [flow-cancel](../../examples/flow-cancel/) |
| An approval step (`workflow.ApprovalStep`, `Approve`, `ApproveRun`) | A person, with who approved and a note | Kept, as a signal | `CapWaitForEvent` | [Human approval](../workflows/polling-and-approval.md#human-approval) | [flow-patterns](../../examples/flow-patterns/) |
| `workflow.Poll(ctx, name, spec, check)` | Nobody: the step asks `check` on a schedule until it reports done | not applicable | the Journal plugin and `CapSnooze` | [Poll until a condition holds](../workflows/polling-and-approval.md#poll-until-a-condition-holds) | [flow-patterns](../../examples/flow-patterns/) |
| `workflow.AwaitPromise` and `ResolvePromise` / `RejectPromise` | Anyone who knows the promise ID (an HTTP handler, a webhook), settling it once | The value is stored, so a late waiter still reads it | a `PromiseStore` shared by both sides; `CapSnooze` | [Wait for a promise](../workflows/promises.md#wait-for-a-promise) | [flow-promises](../../examples/flow-promises/) |
| `workflow.Update(ctx, c, store, ev, opts)` | The caller, who also gets the run's answer back | Kept, as a signal | `CapWaitForEvent` and a `PromiseStore` | [Ask a run and get its answer](../workflows/promises.md#ask-a-run-and-get-its-answer) | [flow-promises](../../examples/flow-promises/) |
| `workflow.SignalWithStart(ctx, c, job, ev)` | The caller, starting the run first when none with the job's key is unfinished | Kept, so the new run takes it at its wait step | `CapWaitForEvent` and `CapIdempotencyKey` | [Start a run if it is not running, then signal it](../workflows/promises.md#start-a-run-if-it-is-not-running-then-signal-it) | [flow-promises](../../examples/flow-promises/) |
| `events.New(c, events.WithSignal())` | An event published on the bus, which also starts its subscribed runs | Kept, as a signal | as `Signal` | [Deliver to waiting runs](../events/publish-subscribe.md#deliver-to-waiting-runs) | [events-signal](../../examples/events-signal/) |

### If X, then Y

- **If the delay is measured from an earlier step finishing**, use a sleep step; **if from an absolute time**, `RunAt` for a whole run or `SleepUntil` inside a step.
- **If the outside world can call you back** (a webhook, a button), use a wait step and `Signal`; it is cheaper than a poll and wakes at once. **If you can only ask** (is the export done?), use `Poll`.
- **If the sender knows the run's ID rather than a business key**, use `OwnRun` and `SignalRun`; behind `regional`, `failover` and `cutover` the prefixed ID still reaches the right backend.
- **If the run must keep working while it listens** (an escalation that raises a flag, a cancel request), use `OnSignals` handlers instead of a wait step.
- **If the decision is made by an API call that may come before or after the run asks**, use a promise: the value is stored, so ordering does not matter, and it settles once.
- **If the caller needs an answer** (add an item to this cart and tell me the total), use `Update`.
- **If a person must decide, and you want who and why recorded**, use an approval step, with a `Timeout` and an `OnFailure` that escalates.
- **If a late event would do harm**, use `SignalWaiting`; anything that must arrive goes through `Signal`.

## Pitfalls

- A wait that reaches its `Timeout` or `Deadline` fails the run with an error matching `context.DeadlineExceeded`; waits are not retried, so `OnFailure` runs.
- A wait outputs the event's payload: the step after it reads the event, not what the step before the wait returned.
- Wait keys are Go functions (`KeyFunc`) on every driver; Hatchet refuses a `KeyExpr` there, though it takes only CEL for concurrency keys.
- Kept events are pruned once older than the oldest unfinished run; a signal is not a durable message queue.
- `SleepUntil`, `Poll`, `AwaitPromise` and `AwaitChildren` wait by snoozing. On a driver without `CapSnooze` (Hatchet) a snooze is an ordinary error that spends an attempt, so a long wait burns the retry budget; `AwaitChildren` refuses outright.
- Code above a `SleepUntil` or `Poll` call runs on every return of the step; keep the call at the top of the step or put earlier work in `Memo`.
- Promises stay in the store until deleted; delete the ones you settle and nobody reads, or give the Redis store a TTL. The caller and the run must use the same store.
- On the memory driver a sleep, a wait and every kept event are lost when the process exits; nothing refuses them, because durability is never inferred from a workflow.

## Related

- [Choose the shape of the work](shapes.md), [Choose a method](methods.md#retry-snooze-or-release)
- [Wait for an event](../workflows/signals.md), [Escalation and signal handlers](../workflows/escalation.md), [Promises, updates and signal helpers](../workflows/promises.md), [Polling, sleeping until, and approvals](../workflows/polling-and-approval.md)
