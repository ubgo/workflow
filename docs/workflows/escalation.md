# Escalation and signal handlers

A run often needs to hear from someone who knows exactly which run they mean: an operator approving run 42, a child telling its parent that a receipt is over the limit, a payment service answering the order that asked. Every run has an address, its own ID, and a signal sent to that address reaches that run alone. The run takes it in one of two ways: a wait step that blocks until it arrives, or a handler that runs between the run's steps while the run goes on (a BPMN event sub-process).

## Before you start

- A driver with `workflow.CapWaitOwnRun` for a wait on the run's own address: memory, SQL, Redis and Hatchet have it.
- A driver with `workflow.RunSignalReader` for handlers between steps (`workflow.Supports(c, workflow.OpRunSignals)`): memory, SQL and Redis. Hatchet does not have one, so `OnSignals` is refused there.
- [Wait for an event](signals.md) and [child runs](children.md).
- The example is [`examples/escalation`](../../examples/escalation). It runs on the memory driver. Run it with `go run ./escalation` from `examples/`.

## Signal one run by its ID

**Problem:** a wait step matches events by name and key, and `WaitSpec.KeyFrom` sees the job, not the run, so to reach one run you had to invent a key, store it in the job and keep it next to the run ID.

**Fix:** set `WaitSpec.OwnRun`. The wait takes only the signals addressed to its own run, and the driver resolves that address from the run's ID when the wait begins:

<!-- snippet: examples/escalation/main.go#address -->
```go
order := workflow.Workflow{Name: "order", Steps: []workflow.Step{
	{Name: "await-payment", Wait: &workflow.WaitSpec{Event: "payment", OwnRun: true}},
	{Name: "ship", After: []string{"await-payment"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
		return in, nil
	}},
}}
```

Send with `workflow.SignalRun` and the run's ref:

<!-- snippet: examples/escalation/main.go#send -->
```go
if err := workflow.SignalRun(ctx, c, second, "payment", []byte(`"card 4242"`)); err != nil {
	return err
}
```

`SignalRun` is a `Client.Signal` whose key is `workflow.RunKey(id)`, so the [signal contract](signals.md) holds: a signal sent before the run reaches its wait is kept, and the run takes it once. A plain `Signal` on the same event name, with or without a key, does not reach an `OwnRun` wait. `Key`, `KeyFrom` and `OwnRun` are exclusive; `Register` refuses a driver without `CapWaitOwnRun`.

## Escalate from a child to its parent

**Problem:** a child run finds something its parent must decide (BPMN escalation), but it should not fail or stop to say so, and the parent may be busy waiting for it.

**Fix:** the child calls `workflow.SignalParent` from a step. It sends to the run that spawned it (the `LabelParentRun` that `Spawn`, `AwaitChild` and `JoinChildren` set) and returns; the child goes on. The parent takes the signal with an `OwnRun` wait or an `OnSignals` handler:

<!-- snippet: examples/escalation/main.go#escalate -->
```go
checkReceipt := workflow.Workflow{Name: "check-receipt", Steps: []workflow.Step{{Name: "check",
	Run: func(ctx context.Context, in []byte) ([]byte, error) {
		var amount int
		if err := json.Unmarshal(in, &amount); err != nil {
			return nil, workflow.Permanent(err)
		}
		if amount > 1000 {
			note, _ := json.Marshal(fmt.Sprintf("receipt over limit: %d", amount))
			if err := workflow.SignalParent(ctx, c, "escalation", note); err != nil {
				return nil, err
			}
		}
		return []byte(`"checked"`), nil // the child goes on after escalating
	}}}}
expense := workflow.Workflow{Name: "expense", Steps: []workflow.Step{
	{Name: "check", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		_, err := workflow.Spawn(ctx, c, "receipt", []workflow.Job{{Workflow: "check-receipt", Payload: in}})
		return nil, err
	}},
	{Name: "escalated", After: []string{"check"}, Wait: &workflow.WaitSpec{Event: "escalation", OwnRun: true, Timeout: time.Minute}},
	{Name: "review", After: []string{"escalated"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
		return in, nil
	}},
}}
```

`client` is the client the parent runs on. A run that is no child gets a `Permanent` `ErrInvalid`, so the step does not retry it.

## Handle signals between steps

**Problem:** a wait step stops the run until a signal comes, and a signal that may never come would keep the run open forever. You want the run to go on and react to a signal whenever one arrives: a progress note, a "slow shard" warning, a changed address.

**Fix:** the `workflow.OnSignals` plugin. Before each step attempt of a run of `SignalHandlers.Workflow`, it lists the run's pending signals whose name has a handler, calls each handler in the order the signals were sent, and takes each one its handler accepted. Then the step runs:

<!-- snippet: examples/escalation/main.go#handlers -->
```go
onSlow := workflow.OnSignals(workflow.SignalHandlers{Workflow: "migrate", On: map[string]workflow.SignalHandler{
	"slow": func(_ context.Context, s workflow.RunSignal) error {
		heard.add(string(s.Payload)) // runs before a step of the migration, then the step goes on
		return nil
	},
}})
c, err := workflow.Use(memory.New(), workflow.Journal(workflow.NewMemoryJournal()), onSlow)
```

A step that snoozes is looked at again every poll, so a parent in `JoinChildren`, `AwaitChild` or `Poll` hears its children while it waits. In the example the migration's join hears both slow shards before the join decides.

What a handler returns decides what happens next:

| Handler returns | What happens |
|---|---|
| `nil` | The signal is taken and the step runs (non-interrupting). |
| An error | The step's attempt fails with it and the signal stays; the retry handles it again. |
| A `workflow.Permanent` error | The step fails without a retry, so the run fails (interrupting). |

To end the run early instead, call `workflow.EndRun` (with the `EndRuns` plugin) or `Client.Cancel` from the handler. Signals of names without a handler are left for wait steps or other handlers; a wait step and a handler on one name share one queue, and whichever looks first takes a signal.

`Use` binds the plugin to the client it wraps (the `workflow.ClientBinder` hook) and refuses a client without `RunSignalReader` with an `UnsupportedError`, before anything is registered.

## Approvals, races and updates by run ID

The helpers that build wait steps take `OwnRun` too, each with a sender that addresses one run:

| Wait | Sender |
|---|---|
| `ApprovalStep(ApprovalSpec{OwnRun: true})` | `ApproveRun`, `RejectRun` |
| `Race(RaceSpec{OwnRun: true})` | `SignalChoiceRun` |
| A wait with `OwnRun` before an `UpdateHandler` step | `UpdateRun` |

## Behind wrappers

`regional`, `failover` and `cutover` prefix the refs they return and the `RunID` a step sees. They implement `workflow.RunSignaller`: `SignalRun` strips the prefix and signals the run's own address on the backend that holds it, and `PendingSignals` and `TakeSignal` reach that backend too. A ref without a known prefix goes wherever `Signal` goes. `hybrid` and the client `Use` returns forward to what they wrap.

## Which drivers

| Driver | `OwnRun` wait (`CapWaitOwnRun`) | Handlers between steps (`RunSignalReader`) |
|---|---|---|
| memory | yes | yes |
| SQL (SQLite, Postgres, MySQL) | yes | yes: the signals table and the run's consumed rows, the take under the run's row lock |
| Redis | yes | yes: Lua scripts over the signal sets and the run's consumed markers |
| Hatchet | yes: the durable wait resolves the key from the engine's run ID | no: the engine keeps no record a step could read of which events a run took |

## Pitfalls

- **Handlers run at step boundaries, not at once.** A run parked in a wait or a sleep handles nothing until its next step begins. Block on a signal with an `OwnRun` wait; keep a snoozing step's `Poll` short when a handler must react quickly.
- **Handlers run at least once.** A worker that dies between a handler and its take, or two steps of one run starting at the same moment, run a handler twice for one signal. Make handlers idempotent; `RunSignal.ID` identifies the signal.
- **A retried step sends again.** `SignalParent` and `SignalRun` from a step that is retried send a second signal. Put an ID in the payload when the parent must count each one once.
- **Every step attempt reads the run's signals.** `OnSignals` costs one read per step attempt of the workflows it serves (one query on SQL, one script on Redis). Name the workflow in `SignalHandlers.Workflow` so other workflows pay nothing.
- **`OnFailure` and `OnTimeout` are wrapped too.** Like every step middleware, the plugin wraps them; a driver that passes them the run's `StepInfo` handles the pending signals before them, and an error there is the failure handler's error, which the driver logs.

## Related

- [Wait for an event](signals.md), [child runs](children.md), [joins](joins.md), [polling and approval](polling-and-approval.md)
- Example: [`examples/escalation`](../../examples/escalation)
