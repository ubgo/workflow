# State machines

An order, a subscription, a support ticket or a device moves through states over days or weeks, and what an event may do depends on the state it arrives in: a payment is welcome while the order awaits one and an error after it shipped. The usual answer is a `status` column, a `switch` in every handler, a lock so two webhooks for one order do not race, a cron job that finds orders stuck too long in a state, and a table for the history. A durable state machine replaces all of it: you declare the states and the transitions between them, every instance is a key, every event sent to it is a run, and its state is one record in the journal store.

The interpreter follows SCXML, the W3C statechart standard: compound (nested) and parallel states, done events, shallow and deep history, entry and exit actions, and timers per state, in plain Go with no expressions.

## Before you start

- A machine is built on the [virtual object](objects.md) model: events of one key run one at a time under `Concurrency{MaxRuns: 1}` keyed in Go ([`DefineMachine`](ds:block?id=definemachine-zgu9ppny)), so the driver needs [`workflow.CapConcurrencyKey`](ds:block?id=capconcurrencykey-5amtud9g). [Memory](ds:block?id=memory-caps-yxkuq583), the [SQL driver](ds:block?id=sql-caps-yu37ef2j) (SQLite, Postgres, MySQL) and [Redis](ds:block?id=redis-caps-bfgdst9t) run machines; Hatchet refuses the workflow at `Register` ([`checkSupported`](ds:block?id=hatchet-checksupported-733uwgsp)), since its concurrency keys are engine expressions. `MachineSpec.Ordered` also needs [`workflow.CapKeyFIFO`](ds:block?id=capkeyfifo-h9enk5d4). See [Hatchet limitations](../drivers/hatchet.md#limitations).
- Each instance lives in a [`workflow.JournalStore`](ds:block?id=journalstore-tyc8j5tt): [`sqlstore.Client.JournalStore()`](ds:block?id=sql-client-journalstore-wdbnns8s) or [`redis.Client.JournalStore()`](ds:block?id=redis-client-journalstore-9z2gqbez) so it survives restarts and every worker sees it, [`workflow.NewMemoryJournal()`](ds:block?id=newmemoryjournal-cvvh3zbe) for one process. No new table is needed.
- A machine with timers needs a client with [`workflow.CapDelayedStart`](ds:block?id=capdelayedstart-qbxsx6hb) and [`workflow.CapIdempotencyKey`](ds:block?id=capidempotencykey-5wcqqwy8); `DefineMachine` checks it and returns an `*UnsupportedError` otherwise ([`compileMachine`](ds:block?id=compilemachine-vd5zfvzw)).
- The example is [`examples/state-machine`](../../examples/state-machine). Run it with `go run ./state-machine` from `examples/`.
- To keep a machine's states and transitions in JSON instead of Go, see [state machines as data](definitions-as-data.md#state-machines-as-data) and [`examples/machine-dsl`](../../examples/machine-dsl).

## The example machine

The example models an order. After checkout it is in `fulfilment`, a parallel state whose two regions, payment and shipping, progress independently; when both reach their final state the order is complete. The order can be put on hold and resumed from where it was, and an order not paid within 30 minutes is cancelled.

```text
cart
fulfilment (parallel)
├── payment:  awaiting (30-minute timer) → paid (final)
└── shipping: packing → in-transit → delivered (final)
on-hold
completed (final)
cancelled (final)
```

## Data, guards and actions

Each instance carries extended state of a type you choose (`D`), saved as JSON with its configuration. Guards and actions receive a [`workflow.MachineCtx[D]`](ds:block?id=machinectx-23pfpah3): the machine and key, the event (`Name` and `Payload`), the state concerned, `Data` and `Seq`, the instance's microstep number.

<!-- snippet: examples/state-machine/main.go#data -->
```go
// Order is each instance's extended state, saved with its configuration.
type Order struct {
	Total int `json:"total"`
	Paid  int `json:"paid"`
}

// amount reads a "pay" event's payload.
func amount(ev workflow.MachineEvent) int {
	var n int
	_ = json.Unmarshal(ev.Payload, &n)
	return n
}

// coversTotal is the guard of "pay": a payment must cover what is owed.
// A guard only reads.
func coversTotal(m workflow.MachineCtx[Order]) bool {
	return m.Data.Paid+amount(m.Event) >= m.Data.Total
}

// recordPayment is the action of "pay": it changes the instance's data.
func recordPayment(_ context.Context, m *workflow.MachineCtx[Order]) error {
	m.Data.Paid += amount(m.Event)
	fmt.Printf("  action: record payment of %d\n", amount(m.Event))
	return nil
}
```

- A [`workflow.MachineGuard[D]`](ds:block?id=machineguard-yfjzyrtr) returns whether its transition is taken. It must only read: it may be called more than once per event, it gets a shallow copy of the data (a change to a top-level field is dropped, but maps and slices are shared; [`selectTransitions`](ds:block?id=machinerun-selecttransitions-cu4hj8az)), and [`Raise`](ds:block?id=machinectx-raise-qnr6t885) from a guard does nothing.
- A [`workflow.MachineAction[D]`](ds:block?id=machineaction-tqyyjkvw) may change `m.Data`; the changes are saved with the new configuration when the event's handling ends. An action's error fails the event's run, and the retry policy (`MachineSpec.Retry`) runs the event's whole handling again; nothing is saved until all of it succeeds ([the event step](ds:block?id=machine-step-vvjdn77u) saves once, at the end).
- [`m.In(state)`](ds:block?id=machinectx-in-kbnwcsu9) reports whether a state is active, for a guard that depends on another region.

## States

A [`workflow.MachineState[D]`](ds:block?id=machinestate-y2nqpmrz) has a unique `Name`, and optionally a `Parent` (making it a child of a compound or parallel state), an `Initial` child (required for a compound state), `Parallel`, `Final`, `Entry` and `Exit` actions, and `After` timers. The order of `States` is the document order that breaks ties ([`MachineSpec`](ds:block?id=machinespec-wmxfjeh7)): which region's transition is tried first and in which order sibling actions run.

<!-- snippet: examples/state-machine/main.go#states -->
```go
// orderStates is the order's statechart: its states, their parents and
// the actions and timers on them.
func orderStates() []workflow.MachineState[Order] {
	return []workflow.MachineState[Order]{
		{Name: "cart", Exit: say("exit cart")},

		// fulfilment is parallel: its regions payment and shipping are
		// both active while it is.
		{Name: "fulfilment", Parallel: true, Entry: say("enter fulfilment"), Exit: say("exit fulfilment")},

		{Name: "payment", Parent: "fulfilment", Initial: "awaiting"},
		{Name: "awaiting", Parent: "payment", Exit: say("exit awaiting"),
			After: []workflow.StateTimer{{Delay: 30 * time.Minute, Event: "payment-timeout"}}}, // unpaid after half an hour: cancel
		{Name: "paid", Parent: "payment", Final: true, Entry: say("enter paid")},

		{Name: "shipping", Parent: "fulfilment", Initial: "packing"},
		{Name: "packing", Parent: "shipping"},
		{Name: "in-transit", Parent: "shipping"},
		{Name: "delivered", Parent: "shipping", Final: true},

		{Name: "on-hold", Entry: say("enter on-hold"), Exit: say("exit on-hold")},
		{Name: "completed", Final: true, Entry: say("enter completed")},
		{Name: "cancelled", Final: true, Entry: say("enter cancelled")},
	}
}
```

## Transitions and guards

A [`workflow.MachineTransition[D]`](ds:block?id=machinetransition-rxp5bc6d) moves the machine when `Event` arrives while `From` is active. For one source, earlier transitions are tried first and the first whose `Guard` passes is taken ([`selectTransitions`](ds:block?id=machinerun-selecttransitions-cu4hj8az)). A transition of an ancestor applies to every state below it (the `hold` and `cancel` transitions of `fulfilment` work in any of its states), and the innermost active source with an enabled transition wins.

<!-- snippet: examples/state-machine/main.go#transitions -->
```go
// orderTransitions says which event moves the order from which state to
// which, with the guard and actions on each.
func orderTransitions(c workflow.Client) []workflow.MachineTransition[Order] {
	return []workflow.MachineTransition[Order]{
		{From: "cart", Event: "checkout", To: "fulfilment"},

		// Payment region.
		{From: "awaiting", Event: "pay", To: "paid", Guard: coversTotal,
			Actions: []workflow.MachineAction[Order]{recordPayment}},
		{From: "awaiting", Event: "payment-timeout", To: "cancelled"},

		// Shipping region.
		{From: "packing", Event: "ship", To: "in-transit",
			Actions: []workflow.MachineAction[Order]{shippedEmail(c)}},
		{From: "in-transit", Event: "deliver", To: "delivered"},

		// Both regions final: the order is complete.
		{From: "fulfilment", Event: workflow.DoneEvent("fulfilment"), To: "completed"},

		// A transition of fulfilment applies in every state below it.
		{From: "fulfilment", Event: "hold", To: "on-hold"},
		// Deep history: resume where each region was, not at its initial state.
		{From: "on-hold", Event: "resume", To: "fulfilment", History: workflow.HistoryDeep},
		{From: "fulfilment", Event: "cancel", To: "cancelled"},
	}
}
```

- An empty `To` makes a targetless transition: its `Actions` run and no state is left or entered ([`domain`](ds:block?id=machinerun-domain-8t7hkkbb) is nil for it).
- A transition to its own source (or a descendant of it) leaves nothing by default; with `Reenter` it leaves and enters the source again, running its exit and entry actions and restarting its timers ([`MachineTransition`](ds:block?id=machinetransition-rxp5bc6d)).
- `History` enters a compound or parallel target where it was last left (see [History](#history)).

## The order actions run in

When a transition is taken, the exit actions of the states it leaves run first (descendants before ancestors), then the transition's `Actions`, then the entry actions of the states it enters (ancestors before descendants), as [`microstep`](ds:block?id=machinerun-microstep-33j5aqdq) does. Paying the order shows the three:

```text
> pay 120
  exit awaiting
  action: record payment of 120
  enter paid
  states: [paid packing]
```

## Compound and parallel states and done events

- A compound state has exactly one active child; entering it enters its `Initial` child unless the transition targets a deeper state ([`addDescendants`](ds:block?id=machinerun-adddescendants-7p23ezrv)).
- A parallel state has every child (region) active at once. The example's configuration after checkout is `[awaiting packing]`: one atomic state per region, in document order.
- When a compound state's final child is entered, the machine raises [`workflow.DoneEvent(parent)`](ds:block?id=doneevent-xdae986h) ([`enter`](ds:block?id=machinerun-enter-w658d86g)); when every region of a parallel state is in a final state, it raises `DoneEvent` of the parallel state. Handle it with a transition whose `Event` is `workflow.DoneEvent("fulfilment")` and whose `From` is that state or an ancestor; the example moves to `completed` when the order is both paid and delivered.
- A top-level final state ends the instance: [`MachineResult.Done`](ds:block?id=machineresult-2phczvcd) and [`MachineSnapshot.Done`](ds:block?id=machinesnapshot-8w3cnx9s) are true, and every later event is unhandled ([the event step](ds:block?id=machine-step-vvjdn77u)).
- One event can take transitions in several regions at once. When two of them would leave the same states, the one with the deeper source wins, otherwise the first in document order ([`selectTransitions`](ds:block?id=machinerun-selecttransitions-cu4hj8az)).
- [`m.Raise(event, payload)`](ds:block?id=machinectx-raise-qnr6t885) from an action queues an internal event, handled after the current transitions and before the sent event's handling ends; a raised event no state takes is dropped ([`drain`](ds:block?id=machinerun-drain-rbqwphvs)). Done events are raised the same way.

## History

A transition with `History` ([`HistoryMode`](ds:block?id=historymode-r2hqm3cm)) enters a compound or parallel target where it was last left instead of through its initial states:

- [`workflow.HistoryShallow`](ds:block?id=historyshallow-685x9736) re-enters the child (or, for a parallel target, the children) that was active, and that child through its own initial state.
- [`workflow.HistoryDeep`](ds:block?id=historydeep-jduve6dh) re-enters the whole configuration below the target.
- [`workflow.HistoryNone`](ds:block?id=historynone-fnxs4w4g) (the default) enters through the initial states. A target never left before is entered through its initial states whatever the mode ([`addDescendants`](ds:block?id=machinerun-adddescendants-7p23ezrv)).

Set it on the transition that comes back. The example's `hold` is a transition of `fulfilment`, so it applies in every state below it, and `resume` returns with `workflow.HistoryDeep`:

<!-- snippet: examples/state-machine/main.go#history -->
```go
// A transition of fulfilment applies in every state below it.
{From: "fulfilment", Event: "hold", To: "on-hold"},
// Deep history: resume where each region was, not at its initial state.
{From: "on-hold", Event: "resume", To: "fulfilment", History: workflow.HistoryDeep},
```

`History` needs a compound or parallel target: [`DefineMachine`](ds:block?id=compilemachine-vd5zfvzw) refuses a history transition into an atomic state, which has nothing below it to remember.

The example puts a shipped order on hold and resumes it with deep history, so it comes back paid and in transit:

```text
> hold
  exit fulfilment
  enter on-hold
  states: [on-hold]
> resume
  exit on-hold
  enter fulfilment
  enter paid
  states: [paid in-transit]
```

For a parallel target, shallow history only remembers the regions, which are all active anyway, so it returns to `[awaiting packing]` like no history; only deep history keeps each region's progress. Entry actions run again on a history entry (`enter paid` above), and re-entering a final state raises its done event again.

Under a compound target the three modes differ visibly. In [`examples/machine-dsl`](../../examples/machine-dsl) a ticket in `review > escalated > investigating` is put on hold and resumed into `review` once per mode (`history` is the data form of the same field, see [state machines as data](definitions-as-data.md#edit-a-definition-in-go-and-build-it)):

```text
history modes, after assign, escalate, investigate, hold:
  none    resume enters [first-line]
  shallow resume enters [triage]
  deep    resume enters [investigating]
```

## Timers

`MachineState.After` lists [`workflow.StateTimer{Delay, Event}`](ds:block?id=statetimer-wwkknhcg) values: when the state has been active for `Delay`, `Event` is sent to the instance, a timeout per state. In the example an order still `awaiting` payment after 30 minutes takes `payment-timeout` to `cancelled`.

- A timer is a delayed run of the machine's own workflow, sent through `MachineSpec.Client` when the event that entered the state ends, with an idempotency key built from the instance, the state, its entry and the timer's index, so a retried event sends it once ([`sendTimers`](ds:block?id=machinerun-sendtimers-kuc82nwf)).
- A timer whose state was left before it fires does nothing ([`MachineResult.Stale`](ds:block?id=machineresult-2phczvcd), set in [the event step](ds:block?id=machine-step-vvjdn77u)), even when the state was entered again since: that entry has a timer of its own. A `Reenter` self-transition restarts a state's timers.
- [`MachineSpec.Clock`](ds:block?id=machinespec-wmxfjeh7) times the timers and the log. The example passes a fake clock to both the machine and `memory.WithClock`, so the 30 minutes pass when it calls `Advance` (see [Drive time in tests](../testing/fake-clock.md)):

```text
> 30 minutes pass without a payment
  exit awaiting
  exit fulfilment
  enter cancelled
status of o-2: states [cancelled], done true, paid 0 of 80
  1 09:00 (start)         [] -> [cart]
  2 09:00 checkout        [cart] -> [awaiting packing]
  3 09:30 payment-timeout [awaiting packing] -> [cancelled]
```

## Define the machine

[`workflow.DefineMachine(spec)`](ds:block?id=definemachine-zgu9ppny) checks the spec ([`compileMachine`](ds:block?id=compilemachine-vd5zfvzw): unknown parents and targets, cycles, a compound state without an `Initial` child, a final state with children, timers without a capable client) and returns the workflow that handles its events; register it like any workflow.

<!-- snippet: examples/state-machine/main.go#define -->
```go
// orderMachine defines the order machine from its states and transitions,
// keeping each instance's record in store.
func orderMachine(store workflow.JournalStore, c workflow.Client, clk clock.Clock) (workflow.Workflow, error) {
	return workflow.DefineMachine(workflow.MachineSpec[Order]{
		Name:        "order",
		Initial:     "cart",
		States:      orderStates(),
		Transitions: orderTransitions(c),
		Store:       store,                  // where each instance's record lives
		Client:      c,                      // sends the timers: delayed runs of the machine
		Clock:       clk,                    // times the timers and the log
		Unhandled:   workflow.MachineReject, // an event no state takes fails its run
	})
}
```

## Start an instance and send it events

[`workflow.StartMachine(ctx, client, machine, key, data, job)`](ds:block?id=startmachine-rwxg4f9d) starts instance `key` with `data` as its extended state and runs the initial states' entry actions. Starting a key already started changes nothing (`MachineResult.Started` is false), so it is safe to repeat. `workflow.AwaitResult` gives the run's `workflow.MachineResult` as JSON:

<!-- snippet: examples/state-machine/main.go#start -->
```go
ref, err := workflow.StartMachine(ctx, c, "order", key, Order{Total: total}, workflow.Job{})
if err != nil {
	return err
}
out, err := workflow.AwaitResult(ctx, c, ref, await)
if err != nil {
	return err
}
var res workflow.MachineResult
if err := json.Unmarshal(out, &res); err != nil {
	return err
}
fmt.Println("  states:", res.States)
```

[`workflow.SendEvent(ctx, client, machine, key, event, payload, job)`](ds:block?id=sendevent-ex33hwx5) enqueues one event. A key never seen before starts in the initial state with zero data, then handles the event. `job` may carry the run's other fields (`IdempotencyKey`, `RunAt`, `Labels`, `Tenant`).

<!-- snippet: examples/state-machine/main.go#event -->
```go
ref, err := workflow.SendEvent(ctx, c, "order", key, event, raw, workflow.Job{})
if err != nil {
	return err
}
out, err := workflow.AwaitResult(ctx, c, ref, await)
```

The result ([`MachineResult`](ds:block?id=machineresult-2phczvcd)) reports whether the event took a transition (`Handled`), whether this run started the instance (`Started`), a stale timer (`Stale`), the active atomic states after it (`States`), whether the instance is done (`Done`) and its microstep number (`Seq`).

## Unhandled events

An event no active state takes is dropped by default ([`workflow.MachineIgnore`](ds:block?id=machineignore-ud9b526j)): the run succeeds with `Handled` false and nothing changes. With `MachineSpec.Unhandled` (an [`UnhandledPolicy`](ds:block?id=unhandledpolicy-dzznvx2d)) set to [`workflow.MachineReject`](ds:block?id=machinereject-z7kxh7ka), the run fails with a permanent [`workflow.ErrUnhandledEvent`](ds:block?id=errunhandledevent-92z9ny89) (it matches `workflow.ErrInvalid`), so the sender learns the event was not valid in the current state; an event sent to an instance that reached a top-level final state is rejected the same way. A guard that refuses is the same as no transition: the example's payment of 50 on a total of 120 is rejected.

A sender awaiting the run gets a `*workflow.RunFailedError` whose `Err` is the run's error text only, so `errors.Is` cannot see the sentinel through it; [`workflow.IsUnhandledEvent(err)`](ds:block?id=isunhandledevent-rxg2hpts) recognises both forms:

<!-- snippet: examples/state-machine/main.go#reject -->
```go
if workflow.IsUnhandledEvent(err) {
	fmt.Println("  rejected: no transition takes it in the current state")
	return nil
}
```

Rejected and ignored events change nothing and leave no entry in the log, with one exception: an event sent to a key never started starts the instance first, and that start is kept (and logged as [`workflow.MachineStartEvent`](ds:block?id=machinestartevent-xpyeh6q9)) even when the event itself is then rejected ([the event step](ds:block?id=machine-step-vvjdn77u)), so the initial entry actions never run twice.

## Read an instance and its log

[`workflow.MachineStatus[D](ctx, store, machine, key)`](ds:block?id=machinestatus-dgbvwcmx) reads an instance's last saved record from outside any event, for a query endpoint or a test; false means the key was never started. The [`workflow.MachineSnapshot[D]`](ds:block?id=machinesnapshot-8w3cnx9s) holds `Done`, the active atomic `States`, every active state with its ancestors in `Active`, `Data`, `Seq` and `Log`, and `In(state)` tests one state.

<!-- snippet: examples/state-machine/main.go#status -->
```go
snap, ok, err := workflow.MachineStatus[Order](ctx, store, "order", key)
if err != nil || !ok {
	return fmt.Errorf("order %s: found %t: %w", key, ok, err)
}
fmt.Printf("status of %s: states %v, done %t, paid %d of %d\n",
	key, snap.States, snap.Done, snap.Data.Paid, snap.Data.Total)
for _, step := range snap.Log {
	event := step.Event
	if event == workflow.MachineStartEvent {
		event = "(start)"
	}
	fmt.Printf("  %d %s %-15s %v -> %v\n",
		step.Seq, step.At.Format("15:04"), event, step.From, step.To)
}
```

Each [`workflow.MachineStep`](ds:block?id=machinestep-raj2wu8k) in the log is one handled event: the instance's `Seq` after it, the event's name (`workflow.MachineStartEvent` for the start), the atomic states before and after, and the time from `MachineSpec.Clock`. `Seq` counts microsteps, not events: `deliver` shows 8 because entering `delivered` was one transition and the done event's move to `completed` another.

```text
status of o-1: states [completed], done true, paid 120 of 120
  1 09:00 (start)         [] -> [cart]
  2 09:00 checkout        [cart] -> [awaiting packing]
  3 09:00 pay             [awaiting packing] -> [paid packing]
  4 09:00 ship            [paid packing] -> [paid in-transit]
  5 09:00 hold            [paid in-transit] -> [on-hold]
  6 09:00 resume          [on-hold] -> [paid in-transit]
  8 09:00 deliver         [paid in-transit] -> [completed]
```

The log keeps the last `MachineSpec.KeepLog` events: zero means [`workflow.DefaultMachineLog`](ds:block?id=defaultmachinelog-ujan3j33) (100), a negative value keeps none ([the event step](ds:block?id=machine-step-vvjdn77u)).

## Delete an instance

Instance records are never removed on their own: the [Journal plugin](ds:block?id=journal-pr52wnjp) forgets runs, and an instance is not a run ([`objectRun`](ds:block?id=objectrun-5ayyvf97) names its record). [`workflow.ForgetMachine(ctx, store, machine, key)`](ds:block?id=forgetmachine-t7tdwu6t) deletes one (its state, data, history and log) once it is no longer needed, typically after `MachineSnapshot.Done`. Afterwards `MachineStatus` reports the key as not found:

<!-- snippet: examples/machine-dsl/main.go#forget -->
```go
if err := workflow.ForgetMachine(ctx, store, machineName, "T-1"); err != nil {
	return err
}
_, found, err := workflow.MachineStatus[Ticket](ctx, store, machineName, "T-1")
if err != nil {
	return err
}
fmt.Fprintln(w, "T-1 after ForgetMachine, found:", found)
```

```text
T-1 after ForgetMachine, found: false
```

An event sent to the key afterwards starts a new instance, and a timer of the old one finds nothing to act on. It does not wait for an event being handled at that moment, which may write the record again.

## Guarantees

- **One event at a time per key.** Events of one instance are serialised on the reserved label [`workflow.LabelObjectKey`](ds:block?id=labelobjectkey-cn6pa8c2), set only through `SendEvent` and `StartMachine` ([`sendMachine`](ds:block?id=sendmachine-sukmmbd8)); events of different keys run in parallel. Without `Ordered` a key's events are handled in the order the driver claims them; with it, in the order they were sent.
- **Each event applied once** ([the event step](ds:block?id=machine-step-vvjdn77u)). An event's handling loads the record, takes its transitions and saves the new configuration and data together with the run's ID and result. A run retried after the save finds its own ID and returns the saved result without running anything again.
- **Actions at least once.** A failed action, or a crash before the save, runs the event's whole handling again, actions included. Give anything an action does outside the machine [`m.IdempotencyKey(name)`](ds:block?id=machinectx-idempotencykey-ep456p4b): a key unique to the instance, the microstep, the state and the name, identical when the event is handled again.

<!-- snippet: examples/state-machine/main.go#idempotency -->
```go
// shippedEmail enqueues the "shipped" email. Actions are at least once,
// so the job's IdempotencyKey is one that stays the same when the event
// is handled again after a crash: the email is sent once.
func shippedEmail(c workflow.Client) workflow.MachineAction[Order] {
	return func(ctx context.Context, m *workflow.MachineCtx[Order]) error {
		text, err := json.Marshal("order " + m.Key + " is on its way")
		if err != nil {
			return err
		}
		_, err = c.Enqueue(ctx, workflow.Job{
			Workflow:       "email",
			Payload:        text,
			IdempotencyKey: m.IdempotencyKey("shipped-email"),
		})
		return err
	}
}
```

- **Timers sent once per state entry**, by their idempotency key ([`sendTimers`](ds:block?id=machinerun-sendtimers-kuc82nwf)), and ignored once the entry they belong to is over.

## Drivers and storage

| Driver | Machines | Timers |
|---|---|---|
| Memory | Yes | Yes |
| SQL driver: SQLite, Postgres, MySQL | Yes | Yes |
| Redis | Yes | Yes |
| Hatchet | Refused at `Register` ([`checkSupported`](ds:block?id=hatchet-checksupported-733uwgsp)): its concurrency keys are engine expressions, and a machine's key is computed in Go | — |

There is no table of the machine's own: an instance is one record in the [`JournalStore`](ds:block?id=journalstore-tyc8j5tt) you pass, under the same internal run ID scheme as a virtual object's key ([`objectRun`](ds:block?id=objectrun-5ayyvf97)). The Journal plugin's retention never removes it; [`ForgetMachine`](ds:block?id=forgetmachine-t7tdwu6t) does.

## A machine, workflow steps or an object?

| Use | When |
|---|---|
| [Workflow steps](steps.md) | A process with a start and an end that one run carries through: a pipeline, an import, a saga. It waits with [signals](signals.md) and sleeps, and its history is the run's. |
| [A virtual object](objects.md) | An entity per key whose methods change its state freely, one call at a time: a cart, a balance, settings. Nothing restricts which call is valid when. |
| A state machine | An entity per key whose valid events depend on its state, that lives for days or never ends, with timeouts per state, nested or parallel states, resumption where it was, and a log of what happened. |

A machine's action can still start a workflow (`client.Enqueue` with [`m.IdempotencyKey`](ds:block?id=machinectx-idempotencykey-ep456p4b)), and a workflow step can send a machine an event with [`SendEvent`](ds:block?id=sendevent-ex33hwx5).

## Try it

```sh
cd examples
go run ./state-machine
```

```text
order o-1:
> start, total 120
  states: [cart]
> checkout
  exit cart
  enter fulfilment
  states: [awaiting packing]
> pay 50
  rejected: no transition takes it in the current state
> pay 120
  exit awaiting
  action: record payment of 120
  enter paid
  states: [paid packing]
> ship
  states: [paid in-transit]
  email: order o-1 is on its way
> hold
  exit fulfilment
  enter on-hold
  states: [on-hold]
> resume
  exit on-hold
  enter fulfilment
  enter paid
  states: [paid in-transit]
> deliver
  exit fulfilment
  enter completed
  states: [completed]
status of o-1: states [completed], done true, paid 120 of 120
  1 09:00 (start)         [] -> [cart]
  2 09:00 checkout        [cart] -> [awaiting packing]
  3 09:00 pay             [awaiting packing] -> [paid packing]
  4 09:00 ship            [paid packing] -> [paid in-transit]
  5 09:00 hold            [paid in-transit] -> [on-hold]
  6 09:00 resume          [on-hold] -> [paid in-transit]
  8 09:00 deliver         [paid in-transit] -> [completed]
order o-2:
> start, total 80
  states: [cart]
> checkout
  exit cart
  enter fulfilment
  states: [awaiting packing]
> 30 minutes pass without a payment
  exit awaiting
  exit fulfilment
  enter cancelled
status of o-2: states [cancelled], done true, paid 0 of 80
  1 09:00 (start)         [] -> [cart]
  2 09:00 checkout        [cart] -> [awaiting packing]
  3 09:30 payment-timeout [awaiting packing] -> [cancelled]
```

Order `o-1` goes through checkout, a payment the guard refuses, a payment that covers the total, shipping (which enqueues the email once), a hold and a deep-history resume, and delivery, after which the done event of `fulfilment` completes it. Order `o-2` is cancelled by its 30-minute timer when the fake clock moves. `go test ./state-machine` checks every line.

## Pitfalls

- Renaming or removing a state strands every instance in it: its next event fails permanently with "stored state is not in the machine" ([the event step](ds:block?id=machine-step-vvjdn77u)). Keep the old state until no instance is in it, with a transition out to the new one. History that names a removed state skips it ([`addDescendants`](ds:block?id=machinerun-adddescendants-7p23ezrv)).
- Timer events are machine-wide event names ([`StateTimer`](ds:block?id=statetimer-wwkknhcg)): a transition on `payment-timeout` from any active state takes the timer's event, and a client can send `payment-timeout` itself with `SendEvent`. Give each timer an event name only its own transitions use.
- Guards must be pure ([`MachineGuard`](ds:block?id=machineguard-yfjzyrtr)). A guard can run more than once per event. It gets a shallow copy of the data, so a change to a top-level field is dropped, but maps and slices are shared with the real data: a change to one of those from a guard would be saved.
- An event's handling may take at most [`workflow.MaxMachineMicrosteps`](ds:block?id=maxmachinemicrosteps-kxk4n8rh) (100) transitions, counting those of raised and done events; past it the event fails permanently ([`microstep`](ds:block?id=machinerun-microstep-33j5aqdq)), so a raise loop cannot spin for ever.
- Entry actions run on every entry: through history, a `Reenter` transition and a re-entered state alike ([`enter`](ds:block?id=machinerun-enter-w658d86g)). An action that must happen once per instance belongs on a transition, or behind `m.IdempotencyKey`.
- The log lives in the instance's record, and every handled event rewrites the record ([the event step](ds:block?id=machine-step-vvjdn77u)). Keep `KeepLog` small for instances with many events, or negative to keep none.
- A slow action holds up every later event of its key (`MaxRuns: 1` per key, set by [`DefineMachine`](ds:block?id=definemachine-zgu9ppny)). Keep actions short; start long work as a run of its own from an action.
- [`MachineStatus`](ds:block?id=machinestatus-dgbvwcmx) reads the last saved record; an event being handled now may replace it a moment later.
- With [`workflow.NewMemoryJournal`](ds:block?id=newmemoryjournal-cvvh3zbe) instances live in one process. Use the driver's durable journal for anything that must survive a restart.

## Related

- [Virtual objects](objects.md): the per-key model a machine is built on.
- [State machines as data](definitions-as-data.md#state-machines-as-data): the same machine from JSON, with guards and actions by registered name.
- [Wait for an event](signals.md) and [workflow steps](steps.md): processes one run carries from start to end.
- [Drive time in tests](../testing/fake-clock.md): the fake clock the example uses for timers.
- [Idempotency keys](../jobs/idempotency.md): what `m.IdempotencyKey` feeds.
- Examples: [examples/state-machine](../../examples/state-machine/), [examples/machine-dsl](../../examples/machine-dsl/).
