# state-machine

A durable state machine on the in-memory driver: an order's lifecycle as a statechart, one instance per order key. It shows a guard, entry, exit and transition actions, compound states, a parallel state whose payment and shipping regions are joined by a done event, deep history (an order put on hold resumes where it was), a 30-minute timer per state on a fake clock, a rejected event and each instance's log.

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

The machine:

```text
cart
fulfilment (parallel)
├── payment:  awaiting (30-minute timer) → paid (final)
└── shipping: packing → in-transit → delivered (final)
on-hold
completed (final)
cancelled (final)
```

## The code

The instance's data, the guard of "pay" and its action:

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

The states:

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

The transitions:

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

Define the machine:

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

Start an instance, and send it an event:

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

<!-- snippet: examples/state-machine/main.go#event -->
```go
ref, err := workflow.SendEvent(ctx, c, "order", key, event, raw, workflow.Job{})
if err != nil {
	return err
}
out, err := workflow.AwaitResult(ctx, c, ref, await)
```

Read an instance and its log from outside:

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

The guide is [docs/workflows/state-machines.md](../../docs/workflows/state-machines.md).
