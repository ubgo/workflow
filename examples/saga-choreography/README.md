# saga-choreography

A saga with no orchestrator, on the in-memory driver: three services (order, payment, inventory), each a workflow whose step reacts to an event and publishes the next one through an `events.Bus` with `events.WithCorrelation`. Order 1001 is paid, so the payment service publishes `payment.captured` and the order service confirms. Order 1002 has a declined card, so the payment service publishes `payment.failed`, the order service compensates with `order.cancelled`, and the inventory service releases the stock it reserved. `events.Trail` then lists each order's runs as one unit, drawn as a tree of events through their causation labels.

```sh
cd examples
go run ./saga-choreography
```

```text
order 1001 (card ok): 3 runs succeeded, 0 failed
order 1002 (card declined): 4 runs succeeded, 0 failed
inventory: reserved 2, released 1
trail order-1001 (3 runs):
  order.placed o1001 -> charge succeeded, reserve succeeded
    payment.captured payment.captured/o1001 -> confirm-order succeeded
trail order-1002 (4 runs):
  order.placed o1002 -> charge succeeded, reserve succeeded
    payment.failed payment.failed/o1002 -> cancel-order succeeded
      order.cancelled order.cancelled/payment.failed/o1002 -> release succeeded
```

## The code

The bus with correlation on, and one subscription per reaction:

<!-- snippet: examples/saga-choreography/main.go#bus -->
```go
bus, err := events.New(c, events.WithCorrelation()) // every run gets the chain's labels
if err != nil {
	return err
}
subs := []events.Subscription{
	{Name: "payment-charge", Event: "order.placed", Workflow: "charge"},
	{Name: "inventory-reserve", Event: "order.placed", Workflow: "reserve"},
	{Name: "order-confirm", Event: "payment.captured", Workflow: "confirm-order"},
	{Name: "order-compensate", Event: "payment.failed", Workflow: "cancel-order"},
	{Name: "inventory-release", Event: "order.cancelled", Workflow: "release"},
}
for _, s := range subs {
	if err := bus.Subscribe(s); err != nil {
		return err
	}
}
```

A service: a step that does its work and publishes the next event with the step's `ctx`, under an ID derived from the event that started it:

<!-- snippet: examples/saga-choreography/main.go#service -->
```go
// react turns a service into a workflow whose step publishes the next
// event through the bus. The event's ID is derived from the event that
// started this run, so a retried step publishes the same ID again and the
// bus starts nothing twice; WithCorrelation reads the chain from ctx.
func react(bus *events.Bus, name string, do service) workflow.Workflow {
	return workflow.Workflow{Name: name, Steps: []workflow.Step{{Name: "react", Run: func(ctx context.Context, p []byte) ([]byte, error) {
		var o order
		if err := json.Unmarshal(p, &o); err != nil {
			return nil, workflow.Permanent(err)
		}
		next, err := do(ctx, o)
		if err != nil || next == "" {
			return nil, err
		}
		info, _ := workflow.StepInfoFrom(ctx)
		id := next + "/" + info.Labels[events.LabelEventID]
		_, err = bus.Publish(ctx, events.Message{ID: id, Name: next, Payload: p})
		return nil, err
	}}}}
}
```

The first event, under an explicit chain ID, and the chain listed afterwards:

<!-- snippet: examples/saga-choreography/main.go#publish -->
```go
_, err = bus.Publish(ctx, events.Message{
	ID:          fmt.Sprintf("o%d", o.ID),
	Name:        "order.placed",
	Correlation: fmt.Sprintf("order-%d", o.ID), // optional: the chain's ID; the message ID by default
	Payload:     payload,
})
if err != nil {
	return err
}
```

<!-- snippet: examples/saga-choreography/main.go#trail -->
```go
runs, err := events.Trail(ctx, c, corr, workflow.RunQuery{})
if err != nil {
	return err
}
```

The guide that copies this program is [Choreography and saga choreography](../../docs/events/choreography.md). `go test ./saga-choreography` checks every line.
