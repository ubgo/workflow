# Choreography and saga choreography

An order touches three services. The payment service charges the card, the inventory service reserves stock, and when the card is declined the order must be cancelled and the stock released again. With choreography no orchestrator drives these steps: each service reacts to the events it cares about and publishes its own, its compensation events included. An [`events.Bus`](ds:block?id=events-bus-r886maq3) already lets services react; the gap is that nothing sees the saga as a whole, so "what happened to order 1002" has no single place to look. [`events.WithCorrelation`](ds:block?id=events-withcorrelation-tnjs2y7e) stamps every run a chain of events starts with the chain's ID and the event that caused it, and [`events.Trail`](ds:block?id=events-trail-pyxgkv3b) lists that chain's runs as one unit.

## Before you start

- The labels are written on every driver ([`chainLabels`](ds:block?id=events-bus-chainlabels-t4yjtmj3)). [`Trail`](ds:block?id=events-trail-pyxgkv3b) needs `workflow.RunQuerier` with `workflow.CapRunQuery` ([memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-paritycaps-e5y5vqhk), [Redis](ds:block?id=redis-caps-bfgdst9t), [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z)) and refuses a client without it with an `*workflow.UnsupportedError` naming `CapRunQuery`.
- It is off by default: a bus without [`WithCorrelation`](ds:block?id=events-withcorrelation-tnjs2y7e) writes no new label and behaves exactly as before.
- The example is [`examples/saga-choreography`](../../examples/saga-choreography). Run it with `go run ./saga-choreography` from `examples/`.

## Turn on correlation

[`events.WithCorrelation()`](ds:block?id=events-withcorrelation-tnjs2y7e) on `events.New` makes every run the bus starts (`Publish`, a divert, `Replay`) carry two labels beside the bus's own three ([`chainLabels`](ds:block?id=events-bus-chainlabels-t4yjtmj3)):

- [`events.LabelCorrelation`](ds:block?id=events-labelcorrelation-2f3h8pg4): the chain's ID. A message published from a step whose run carries it inherits it; any other message starts a chain named after its own ID ([`resolveChain`](ds:block?id=events-resolvechain-eg8jtcb6)).
- `events.LabelCausation`: the ID of the event that started the run which published this one. Runs of the first event have none ([`resolveChain`](ds:block?id=events-resolvechain-eg8jtcb6)).

The subscriptions are ordinary ones, one per reaction, across services:

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

## React and publish from a step

Each service is a workflow whose step does its work and publishes the next event through the bus. The step passes its own `ctx` to `Publish`, which is how the bus finds the run's chain ([`workflow.StepInfoFrom`](ds:block?id=stepinfofrom-rr9mwh2c), read by [`resolveChain`](ds:block?id=events-resolvechain-eg8jtcb6)):

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

The payment service publishes `payment.failed` for a declined card; the order service reacts with `order.cancelled`, its compensation event; the inventory service reacts to that by releasing the stock. None of them knows the others exist.

## Start a chain

The first event starts the chain. Its ID names the chain unless [`Message.Correlation`](ds:block?id=events-message-egxsxxf5) gives one, such as an order number or an ID carried in from another system:

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

`Message.Causation` can be set the same way when the cause is known outside a step. Both fields are filled before the message is logged (`WithLog`), so a `Replay` keeps each message in its chain ([`Publish`](ds:block?id=events-bus-publish-k8u3yru9)). A bus without `WithCorrelation` refuses a message that sets either field, with `ErrInvalid`, rather than dropping it ([`checkChain`](ds:block?id=events-checkchain-vhtvgshe)).

## List a chain

[`events.Trail(ctx, client, correlation, q)`](ds:block?id=events-trail-pyxgkv3b) lists every run whose `LabelCorrelation` is `correlation`, following `RunPage.Next` to the last page, newest first as the querier returns them. `q` narrows it (a workflow, statuses, a time range, more labels); `q.Limit` is the page size:

<!-- snippet: examples/saga-choreography/main.go#trail -->
```go
runs, err := events.Trail(ctx, c, corr, workflow.RunQuery{})
if err != nil {
	return err
}
```

Grouping the runs by `LabelEventID` and linking each group to its `LabelCausation` draws the saga as a tree of events:

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

A run that failed, or one still waiting, shows up in the trail with its status ([`Trail`](ds:block?id=events-trail-pyxgkv3b) lists every status unless `q.Statuses` narrows it), so a stuck saga is found by its correlation, not by searching logs.

## Try it

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

Order 1002's declined card took the compensation path: `payment.failed` started `cancel-order`, whose `order.cancelled` released the stock, so inventory shows two reservations and one release. Each indented line is an event whose `LabelCausation` names the line above it, and every event ID after the first is derived from its cause, which is why a retried step could not start the next service twice. `go test ./saga-choreography` checks every line.

## Choreography or an orchestrated saga

[`workflow.Saga`](../workflows/sagas.md) is the orchestrated form: one step holds every action and its compensation, runs them in order and undoes the finished ones newest first ([`Saga`](ds:block?id=saga-pk2fwf5w)). Pick it when one team owns the whole flow and the order of compensations matters. Choreography suits services owned apart that should not know each other: each adds a subscription and publishes events, and no code anywhere holds the whole flow. The price is that the flow lives only in the subscriptions, so [`Trail`](ds:block?id=events-trail-pyxgkv3b) is how you see it, and the compensation order is whatever the events make it.

## Pitfalls

- Pass the step's `ctx` to `Publish`. A `context.Background()` inside a step loses the chain ([`resolveChain`](ds:block?id=events-resolvechain-eg8jtcb6) reads the run from `ctx`): the event starts a new correlation and has no causation.
- Give an event published from a step an ID derived from the event that started the run (`next + "/" + info.Labels[events.LabelEventID]`). A random ID makes a retried step start the next service twice; a derived one is deduplicated by the bus ([`deliver`](ds:block?id=events-bus-deliver-zntu7tc7)).
- `LabelCausation` names the event, not the run ([`resolveChain`](ds:block?id=events-resolvechain-eg8jtcb6) takes the run's `LabelEventID`): when two subscribers of one event both publish, their events share a causation. Tell them apart by the event's name or ID.
- The labels are reserved ([`LabelCorrelation`](ds:block?id=events-labelcorrelation-2f3h8pg4)). `Publish` refuses them in `Message.Labels` and in a `Subscription.Job`'s labels ([`ValidateLabels`](ds:block?id=validatelabels-henfu882)), and a public `Enqueue` refuses them ([`CheckJobLabels`](ds:block?id=checkjoblabels-p6m28b9h)), with `ErrInvalid`; only the bus sets them.
- A trail is as long as the chain: [`Trail`](ds:block?id=events-trail-pyxgkv3b) follows every page. For a chain that loops (an event that leads back to itself), narrow `q` with `Workflow`, `Statuses` or `From`, or the listing grows with every turn.

## Related

- [Publish and subscribe](publish-subscribe.md), [Event log, replay and cancel](replay-and-cancel.md), [Events](README.md)
- [Sagas](../workflows/sagas.md), [Search runs by label](../jobs/manage-runs.md#search-by-label)
- Example: [examples/saga-choreography](../../examples/saga-choreography/)
