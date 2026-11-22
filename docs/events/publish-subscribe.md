# Publish and subscribe

When an order is paid, several things should happen: a receipt is mailed, the CRM is updated, a fulfilment run starts. Calling each from the payment handler couples the handler to every reaction. [`events.Bus`](ds:block?id=events-bus-r886maq3) decouples them: the handler publishes `order.paid` once, and each subscription starts its own run of its own workflow. A bus is not a broker: [`Publish`](ds:block?id=events-bus-publish-k8u3yru9) turns one event into one `Enqueue` per matching subscription on an ordinary client, so on a durable driver every subscriber's run is stored by the time `Publish` returns, and nothing else needs to run.

## Before you start

- The bus deduplicates by event ID through idempotency keys, so [`events.New`](ds:block?id=events-new-k66keudp) refuses a client without `workflow.CapIdempotencyKey` with an `*workflow.UnsupportedError`, unless you pass [`events.AllowDuplicates()`](ds:block?id=events-allowduplicates-cv3prm2t). Every driver declares the capability: [memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-caps-yu37ef2j), [Redis](ds:block?id=redis-caps-bfgdst9t) and [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z).
- Subscriptions are code, registered at boot in every process that publishes, as workflows are.
- The example is [`examples/events-webhooks`](../../examples/events-webhooks). Its runs hold until the program ends, so the deduplication of unfinished runs is visible. Run it with `go run ./events-webhooks` from `examples/`.
- Delivering to waiting runs is shown by [`examples/events-signal`](../../examples/events-signal), run with `go run ./events-signal` from `examples/`. It needs a driver with wait steps (`workflow.CapWaitForEvent`): [memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-caps-yu37ef2j), [Redis](ds:block?id=redis-caps-bfgdst9t) and [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z) declare it.

## Publish an event

`events.New(client, opts...)` returns a bus; [`Subscribe`](ds:block?id=events-bus-subscribe-7fpvhncr) adds a [`events.Subscription`](ds:block?id=events-subscription-fyyhdwra) and `Publish` sends an [`events.Message`](ds:block?id=events-message-egxsxxf5):

<!-- snippet: examples/events-webhooks/main.go#bus -->
```go
b, err := events.New(c)
if err != nil {
	return err
}
err = errors.Join(
	b.Subscribe(events.Subscription{Name: "receipt", Event: "order.paid", Workflow: "send-receipt"}),
	b.Subscribe(events.Subscription{Name: "crm", Event: "order.paid", Workflow: "update-crm",
		Filter: func(m events.Message) bool { return m.Labels["channel"] != "pos" }}),
)
if err != nil {
	return err
}
msg := events.Message{ID: "evt_1", Name: "order.paid", Payload: []byte(`{"order":42}`),
	Labels: map[string]string{"channel": "web"}}
first, err := b.Publish(ctx, msg)
if err != nil {
	return err
}
```

A subscription matches a message whose `Name` equals its `Event` exactly and, when it has a `Filter`, that the filter accepts ([`Publish`](ds:block?id=events-bus-publish-k8u3yru9)). Its `Name` must be unique within the bus and stable across deploys, because it is part of every run's idempotency key. `Workflow` names the workflow to run with the message's payload; set `Job` instead to build the job yourself (another payload, a queue, a priority, `RunAt`), and the bus still adds its labels and, when the job has none, its idempotency key ([`deliver`](ds:block?id=events-bus-deliver-zntu7tc7)). [`Subscribe`](ds:block?id=events-bus-subscribe-7fpvhncr) returns an error wrapping `workflow.ErrInvalid` when the subscription has no `Name` or `Event`, has neither `Workflow` nor `Job`, or reuses a name. [`Bus.Subscriptions()`](ds:block?id=events-bus-subscriptions-c6u993h6) lists the names in order.

[`Publish`](ds:block?id=events-bus-publish-k8u3yru9) returns an [`events.Published`](ds:block?id=events-published-p4vqqdu7): the message's `ID`, one [`events.Delivery`](ds:block?id=events-delivery-9sgr6u6u) per matching subscription (its name, the run's `RunRef` and its error), and whether the message was signalled. A message matching nothing is not an error. A message without a `Name`, or with labels under a reserved prefix, is refused with `ErrInvalid`.

Every run the bus starts carries three labels under the reserved internal prefix `ubgo.workflow/`, beside the message's own `Labels`: [`events.LabelEvent`](ds:block?id=events-labelevent-uazmzegg) (the event name), `events.LabelEventID` (its ID) and `events.LabelSubscription` (the subscription that started it). The bus sets them itself, overwriting any value ([`deliver`](ds:block?id=events-bus-deliver-zntu7tc7)). A driver's `Enqueue` refuses app labels under a reserved prefix ([`workflow.CheckJobLabels`](ds:block?id=checkjoblabels-p6m28b9h)) and the bus allows only its own keys ([`workflow.AllowReservedLabels`](ds:block?id=allowreservedlabels-4czg8eyz): these three, plus the replay and correlation labels when those are in use), so on a driver that enforces `CheckJobLabels` they cannot arrive by accident or from untrusted input; like every reserved label they are a guard, not a sandbox. Labels under a reserved prefix in the job a `Subscription.Job` returns are refused too: that delivery fails with `ErrInvalid`, in its `Delivery.Err` and in the joined error, and the other deliveries go ahead.

## Publish again safely

Every run carries the idempotency key `event/<subscription>/<id>` ([`deliver`](ds:block?id=events-bus-deliver-zntu7tc7)). Publishing the same ID again, after a timeout or a crash between the publish and your own commit, starts nothing new while the first runs are unfinished; the deliveries return the first runs' refs:

<!-- snippet: examples/events-webhooks/main.go#republish -->
```go
again, err := b.Publish(ctx, msg) // a retry after a timeout: same ID
if err != nil {
	return err
}
for i, d := range again.Deliveries {
	fmt.Printf("  republish of %s returned the first run: %v\n", d.Subscription, d.Ref == first.Deliveries[i].Ref)
}
```

Every matching subscription is tried even when one fails, and the error joins each failure ([`Publish`](ds:block?id=events-bus-publish-k8u3yru9)), so the caller publishes again with the same ID and only the deliveries that failed start a run. A message without an `ID` gets a new one, 16 random bytes in hex by default ([`randomID`](ds:block?id=events-randomid-d72gyvx6)) or from [`events.WithIDFunc`](ds:block?id=events-withidfunc-3kfzh5x7) (a UUIDv7 or ULID generator); such a publish is never deduplicated, so give events that may be published twice an ID of your own, such as the payment provider's event ID.

## Deliver to waiting runs

A subscription starts a new run. A run that is already parked on a [wait step](../workflows/signals.md) for the same thing (an order run waiting for its payment) needs the event too, and publishing it and calling `client.Signal` separately means two calls that can half fail. With [`events.WithSignal()`](ds:block?id=events-withsignal-h9xdtvt4), [`Publish`](ds:block?id=events-bus-publish-k8u3yru9) does both: after starting the subscribers' runs it calls `client.Signal(ctx, workflow.Event{Name, Key, Payload})` with the message's `Name`, `Key` and `Payload`.

<!-- snippet: examples/events-signal/main.go#signal-bus -->
```go
bus, err := events.New(p.c, events.WithSignal()) // Publish also signals waiting runs
if err != nil {
	return err
}
err = bus.Subscribe(events.Subscription{Name: "receipt", Event: evPaid, Workflow: wfReceipt})
if err != nil {
	return err
}
```

The waiting run is an ordinary workflow whose wait step takes its key from the run's own labels, so each order waits for its own payment:

<!-- snippet: examples/events-signal/main.go#wait-step -->
```go
{
	Name: "await-payment",
	Wait: &workflow.WaitSpec{
		Event:   evPaid,
		KeyFrom: workflow.KeyFunc(func(j workflow.Job) string { return j.Labels[labelOrder] }), // its own order
	},
},
```

The message's `Key` is what the waiting run matches, so set it to the same value the wait step's key resolves to:

<!-- snippet: examples/events-signal/main.go#signal-publish -->
```go
pub, err := bus.Publish(ctx, events.Message{
	ID:      "pay_1",
	Name:    evPaid,
	Key:     "A-1", // the key the waiting run matches: its order
	Payload: []byte(`{"order":"A-1","cents":4000}`),
})
if err != nil {
	return err // a failed signal is joined in here; Signalled is then false
}
fmt.Fprintf(p.w, "  signalled: %v, subscriber runs started: %d\n", pub.Signalled, len(pub.Deliveries))
```

```text
signal:
  signalled: true, subscriber runs started: 1
  order A-1 woke and shipped with {"order":"A-1","cents":4000}
  order A-2 is still waiting: the event's key was A-1
```

The run for order A-1 continued and its next step received the message's payload; the run for A-2 waits on, because the key did not match. [`Published.Signalled`](ds:block?id=events-published-p4vqqdu7) is true when the signal went out without error. A signal error is joined into the error `Publish` returns, after the deliveries, so the subscribers' runs are already stored: publishing again with the same ID starts no second run (the deduplication above) and signals again.

Once sent, the signal follows the [`Client.Signal`](ds:block?id=client-signal-2sdhmcdy) contract every driver keeps: it resumes every run waiting with that name and key, not only the first; a run enqueued before the publish that has not reached its wait step yet still takes it when it gets there; a run enqueued after the publish never does; and a signal nobody waits for is not an error. A wait matches only a message with exactly its key, and an empty `Key` matches a wait with an empty key. See [Wait for an event](../workflows/signals.md) for the wait step itself, its timeout and fan-in.

## Try it

```sh
cd examples
go run ./events-webhooks
```

The bus's part:

```text
bus:
  receipt started a run: true
  crm started a run: true
  republish of receipt returned the first run: true
  republish of crm returned the first run: true
  a pos order matches 1 subscription(s)
  run labels: event=order.paid id=evt_1 subscription=crm channel=web
```

Delivering to a waiting run:

```sh
go run ./events-signal
```

The first section of its output is the one shown in [Deliver to waiting runs](#deliver-to-waiting-runs).

## Pitfalls

- **An event is only as durable as the client.** On the memory driver, published events are lost with the process: it does not declare `CapDurable` ([memory](ds:block?id=memory-caps-yxkuq583)). Use a durable driver. [`Publish`](ds:block?id=events-bus-publish-k8u3yru9) is not part of your database transaction, so a crash between your commit and the publish loses the event; when that matters, write the job inside the transaction with [transactional enqueue](../reliability/transactional-enqueue.md) or the [outbox](../reliability/outbox.md) instead.
- **Deduplication lasts while the run is unfinished.** The key is the client's idempotency key ([`deliver`](ds:block?id=events-bus-deliver-zntu7tc7)), freed when the run ends; a republish after that starts new runs. Make subscriber workflows idempotent, or record what they did.
- **Renaming a subscription changes its keys.** The [`Subscription`](ds:block?id=events-subscription-fyyhdwra) name is part of the key. A republish across a deploy that renamed it starts a second run.
- **Subscriptions live in each process.** The [`Bus`](ds:block?id=events-bus-r886maq3) holds them in memory. A process that publishes before registering a subscription starts no run for it.
- **No ID means no deduplication.** A message published without an `ID` gets a fresh one every time ([`Publish`](ds:block?id=events-bus-publish-k8u3yru9)).
- **`WithSignal` signals with `Message.Key`, nothing else.** [`Publish`](ds:block?id=events-bus-publish-k8u3yru9) passes `Name`, `Key` and `Payload` to `Signal`. A message whose `Key` is empty wakes only waits with an empty key, not the run whose label holds the order number; set `Key` to what the wait step's `Key` or `KeyFrom` resolves to.
- **The signal carries the upcast payload.** [`Publish`](ds:block?id=events-bus-publish-k8u3yru9) upcasts ([`upcast`](ds:block?id=events-bus-upcast-rwukhj2a)) before it signals. A signalled run gets the message's `Payload` after upcasting ([payload versions](cloudevents.md#version-an-events-payload)), so the step after the wait reads the current shape.

## Related

- [Inbound webhooks](webhooks.md), [Events](README.md)
- [Wait for an event](../workflows/signals.md), [Idempotency keys](../jobs/idempotency.md), [Outbox](../reliability/outbox.md)
- Examples: [examples/events-webhooks](../../examples/events-webhooks/), [examples/events-signal](../../examples/events-signal/)
