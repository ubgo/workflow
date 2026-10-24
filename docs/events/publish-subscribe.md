# Publish and subscribe

When an order is paid, several things should happen: a receipt is mailed, the CRM is updated, a fulfilment run starts. Calling each from the payment handler couples the handler to every reaction. `events.Bus` decouples them: the handler publishes `order.paid` once, and each subscription starts its own run of its own workflow. A bus is not a broker: `Publish` turns one event into one `Enqueue` per matching subscription on an ordinary client, so on a durable driver every subscriber's run is stored by the time `Publish` returns, and nothing else needs to run.

## Before you start

- The bus deduplicates by event ID through idempotency keys, so `events.New` refuses a client without `workflow.CapIdempotencyKey` with an `*workflow.UnsupportedError`, unless you pass `events.AllowDuplicates()`. Every driver declares the capability.
- Subscriptions are code, registered at boot in every process that publishes, as workflows are.
- The example is [`examples/events-webhooks`](../../examples/events-webhooks). Its runs hold until the program ends, so the deduplication of unfinished runs is visible. Run it with `go run ./events-webhooks` from `examples/`.

## Publish an event

`events.New(client, opts...)` returns a bus; `Subscribe` adds a `events.Subscription` and `Publish` sends an `events.Message`:

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

A subscription matches a message whose `Name` equals its `Event` exactly and, when it has a `Filter`, that the filter accepts. Its `Name` must be unique within the bus and stable across deploys, because it is part of every run's idempotency key. `Workflow` names the workflow to run with the message's payload; set `Job` instead to build the job yourself (another payload, a queue, a priority, `RunAt`), and the bus still adds its labels and, when the job has none, its idempotency key. `Subscribe` returns an error wrapping `workflow.ErrInvalid` when the subscription has no `Name` or `Event`, has neither `Workflow` nor `Job`, or reuses a name. `Bus.Subscriptions()` lists the names in order.

`Publish` returns an `events.Published`: the message's `ID`, one `events.Delivery` per matching subscription (its name, the run's `RunRef` and its error), and whether the message was signalled. A message matching nothing is not an error. A message without a `Name`, or with labels under a reserved prefix, is refused with `ErrInvalid`.

Every run the bus starts carries three labels under the reserved internal prefix `ubgo.workflow/`, beside the message's own `Labels`: `events.LabelEvent` (the event name), `events.LabelEventID` (its ID) and `events.LabelSubscription` (the subscription that started it). The bus sets them itself, overwriting any value. A driver's `Enqueue` refuses app labels under a reserved prefix (`workflow.CheckJobLabels`) and the bus allows only these three keys, so on a driver that enforces `CheckJobLabels` they cannot arrive by accident or from untrusted input; like every reserved label they are a guard, not a sandbox. Labels under a reserved prefix in the job a `Subscription.Job` returns are refused too: that delivery fails with `ErrInvalid`, in its `Delivery.Err` and in the joined error, and the other deliveries go ahead.

## Publish again safely

Every run carries the idempotency key `event/<subscription>/<id>`. Publishing the same ID again, after a timeout or a crash between the publish and your own commit, starts nothing new while the first runs are unfinished; the deliveries return the first runs' refs:

<!-- snippet: examples/events-webhooks/main.go#republish -->
```go
again, err := b.Publish(ctx, msg) // a retry after a timeout: same ID
if err != nil {
	return err
}
for i, d := range again.Deliveries {
	fmt.Fprintf(out, "  republish of %s returned the first run: %v\n", d.Subscription, d.Ref == first.Deliveries[i].Ref)
}
```

Every matching subscription is tried even when one fails, and the error joins each failure, so the caller publishes again with the same ID and only the deliveries that failed start a run. A message without an `ID` gets a new one, 16 random bytes in hex by default or from `events.WithIDFunc` (a UUIDv7 or ULID generator); such a publish is never deduplicated, so give events that may be published twice an ID of your own, such as the payment provider's event ID.

## Deliver to waiting runs

With `events.WithSignal()`, `Publish` also calls `client.Signal` with the message's `Name`, `Key` and `Payload`, so a run already parked on a [wait step](../workflows/signals.md) for that event and key continues, and a new subscriber's run and a waiting run see the same event. `Published.Signalled` reports whether the signal went out without error; a signal error joins the publish error.

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

## Pitfalls

- **An event is only as durable as the client.** On the memory driver, published events are lost with the process. Use a durable driver. `Publish` is not part of your database transaction, so a crash between your commit and the publish loses the event; when that matters, write the job inside the transaction with [transactional enqueue](../reliability/transactional-enqueue.md) or the [outbox](../reliability/outbox.md) instead.
- **Deduplication lasts while the run is unfinished.** The key is the client's idempotency key, freed when the run ends; a republish after that starts new runs. Make subscriber workflows idempotent, or record what they did.
- **Renaming a subscription changes its keys.** A republish across a deploy that renamed it starts a second run.
- **Subscriptions live in each process.** A process that publishes before registering a subscription starts no run for it.
- **No ID means no deduplication.** A message published without an `ID` gets a fresh one every time.

## Related

- [Inbound webhooks](webhooks.md), [Events](README.md)
- [Wait for an event](../workflows/signals.md), [Idempotency keys](../jobs/idempotency.md), [Outbox](../reliability/outbox.md)
- Example: [examples/events-webhooks](../../examples/events-webhooks/)
