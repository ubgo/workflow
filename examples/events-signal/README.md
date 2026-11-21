# events-signal

What an `events.Bus` reports beyond starting runs, on the in-memory driver: a bus built with `events.WithSignal()` that both starts a subscriber's run and wakes the order run waiting for that payment, while a run waiting on another key waits on; a `Bus.Replay` while one subscriber is down, whose `ReplayResult` lists each `ReplayDelivery` with its failure, then the same replay ID again finding the delivered runs as `Existing` and starting only the failed ones; a cancel subscription whose key comes from the message's labels, its `Cancellation`, the default `events.UnfinishedStatuses`, and a message with no key refused with `ErrInvalid`; and `events.ValidExtensionName`, the rule that decides which labels `FromMessage` carries into a CloudEvent.

```sh
cd examples
go run ./events-signal
```

```text
signal:
  signalled: true, subscriber runs started: 1
  order A-1 woke and shipped with {"order":"A-1","cents":4000}
  order A-2 is still waiting: the event's key was A-1
replay result:
  replay backfill-1: 2 messages, 4 deliveries, error: true
    inv_1 -> receipt existing=false failed=false
    inv_1 -> ledger  existing=false failed=true
    inv_2 -> receipt existing=false failed=false
    inv_2 -> ledger  existing=false failed=true
  the ledger is back; the same replay ID again:
    inv_1 -> receipt existing=true  failed=false
    inv_1 -> ledger  existing=false failed=false
    inv_2 -> receipt existing=true  failed=false
    inv_2 -> ledger  existing=false failed=false
cancellation:
  default statuses: scheduled, queued, running, waiting, retrying
  stop-reminders: cancelled 1, diverted false, err <nil>
  no key: cancelled 0, Cancellation.Err is ErrInvalid: true, Publish error: true
extension names:
  "tenant"     valid=true
  "region2"    valid=true
  "tenant-id"  valid=false
  "Tenant"     valid=false
  "subject"    valid=false
  ""           valid=false
  FromMessage kept: map[tenant:acme]
```

## The code

A bus that also signals waiting runs, and the order workflow's wait step that takes its key from the run's label:

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

A replay that reports each delivery, then the same replay ID again:

<!-- snippet: examples/events-signal/main.go#replay-result -->
```go
r, err := bus.Replay(ctx, events.ReplayOptions{ID: "backfill-1"}) // zero From and To: the whole log
fmt.Fprintf(p.w, "  replay %s: %d messages, %d deliveries, error: %v\n", r.ID, r.Messages, len(r.Deliveries), err != nil)
for _, d := range r.Deliveries {
	fmt.Fprintf(p.w, "    %s -> %-7s existing=%-5v failed=%v\n", d.MessageID, d.Subscription, d.Existing, d.Err != nil)
}
```

<!-- snippet: examples/events-signal/main.go#replay-again -->
```go
again, err := bus.Replay(ctx, events.ReplayOptions{ID: "backfill-1"}) // retries only what failed
if err != nil {
	return err
}
for _, d := range again.Deliveries {
	fmt.Fprintf(p.w, "    %s -> %-7s existing=%-5v failed=%v\n", d.MessageID, d.Subscription, d.Existing, d.Err != nil)
}
```

A cancel subscription and what it reports:

<!-- snippet: examples/events-signal/main.go#cancellation -->
```go
err = bus.SubscribeCancel(events.CancelSubscription{
	Name:  "stop-reminders",
	Event: evCheckedOut,
	Label: labelCart,
	Key:   func(m events.Message) string { return m.Labels[labelCart] }, // not m.Key
})
if err != nil {
	return err
}
pub, err := bus.Publish(ctx, events.Message{ID: "chk_1", Name: evCheckedOut, Labels: map[string]string{labelCart: "c1"}})
if err != nil {
	return err
}
cn := pub.Cancellations[0]
fmt.Fprintf(p.w, "  %s: cancelled %d, diverted %v, err %v\n", cn.Subscription, cn.Cancelled, !cn.Diverted.IsZero(), cn.Err)
```

<!-- snippet: examples/events-signal/main.go#cancellation-nokey -->
```go
pub, err = bus.Publish(ctx, events.Message{ID: "chk_2", Name: evCheckedOut}) // no cart label
fmt.Fprintf(p.w, "  no key: cancelled %d, Cancellation.Err is ErrInvalid: %v, Publish error: %v\n",
	pub.Cancellations[0].Cancelled, errors.Is(pub.Cancellations[0].Err, workflow.ErrInvalid), err != nil)
```

Which names may name a CloudEvents extension:

<!-- snippet: examples/events-signal/main.go#extension-names -->
```go
for _, name := range []string{"tenant", "region2", "tenant-id", "Tenant", "subject", ""} {
	fmt.Fprintf(p.w, "  %-12q valid=%v\n", name, events.ValidExtensionName(name))
}
ce := events.FromMessage(events.Message{ID: "evt_1", Name: "order.paid",
	Labels: map[string]string{"tenant": "acme", "tenant-id": "7", "subject": "x"}}, "https://shop.example/orders")
fmt.Fprintf(p.w, "  FromMessage kept: %v\n", ce.Extensions) // the other two labels have no CloudEvents spelling
```

The guides that copy this program are [Publish and subscribe](../../docs/events/publish-subscribe.md#deliver-to-waiting-runs), [Event log, replay and cancel on event](../../docs/events/replay-and-cancel.md#read-what-a-replay-did) and [CloudEvents and payload versions](../../docs/events/cloudevents.md#check-an-extension-name). `go test ./events-signal` checks every line.
