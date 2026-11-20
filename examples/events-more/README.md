# events-more

What sits around an `events.Bus`, on the in-memory driver: a CloudEvent sent in HTTP binary mode, read with `events.ReadHTTP` and published with `events.ToMessage`, then written back in the JSON format; an `order.paid` payload at version 0 upcast to version 2 with `events.WithUpcasters`, and a version 3 one refused; an event log with `events.WithLog` and a `Bus.Replay` of one hour for one subscription, repeated without starting anything twice; a cancel subscription that stops a cart's reminder run on checkout; a divert that cancels an order's fulfilment and starts a refund, which a republish does not repeat; and `workflow.Then` starting `index` after a successful import and `alert` after a failed one.

```sh
cd examples
go run ./events-more
```

```text
cloudevents:
  headers: ce-id=evt_1 ce-type=order.paid ce-subject=42 ce-tenant=acme
  message: name=order.paid key=42 labels=map[tenant:acme]
  receipt ran with {"order":42,"cents":1999,"currency":"EUR"}
  structured: {"specversion":"1.0","id":"evt_1","source":"https://shop.example/orders","type":"order.paid","subject":"42","tenant":"acme","data":{"order":42,"cents":1999,"currency":"EUR"}}
versions:
  v0 message reached the receipt run as {"order":7,"cents":500,"currency":"EUR"}
  v3 message refused: ErrInvalid=true
replay:
  log holds 3 messages; replayed 2 to receipt: ["inv_2" "inv_3"]
  same replay ID again: 2 of 2 already started, new receipt runs 0
cancel on event:
  stop-cart-reminders cancelled 1 run(s)
  cart c1 reminder: cancelled; cart c2 reminder: running
divert:
  cancelled 1 fulfilment run(s); refund ran with {"order":42}
  republish: cancelled 0, same refund run true, refund runs 1
then:
  import ok.csv succeeded -> index ran with ok.csv
  import bad.csv failed -> alert ran with bad.csv
```

## The code

A completion trigger: the `Then` plugin, applied with `Use` before `Register` and `Start`:

<!-- snippet: examples/events-more/main.go#then -->
```go
then, err := workflow.Then([]workflow.ThenRule{
	{When: "import", Start: func(ev workflow.RunEvent) (workflow.Job, bool, error) {
		return workflow.Job{Workflow: "index", Payload: []byte(ev.Labels["file"])}, true, nil
	}},
	{Name: "import-failed", When: "import", On: []workflow.Status{workflow.StatusFailed},
		Start: func(ev workflow.RunEvent) (workflow.Job, bool, error) {
			return workflow.Job{Workflow: "alert", Payload: []byte(ev.Labels["file"])}, true, nil // ev.Err says why
		}},
})
if err != nil {
	return err
}
c, err := workflow.Use(memory.New(), then) // before Register and Start
if err != nil {
	return err
}
```

A CloudEvent written in HTTP binary mode, read back and published:

<!-- snippet: examples/events-more/main.go#cloudevents-send -->
```go
ce := events.CloudEvent{SpecVersion: events.CloudEventsSpecVersion, ID: "evt_1", Source: "https://shop.example/orders",
	Type: "order.paid", Subject: "42", DataContentType: "application/json",
	Data: []byte(`{"order":42,"cents":1999,"currency":"EUR"}`), Extensions: map[string]string{"tenant": "acme"}}
h := http.Header{}
body, err := ce.WriteHTTP(h) // binary mode: ce-* headers, Data as the body
if err != nil {
	return err
}
req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(body))
req.Header = h
```

<!-- snippet: examples/events-more/main.go#cloudevents-receive -->
```go
got, err := events.ReadHTTP(req) // binary or structured mode; refusals wrap ErrInvalid
if err != nil {
	return err
}
m := events.ToMessage(got) // ID, Type→Name, Subject→Key, Data→Payload, extensions→Labels
p, err := b.Publish(ctx, m)
if err != nil {
	return err
}
```

Payload versions, upcast before subscribers see the message:

<!-- snippet: examples/events-more/main.go#versions -->
```go
b, err := events.New(c, events.WithUpcasters("order.paid", 2, map[int]workflow.Upcaster{
	0: func(raw []byte) ([]byte, error) { // v0 -> v1: "amount" is now "cents"
		return bytes.Replace(raw, []byte(`"amount"`), []byte(`"cents"`), 1), nil
	},
	1: func(raw []byte) ([]byte, error) { // v1 -> v2: add the currency, EUR before v2
		return bytes.Replace(raw, []byte(`}`), []byte(`,"currency":"EUR"}`), 1), nil
	},
}))
if err != nil {
	return err
}
```

A replay of one hour of the log to one subscription:

<!-- snippet: examples/events-more/main.go#replay -->
```go
from := time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC) // the hour the receipt mailer was down
r, err := b.Replay(ctx, events.ReplayOptions{
	ID:            "mailer-outage-2026-11-20",
	From:          from,
	To:            from.Add(2 * time.Hour),
	Subscriptions: []string{"receipt"},
})
if err != nil {
	return err
}
```

A cancel on event, and a divert:

<!-- snippet: examples/events-more/main.go#cancel -->
```go
err = b.SubscribeCancel(events.CancelSubscription{
	Name:  "stop-cart-reminders",
	Event: "cart.checked-out",
	Label: "cart", // cancels every unfinished run labelled cart=<the message's Key>
})
if err != nil {
	return err
}
```

<!-- snippet: examples/events-more/main.go#divert -->
```go
err = b.SubscribeCancel(events.CancelSubscription{
	Name:   "cancel-to-refund",
	Event:  "order.cancelled",
	Label:  "order",
	Divert: "refund", // after the cancel, start refund with the message's payload
})
if err != nil {
	return err
}
```

The guides that copy this program are [CloudEvents and payload versions](../../docs/events/cloudevents.md), [Event log, replay and cancel on event](../../docs/events/replay-and-cancel.md) and [Completion triggers](../../docs/workflows/then.md). `go test ./events-more` checks every line.
