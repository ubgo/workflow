# events-webhooks

The `events` package on the in-memory driver: a `Bus` that turns one published event into a run per subscription and deduplicates a republish by the event's ID, a webhook handler that verifies an HMAC signature and enqueues, and the Standard Webhooks scheme refusing a changed body and a replay. The handler is called through `httptest`, so nothing listens on a port. Every run holds until the program ends, so the deduplication of unfinished runs is visible.

```sh
cd examples
go run ./events-webhooks
```

```text
bus:
  receipt started a run: true
  crm started a run: true
  republish of receipt returned the first run: true
  republish of crm returned the first run: true
  a pos order matches 1 subscription(s)
  run labels: event=order.paid id=evt_1 subscription=crm channel=web
webhook:
  GET                    405 method not allowed
  no signature           401 unauthorized
  wrong signature        401 unauthorized
  too large              413 body too large
  signed                 200 {"id":"d-1"}
  redelivered            200 {"id":"d-1"}
  runs of github-push: 1
  backend down           503 unavailable
  no Verifier refused: true
standard webhooks:
  verified id=msg_1 err=<nil>
  changed body: ErrSignature=true
  replayed after 10m: ErrSignature=true
```

## The code

A bus with two subscriptions to one event, and a publish:

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

Publishing the same ID again starts nothing new while the first runs are unfinished:

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

A webhook handler for GitHub-style signatures, enqueueing one run per delivery:

<!-- snippet: examples/events-webhooks/main.go#webhook -->
```go
signer := events.HMAC{
	Header:   "X-Hub-Signature-256",
	Prefix:   "sha256=",
	Secrets:  [][]byte{[]byte("new-secret"), []byte("old-secret")}, // the first signs, all verify
	IDHeader: "X-GitHub-Delivery",
}
h, err := events.NewWebhook(events.WebhookConfig{
	Verifier: signer,
	Client:   c,
	Workflow: "github-push",
	MaxBody:  1 << 10,
})
if err != nil {
	return err
}
```

A client whose backend is unreachable makes the handler answer 503, so the sender retries:

<!-- snippet: examples/events-webhooks/main.go#unavailable -->
```go
down, err := events.NewWebhook(events.WebhookConfig{Verifier: signer, Client: unreachable{c}, Workflow: "github-push"})
if err != nil {
	return err
}
```

Standard Webhooks headers for an outgoing request, verified on the way in:

<!-- snippet: examples/events-webhooks/main.go#standard -->
```go
sw, err := events.NewStandardWebhooks([]string{"whsec_c2VjcmV0LWtleS0x"}, 5*time.Minute, clk)
if err != nil {
	return err
}
body := []byte(`{"type":"invoice.paid"}`)
req := httptest.NewRequest(http.MethodPost, "/hooks/billing", bytes.NewReader(body))
req.Header = sw.Headers("msg_1", body) // webhook-id, webhook-timestamp, webhook-signature
id, err := sw.Verify(req, body)
```

The guides that copy this program are [publish and subscribe](../../docs/events/publish-subscribe.md) and [inbound webhooks](../../docs/events/webhooks.md). `go test ./events-webhooks` checks every line.
