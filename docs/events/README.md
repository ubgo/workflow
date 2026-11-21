# Events

The `events` package (`github.com/ubgo/workflow/events`, in the core module) starts runs from events: publish and subscribe inside your app, and an HTTP handler that turns a signed webhook delivery into a run. It is not a message broker. Publishing an event enqueues one run per matching subscription on an ordinary `workflow.Client`, so an event is exactly as durable as that client's runs, and deduplication rides on the client's idempotency keys.

Use it when one thing that happened should start several independent reactions (a receipt, a CRM update, a fulfilment run), when another system calls you with a webhook or a CloudEvent, or when a workflow should start because a dataset it reads has a new version. Use a plain `Enqueue` when only one workflow ever reacts, and a broker (SQS, NATS, Kafka) when the event must travel between apps that do not share a client.

| Guide | What it covers |
|---|---|
| [Publish and subscribe](publish-subscribe.md) | A `Bus` with subscriptions, publishing, deduplication by event ID, filters, building the job, delivering to waiting runs |
| [Inbound webhooks](webhooks.md) | `NewWebhook`, a closed-by-default handler with its status codes, HMAC signatures (GitHub, Shopify) and rotating their secrets, Standard Webhooks, your own verifier |
| [Datasets and lineage](datasets.md) | `Datasets.Updated`, any-of and all-of dataset triggers, the lineage store and its queries, `MemoryLineage` and the shared SQL and Redis stores |
| [CloudEvents and payload versions](cloudevents.md) | The CloudEvents 1.0 envelope over HTTP, mapping to `Message`, valid extension names, upcasting old payload versions |
| [Event log, replay and cancel on event](replay-and-cancel.md) | `EventLog`, `Replay` under a replay ID and its `ReplayResult`, cancel subscriptions and their `Cancellation`, divert |
| [Choreography and saga choreography](choreography.md) | `WithCorrelation`, correlation and causation labels, `Trail` to list one chain's runs, compensation by events compared with `Saga` |

Every guide has a runnable program on the in-memory driver: [examples/events-webhooks](../../examples/events-webhooks/) (the bus and webhooks), [examples/events-signal](../../examples/events-signal/) (delivering to waiting runs, replay and cancel results, extension names), [examples/events-more](../../examples/events-more/) (CloudEvents, versions, the log, replay, cancel and divert), [examples/datasets](../../examples/datasets/) and [examples/datasets-shared](../../examples/datasets-shared/) (dataset triggers, lineage and its stores) and [examples/saga-choreography](../../examples/saga-choreography/) (choreography).

## Before you start

- The package is part of the core module: `import "github.com/ubgo/workflow/events"`. It needs nothing but a `workflow.Client`; every driver works.
- The bus deduplicates by event ID through idempotency keys, so `events.New` refuses a client without `workflow.CapIdempotencyKey` (every driver declares it) unless you pass `events.AllowDuplicates()`.
- Register the workflows the subscriptions start, and the subscriptions themselves, at boot in every process that publishes, as you register workflows.

## Step 1: subscribe and publish

`events.New(client)` returns a bus. Each `Subscription` names an event and the workflow to run for it; `Publish` enqueues one run per matching subscription, with the message's payload, and returns what it did:

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

Give every message the ID it already has in the world (the payment provider's event ID, your outbox row ID): publishing the same ID again while the first runs are unfinished starts nothing new. The full rules, filters and building the job yourself are on [Publish and subscribe](publish-subscribe.md).

## Step 2: turn a webhook into a run

`events.NewWebhook` is an `http.Handler` that verifies the sender's signature, stores a run per delivery and answers 200 once it is stored. It refuses to be built without a verifier:

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
	MaxBody:  1 << 10, // 1 KiB; a bigger body is refused with 413
})
if err != nil {
	return err
}
```

Mount `h` on your router. Status codes, Standard Webhooks and your own verifier are on [Inbound webhooks](webhooks.md).

## Step 3: wake runs that wait for the event

A run parked on a wait step for the same event continues when the bus also signals it. Add `events.WithSignal()`:

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

Set `Message.Key` to the key the wait step resolves; see [Deliver to waiting runs](publish-subscribe.md#deliver-to-waiting-runs).

## Try it

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
hmac rotation:
  new signature: g0kvyILC8kxerVjwwPqiYVmYAoBSuxlWZ1pbtLCkJS4=
  during rotation: old secret verifies true, new secret verifies true
  after rotation:  old secret verifies false, new secret verifies true
```

The republish returned the first runs, the `pos` message skipped the subscription whose filter excludes it, the webhook's redelivery started no second run, and a request that fails any check is answered without saying which.

## Quick answers

| If you need | Use | Read |
|---|---|---|
| Several independent reactions to one thing that happened (send a receipt, update the CRM) | A `Bus` with one subscription per reaction | [Publish and subscribe](publish-subscribe.md#publish-an-event) |
| A run that is already waiting for the event to continue as well | `events.WithSignal()` on the bus | [Publish and subscribe › Deliver to waiting runs](publish-subscribe.md#deliver-to-waiting-runs) |
| Accept a webhook from GitHub, Shopify or a payment provider and process it once | `events.NewWebhook` with an `events.HMAC` verifier | [Inbound webhooks](webhooks.md#accept-a-signed-webhook) |
| Sign your own outgoing webhooks so receivers can refuse replays | `events.NewStandardWebhooks` | [Inbound webhooks › Standard Webhooks](webhooks.md#standard-webhooks) |
| Services that each react to events and publish their own, compensation included, with one place to see the whole chain | `events.WithCorrelation()` and `events.Trail` | [Choreography](choreography.md#list-a-chain) |
| A durable broker between services (SQS, NATS, Kafka) | Not this package: it delivers within one app's client | [Choose a driver](../choosing/drivers.md) |

## Pitfalls

- **It is not a broker.** An event becomes runs on your client and is exactly as durable as they are: on the memory driver it dies with the process, and `Publish` is not part of your database transaction. Use [transactional enqueue](../reliability/transactional-enqueue.md) or the [outbox](../reliability/outbox.md) when the event must not be lost between your commit and the publish.
- **Deduplication lasts while the run is unfinished.** The key is the client's idempotency key; a republish after the run ended starts a new run, and a message without an `ID` is never deduplicated.
- **Subscriptions and triggers live in each process.** A process that publishes before it registers a subscription starts no run for it.
- **Several processes need shared stores.** The event log (`MemoryLog`) and dataset lineage (`MemoryLineage`) default to one process; a fleet needs one durable store per kind, or each replica decides alone.
- **Labels under `ubgo.workflow/` are the bus's.** `Publish` refuses them in a message, so untrusted input cannot pose as an event's labels.

## Related

- [Wait for an event](../workflows/signals.md), [Completion triggers](../workflows/then.md), [Sagas](../workflows/sagas.md)
- [Idempotency keys](../jobs/idempotency.md), [Outbox](../reliability/outbox.md)
- [Choose a driver](../choosing/drivers.md)
