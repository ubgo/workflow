# Events

The `events` package (`github.com/ubgo/workflow/events`, in the core module) starts runs from events: publish and subscribe inside your app, and an HTTP handler that turns a signed webhook delivery into a run. It is not a message broker. Publishing an event enqueues one run per matching subscription on an ordinary `workflow.Client`, so an event is exactly as durable as that client's runs, and deduplication rides on the client's idempotency keys.

| Guide | What it covers |
|---|---|
| [Publish and subscribe](publish-subscribe.md) | A `Bus` with subscriptions, publishing, deduplication by event ID, filters, building the job, delivering to waiting runs |
| [Inbound webhooks](webhooks.md) | `NewWebhook`, a closed-by-default handler with its status codes, HMAC signatures (GitHub, Shopify), Standard Webhooks, your own verifier |

Both are shown by one runnable program, [examples/events-webhooks](../../examples/events-webhooks/), on the in-memory driver.

## Quick answers

| If you need | Use | Read |
|---|---|---|
| Several independent reactions to one thing that happened (send a receipt, update the CRM) | A `Bus` with one subscription per reaction | [Publish and subscribe](publish-subscribe.md#publish-an-event) |
| A run that is already waiting for the event to continue as well | `events.WithSignal()` on the bus | [Publish and subscribe › Deliver to waiting runs](publish-subscribe.md#deliver-to-waiting-runs) |
| Accept a webhook from GitHub, Shopify or a payment provider and process it once | `events.NewWebhook` with an `events.HMAC` verifier | [Inbound webhooks](webhooks.md#accept-a-signed-webhook) |
| Sign your own outgoing webhooks so receivers can refuse replays | `events.NewStandardWebhooks` | [Inbound webhooks › Standard Webhooks](webhooks.md#standard-webhooks) |
| A durable broker between services (SQS, NATS, Kafka) | Not this package: it delivers within one app's client | [Choose a driver](../choosing/drivers.md) |
