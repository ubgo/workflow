# Inbound webhooks

A webhook sender retries until it gets a 2xx, and may deliver the same event twice. `events.NewWebhook` returns an `http.Handler` that answers both: it verifies the delivery's signature, stores a run for it, answers 200 as soon as the run is stored (before it runs), and deduplicates a redelivery by its ID, so the same delivery never starts two runs while the first is unfinished. The handler is closed by default: it refuses to be built without a verifier unless you say, in a field of its own, that the endpoint is unsigned.

## Before you start

- The handler enqueues with an idempotency key, so the client needs `workflow.CapIdempotencyKey`; every driver declares it.
- Mount it on your own router, behind whatever TLS and routing you already have; the package reads nothing from the process.
- The example is [`examples/events-webhooks`](../../examples/events-webhooks). It calls the handler through `httptest`, so nothing listens on a port. Run it with `go run ./events-webhooks` from `examples/`.

## Accept a signed webhook

Build an `events.WebhookConfig` with a `Verifier` and exactly one target: `Client` and `Workflow` to enqueue one run of `Workflow` per delivery, or `Bus` and `Event` to [publish](publish-subscribe.md) each delivery as an event. This one verifies GitHub-style signatures:

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

What the handler answers:

| Request | Status | Body |
|---|---|---|
| Any method but `POST` | 405, with `Allow: POST` | `method not allowed` |
| A body over `MaxBody` bytes | 413 | `body too large` |
| A body that cannot be read | 400 | `cannot read body` |
| A signature that does not verify | 401 | `unauthorized`, never which check failed |
| Stored | 200 | `{"id":"<delivery id>"}` |
| The backend is unavailable (an error matching `workflow.ErrUnavailable`) | 503 | `unavailable` |
| Any other enqueue or publish failure | 500 | `internal error` |

The body of a failure never carries the error; `WebhookConfig.Logger` receives it (nil discards). A 503 or 500 makes the sender retry, and the retry carries the same ID.

The delivery ID is what deduplicates. It comes from the verifier when the scheme carries one (`HMAC.IDHeader`, the Standard Webhooks `webhook-id`), else from `WebhookConfig.ID`, else it is the SHA-256 of the body in hex, so a sender's retry of the same body starts nothing new. A run's idempotency key is `webhook/<workflow>/<id>`; with a bus target, the delivery ID becomes the event's ID.

Other fields: `MaxBody` caps the body (`events.DefaultWebhookMaxBody`, 1 MiB, when zero), and `Labels` adds labels to the run from the request, such as a tenant from the path; a reserved key is a configuration error: nothing is enqueued, the handler answers 500, and the logger gets "webhook: configuration error: the Labels func returned a reserved label key; nothing was enqueued".

`NewWebhook` returns an error wrapping `workflow.ErrInvalid` when the config has no target, two targets, half a target, or no `Verifier` without `Unsigned: true`. `Unsigned` exists for a sender that cannot sign, behind a check of your own; it must be set on purpose, so an endpoint is never left open by omission.

## HMAC signatures

`events.HMAC` is a plain HMAC scheme: a header holding an optional `Prefix` followed by the HMAC of the body. It fits GitHub (`X-Hub-Signature-256`, prefix `sha256=`, hex, delivery ID in `X-GitHub-Delivery`) and Shopify (`X-Shopify-Hmac-Sha256`, `Encoding: events.EncodingBase64`). `Hash` defaults to SHA-256. It both verifies incoming requests and signs outgoing bodies with `Sign(body)`, which returns the header value.

`Secrets` holds several keys: `Sign` uses the first and `Verify` accepts any, compared in constant time, so a secret is rotated by putting the new one first, keeping the old one until every sender has switched, then removing it.

A plain HMAC has no timestamp, so it cannot refuse a replayed request; the handler's deduplication by delivery ID is what makes a replay of an unfinished delivery start nothing. Prefer Standard Webhooks where the sender supports it.

## Standard Webhooks

`events.NewStandardWebhooks(secrets, tolerance, clk)` is the [Standard Webhooks](https://www.standardwebhooks.com/) scheme, offered as one convention among others, not a default: an HMAC-SHA256 over `<id>.<timestamp>.<body>`, sent as `v1,<base64>` in `webhook-signature` beside `webhook-id` and `webhook-timestamp`. Secrets are `whsec_` followed by base64; the first signs and all verify. `Headers(id, body)` signs an outgoing request; `Verify` checks an incoming one and returns its `webhook-id`:

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

`Verify` refuses a timestamp further than the tolerance from now in either direction (`events.StandardWebhooksTolerance`, 5 minutes, when zero), which is what refuses a replay. The clock is injected (`clock.Real` when nil), so tests drive it. The header names are the constants `events.HeaderWebhookID`, `events.HeaderWebhookTimestamp` and `events.HeaderWebhookSignature`.

## Your own verifier

`events.Verifier` has one method, `Verify(r *http.Request, body []byte) (id string, err error)`: it checks the request against the raw body, read once by the handler, and returns the sender's delivery ID or `""`. Return an error matching `events.ErrSignature` for a request that is not authentic; the handler answers any verifier error with 401. A sender with its own scheme (a signed timestamp header, an asymmetric signature) is one type of yours.

## Answer 503 when the backend is down

An enqueue that fails with an error marked `workflow.Unavailable` gets 503, so the sender backs off and retries. The example wraps a client whose every enqueue is marked unavailable:

<!-- snippet: examples/events-webhooks/main.go#unavailable -->
```go
down, err := events.NewWebhook(events.WebhookConfig{Verifier: signer, Client: unreachable{c}, Workflow: "github-push"})
if err != nil {
	return err
}
```

Drivers mark their own outages: the SQL driver's connection failures and Hatchet's unreachable engine match `workflow.ErrUnavailable`.

## Try it

```sh
cd examples
go run ./events-webhooks
```

The webhook and signing parts:

```text
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

## Pitfalls

- **Deduplication lasts while the run is unfinished.** A delivery repeated after its run ended starts a new run; make the workflow idempotent, or record the delivery ID it handled.
- **The default ID is the body's hash.** Two different deliveries with identical bodies share an ID; use a scheme or an `ID` function that returns the sender's own delivery ID.
- **Plain HMAC cannot refuse a replay** of a delivery whose run has ended. Standard Webhooks can, within its tolerance.
- **Keep the raw body.** Signatures are over the exact bytes; a proxy or middleware that re-encodes JSON before the handler breaks every signature.
- **A 200 means stored, not processed.** The run may still fail; watch it like any other run.

## Related

- [Publish and subscribe](publish-subscribe.md), [Events](README.md)
- [Idempotency keys](../jobs/idempotency.md), [Errors](../concepts/errors.md)
- Example: [examples/events-webhooks](../../examples/events-webhooks/)
