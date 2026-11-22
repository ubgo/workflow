# Inbound webhooks

A webhook sender retries until it gets a 2xx, and may deliver the same event twice. [`events.NewWebhook`](ds:block?id=events-newwebhook-gweyfedm) returns an `http.Handler` that answers both: it verifies the delivery's signature, stores a run for it, answers 200 as soon as the run is stored (before it runs, [`ServeHTTP`](ds:block?id=events-webhook-servehttp-vwuu8jtt)), and deduplicates a redelivery by its ID ([`enqueue`](ds:block?id=events-webhook-enqueue-kem7sxby)), so the same delivery never starts two runs while the first is unfinished. The handler is closed by default: it refuses to be built without a verifier unless you say, in a field of its own, that the endpoint is unsigned.

## Before you start

- The handler enqueues with an idempotency key ([`enqueue`](ds:block?id=events-webhook-enqueue-kem7sxby)), so the client needs `workflow.CapIdempotencyKey`; every driver declares it: [memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-caps-yu37ef2j), [Redis](ds:block?id=redis-caps-bfgdst9t), [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z).
- Mount it on your own router, behind whatever TLS and routing you already have; the package reads nothing from the process (everything comes from the [`WebhookConfig`](ds:block?id=events-webhookconfig-7atgkns7)).
- The example is [`examples/events-webhooks`](../../examples/events-webhooks). It calls the handler through `httptest`, so nothing listens on a port. Run it with `go run ./events-webhooks` from `examples/`.

## Accept a signed webhook

Build an [`events.WebhookConfig`](ds:block?id=events-webhookconfig-7atgkns7) with a `Verifier` and exactly one target: `Client` and `Workflow` to enqueue one run of `Workflow` per delivery, or `Bus` and `Event` to [publish](publish-subscribe.md) each delivery as an event. This one verifies GitHub-style signatures:

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

What the handler answers ([`ServeHTTP`](ds:block?id=events-webhook-servehttp-vwuu8jtt), [`fail`](ds:block?id=events-webhook-fail-qxvpc8yk)):

| Request | Status | Body |
|---|---|---|
| Any method but `POST` | 405, with `Allow: POST` | `method not allowed` |
| A body over `MaxBody` bytes | 413 | `body too large` |
| A body that cannot be read | 400 | `cannot read body` |
| A signature that does not verify | 401 | `unauthorized`, never which check failed |
| Stored | 200 | `{"id":"<delivery id>"}` |
| The backend is unavailable (an error matching `workflow.ErrUnavailable`) | 503 | `unavailable` |
| Any other enqueue or publish failure | 500 | `internal error` |

The body of a failure never carries the error; `WebhookConfig.Logger` receives it (nil discards, [`NewWebhook`](ds:block?id=events-newwebhook-gweyfedm)). A 503 or 500 makes the sender retry, and the retry carries the same ID.

The delivery ID is what deduplicates. It comes from the verifier when the scheme carries one (`HMAC.IDHeader`, the Standard Webhooks `webhook-id`), else from `WebhookConfig.ID`, else it is the SHA-256 of the body in hex ([`deliveryID`](ds:block?id=events-webhook-deliveryid-egxfcpng)), so a sender's retry of the same body starts nothing new. A run's idempotency key is `webhook/<workflow>/<id>`; with a bus target, the delivery ID becomes the event's ID ([`enqueue`](ds:block?id=events-webhook-enqueue-kem7sxby)).

Other fields: `MaxBody` caps the body ([`events.DefaultWebhookMaxBody`](ds:block?id=events-defaultwebhookmaxbody-kdm3hgb3), 1 MiB, when zero), and `Labels` adds labels to the run from the request, such as a tenant from the path; a reserved key is a configuration error ([`ServeHTTP`](ds:block?id=events-webhook-servehttp-vwuu8jtt)): nothing is enqueued, the handler answers 500, and the logger gets "webhook: configuration error: the Labels func returned a reserved label key; nothing was enqueued".

[`NewWebhook`](ds:block?id=events-newwebhook-gweyfedm) returns an error wrapping `workflow.ErrInvalid` when the config has no target, two targets, half a target, or no `Verifier` without `Unsigned: true`. `Unsigned` exists for a sender that cannot sign, behind a check of your own; it must be set on purpose, so an endpoint is never left open by omission.

## HMAC signatures

[`events.HMAC`](ds:block?id=events-hmac-5mvsrx94) is a plain HMAC scheme: a header holding an optional `Prefix` followed by the HMAC of the body. It implements [`events.Verifier`](ds:block?id=events-verifier-cqn4e5me) for incoming requests and signs outgoing bodies with [`Sign(body)`](ds:block?id=events-hmac-sign-bn32q9c3), which returns the header value. Its fields:

| Field | Meaning | Default |
|---|---|---|
| `Header` | The header holding the signature. Required | none |
| `Secrets` | The keys, at least one. `Sign` uses the first, `Verify` accepts any | none |
| `Prefix` | Written before the digest, such as `sha256=` | none |
| `Encoding` | How the digest is written: [`events.EncodingHex`](ds:block?id=events-encodinghex-ma2s9r9m) or `events.EncodingBase64` | hex |
| `Hash` | The hash constructor, such as `sha1.New` for an old sender | SHA-256 ([`HMAC`](ds:block?id=events-hmac-5mvsrx94)) |
| `IDHeader` | The header carrying the sender's delivery ID, which `Verify` returns | none |

It fits GitHub (`X-Hub-Signature-256`, prefix `sha256=`, hex, delivery ID in `X-GitHub-Delivery`, as in [Accept a signed webhook](#accept-a-signed-webhook)) and Shopify (`X-Shopify-Hmac-Sha256`, no prefix, `events.EncodingBase64`).

### Rotate a secret

[`Verify`](ds:block?id=events-hmac-verify-wrhym82e) tries every secret, each compared in constant time, so a secret is rotated without dropping a delivery: put the new secret first, keep the old one until every sender has switched, then remove it.

<!-- snippet: examples/events-webhooks/main.go#hmac-rotate -->
```go
before := events.HMAC{
	Header:   "X-Shopify-Hmac-Sha256",
	Encoding: events.EncodingBase64, // GitHub's hex is the default, events.EncodingHex
	Secrets:  [][]byte{[]byte("old-secret")},
}
during := before
during.Secrets = [][]byte{[]byte("new-secret"), []byte("old-secret")} // new first: it signs, both verify
after := before
after.Secrets = [][]byte{[]byte("new-secret")} // every sender has switched

sigOld, err := before.Sign(body) // a sender still on the old secret
if err != nil {
	return err
}
sigNew, err := during.Sign(body) // a sender already on the new one
if err != nil {
	return err
}
// verifies reports whether v accepts body signed with sig.
verifies := func(v events.Verifier, sig string) bool {
	req := httptest.NewRequest(http.MethodPost, "/hooks/shopify", bytes.NewReader(body))
	req.Header.Set("X-Shopify-Hmac-Sha256", sig)
	_, err := v.Verify(req, body)
	return !errors.Is(err, events.ErrSignature)
}
```

```text
hmac rotation:
  new signature: g0kvyILC8kxerVjwwPqiYVmYAoBSuxlWZ1pbtLCkJS4=
  during rotation: old secret verifies true, new secret verifies true
  after rotation:  old secret verifies false, new secret verifies true
```

[`Verify`](ds:block?id=events-hmac-verify-wrhym82e) returns an error matching [`events.ErrSignature`](ds:block?id=events-errsignature-deeb8maq) for a missing header, a missing prefix, a digest that does not decode in the configured encoding, and a digest no secret produces. An `HMAC` with no `Header` or no `Secrets` refuses every request the same way, so a misconfigured verifier fails closed. [`Sign`](ds:block?id=events-hmac-sign-bn32q9c3) returns an error when the first secret is missing or empty.

A plain [`HMAC`](ds:block?id=events-hmac-5mvsrx94) has no timestamp, so it cannot refuse a replayed request; the handler's deduplication by delivery ID is what makes a replay of an unfinished delivery start nothing. Prefer Standard Webhooks where the sender supports it.

## Standard Webhooks

[`events.NewStandardWebhooks(secrets, tolerance, clk)`](ds:block?id=events-newstandardwebhooks-nchrkphq) is the [Standard Webhooks](https://www.standardwebhooks.com/) scheme, offered as one convention among others, not a default: an HMAC-SHA256 over `<id>.<timestamp>.<body>`, sent as `v1,<base64>` in `webhook-signature` beside `webhook-id` and `webhook-timestamp` ([`StandardWebhooks`](ds:block?id=events-standardwebhooks-vfagtuu5)). Secrets are `whsec_` followed by base64; the first signs and all verify. [`Headers(id, body)`](ds:block?id=events-standardwebhooks-headers-ftsw7fhm) signs an outgoing request; [`Verify`](ds:block?id=events-standardwebhooks-verify-bfd6fwea) checks an incoming one and returns its `webhook-id`:

<!-- snippet: examples/events-webhooks/main.go#standard -->
```go
sw, err := events.NewStandardWebhooks([]string{"whsec_c2VjcmV0LWtleS0x"}, 5*time.Minute, clk) // older timestamps are replays
if err != nil {
	return err
}
body := []byte(`{"type":"invoice.paid"}`)
req := httptest.NewRequest(http.MethodPost, "/hooks/billing", bytes.NewReader(body))
req.Header = sw.Headers("msg_1", body) // webhook-id, webhook-timestamp, webhook-signature
id, err := sw.Verify(req, body)
```

[`Verify`](ds:block?id=events-standardwebhooks-verify-bfd6fwea) refuses a timestamp further than the tolerance from now in either direction ([`events.StandardWebhooksTolerance`](ds:block?id=events-standardwebhookstolerance-fkqrj65c), 5 minutes, when zero), which is what refuses a replay. The clock is injected (`clock.Real` when nil, [`NewStandardWebhooks`](ds:block?id=events-newstandardwebhooks-nchrkphq)), so tests drive it. The header names are the constants [`events.HeaderWebhookID`](ds:block?id=events-headerwebhookid-nbkr4m47), `events.HeaderWebhookTimestamp` and `events.HeaderWebhookSignature`.

## Your own verifier

[`events.Verifier`](ds:block?id=events-verifier-cqn4e5me) has one method, `Verify(r *http.Request, body []byte) (id string, err error)`: it checks the request against the raw body, read once by the handler, and returns the sender's delivery ID or `""`. Return an error matching [`events.ErrSignature`](ds:block?id=events-errsignature-deeb8maq) for a request that is not authentic; the handler answers any verifier error with 401 ([`ServeHTTP`](ds:block?id=events-webhook-servehttp-vwuu8jtt)). A sender with its own scheme (a signed timestamp header, an asymmetric signature) is one type of yours.

## Answer 503 when the backend is down

An enqueue that fails with an error marked [`workflow.Unavailable`](ds:block?id=unavailable-wpp9cehg) gets 503 ([`fail`](ds:block?id=events-webhook-fail-qxvpc8yk)), so the sender backs off and retries. The example wraps a client whose every enqueue is marked unavailable:

<!-- snippet: examples/events-webhooks/main.go#unavailable -->
```go
down, err := events.NewWebhook(events.WebhookConfig{Verifier: signer, Client: unreachable{c}, Workflow: "github-push"})
if err != nil {
	return err
}
```

Drivers mark their own outages: the SQL driver's connection failures ([`mark`](ds:block?id=sql-store-mark-6tkf8nxr)) and Hatchet's unreachable engine ([`engineErr`](ds:block?id=hatchet-engineerr-ungnu8e7)) match `workflow.ErrUnavailable`.

## Drop redeliveries with an inbox

The idempotency key a delivery carries dedups it while its run is unfinished. To drop a redelivered ID for longer, whatever the driver keeps, give the handler an inbox: [`WebhookConfig.Inbox`](ds:block?id=events-webhookconfig-7atgkns7) takes a `workflow.InboxConsumer`, records each delivery ID it enqueued ([`deliver`](ds:block?id=events-webhook-deliver-e8x8hcsu)), answers a redelivery 200 without enqueueing again, and answers 503 while another request is processing the same ID ([`fail`](ds:block?id=events-webhook-fail-qxvpc8yk)). A failed enqueue releases the ID ([`InboxConsumer.Once`](ds:block?id=inboxconsumer-once-mta79w6e)), so the sender's retry enqueues it. Use `sqlstore.Inbox` or `redis.Inbox` behind several replicas:

<!-- snippet: examples/consumer-inbox/main.go#webhook -->
```go
hook, err := events.NewWebhook(events.WebhookConfig{
	Unsigned: true, // a real endpoint sets a Verifier
	Client:   c, Workflow: "ingest",
	ID:    func(r *http.Request, _ []byte) string { return r.Header.Get("Webhook-Id") },
	Inbox: &workflow.InboxConsumer{Inbox: workflow.NewMemoryInbox(), Consumer: "shop-webhooks"},
})
```

The inbox and its stores are described in [Idempotency keys › Drop redelivered messages with an inbox](../jobs/idempotency.md#drop-redelivered-messages-with-an-inbox).

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
hmac rotation:
  new signature: g0kvyILC8kxerVjwwPqiYVmYAoBSuxlWZ1pbtLCkJS4=
  during rotation: old secret verifies true, new secret verifies true
  after rotation:  old secret verifies false, new secret verifies true
```

## Pitfalls

- **Deduplication lasts while the run is unfinished.** A delivery repeated after its run ended starts a new run ([`enqueue`](ds:block?id=events-webhook-enqueue-kem7sxby) uses the idempotency key); set `Inbox` to drop it for as long as the inbox keeps the ID ([`DefaultInboxKeep`](ds:block?id=defaultinboxkeep-kzbwa4qr), seven days, by default), or make the workflow idempotent.
- **The default ID is the body's hash** ([`deliveryID`](ds:block?id=events-webhook-deliveryid-egxfcpng)). Two different deliveries with identical bodies share an ID; use a scheme or an `ID` function that returns the sender's own delivery ID.
- **Plain HMAC cannot refuse a replay** of a delivery whose run has ended: [`HMAC.Verify`](ds:block?id=events-hmac-verify-wrhym82e) reads no timestamp. Standard Webhooks can, within its tolerance ([`StandardWebhooks.Verify`](ds:block?id=events-standardwebhooks-verify-bfd6fwea)).
- **Remove an old secret only when no sender uses it.** A secret dropped too early turns every delivery from a sender still on it into a 401 ([`ServeHTTP`](ds:block?id=events-webhook-servehttp-vwuu8jtt)), and the sender retries until it gives up.
- **Keep the raw body.** Signatures are over the exact bytes ([`HMAC.Verify`](ds:block?id=events-hmac-verify-wrhym82e)); a proxy or middleware that re-encodes JSON before the handler breaks every signature.
- **A 200 means stored, not processed.** The handler answers once the enqueue returns ([`ServeHTTP`](ds:block?id=events-webhook-servehttp-vwuu8jtt)). The run may still fail; watch it like any other run.

## Related

- [Publish and subscribe](publish-subscribe.md), [Events](README.md)
- [Idempotency keys](../jobs/idempotency.md), [Errors](../concepts/errors.md)
- Example: [examples/events-webhooks](../../examples/events-webhooks/)
