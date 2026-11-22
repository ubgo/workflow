# Push delivery to an HTTP endpoint

Your email sending runs as a serverless function, not in the worker process. You still want the queue's retries, delays, concurrency limits and admin view, only the work itself should run over there. The `push` package does this: [`push.Deliver`](ds:block?id=push-deliver-n3r3thp8) is a step that POSTs the job, signed, to the endpoint, and [`push.Handler`](ds:block?id=push-handler-g994qhc5) is the endpoint's side, which checks the signature and runs a workflow on the payload. The HTTP status carries the outcome back, so the queue retries a 5xx and stops on a 4xx ([`deliverConfig.answer`](ds:block?id=push-deliverconfig-answer-t9wu9nmj)).

## Before you start

- [`push.Deliver`](ds:block?id=push-deliver-n3r3thp8) is an ordinary step handler, so it runs on every driver.
- [`push.Handler`](ds:block?id=push-handler-g994qhc5) runs its workflow with `workflow.RunInline`, so the receiving side needs no driver and no database.
- A `Retry-After` from the endpoint is honoured when the delivering step's retry policy reads it (`RetryPolicy.DelayFor: workflow.RetryAfterDelay`, which needs [`CapRetryDelay`](ds:block?id=capretrydelay-pp5car73)); under any other policy the delivery retries on the normal backoff ([`workflow.RetryAfter`](ds:block?id=retryafter-xck6cdyv)).
- Both packages need only the standard library and the core module.
- The example is [`examples/push-delivery`](../../examples/push-delivery). It runs the sender on the memory driver and the receiver on an `httptest` server. Run it with `go run ./push-delivery` from `examples/`.

## The receiving side

The endpoint's workflow is ordinary code. Its errors decide the answer ([`answerError`](ds:block?id=push-answererror-qvwz2cde)): `Permanent` (or `Discard`) answers 422, `RetryAfter` answers 503 with `Retry-After`, any other error 500:

<!-- snippet: examples/push-delivery/main.go#receiver -->
```go
// sendEmail is the workflow the endpoint runs. It fails once for
// "flaky@example.com" (a cold start) and refuses "bounced@example.com"
// for good.
func sendEmail(coldStarts *atomic.Int32) workflow.Workflow {
	return workflow.Workflow{Name: "send-email", Steps: []workflow.Step{{Name: "send", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		var to string
		if err := json.Unmarshal(in, &to); err != nil {
			return nil, workflow.Permanent(err) // 422: the sender stops
		}
		switch {
		case to == "bounced@example.com":
			return nil, workflow.Permanent(errors.New("address bounced")) // 422
		case to == "flaky@example.com" && coldStarts.Add(1) == 1:
			return nil, errors.New("cold start") // 500: the sender retries
		}
		return json.Marshal(map[string]string{"sent_to": to})
	}}}}
}
```

[`push.Handler`](ds:block?id=push-handler-g994qhc5) verifies every request with an `events.Verifier` before reading it as a job. With [`events.StandardWebhooks`](ds:block?id=events-standardwebhooks-verify-bfd6fwea) a delivery whose timestamp is further from now than the tolerance is refused, so a captured request cannot be replayed later:

<!-- snippet: examples/push-delivery/main.go#handler -->
```go
verifier, err := events.NewStandardWebhooks([]string{secret}, 5*time.Minute, nil)
if err != nil {
	return err
}
var coldStarts atomic.Int32
endpoint := httptest.NewServer(push.Handler(sendEmail(&coldStarts), verifier))
defer endpoint.Close()
```

| Handler answer | When |
|---|---|
| 200 with the workflow's output | every step succeeded (the output is [`workflow.RunOutput`](ds:block?id=runoutput-hvt3unpt)) |
| 401 | the signature is missing, wrong, too old or too new (never says which), or the verifier is nil ([`Handler`](ds:block?id=push-handler-g994qhc5)) |
| 400 | the body is not a delivery envelope, or the job is invalid ([`answerError`](ds:block?id=push-answererror-qvwz2cde)) |
| 405, 413 | not a POST; a body over [`WithMaxBody`](ds:block?id=push-withmaxbody-v56e8b2s) (1 MiB, [`DefaultMaxBody`](ds:block?id=push-defaultmaxbody-rk3rgbcy)) |
| 422 | a step failed for good (`Permanent`) or discarded the run ([`answerError`](ds:block?id=push-answererror-qvwz2cde)) |
| 503 with `Retry-After` | a step returned `workflow.RetryAfter` ([`answerError`](ds:block?id=push-answererror-qvwz2cde)) |
| 500 | any other step error ([`answerError`](ds:block?id=push-answererror-qvwz2cde)) |

A step's own error message is never sent back: it can name internal hosts or data ([`answerError`](ds:block?id=push-answererror-qvwz2cde)).

## The sending side

[`push.Deliver(url, signer, opts...)`](ds:block?id=push-deliver-n3r3thp8) returns the step. Each delivery is a JSON envelope `{"workflow", "run", "step", "attempt", "payload"}` ([`push.Envelope`](ds:block?id=push-envelope-53rwh3es); the payload base64) signed with the Standard Webhooks headers; the `webhook-id` is `<run>.<step>`, the same for every attempt, so the endpoint can deduplicate:

<!-- snippet: examples/push-delivery/main.go#deliver -->
```go
signer, err := events.NewStandardWebhooks([]string{secret}, 0, nil)
if err != nil {
	return err
}
dispatch := workflow.Workflow{
	Name:  "dispatch-email",
	Retry: workflow.RetryPolicy{Max: 3, DelayFor: workflow.RetryAfterDelay}, // honour a 429/503 Retry-After
	Steps: []workflow.Step{{Name: "deliver", Run: push.Deliver(endpoint.URL, signer,
		push.WithClient(endpoint.Client()), push.WithTimeout(10*time.Second))}},
}
c := memory.New()
if err := c.Register(dispatch); err != nil {
	return err
}
if err := c.Start(ctx); err != nil {
	return err
}
```

| Endpoint answer | The step |
|---|---|
| 2xx | succeeds; the response body (at most [`WithMaxResponse`](ds:block?id=push-withmaxresponse-az87qpmk), 1 MiB, [`DefaultMaxResponse`](ds:block?id=push-defaultmaxresponse-5fbhh25p)) is its output |
| 429 or 503 with `Retry-After` (seconds or a date, [`retryAfter`](ds:block?id=push-retryafter-pnmntf8x)) | fails with `workflow.RetryAfter`, retried after that wait |
| 429 or 503 without it, other 5xx, a network error or timeout | fails and is retried under its policy ([`deliverConfig.answer`](ds:block?id=push-deliverconfig-answer-t9wu9nmj)) |
| any other 4xx | fails for good (`workflow.Permanent`, [`deliverConfig.answer`](ds:block?id=push-deliverconfig-answer-t9wu9nmj)) |

[`WithClient`](ds:block?id=push-withclient-3d86bh2a) sets the `*http.Client`, [`WithTimeout`](ds:block?id=push-withtimeout-7r4g988d) bounds one delivery (30 seconds, [`DefaultTimeout`](ds:block?id=push-defaulttimeout-85ebt6k7)), [`WithHeader`](ds:block?id=push-withheader-w7tfu4ss) adds a header such as a gateway key, [`WithDeliveryID`](ds:block?id=push-withdeliveryid-u9vheqyc) changes the delivery ID, and [`WithClock`](ds:block?id=push-withclock-rbhnyx5d) is read for a `Retry-After` date. Any [`push.Signer`](ds:block?id=push-signer-46q9qw54) signs; `*events.StandardWebhooks` is the stock one.

## Try it

```text
deliveries:
  ann@example.com: succeeded after 1 attempt(s), result {"sent_to":"ann@example.com"}
  flaky@example.com: succeeded after 2 attempt(s), result {"sent_to":"flaky@example.com"}
  bounced@example.com: failed after 1 attempt(s): memory: step "deliver" failed after 1 attempt(s): push: endpoint answered 422: the job failed for good
forged request:
  unsigned: 401
```

## Pitfalls

- **Delivery is at least once.** If the endpoint finishes but its response is lost (a timeout, a dropped connection), the step is retried and the endpoint runs again ([`Handler`](ds:block?id=push-handler-g994qhc5)). Make its steps idempotent, keyed on the `webhook-id` header.
- **The step's timeout must cover the endpoint's work.** [`WithTimeout`](ds:block?id=push-withtimeout-7r4g988d) and the step's own `Timeout` both bound the request ([`Deliver`](ds:block?id=push-deliver-n3r3thp8)); a slow function that answers after either is retried even though it succeeded.
- **A big response fails for good.** A 2xx body over `WithMaxResponse` is refused with [`push.ErrResponseTooLarge`](ds:block?id=push-errresponsetoolarge-j8fkf2m6) instead of being cut short ([`deliverConfig.answer`](ds:block?id=push-deliverconfig-answer-t9wu9nmj)). Return a reference, not the data.
- **Keep the clocks close.** Standard Webhooks refuses a timestamp outside the tolerance (five minutes by default, [`events.StandardWebhooksTolerance`](ds:block?id=events-standardwebhookstolerance-fkqrj65c)) either way, so a sender or receiver whose clock drifts further sees every delivery answered 401.
- **Rotate secrets with two.** [`NewStandardWebhooks`](ds:block?id=events-newstandardwebhooks-nchrkphq) verifies with every secret it is given and signs with the first: give the receiver the old and the new secret, move the sender to the new one, then drop the old.

## Related

- [Retries](retries.md): `RetryAfter`, `RetryAfterDelay` and the retry policy.
- [Run inline](run-inline.md): how the receiving side runs its workflow.
- [Inbound webhooks](../events/webhooks.md): the signing schemes and the inbound webhook handler.
- [Start a run over HTTP](http-start.md): the other HTTP direction.
- Example: [examples/push-delivery](../../examples/push-delivery/).
