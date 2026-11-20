# Push delivery to an HTTP endpoint

Your email sending runs as a serverless function, not in the worker process. You still want the queue's retries, delays, concurrency limits and admin view, only the work itself should run over there. The `push` package does this: `push.Deliver` is a step that POSTs the job, signed, to the endpoint, and `push.Handler` is the endpoint's side, which checks the signature and runs a workflow on the payload. The HTTP status carries the outcome back, so the queue retries a 5xx and stops on a 4xx.

## Before you start

- `push.Deliver` is an ordinary step handler, so it runs on every driver.
- `push.Handler` runs its workflow with `workflow.RunInline`, so the receiving side needs no driver and no database.
- A `Retry-After` from the endpoint is honoured when the delivering step's retry policy reads it (`RetryPolicy.DelayFor: workflow.RetryAfterDelay`, which needs `CapRetryDelay`); under any other policy the delivery retries on the normal backoff.
- Both packages need only the standard library and the core module.
- The example is [`examples/push-delivery`](../../examples/push-delivery). It runs the sender on the memory driver and the receiver on an `httptest` server. Run it with `go run ./push-delivery` from `examples/`.

## The receiving side

The endpoint's workflow is ordinary code. Its errors decide the answer: `Permanent` (or `Discard`) answers 422, `RetryAfter` answers 503 with `Retry-After`, any other error 500:

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

`push.Handler` verifies every request with an `events.Verifier` before reading it as a job. With `events.StandardWebhooks` a delivery whose timestamp is further from now than the tolerance is refused, so a captured request cannot be replayed later:

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
| 200 with the workflow's output | every step succeeded (the output is `workflow.RunOutput`) |
| 401 | the signature is missing, wrong, too old or too new (never says which), or the verifier is nil |
| 400 | the body is not a delivery envelope, or the job is invalid |
| 405, 413 | not a POST; a body over `WithMaxBody` (1 MiB) |
| 422 | a step failed for good (`Permanent`) or discarded the run |
| 503 with `Retry-After` | a step returned `workflow.RetryAfter` |
| 500 | any other step error |

A step's own error message is never sent back: it can name internal hosts or data.

## The sending side

`push.Deliver(url, signer, opts...)` returns the step. Each delivery is a JSON envelope `{"workflow", "run", "step", "attempt", "payload"}` (the payload base64) signed with the Standard Webhooks headers; the `webhook-id` is `<run>.<step>`, the same for every attempt, so the endpoint can deduplicate:

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
| 2xx | succeeds; the response body (at most `WithMaxResponse`, 1 MiB) is its output |
| 429 or 503 with `Retry-After` (seconds or a date) | fails with `workflow.RetryAfter`, retried after that wait |
| 429 or 503 without it, other 5xx, a network error or timeout | fails and is retried under its policy |
| any other 4xx | fails for good (`workflow.Permanent`) |

`WithClient` sets the `*http.Client`, `WithTimeout` bounds one delivery (30 seconds), `WithHeader` adds a header such as a gateway key, `WithDeliveryID` changes the delivery ID, and `WithClock` is read for a `Retry-After` date. Any `push.Signer` signs; `*events.StandardWebhooks` is the stock one.

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

- **Delivery is at least once.** If the endpoint finishes but its response is lost (a timeout, a dropped connection), the step is retried and the endpoint runs again. Make its steps idempotent, keyed on the `webhook-id` header.
- **The step's timeout must cover the endpoint's work.** `WithTimeout` and the step's own `Timeout` both bound the request; a slow function that answers after either is retried even though it succeeded.
- **A big response fails for good.** A 2xx body over `WithMaxResponse` is refused with `push.ErrResponseTooLarge` instead of being cut short. Return a reference, not the data.
- **Keep the clocks close.** Standard Webhooks refuses a timestamp outside the tolerance (five minutes by default) either way, so a sender or receiver whose clock drifts further sees every delivery answered 401.
- **Rotate secrets with two.** `NewStandardWebhooks` verifies with every secret it is given and signs with the first: give the receiver the old and the new secret, move the sender to the new one, then drop the old.

## Related

- [Retries](retries.md): `RetryAfter`, `RetryAfterDelay` and the retry policy.
- [Run inline](run-inline.md): how the receiving side runs its workflow.
- [Inbound webhooks](../events/webhooks.md): the signing schemes and the inbound webhook handler.
- [Start a run over HTTP](http-start.md): the other HTTP direction.
- Example: [examples/push-delivery](../../examples/push-delivery/).
