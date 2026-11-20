# push-delivery

Jobs delivered to an HTTP endpoint instead of run in the worker: a step built with `push.Deliver` POSTs each job signed with Standard Webhooks, and `push.Handler` on the receiving side (an `httptest` server standing in for a serverless function) verifies it and runs its own workflow inline. A receiver that answers 500 is retried, one that refuses for good (422) fails the run after one attempt, and an unsigned request is refused with 401.

```sh
cd examples
go run ./push-delivery
```

```text
deliveries:
  ann@example.com: succeeded after 1 attempt(s), result {"sent_to":"ann@example.com"}
  flaky@example.com: succeeded after 2 attempt(s), result {"sent_to":"flaky@example.com"}
  bounced@example.com: failed after 1 attempt(s): memory: step "deliver" failed after 1 attempt(s): push: endpoint answered 422: the job failed for good
forged request:
  unsigned: 401
```

The guide that copies this program is [push delivery to an HTTP endpoint](../../docs/jobs/push-delivery.md). `go test ./push-delivery` checks every line.
