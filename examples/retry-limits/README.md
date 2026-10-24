# retry-limits

Retry limits beyond `Max` on the in-memory driver: a per-dispatch policy (`Job.Retry`), a deadline from the first attempt (`RetryPolicy.Until`), a cap on real errors that lets rate-limit releases through (`RetryPolicy.MaxErrors`), and dropping a run whose work no longer makes sense (`workflow.Discard`).

```sh
cd examples
go run ./retry-limits
```

```text
send-sms with Job.Retry{Max: 4}: failed after 5 attempts
sync-crm with Until 2m: failed after 3 attempts, 2m30s after the first
call-partner-api with MaxErrors 2: failed after 5 attempts (3 releases, 2 errors)
ship-order discarded: discarded after 1 attempt, OnFailure ran 0 times, ErrDiscarded: true
```

`Job.Retry{Max: 4}` replaced the workflow's one retry for that run. Each `sync-crm` attempt moves the fake clock 50 seconds, so the retry that would start at 2m30s is past the two-minute deadline and the step fails at its third attempt. The three `429`s are releases (`RetryAfter`) and count against `Max` only; the second real error reaches `MaxErrors`. The discarded run ends `discarded` (on a driver without `CapRunStates`, `cancelled`), with no retry and no `OnFailure`.

The guide that copies this program is [retries](../../docs/jobs/retries.md). `go test ./retry-limits` checks every line.
