# Polling, sleeping until, and approvals

Three ways a step waits for the world without holding a worker: poll until a condition holds (a file lands in a bucket, an export finishes), sleep until a time (renew a subscription on its date), or suspend until a person answers (approve a refund). Each keeps its progress outside the worker, so a wait of minutes or weeks costs no goroutine and survives a restart.

Use `Poll` when you can only ask whether something is ready, `SleepUntil` when you know the time, and an approval when a person or another system will tell you. When the world can call back instead of being asked, a [signal](signals.md) is cheaper than a poll.

## Before you start

- `Poll` needs the Journal plugin and `CapSnooze` (memory, sqlstore, Redis); `SleepUntil` needs `CapSnooze`. On a driver without it the snooze spends an attempt.
- Approvals need `CapWaitForEvent` (memory, sqlstore, Redis, Hatchet).
- The example is [`examples/flow-patterns`](../../examples/flow-patterns). Run it with `go run ./flow-patterns` from `examples/`.

## Poll until a condition holds

<!-- snippet: examples/flow-patterns/main.go#poll -->
```go
// First look after 1ms, the gap doubling to at most 8ms, for up to an hour.
path, err := workflow.Poll(ctx, "partition", workflow.PollSpec{Every: time.Millisecond, Factor: 2, Max: 8 * time.Millisecond, Timeout: time.Hour},
	func(ctx context.Context) (string, bool, error) {
		looks++ // a real sensor would stat the file or list the bucket; here it is there on the third look
		return "s3://lake/2026-11-20/", looks == 3, nil
	})
if err != nil {
	return nil, err // a Snooze until the next look, ErrPollTimeout at the end
}
```

`workflow.Poll(ctx, name, spec, check)` calls `check`; while it reports not done, the attempt ends with a snooze for the next wait, so no worker is held and no retry is spent. `PollSpec` sets the wait (`Every`, growing by `Factor` up to `Max`, or your own `Next`), a `Timeout` after which the step fails with `ErrPollTimeout` (it matches `ErrTimeout`), and a `Clock`. The start time and the number of looks live in the journal, so the interval and the timeout hold across looks, retries and restarts. Use it for a sensor (a file or partition exists), or to poll an external job an earlier step started.

## Sleep until a time

`workflow.SleepUntil(clk, t)` returns a snooze for the time left before `t` and nil once `t` has passed; return its error from the handler. The step comes back at `t` holding no worker and spending no attempt:

<!-- snippet: examples/flow-patterns/main.go#sleepuntil -->
```go
var at time.Time
if err := json.Unmarshal(in, &at); err != nil {
	return nil, workflow.Permanent(err)
}
if err := workflow.SleepUntil(clock.Real{}, at); err != nil {
	return nil, err // a Snooze for the time left; the step comes back at `at`
}
return json.Marshal("renewed") // runs once `at` has passed
```

A nil clock means the real one; pass a fake clock in tests ([fake clock](../testing/fake-clock.md)). A wait longer than `MaxSnooze` takes several snoozes, each coming back to find `t` still ahead. Put the call at the top of the step, or put earlier work in `Memo`, since the code above it runs on every return. To sleep between steps instead of inside one, use a [sleep step](sleep.md).

## Human approval

<!-- snippet: examples/flow-patterns/main.go#approval -->
```go
approval, err := workflow.ApprovalStep(workflow.ApprovalSpec{
	Name:    "approve",
	KeyFrom: workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["refund"] }),
	Timeout: 7 * 24 * time.Hour, // a week to answer
})
if err != nil {
	return workflow.Workflow{}, err
}
pay := workflow.Step{Name: "pay", After: []string{"approve"}, Run: func(ctx context.Context, in []byte) ([]byte, error) {
	d, err := workflow.DecisionOf(in)
	if err != nil {
		return nil, err
	}
	if !d.Approved {
		return json.Marshal("refund refused by " + d.By)
	}
	return json.Marshal("refund paid, approved by " + d.By)
}}
```

`workflow.ApprovalStep` is a wait step on `ApprovalEvent` keyed by the request (`Key`, or `KeyFrom` reading the job). `Approve` and `Reject` resume it with a `Decision`, which the next step reads with `DecisionOf`. An answer sent before the run reaches the step is kept. For a request with no natural ID, `NewApprovalToken` returns a random one to store in the job.

To put an approve button in an email, sign the key with an expiry and check it in the link's handler:

<!-- snippet: examples/flow-patterns/main.go#signer -->
```go
signer := workflow.ApprovalSigner{Keys: [][]byte{[]byte("a-secret-of-32-bytes-from-config")}}
token, err := signer.Sign("r-19", now.Add(48*time.Hour)) // valid for two days; put it in the email's link
if err != nil {
	return err
}
// In the link's handler: check the token, then answer.
key, err := signer.Verify(token, now.Add(time.Hour))
if err != nil {
	return err // forged or expired: ErrApprovalToken
}
if err := workflow.Approve(ctx, c, key, "ana@example.com", "looks right"); err != nil {
	return err
}
```

`ApprovalSigner` needs no storage: the token carries the key and its expiry under an HMAC-SHA256. `Keys[0]` signs and every key verifies, so keys rotate. A forged or expired token matches `ErrApprovalToken`.

## Try it

```sh
cd examples
go run ./flow-patterns
```

Among its output, these lines are this page's: the sensor found the partition on its third look, the approval signed into a link paid the refund and the same link is refused once expired, and the renewal step slept until its time before it ran.

```text
poll:
  "s3://lake/2026-11-20/" after 3 looks
approval:
  "refund paid, approved by ana@example.com"
  the same link two days later: workflow: invalid approval token: expired
sleep until:
  "renewed"
    renewed on time: true
```

## Pitfalls

- **A poll's `check` runs on every look:** keep it cheap and side-effect free. Its looks and start time live in the journal, so an in-process `MemoryJournal` restarts the count on another worker; use a durable journal store in production.
- **Without `CapSnooze` every look or sleep spends an attempt.** On Hatchet a `Poll` or `SleepUntil` would burn through the step's retries; use a [sleep step](sleep.md) or a signal there.
- **An approval's `Timeout` fails the run** with `ErrWaitTimeout`; tolerate it with [trigger rules](trigger-rules.md) for an escalation branch.
- **An approval key is the whole secret of an unsigned link.** Anyone who can call `Approve` with the key decides; put keys in links only signed with `ApprovalSigner`, and keep its keys in your secret store.
- **One answer resumes every run waiting with that key.** Give each request its own key, such as `NewApprovalToken`, when two runs must not share one decision.

## Related

- [Signals and waits](signals.md), [Sleep steps](sleep.md), [Escalation and signal handlers](escalation.md)
- [Memoise work inside a step](memoisation.md): the journal `Poll` keeps its looks in
- [Snooze a step that is not ready](../jobs/retries.md#snooze-a-step-that-is-not-ready), [Fake clock](../testing/fake-clock.md)
- [Trigger rules](trigger-rules.md): an expired approval routed to an escalation step
- Example: [examples/flow-patterns](../../examples/flow-patterns/)
