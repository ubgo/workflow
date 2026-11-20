# Polling, sleeping until, and approvals

Three ways a step waits for the world without holding a worker: poll until a condition holds, sleep until a time, or suspend until a person answers.

## Before you start

- `Poll` needs the Journal plugin and `CapSnooze` (memory, sqlstore, Redis); `SleepUntil` needs `CapSnooze`. On a driver without it the snooze spends an attempt.
- Approvals need `CapWaitForEvent` (memory, sqlstore, Redis, Hatchet).
- The example is [`examples/flow-patterns`](../../examples/flow-patterns).

## Poll until a condition holds

<!-- snippet: examples/flow-patterns/main.go#poll -->
```go
path, err := workflow.Poll(ctx, "partition", workflow.PollSpec{Every: time.Millisecond, Factor: 2, Max: 8 * time.Millisecond, Timeout: time.Hour},
	func(ctx context.Context) (string, bool, error) {
		looks++ // a real sensor would stat the file or list the bucket
		return "s3://lake/2026-11-20/", looks == 3, nil
	})
if err != nil {
	return nil, err // a Snooze until the next look, ErrPollTimeout at the end
}
```

`workflow.Poll(ctx, name, spec, check)` calls `check`; while it reports not done, the attempt ends with a snooze for the next wait, so no worker is held and no retry is spent. `PollSpec` sets the wait (`Every`, growing by `Factor` up to `Max`, or your own `Next`), a `Timeout` after which the step fails with `ErrPollTimeout` (it matches `ErrTimeout`), and a `Clock`. The start time and the number of looks live in the journal, so the interval and the timeout hold across looks, retries and restarts. Use it for a sensor (a file or partition exists), or to poll an external job an earlier step started.

## Sleep until a time

`workflow.SleepUntil(clk, t)` returns a snooze for the time left before `t` and nil once `t` has passed; return its error from the handler. A wait longer than `MaxSnooze` takes several snoozes. Put it at the top of the step, or put earlier work in `Memo`, since the code above it runs on every return.

## Human approval

<!-- snippet: examples/flow-patterns/main.go#approval -->
```go
approval, err := workflow.ApprovalStep(workflow.ApprovalSpec{
	Name:    "approve",
	KeyFrom: workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["refund"] }),
	Timeout: 7 * 24 * time.Hour,
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
token, err := signer.Sign("r-19", now.Add(48*time.Hour)) // put it in the email's link
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

## Pitfalls

- A poll's `check` runs on every look: keep it cheap and side-effect free.
- An approval's `Timeout` fails the run with `ErrWaitTimeout`; tolerate it with [trigger rules](trigger-rules.md) for an escalation branch.
