# Retries

A step that calls something outside your process will sometimes fail for reasons that pass: a 503, a dropped connection, a 429 that says "come back in 30 seconds". A retry policy says how often and how soon a failed step is tried again, so a transient failure costs a short delay instead of a failed run. This page covers the policy, overriding it per step, spreading retries with jitter, taking the delay from the error, errors that must not be retried at all, and snoozing a step that is not ready yet without spending an attempt.

## Before you start

- Plain retries with exponential backoff work on every driver. Jitter needs `workflow.CapRetryJitter` and a delay taken from the error needs `workflow.CapRetryDelay` ([`retryCaps`](ds:block?id=retrycaps-vqww8mgj)): the [memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-caps-yu37ef2j) and [Redis](ds:block?id=redis-caps-bfgdst9t) drivers have both, [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z) neither (see [Pitfalls](#pitfalls)).
- Ending a run `discarded` instead of `failed` when retries run out needs `workflow.CapDiscardExhausted` and `workflow.CapRunStates` ([`retryCaps`](ds:block?id=retrycaps-vqww8mgj)): the [memory](ds:block?id=memory-caps-yxkuq583), SQL ([`parityCaps`](ds:block?id=sql-paritycaps-e5y5vqhk), [`batch2Caps`](ds:block?id=sql-batch2caps-9rm4b4ck)) and [Redis](ds:block?id=redis-caps-bfgdst9t) drivers have both, [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z) neither. See [Quiet giving up](#quiet-giving-up-discardonexhaust).
- Snoozing a step without spending an attempt needs `workflow.CapSnooze`. The [memory](ds:block?id=memory-caps-yxkuq583), [Redis](ds:block?id=redis-caps-bfgdst9t) and [SQL](ds:block?id=sql-p5caps-wggh6esn) drivers have it; [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z) does not. See [Snooze a step that is not ready](#snooze-a-step-that-is-not-ready).
- The example is [`examples/retries`](../../examples/retries). It runs on the memory driver with a fake clock and a pinned jitter source, so every delay in its output is exact. Run it with `go run ./retries` from `examples/`.

## Retry policy

[`workflow.RetryPolicy`](ds:block?id=retrypolicy-cjkmhshn) has three core parts: `Max`, the number of retries after the first attempt; `Backoff`, the wait between attempts; and `NoRetryOn`, a function that marks errors not worth retrying. Its other fields (`DelayFor`, `Until`, `MaxErrors`, `DiscardOnExhaust`) are covered further down. Its zero value means no retries ([`DefaultRetryMax`](ds:block?id=defaultretrymax-wwnmv64w)).

<!-- snippet: examples/retries/main.go#policy -->
```go
policy := workflow.RetryPolicy{
	Max: 3, // three retries after the first attempt
	Backoff: workflow.Backoff{
		Initial: time.Second,
		Factor:  2, // each delay doubles: 1s, 2s, 4s
		Max:     time.Minute,
		Jitter:  0.5, // each delay is drawn from [d/2, d]
	},
}
```

Set it as `Workflow.Retry`, the default for every step, or as `Step.Retry` for one step. [`Backoff`](ds:block?id=backoff-3jh78r24) is exponential: `Initial`, then multiplied by `Factor` per retry, capped at `Max`. A `Factor` below 1 counts as 1, which gives a constant delay. A zero `Initial` retries at once ([`Backoff.Delay`](ds:block?id=backoff-delay-4u7wxm4a)).

[`Backoff.Delay(n)`](ds:block?id=backoff-delay-4u7wxm4a) is the wait before retry number `n` (1 for the first). It is a pure function, so you can print a policy's schedule before you ship it:

<!-- snippet: examples/retries/main.go#delays -->
```go
b := workflow.Backoff{Initial: time.Second, Factor: 2, Max: 10 * time.Second, Jitter: 0.5} // doubling, capped at 10s
// Five retries: the fifth hits the 10s cap.
for n := 1; n <= 5; n++ {
	// JitteredDelay takes the uniform draw in [0, 1) as an argument.
	fmt.Printf("retry %d: delay %s, with draw 0.5: %s\n", n, b.Delay(n), b.JitteredDelay(n, 0.5))
}
```

```text
retry 1: delay 1s, with draw 0.5: 750ms
retry 2: delay 2s, with draw 0.5: 1.5s
retry 3: delay 4s, with draw 0.5: 3s
retry 4: delay 8s, with draw 0.5: 6s
retry 5: delay 10s, with draw 0.5: 7.5s
```

## Retries per step

`Step.Retry` overrides the workflow default for one step ([`EffectiveRetriesFor`](ds:block?id=effectiveretriesfor-cv2kmktb)). Nil means "use the workflow's", and [`workflow.RetriesNone()`](ds:block?id=retriesnone-gvsc2j8f) means "never retry this step", for a step whose side effect must not happen twice, such as an email:

<!-- snippet: examples/retries/main.go#per-step -->
```go
signupDefault := workflow.RetryPolicy{Max: 3, Backoff: workflow.Backoff{Initial: time.Second}} // three retries, 1s apart
signup := workflow.Workflow{
	Name:  "signup",
	Retry: signupDefault, // the default for every step that leaves Retry nil
	Steps: []workflow.Step{
		{Name: "create", Run: step("create", signupDefault, flaky)},
		{
			Name:  "welcome",
			After: []string{"create"},
			Retry: workflow.RetriesNone(), // a welcome email is sent once or not at all
			Run:   step("welcome", workflow.RetryPolicy{}, func(int) error { return errors.New("smtp refused") }),
		},
	},
}
```

In the example `create` inherits three retries and succeeds on its third attempt; `welcome` fails once and the run fails. A per-step policy is one of four places a policy can come from; the next section says which wins.

## Which policy a step gets

A step's retry policy can be set in four places, and the first one that is set wins. The winner is used whole: policies are never merged field by field ([`EffectiveRetriesFor`](ds:block?id=effectiveretriesfor-cv2kmktb)), so a `Job.Retry` of `{Max: 4}` also drops the workflow's backoff.

| Order | Where | Set by | Scope |
|---|---|---|---|
| 1 | `Job.Retry` | The code that enqueues | Every step of that one run |
| 2 | `Step.Retry` (`RetriesNone()` means never retry) | The workflow author | One step |
| 3 | `Workflow.Retry` | The workflow author | Every step whose `Retry` is nil |
| 4 | [`workflow.DefaultRetryMax`](ds:block?id=defaultretrymax-wwnmv64w) retries (none) | The library | Everything else |

[`workflow.EffectiveRetriesFor(step, wf, job)`](ds:block?id=effectiveretriesfor-cv2kmktb) is that rule in one function, and every driver that retries calls it (memory, SQL and Redis; `workflow.RunInline` too), so no two drivers resolve a step's policy differently; [`TestEffectiveRetriesForPrecedence`](ds:block?id=testeffectiveretriesforprecedence-cdkxqcpy) pins the order. [`workflow.EffectiveRetries(step, wf)`](ds:block?id=effectiveretries-52e5k4q9) is the same with no job, for code that decides at declaration time. The field comments on [`RetryPolicy`](ds:block?id=retrypolicy-cjkmhshn) carry the same table, and `Max` counts retries after the first attempt, so the value that applies gives `Max+1` attempts at most ([`RetryPolicy.Retryable`](ds:block?id=retrypolicy-retryable-ve8muksd)).

The rule is a pure function, so you can ask it which policy a step would get before any run exists. The `retries` example resolves the same step at each level; only the `Max` of the winner is printed:

<!-- snippet: examples/retries/main.go#precedence -->
```go
noop := func(context.Context, []byte) ([]byte, error) { return nil, nil }
wf := workflow.Workflow{
	Name:  "ship",
	Retry: workflow.RetryPolicy{Max: 2}, // level 3: every step whose Retry is nil
	Steps: []workflow.Step{
		{Name: "label", Retry: &workflow.RetryPolicy{Max: 5}, Run: noop}, // level 2: this step only
		{Name: "notify", Run: noop},
	},
}
bare := workflow.Workflow{Name: "bare", Steps: []workflow.Step{{Name: "only", Run: noop}}}
override := workflow.Job{Workflow: "ship", Retry: &workflow.RetryPolicy{Max: 1}} // level 1: this run only

fmt.Println("  job over step:", workflow.EffectiveRetriesFor(wf.Steps[0], wf, override).Max)
fmt.Println("  step over workflow:", workflow.EffectiveRetriesFor(wf.Steps[0], wf, workflow.Job{}).Max)
fmt.Println("  workflow default:", workflow.EffectiveRetriesFor(wf.Steps[1], wf, workflow.Job{}).Max)
fmt.Println("  library default:", workflow.EffectiveRetriesFor(bare.Steps[0], bare, workflow.Job{}).Max,
	"| DefaultRetryMax:", workflow.DefaultRetryMax)
```

```text
  job over step: 1
  step over workflow: 5
  workflow default: 2
  library default: 0 | DefaultRetryMax: 0
```

The `retry-limits` example shows the job override on a real run and the `discard-exhausted` example runs all four levels side by side:

<!-- snippet: examples/discard-exhausted/main.go#job -->
```go
// Job.Retry beats both: this dispatch gets no retries and the default ending.
if err := report("Job.Retry beats step policy", workflow.Job{Workflow: "step-own", Retry: &workflow.RetryPolicy{Max: 0}}); err != nil {
	return err
}
```

## Backoff and jitter

When an outage fails a thousand runs at once, an exact backoff retries all thousand at the same instant and can knock the recovering service over again. [`Backoff.Jitter`](ds:block?id=backoff-3jh78r24) spreads them: a delay `d` becomes a uniform random value in `[d*(1-Jitter), d]`. Jitter is a fraction in `[0, 1]` ([`validateRetry`](ds:block?id=validateretry-jcn44n64) refuses anything else); zero means the exact delay.

[`Backoff.JitteredDelay(n, u)`](ds:block?id=backoff-jittereddelay-me9rbcyv) is the jittered delay for a uniform draw `u` in `[0, 1)`: `u = 0` gives the full delay and `u` near 1 the shortest. The draw is an argument, not read inside, so the result can be pinned in a test. Drivers draw from `math/rand/v2` by default ([`RetryPolicy.NextDelay`](ds:block?id=retrypolicy-nextdelay-94cqz66r)); the memory, SQL and Redis drivers take `WithJitterSource` to pin it ([`memory`](ds:block?id=memory-withjittersource-5dryr759), [`SQL`](ds:block?id=sql-withjittersource-bcd4dpzh), [`Redis`](ds:block?id=redis-withjittersource-a3mqs5ft)), which is how the example's output stays exact:

<!-- snippet: examples/retries/main.go#client -->
```go
c := memory.New(
	memory.WithClock(clk),
	memory.WithJitterSource(half), // pin the jitter draws for a repeatable demo
)
```

With `Jitter: 0.5` and a pinned draw of 0.5, the `sync` workflow's first retry comes 750ms after the failure (1s shortened by a quarter) and the second 1.5s after that.

The range is easiest to see with the draw at its ends. Jitter needs `workflow.CapRetryJitter` on the client: the [memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-caps-yu37ef2j) and [Redis](ds:block?id=redis-caps-bfgdst9t) drivers have it, and Hatchet refuses a policy with jitter at `Register` ([`retryBackoff`](ds:block?id=hatchet-retrybackoff-vrbwvu97), [Pitfalls](#pitfalls)). `Backoff.Delay(n)` is the exact delay before retry `n`, and [`JitteredDelay`](ds:block?id=backoff-jittereddelay-me9rbcyv) shortens it by at most `Jitter` of itself:

<!-- snippet: examples/retries/main.go#jitter -->
```go
b := workflow.Backoff{Initial: 2 * time.Second, Factor: 2, Jitter: 0.25} // the last quarter of each delay is random
fmt.Println("  needs:", workflow.CapRetryJitter, "| memory has it:", memory.New().Capabilities().Has(workflow.CapRetryJitter))
fmt.Println("  retry", jitterRetry, "exact delay:", b.Delay(jitterRetry))
for _, u := range []float64{0, 0.5, 0.999} { // the shortest, a middle and almost the full delay
	fmt.Printf("  draw %.3f: %s\n", u, b.JitteredDelay(jitterRetry, u))
}
```

```text
  needs: RetryJitter | memory has it: true
  retry 2 exact delay: 4s
  draw 0.000: 4s
  draw 0.500: 3.5s
  draw 0.999: 3.001s
```

Jitter only ever shortens a delay, never lengthens it ([`JitteredDelay`](ds:block?id=backoff-jittereddelay-me9rbcyv)), so `Backoff.Max` stays a true upper bound.

## Retry after a delay the error names

Sometimes the failure says when to come back: a 429 with `Retry-After: 30`, a lock held until a known time. Wrap the error with [`workflow.RetryAfter(err, d)`](ds:block?id=retryafter-xck6cdyv) and give the policy a `DelayFor` that reads it. [`workflow.RetryAfterDelay`](ds:block?id=retryafterdelay-pgsc6bvn) is the stock one:

<!-- snippet: examples/retries/main.go#retry-after -->
```go
rateLimited := workflow.RetryPolicy{
	Max:      2, // two retries after the first attempt
	Backoff:  workflow.Backoff{Initial: time.Second},
	DelayFor: workflow.RetryAfterDelay, // honour RetryAfter(err, d)
}
// callAPI is rate limited on its first attempt and succeeds after.
callAPI := func(n int) error {
	if n == 1 {
		// The API answered 429 with Retry-After: 30.
		return workflow.RetryAfter(errors.New("429 too many requests"), 30*time.Second) // the Retry-After value
	}
	return nil
}
```

`DelayFor(err, retry)` returns the delay and true, or false to fall back to `Backoff`. Its delay is used as returned, with no jitter (a negative one counts as zero; [`RetryPolicy.NextDelay`](ds:block?id=retrypolicy-nextdelay-94cqz66r)). You can write your own, for example to read a typed error from your HTTP client.

[`RetryPolicy.NextDelay(err, retry, rnd)`](ds:block?id=retrypolicy-nextdelay-94cqz66r) is the whole rule in one call: `DelayFor`'s answer when it gives one, else the backoff with its jitter drawn from `rnd`. Every driver calls it, so no two drivers disagree on a delay. The example uses it to move its fake clock by exactly the delay the driver chose:

<!-- snippet: examples/retries/main.go#next-delay -->
```go
d := a.policy.NextDelay(a.err, a.n, half)
```

## Errors that must not retry

Some failures are final: a declined card, a payload that does not parse, a record that does not exist. Retrying only delays the failure. Two ways to stop retries:

- Return [`workflow.Permanent(err)`](ds:block?id=permanent-9e6vjm6s) from the handler. The step fails now, whatever `Max` says. [`workflow.IsPermanent(err)`](ds:block?id=ispermanent-pswuapn7) reports it, through any wrapping.
- Set `RetryPolicy.NoRetryOn` ([`RetryPolicy.Retryable`](ds:block?id=retrypolicy-retryable-ve8muksd) reads it), which keeps the decision in the policy instead of the handler: useful when the handler returns an error from a library you do not control.

<!-- snippet: examples/retries/main.go#no-retry -->
```go
noRetry := workflow.RetryPolicy{
	Max:       5, // never reached: both errors below stop at the first attempt
	Backoff:   workflow.Backoff{Initial: time.Second},
	NoRetryOn: func(err error) bool { return errors.Is(err, errNotFound) },
}
// charge fails with an error marked Permanent; lookup with one NoRetryOn matches.
charge := func(int) error {
	return workflow.Permanent(errors.New("card declined")) // retrying cannot help
}
lookup := func(int) error { return fmt.Errorf("lookup: %w", errNotFound) }
```

[`workflow.Define`](ds:block?id=define-d3ttbgk3) already marks input that does not decode as permanent, since the same bytes can never decode. Cancellation (`workflow.ErrCancelled`) is never retried either, and `Permanent` wins over `RetryAfter`: `RetryAfter(Permanent(err), d)` is not retried ([`RetryPolicy.Retryable`](ds:block?id=retrypolicy-retryable-ve8muksd) checks both before anything else).

## Limits beyond Max

`Max` caps the number of retries. Four more limits cover what a count cannot, and a fifth setting decides how a run ends once they stop it. The program behind them is [`examples/retry-limits`](../../examples/retry-limits) (`go run ./retry-limits` from `examples/`).

### Per-dispatch retries

`Job.Retry` replaces every step's policy for one run, so one important customer can get more tries than the workflow's default. It wins over `Step.Retry` and `Workflow.Retry`; [`workflow.EffectiveRetriesFor(step, wf, job)`](ds:block?id=effectiveretriesfor-cv2kmktb) is the rule. It needs `workflow.CapJobRetry` ([`RequiresJob`](ds:block?id=requiresjob-mrfs7vqd)).

<!-- snippet: examples/retry-limits/main.go#job-retry -->
```go
sendSMS := workflow.Workflow{
	Name:  "send-sms",
	Retry: workflow.RetryPolicy{Max: 1}, // the default: one retry
	Steps: []workflow.Step{{Name: "send", Run: func(context.Context, []byte) ([]byte, error) {
		sms.Add(1)
		return nil, errBoom
	}}},
}
```

<!-- snippet: examples/retry-limits/main.go#dispatch -->
```go
vip := &workflow.RetryPolicy{Max: 4} // this customer gets more tries
if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "send-sms", Retry: vip}); err != nil {
	return err
}
```

A per-dispatch policy is data only: `NoRetryOn` and `DelayFor` must be nil, because a durable driver stores the job and cannot store a function. [`workflow.ValidateJob`](ds:block?id=validatejob-a7nq3u2t) refuses them with `ErrInvalid`; keep those on the step.

### A deadline: Until

[`RetryPolicy.Until`](ds:block?id=retrypolicy-next-vj4catr9) stops retrying once a retry would start more than `Until` after the step's first attempt started. It wins over `Max`. It needs `workflow.CapRetryUntil` ([`retryCaps`](ds:block?id=retrycaps-vqww8mgj)).

<!-- snippet: examples/retry-limits/main.go#until -->
```go
syncCRM := workflow.Workflow{
	Name: "sync-crm",
	// Retry as often as needed, but give up two minutes after the
	// first attempt started: the data is stale after that.
	Retry: workflow.RetryPolicy{Max: 100, Until: 2 * time.Minute},
	Steps: []workflow.Step{{Name: "push", Run: func(context.Context, []byte) ([]byte, error) {
		pushes.Add(1)
		clk.Advance(50 * time.Second) // each attempt takes 50 seconds
		return nil, errBoom
	}}},
}
```

### Errors, not releases: MaxErrors

[`RetryPolicy.MaxErrors`](ds:block?id=retrypolicy-next-vj4catr9) fails a step for good on its `MaxErrors`-th error, even when `Max` allows more attempts. A release, an error marked by `workflow.RetryAfter` (a 429, a lock held elsewhere; [`IsRelease`](ds:block?id=isrelease-b5qv6a9e)), counts against `Max` only; any other error counts against both. It needs `workflow.CapRetryMaxErrors` ([`retryCaps`](ds:block?id=retrycaps-vqww8mgj)).

<!-- snippet: examples/retry-limits/main.go#max-errors -->
```go
callAPI := workflow.Workflow{
	Name: "call-partner-api",
	// Up to 10 attempts, but only 2 real errors: being rate limited
	// (a release, marked by RetryAfter) is not the API's fault.
	Retry: workflow.RetryPolicy{Max: 10, MaxErrors: 2, DelayFor: workflow.RetryAfterDelay},
	Steps: []workflow.Step{{Name: "call", Run: func(context.Context, []byte) ([]byte, error) {
		if rate.Add(1) <= 3 {
			return nil, workflow.RetryAfter(errors.New("429 too many requests"), 0)
		}
		return nil, errBoom
	}}},
}
```

[`RetryPolicy.Next(err, attempts, now, rnd)`](ds:block?id=retrypolicy-next-vj4catr9) is the whole decision in one pure call, `Max`, `MaxErrors`, the delay and `Until` together, and every driver calls it. [`workflow.Attempts.Count(err)`](ds:block?id=attempts-count-ttmdy23p) counts an attempt and, unless it was a release, an error.

### Drop the run: Discard

Some failures mean the work no longer makes sense: the order was deleted after the job was queued. Return [`workflow.Discard(err)`](ds:block?id=discard-ttxam8w8): the step is not retried, `OnFailure` does not run, and the run ends `discarded` (`workflow.StatusDiscarded`) with an `Err` matching `workflow.ErrDiscarded` and `workflow.ErrCancelled` ([`DiscardedError`](ds:block?id=discardederror-ebygkc2v)). A driver without `workflow.CapRunStates` reports it as `cancelled` ([`EndStatus`](ds:block?id=endstatus-d3uc8s8n)); see [Run states](run-states.md#discarded). Use `Permanent` instead when an operator should see the failure. `MaxErrors` and `Until` are not discards on their own: a run that reaches either ends `failed`, unless the policy opts in to the next section ([`ExhaustedError`](ds:block?id=retrypolicy-exhaustederror-uwdsjnja)).

<!-- snippet: examples/retry-limits/main.go#discard -->
```go
shipOrder := workflow.Workflow{
	Name:  "ship-order",
	Retry: workflow.RetryPolicy{Max: 5},
	Steps: []workflow.Step{{Name: "ship", Run: func(context.Context, []byte) ([]byte, error) {
		ship.Add(1)
		// The order was deleted after the job was queued: nothing to
		// ship, nothing to retry, nothing an operator must look at.
		return nil, workflow.Discard(errors.New("order o-9 no longer exists"))
	}}},
	OnFailure: func(context.Context, []byte) ([]byte, error) { onFailure.Add(1); return nil, nil },
}
```

On a driver without `workflow.CapDiscard`, `Discard` behaves as `Permanent`: the run fails ([`EndStatus`](ds:block?id=endstatus-d3uc8s8n)), which never hides a failure.

```text
send-sms with Job.Retry{Max: 4}: failed after 5 attempts
sync-crm with Until 2m: failed after 3 attempts, 2m30s after the first
call-partner-api with MaxErrors 2: failed after 5 attempts (3 releases, 2 errors)
ship-order discarded: discarded after 1 attempt, OnFailure ran 0 times, ErrDiscarded: true
```

### Quiet giving up: DiscardOnExhaust

A run that runs out of retries normally ends `failed`: it lands in the failed-run list, `OnFailure` runs and a batch's `Catch` fires. For work where giving up is routine (a welcome email nobody needs after five tries, a cache warm-up), that makes the failed list noisy. [`RetryPolicy.DiscardOnExhaust`](ds:block?id=retrypolicy-exhaustederror-uwdsjnja) makes the run end `discarded` instead, with an `Err` matching `workflow.ErrDiscarded` and `workflow.ErrCancelled`, no `OnFailure` and no chain:

<!-- snippet: examples/discard-exhausted/main.go#policies -->
```go
// Workflow.Retry is the default for steps that set none: 3 retries.
// "welcome-mail" gives up quietly; "invoice-mail" keeps the default
// ending (failed, OnFailure pages someone).
welcome := workflow.Workflow{
	Name:      "welcome-mail",
	Retry:     workflow.RetryPolicy{Max: 3, DiscardOnExhaust: true},
	Steps:     []workflow.Step{{Name: "send", Run: failing(errDown)}},
	OnFailure: onFailure,
}
invoice := workflow.Workflow{
	Name:      "invoice-mail",
	Retry:     workflow.RetryPolicy{Max: 3},
	Steps:     []workflow.Step{{Name: "send", Run: failing(errDown)}},
	OnFailure: onFailure,
}
// A step's own Retry beats the workflow's, and RetriesNone() is "never".
stepOwn := workflow.Workflow{
	Name:  "step-own",
	Retry: workflow.RetryPolicy{Max: 5},
	Steps: []workflow.Step{{Name: "send", Run: failing(errDown),
		Retry: &workflow.RetryPolicy{Max: 1, DiscardOnExhaust: true}}},
}
// A Permanent error stops at once; with the option on it is discarded
// instead of failed.
permanent := workflow.Workflow{
	Name:  "permanent",
	Retry: workflow.RetryPolicy{Max: 5, DiscardOnExhaust: true},
	Steps: []workflow.Step{{Name: "send", Run: failing(workflow.Permanent(errors.New("mailbox does not exist")))}},
}
```

It applies whenever this policy stops retrying a failed attempt: attempts ran out (`Max`), `MaxErrors` or `Until` stopped it, `NoRetryOn` excluded the error, or the handler returned `Permanent`. It does not touch an explicit `Discard` or a cancellation, which already end the run their own way ([`ExhaustedError`](ds:block?id=retrypolicy-exhaustederror-uwdsjnja)). The option is on the policy that wins the precedence above ([`EffectiveRetriesFor`](ds:block?id=effectiveretriesfor-cv2kmktb)), so a `Job.Retry` without it brings back the `failed` ending for that one run. The default is off, so `failed` stays a signal that a person should look.

```text
workflow policy, DiscardOnExhaust: discarded after 4 attempts, OnFailure ran 0, ErrDiscarded true
workflow policy, default ending:   failed    after 4 attempts, OnFailure ran 1, ErrDiscarded false
step policy beats workflow:        discarded after 2 attempts, OnFailure ran 0, ErrDiscarded true
Job.Retry beats step policy:       failed    after 1 attempts, OnFailure ran 0, ErrDiscarded false
Permanent with DiscardOnExhaust:   discarded after 1 attempts, OnFailure ran 0, ErrDiscarded true
```

It needs `workflow.CapDiscardExhausted`, which also needs `workflow.CapRunStates` to report the status ([`retryCaps`](ds:block?id=retrycaps-vqww8mgj)): the [memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-paritycaps-e5y5vqhk) and [Redis](ds:block?id=redis-caps-bfgdst9t) drivers declare both. Hatchet refuses the option at `Register` (field `Retry.DiscardOnExhaust`, [`checkRetry`](ds:block?id=hatchet-checkretry-ccamdfbn)) because its engine has no discarded status, so a run out of retries can only be `failed` there. [`workflow.RetryPolicy.ExhaustedError(attempts, err)`](ds:block?id=retrypolicy-exhaustederror-uwdsjnja) is the one function a driver calls at the point `Next` says stop; write a driver with it. The program is [`examples/discard-exhausted`](../../examples/discard-exhausted).

## Snooze a step that is not ready

Not every "try again later" is a failure. A report the upstream has not finished building, a partner's window that opens at nine, a quota that refills on the hour: the step did nothing wrong and should neither spend an attempt nor be recorded as an error. Return [`workflow.Snooze(d)`](ds:block?id=snooze-mcp3r4p7) from the handler and the step is put back to run again after `d`:

<!-- snippet: examples/snooze-heartbeat/main.go#snooze -->
```go
// fetch snoozes until the report it downloads is ready.
fetch := func(ctx context.Context, _ []byte) ([]byte, error) {
	info, _ := workflow.StepInfoFrom(ctx)
	tries <- info.Attempt
	if !reportReady() {
		// Not a failure: put the step back for a minute, same attempt.
		return nil, workflow.Snooze(time.Minute)
	}
	return []byte(`"report.csv"`), nil
}
wf := workflow.Workflow{
	Name:  "download-report",
	Retry: workflow.RetryPolicy{}, // no retries, and a snooze needs none
	Steps: []workflow.Step{{Name: "fetch", Run: fetch}},
}
```

On a driver with `CapSnooze` a snooze is not a failure:

- The next try has the same `StepInfo.Attempt`, and `RunInfo.Attempts` does not count the snoozed try ([`Snooze`](ds:block?id=snooze-mcp3r4p7)).
- The retry policy is not consulted, so a snooze spends none of `Max` or `MaxErrors`. `Until` keeps counting from the first attempt ([`Attempts`](ds:block?id=attempts-count-ttmdy23p) keeps `First`), since it is a deadline, not a count.
- Nothing is added to `RunInfo.Errors`, and the run reports `StatusWaiting`, not `retrying`, while it sleeps (`NextAttempt` and `NextAttemptAt` stay zero; [`Snooze`](ds:block?id=snooze-mcp3r4p7)).
- A lifecycle observer sees a [`workflow.LifecycleSnoozed`](ds:block?id=lifecyclesnoozed-k96yqmmm) event; `errors.Is(err, workflow.ErrSnoozed)` ([`ErrSnoozed`](ds:block?id=errsnoozed-3x74fmpm)) and [`workflow.SnoozeDelay(err)`](ds:block?id=snoozedelay-hpbukddn) recognise the error and read its delay.

The example's workflow allows no retries at all, yet its step snoozes twice and succeeds on the third try, still attempt 1:

```text
snooze:
  try at +0s: attempt 1
  snoozed, status waiting
  try at +1m0s: attempt 1
  snoozed, status waiting
  try at +2m0s: attempt 1
  run succeeded, attempts 1, errors recorded 0
```

A negative `d` counts as zero (try again as soon as a worker is free), and a `d` over [`workflow.MaxSnooze`](ds:block?id=maxsnooze-jzy97vu4) (30 days) is cut to it ([`Snooze`](ds:block?id=snooze-mcp3r4p7)), so a bug computing a huge delay cannot park a run for years.

Three tools put a step back for a while; they differ in what they cost and who decides:

| | [`workflow.Snooze(d)`](ds:block?id=snooze-mcp3r4p7) | [`workflow.RetryAfter(err, d)`](ds:block?id=retryafter-xck6cdyv) | [`middleware.Release(when, d)`](ds:block?id=mw-release-ek3ppr3h) |
|---|---|---|---|
| Decided by | The handler, after starting | The handler, after it failed | Middleware, before the handler runs |
| Spends an attempt | No | Yes | Yes |
| Recorded as an error | No | Yes | Yes |
| Run status meanwhile | `waiting` | `retrying` | `retrying` |
| Delay honoured when | The driver has `CapSnooze` | The policy's `DelayFor` is [`RetryAfterDelay`](ds:block?id=retryafterdelay-pgsc6bvn) | The policy's `DelayFor` is `RetryAfterDelay` |
| Use it for | "Not yet": the work is not ready | A failure that names when to come back (a 429) | A condition known before any work starts (maintenance) |

`Release` is described in [step middleware](../middleware/step-middleware.md#release-a-step-for-later). A step that waits for an outside event it can be told about (a webhook, an approval) is better as a [wait step](../workflows/signals.md), which holds no attempt and wakes when the event arrives instead of polling.

## Try it

```sh
cd examples
go run ./retries
```

After the backoff table above, the program runs one workflow per section, then the refusals, the jitter range and the policy precedence:

```text
sync:
  call attempt 1 at +0s: upstream returned 503
  call attempt 2 at +750ms: upstream returned 503
  call attempt 3 at +2.25s: ok
  run succeeded

import:
  fetch attempt 1 at +0s: 429 too many requests
  fetch attempt 2 at +30s: ok
  run succeeded

charge:
  charge attempt 1 at +0s: card declined
  run failed (permanent: true): memory: step "charge" failed after 1 attempt(s): card declined

lookup:
  lookup attempt 1 at +0s: lookup: customer not found
  run failed (permanent: false): memory: step "lookup" failed after 1 attempt(s): lookup: customer not found

signup:
  create attempt 1 at +0s: upstream returned 503
  create attempt 2 at +1s: upstream returned 503
  create attempt 3 at +2s: ok
  welcome attempt 1 at +2s: smtp refused
  run failed (permanent: false): memory: step "welcome" failed after 1 attempt(s): smtp refused

refusals:
  needs: RetryJitter
  memory: <nil>
  hatchet: workflow "sync" step "call": workflow: Retry.Backoff.Jitter unsupported by this driver (missing RetryJitter): Hatchet waits exactly Factor^n seconds and has no jitter | unsupported: true
  hatchet: workflow "sync" step "call": workflow: Retry.DelayFor unsupported by this driver (missing RetryDelay): Hatchet sets a retry's delay from the backoff only, never from the error (RetryAfter)

jitter:
  needs: RetryJitter | memory has it: true
  retry 2 exact delay: 4s
  draw 0.000: 4s
  draw 0.500: 3.5s
  draw 0.999: 3.001s

precedence:
  job over step: 1
  step over workflow: 5
  workflow default: 2
  library default: 0 | DefaultRetryMax: 0
```

The attempt number comes from `workflow.StepInfoFrom(ctx).Attempt`, 1 for the first attempt.

## Pitfalls

- **A driver that cannot honour a policy refuses it; it never retries differently.** [`workflow.Requires(wf)`](ds:block?id=requires-6tcjbcfj) lists what a workflow needs (`RetryJitter` above). The Hatchet driver refuses at `Register` ([`checkRetry`](ds:block?id=hatchet-checkretry-ccamdfbn)), and the error names the field, as the example shows:

<!-- snippet: examples/retries/main.go#refusal -->
```go
wf := workflow.Workflow{
	Name: "sync",
	Retry: workflow.RetryPolicy{
		Max:     3,
		Backoff: workflow.Backoff{Initial: 2 * time.Second, Factor: 2, Jitter: 0.5}, // the jitter is what Hatchet refuses
	},
	Steps: []workflow.Step{{Name: "call", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}},
}
fmt.Println("  needs:", workflow.Requires(wf))
fmt.Println("  memory:", memory.New().Register(wf))
err := hatchet.New(hatchet.Config{}).Register(wf)
fmt.Println("  hatchet:", err, "| unsupported:", errors.Is(err, workflow.ErrUnsupported))

wf.Retry.Backoff.Jitter = 0
wf.Retry.DelayFor = workflow.RetryAfterDelay
fmt.Println("  hatchet:", hatchet.New(hatchet.Config{}).Register(wf))
```

- **Hatchet's backoff is `Factor^n` seconds.** The Hatchet driver refuses a `Backoff` whose `Initial` is not `Factor` seconds (the example's `Initial: 2s, Factor: 2` is the shape it accepts), any `Jitter` ([`retryBackoff`](ds:block?id=hatchet-retrybackoff-vrbwvu97)), and any `DelayFor`, including `RetryAfterDelay` ([`checkRetry`](ds:block?id=hatchet-checkretry-ccamdfbn)). Keep jitter and server-named delays to the memory, SQL and Redis drivers, or give Hatchet workflows a plain policy. See [Hatchet limitations](../drivers/hatchet.md#limitations).
- **`RetryAfter` does nothing without `DelayFor`.** Under a policy with no `DelayFor`, an error wrapped by [`RetryAfter`](ds:block?id=retryafter-xck6cdyv) retries on the normal backoff, on every driver ([`RetryPolicy.NextDelay`](ds:block?id=retrypolicy-nextdelay-94cqz66r)).
- **Requirements follow the effective policy.** A workflow default with jitter asks for `CapRetryJitter` only if some step inherits it; when every step sets its own `Retry`, the default asks for nothing ([`Requires`](ds:block?id=requires-6tcjbcfj)).
- **Hatchet fixes retries when the task is declared.** It has no per-dispatch policy, so it refuses `Job.Retry` ([`CheckJob`](ds:block?id=checkjob-2rsfu9ma) against its [caps](ds:block?id=hatchet-caps-d6x9fv8z)); check `Capabilities()` for `CapJobRetry`, `CapRetryUntil`, `CapRetryMaxErrors` and `CapDiscardExhausted` before relying on them.
- **`Max` counts retries, not attempts.** `Max: 3` means up to four attempts ([`RetryPolicy.Retryable`](ds:block?id=retrypolicy-retryable-ve8muksd)). A negative `Max` and a `Jitter` outside `[0, 1]` fail validation with `workflow.ErrInvalid` ([`validateRetry`](ds:block?id=validateretry-jcn44n64)).
- **A job's policy replaces the whole policy.** `Job.Retry: &RetryPolicy{Max: 4}` does not inherit the workflow's `Backoff`, `DelayFor` or `DiscardOnExhaust` ([`EffectiveRetriesFor`](ds:block?id=effectiveretriesfor-cv2kmktb)); set what you need on the job's policy (data fields only, see above).
- **A retried step runs again from the start.** Make its side effects idempotent (an upsert, an idempotency key on the API you call), or split the part that must not repeat into its own step with [`RetriesNone()`](ds:block?id=retriesnone-gvsc2j8f).
- **Snooze needs `CapSnooze`, or it is an ordinary error.** On a driver without it, [`Snooze`](ds:block?id=snooze-mcp3r4p7) is a failure like any other: it spends an attempt, goes through the retry policy (its delay is not used), and a step snoozing on its last attempt fails the run. Check `c.Capabilities().Has(workflow.CapSnooze)` where that matters; [`Requires`](ds:block?id=requires-6tcjbcfj) cannot tell, because a snooze is a return value, not a field.
- **A step that snoozes forever never ends.** Snoozes spend no attempt ([`Snooze`](ds:block?id=snooze-mcp3r4p7)), so nothing stops a step that is never ready; the retry policy is never asked, so `Max` and `Until` do not stop it. Bound it with `Workflow.RunTimeout`, or have the step return an error once it has waited long enough by its own measure.
- **On the SQL driver, a step whose worker dies is retried too.** An attempt whose lease expires more than [`WithMaxReclaims`](ds:block?id=sql-withmaxreclaims-mq8n5ud9) times (default [`5`](ds:block?id=sql-defaultmaxreclaims-p88haut9)) fails with [`sqlstore.ErrAbandoned`](ds:block?id=sql-errabandoned-m79vqdps), and the retry policy then applies as for any failure.

## Related

- [Workflows and steps](../workflows/steps.md), including [step timeouts](../workflows/steps.md#timeouts) and [OnFailure](../workflows/steps.md#run-a-handler-when-a-run-fails)
- [Capabilities](../concepts/capabilities.md)
- [Timeouts](timeouts.md): the deadlines that end a run that keeps retrying or snoozing.
- [Step middleware › Release a step for later](../middleware/step-middleware.md#release-a-step-for-later): put a step back before it runs.
- Example: [`examples/retries`](../../examples/retries)
- Example: [`examples/discard-exhausted`](../../examples/discard-exhausted) (the precedence, and giving up quietly)
- Example: [`examples/snooze-heartbeat`](../../examples/snooze-heartbeat) (snooze)
