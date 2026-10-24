# retries

How a failed step is tried again: an exponential backoff with jitter, a delay named by the error (a 429's Retry-After), errors that must not retry, a step that opts out of the workflow's default, and a driver that refuses a policy it cannot honour. It runs on the in-memory driver with a fake clock and a pinned jitter source, so every delay below is exact; the Hatchet refusals need no server, because the Hatchet client connects only at `Start`.

```sh
cd examples
go run ./retries
```

```text
retry 1: delay 1s, with draw 0.5: 750ms
retry 2: delay 2s, with draw 0.5: 1.5s
retry 3: delay 4s, with draw 0.5: 3s
retry 4: delay 8s, with draw 0.5: 6s
retry 5: delay 10s, with draw 0.5: 7.5s

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
```

Each attempt's time is measured from the enqueue on the fake clock. With `Jitter: 0.5` and a pinned draw of 0.5, the 1s and 2s delays become 750ms and 1.5s.

## The code

A policy with three retries and a jittered exponential backoff:

<!-- snippet: examples/retries/main.go#policy -->
```go
policy := workflow.RetryPolicy{
	Max: 3, // three retries after the first attempt
	Backoff: workflow.Backoff{
		Initial: time.Second,
		Factor:  2,
		Max:     time.Minute,
		Jitter:  0.5, // each delay is drawn from [d/2, d]
	},
}
```

The delays a backoff produces, with no client:

<!-- snippet: examples/retries/main.go#delays -->
```go
b := workflow.Backoff{Initial: time.Second, Factor: 2, Max: 10 * time.Second, Jitter: 0.5}
for n := 1; n <= 5; n++ {
	// JitteredDelay takes the uniform draw in [0, 1) as an argument.
	fmt.Fprintf(out, "retry %d: delay %s, with draw 0.5: %s\n", n, b.Delay(n), b.JitteredDelay(n, 0.5))
}
```

A retry delay taken from the error:

<!-- snippet: examples/retries/main.go#retry-after -->
```go
rateLimited := workflow.RetryPolicy{
	Max:      2,
	Backoff:  workflow.Backoff{Initial: time.Second},
	DelayFor: workflow.RetryAfterDelay, // honour RetryAfter(err, d)
}
callAPI := func(n int) error {
	if n == 1 {
		// The API answered 429 with Retry-After: 30.
		return workflow.RetryAfter(errors.New("429 too many requests"), 30*time.Second)
	}
	return nil
}
```

Errors that must not retry, by marking them or by policy:

<!-- snippet: examples/retries/main.go#no-retry -->
```go
noRetry := workflow.RetryPolicy{
	Max:       5,
	Backoff:   workflow.Backoff{Initial: time.Second},
	NoRetryOn: func(err error) bool { return errors.Is(err, errNotFound) },
}
charge := func(int) error {
	return workflow.Permanent(errors.New("card declined")) // retrying cannot help
}
lookup := func(int) error { return fmt.Errorf("lookup: %w", errNotFound) }
```

A step that never retries inside a workflow that does:

<!-- snippet: examples/retries/main.go#per-step -->
```go
signupDefault := workflow.RetryPolicy{Max: 3, Backoff: workflow.Backoff{Initial: time.Second}}
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

The memory driver with its jitter draws pinned:

<!-- snippet: examples/retries/main.go#client -->
```go
c := memory.New(
	memory.WithClock(clk),
	memory.WithJitterSource(half), // pin the jitter draws for a repeatable demo
)
```

The delay every driver computes for a retry:

<!-- snippet: examples/retries/main.go#next-delay -->
```go
d := a.policy.NextDelay(a.err, a.n, half)
```

What a workflow needs, and Hatchet refusing jitter and DelayFor:

<!-- snippet: examples/retries/main.go#refusal -->
```go
wf := workflow.Workflow{
	Name: "sync",
	Retry: workflow.RetryPolicy{
		Max:     3,
		Backoff: workflow.Backoff{Initial: 2 * time.Second, Factor: 2, Jitter: 0.5},
	},
	Steps: []workflow.Step{{Name: "call", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}},
}
fmt.Fprintln(out, "  needs:", workflow.Requires(wf))
fmt.Fprintln(out, "  memory:", memory.New().Register(wf))
err := hatchet.New(hatchet.Config{}).Register(wf)
fmt.Fprintln(out, "  hatchet:", err, "| unsupported:", errors.Is(err, workflow.ErrUnsupported))

wf.Retry.Backoff.Jitter = 0
wf.Retry.DelayFor = workflow.RetryAfterDelay
fmt.Fprintln(out, "  hatchet:", hatchet.New(hatchet.Config{}).Register(wf))
```

`main_test.go` runs the program and compares its whole output with the block above.

## Guides

- [Retries](../../docs/jobs/retries.md)
