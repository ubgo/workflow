# Time

Every scheduling decision in the library reads time from an injected clock, never from `time.Now` or `time.NewTimer`. Production passes the wall clock; tests pass a fake clock that moves only when told to, which makes "sleep three days" run in microseconds and makes every run repeatable. This page explains the interface and where to pass a clock; driving a fake one in tests is covered in [Drive time in tests](../testing/fake-clock.md#drive-time-in-tests).

## Before you start

- You know the [client](client.md) lifecycle.

## The clock interface

`clock.Clock` has two methods:

| Method | Returns |
|---|---|
| `Now() time.Time` | the current time |
| `NewTimer(d time.Duration) clock.Timer` | a one-shot timer that delivers once `d` has passed; a `d` of zero or less fires at once |

A `clock.Timer` has `C() <-chan time.Time`, which delivers the fire time once (its buffer holds one value, so a timer never blocks the clock that fires it), and `Stop() bool`, which reports whether it stopped the timer before it fired.

`clock.Real{}` is the wall clock and the one place in the module that reads `time.Now`. `clock.NewFake(start)` returns a `*clock.Fake` that moves only on `Advance` or `Set`, firing due timers in deadline order.

What reads the clock, and the option that sets it:

| Component | Option | Default |
|---|---|---|
| memory driver: delayed starts, sleeps, wait deadlines, retry backoff, step timeouts, rate-limit windows, cron fires | `memory.WithClock` | `clock.Real{}` |
| SQL driver: due times, backoff, cron | `sqlstore.WithClock`; leases on `sqlstore.WithLeaseClock`, separately | `clock.Real{}` for both |
| Hatchet driver: what is local to the process (poll tickers, the delivered-run window) | `hatchet.Config.Clock` | `clock.Real{}`; times compared with the engine's stay on the wall clock |
| `middleware.Logging`, `Debounce`, `NewMemoryUnique`, `NewMemoryDebounce`, `NewMemoryCircuit` | a `clock.Clock` argument | — |
| `batch`, `failover`, the outbox, the `otel` module | `WithClock` options | `clock.Real{}` |

A driver you write takes a clock the same way and reads every timer from it. The [custom-driver](../../examples/custom-driver/) example's step loop waits for its retry backoff and its step timeout on the injected clock:

<!-- snippet: examples/custom-driver/driver.go#attempts -->
```go
// runStep tries a step until it succeeds or its effective retry policy
// gives up. Every attempt carries StepInfo on its ctx and its timeout,
// and every wait reads the injected clock, never time.Now.
func (t *Tiny) runStep(ctx context.Context, wf workflow.Workflow, s workflow.Step, job workflow.Job, id workflow.RunID, in []byte) ([]byte, error) {
	policy := workflow.EffectiveRetries(s, wf)
	timeout := s.Timeout
	if timeout == 0 {
		timeout = wf.Timeout
	}
	for attempt := 1; ; attempt++ {
		info := workflow.StepInfo{Workflow: wf.Name, Step: s.Name, RunID: id, Attempt: attempt, Labels: job.Labels}
		out, err := t.attempt(workflow.WithStepInfo(ctx, info), s.Run, in, timeout)
		if err == nil {
			return out, nil
		}
		if ctx.Err() != nil || !policy.Retryable(err, attempt) {
			return nil, err
		}
		if !t.sleep(ctx, policy.NextDelay(err, attempt, nil)) {
			return nil, ctx.Err()
		}
	}
}

// attempt runs h once, cancelling its ctx when timeout passes on the
// injected clock.
func (t *Tiny) attempt(ctx context.Context, h workflow.Handler, in []byte, timeout time.Duration) ([]byte, error) {
	if timeout <= 0 {
		return h(ctx, in)
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	timer := t.clk.NewTimer(timeout)
	defer timer.Stop()
	go func() {
		select {
		case <-timer.C():
			cancel(fmt.Errorf("step timed out after %s", timeout))
		case <-ctx.Done():
		}
	}()
	out, err := h(ctx, in)
	if err == nil && ctx.Err() != nil {
		err = context.Cause(ctx)
	}
	return out, err
}

// sleep waits d on the injected clock, or reports false when ctx ends.
func (t *Tiny) sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := t.clk.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C():
		return true
	case <-ctx.Done():
		return false
	}
}
```

Because of that, the conformance suite can run it on a fake clock and move time itself:

<!-- snippet: examples/custom-driver/main_test.go#conformance -->
```go
// TestConformance holds Tiny to the same suite as every shipped driver.
// Subtests that need a capability Tiny lacks are skipped, naming it; the
// rest must pass.
func TestConformance(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	workflowtest.Run(t, workflowtest.Harness{New: func(t *testing.T) workflowtest.Instance {
		clk := clock.NewFake(start)
		return workflowtest.Instance{Client: NewTiny(clk), Clock: clk}
	}})
}
```

Handlers should do the same: read time from a clock your app injects, not from `time.Now`, so a replayed or tested step sees the same values.

## Try it

```sh
cd examples
go test ./custom-driver -run TestConformance
```

```text
ok  	github.com/ubgo/workflow/examples/custom-driver	0.507s
```

The suite's retry and timeout cases run in milliseconds although they ask for minutes, because the fake clock jumps.

## Pitfalls

- A handler that reads `time.Now` is invisible to the fake clock: a test that advances an hour still sees the wall time inside the step.
- Handlers run on real goroutines. Advancing a fake clock before the goroutine under test has armed its timer fires nothing; `Fake.BlockUntil(ctx, n)` waits until `n` timers are pending ([Drive time in tests](../testing/fake-clock.md#drive-time-in-tests)).
- The fake clock never goes back: a negative `Advance` and a `Set` to an earlier time are ignored.
- On the SQL driver, leases measure whether a worker is alive, which is wall time, so they have their own clock (`WithLeaseClock`): moving the scheduling clock by an hour in a test must not make every running step look abandoned.
- Crons evaluate in an explicit zone, not in the clock's zone ([Schedules in a time zone](../schedules/time-zones.md#schedules-in-a-time-zone)).

## Related

- [Drive time in tests](../testing/fake-clock.md#drive-time-in-tests)
- [Write a driver](../extending/writing-a-driver.md#what-a-driver-implements)
- Example: [custom-driver](../../examples/custom-driver/)
