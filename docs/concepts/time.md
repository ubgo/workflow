# Time

Every scheduling decision in the library reads time from an injected clock, never from `time.Now` or `time.NewTimer`. Production passes the wall clock; tests pass a fake clock that moves only when told to, which makes "sleep three days" run in microseconds and makes every run repeatable. This page explains the interface and where to pass a clock; driving a fake one in tests is covered in [Drive time in tests](../testing/fake-clock.md#drive-time-in-tests).

## Before you start

- You know the [client](client.md) lifecycle.

## The clock interface

[`clock.Clock`](ds:block?id=clock-clock-mw5ene3w) has two methods:

| Method | Returns |
|---|---|
| `Now() time.Time` | the current time |
| `NewTimer(d time.Duration) clock.Timer` | a one-shot timer that delivers once `d` has passed; a `d` of zero or less fires at once ([`Real`](ds:block?id=clock-real-newtimer-tha74sh9), [`Fake`](ds:block?id=clock-fake-newtimer-czeubckk)) |

A [`clock.Timer`](ds:block?id=clock-timer-h7br8pye) has `C() <-chan time.Time`, which delivers the fire time once (its buffer holds one value, so a timer never blocks the clock that fires it), and `Stop() bool`, which reports whether it stopped the timer before it fired.

[`clock.Real{}`](ds:block?id=clock-real-4mnh8435) is the wall clock and the one place in the module that reads `time.Now`. [`clock.NewFake(start)`](ds:block?id=clock-newfake-3fjcs7pm) returns a `*clock.Fake` that moves only on [`Advance`](ds:block?id=clock-fake-advance-r72nbpb5) or [`Set`](ds:block?id=clock-fake-set-dtnyakc2), firing due timers in deadline order ([`setLocked`](ds:block?id=clock-fake-setlocked-wzzuc4u9)).

What reads the clock, and the option that sets it:

| Component | Option | Default |
|---|---|---|
| memory driver: delayed starts, sleeps, wait deadlines, retry backoff, step timeouts, rate-limit windows, cron fires | [`memory.WithClock`](ds:block?id=memory-withclock-bkbmyc67) | `clock.Real{}` |
| SQL driver: due times, backoff, cron | [`sqlstore.WithClock`](ds:block?id=sql-withclock-43e3rabg); leases on [`sqlstore.WithLeaseClock`](ds:block?id=sql-withleaseclock-a4ytvmv4), separately | `clock.Real{}` for both |
| Redis driver: delayed starts, sleeps, retry backoff, step timeouts, cron fires | [`redis.WithClock`](ds:block?id=redis-withclock-beffbra2); leases and polling on [`redis.WithLeaseClock`](ds:block?id=redis-withleaseclock-csjjgd58), separately | `clock.Real{}` for both |
| Hatchet driver: what is local to the process (poll tickers, the delivered-run window) | [`hatchet.Config.Clock`](ds:block?id=hatchet-config-wdky3m9f) | `clock.Real{}`; times compared with the engine's stay on the wall clock |
| [`middleware.Logging`](ds:block?id=mw-logging-zanq5b67), [`Debounce`](ds:block?id=mw-debounce-5g2ska6s), [`NewMemoryUnique`](ds:block?id=mw-newmemoryunique-7y4bnqj8), [`NewMemoryDebounce`](ds:block?id=mw-newmemorydebounce-kqz9w4se), [`NewMemoryCircuit`](ds:block?id=mw-newmemorycircuit-fcnep8zj) | a `clock.Clock` argument | — |
| `batch`, `failover`, the outbox, the `otel` module | `WithClock` options ([`batch`](ds:block?id=batch-withclock-9hfgunah), [`failover`](ds:block?id=failover-withclock-4b2e3nph), [outbox](ds:block?id=outbox-withclock-bbxuj3eu), [`otel`](ds:block?id=otel-withclock-qtagzrcf)) | `clock.Real{}` |

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

// attempt runs h once. When timeout passes on the injected clock it
// cancels h's ctx with a workflow.TimeoutError, not wrapped, so the
// handler's context.Cause matches workflow.ErrStepTimeout, and fails the
// attempt with it whatever h returned, unless the handler chain claimed
// it (workflow.ClaimTimeout: a step WithRules tolerates keeps its record).
func (t *Tiny) attempt(ctx context.Context, h workflow.Handler, in []byte, timeout time.Duration) ([]byte, error) {
	if timeout <= 0 {
		return h(ctx, in)
	}
	info, _ := workflow.StepInfoFrom(ctx)
	errStep := workflow.TimeoutError(workflow.ErrStepTimeout,
		fmt.Sprintf("tiny: step %q attempt %d after %s", info.Step, info.Attempt, timeout))
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	timer := t.clk.NewTimer(timeout)
	defer timer.Stop()
	go func() {
		select {
		case <-timer.C():
			cancel(errStep)
		case <-ctx.Done():
		}
	}()
	hctx, claimed := workflow.WithTimeoutClaim(ctx)
	out, err := h(hctx, in)
	// Compare by identity: any other cause (the client stopping) is not
	// this attempt's timeout. A late success is never a success.
	switch cause := context.Cause(ctx); {
	case cause == errStep && !claimed():
		return nil, errStep
	case cause != errStep && err == nil && ctx.Err() != nil:
		return nil, cause
	}
	return out, err
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
- Handlers run on real goroutines. Advancing a fake clock before the goroutine under test has armed its timer fires nothing; [`Fake.BlockUntil(ctx, n)`](ds:block?id=clock-fake-blockuntil-mabgbssy) waits until `n` timers are pending ([Drive time in tests](../testing/fake-clock.md#drive-time-in-tests)).
- The fake clock never goes back: a negative [`Advance`](ds:block?id=clock-fake-advance-r72nbpb5) and a [`Set`](ds:block?id=clock-fake-set-dtnyakc2) to an earlier time are ignored.
- On the SQL and Redis drivers, leases measure whether a worker is alive, which is wall time, so they have their own clock (`WithLeaseClock`: [SQL](ds:block?id=sql-withleaseclock-a4ytvmv4), [Redis](ds:block?id=redis-withleaseclock-csjjgd58)): moving the scheduling clock by an hour in a test must not make every running step look abandoned.
- Crons evaluate in an explicit zone, not in the clock's zone: [`ValidateSchedule`](ds:block?id=validateschedule-knj437ed) refuses a schedule without a `Location` ([Schedules in a time zone](../schedules/time-zones.md#schedules-in-a-time-zone)).

## Related

- [Drive time in tests](../testing/fake-clock.md#drive-time-in-tests)
- [Write a driver](../extending/writing-a-driver.md#what-a-driver-implements)
- Example: [custom-driver](../../examples/custom-driver/)
