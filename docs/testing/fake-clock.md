# Drive time in tests

Scheduling code is full of waits: a reminder in a day, a retry in five minutes, a debounce window, a cool-off. A test that sleeps for them is slow and flaky. Nothing in this library reads `time.Now` or starts a `time.Timer` in its logic: it takes a `clock.Clock`, so production passes `clock.Real{}` and a test passes `clock.NewFake(start)`, a clock that moves only when the test says so. "Wait a day" then runs in microseconds, and every run is the same.

Your app code can take the same interface, which is what makes its waits testable too.

## Before you start

- Code that takes a `clock.Clock` (`Now()` and `NewTimer(d)`) instead of calling `time.Now` and `time.NewTimer`.
- The drivers and packages that schedule anything take one: `memory.WithClock`, `sqlstore.WithClock`, `outbox.WithClock`, `failover.WithClock`, and the clock argument of `middleware.Debounce`, `NewMemoryUnique` and `NewMemoryDebounce`.
- The runnable program for this page is [examples/app-tests](../../examples/app-tests/).

## Drive time in tests

The app code waits on its clock, then enqueues a nudge:

<!-- snippet: examples/app-tests/main.go#nudge -->
```go
// NudgeAfter waits d on the app's clock, then enqueues a nudge. A test on
// a fake clock moves d on without sleeping.
func (a *Accounts) NudgeAfter(ctx context.Context, d time.Duration, email string) error {
	t := a.Clock.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C():
	}
	_, err := a.Jobs.Enqueue(ctx, workflow.Job{Workflow: "nudge", Payload: fmt.Appendf(nil, `{"email":%q}`, email)})
	return err
}
```

The test runs it in a goroutine on a fake clock and moves time on:

<!-- snippet: examples/app-tests/main_test.go#fake-clock -->
```go
func TestNudgeAfterADay(t *testing.T) {
	ctx := context.Background()
	app, fake := newApp(t)
	clk := clock.NewFake(start)
	app.Clock = clk

	done := make(chan error, 1)
	go func() { done <- app.NudgeAfter(ctx, 24*time.Hour, "ada@example.com") }()

	// Wait until the goroutine has armed its timer, or Advance could run
	// first and the timer would be due a day after the advance.
	if err := clk.BlockUntil(ctx, 1); err != nil {
		t.Fatal(err)
	}
	clk.Advance(23 * time.Hour)
	if clk.Pending() != 1 {
		t.Fatal("the nudge fired early")
	}
	clk.Set(start.Add(24 * time.Hour)) // jump to an absolute time
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	fake.AssertEnqueued(t, "nudge", workflowtest.HasPayloadJSON(map[string]string{"email": "ada@example.com"}))
	if clk.Pending() != 0 {
		t.Fatal("a timer is still pending")
	}
}
```

What the fake clock offers:

| Method | What it does |
|---|---|
| `clock.NewFake(start)` | A clock set to `start`. The zero value is not ready; always use `NewFake` |
| `Now()` | The fake time |
| `NewTimer(d)` | A timer due at `Now()+d`; a `d` of zero or less fires at once, as `time.NewTimer` does |
| `Advance(d)` | Moves the clock forward by `d` and fires every timer now due, in deadline order, each receiving its deadline. A negative `d` is ignored: the clock never goes back |
| `Set(t)` | Moves the clock to `t`, if `t` is later, and fires every timer now due |
| `Pending()` | The number of timers that have not fired or been stopped |
| `BlockUntil(ctx, n)` | Waits until at least `n` timers are pending, so the test advances only after the code under test has armed its timer; returns `ctx.Err()` if `ctx` ends first |

The other examples on these pages use the same clock for library features: [unique-jobs](../../examples/unique-jobs/) expires a lock, [debounce](../../examples/debounce/) passes a window on the memory driver, [claim-check](../../examples/claim-check/) fires a delayed run, [outbox](../../examples/outbox/) ends a backoff and [failover](../../examples/failover/) ends a cool-off.

## Try it

```sh
cd examples
go test ./app-tests/ -run TestNudgeAfterADay -v
```

```text
=== RUN   TestNudgeAfterADay
--- PASS: TestNudgeAfterADay (0.00s)
PASS
```

## Pitfalls

- Advance only after the timer exists. A timer created after `Advance` is due `d` after the new time, so the test waits forever; `BlockUntil` is the fix when the timer is armed on another goroutine.
- `BlockUntil` counts every pending timer of the clock. When a driver shares the clock (its poll timer, a retry backoff), wait for the total you expect, not for one.
- Handlers still run on real goroutines: advancing the clock makes a delayed run due, not finished. Wait for the run's effect (a channel, a run-finished observer, `Status`) before asserting.
- The clock never goes back: `Set` to an earlier time and a negative `Advance` are ignored.
- A fake clock passed to a driver controls its due times; tests on a durable driver still read and write a real database, so they are not instant.

## Related

- [Test app code with the Fake](fake.md).
- [Time in the library](../concepts/time.md): where each package reads its clock.
- [Debounce](../reliability/debounce.md) and [failover cool-off](../reliability/failover.md#cool-off): features whose tests rely on a fake clock.
- Example: [examples/app-tests](../../examples/app-tests/).
