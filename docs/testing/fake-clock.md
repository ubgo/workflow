# Drive time in tests

Scheduling code is full of waits: a reminder in a day, a retry in five minutes, a debounce window, a cool-off. A test that sleeps for them is slow and flaky. Nothing in this library reads `time.Now` or starts a `time.Timer` in its logic: it takes a [`clock.Clock`](ds:block?id=clock-clock-mw5ene3w), so production passes [`clock.Real{}`](ds:block?id=clock-real-4mnh8435) and a test passes [`clock.NewFake(start)`](ds:block?id=clock-newfake-3fjcs7pm), a clock that moves only when the test says so. "Wait a day" then runs in microseconds, and every run is the same.

Your app code can take the same interface, which is what makes its waits testable too.

## Before you start

- Code that takes a [`clock.Clock`](ds:block?id=clock-clock-mw5ene3w) (`Now()` and `NewTimer(d)`) instead of calling `time.Now` and `time.NewTimer`.
- The drivers and packages that schedule anything take one: [`memory.WithClock`](ds:block?id=memory-withclock-bkbmyc67), [`sqlstore.WithClock`](ds:block?id=sql-withclock-43e3rabg), [`redis.WithClock`](ds:block?id=redis-withclock-beffbra2), [`outbox.WithClock`](ds:block?id=outbox-withclock-bbxuj3eu), [`failover.WithClock`](ds:block?id=failover-withclock-4b2e3nph), and the clock argument of [`middleware.Debounce`](ds:block?id=mw-debounce-5g2ska6s), [`NewMemoryUnique`](ds:block?id=mw-newmemoryunique-7y4bnqj8) and [`NewMemoryDebounce`](ds:block?id=mw-newmemorydebounce-kqz9w4se).
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
| [`clock.NewFake(start)`](ds:block?id=clock-newfake-3fjcs7pm) | A clock set to `start`. The zero value is not ready; always use `NewFake` |
| `Now()` | The fake time |
| [`NewTimer(d)`](ds:block?id=clock-fake-newtimer-czeubckk) | A timer due at `Now()+d`; a `d` of zero or less fires at once, as `time.NewTimer` does |
| [`Advance(d)`](ds:block?id=clock-fake-advance-r72nbpb5) | Moves the clock forward by `d` and fires every timer now due, in deadline order, each receiving its deadline. A negative `d` is ignored: the clock never goes back |
| [`Set(t)`](ds:block?id=clock-fake-set-dtnyakc2) | Moves the clock to `t`, if `t` is later, and fires every timer now due |
| [`Pending()`](ds:block?id=clock-fake-pending-agw73umv) | The number of timers that have not fired or been stopped |
| [`BlockUntil(ctx, n)`](ds:block?id=clock-fake-blockuntil-mabgbssy) | Waits until at least `n` timers are pending, so the test advances only after the code under test has armed its timer; returns `ctx.Err()` if `ctx` ends first |

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

- Advance only after the timer exists. A timer created after `Advance` is due `d` after the new time ([`NewTimer`](ds:block?id=clock-fake-newtimer-czeubckk)), so the test waits forever; [`BlockUntil`](ds:block?id=clock-fake-blockuntil-mabgbssy) is the fix when the timer is armed on another goroutine.
- [`BlockUntil`](ds:block?id=clock-fake-blockuntil-mabgbssy) counts every pending timer of the clock. When a driver shares the clock (its poll timer, a retry backoff), wait for the total you expect, not for one.
- Handlers still run on real goroutines: [`Advance`](ds:block?id=clock-fake-advance-r72nbpb5) fires the timers now due and returns, so it makes a delayed run due, not finished. Wait for the run's effect (a channel, a run-finished observer, `Status`) before asserting.
- The clock never goes back: [`Set`](ds:block?id=clock-fake-set-dtnyakc2) to an earlier time and a negative [`Advance`](ds:block?id=clock-fake-advance-r72nbpb5) are ignored ([`TestFakeStopAndImmediate`](ds:block?id=clock-testfakestopandimmediate-9dpsy628)).
- A fake clock passed to a driver ([`sqlstore.WithClock`](ds:block?id=sql-withclock-43e3rabg)) controls its due times; tests on a durable driver still read and write a real database, so they are not instant.

## Related

- [Test app code with the Fake](fake.md).
- [Time in the library](../concepts/time.md): where each package reads its clock.
- [Debounce](../reliability/debounce.md) and [failover cool-off](../reliability/failover.md#cool-off): features whose tests rely on a fake clock.
- Example: [examples/app-tests](../../examples/app-tests/).
