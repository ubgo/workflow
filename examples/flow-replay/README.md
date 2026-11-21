# flow-replay

Deterministic replay on the in-memory driver with a fake clock: a refund written as one Go function with local variables waits three days, then for an approver who resolves a promise from outside the run, then a day before closing the ticket. The function runs again from the top on every wake; each recorded call returns its result without running again, so the body runs four times while every side effect runs once. A card refund refused with a permanent error is recorded, and every wake takes the store credit branch again. A second version of the code branches on a flag it never recorded and fails with the nondeterminism error.

```sh
cd examples
go run ./flow-replay
```

```text
refund:
  order A-1 loaded: 40 EUR
  in the return window the run is waiting, holding no worker
  three days later it waits for the approver
  approved by dana
  card refund refused (card expired), paying store credit
  result: A-1: 40 EUR as store credit credit-7, approved by dana, closed 2026-03-06T09:15:00Z
  the body ran 4 times; load-order ran 1, refund-card 1, store-credit 1
nondeterminism:
  the run ended failed: memory: step "orchestrate" failed after 1 attempt(s): workflow: replay: call 0 is step store-credit, but the history has step refund-card
  position 0: recorded "step refund-card", the code called "step store-credit"; permanent true
  matches ErrNondeterminism: true
child and pay day:
  invoice child returned "INV-s-9"
  result: paid "INV-s-9" at 2026-03-31T17:00:00Z
  the body ran 3 times; the invoice child ran 1
journal:
  runs holding history after every run finished: 0
```

## The code

The client, with the Journal plugin that keeps the history:

<!-- snippet: examples/flow-replay/main.go#client -->
```go
clk := clock.NewFake(time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC)) // clock.Real{} in production
journal := workflow.NewMemoryJournal()                            // the run's history
c, err := workflow.Use(memory.New(memory.WithClock(clk)), workflow.Journal(journal))
if err != nil {
	return err
}
```

The refund as one replayed function:

<!-- snippet: examples/flow-replay/main.go#refund -->
```go
// refund is the whole refund as one function: each wake runs it from
// the top, and the recorded calls return without running again.
refund := func(rc *workflow.ReplayCtx, in []byte) ([]byte, error) {
	calls.add("body")
	var o order
	if err := json.Unmarshal(in, &o); err != nil {
		return nil, workflow.Permanent(err)
	}
	amount, err := workflow.ReplayStep(rc, "load-order", func(ctx context.Context) (int, error) {
		calls.add("load-order")
		return 40, nil // read from the orders table
	})
	if err != nil {
		return nil, err
	}
	if !rc.Replaying() { // log once, not on every wake
		fmt.Printf("  order %s loaded: %d EUR\n", o.ID, amount)
	}

	// Three days for the parcel to come back; no worker is held.
	if err := rc.Sleep("return-window", 72*time.Hour); err != nil {
		return nil, err
	}
	approver, err := rc.AwaitPromise("approval", promises, "refund-"+o.ID)
	if err != nil {
		return nil, err
	}
	if !rc.Replaying() {
		fmt.Printf("  approved by %s\n", approver)
	}

	method := "card"
	_, err = workflow.ReplayStep(rc, "refund-card", func(ctx context.Context) (string, error) {
		calls.add("refund-card")
		return "", workflow.Permanent(errors.New("card expired")) // the payment provider's answer
	})
	if err != nil && !workflow.IsPermanent(err) {
		return nil, err // not recorded: the step's retry calls the provider again
	}
	if err != nil { // recorded: every wake takes this branch again
		if !rc.Replaying() {
			fmt.Printf("  card refund refused (%v), paying store credit\n", err)
		}
		credit, err := workflow.ReplayStep(rc, "store-credit", func(ctx context.Context) (string, error) {
			calls.add("store-credit")
			return "credit-7", nil
		})
		if err != nil {
			return nil, err
		}
		method = "store credit " + credit
	}

	// Keep the ticket open a day, then close it.
	if err := rc.Sleep("close-after", 24*time.Hour); err != nil {
		return nil, err
	}
	closed, err := rc.Now("closed-at")
	if err != nil {
		return nil, err
	}
	return json.Marshal(fmt.Sprintf("%s: %d EUR as %s, approved by %s, closed %s",
		o.ID, amount, method, approver, closed.Format(time.RFC3339)))
}
```

Turning it into a step handler:

<!-- snippet: examples/flow-replay/main.go#register -->
```go
handler, err := workflow.NewReplay(refund, workflow.ReplayOptions{
	Client: c,                // refused here without CapSnooze
	Clock:  clk,              // the time of Sleep and Now; the driver's clock
	Poll:   15 * time.Minute, // how often an unsettled promise is looked at
})
if err != nil {
	return workflow.Workflow{}, err
}
wf := workflow.Workflow{Name: "refund", Steps: []workflow.Step{{Name: "orchestrate", Run: handler}}}
```

The approver resolving the promise from outside the run:

<!-- snippet: examples/flow-replay/main.go#approve -->
```go
if err := workflow.ResolvePromise(ctx, c, promises, "refund-A-1", []byte("dana")); err != nil {
	return err
}
```

Code that branches on a value it did not record:

<!-- snippet: examples/flow-replay/main.go#drift -->
```go
// drift records one call whose name depends on an unrecorded flag.
drift := func(rc *workflow.ReplayCtx, _ []byte) ([]byte, error) {
	step := "refund-card"
	if creditFirst.Load() { // wrong: read again on every run, never recorded
		step = "store-credit"
	}
	if _, err := workflow.ReplayStep(rc, step, func(ctx context.Context) (bool, error) { return true, nil }); err != nil {
		return nil, err
	}
	return nil, rc.Sleep("close-after", 24*time.Hour) // a day, as in the refund
}
```

Reading the error's fields from the run-finished hook:

<!-- snippet: examples/flow-replay/main.go#drift-error -->
```go
stepErr := <-ended // RunEvent.Err from the run-finished hook
var nd *workflow.NondeterminismError
if errors.As(stepErr, &nd) {
	fmt.Printf("  position %d: recorded %q, the code called %q; permanent %v\n",
		nd.Position, nd.Recorded, nd.Got, workflow.IsPermanent(stepErr))
}
```

The guide is [docs/workflows/replay.md](../../docs/workflows/replay.md).
