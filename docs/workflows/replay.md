# Deterministic replay

A refund waits three days for the parcel to come back, then for a person to approve it, then a day before the ticket closes. Written as workflow steps, every wait is a step boundary: the order amount, the approver's name and the outcome of the card refund have to be passed from step to step as input and output, and the logic that reads as ten lines of Go is spread over a graph. Written as one handler, it would hold a worker for four days and lose its local variables in a crash. Deterministic replay lets you write it as the ten lines: one Go function with local variables, whose waits suspend it without holding a worker, and which picks up where it stopped after a wake, a retry or a crash. This is the model of the replay-based engines, built on the journal, snoozes and promises of this library and run on any driver that can snooze.

## Before you start

- The client needs the `workflow.Journal` plugin: the history of a replayed handler lives in the run's journal. Without it the step fails with `workflow.ErrNoJournal`, marked permanent. Pick a durable `JournalStore` (`sqlstore.Client.JournalStore()`, `redis.Client.JournalStore()`) when a run must survive a crash or move to another process; `workflow.MemoryJournal` holds only in one process. See [Memoise work inside a step](memoisation.md#turn-on-the-journal).
- Waits suspend the step with a snooze, so the driver needs `workflow.CapSnooze`: memory, SQL and Redis. Hatchet has no snooze, and `NewReplay` refuses it when you pass the client in `ReplayOptions.Client`.
- `AwaitPromise` reads a `workflow.PromiseStore`, the same one the code that resolves the promise uses ([Promises](promises.md#wait-for-a-promise)).
- The example is [`examples/flow-replay`](../../examples/flow-replay). Run it with `go run ./flow-replay` from `examples/`.

## Write the code as one function

Turn on the journal on the client. The example runs on a fake clock so that three days pass at once; production passes `clock.Real{}` or nothing:

<!-- snippet: examples/flow-replay/main.go#client -->
```go
clk := clock.NewFake(time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC)) // clock.Real{} in production
journal := workflow.NewMemoryJournal()                            // the run's history
c, err := workflow.Use(memory.New(memory.WithClock(clk)), workflow.Journal(journal))
if err != nil {
	return err
}
```

A replayed handler is a `workflow.ReplayFunc`: it gets a `*workflow.ReplayCtx` and the step's input, and returns the step's output. Every call that touches the world goes through the `ReplayCtx`. Local variables (`amount`, `approver`, `method`) live across the waits:

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

`workflow.NewReplay(fn, opts)` turns it into an ordinary step `Handler`, so it goes into a `workflow.Step` like any other:

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

`ReplayOptions` has three fields. `Client`, when set, is checked for `CapSnooze` here, so a wrong driver is refused at start-up with an `UnsupportedError` instead of failing the first wait. `Clock` is the time source of `Sleep`, `SleepUntil` and `Now`; nil means `clock.Real{}`. Give it the clock the driver uses, so a suspended wait and the driver's snooze timer agree. `Poll` is how often a wait on an unsettled promise looks again; zero means `workflow.DefaultPromisePoll` (five seconds).

The approver settles the promise from outside the run, from an HTTP handler or a tool, with the same store:

<!-- snippet: examples/flow-replay/main.go#approve -->
```go
if err := workflow.ResolvePromise(ctx, c, promises, "refund-A-1", []byte("dana")); err != nil {
	return err
}
```

## How a replayed handler runs

- **It runs again from the top on every wake.** Each run of the function is one attempt of the step. The first run in the example loads the order, reaches the three-day sleep and stops; three days later the function starts again at its first line.
- **Recorded calls return their results.** Every call through the `ReplayCtx` is recorded in order in the run's journal. On a later run each recorded call returns its recorded result and does not run its function again, so the code reaches the same point with the same local variables. In the example the body runs four times while `load-order`, `refund-card` and `store-credit` run once each.
- **A wait that is not over suspends the function.** `Sleep` records its wake time and `AwaitPromise` looks at the promise; while the wait is not over the call returns an error that snoozes the step, and the handler returns it. The run reports `StatusWaiting`, no worker is held and no attempt is spent, and the step runs again when the wait may be over: at the wake time, or after `Poll` for a promise.
- **The function finishes like any handler.** Its output is the step's output; the history is forgotten with the rest of the run's journal when the run ends.

`rc.Replaying()` reports whether the code is still inside its recorded history: true while the next call is one an earlier run already made, false once the code has gone past the history and reaches this point for the first time. Use it to log or emit a metric once instead of on every wake, as the example does after `load-order` and `approval`: on the run that first gets there the next call is not recorded yet, so it is false, and on every later run it is true. It reads the journal once per position. `rc.Context()` is the step's context, for code that needs it outside a call.

## The calls

| Call | What it records | On a replay |
|---|---|---|
| `workflow.ReplayStep[T](rc, name, fn)` | `fn`'s result as JSON, or its permanent error | The recorded result or error; `fn` is not called |
| `rc.Now(name)` | The time on the replay's clock | The recorded time, so code may branch on it |
| `rc.Sleep(name, d)` | The wake time, `d` after the first time the code reached it | Suspends until the recorded wake time, then returns nil |
| `rc.SleepUntil(name, t)` | `t` as the wake time | As `Sleep` |
| `rc.AwaitPromise(name, store, id)` | The promise's value once settled, or its rejection | The recorded value, or a permanent `*workflow.PromiseRejectedError` |
| `workflow.ReplayChild(rc, name, client, job, opts)` | A child run's result once it succeeded, or its failure (the child is spawned once, through `Spawn`, and looked at every `opts.Poll` while it runs) | The recorded result, or a permanent error with the recorded text; the client is not asked again |

The refund above uses `ReplayStep`, `Sleep`, `AwaitPromise` and `Now`. A payout in the same example uses the other two: it starts an invoice as a child run, which is spawned once however often the body runs, and then sleeps until a fixed pay day rather than for a duration:

<!-- snippet: examples/flow-replay/main.go#calls -->
```go
// payout pays a seller after an invoice child run, on pay day.
payout := func(rc *workflow.ReplayCtx, in []byte) ([]byte, error) {
	calls.add("payout body")
	// The child is spawned once; later wakes look at it until it ended.
	invoice, err := workflow.ReplayChild(rc, "invoice", c,
		workflow.Job{Workflow: "invoice", Payload: in}, workflow.ChildOptions{Poll: childPoll})
	if err != nil {
		return nil, err // a suspension while the child runs, or its failure
	}
	if !rc.Replaying() {
		fmt.Printf("  invoice child returned %s\n", invoice)
	}
	// A fixed instant, unlike Sleep's duration from the first time here.
	if err := rc.SleepUntil("pay-day", payDay); err != nil {
		return nil, err
	}
	paidAt, err := rc.Now("paid-at")
	if err != nil {
		return nil, err
	}
	return json.Marshal(fmt.Sprintf("paid %s at %s", invoice, paidAt.Format(time.RFC3339)))
}
```

While the child runs, `ReplayChild` suspends the handler for `ChildOptions.Poll` (`workflow.DefaultChildPoll`, five seconds, when zero) and looks again on the next wake; once the child succeeded its result is recorded and the client is not asked again. The child's client needs results (`workflow.ResultReader`), and the parent's driver `CapSnooze`, as for [`AwaitChild`](children.md#wait-for-children-with-awaitchildren). `SleepUntil` records the instant it was given, so a time already past returns at once, and a replay keeps the recorded instant even when the code would now pass another one.

Calls are matched to the history by their position, and the name labels the position: it must be the same on every run, but it need not be unique, so a loop may call `ReplayStep(rc, "page", ...)` on every iteration. `ReplayChild` is the exception: its name must be unique within the run (see the pitfalls). An empty name is refused.

**Side effects are at least once.** The process can die after `fn` returns and before its result is saved, and a save can fail; the next run then calls `fn` again. Work that must never repeat, such as a charge or a refund, also carries an idempotency key the remote side honours: build it from the run (`workflow.StepInfoFrom(rc.Context())` gives the `RunID`) and the call's name, never from a random value.

**A result goes through JSON, on every run.** The code sees the value decoded back from its JSON, on the first run as on every replay, so both are the same even for a value JSON does not carry exactly (a time's zone name, unexported fields). Keep results plain data (numbers, strings, structs of exported fields, UTC times). A recorded result that no longer decodes into `T` fails the run, marked permanent.

## Errors

- **A permanent error is recorded.** When `fn` returns an error marked `workflow.Permanent`, the error is recorded and returned, and every replay returns it again without calling `fn`, so the code takes the same branch on every run. Return it to fail the step, or handle it and carry on, as the example does when the card refund is refused and it pays store credit instead.
- **A recorded error keeps its text, not its type, on every run.** The code gets a new permanent error with the recorded text, on the first run as on every replay, so it can never take one branch on the first run and another later: `workflow.IsPermanent` and `err.Error()` work, `errors.Is` against your own sentinel and `errors.As` into your own type do not. Branch on `err != nil`, `IsPermanent` or the text, or record the decision itself: return `(decision, nil)` from `fn` instead of an error.
- **Any other error is not recorded, and fails this run of the function.** It is returned as it is, and every later call through the `ReplayCtx` returns it too, and the handler returns it whatever the function returns: code cannot ignore it and record a later call at a position it does not own. The step's retry policy runs the function, and `fn`, again. Retries spend attempts as for any step; snoozes do not. A journal read or write that fails is handled the same way.
- **A rejected promise** is recorded like a permanent error and returns a permanent `*workflow.PromiseRejectedError` on every run.

## Keep the code deterministic

The rule a replayed handler keeps: given the same recorded results, it makes the same calls in the same order. Branching on a recorded result is fine, since every run sees the same value. Branching on anything else (the wall clock, a random number, map iteration order, a global, a feature flag, a database read outside `ReplayStep`) can send a later run down another path, and the history then no longer matches the code.

The example has a second version of the refund that reads a feature flag on every run instead of recording it:

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

The flag changes while the run sleeps, so the second run calls `store-credit` where the history has `refund-card`. Instead of mixing two versions of the code, the run fails with a `*workflow.NondeterminismError`, marked permanent so it is not retried, which matches `workflow.ErrNondeterminism`:

```text
workflow: replay: call 0 is step store-credit, but the history has step refund-card
```

There are two kinds:

- **Another call at a recorded position**: another kind of call (a sleep where a step was) or another name. `Position` is the call's index from 0, `Recorded` is what the history has there and `Got` what the code called, each as kind and name (`"step refund-card"`).
- **Recorded calls left over**: the function returned before making every recorded call, so it took a shorter path than the recorded run. `Got` is empty: `the code returned before call 0, but the history has step x there`.

On an in-process driver the run-finished hook carries the step's error itself, so `errors.As` reads its fields; across a database or an engine only the text is kept, in `RunInfo.Error` and `RunFailedError.Err`:

<!-- snippet: examples/flow-replay/main.go#drift-error -->
```go
stepErr := <-ended // RunEvent.Err from the run-finished hook
var nd *workflow.NondeterminismError
if errors.As(stepErr, &nd) {
	fmt.Printf("  position %d: recorded %q, the code called %q; permanent %v\n",
		nd.Position, nd.Recorded, nd.Got, workflow.IsPermanent(stepErr))
}
```

To make the flag deterministic, record it: `useCredit, err := workflow.ReplayStep(rc, "read-flag", readFlag)`. Every run then sees the value the first run read.

### Deploy changed code

A run that is waiting keeps its history across a deploy, and the new code replays it. A change is safe for a run when the new code makes the same calls at every position that run has recorded; a change before a position some run has passed (a call added, removed, reordered or renamed, a branch condition changed) fails that run with the nondeterminism error. Changing what a recorded call does inside `fn` is safe, since `fn` does not run again; changing a `Sleep`'s duration is safe too, since the recorded wake time wins.

Since you rarely know how far each running run has got, version the workflow for such a change ([Deploy a new version while old workers run](../operations/deploy.md#deploy-a-new-version-while-old-workers-run)):

- Give the new definition a higher `Workflow.Version` and set `MinVersion` to the same number, so every run stays on the code that started it. Keep the old code deployed, under the old version, until its runs finish; then remove it.
- Or keep both paths in one function and branch on the run's version, which never changes during a run: `info, _ := workflow.StepInfoFrom(rc.Context())`, then `if info.Version >= 2 { ... }`.
- A run that already failed with the nondeterminism error is forgotten with its journal by default. With `workflow.JournalKeepFailed()` its history is kept, and a `RunRetrier` retry after the fix resumes from it instead of making every call again.

## Pitfalls

- **No goroutines inside the function.** Calls are recorded in the order they are made, so calls from several goroutines land in a different order on every run; a `ReplayCtx` is not safe for concurrent use. Run parallel work inside one `ReplayStep` (and record its combined result), or as [child runs](children.md) awaited one at a time with `ReplayChild`.
- **Map iteration order changes on every run.** Ranging over a map and making a call per entry records the entries in one order and replays them in another. Sort the keys first.
- **`time.Now`, `rand` and IDs need recording.** Use `rc.Now(name)` for the time and wrap a random value or a new ID in `ReplayStep`, so every run sees the first run's value.
- **Never swallow a wait's error.** A `Sleep` or `AwaitPromise` that is not over returns the suspension; return it. Code that ignores it is caught: every later call through the `ReplayCtx` returns the same suspension without running, and the step snoozes anyway. Plain code between those calls still runs, though, so a side effect made outside `ReplayStep` after an ignored wait happens before the wait is over.
- **Do not rename calls in a running deploy.** A rename is another name at a recorded position, which fails every run that passed it. Version the workflow instead, as above.
- **Long histories cost on every wake.** Each run of the function reads every recorded call back from the journal, one read per call: a loop of thousands of calls, or a promise looked at every few seconds for weeks, pays that on every wake. Keep loops inside a `ReplayStep`, raise `ReplayOptions.Poll` for waits measured in days, and end a run that would loop for ever with `workflow.ContinueAsNew`, carrying the state in the next run's payload ([Loop for ever as a chain of runs](promises.md#loop-for-ever-as-a-chain-of-runs)).
- **A promise wait wakes on its poll, not on the resolve.** `ResolvePromise` signals `workflow.PromiseEvent`, which wakes wait steps; a replayed `AwaitPromise` is a snooze and sees the promise at its next look, up to `Poll` later.
- **Give every `ReplayChild` call its own name.** Unlike the other calls, it spawns the child through `Spawn`, memoised under `"replay-child/"` plus the name, and a memo name used twice in one run of the body is refused: a loop that calls `ReplayChild(rc, "invoice", ...)` on each pass fails the run with `memo name "replay-child/invoice" is used twice`. Put the index in the name (`"invoice-" + strconv.Itoa(i)`).
- **`Step.Timeout` is per run of the function**, not for the whole wait; a sleep of three days does not need a three-day timeout.

## Replay, memoisation or steps

| | Replay (`NewReplay`) | Memoisation (`Memo`) | Workflow steps |
|---|---|---|---|
| Shape of the code | One function, top to bottom, with locals across waits | One step's handler; work inside it remembered by name | A graph of steps; data passed as step input and output |
| Waits | `Sleep`, `SleepUntil`, `AwaitPromise` inside the code, by snoozing | None inside the step beyond `Snooze` | Sleep steps, wait steps, child runs, promises |
| What runs again on a wake or retry | The whole function; recorded calls return their results | The step from the top; memoised calls return their results | The step that is next or failed |
| How calls are matched | By position and name, under the determinism rule | By name, unique within an attempt | Not applicable |
| Code changes under running runs | Nondeterminism error unless versioned | Safe while names and result types stay | The graph is fixed when the run starts; versions for rolling deploys |
| Visible in run listings and admin | One step | One step | Each step with its status, attempts and timing |
| Needs | Journal plugin; `CapSnooze` for waits | Journal plugin | Any driver |

Use replay for orchestration that reads best as sequential code and waits in the middle: approvals, cool-off periods, retries spread over days. Use `Memo` when a step only needs its finished work skipped on a retry. Use steps when the parts should be visible and retried on their own, run in parallel, or be operated from the admin UI.

## Try it

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

Each line inside the function was printed once, although the body ran four times: the first run stopped at the return window, the second at the approval, the third after the store credit at the one-day wait, and the fourth replayed every call and returned. The payout's body ran three times (spawning the child, recording its result, after pay day) and its child once.

## Related

- [Memoise work inside a step](memoisation.md), [Promises, updates and signal helpers](promises.md), [Sleep inside a workflow](sleep.md)
- [Snooze a step that is not ready](../jobs/retries.md#snooze-a-step-that-is-not-ready), [Errors that must not retry](../jobs/retries.md#errors-that-must-not-retry)
- [Deploy a new version while old workers run](../operations/deploy.md#deploy-a-new-version-while-old-workers-run)
- Example: [examples/flow-replay](../../examples/flow-replay/)
