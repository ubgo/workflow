# Sagas

A trip books a car, a flight and a hotel with three providers. When the hotel has no rooms, the car and the flight must be cancelled again, newest first, and nothing may be cancelled twice or booked again on a retry. [`workflow.Saga`](ds:block?id=saga-pk2fwf5w) does this inside one step: every action and every compensation is memoised through the journal, so a retried step resumes where it stopped. When the bookings should be steps of their own, so they run in parallel, show separately in run history and retry on their own, a compensation step with a trigger rule does the same across steps.

Use a saga when several remote changes must stand or fall together and no single transaction covers them: payments, reservations, provisioning across services. For changes inside one database, a transaction is simpler.

## Before you start

- The client needs the Journal plugin ([`workflow.Journal`](ds:block?id=journal-pr52wnjp)), as [memoisation](memoisation.md) does; a durable [`JournalStore`](ds:block?id=journalstore-tyc8j5tt) ([sqlstore](ds:block?id=sql-journalstore-tsrb65bx), [Redis](ds:block?id=redis-journalstore-wshcrgdq)) makes the saga survive a crash. It runs on every driver.
- Across steps, nothing more is needed: [trigger rules](trigger-rules.md) are ordinary step code.
- The example is [`examples/flow-patterns`](../../examples/flow-patterns). Run it with `go run ./flow-patterns` from `examples/`.

## Do, then abort

<!-- snippet: examples/flow-patterns/main.go#saga -->
```go
saga := workflow.NewSaga("trip")
if err := saga.Do(ctx, "car", act("book car"), act("cancel car")); err != nil {
	return nil, saga.Abort(ctx, err)
}
if err := saga.Do(ctx, "flight", act("book flight"), act("cancel flight")); err != nil {
	return nil, saga.Abort(ctx, err)
}
if err := saga.Do(ctx, "hotel", noRooms, act("cancel hotel")); err != nil {
	return nil, saga.Abort(ctx, err) // cancels the flight, then the car; Permanent
}
return []byte(`"booked"`), nil
```

- [`NewSaga(name)`](ds:block?id=newsaga-a6nhw8hx) starts a saga for this attempt; build it in every call of the handler.
- [`saga.Do(ctx, action, do, undo)`](ds:block?id=saga-do-h2g7bwvx) runs `do` once per run under `action` and remembers `undo`. A nil `undo` means nothing to undo.
- [`saga.Abort(ctx, cause)`](ds:block?id=saga-abort-uj8tk5xq) marks the saga aborted and runs the compensations of the finished actions, newest first, each once. When all ran it returns a `Permanent` error matching [`ErrCompensated`](ds:block?id=errcompensated-4d6vb255) with the first cause; the step is not retried. When a compensation fails it returns that error, not permanent, so the step is retried and the compensation resumes from where it stopped.
- After an abort, `Do` calls nothing: an action that had finished returns nil again and the first one that had not returns [`ErrSagaAborted`](ds:block?id=errsagaaborted-c3s8tp6k), so the step's code reaches its `Abort` call with every finished action known.

## Across steps

Make each booking a step, tolerate them so a failure is recorded instead of failing the run, and give the compensation its own step with [`RuleOneFailed`](ds:block?id=ruleonefailed-epgevkfz). Its input is an object keyed by parent, where a failed booking's output is `null`, so it knows which bookings went through and cancels those, newest first:

<!-- snippet: examples/flow-patterns/main.go#sagasteps -->
```go
order := []string{"car", "flight", "hotel"} // booking order: undo runs it backwards
wf := workflow.Workflow{Name: "booking", Steps: []workflow.Step{
	book("car", nil), book("flight", nil), book("hotel", errors.New("no rooms")),
	{Name: "confirm", After: order, Run: func(context.Context, []byte) ([]byte, error) {
		did.add("confirm")
		return []byte(`"trip booked"`), nil
	}},
	{Name: "undo", After: order, Run: func(ctx context.Context, in []byte) ([]byte, error) {
		var booked map[string]*string // a failed booking's output is null
		if err := json.Unmarshal(in, &booked); err != nil {
			return nil, workflow.Permanent(err)
		}
		for _, what := range slices.Backward(order) {
			if booked[what] != nil {
				did.add("cancel " + *booked[what])
			}
		}
		f := workflow.ParentFailures(ctx)[0]
		return nil, workflow.Permanent(fmt.Errorf("trip compensated: %s: %s", f.Step, f.Error))
	}},
}}
return workflow.WithRules(wf, workflow.RulesSpec{
	Tolerate: order, // a failed booking is recorded, not fatal
	Rules:    map[string]workflow.TriggerRule{"undo": workflow.RuleOneFailed},
})
```

`confirm` has no rule of its own, so below tolerated steps it gets [`RuleAllSucceeded`](ds:block?id=ruleallsucceeded-rb23vtac) ([`WithRules`](ds:block?id=withrules-q3mcee45) gives it to every unruled child of a tolerated step) and is skipped when any booking failed; `undo` runs only then. `undo` ends with a `Permanent` error so the run ends failed with the cause: without it the run would end succeeded, since a tolerated failure counts as handled. Each booking step retries by its own policy before its failure counts, and the cancellations are ordinary step work: put them in [`workflow.Memo`](ds:block?id=memo-wg47wmpf) (or a `Saga` inside `undo`) when a retried `undo` must not cancel twice. To compensate from one place for any failure of the run instead, use `Workflow.OnFailure`, which runs once with the failing step's input.

## Try it

```sh
cd examples
go run ./flow-patterns
```

Among its output, these lines are this page's: the saga inside one step booked the car and the flight, found no rooms, and cancelled the flight then the car; the saga across steps cancelled the two bookings that went through, newest first, and failed the run with the hotel's error.

```text
saga:
  failed: workflow: saga compensated: saga "trip": no rooms
    ran: book car, book flight, book hotel, cancel flight, cancel car
saga across steps:
  failed: trip compensated: hotel: no rooms
    ran: cancel flight-booking-1, cancel car-booking-1
```

## Pitfalls

- **Call `Do` for the same actions in the same order on every attempt:** the names are the memo keys ([`Saga.Do`](ds:block?id=saga-do-h2g7bwvx)). Build a new `NewSaga` in every call of the handler; a `Saga` value belongs to one attempt.
- **The guarantee is at least once**, as with [`Memo`](ds:block?id=memo-wg47wmpf): if the process dies after an action and before its result is saved, the action runs again. Give remote calls an idempotency key the provider honours.
- **A failing compensation retries the step.** [`Abort`](ds:block?id=saga-abort-uj8tk5xq) returns that error, not permanent, and the next attempt resumes the compensation where it stopped; a compensation that can never succeed retries until the step's policy gives up, so make compensations idempotent and safe to repeat.
- **An in-process journal does not survive a crash.** With [`NewMemoryJournal`](ds:block?id=newmemoryjournal-cvvh3zbe) a restarted worker forgets which actions ran; use a durable `JournalStore` (sqlstore, Redis) in production.
- **Across steps, a handled failure ends the run succeeded** ([`WithRules`](ds:block?id=withrules-q3mcee45) tolerates the step) unless the compensation step returns an error or a [Finally step](trigger-rules.md#tolerate-a-step-route-its-failure-always-clean-up) keeps it failed.

## Related

- [Memoise work inside a step](memoisation.md): the journal behind `Saga`
- [Trigger rules and exit handlers](trigger-rules.md): tolerated steps, `RuleOneFailed`, `Finally`
- [Choreographed sagas over events](../events/choreography.md): compensation between services that publish events
- [Idempotency keys](../jobs/idempotency.md), [Retries](../jobs/retries.md)
- Example: [examples/flow-patterns](../../examples/flow-patterns/)
