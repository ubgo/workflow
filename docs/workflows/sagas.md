# Sagas

A trip books a car, a flight and a hotel with three providers. When the hotel has no rooms, the car and the flight must be cancelled again, newest first, and nothing may be cancelled twice or booked again on a retry. `workflow.Saga` does this inside one step: every action and every compensation is memoised through the journal, so a retried step resumes where it stopped.

## Before you start

- The client needs the Journal plugin (`workflow.Journal`), as [memoisation](memoisation.md) does; a durable `JournalStore` (sqlstore, Redis) makes the saga survive a crash. It runs on every driver.
- The example is [`examples/flow-patterns`](../../examples/flow-patterns).

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

- `NewSaga(name)` starts a saga for this attempt; build it in every call of the handler.
- `saga.Do(ctx, action, do, undo)` runs `do` once per run under `action` and remembers `undo`. A nil `undo` means nothing to undo.
- `saga.Abort(ctx, cause)` marks the saga aborted and runs the compensations of the finished actions, newest first, each once. When all ran it returns a `Permanent` error matching `ErrCompensated` with the first cause; the step is not retried. When a compensation fails it returns that error, not permanent, so the step is retried and the compensation resumes from where it stopped.
- After an abort, `Do` calls nothing: an action that had finished returns nil again and the first one that had not returns `ErrSagaAborted`, so the step's code reaches its `Abort` call with every finished action known.

## Across steps

For a saga whose actions are separate steps, tolerate the steps with [trigger rules](trigger-rules.md) and give the compensation its own step with `RuleOneFailed`, or put the compensation in `Workflow.OnFailure`.

## Pitfalls

- Call `Do` for the same actions in the same order on every attempt: the names are the memo keys.
- The guarantee is at least once, as with `Memo`: if the process dies after an action and before its result is saved, the action runs again. Give remote calls an idempotency key the provider honours.
