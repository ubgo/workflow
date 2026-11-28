# Checkpoints

A step that does long resumable work (paging through an API, walking a large table) needs to remember how far it got, so the next attempt after a retry, a snooze, a crash or a move to another worker continues there instead of starting over. `workflow.SaveCheckpoint` saves a small piece of state under the run and the step; `workflow.LoadCheckpoint` returns it on the next attempt; the driver deletes it when the run ends.

## Before you start

- A driver with `workflow.CapCheckpoint`: memory (in memory, for one process and tests) and sqlstore (durable, migration `014_checkpoints_coalesce_schedule_idempotency`, table `<prefix>checkpoints`). On Redis and Hatchet, and outside a step, every checkpoint call returns an `UnsupportedError` naming `CapCheckpoint`: the step asked for state to be kept, so it is never dropped silently.
- The example is [`examples/step-checkpoint`](../../examples/step-checkpoint). Run it with `go run ./step-checkpoint` from `examples/`.

## Save and resume

Declare a typed checkpoint once, in the style of `Define`; it is stored as JSON:

<!-- snippet: examples/step-checkpoint/main.go#cursor -->
```go
// cursor is where the export stands: the next page to fetch.
type cursor struct {
	Page int `json:"page"`
}

// progress is the step's checkpoint, typed.
var progress = workflow.DefineCheckpoint[cursor]()
```

Load it at the top of the step and save the next position after each unit of work:

<!-- snippet: examples/step-checkpoint/main.go#step -->
```go
export := func(ctx context.Context, _ []byte) ([]byte, error) {
	at, ok, err := progress.Load(ctx)
	if err != nil {
		return nil, err
	}
	if !ok {
		at = cursor{Page: 1} // the first attempt starts at the beginning
	}
	info, _ := workflow.StepInfoFrom(ctx)
	fmt.Printf("attempt %d starts at page %d\n", info.Attempt, at.Page)
	for ; at.Page <= lastPage; at.Page++ {
		if at.Page == 3 && !failed {
			failed = true
			return nil, errUpstream // retried; the cursor says page 3
		}
		fmt.Println("fetched page", at.Page)
		if err := progress.Save(ctx, cursor{Page: at.Page + 1}); err != nil {
			return nil, err
		}
	}
	return nil, nil
}
```

The bytes API is `SaveCheckpoint(ctx, data, opts...)`, `LoadCheckpoint(ctx) (data, ok, err)` and `ClearCheckpoint(ctx)`; `Checkpoint[T]` wraps it with `Save`, `Load` and `Clear`. A checkpoint belongs to one step of one run: another step, and another run of the same workflow, never see it.

## Settings

| Setting | Default | Effect |
|---|---|---|
| `memory.WithMaxCheckpoint(n)`, `sqlstore.WithMaxCheckpoint(n)` | `workflow.DefaultMaxCheckpoint` (64 KiB) | A bigger save is refused with a `*CheckpointTooLargeError` (matches `ErrCheckpointTooLarge` and `ErrInvalid`) naming the step, the size, the limit and the setting; nothing is written |
| `workflow.CheckpointTTL(d)` on a save | none | `LoadCheckpoint` stops returning it `d` after the save, on the driver's clock, so a cursor into an upstream that forgets its page tokens is not reused after it went stale |

## A stale worker cannot overwrite it

Every save and clear is fenced. On sqlstore the row is written and the step's lease token checked in one transaction (a shared row lock on Postgres and MySQL, the writer lock on SQLite), so an attempt whose lease expired and was taken over, or whose run was cancelled or failed elsewhere, is refused with an error matching `workflow.ErrAttemptLost` (and `sqlstore.ErrLeaseLost`), and the attempt that replaced it keeps its state. On memory, a save after the run ended or after the handler returned is refused the same way. A handler whose ctx was cancelled but whose attempt still owns the step can still save.

## Pitfalls

- A checkpoint is a cursor, not the work's data: keep it small and keep results in your own store.
- A type change between deploys can make stored bytes fail to decode: `Checkpoint[T].Load` returns a `Permanent` error then; call `Clear` to start over.
- Checkpoints are not listed by `RunQuerier` or the admin API; they live only until the run ends.
- A run that gives its slots back under [`HoldWhileRunning`](concurrency.md#choose-how-long-a-run-holds-its-slot) keeps its checkpoints while it is parked: the attempt that runs once it takes its slots again reads them. A `CheckpointTTL` keeps counting while the run is parked.

## Related

- [Progress and heartbeats](progress.md): show how far a step has got, rather than resume from it.
- [Memoisation](../workflows/memoisation.md): `Memo` and the journal record results of finished pieces of work; a checkpoint is one mutable position.
