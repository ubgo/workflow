package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// DefaultMaxCheckpoint is the largest checkpoint a driver accepts unless
// configured otherwise (memory.WithMaxCheckpoint, sqlstore.WithMaxCheckpoint):
// 64 KiB. A checkpoint is a cursor, a page token or a few counters, not the
// work's data; anything bigger belongs in the app's own store.
const DefaultMaxCheckpoint = 64 << 10

// ErrCheckpointTooLarge matches a SaveCheckpoint refused for its size
// (CheckpointTooLargeError).
var ErrCheckpointTooLarge = errors.New("workflow: checkpoint too large")

// ErrAttemptLost matches a write a step attempt makes after it stopped
// owning its step: its lease expired and another worker took the step
// over, the run ended, or the attempt timed out and a later attempt runs.
// SaveCheckpoint, ClearCheckpoint and TxCompleter.CompleteInTx refuse such
// a write with it, so a stale worker never overwrites the state of the
// attempt that replaced it. The handler should return; its result is not
// recorded either.
var ErrAttemptLost = errors.New("workflow: the step attempt no longer owns its step")

// CheckpointTooLargeError is the refusal of a checkpoint over the driver's
// limit. It matches ErrCheckpointTooLarge and ErrInvalid.
type CheckpointTooLargeError struct {
	Step  string
	Size  int
	Limit int
	// Source names the setting that holds the limit
	// ("memory.WithMaxCheckpoint", "sqlstore.WithMaxCheckpoint").
	Source string
}

// Error names the sizes and the setting, so the message alone says what to
// change.
func (e *CheckpointTooLargeError) Error() string {
	return fmt.Sprintf("workflow: checkpoint of step %q is %d bytes, over the limit of %d (%s)", e.Step, e.Size, e.Limit, e.Source)
}

// Is matches ErrCheckpointTooLarge and ErrInvalid.
func (e *CheckpointTooLargeError) Is(target error) bool {
	return target == ErrCheckpointTooLarge || target == ErrInvalid
}

// CheckCheckpointSize refuses data over limit with a
// *CheckpointTooLargeError naming step and source; a limit of zero or less
// means DefaultMaxCheckpoint. Drivers call it from their Checkpointer.
func CheckCheckpointSize(step string, data []byte, limit int, source string) error {
	if limit <= 0 {
		limit = DefaultMaxCheckpoint
	}
	if len(data) > limit {
		return &CheckpointTooLargeError{Step: step, Size: len(data), Limit: limit, Source: source}
	}
	return nil
}

// Checkpointer keeps the checkpoint of the step attempt running on ctx,
// keyed by its run and step, so the next attempt of that step (after a
// retry, a snooze, a crash or a move to another worker) reads back what
// the last one saved. A driver with CapCheckpoint sets one on every step's
// ctx (WithCheckpointer). Every write is fenced: an attempt that no longer
// owns its step gets an error matching ErrAttemptLost and changes nothing.
// The driver deletes a run's checkpoints when the run ends.
type Checkpointer interface {
	// SaveCheckpoint replaces the step's checkpoint with data. A positive
	// ttl makes LoadCheckpoint stop returning it ttl after this save, on
	// the driver's clock; zero keeps it until the run ends.
	SaveCheckpoint(ctx context.Context, data []byte, ttl time.Duration) error
	// LoadCheckpoint returns the step's checkpoint; ok is false when none
	// was saved, it was cleared, or it expired.
	LoadCheckpoint(ctx context.Context) (data []byte, ok bool, err error)
	// ClearCheckpoint deletes the step's checkpoint; deleting none is not
	// an error.
	ClearCheckpoint(ctx context.Context) error
}

type checkpointKey struct{}

// WithCheckpointer returns a ctx carrying cp. Drivers call it for each step
// attempt.
func WithCheckpointer(ctx context.Context, cp Checkpointer) context.Context {
	return context.WithValue(ctx, checkpointKey{}, cp)
}

// checkpointer is the step's Checkpointer, or the UnsupportedError a call
// without one returns.
func checkpointer(ctx context.Context, call string) (Checkpointer, error) {
	cp, ok := ctx.Value(checkpointKey{}).(Checkpointer)
	if !ok || cp == nil {
		return nil, &UnsupportedError{Field: call, Missing: CapCheckpoint, Reason: "the driver keeps no checkpoints, or ctx is not a step's"}
	}
	return cp, nil
}

// CheckpointOption configures one SaveCheckpoint.
type CheckpointOption func(*checkpointOptions)

type checkpointOptions struct {
	ttl time.Duration
}

// CheckpointTTL makes the saved checkpoint expire d after the save: a
// later LoadCheckpoint reports none, so a cursor into an upstream that
// forgets its page tokens is not reused after it went stale. Zero (the
// default) keeps it until the run ends; a negative d is refused with
// ErrInvalid.
func CheckpointTTL(d time.Duration) CheckpointOption {
	return func(o *checkpointOptions) { o.ttl = d }
}

// SaveCheckpoint saves data as the checkpoint of the step running on ctx,
// replacing the previous one, so the next attempt of this step in this
// run resumes from it (LoadCheckpoint): a step paging through an API saves
// the page token after each page. On a durable driver the save is durable
// when it returns. It refuses data over the driver's limit
// (CheckpointTooLargeError), a save from an attempt that no longer owns
// its step (ErrAttemptLost), and, outside a step or on a driver without
// CapCheckpoint, returns an UnsupportedError naming CapCheckpoint: the
// caller asked for state to be kept, so dropping it silently would lose
// the resume point.
func SaveCheckpoint(ctx context.Context, data []byte, opts ...CheckpointOption) error {
	var o checkpointOptions
	for _, f := range opts {
		f(&o)
	}
	if o.ttl < 0 {
		return invalidf("checkpoint: CheckpointTTL is negative")
	}
	cp, err := checkpointer(ctx, "SaveCheckpoint")
	if err != nil {
		return err
	}
	return cp.SaveCheckpoint(ctx, data, o.ttl)
}

// LoadCheckpoint returns the checkpoint the step running on ctx last saved
// in this run, by this attempt or an earlier one; ok is false on the first
// attempt, after ClearCheckpoint, and once a CheckpointTTL ran out. It
// returns an UnsupportedError outside a step or on a driver without
// CapCheckpoint.
func LoadCheckpoint(ctx context.Context) (data []byte, ok bool, err error) {
	cp, err := checkpointer(ctx, "LoadCheckpoint")
	if err != nil {
		return nil, false, err
	}
	return cp.LoadCheckpoint(ctx)
}

// ClearCheckpoint deletes the checkpoint of the step running on ctx, for a
// step that wants its next attempt to start over. The driver deletes every
// checkpoint of a run when it ends, so a finished step need not call it.
// Fenced like SaveCheckpoint.
func ClearCheckpoint(ctx context.Context) error {
	cp, err := checkpointer(ctx, "ClearCheckpoint")
	if err != nil {
		return err
	}
	return cp.ClearCheckpoint(ctx)
}

// Checkpoint is a typed view of a step's checkpoint, encoded as JSON, the
// way Define gives a handler typed input and output. Declare it once next
// to the step:
//
//	var cursor = workflow.DefineCheckpoint[PageCursor]()
//
// and call cursor.Load and cursor.Save from the handler.
type Checkpoint[T any] struct{}

// DefineCheckpoint returns the typed checkpoint accessor for T.
func DefineCheckpoint[T any]() Checkpoint[T] { return Checkpoint[T]{} }

// Save encodes v as JSON and saves it (SaveCheckpoint). A v that does not
// encode is refused with ErrInvalid before anything is written.
func (Checkpoint[T]) Save(ctx context.Context, v T, opts ...CheckpointOption) error {
	b, err := json.Marshal(v)
	if err != nil {
		return invalidf("checkpoint: encode: %v", err)
	}
	return SaveCheckpoint(ctx, b, opts...)
}

// Load returns the saved checkpoint decoded into T; ok is false, with the
// zero T, when there is none (LoadCheckpoint). Stored bytes that do not
// decode into T (the type changed between deploys) are a Permanent error:
// retrying cannot make them decode; clear the checkpoint to start over.
func (Checkpoint[T]) Load(ctx context.Context) (v T, ok bool, err error) {
	b, ok, err := LoadCheckpoint(ctx)
	if err != nil || !ok {
		return v, false, err
	}
	if err := json.Unmarshal(b, &v); err != nil {
		return v, false, Permanent(fmt.Errorf("workflow: decode checkpoint: %w", err))
	}
	return v, true, nil
}

// Clear deletes the checkpoint (ClearCheckpoint).
func (Checkpoint[T]) Clear(ctx context.Context) error { return ClearCheckpoint(ctx) }
