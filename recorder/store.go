package recorder

import (
	"context"
	"log/slog"
	"time"

	"github.com/ubgo/workflow"
)

// Store is the contract a run store keeps. The Recorder writes through it,
// the Reconciler reads and writes through it, and apps read run history
// from it (it is a workflow.RunQuerier, so anything that lists runs, an
// admin view included, can read the recorder instead of the driver).
//
// The rules every implementation keeps (recordertest.RunStore checks them):
//
//   - Every write is an upsert keyed by the run (RunRef): delivery from the
//     seams is at least once and in no guaranteed order across steps, so
//     the same write twice, or a step write before the run's own, must
//     land on one record. A write for a run the store has never seen
//     creates its record from what the write carries.
//   - A record is found by its RunID, else by its ScheduledID. A write
//     whose Ref carries both IDs joins a record kept under the ScheduledID
//     with the one under the RunID, so a delayed run ends as one record.
//   - Merging follows the Apply* functions of this package, so every store
//     merges a write the same way: ApplyRunWrite, ApplyStepStart,
//     ApplyStepEnd, ApplyEvent, ApplyOutcome, ApplyFinish, ApplyCheck,
//     MergeRecords.
//   - Reads return copies; a caller can modify what it got.
//   - Runs and Records order newest enqueue first, ties broken by Key
//     descending, and page with an opaque cursor (EncodeCursor is the one
//     the bundled stores use); they validate the query with ValidateQuery.
//   - Run returns workflow.ErrNotFound for a run the store does not hold.
//
// Methods may be called concurrently.
//
//ds:def id=rec-store-pgu7xhxs owner=@khanakia stability=stable
type Store interface {
	// UpsertRun records that a run was accepted (the enqueue seam) or found
	// on the client (the Reconciler's backfill).
	UpsertRun(ctx context.Context, w RunWrite) error

	// StartStep records that a step attempt started.
	StartStep(ctx context.Context, s StepStart) error

	// EndStep records that a step attempt ended.
	EndStep(ctx context.Context, e StepEnd) error

	// AddEvent appends a timeline event to a step attempt.
	AddEvent(ctx context.Context, e EventWrite) error

	// SetOutcome records the outcome a step set.
	SetOutcome(ctx context.Context, o OutcomeWrite) error

	// FinishRun records that a run reached a terminal status.
	FinishRun(ctx context.Context, f Finish) error

	// MarkChecked records a Reconciler check of an unfinished run.
	MarkChecked(ctx context.Context, c Check) error

	// Runs answers workflow.RunQuerier from the store's records.
	Runs(ctx context.Context, q workflow.RunQuery) (workflow.RunPage, error)

	// Records lists records with the recorder-only filters (Query); its
	// records carry no Steps.
	Records(ctx context.Context, q Query) (RecordPage, error)

	// Run returns one record with its steps and their events.
	Run(ctx context.Context, ref workflow.RunRef) (RunRecord, error)

	// Pending returns up to limit unfinished records (status not
	// terminal), the least recently checked first (never checked before
	// any checked), for the Reconciler. They carry no Steps.
	Pending(ctx context.Context, limit int) ([]RunRecord, error)
}

// Interface checks: a Store is a RunQuerier.
var _ workflow.RunQuerier = Store(nil)

// RunWrite is a run accepted by the client (or found on it). Fields are
// what the writer knows; the zero value of each means "unknown".
type RunWrite struct {
	// Ref is the run: only ScheduledID for a delayed run the enqueue seam
	// saw, both IDs when the Reconciler found it on the client.
	Ref workflow.RunRef

	Workflow string
	Queue    string
	Tenant   string
	Labels   map[string]string

	Source    workflow.Source
	SourceRef string

	// Status is the run's status when the writer saw it:
	// workflow.StatusScheduled or StatusQueued at enqueue, whatever the
	// client reported in a backfill. It sets the record's status only when
	// the record is new.
	Status workflow.Status

	// Payload is the input after redaction, nil when not kept;
	// PayloadSize is its size before that.
	Payload     []byte
	PayloadSize int

	Priority int

	// RunAt is the time a delayed run is due; zero to start now.
	RunAt time.Time

	// EnqueuedAt is when the run was accepted. The earliest known value
	// wins, because a record first seen at a step start carries that
	// start as its guess.
	EnqueuedAt time.Time

	// At is when the write was made, on the recorder's clock.
	At time.Time
}

// StepStart is a step attempt starting. It carries the run's own fields
// too, because the run may not have been seen enqueued (a schedule fire, a
// chained job, a child spawned on the driver's internal path).
type StepStart struct {
	// Ref is the run; a step always knows its RunID.
	Ref workflow.RunRef

	Workflow  string
	Queue     string
	Tenant    string
	Labels    map[string]string
	Source    workflow.Source
	SourceRef string

	Step    string
	Attempt int
	Node    string

	// Input is the step's input after redaction (nil when not kept);
	// InputSize is its size before that.
	Input     []byte
	InputSize int

	// At is when the attempt started.
	At time.Time
}

// StepEnd is a step attempt ending.
type StepEnd struct {
	Ref     workflow.RunRef
	Step    string
	Attempt int

	// Status is StepSucceeded, StepFailed or StepSnoozed.
	Status StepStatus

	// Output is the step's output after redaction (nil when not kept);
	// OutputSize is its size before that.
	Output     []byte
	OutputSize int

	// Error and Stack are the attempt's error (empty when it succeeded).
	Error string
	Stack string

	// Node ran the attempt.
	Node string

	// ErrorHistory bounds the run's error history (RunInfo.Errors) when
	// this end adds to it: the newest ErrorHistory entries are kept, zero
	// or less keeps none.
	ErrorHistory int

	// At is when the attempt ended.
	At time.Time
}

// EventWrite is one step event to append.
type EventWrite struct {
	Ref     workflow.RunRef
	Step    string
	Attempt int

	Message string
	Level   slog.Level
	Attrs   []workflow.EventAttr

	// At is when the event was recorded.
	At time.Time
}

// OutcomeWrite is the outcome a step set.
type OutcomeWrite struct {
	Ref     workflow.RunRef
	Outcome workflow.Outcome
	Note    string
	At      time.Time
}

// Finish is a run reaching a terminal status (the run-finished seam, or the
// Reconciler finding the client's answer).
type Finish struct {
	// Ref carries both IDs when the run was delayed and the driver knows
	// both (workflow.RunEvent.Ref does).
	Ref workflow.RunRef

	Workflow  string
	Tenant    string
	Labels    map[string]string
	Source    workflow.Source
	SourceRef string

	// Status is terminal.
	Status workflow.Status

	// Error is why the run failed or was cancelled; FailedStep the step
	// whose failure ended it.
	Error      string
	FailedStep string

	// Output is the run's result after redaction when the writer read it,
	// else nil; OutputSize is its size before that.
	Output     []byte
	OutputSize int

	// FinishedAt is when the run ended, on the driver's clock.
	FinishedAt time.Time

	// At is when the write was made.
	At time.Time
}

// Check is the Reconciler's verdict on an unfinished record.
type Check struct {
	Ref workflow.RunRef

	// Status is the non-terminal status the client reported, or "" to
	// leave the record's status (the client did not know the run).
	Status workflow.Status

	// Stale sets or clears the record's Stale flag.
	Stale bool

	// At is when the check was made; it becomes the record's CheckedAt.
	At time.Time
}

// RecordPage is one page of Records.
//
//ds:def id=rec-recordpage-8ehzyqwp owner=@khanakia stability=stable
type RecordPage struct {
	Runs []RunRecord
	Next string
}

// Pruner is the optional retention of a Store: MemoryStore and
// sqlrecorder.Store implement it, configured with a Retention.
//
//ds:def id=rec-pruner-tz43nn4x owner=@khanakia stability=stable
type Pruner interface {
	Prune(ctx context.Context) (PruneResult, error)
}
