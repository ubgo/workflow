package events

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/ubgo/workflow"
)

// LineageKind says which side of a dataset version a lineage record is
// about.
type LineageKind string

// The lineage kinds.
const (
	// LineageProduced records that a run (or other code) wrote a
	// dataset version: Datasets.Updated records one.
	LineageProduced LineageKind = "produced"

	// LineageConsumed records that a DatasetTrigger started a run for a
	// dataset version. It is the trigger's memory of what it already
	// used, as well as lineage.
	LineageConsumed LineageKind = "consumed"
)

// Lineage is one record of asset lineage (ET-05): which run and step
// produced a dataset version, or which run a trigger started for it, so
// "where did this table's 09:00 version come from" and "what did this
// run read" have answers after the fact.
type Lineage struct {
	// Kind is produced or consumed.
	Kind LineageKind

	// Dataset is the dataset's name ("warehouse.orders", an S3 prefix).
	Dataset string

	// Version is the dataset version: opaque to the library, compared
	// only for equality, so make each update's version unique (a
	// partition, a timestamp, a content hash).
	Version string

	// Meta is what the producer reported with the version (row count,
	// location); nil for a consumed record.
	Meta map[string]string

	// Run, Workflow and Step name the run involved: for a produced
	// record the step that called Updated (all empty when it was called
	// outside a step), for a consumed record the run the trigger started
	// (Step empty; Run empty when the driver hands out only a
	// ScheduledID for a delayed run).
	Run      workflow.RunID
	Workflow string
	Step     string

	// Trigger is the DatasetTrigger that consumed the version; empty
	// for a produced record.
	Trigger string

	// At is when the record was made, on the Datasets' clock.
	At time.Time
}

// same reports whether l and o are the same fact (every identifying
// field equal; Meta and At are not part of it), so a retried step that
// records its output again changes nothing.
func (l Lineage) same(o Lineage) bool {
	return l.Kind == o.Kind && l.Dataset == o.Dataset && l.Version == o.Version &&
		l.Run == o.Run && l.Workflow == o.Workflow && l.Step == o.Step && l.Trigger == o.Trigger
}

// LineageQuery filters lineage records for LineageStore.Find. Every field
// that is set must match; the zero value matches every record.
type LineageQuery struct {
	// Kind is produced or consumed; empty matches both.
	Kind LineageKind

	// Dataset is the dataset name; empty matches any.
	Dataset string

	// Run is the run; empty matches any.
	Run workflow.RunID

	// Trigger is the consuming trigger; empty matches any.
	Trigger string

	// Limit is the most records to return; zero means all.
	Limit int
}

// match reports whether l passes q's filters.
func (q LineageQuery) match(l Lineage) bool {
	return (q.Kind == "" || l.Kind == q.Kind) && (q.Dataset == "" || l.Dataset == q.Dataset) &&
		(q.Run == "" || l.Run == q.Run) && (q.Trigger == "" || l.Trigger == q.Trigger)
}

// LineageStore keeps lineage records. It is where Datasets writes every
// update and where its triggers read which versions they already used,
// so a store shared by every process that calls Updated (a table in the
// app's database) makes triggers and lineage fleet-wide; MemoryLineage,
// the default, is in-process. Implementations must be safe for
// concurrent use.
type LineageStore interface {
	// Record saves l. A record equal to one already saved in Kind,
	// Dataset, Version, Run, Workflow, Step and Trigger is ignored
	// (the first one's Meta and At are kept), so recording is
	// idempotent and a retried step never moves a version forward.
	Record(ctx context.Context, l Lineage) error

	// Find returns the records matching q, newest first: by At, latest
	// first, ties in reverse order of recording.
	Find(ctx context.Context, q LineageQuery) ([]Lineage, error)
}

// Producers returns who produced the versions of dataset, newest first.
func Producers(ctx context.Context, store LineageStore, dataset string) ([]Lineage, error) {
	return store.Find(ctx, LineageQuery{Kind: LineageProduced, Dataset: dataset})
}

// Consumers returns the runs triggers started for the versions of
// dataset, newest first.
func Consumers(ctx context.Context, store LineageStore, dataset string) ([]Lineage, error) {
	return store.Find(ctx, LineageQuery{Kind: LineageConsumed, Dataset: dataset})
}

// Outputs returns the dataset versions run produced, newest first.
func Outputs(ctx context.Context, store LineageStore, run workflow.RunID) ([]Lineage, error) {
	if run.IsZero() {
		return nil, fmt.Errorf("%w: events: lineage outputs of an empty run ID", workflow.ErrInvalid)
	}
	return store.Find(ctx, LineageQuery{Kind: LineageProduced, Run: run})
}

// Inputs returns the dataset versions a trigger started run for, newest
// first.
func Inputs(ctx context.Context, store LineageStore, run workflow.RunID) ([]Lineage, error) {
	if run.IsZero() {
		return nil, fmt.Errorf("%w: events: lineage inputs of an empty run ID", workflow.ErrInvalid)
	}
	return store.Find(ctx, LineageQuery{Kind: LineageConsumed, Run: run})
}

// MemoryLineage is an in-process LineageStore, the Datasets default: right
// for tests and a single process, lost on restart, and it grows by one
// record per new version and per trigger run. Processes that each keep
// their own cannot see each other's updates, so give every process one
// shared store instead.
type MemoryLineage struct {
	mu   sync.Mutex
	recs []Lineage
}

// NewMemoryLineage returns an empty MemoryLineage.
func NewMemoryLineage() *MemoryLineage { return &MemoryLineage{} }

var _ LineageStore = (*MemoryLineage)(nil)

// Record saves a copy of l unless the same fact is already saved.
func (m *MemoryLineage) Record(ctx context.Context, l Lineage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if slices.ContainsFunc(m.recs, l.same) {
		return nil
	}
	l.Meta = maps.Clone(l.Meta)
	m.recs = append(m.recs, l)
	return nil
}

// Find returns copies of the matching records, newest first.
func (m *MemoryLineage) Find(ctx context.Context, q LineageQuery) ([]Lineage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if q.Limit < 0 {
		return nil, fmt.Errorf("%w: events: lineage query Limit %d is negative", workflow.ErrInvalid, q.Limit)
	}
	m.mu.Lock()
	var out []Lineage
	for i := len(m.recs) - 1; i >= 0; i-- { // reverse order of recording
		if q.match(m.recs[i]) {
			l := m.recs[i]
			l.Meta = maps.Clone(l.Meta)
			out = append(out, l)
		}
	}
	m.mu.Unlock()
	slices.SortStableFunc(out, func(a, b Lineage) int { return b.At.Compare(a.At) })
	if q.Limit > 0 && len(out) > q.Limit {
		out = out[:q.Limit]
	}
	return out, nil
}
