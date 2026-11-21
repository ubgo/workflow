package main

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/recorder"
)

// snippet:store
// docStore is a recorder.Store of the app's own, shaped like a store over a
// document database: one document per run (the run with its steps and their
// events) under the run's key, and a secondary index from a delayed run's
// ScheduledID to that key. Every write is the same three moves in one
// transaction: load the document, apply the recorder's merge rule for the
// write (the recorder.Apply* functions), save it. The rules are shared, so
// this store merges a write exactly as the bundled stores do, and
// recordertest.RunStore holds it to their contract (see main_test.go).
//
// Here the "database" is a map behind a mutex; over a real one, update is
// a transaction that reads the document, runs fn and writes it back.
type docStore struct {
	mu      sync.Mutex
	docs    map[string]recorder.RunRecord
	bySched map[workflow.ScheduledID]string
}

var _ recorder.Store = (*docStore)(nil)

// newDocStore returns an empty store.
func newDocStore() *docStore {
	return &docStore{docs: map[string]recorder.RunRecord{}, bySched: map[workflow.ScheduledID]string{}}
}

// update is one write: it loads the run's document (joining a record kept
// under the ScheduledID into the one under the RunID when ref carries
// both, as recorder.MergeRecords says), hands it to fn, and saves it under
// its key, moving it off a ScheduledID-only key once it has a RunID.
func (s *docStore) update(ref workflow.RunRef, fn func(r *recorder.RunRecord, isNew bool)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var byRun, bySched *recorder.RunRecord
	if !ref.RunID.IsZero() {
		if d, ok := s.docs[recorder.KeyOf(workflow.RunRef{RunID: ref.RunID})]; ok {
			byRun = &d
		}
	}
	if k, ok := s.bySched[ref.ScheduledID]; ok && !ref.ScheduledID.IsZero() {
		if d, ok := s.docs[k]; ok {
			bySched = &d
		}
	}
	var r recorder.RunRecord
	isNew := false
	switch {
	case byRun != nil && bySched != nil && byRun.Key() != bySched.Key():
		delete(s.docs, bySched.Key())
		r = recorder.MergeRecords(*byRun, *bySched)
	case byRun != nil:
		r = *byRun
	case bySched != nil:
		r = *bySched
	default:
		r, isNew = recorder.NewRecord(ref), true
	}
	fn(&r, isNew)
	key := r.Key()
	if !r.Ref.ScheduledID.IsZero() {
		if old := recorder.KeyOf(workflow.RunRef{ScheduledID: r.Ref.ScheduledID}); old != key {
			delete(s.docs, old)
		}
		s.bySched[r.Ref.ScheduledID] = key
	}
	s.docs[key] = r.Clone() // the document owns its data, never the caller's slices
}

// snippet:end

// snippet:writes
// UpsertRun implements recorder.Store.
func (s *docStore) UpsertRun(_ context.Context, w recorder.RunWrite) error {
	if err := recorder.CheckRef("UpsertRun", w.Ref); err != nil {
		return err
	}
	s.update(w.Ref, func(r *recorder.RunRecord, isNew bool) { recorder.ApplyRunWrite(r, isNew, w) })
	return nil
}

// StartStep implements recorder.Store.
func (s *docStore) StartStep(_ context.Context, st recorder.StepStart) error {
	if err := recorder.CheckStep("StartStep", st.Ref, st.Step, st.Attempt); err != nil {
		return err
	}
	s.update(st.Ref, func(r *recorder.RunRecord, _ bool) {
		i := attempt(r.Steps, st.Step, st.Attempt)
		var existing *recorder.StepRecord
		if i >= 0 {
			existing = &r.Steps[i]
		}
		recorder.ApplyStepStart(r, recorder.StartsNewAttempt(existing), st)
		if i < 0 {
			r.Steps = append(r.Steps, recorder.NewStepRecord(st))
		} else {
			recorder.ResetStepRecord(&r.Steps[i], st) // a repeated start keeps the attempt's events
		}
		recorder.SortSteps(r.Steps)
	})
	return nil
}

// snippet:end

// EndStep implements recorder.Store.
func (s *docStore) EndStep(_ context.Context, e recorder.StepEnd) error {
	if err := recorder.CheckStep("EndStep", e.Ref, e.Step, e.Attempt); err != nil {
		return err
	}
	if !e.Status.Valid() || e.Status == recorder.StepRunning {
		return fmt.Errorf("%w: EndStep: status %q is not an end", workflow.ErrInvalid, e.Status)
	}
	s.update(e.Ref, func(r *recorder.RunRecord, _ bool) {
		recorder.ApplyStepEnd(r, e)
		i := attempt(r.Steps, e.Step, e.Attempt)
		if i < 0 { // the end raced ahead of the start
			r.Steps = append(r.Steps, recorder.StepRecord{})
			i = len(r.Steps) - 1
		}
		recorder.ApplyStepEndToStep(&r.Steps[i], e)
		recorder.SortSteps(r.Steps)
	})
	return nil
}

// AddEvent implements recorder.Store.
func (s *docStore) AddEvent(_ context.Context, e recorder.EventWrite) error {
	if err := recorder.CheckStep("AddEvent", e.Ref, e.Step, e.Attempt); err != nil {
		return err
	}
	s.update(e.Ref, func(r *recorder.RunRecord, _ bool) {
		recorder.ApplyEvent(r, e)
		i := attempt(r.Steps, e.Step, e.Attempt)
		if i < 0 { // the event raced ahead of its attempt's start: a placeholder the start fills
			r.Steps = append(r.Steps, recorder.StepRecord{Step: e.Step, Attempt: e.Attempt, Status: recorder.StepRunning})
			i = len(r.Steps) - 1
		}
		r.Steps[i].Events = append(r.Steps[i].Events, recorder.NewEventRecord(e, len(r.Steps[i].Events)+1))
		recorder.SortSteps(r.Steps)
	})
	return nil
}

// SetOutcome implements recorder.Store.
func (s *docStore) SetOutcome(_ context.Context, o recorder.OutcomeWrite) error {
	if err := recorder.CheckRef("SetOutcome", o.Ref); err != nil {
		return err
	}
	if !o.Outcome.Valid() {
		return fmt.Errorf("%w: SetOutcome: unknown outcome %q", workflow.ErrInvalid, o.Outcome)
	}
	s.update(o.Ref, func(r *recorder.RunRecord, _ bool) { recorder.ApplyOutcome(r, o) })
	return nil
}

// FinishRun implements recorder.Store.
func (s *docStore) FinishRun(_ context.Context, f recorder.Finish) error {
	if err := recorder.CheckRef("FinishRun", f.Ref); err != nil {
		return err
	}
	if !f.Status.IsTerminal() {
		return fmt.Errorf("%w: FinishRun: status %q is not terminal", workflow.ErrInvalid, f.Status)
	}
	s.update(f.Ref, func(r *recorder.RunRecord, _ bool) {
		var latest time.Time // the newest attempt start
		for _, st := range r.Steps {
			if st.StartedAt.After(latest) {
				latest = st.StartedAt
			}
		}
		recorder.ApplyFinish(r, f, latest) // ignored when a retry started after this end
	})
	return nil
}

// MarkChecked implements recorder.Store. A check of a run the store does
// not hold does nothing: the Reconciler checks only records it read.
func (s *docStore) MarkChecked(_ context.Context, c recorder.Check) error {
	if err := recorder.CheckRef("MarkChecked", c.Ref); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key, ok := s.keyOf(c.Ref)
	if !ok {
		return nil
	}
	r := s.docs[key]
	recorder.ApplyCheck(&r, c)
	s.docs[key] = r
	return nil
}

// Run implements recorder.Store.
func (s *docStore) Run(_ context.Context, ref workflow.RunRef) (recorder.RunRecord, error) {
	if err := recorder.CheckRef("Run", ref); err != nil {
		return recorder.RunRecord{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key, ok := s.keyOf(ref)
	if !ok {
		return recorder.RunRecord{}, fmt.Errorf("run %s: %w", recorder.KeyOf(ref), workflow.ErrNotFound)
	}
	return s.docs[key].Clone(), nil // a copy: the caller may modify it
}

// snippet:records
// Records implements recorder.Store: the filters are recorder.Query.Match,
// the order recorder.SortRecords, the cursor recorder.EncodeCursor, so a
// page reads the same as on the bundled stores. A database store turns the
// same filters into its own query.
func (s *docStore) Records(_ context.Context, q recorder.Query) (recorder.RecordPage, error) {
	if err := recorder.ValidateQuery(q); err != nil {
		return recorder.RecordPage{}, err
	}
	var after *recorder.Cursor
	if q.Cursor != "" {
		c, err := recorder.DecodeCursor(q.Cursor)
		if err != nil {
			return recorder.RecordPage{}, err
		}
		after = &c
	}
	s.mu.Lock()
	var page []recorder.RunRecord
	for _, r := range s.docs {
		if q.Match(r) && (after == nil || after.After(r)) {
			c := r.Clone()
			c.Steps = nil // listings carry no steps
			page = append(page, c)
		}
	}
	s.mu.Unlock()
	recorder.SortRecords(page)
	out := recorder.RecordPage{Runs: page}
	if limit := q.PageLimit(); len(page) > limit {
		out.Runs = page[:limit]
		last := out.Runs[limit-1]
		out.Next = recorder.EncodeCursor(recorder.Cursor{EnqueuedAt: last.EnqueuedAt, Key: last.Key()})
	}
	return out, nil
}

// snippet:end

// Runs implements recorder.Store and workflow.RunQuerier, so anything that
// lists runs (the admin API, the Reconciler) can read this store.
func (s *docStore) Runs(ctx context.Context, q workflow.RunQuery) (workflow.RunPage, error) {
	p, err := s.Records(ctx, recorder.Query{RunQuery: q})
	if err != nil {
		return workflow.RunPage{}, err
	}
	return recorder.InfoPage(p), nil
}

// Pending implements recorder.Store: unfinished runs, least recently
// checked first, for the Reconciler.
func (s *docStore) Pending(_ context.Context, limit int) ([]recorder.RunRecord, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("%w: Pending: limit %d is not positive", workflow.ErrInvalid, limit)
	}
	s.mu.Lock()
	var out []recorder.RunRecord
	for _, r := range s.docs {
		if !r.Status.IsTerminal() {
			c := r.Clone()
			c.Steps = nil
			out = append(out, c)
		}
	}
	s.mu.Unlock()
	recorder.SortPending(out)
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// keyOf finds the key of the document ref names. Callers hold mu.
func (s *docStore) keyOf(ref workflow.RunRef) (string, bool) {
	if !ref.RunID.IsZero() {
		if k := recorder.KeyOf(workflow.RunRef{RunID: ref.RunID}); hasKey(s.docs, k) {
			return k, true
		}
	}
	if k, ok := s.bySched[ref.ScheduledID]; ok && !ref.ScheduledID.IsZero() && hasKey(s.docs, k) {
		return k, true
	}
	return "", false
}

// hasKey reports whether docs holds k.
func hasKey(docs map[string]recorder.RunRecord, k string) bool {
	_, ok := docs[k]
	return ok
}

// attempt is the index of the (step, attempt) record in steps, or -1.
func attempt(steps []recorder.StepRecord, step string, n int) int {
	return slices.IndexFunc(steps, func(s recorder.StepRecord) bool { return s.Step == step && s.Attempt == n })
}
