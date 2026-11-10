package recorder

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// MemoryStore is the in-process Store: records live in a map guarded by a
// mutex, so it is for tests, single-process apps and as the reference the
// store conformance suite (recordertest.RunStore) is written against. It
// keeps nothing across a restart. It implements Pruner.
type MemoryStore struct {
	clk       clock.Clock
	retention Retention

	mu sync.Mutex
	// runs is every record by Key (KeyOf). A record holds its steps; a
	// step holds its events.
	runs map[string]*RunRecord
	// bySched maps a ScheduledID to the Key its record lives under, so a
	// record moved under its RunID is still found by the ScheduledID.
	bySched map[workflow.ScheduledID]string
}

var (
	_ Store  = (*MemoryStore)(nil)
	_ Pruner = (*MemoryStore)(nil)
)

// MemoryOption configures a MemoryStore.
type MemoryOption func(*MemoryStore)

// WithMemoryClock sets the clock Prune measures ages with; clock.Real by
// default.
func WithMemoryClock(c clock.Clock) MemoryOption {
	return func(s *MemoryStore) {
		if c != nil {
			s.clk = c
		}
	}
}

// WithMemoryRetention sets the retention Prune applies (see Retention); none
// by default, so Prune does nothing.
func WithMemoryRetention(r Retention) MemoryOption {
	return func(s *MemoryStore) { s.retention = r }
}

// NewMemoryStore returns an empty MemoryStore. It fails only on an invalid
// retention (ValidateRetention).
func NewMemoryStore(opts ...MemoryOption) (*MemoryStore, error) {
	s := &MemoryStore{clk: clock.Real{}, runs: map[string]*RunRecord{}, bySched: map[workflow.ScheduledID]string{}}
	for _, o := range opts {
		o(s)
	}
	if err := ValidateRetention(s.retention); err != nil {
		return nil, err
	}
	return s, nil
}

// find returns the record ref points at, or nil. Callers hold mu.
func (s *MemoryStore) find(ref workflow.RunRef) *RunRecord {
	if !ref.RunID.IsZero() {
		if r, ok := s.runs[KeyOf(workflow.RunRef{RunID: ref.RunID})]; ok {
			return r
		}
	}
	if !ref.ScheduledID.IsZero() {
		if k, ok := s.bySched[ref.ScheduledID]; ok {
			return s.runs[k]
		}
	}
	return nil
}

// load returns the record for ref, joining a record kept under the
// ScheduledID into the one under the RunID when ref carries both, and
// creating an empty one when none exists. isNew reports the creation.
// Callers hold mu and save the record back with save.
func (s *MemoryStore) load(ref workflow.RunRef) (r *RunRecord, isNew bool) {
	var byRun, bySched *RunRecord
	if !ref.RunID.IsZero() {
		byRun = s.runs[KeyOf(workflow.RunRef{RunID: ref.RunID})]
	}
	if !ref.ScheduledID.IsZero() {
		if k, ok := s.bySched[ref.ScheduledID]; ok {
			bySched = s.runs[k]
		}
	}
	switch {
	case byRun != nil && bySched != nil && byRun != bySched:
		merged := MergeRecords(*byRun, *bySched)
		delete(s.runs, bySched.Key())
		return &merged, false
	case byRun != nil:
		return byRun, false
	case bySched != nil:
		return bySched, false
	}
	rec := NewRecord(ref)
	return &rec, true
}

// save stores r under its Key, removing an entry it had under its
// ScheduledID-only key when it now has a RunID. Callers hold mu.
func (s *MemoryStore) save(r *RunRecord) {
	key := r.Key()
	if !r.Ref.ScheduledID.IsZero() {
		old := KeyOf(workflow.RunRef{ScheduledID: r.Ref.ScheduledID})
		if old != key {
			delete(s.runs, old)
		}
		s.bySched[r.Ref.ScheduledID] = key
	}
	s.runs[key] = r
}

// UpsertRun implements Store.
func (s *MemoryStore) UpsertRun(_ context.Context, w RunWrite) error {
	if err := CheckRef("UpsertRun", w.Ref); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, isNew := s.load(w.Ref)
	ApplyRunWrite(r, isNew, w)
	s.save(r)
	return nil
}

// StartStep implements Store.
func (s *MemoryStore) StartStep(_ context.Context, st StepStart) error {
	if err := CheckStep("StartStep", st.Ref, st.Step, st.Attempt); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, _ := s.load(st.Ref)
	i := stepIndex(r.Steps, st.Step, st.Attempt)
	var existing *StepRecord
	if i >= 0 {
		existing = &r.Steps[i]
	}
	ApplyStepStart(r, StartsNewAttempt(existing), st)
	if i < 0 {
		r.Steps = append(r.Steps, NewStepRecord(st))
		SortSteps(r.Steps)
	} else {
		ResetStepRecord(&r.Steps[i], st)
	}
	s.save(r)
	return nil
}

// EndStep implements Store.
func (s *MemoryStore) EndStep(_ context.Context, e StepEnd) error {
	if err := CheckStep("EndStep", e.Ref, e.Step, e.Attempt); err != nil {
		return err
	}
	if !e.Status.Valid() || e.Status == StepRunning {
		return fmt.Errorf("%w: recorder EndStep: status %q is not an end", workflow.ErrInvalid, e.Status)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, _ := s.load(e.Ref)
	ApplyStepEnd(r, e)
	i := stepIndex(r.Steps, e.Step, e.Attempt)
	if i < 0 {
		r.Steps = append(r.Steps, StepRecord{})
		i = len(r.Steps) - 1
	}
	ApplyStepEndToStep(&r.Steps[i], e)
	SortSteps(r.Steps)
	s.save(r)
	return nil
}

// AddEvent implements Store.
func (s *MemoryStore) AddEvent(_ context.Context, e EventWrite) error {
	if err := CheckStep("AddEvent", e.Ref, e.Step, e.Attempt); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, _ := s.load(e.Ref)
	ApplyEvent(r, e)
	i := stepIndex(r.Steps, e.Step, e.Attempt)
	if i < 0 {
		// An event for an attempt not yet recorded (writes raced): hold it
		// on a placeholder, StartedAt zero, that the attempt's start fills
		// (StartsNewAttempt).
		r.Steps = append(r.Steps, StepRecord{Step: e.Step, Attempt: e.Attempt, Status: StepRunning})
		i = len(r.Steps) - 1
	}
	sr := &r.Steps[i]
	sr.Events = append(sr.Events, NewEventRecord(e, len(sr.Events)+1))
	SortSteps(r.Steps)
	s.save(r)
	return nil
}

// SetOutcome implements Store.
func (s *MemoryStore) SetOutcome(_ context.Context, o OutcomeWrite) error {
	if err := CheckRef("SetOutcome", o.Ref); err != nil {
		return err
	}
	if !o.Outcome.Valid() {
		return fmt.Errorf("%w: recorder SetOutcome: unknown outcome %q", workflow.ErrInvalid, o.Outcome)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, _ := s.load(o.Ref)
	ApplyOutcome(r, o)
	s.save(r)
	return nil
}

// FinishRun implements Store.
func (s *MemoryStore) FinishRun(_ context.Context, f Finish) error {
	if err := CheckRef("FinishRun", f.Ref); err != nil {
		return err
	}
	if !f.Status.IsTerminal() {
		return fmt.Errorf("%w: recorder FinishRun: status %q is not terminal", workflow.ErrInvalid, f.Status)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, _ := s.load(f.Ref)
	var latest time.Time
	for _, st := range r.Steps {
		if st.StartedAt.After(latest) {
			latest = st.StartedAt
		}
	}
	ApplyFinish(r, f, latest)
	s.save(r)
	return nil
}

// MarkChecked implements Store. A check of a run the store does not hold
// does nothing: the Reconciler only checks records it read.
func (s *MemoryStore) MarkChecked(_ context.Context, c Check) error {
	if err := CheckRef("MarkChecked", c.Ref); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.find(c.Ref)
	if r == nil {
		return nil
	}
	ApplyCheck(r, c)
	return nil
}

// Runs implements Store and workflow.RunQuerier.
func (s *MemoryStore) Runs(ctx context.Context, q workflow.RunQuery) (workflow.RunPage, error) {
	p, err := s.Records(ctx, Query{RunQuery: q})
	if err != nil {
		return workflow.RunPage{}, err
	}
	return InfoPage(p), nil
}

// Records implements Store.
func (s *MemoryStore) Records(_ context.Context, q Query) (RecordPage, error) {
	if err := ValidateQuery(q); err != nil {
		return RecordPage{}, err
	}
	var cur *Cursor
	if q.Cursor != "" {
		c, err := DecodeCursor(q.Cursor)
		if err != nil {
			return RecordPage{}, err
		}
		cur = &c
	}
	s.mu.Lock()
	all := make([]RunRecord, 0, len(s.runs))
	for _, r := range s.runs {
		if q.Match(*r) && (cur == nil || cur.After(*r)) {
			c := r.Clone()
			c.Steps = nil
			all = append(all, c)
		}
	}
	s.mu.Unlock()
	SortRecords(all)
	limit := q.PageLimit()
	page := RecordPage{Runs: all}
	if len(all) > limit {
		page.Runs = all[:limit]
		last := page.Runs[limit-1]
		page.Next = EncodeCursor(Cursor{EnqueuedAt: last.EnqueuedAt, Key: last.Key()})
	}
	return page, nil
}

// Run implements Store.
func (s *MemoryStore) Run(_ context.Context, ref workflow.RunRef) (RunRecord, error) {
	if err := CheckRef("Run", ref); err != nil {
		return RunRecord{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.find(ref)
	if r == nil {
		return RunRecord{}, fmt.Errorf("recorder: run %s: %w", KeyOf(ref), workflow.ErrNotFound)
	}
	return r.Clone(), nil
}

// Pending implements Store.
func (s *MemoryStore) Pending(_ context.Context, limit int) ([]RunRecord, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("%w: recorder Pending: limit %d is not positive", workflow.ErrInvalid, limit)
	}
	s.mu.Lock()
	var out []RunRecord
	for _, r := range s.runs {
		if !r.Status.IsTerminal() {
			c := r.Clone()
			c.Steps = nil
			out = append(out, c)
		}
	}
	s.mu.Unlock()
	SortPending(out)
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// Prune implements Pruner: stage one drops the steps of runs finished
// longer ago than the rule's Steps age, stage two archives and deletes runs
// finished longer ago than its Runs age.
func (s *MemoryStore) Prune(ctx context.Context) (PruneResult, error) {
	var res PruneResult
	if s.retention.IsZero() {
		return res, nil
	}
	now := s.clk.Now()
	s.mu.Lock()
	var doomed []RunRecord
	for _, r := range s.runs {
		if !r.Status.IsTerminal() || r.FinishedAt.IsZero() {
			continue
		}
		rule := s.retention.Rule(r.Workflow)
		age := now.Sub(r.FinishedAt)
		if rule.Runs > 0 && age >= rule.Runs {
			doomed = append(doomed, r.Clone())
			continue
		}
		if rule.Steps > 0 && age >= rule.Steps && r.Steps != nil {
			r.Steps = nil
			res.StepsCleared++
		}
	}
	s.mu.Unlock()
	SortRecords(doomed)
	batch := s.retention.BatchSize()
	for start := 0; start < len(doomed); start += batch {
		chunk := doomed[start:min(start+batch, len(doomed))]
		if s.retention.Archive != nil {
			if err := s.retention.Archive(ctx, chunk); err != nil {
				return res, fmt.Errorf("recorder: archive: %w", err)
			}
			res.Archived += len(chunk)
		}
		s.mu.Lock()
		for _, r := range chunk {
			delete(s.runs, r.Key())
			if !r.Ref.ScheduledID.IsZero() {
				delete(s.bySched, r.Ref.ScheduledID)
			}
			res.RunsDeleted++
		}
		s.mu.Unlock()
	}
	return res, nil
}

// SortPending orders records the way Pending returns them: never checked
// first, then least recently checked, ties by Key.
func SortPending(rs []RunRecord) {
	slices.SortFunc(rs, func(a, b RunRecord) int {
		if c := a.CheckedAt.Compare(b.CheckedAt); c != 0 {
			return c
		}
		switch {
		case a.Key() < b.Key():
			return -1
		case a.Key() > b.Key():
			return 1
		}
		return 0
	})
}

// stepIndex is the index of the (step, attempt) record in steps, or -1.
func stepIndex(steps []StepRecord, step string, attempt int) int {
	for i, s := range steps {
		if s.Step == step && s.Attempt == attempt {
			return i
		}
	}
	return -1
}

// CheckRef refuses, with workflow.ErrInvalid, a write that names no run.
// The bundled stores call it first; an app's own store should too
// (recordertest.RunStore expects the refusal).
func CheckRef(op string, ref workflow.RunRef) error {
	if ref.IsZero() {
		return fmt.Errorf("%w: recorder %s: empty RunRef", workflow.ErrInvalid, op)
	}
	return nil
}

// CheckStep refuses, with workflow.ErrInvalid, a step write without a
// RunID, a step name or an attempt from 1.
func CheckStep(op string, ref workflow.RunRef, step string, attempt int) error {
	if ref.RunID.IsZero() {
		return fmt.Errorf("%w: recorder %s: a step write needs the RunID", workflow.ErrInvalid, op)
	}
	if step == "" || attempt < 1 {
		return fmt.Errorf("%w: recorder %s: step %q attempt %d", workflow.ErrInvalid, op, step, attempt)
	}
	return nil
}
