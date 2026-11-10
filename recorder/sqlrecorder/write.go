package sqlrecorder

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/recorder"
)

// stepColumns is the column list of <prefix>steps, in the order scanStep
// reads and saveStep writes.
var stepColumns = []string{
	"run_id", "step", "attempt", "status", "input", "output", "input_size", "output_size",
	"error", "stack", "node", "started_at", "finished_at",
}

// scanStep reads one steps row.
func scanStep(sc rowScanner) (recorder.StepRecord, error) {
	var (
		sr                       recorder.StepRecord
		runID, status            string
		attempt, inSize, outSize int64
		started, finished        int64
	)
	err := sc.Scan(&runID, &sr.Step, &attempt, &status, &sr.Input, &sr.Output, &inSize, &outSize,
		&sr.Error, &sr.Stack, &sr.Node, &started, &finished)
	sr.Attempt, sr.Status = int(attempt), recorder.StepStatus(status)
	sr.InputSize, sr.OutputSize = int(inSize), int(outSize)
	sr.StartedAt, sr.FinishedAt = fromNanos(started), fromNanos(finished)
	return sr, err
}

// selectSteps is "SELECT <step columns> FROM <steps>".
func (s *Store) selectSteps() string {
	cols := make([]string, len(stepColumns))
	for i, c := range stepColumns {
		cols[i] = s.d.quote(c)
	}
	return "SELECT " + strings.Join(cols, ", ") + " FROM " + s.table(tableSteps)
}

// loadStep reads one attempt's row; nil when there is none.
func (s *Store) loadStep(ctx context.Context, tx *sql.Tx, runID workflow.RunID, step string, attempt int) (*recorder.StepRecord, error) {
	q := s.d.quote
	a := &args{d: s.d}
	stmt := s.selectSteps() + " WHERE " + q("run_id") + " = " + a.add(string(runID)) + " AND " + q("step") + " = " + a.add(step) +
		" AND " + q("attempt") + " = " + a.add(int64(attempt))
	sr, err := scanStep(tx.QueryRowContext(ctx, stmt, a.vals...))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &sr, nil
}

// saveStep writes an attempt's row: an UPDATE when it existed, else an
// INSERT.
func (s *Store) saveStep(ctx context.Context, tx *sql.Tx, runID workflow.RunID, sr recorder.StepRecord, existed bool) error {
	q := s.d.quote
	vals := []any{
		string(runID), sr.Step, int64(sr.Attempt), string(sr.Status), sr.Input, sr.Output, int64(sr.InputSize),
		int64(sr.OutputSize), sr.Error, sr.Stack, sr.Node, nanos(sr.StartedAt), nanos(sr.FinishedAt),
	}
	a := &args{d: s.d}
	if existed {
		sets := make([]string, 0, len(stepColumns)-3)
		for i, c := range stepColumns[3:] {
			sets = append(sets, q(c)+" = "+a.add(vals[i+3]))
		}
		stmt := "UPDATE " + s.table(tableSteps) + " SET " + strings.Join(sets, ", ") + " WHERE " + q("run_id") + " = " + a.add(vals[0]) +
			" AND " + q("step") + " = " + a.add(vals[1]) + " AND " + q("attempt") + " = " + a.add(vals[2])
		_, err := tx.ExecContext(ctx, stmt, a.vals...)
		return err
	}
	ps := make([]string, len(vals))
	cols := make([]string, len(stepColumns))
	for i, v := range vals {
		ps[i] = a.add(v)
		cols[i] = q(stepColumns[i])
	}
	_, err := tx.ExecContext(ctx, "INSERT INTO "+s.table(tableSteps)+" ("+strings.Join(cols, ", ")+") VALUES ("+strings.Join(ps, ", ")+")", a.vals...)
	return err
}

// UpsertRun implements recorder.Store.
func (s *Store) UpsertRun(ctx context.Context, w recorder.RunWrite) error {
	if err := recorder.CheckRef("UpsertRun", w.Ref); err != nil {
		return err
	}
	return s.mutate(ctx, w.Ref, true, func(_ *sql.Tx, l *loaded) error {
		recorder.ApplyRunWrite(&l.rec, l.isNew, w)
		return nil
	})
}

// StartStep implements recorder.Store.
func (s *Store) StartStep(ctx context.Context, st recorder.StepStart) error {
	if err := recorder.CheckStep("StartStep", st.Ref, st.Step, st.Attempt); err != nil {
		return err
	}
	return s.mutate(ctx, st.Ref, true, func(tx *sql.Tx, l *loaded) error {
		existing, err := s.loadStep(ctx, tx, st.Ref.RunID, st.Step, st.Attempt)
		if err != nil {
			return err
		}
		recorder.ApplyStepStart(&l.rec, recorder.StartsNewAttempt(existing), st)
		if existing == nil {
			return s.saveStep(ctx, tx, st.Ref.RunID, recorder.NewStepRecord(st), false)
		}
		recorder.ResetStepRecord(existing, st)
		return s.saveStep(ctx, tx, st.Ref.RunID, *existing, true)
	})
}

// EndStep implements recorder.Store.
func (s *Store) EndStep(ctx context.Context, e recorder.StepEnd) error {
	if err := recorder.CheckStep("EndStep", e.Ref, e.Step, e.Attempt); err != nil {
		return err
	}
	if !e.Status.Valid() || e.Status == recorder.StepRunning {
		return fmt.Errorf("%w: sqlrecorder EndStep: status %q is not an end", workflow.ErrInvalid, e.Status)
	}
	return s.mutate(ctx, e.Ref, true, func(tx *sql.Tx, l *loaded) error {
		recorder.ApplyStepEnd(&l.rec, e)
		existing, err := s.loadStep(ctx, tx, e.Ref.RunID, e.Step, e.Attempt)
		if err != nil {
			return err
		}
		var sr recorder.StepRecord
		if existing != nil {
			sr = *existing
		}
		recorder.ApplyStepEndToStep(&sr, e)
		return s.saveStep(ctx, tx, e.Ref.RunID, sr, existing != nil)
	})
}

// AddEvent implements recorder.Store. The event's Seq is one more than the
// attempt's last, read under the run's row lock, so concurrent events of
// one run never share a Seq.
func (s *Store) AddEvent(ctx context.Context, e recorder.EventWrite) error {
	if err := recorder.CheckStep("AddEvent", e.Ref, e.Step, e.Attempt); err != nil {
		return err
	}
	q := s.d.quote
	return s.mutate(ctx, e.Ref, true, func(tx *sql.Tx, l *loaded) error {
		recorder.ApplyEvent(&l.rec, e)
		existing, err := s.loadStep(ctx, tx, e.Ref.RunID, e.Step, e.Attempt)
		if err != nil {
			return err
		}
		if existing == nil {
			// An event that raced ahead of its attempt's start: a
			// placeholder the start fills (recorder.StartsNewAttempt).
			ph := recorder.StepRecord{Step: e.Step, Attempt: e.Attempt, Status: recorder.StepRunning}
			if err := s.saveStep(ctx, tx, e.Ref.RunID, ph, false); err != nil {
				return err
			}
		}
		a := &args{d: s.d}
		where := q("run_id") + " = " + a.add(string(e.Ref.RunID)) + " AND " + q("step") + " = " + a.add(e.Step) +
			" AND " + q("attempt") + " = " + a.add(int64(e.Attempt))
		var last sql.NullInt64
		if err := tx.QueryRowContext(ctx, "SELECT MAX("+q("seq")+") FROM "+s.table(tableEvents)+" WHERE "+where, a.vals...).Scan(&last); err != nil {
			return err
		}
		attrs, err := encodeJSON(e.Attrs)
		if err != nil {
			return err
		}
		rec := recorder.NewEventRecord(e, int(last.Int64)+1)
		b := &args{d: s.d}
		stmt := "INSERT INTO " + s.table(tableEvents) + " (" + q("run_id") + ", " + q("step") + ", " + q("attempt") + ", " + q("seq") + ", " +
			q("message") + ", " + q("level") + ", " + q("attrs") + ", " + q("at") + ") VALUES (" +
			b.add(string(e.Ref.RunID)) + ", " + b.add(e.Step) + ", " + b.add(int64(e.Attempt)) + ", " + b.add(int64(rec.Seq)) + ", " +
			b.add(rec.Message) + ", " + b.add(int64(rec.Level)) + ", " + b.add(attrs) + ", " + b.add(nanos(rec.At)) + ")"
		_, err = tx.ExecContext(ctx, stmt, b.vals...)
		return err
	})
}

// SetOutcome implements recorder.Store.
func (s *Store) SetOutcome(ctx context.Context, o recorder.OutcomeWrite) error {
	if err := recorder.CheckRef("SetOutcome", o.Ref); err != nil {
		return err
	}
	if !o.Outcome.Valid() {
		return fmt.Errorf("%w: sqlrecorder SetOutcome: unknown outcome %q", workflow.ErrInvalid, o.Outcome)
	}
	return s.mutate(ctx, o.Ref, true, func(_ *sql.Tx, l *loaded) error {
		recorder.ApplyOutcome(&l.rec, o)
		return nil
	})
}

// FinishRun implements recorder.Store.
func (s *Store) FinishRun(ctx context.Context, f recorder.Finish) error {
	if err := recorder.CheckRef("FinishRun", f.Ref); err != nil {
		return err
	}
	if !f.Status.IsTerminal() {
		return fmt.Errorf("%w: sqlrecorder FinishRun: status %q is not terminal", workflow.ErrInvalid, f.Status)
	}
	q := s.d.quote
	return s.mutate(ctx, f.Ref, true, func(tx *sql.Tx, l *loaded) error {
		var latest sql.NullInt64
		if runID := l.rec.Ref.RunID; !runID.IsZero() || !f.Ref.RunID.IsZero() {
			if runID.IsZero() {
				runID = f.Ref.RunID
			}
			a := &args{d: s.d}
			stmt := "SELECT MAX(" + q("started_at") + ") FROM " + s.table(tableSteps) + " WHERE " + q("run_id") + " = " + a.add(string(runID))
			if err := tx.QueryRowContext(ctx, stmt, a.vals...).Scan(&latest); err != nil {
				return err
			}
		}
		recorder.ApplyFinish(&l.rec, f, fromNanos(latest.Int64))
		return nil
	})
}

// MarkChecked implements recorder.Store. A check of a run the store does
// not hold writes nothing.
func (s *Store) MarkChecked(ctx context.Context, c recorder.Check) error {
	if err := recorder.CheckRef("MarkChecked", c.Ref); err != nil {
		return err
	}
	return s.mutate(ctx, c.Ref, false, func(_ *sql.Tx, l *loaded) error {
		recorder.ApplyCheck(&l.rec, c)
		return nil
	})
}
