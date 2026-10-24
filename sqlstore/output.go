package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/sqlb/contrib/migrate"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/middleware"
)

// OutputStore keeps the output middleware.CaptureOutput captured from each
// step attempt in <prefix>outputs, so it lives with the run and every
// process reads the same history: it is a middleware.OutputSink for
// writes, and RunOutput reads a run's attempts back (an admin screen,
// through admin.OutputSourceFunc, which this package does not import).
//
//	store := c.OutputStore(sqlstore.OutputLimits{})
//	capture := middleware.CaptureOutput(store /* , options */)
//	// admin.WithOutputs(admin.OutputSourceFunc(func(ctx context.Context, id workflow.RunID) ([]admin.AttemptOutput, error) {
//	//	rows, err := store.RunOutput(ctx, id) // map each StoredOutput to admin.AttemptOutput
//	// }))
//
// Like UniqueStore it needs no running Client: NewOutputStore builds it
// over any *sql.DB (an app on Hatchet with a database). A run's rows are
// deleted with the run by a Client over the same prefix (Delete,
// retention); a store without a Client bounds its table with PruneBefore.
// Output may hold whatever a step logged, so treat the table like the
// payloads: it is read only by what the app wires to it.
type OutputStore struct {
	st  *store
	lim OutputLimits
}

var _ middleware.OutputSink = (*OutputStore)(nil)

// Defaults of OutputLimits, the same bounds as admin's in-memory store, so
// moving from it to this one keeps what an operator sees.
const (
	DefaultOutputPerRun = 20       // attempts kept per run, the newest
	DefaultOutputBytes  = 64 << 10 // bytes of text kept per attempt
)

// OutputLimits bounds what an OutputStore keeps. A zero or negative field
// takes its default.
type OutputLimits struct {
	// PerRun is how many attempts are kept per run, the newest; older ones
	// are deleted as new ones arrive. Default DefaultOutputPerRun.
	PerRun int
	// Bytes caps one attempt's text; longer text is cut on a character
	// boundary and marked Truncated. Default DefaultOutputBytes.
	Bytes int
}

func (l OutputLimits) withDefaults() OutputLimits {
	if l.PerRun <= 0 {
		l.PerRun = DefaultOutputPerRun
	}
	if l.Bytes <= 0 {
		l.Bytes = DefaultOutputBytes
	}
	return l
}

// StoredOutput is one attempt's captured output as RunOutput returns it.
// Its fields match admin.AttemptOutput one for one, same names, types and
// order, so an app converts with admin.AttemptOutput(o) inside an
// admin.OutputSourceFunc. Keep them identical: a change here breaks that
// conversion at compile time.
type StoredOutput struct {
	Step    string
	Attempt int
	// Started and Finished bound the attempt, on the capturing clock.
	Started, Finished time.Time
	// Text is middleware.FormatOutput's rendering of the attempt.
	Text string
	// Truncated is true when Text was cut to OutputLimits.Bytes.
	Truncated bool
	// Error is the attempt's error message; empty when it succeeded.
	Error string
}

// ErrOutputNoRun is returned by Deliver for output captured outside a
// driver (no StepInfo, or no RunID): there is no run to keep it under.
var ErrOutputNoRun = errors.New("sqlstore: output has no run (captured outside a driver)")

// OutputMigrationOwner is the ledger owner of OutputStore's migrations, a
// list of its own like UniqueStore's: NewOutputStore(...).Migrate creates
// only the outputs table, Client.Migrate applies it too.
const OutputMigrationOwner = "ubgo/workflow/sqlstore/output"

func (s *store) outputMigrations() []migrate.Migration {
	return []migrate.Migration{
		{Owner: s.owner(OutputMigrationOwner), Name: "001_output", Script: s.script(s.render(outputDefs(typesFor(s.engine))))},
	}
}

// outputDefs is OutputStore's table. Its rendered text is frozen once
// released, like ddl.
func outputDefs(t sqlTypes) []tableDef {
	return []tableDef{{stem: tblOutputs, lines: []string{
		// id orders a run's attempts as they were delivered.
		"id " + t.serial,
		"run_id " + t.id + " NOT NULL",
		"step " + t.name + " NOT NULL",
		"attempt " + t.int + " NOT NULL",
		// started_at and finished_at are Unix nanoseconds.
		"started_at " + t.int + " NOT NULL",
		"finished_at " + t.int + " NOT NULL",
		"body " + t.text + " NOT NULL",
		"truncated " + t.int + " NOT NULL",
		"err " + t.text + " NOT NULL",
	}, indexes: []index{{"run", "run_id, id"}, {"finished", "finished_at"}}}}
}

// NewOutputStore returns the output store over db for dialect d, bounded
// by lim. Of the Options, WithTablePrefix, WithSchema, WithTxAttempts and
// WithErrorClassifier apply; the others configure a Client and are
// ignored. Call Migrate before the first Deliver.
func NewOutputStore(db *sql.DB, d sqlb.Dialect, lim OutputLimits, opts ...Option) (*OutputStore, error) {
	c, err := New(db, d, opts...)
	if err != nil {
		return nil, err
	}
	return c.OutputStore(lim), nil
}

// OutputStore returns the output store in this client's database and
// prefix, bounded by lim. Client.Migrate creates its table, and the client
// deletes a run's output with the run.
func (c *Client) OutputStore(lim OutputLimits) *OutputStore {
	return &OutputStore{st: c.st, lim: lim.withDefaults()}
}

// Migrate creates the outputs table (its own migration list, under the
// same ledger and lock as Client.Migrate), and nothing else.
func (s *OutputStore) Migrate(ctx context.Context) error {
	return s.st.migrate(ctx, s.st.outputMigrations())
}

// Deliver implements middleware.OutputSink: it stores one attempt's output
// under its run and deletes the run's attempts past OutputLimits.PerRun,
// in one transaction.
func (s *OutputStore) Deliver(ctx context.Context, out middleware.Output) error {
	if !out.HasInfo || out.Info.RunID.IsZero() {
		return ErrOutputNoRun
	}
	var b strings.Builder
	if err := middleware.FormatOutput(&b, out); err != nil {
		return err
	}
	text, truncated := cutText(b.String(), s.lim.Bytes)
	msg := ""
	if out.Err != nil {
		msg = out.Err.Error()
	}
	run := string(out.Info.RunID)
	return s.st.tx(ctx, func(tx *sql.Tx) error {
		if _, err := s.st.exec(ctx, tx, `INSERT INTO {outputs} (run_id, step, attempt, started_at, finished_at, body, truncated, err)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, run, out.Info.Step, out.Info.Attempt, out.Started.UnixNano(), out.Finished.UnixNano(),
			text, boolInt(truncated), msg); err != nil {
			return fmt.Errorf("sqlstore: store output: %w", err)
		}
		var cut int64
		err := s.st.one(ctx, tx, "SELECT id FROM {outputs} WHERE run_id = ? ORDER BY id DESC LIMIT 1 OFFSET ?", []any{run, s.lim.PerRun}, &cut)
		if isNoRows(err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("sqlstore: read the output bound: %w", err)
		}
		_, err = s.st.exec(ctx, tx, "DELETE FROM {outputs} WHERE run_id = ? AND id <= ?", run, cut)
		return err
	})
}

// RunOutput returns the stored output of run's attempts, oldest first; an
// empty slice when it has none.
func (s *OutputStore) RunOutput(ctx context.Context, run workflow.RunID) ([]StoredOutput, error) {
	out := []StoredOutput{}
	var r StoredOutput
	var attempt, started, finished, truncated int64
	err := s.st.each(ctx, s.st.db, "SELECT step, attempt, started_at, finished_at, body, truncated, err FROM {outputs} WHERE run_id = ? ORDER BY id",
		[]any{string(run)}, func() error {
			r.Attempt, r.Started, r.Finished, r.Truncated = int(attempt), unixTime(started), unixTime(finished), truncated == 1
			out = append(out, r)
			return nil
		}, &r.Step, &attempt, &started, &finished, &r.Text, &truncated, &r.Error)
	if err != nil {
		return nil, fmt.Errorf("sqlstore: read output: %w", err)
	}
	return out, nil
}

// PruneBefore deletes the output of every attempt that finished before t
// and returns how many rows went: the retention of a store used without a
// Client, whose runs' deletes would otherwise take their output along.
func (s *OutputStore) PruneBefore(ctx context.Context, t time.Time) (int64, error) {
	return s.st.execN(ctx, s.st.db, "DELETE FROM {outputs} WHERE finished_at < ?", t.UnixNano())
}

// cutText keeps at most max bytes of s, ending on a character boundary so
// the stored text stays valid UTF-8 (Postgres and MySQL refuse anything
// else), and reports whether it cut.
func cutText(s string, max int) (string, bool) {
	if len(s) <= max {
		return s, false
	}
	n := max
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n], true
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
