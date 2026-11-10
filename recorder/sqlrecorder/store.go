package sqlrecorder

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/recorder"
)

// Defaults of a Store.
const (
	// DefaultTablePrefix starts every table name: wf_rec_runs,
	// wf_rec_steps, wf_rec_events, wf_rec_labels and wf_rec_schema.
	DefaultTablePrefix = "wf_rec_"

	// DefaultTxAttempts is how many times a write transaction runs before
	// a contention error (see DefaultRetryable) is returned.
	DefaultTxAttempts = 8

	// maxPrefix bounds the table prefix, so every table and index name
	// stays under the engines' 63-character identifier limit.
	maxPrefix = 32
)

// prefixPattern is what a table prefix may hold: it is pasted into SQL, so
// only lower-case letters, digits and underscores, starting with a letter.
var prefixPattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// Store is a recorder.Store on a SQL database (Postgres, MySQL or SQLite)
// through database/sql. Each run is one row in <prefix>runs, each step
// attempt one row in <prefix>steps, each event one row in <prefix>events,
// and the run's labels one row per key in <prefix>labels (so label filters
// are indexed lookups, not JSON scans). A busy run therefore never rewrites
// one large row per event.
//
// Every write is one transaction that locks the run's row first, so the
// writes of one run are serialized across processes; a contention error
// (two first writes of a run racing, a deadlock, a busy SQLite file) is
// retried (WithTxAttempts, WithRetryable). Times are stored as Unix
// nanoseconds in UTC, zero for an unset time, so ordering and range
// filters behave the same on every engine.
//
// It implements recorder.Store, recorder.Pruner and workflow.RunQuerier.
type Store struct {
	db        *sql.DB
	d         Dialect
	prefix    string
	clk       clock.Clock
	retention recorder.Retention
	attempts  int
	retryable func(error) bool

	// sqliteMu serializes this process's write transactions on SQLite,
	// which has one writer per database anyway: without it two of our own
	// connections upgrade read locks to write locks at once and one fails
	// with SQLITE_BUSY at once, before any busy timeout applies.
	sqliteMu sync.Mutex
}

var (
	_ recorder.Store      = (*Store)(nil)
	_ recorder.Pruner     = (*Store)(nil)
	_ workflow.RunQuerier = (*Store)(nil)
)

// Option configures a Store.
type Option func(*Store) error

// WithTablePrefix sets the prefix of every table name (DefaultTablePrefix
// by default): lower-case letters, digits and underscores, starting with a
// letter, at most 32 characters. Two stores with different prefixes share
// one database without meeting.
func WithTablePrefix(p string) Option {
	return func(s *Store) error {
		if len(p) > maxPrefix || !prefixPattern.MatchString(p) {
			return fmt.Errorf("%w: sqlrecorder.WithTablePrefix(%q): use [a-z][a-z0-9_]*, at most %d characters", workflow.ErrInvalid, p, maxPrefix)
		}
		s.prefix = p
		return nil
	}
}

// WithRetention sets the retention Prune applies (see recorder.Retention);
// none by default, so Prune does nothing.
func WithRetention(r recorder.Retention) Option {
	return func(s *Store) error {
		if err := recorder.ValidateRetention(r); err != nil {
			return err
		}
		s.retention = r
		return nil
	}
}

// WithClock sets the clock Prune measures ages with (clock.Real by
// default).
func WithClock(c clock.Clock) Option {
	return func(s *Store) error {
		if c == nil {
			return fmt.Errorf("%w: sqlrecorder.WithClock(nil)", workflow.ErrInvalid)
		}
		s.clk = c
		return nil
	}
}

// WithTxAttempts sets how many times a write transaction runs on a
// contention error (DefaultTxAttempts by default, at least 1).
func WithTxAttempts(n int) Option {
	return func(s *Store) error {
		if n < 1 {
			return fmt.Errorf("%w: sqlrecorder.WithTxAttempts(%d)", workflow.ErrInvalid, n)
		}
		s.attempts = n
		return nil
	}
}

// WithRetryable replaces DefaultRetryable, the test of whether a failed
// transaction is retried, for a driver whose contention errors read
// differently.
func WithRetryable(fn func(error) bool) Option {
	return func(s *Store) error {
		if fn == nil {
			return fmt.Errorf("%w: sqlrecorder.WithRetryable(nil)", workflow.ErrInvalid)
		}
		s.retryable = fn
		return nil
	}
}

// New returns a Store over db, which the app opened with its own driver
// for dialect d. It does not touch the database: call Migrate before the
// first write. It fails on a nil db, an unknown dialect or an invalid
// option, with workflow.ErrInvalid.
func New(db *sql.DB, d Dialect, opts ...Option) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: sqlrecorder.New: nil *sql.DB", workflow.ErrInvalid)
	}
	if err := d.validate(); err != nil {
		return nil, err
	}
	s := &Store{db: db, d: d, prefix: DefaultTablePrefix, clk: clock.Real{}, attempts: DefaultTxAttempts, retryable: DefaultRetryable}
	for _, o := range opts {
		if o == nil {
			continue
		}
		if err := o(s); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// Dialect returns the store's dialect.
func (s *Store) Dialect() Dialect { return s.d }

// TablePrefix returns the store's table prefix.
func (s *Store) TablePrefix() string { return s.prefix }

// nanos is t as stored: Unix nanoseconds, 0 for the zero time.
func nanos(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

// fromNanos reads a stored time back, in UTC.
func fromNanos(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}

// nullID stores an empty ID as NULL, so the unique indexes on run_id and
// scheduled_id allow many runs without one.
func nullID(s string) sql.NullString { return sql.NullString{String: s, Valid: s != ""} }

// boolInt stores a bool as 0 or 1.
func boolInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// inTx runs fn in a transaction, retrying the whole transaction on a
// contention error, with a short growing pause between tries.
func (s *Store) inTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	var err error
	for attempt := 1; attempt <= s.attempts; attempt++ {
		err = s.tryTx(ctx, fn)
		if err == nil || !s.retryable(err) || ctx.Err() != nil {
			return err
		}
		pause := time.Duration(attempt*attempt) * time.Millisecond
		select {
		case <-ctx.Done():
			return err
		case <-time.After(pause):
		}
	}
	return fmt.Errorf("sqlrecorder: gave up after %d attempts: %w", s.attempts, err)
}

// tryTx runs fn in one transaction. On SQLite it holds sqliteMu and opens
// the transaction with a write that changes nothing, so the transaction
// takes the database's write lock first (waiting through the busy timeout
// for another process) instead of upgrading a read lock later, which
// SQLite refuses at once.
func (s *Store) tryTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	if s.d == SQLite {
		s.sqliteMu.Lock()
		defer s.sqliteMu.Unlock()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if s.d == SQLite {
		q := s.d.quote
		if _, err := tx.ExecContext(ctx, "UPDATE "+s.table(tableSchema)+" SET "+q("version")+" = "+q("version")+" WHERE 1 = 0"); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// runColumns is the column list of <prefix>runs, in the order scanRun
// reads and saveRun writes.
var runColumns = []string{
	"run_key", "run_id", "scheduled_id", "workflow", "queue", "tenant", "source", "source_ref", "status",
	"outcome", "outcome_note", "outcome_set", "error", "failed_step", "labels", "payload", "payload_size",
	"output", "output_size", "priority", "attempts", "attempt_errors", "attempted_by", "enqueued_at",
	"started_at", "finished_at", "run_at", "updated_at", "checked_at", "stale",
}

// selectRuns is "SELECT <run columns> FROM <runs> r".
func (s *Store) selectRuns() string {
	cols := make([]string, len(runColumns))
	for i, c := range runColumns {
		cols[i] = "r." + s.d.quote(c)
	}
	return "SELECT " + strings.Join(cols, ", ") + " FROM " + s.table(tableRuns) + " r"
}

// rowScanner is a *sql.Row or *sql.Rows.
type rowScanner interface{ Scan(dest ...any) error }

// scanRun reads one runs row.
func scanRun(sc rowScanner) (recorder.RunRecord, error) {
	var (
		r                                          recorder.RunRecord
		key                                        string
		runID, schedID                             sql.NullString
		status, outcome, source                    string
		labels, attemptErrs, attemptedBy           string
		outcomeSet, stale                          int64
		payloadSize, outputSize, priority, tries   int64
		enq, started, finished, runAt, upd, cheked int64
	)
	err := sc.Scan(&key, &runID, &schedID, &r.Workflow, &r.Queue, &r.Tenant, &source, &r.SourceRef, &status,
		&outcome, &r.OutcomeNote, &outcomeSet, &r.Error, &r.FailedStep, &labels, &r.Payload, &payloadSize,
		&r.Output, &outputSize, &priority, &tries, &attemptErrs, &attemptedBy, &enq,
		&started, &finished, &runAt, &upd, &cheked, &stale)
	if err != nil {
		return r, err
	}
	r.Ref = workflow.RunRef{RunID: workflow.RunID(runID.String), ScheduledID: workflow.ScheduledID(schedID.String)}
	r.Status, r.Outcome, r.Source = workflow.Status(status), workflow.Outcome(outcome), workflow.Source(source)
	r.OutcomeSet, r.Stale = outcomeSet == 1, stale == 1
	r.PayloadSize, r.OutputSize, r.Priority, r.Attempts = int(payloadSize), int(outputSize), int(priority), int(tries)
	r.EnqueuedAt, r.StartedAt, r.FinishedAt = fromNanos(enq), fromNanos(started), fromNanos(finished)
	r.RunAt, r.UpdatedAt, r.CheckedAt = fromNanos(runAt), fromNanos(upd), fromNanos(cheked)
	if err := decodeJSON(labels, &r.Labels); err != nil {
		return r, fmt.Errorf("sqlrecorder: run %s labels: %w", key, err)
	}
	if err := decodeJSON(attemptErrs, &r.Errors); err != nil {
		return r, fmt.Errorf("sqlrecorder: run %s errors: %w", key, err)
	}
	if err := decodeJSON(attemptedBy, &r.AttemptedBy); err != nil {
		return r, fmt.Errorf("sqlrecorder: run %s attempted_by: %w", key, err)
	}
	return r, nil
}

// decodeJSON reads a JSON column; "" and "null" leave dst as it is.
func decodeJSON[T any](s string, dst *T) error {
	if s == "" || s == "null" {
		return nil
	}
	return json.Unmarshal([]byte(s), dst)
}

// encodeJSON writes a JSON column; a nil slice or map is "null".
func encodeJSON[T any](v T) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// runValues returns r's column values in runColumns order.
func runValues(r recorder.RunRecord) ([]any, error) {
	lab := "null"
	if len(r.Labels) > 0 {
		var err error
		if lab, err = encodeJSON(r.Labels); err != nil {
			return nil, err
		}
	}
	errs, err := encodeJSON(r.Errors)
	if err != nil {
		return nil, err
	}
	by, err := encodeJSON(r.AttemptedBy)
	if err != nil {
		return nil, err
	}
	return []any{
		r.Key(), nullID(string(r.Ref.RunID)), nullID(string(r.Ref.ScheduledID)), r.Workflow, r.Queue, r.Tenant,
		string(r.Source), r.SourceRef, string(r.Status), string(r.Outcome), r.OutcomeNote, boolInt(r.OutcomeSet),
		r.Error, r.FailedStep, lab, r.Payload, int64(r.PayloadSize), r.Output, int64(r.OutputSize),
		int64(r.Priority), int64(r.Attempts), errs, by, nanos(r.EnqueuedAt), nanos(r.StartedAt),
		nanos(r.FinishedAt), nanos(r.RunAt), nanos(r.UpdatedAt), nanos(r.CheckedAt), boolInt(r.Stale),
	}, nil
}

// loaded is a run's row state inside a write transaction.
type loaded struct {
	rec    recorder.RunRecord
	isNew  bool
	keys   []string // the run_keys the record was read from (one, or two when joined)
	labels map[string]string
}

// load reads, and locks, the rows ref points at: the row of its RunID and
// the row of its ScheduledID, joined with recorder.MergeRecords when they
// are two. With create false a missing run returns nil.
func (s *Store) load(ctx context.Context, tx *sql.Tx, ref workflow.RunRef, create bool) (*loaded, error) {
	a := &args{d: s.d}
	var conds []string
	if !ref.RunID.IsZero() {
		conds = append(conds, "r."+s.d.quote("run_id")+" = "+a.add(string(ref.RunID)))
	}
	if !ref.ScheduledID.IsZero() {
		conds = append(conds, "r."+s.d.quote("scheduled_id")+" = "+a.add(string(ref.ScheduledID)))
	}
	q := s.selectRuns() + " WHERE " + strings.Join(conds, " OR ") + s.d.forUpdate()
	rows, err := tx.QueryContext(ctx, q, a.vals...)
	if err != nil {
		return nil, err
	}
	var found []recorder.RunRecord
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			_ = rows.Close()
			return nil, err
		}
		found = append(found, r)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	l := &loaded{}
	switch len(found) {
	case 0:
		if !create {
			return nil, nil
		}
		l.rec, l.isNew = recorder.NewRecord(ref), true
	case 1:
		l.rec, l.keys = found[0], []string{found[0].Key()}
	default:
		byRun, bySched := found[0], found[1]
		if byRun.Ref.RunID.IsZero() {
			byRun, bySched = bySched, byRun
		}
		l.rec, l.keys = recorder.MergeRecords(byRun, bySched), []string{byRun.Key(), bySched.Key()}
	}
	l.labels = maps.Clone(l.rec.Labels)
	return l, nil
}

// save writes l.rec back: an UPDATE of its row when its key is unchanged,
// else a DELETE of the old rows and an INSERT under the new key (a joined
// or newly identified run). Labels rows follow the record.
func (s *Store) save(ctx context.Context, tx *sql.Tx, l *loaded) error {
	vals, err := runValues(l.rec)
	if err != nil {
		return err
	}
	key := l.rec.Key()
	q := s.d.quote
	if len(l.keys) == 1 && l.keys[0] == key {
		a := &args{d: s.d}
		sets := make([]string, 0, len(runColumns)-1)
		for i, c := range runColumns[1:] {
			sets = append(sets, q(c)+" = "+a.add(vals[i+1]))
		}
		stmt := "UPDATE " + s.table(tableRuns) + " SET " + strings.Join(sets, ", ") + " WHERE " + q("run_key") + " = " + a.add(key)
		if _, err := tx.ExecContext(ctx, stmt, a.vals...); err != nil {
			return err
		}
		if !maps.Equal(l.labels, l.rec.Labels) {
			return s.writeLabels(ctx, tx, key, key, l.rec.Labels)
		}
		return nil
	}
	for _, old := range l.keys {
		a := &args{d: s.d}
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+s.table(tableRuns)+" WHERE "+q("run_key")+" = "+a.add(old), a.vals...); err != nil {
			return err
		}
		if err := s.writeLabels(ctx, tx, old, "", nil); err != nil {
			return err
		}
	}
	a := &args{d: s.d}
	ps := make([]string, len(vals))
	for i, v := range vals {
		ps[i] = a.add(v)
	}
	cols := make([]string, len(runColumns))
	for i, c := range runColumns {
		cols[i] = q(c)
	}
	stmt := "INSERT INTO " + s.table(tableRuns) + " (" + strings.Join(cols, ", ") + ") VALUES (" + strings.Join(ps, ", ") + ")"
	if _, err := tx.ExecContext(ctx, stmt, a.vals...); err != nil {
		return err
	}
	return s.writeLabels(ctx, tx, key, key, l.rec.Labels)
}

// writeLabels replaces the labels rows of oldKey with labels under newKey
// (newKey "" only deletes).
func (s *Store) writeLabels(ctx context.Context, tx *sql.Tx, oldKey, newKey string, labels map[string]string) error {
	q := s.d.quote
	a := &args{d: s.d}
	if _, err := tx.ExecContext(ctx, "DELETE FROM "+s.table(tableLabels)+" WHERE "+q("run_key")+" = "+a.add(oldKey), a.vals...); err != nil {
		return err
	}
	if newKey == "" {
		return nil
	}
	for _, k := range slices.Sorted(maps.Keys(labels)) {
		a := &args{d: s.d}
		stmt := "INSERT INTO " + s.table(tableLabels) + " (" + q("run_key") + ", " + q("k") + ", " + q("v") + ") VALUES (" +
			a.add(newKey) + ", " + a.add(k) + ", " + a.add(labels[k]) + ")"
		if _, err := tx.ExecContext(ctx, stmt, a.vals...); err != nil {
			return err
		}
	}
	return nil
}

// mutate loads the run ref points at (creating it when create is true),
// applies fn and saves it, all in one retried transaction. With create
// false and no run, fn is not called and nothing is written.
func (s *Store) mutate(ctx context.Context, ref workflow.RunRef, create bool, fn func(tx *sql.Tx, l *loaded) error) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		l, err := s.load(ctx, tx, ref, create)
		if err != nil || l == nil {
			return err
		}
		if err := fn(tx, l); err != nil {
			return err
		}
		return s.save(ctx, tx, l)
	})
}
