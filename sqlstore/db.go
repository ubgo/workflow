package sqlstore

import (
	"context"
	"crypto/rand"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	mrand "math/rand/v2"
	"net"
	"strings"
	"time"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/workflow"
)

// The engines this driver runs on, as sqlb names them. A dialect whose
// sqlb.DialectName is not in this list is refused by New, because the
// claim statement and the DDL are written per engine.
const (
	enginePostgres = sqlb.DialectPostgres
	engineSQLite   = sqlb.DialectSQLite
	engineMySQL    = sqlb.DialectMySQL
)

// Table name stems. The real name is the prefix (WithTablePrefix) plus the
// stem, quoted by the dialect; statements refer to a table as {stem} and
// sqlText substitutes it, so no table name is ever concatenated by hand.
const (
	tblRuns      = "runs"
	tblSteps     = "steps"
	tblSignals   = "signals"
	tblConsumed  = "consumed"
	tblSchedules = "schedules"
	tblTicks     = "ticks"
	tblLocks     = "locks"
	tblStarts    = "starts"
	tblBatches   = "batches"
	// tblUnique holds UniqueStore's locks. Not "unique", a reserved word
	// on every engine.
	tblUnique = "unique_locks"
	// tblMisfire holds the misfire policy of the schedules that set one
	// (migration 002); a schedule without a row uses the default.
	tblMisfire = "schedule_misfire"
	// tblDebounce holds DebounceStore's bursts (its own migration list).
	tblDebounce = "debounce"
	// tblRunExt, tblStepExt, tblPauses and tblScheduleExt are migration
	// 003's tables (adminDefs): queues, retry overrides and discards per
	// run, attempt counts per step, pauses, and schedule windows and pauses.
	tblRunExt      = "run_ext"
	tblStepExt     = "step_ext"
	tblPauses      = "pauses"
	tblScheduleExt = "schedule_ext"
	// tblRunChain and tblStepErr are migration 004's tables
	// (statesChainDefs): the rest of a run's job chain (chain.go), and the
	// error of a step's last failed attempt while it waits on its backoff.
	tblRunChain = "run_chain"
	tblStepErr  = "step_err"
	// tblRunLabels is migration 005's label index (labelDefs, labels.go):
	// one row per label of a run, so a label filter reads an index.
	tblRunLabels = "run_labels"
	// tblScheduleLast and tblAttemptErr are migration 006's tables
	// (historyDefs): each schedule's overlap policy and last fire, and the
	// bounded per-attempt error history of each run.
	tblScheduleLast = "schedule_last"
	tblAttemptErr   = "attempt_errors"
	// tblRunForced is migration 006's marker of runs ForceFail ended.
	tblRunForced = "run_forced"
	// tblOutputs is OutputStore's table (its own migration list, output.go).
	tblOutputs = "outputs"
	// tblScheduleTenant is migration 009: Schedule.Tenant per schedule.
	tblScheduleTenant = "schedule_tenant"
	// tblJournal and tblBuckets are JournalStore's and BucketStore's tables
	// (each its own migration list).
	tblJournal = "journal"
	tblBuckets = "buckets"
)

var allTables = []string{tblRuns, tblSteps, tblSignals, tblConsumed, tblSchedules, tblTicks, tblLocks, tblStarts, tblBatches, tblUnique, tblMisfire, tblDebounce,
	tblRunExt, tblStepExt, tblPauses, tblScheduleExt, tblRunChain, tblStepErr, tblRunLabels, tblScheduleLast, tblAttemptErr, tblRunForced, tblOutputs,
	tblLeaders, tblRunsArchive, tblStepsArchive, tblRunInfo, tblAttemptNode, tblNodes, tblStepCancel, tblScheduleSpec, tblUniqueHolder, tblScheduleTenant, tblJournal, tblBuckets}

// store is the database half of the client: the handle, the dialect, the
// statement text rewriting and the database-facing policies (error
// classification, transaction attempts, ID generation). It holds no run
// state; every fact lives in the tables.
type store struct {
	db     *sql.DB
	d      sqlb.Dialect
	engine string
	names  *strings.Replacer // {stem} -> quoted, schema-qualified, prefixed table name
	prefix string
	// schema is the Postgres schema the tables live in (WithSchema); empty
	// means the connection's search_path.
	schema     string
	classify   ErrorClassifier
	txAttempts int
	ids        IDGenerator
}

func newStore(db *sql.DB, d sqlb.Dialect, c *Client) *store {
	s := &store{db: db, d: d, engine: sqlb.DialectName(d), prefix: c.prefix, schema: c.schema,
		classify: c.classify, txAttempts: c.txAttempts, ids: c.ids}
	pairs := make([]string, 0, 2*len(allTables))
	for _, t := range allTables {
		pairs = append(pairs, "{"+t+"}", s.table(t))
	}
	s.names = strings.NewReplacer(pairs...)
	return s
}

// table is the quoted name of the table stem, qualified by the schema when
// WithSchema set one.
func (s *store) table(stem string) string {
	if s.schema == "" {
		return s.d.QuoteIdent(s.prefix + stem)
	}
	return s.d.QuoteIdent(s.schema) + "." + s.d.QuoteIdent(s.prefix+stem)
}

// owner is the ledger owner of a migration list in this store. Under
// WithSchema it carries "@<schema>", because sqlb's ledger table is not
// schema-qualified: it lives in the connection's search_path, and two
// schemas sharing one ledger must keep separate histories.
func (s *store) owner(base string) string {
	if s.schema == "" {
		return base
	}
	return base + "@" + s.schema
}

// sqlText turns a fixed statement into the engine's text: {stem} becomes
// the quoted table name and each ? becomes the dialect's placeholder. The
// statements this package writes contain no ? other than parameters and no
// string literal holding a brace, which is what makes the rewrite safe.
func (s *store) sqlText(q string) string {
	q = s.names.Replace(q)
	if s.d.Placeholder(1) == "?" {
		return q
	}
	var b strings.Builder
	n := 0
	for _, r := range q {
		if r == '?' {
			n++
			b.WriteString(s.d.Placeholder(n))
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// exec runs a fixed write statement on x (the pool or a transaction).
func (s *store) exec(ctx context.Context, x sqlb.DBTX, q string, args ...any) (sql.Result, error) {
	res, err := x.ExecContext(ctx, s.sqlText(q), args...)
	if err != nil {
		return nil, fmt.Errorf("sqlstore: %w", s.mark(err))
	}
	return res, nil
}

// execN runs a write and returns the number of rows it changed.
func (s *store) execN(ctx context.Context, x sqlb.DBTX, q string, args ...any) (int64, error) {
	res, err := s.exec(ctx, x, q, args...)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("sqlstore: rows affected: %w", err)
	}
	return n, nil
}

// one reads exactly one row into targets through sqlb's raw query, so the
// scan targets travel with the statement. It returns sqlb.ErrNoRows when
// nothing matches.
func (s *store) one(ctx context.Context, x sqlb.DBTX, q string, args []any, targets ...any) error {
	return s.mark(sqlb.One(ctx, x, sqlb.NewQuery(s.sqlText(q), args, targets)))
}

// each reads every row into targets, calling fn after each scan. The
// targets are reused per row, so fn copies what it keeps.
func (s *store) each(ctx context.Context, x sqlb.DBTX, q string, args []any, fn func() error, targets ...any) error {
	return s.mark(sqlb.Each(ctx, x, sqlb.NewQuery(s.sqlText(q), args, targets), fn))
}

// defaultTxAttempts bounds how often a transaction that lost a lock race
// (SQLite busy, a Postgres or MySQL deadlock or serialization failure) is
// tried in all before the error reaches the caller (WithTxAttempts). Five
// covers the short bursts of contention a busy queue sees without hiding a
// lock that never frees.
const defaultTxAttempts = 5

// tx runs fn in a transaction, committing when it returns nil. A
// transaction that loses a lock race is retried from the start, so fn must
// derive everything from what it reads inside the transaction.
func (s *store) tx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	return s.retry(ctx, func() error { return s.txOnce(ctx, fn) })
}

// retry runs op up to WithTxAttempts times while it fails with an error the
// classifier calls retryable (a lost lock race), pausing a jittered moment
// between attempts. op must be safe to run again. tx uses it for whole
// transactions; a single autocommit statement that can lose the same race
// (the lock-row insert at Start, where two clients open one SQLite file at
// once) uses it directly.
func (s *store) retry(ctx context.Context, op func() error) error {
	var err error
	for attempt := 1; attempt <= s.txAttempts; attempt++ {
		err = op()
		if err == nil || !s.isRetryable(err) || ctx.Err() != nil {
			return err
		}
		if attempt < s.txAttempts {
			sleepCtx(ctx, txRetryBackoff(attempt))
		}
	}
	return err
}

// txRetryStep is the base wait before a lost-lock-race transaction is
// tried again; attempt n waits a random time in (0, n*txRetryStep]. Retrying
// at once makes the transactions that just deadlocked collide again in the
// same order (seen on MySQL, where a dozen Swaps of one debounce key
// exhausted every attempt back to back); spreading them lets the winner
// commit. Wall time on purpose: lock waits are real I/O, not scheduling.
const txRetryStep = 4 * time.Millisecond

// txRetryBackoff is the jittered wait after the attempt-th failed attempt.
func txRetryBackoff(attempt int) time.Duration {
	return time.Duration(1 + mrand.Int64N(int64(attempt)*int64(txRetryStep)))
}

// sleepCtx waits d or until ctx ends.
func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func (s *store) txOnce(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlstore: begin: %w", s.mark(err))
	}
	if err := fn(tx); err != nil {
		// The rollback error is dropped on purpose: fn's error is the cause
		// the caller acts on, and a failed rollback of a failed transaction
		// leaves nothing committed either way.
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlstore: commit: %w", s.mark(err))
	}
	return nil
}

// Error signatures the default classifier recognises. The core of this
// module cannot import the database drivers, so it recognises each driver's
// error by the one stable property it exposes: Postgres's SQLSTATE through
// an SQLState() method (pgx and lib/pq both have it), and the message text
// for SQLite and MySQL. The SQLite and Postgres forms are pinned by this
// module's tests on those engines; the MySQL forms follow ubgo/auth, where
// they are pinned. A driver or wrapper whose errors read differently plugs
// in its own ErrorClassifier.
const (
	pgUniqueViolation = "23505"
	pgDeadlock        = "40P01"
	pgSerialization   = "40001"
	// pgConnectionClass is SQLSTATE class 08, connection exceptions.
	pgConnectionClass = "08"
	// pgAdminShutdown, pgCrashShutdown and pgCannotConnectNow are the
	// server going away or not accepting connections yet.
	pgAdminShutdown    = "57P01"
	pgCrashShutdown    = "57P02"
	pgCannotConnectNow = "57P03"
	// pgxConnectFailed starts every pgx dial error ("failed to connect
	// to `...`: ..."), which is not always a *net.OpError (a dial timeout,
	// a TLS failure).
	pgxConnectFailed = "failed to connect to"
	// mysqlInvalidConn is go-sql-driver's ErrInvalidConn; Error 1040 is too
	// many connections and Error 1053 a server shutting down.
	mysqlInvalidConn     = "invalid connection"
	mysqlTooManyConns    = "Error 1040"
	mysqlServerShutdown  = "Error 1053"
	mysqlDuplicate       = "Error 1062"
	mysqlDeadlock        = "Error 1213"
	mysqlLockWaitTimeout = "Error 1205"
	sqliteUnique         = "UNIQUE constraint failed"
	sqliteBusy           = "SQLITE_BUSY"
	sqliteLocked         = "database is locked"
)

// ErrorClass is what an ErrorClassifier says about one database error. The
// zero value, "neither", is the safe answer: the error reaches the caller
// as a failure.
type ErrorClass struct {
	// UniqueViolation means a unique constraint refused the write. The
	// driver's correctness guards are unique indexes (an idempotency key
	// held twice, a cron tick fired twice, a lock row inserted twice), so
	// this is how a lost race is told from a real failure. Saying true for
	// an error that is not one makes the driver treat a failure as "someone
	// else won", so a classifier says it only when sure.
	UniqueViolation bool
	// Retryable means the transaction lost a lock race (a deadlock, a
	// serialization failure, SQLite busy) and is tried again from the
	// start, up to WithTxAttempts.
	Retryable bool
	// Unavailable means the database could not be reached or dropped the
	// connection (refused, reset, shut down, out of connections), as
	// opposed to refusing the statement. The driver marks such errors with
	// workflow.Unavailable, so errors.Is(err, workflow.ErrUnavailable)
	// holds and a failover client can tell an outage from a bad request.
	Unavailable bool
}

// ErrorClassifier sorts a database error the driver got back. It is called
// only with a non-nil error, which may be wrapped (use errors.As), and must
// be safe for concurrent use. DefaultErrorClassifier is the default; a
// classifier for a driver with other error text usually checks its own
// forms and falls back to DefaultErrorClassifier:
//
//	sqlstore.WithErrorClassifier(func(err error) sqlstore.ErrorClass {
//		var e *mydriver.Error
//		if errors.As(err, &e) {
//			return sqlstore.ErrorClass{UniqueViolation: e.Code == mydriver.Dup, Retryable: e.Code == mydriver.Busy}
//		}
//		return sqlstore.DefaultErrorClassifier(err)
//	})
type ErrorClassifier func(err error) ErrorClass

// DefaultErrorClassifier recognises the errors of the drivers sqlb's
// contrib modules open: Postgres by SQLSTATE (23505 unique; 40P01 and 40001
// retryable; class 08 and 57P01-57P03 unavailable), MySQL by "Error 1062"
// (unique), "Error 1213"/"Error 1205" (retryable) and "invalid
// connection"/"Error 1040"/"Error 1053" (unavailable), SQLite by "UNIQUE
// constraint failed" (unique) and "SQLITE_BUSY"/"database is locked"
// (retryable). On any engine a *net.OpError, driver.ErrBadConn,
// sql.ErrConnDone or a pgx "failed to connect to" dial error is
// unavailable.
func DefaultErrorClassifier(err error) ErrorClass {
	if err == nil {
		return ErrorClass{}
	}
	var state interface{ SQLState() string }
	if errors.As(err, &state) {
		code := state.SQLState()
		switch code {
		case pgUniqueViolation:
			return ErrorClass{UniqueViolation: true}
		case pgDeadlock, pgSerialization:
			return ErrorClass{Retryable: true}
		case pgAdminShutdown, pgCrashShutdown, pgCannotConnectNow:
			return ErrorClass{Unavailable: true}
		}
		if strings.HasPrefix(code, pgConnectionClass) {
			return ErrorClass{Unavailable: true}
		}
	}
	var netErr *net.OpError
	if errors.As(err, &netErr) || errors.Is(err, driver.ErrBadConn) || errors.Is(err, sql.ErrConnDone) {
		return ErrorClass{Unavailable: true}
	}
	msg := err.Error()
	for _, sig := range []string{pgxConnectFailed, mysqlInvalidConn, mysqlTooManyConns, mysqlServerShutdown} {
		if strings.Contains(msg, sig) {
			return ErrorClass{Unavailable: true}
		}
	}
	if strings.Contains(msg, mysqlDuplicate) || strings.Contains(msg, sqliteUnique) {
		return ErrorClass{UniqueViolation: true}
	}
	for _, sig := range []string{sqliteBusy, sqliteLocked, mysqlDeadlock, mysqlLockWaitTimeout} {
		if strings.Contains(msg, sig) {
			return ErrorClass{Retryable: true}
		}
	}
	return ErrorClass{}
}

// isUnique reports whether err is a unique-constraint failure, per the
// store's classifier.
func (s *store) isUnique(err error) bool {
	return err != nil && s.classify(err).UniqueViolation
}

// mark wraps err with workflow.Unavailable when the classifier says the
// database could not be reached, so callers can match
// workflow.ErrUnavailable; any other error is returned as is.
func (s *store) mark(err error) error {
	if err != nil && s.classify(err).Unavailable && !workflow.IsUnavailable(err) {
		return workflow.Unavailable(err)
	}
	return err
}

// isRetryable reports whether err means "another transaction won a lock
// race; try again", per the store's classifier.
func (s *store) isRetryable(err error) bool {
	return err != nil && s.classify(err).Retryable
}

// IDKind names what a generated ID identifies, so one IDGenerator can shape
// each kind differently. The values are the prefixes the default uses.
type IDKind string

// The kinds of ID the driver asks an IDGenerator for. Lease and failure
// tokens are not among them: they are internal, never shown to a caller,
// and always 128 random bits.
const (
	// IDRun is a run's RunID.
	IDRun IDKind = "run"
	// IDScheduled is the ScheduledID of a run enqueued with a future RunAt.
	IDScheduled IDKind = "sched"
	// IDStep is a step row's ID (StepInfo carries it).
	IDStep IDKind = "step"
	// IDSchedule is the ScheduleID of a Client.Schedule without a Name. A
	// named schedule's ID is derived from its workflow and name, so the
	// same name is the same row on every node and every boot.
	IDSchedule IDKind = "sch"
)

// maxIDLen is the width of every ID column (VARCHAR(64)).
const maxIDLen = 64

// IDGenerator returns a new ID of kind. IDs must be unique per kind across
// every client of the database: a duplicate fails the write that uses it
// (each ID column is a primary or unique key). An ID is 1 to 64 bytes; the
// driver refuses anything else, and refuses the operation when the
// generator returns an error, so a broken generator fails closed rather
// than writing a bad row. Use it for time-sortable IDs (UUIDv7, ULID) or
// IDs that line up with the app's own:
//
//	sqlstore.WithIDGenerator(func(kind sqlstore.IDKind) (string, error) {
//		id, err := uuid.NewV7()
//		return string(kind) + "_" + id.String(), err
//	})
type IDGenerator func(kind IDKind) (string, error)

// DefaultIDGenerator returns the kind, an underscore and 16 random bytes in
// hex ("run_3f2a..."). It panics only if the operating system's random
// source fails, which crypto/rand documents as never happening on
// supported platforms.
func DefaultIDGenerator(kind IDKind) (string, error) { return randomID(string(kind)), nil }

// newID asks the store's generator for an ID of kind and checks it fits.
// IDs are made in Go before the insert because MySQL has no RETURNING.
func (s *store) newID(kind IDKind) (string, error) {
	id, err := s.ids(kind)
	if err != nil {
		return "", fmt.Errorf("sqlstore: generate a %s id: %w", kind, err)
	}
	if id == "" || len(id) > maxIDLen {
		return "", fmt.Errorf("%w: the id generator returned %q for a %s id; an id is 1 to %d bytes", workflow.ErrInvalid, id, kind, maxIDLen)
	}
	return id, nil
}

// randomID returns prefix plus 16 random bytes in hex: the default ID
// format and every internal token. It panics only if the operating
// system's random source fails, which crypto/rand documents as never
// returning an error on supported platforms.
func randomID(prefix string) string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return prefix + "_" + hex.EncodeToString(b[:])
}

// inList returns "?, ?, ?" for n parameters (at least one, so an empty
// list still renders valid SQL that matches nothing when given an
// impossible value).
func inList(n int) string {
	if n < 1 {
		n = 1
	}
	return strings.TrimSuffix(strings.Repeat("?, ", n), ", ")
}

// isNoRows reports whether err is sqlb's "no row matched".
func isNoRows(err error) bool { return errors.Is(err, sqlb.ErrNoRows) }
