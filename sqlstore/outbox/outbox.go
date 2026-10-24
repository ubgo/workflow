package outbox

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strings"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/sqlb/contrib/migrate"
	"github.com/ubgo/sqlb/ddl"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// Entry states as stored in the status column. A delivered entry is
// deleted, so there is no sent state.
const (
	statusPending = "pending" // waiting for a relay, or for its backoff to end
	statusDead    = "dead"    // failed WithMaxAttempts times; never relayed again
)

// MigrationOwner is the ledger owner of the outbox's migrations, distinct
// from sqlstore's, so the two keep separate histories in one ledger table.
// Under WithSchema the recorded owner is "<MigrationOwner>@<schema>".
const MigrationOwner = "ubgo/workflow/sqlstore/outbox"

// Ledger naming, the same stems sqlstore uses, so an app running both over
// one prefix has one ledger table and one migration lock.
const (
	ledgerStem = "migrations"
	lockStem   = "migrate"
	tableStem  = "outbox"
)

// defaultTablePrefix starts every table name, as in sqlstore.
const defaultTablePrefix = "wf_"

// prefixPattern keeps table prefixes to characters every engine accepts
// unquoted, as sqlstore does.
var prefixPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,30}$`)

// ErrSchemaTooNew means the ledger records an outbox migration this code
// does not know: a newer release migrated the database.
var ErrSchemaTooNew = errors.New("outbox: the database schema is newer than this code")

// Option configures an Outbox.
type Option func(*Outbox)

// WithClock sets the time source of due times, backoff, leases and the
// relay's poll timer. The default is clock.Real.
func WithClock(c clock.Clock) Option {
	return func(o *Outbox) {
		if c != nil {
			o.clk = c
		}
	}
}

// WithTablePrefix sets the prefix of the outbox and ledger table names
// (default "wf_"). It must match ^[a-z][a-z0-9_]{0,30}$.
func WithTablePrefix(p string) Option { return func(o *Outbox) { o.prefix = p } }

// WithSchema puts the outbox table in the named Postgres schema, as
// sqlstore.WithSchema does: the schema must exist and match the prefix
// pattern; New refuses it on SQLite and MySQL. The migration ledger stays
// in the connection's search_path, its rows owned as
// "<MigrationOwner>@<schema>".
func WithSchema(name string) Option { return func(o *Outbox) { o.schema = name } }

// IDGenerator returns a new entry ID. The ID is also the idempotency key the
// relay sends for a job without its own, so it must be unique across every
// writer of the table; a duplicate fails Write. An ID is 1 to 64 bytes;
// Write refuses anything else, and fails when the generator returns an
// error, so a broken generator writes nothing.
type IDGenerator func() (string, error)

// DefaultIDGenerator returns "obx_" and 16 random bytes in hex.
func DefaultIDGenerator() (string, error) { return randomID("obx"), nil }

// WithIDGenerator sets how entry IDs are made (default DefaultIDGenerator),
// for time-sortable IDs (UUIDv7, ULID) or IDs that line up with the app's
// own. A nil generator is ignored.
func WithIDGenerator(g IDGenerator) Option {
	return func(o *Outbox) {
		if g != nil {
			o.ids = g
		}
	}
}

// maxIDLen is the width of the id column (VARCHAR(64)).
const maxIDLen = 64

// WithLogger sets where relays report what they cannot return to a caller
// (a failed enqueue, a dead entry). The default discards.
func WithLogger(l *slog.Logger) Option {
	return func(o *Outbox) {
		if l != nil {
			o.log = l
		}
	}
}

// Outbox is one outbox table. Write adds entries inside the app's
// transactions; NewRelay builds the relays that move them to a client. It
// holds no state of its own: every entry is a row.
type Outbox struct {
	db     *sql.DB
	d      sqlb.Dialect
	engine string
	prefix string
	schema string // Postgres schema (WithSchema); empty: the search_path
	table  string // quoted, schema-qualified <prefix>outbox
	ids    IDGenerator
	clk    clock.Clock
	log    *slog.Logger
}

// New returns the outbox over db, whose SQL is written for d (sqlb's
// Postgres, SQLite or MySQL dialect). It opens nothing and starts nothing.
func New(db *sql.DB, d sqlb.Dialect, opts ...Option) (*Outbox, error) {
	if db == nil || d == nil {
		return nil, fmt.Errorf("%w: outbox.New needs a *sql.DB and a dialect", workflow.ErrInvalid)
	}
	return build(db, d, opts)
}

// build is New without the nil-pool check: MigrationScripts renders SQL
// text and never touches a pool.
func build(db *sql.DB, d sqlb.Dialect, opts []Option) (*Outbox, error) {
	if d == nil {
		return nil, fmt.Errorf("%w: the outbox needs a dialect", workflow.ErrInvalid)
	}
	o := &Outbox{db: db, d: d, engine: sqlb.DialectName(d), prefix: defaultTablePrefix, ids: DefaultIDGenerator,
		clk: clock.Real{}, log: slog.New(slog.DiscardHandler)}
	switch o.engine {
	case sqlb.DialectPostgres, sqlb.DialectSQLite, sqlb.DialectMySQL:
	default:
		return nil, workflow.Unsupported("dialect "+o.engine, "the outbox runs on Postgres, SQLite and MySQL")
	}
	for _, opt := range opts {
		opt(o)
	}
	if !prefixPattern.MatchString(o.prefix) {
		return nil, fmt.Errorf("%w: table prefix %q must match %s", workflow.ErrInvalid, o.prefix, prefixPattern)
	}
	if o.schema != "" {
		if o.engine != sqlb.DialectPostgres {
			return nil, workflow.Unsupported("WithSchema on "+o.engine, "only Postgres has schemas inside a database; on MySQL point the pool at the database")
		}
		if !prefixPattern.MatchString(o.schema) {
			return nil, fmt.Errorf("%w: schema %q must match %s", workflow.ErrInvalid, o.schema, prefixPattern)
		}
		o.table = d.QuoteIdent(o.schema) + "." + d.QuoteIdent(o.prefix+tableStem)
	} else {
		o.table = d.QuoteIdent(o.prefix + tableStem)
	}
	return o, nil
}

// Write adds job to the outbox inside the app's transaction tx and returns
// the entry's ID, which is also the idempotency key the relay sends when
// the job has none. The entry exists only if tx commits. tx must be open on
// the database the outbox lives in. Nothing about the target is checked
// here (the outbox does not know it); a job the target refuses is retried
// with backoff, then dead after WithMaxAttempts.
func (o *Outbox) Write(ctx context.Context, tx *sql.Tx, job workflow.Job) (string, error) {
	if tx == nil {
		return "", fmt.Errorf("%w: outbox.Write needs a transaction", workflow.ErrInvalid)
	}
	if job.Workflow == "" {
		return "", fmt.Errorf("%w: outbox.Write needs Job.Workflow", workflow.ErrInvalid)
	}
	labels, err := json.Marshal(job.Labels)
	if err != nil {
		return "", fmt.Errorf("outbox: encode labels: %w", err)
	}
	var idem any // NULL unless set
	runAt := int64(0)
	if job.IdempotencyKey != "" {
		idem = job.IdempotencyKey
	}
	if !job.RunAt.IsZero() {
		runAt = job.RunAt.UnixNano()
	}
	id, err := o.newID()
	if err != nil {
		return "", err
	}
	now := o.clk.Now().UnixNano()
	_, err = tx.ExecContext(ctx, o.sql(`INSERT INTO {t} (id, workflow, payload, labels, idem, priority, run_at, status,
		attempts, due, lease_until, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, 0, ?)`),
		id, job.Workflow, job.Payload, string(labels), idem, job.Priority, runAt, statusPending, now, now)
	if err != nil {
		return "", fmt.Errorf("outbox: write: %w", err)
	}
	return id, nil
}

// sql turns a fixed statement into the engine's text: {t} becomes the
// quoted outbox table and each ? the dialect's placeholder. The statements
// in this package hold no ? other than parameters and no string literal
// with a brace.
func (o *Outbox) sql(q string) string {
	q = strings.ReplaceAll(q, "{t}", o.table)
	if o.d.Placeholder(1) == "?" {
		return q
	}
	var b strings.Builder
	n := 0
	for _, r := range q {
		if r == '?' {
			n++
			b.WriteString(o.d.Placeholder(n))
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// newID asks the generator for an entry ID and checks it fits the column.
func (o *Outbox) newID() (string, error) {
	id, err := o.ids()
	if err != nil {
		return "", fmt.Errorf("outbox: generate an entry id: %w", err)
	}
	if id == "" || len(id) > maxIDLen {
		return "", fmt.Errorf("%w: the id generator returned %q; an entry id is 1 to %d bytes", workflow.ErrInvalid, id, maxIDLen)
	}
	return id, nil
}

// randomID returns prefix, an underscore and 16 random bytes in hex: the
// default entry ID and every lease token. It panics only if the operating
// system's random source fails, which crypto/rand documents as never
// returning an error on supported platforms.
func randomID(prefix string) string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return prefix + "_" + hex.EncodeToString(b[:])
}

// owner is the ledger owner of the outbox's list in this outbox: under
// WithSchema it carries "@<schema>", because the ledger table is shared.
func (o *Outbox) owner() string {
	if o.schema == "" {
		return MigrationOwner
	}
	return MigrationOwner + "@" + o.schema
}

// migrations is every outbox schema change, in order. A released migration
// is never edited (the ledger checks its checksum); a change is a new,
// numbered migration appended here. Each script is safe to run twice (IF
// NOT EXISTS), because MySQL commits DDL implicitly.
func (o *Outbox) migrations() []migrate.Migration {
	return []migrate.Migration{{Owner: o.owner(), Name: "001_outbox", Script: o.script001()}}
}

// script001 creates the outbox table. Its text is frozen once released.
func (o *Outbox) script001() ddl.Script {
	id, name, short, blob, text, integer, serial, suffix := "VARCHAR(64)", "VARCHAR(191)", "VARCHAR(16)", "BYTEA", "TEXT", "BIGINT", "BIGSERIAL PRIMARY KEY", ""
	switch o.engine {
	case sqlb.DialectMySQL:
		// utf8mb4_bin: keys compare byte for byte, as in sqlstore.
		blob, text, serial = "LONGBLOB", "LONGTEXT", "BIGINT AUTO_INCREMENT PRIMARY KEY"
		suffix = " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	case sqlb.DialectSQLite:
		id, name, short, blob, integer, serial = "TEXT", "TEXT", "TEXT", "BLOB", "INTEGER", "INTEGER PRIMARY KEY AUTOINCREMENT"
	}
	index := o.d.QuoteIdent(o.prefix + tableStem + "_claim")
	lines := []string{
		// seq is the relay order: first written, first relayed.
		"seq " + serial,
		// id is the entry's ID and the default idempotency key.
		"id " + id + " NOT NULL",
		"workflow " + name + " NOT NULL",
		"payload " + blob,
		"labels " + text + " NOT NULL",
		// idem is the job's own idempotency key; NULL when it had none.
		"idem " + name,
		"priority " + integer + " NOT NULL",
		// run_at is Job.RunAt (0: now), passed on to the target.
		"run_at " + integer + " NOT NULL",
		// status is statusPending or statusDead.
		"status " + short + " NOT NULL",
		// attempts counts failed enqueues.
		"attempts " + integer + " NOT NULL",
		// due is when the entry may be claimed (after a failure: its backoff).
		"due " + integer + " NOT NULL",
		"lease_token " + id,
		"lease_until " + integer + " NOT NULL",
		"created_at " + integer + " NOT NULL",
		"last_err " + text,
		"UNIQUE (id)",
	}
	if o.engine == sqlb.DialectMySQL { // no CREATE INDEX IF NOT EXISTS
		lines = append(lines, "INDEX "+index+" (status, due)", "INDEX "+o.d.QuoteIdent(o.prefix+tableStem+"_lease")+" (lease_token)")
	}
	stmts := []string{"CREATE TABLE IF NOT EXISTS {t} (\n\t" + strings.Join(lines, ",\n\t") + "\n)" + suffix}
	if o.engine != sqlb.DialectMySQL {
		stmts = append(stmts,
			"CREATE INDEX IF NOT EXISTS "+index+" ON {t} (status, due)",
			"CREATE INDEX IF NOT EXISTS "+o.d.QuoteIdent(o.prefix+tableStem+"_lease")+" ON {t} (lease_token)")
	}
	out := make(ddl.Script, len(stmts))
	for i, q := range stmts {
		out[i] = ddl.Statement{SQL: o.sql(q), Transactional: true}
	}
	return out
}

// ledger is the migration ledger shared with sqlstore for this prefix. The
// ledger table is never schema-qualified (sqlb's ledger has no schema);
// under WithSchema the lock key carries the schema, as in sqlstore.
func (o *Outbox) ledger() (migrate.Ledger, error) {
	key := o.prefix + lockStem
	if o.schema != "" {
		key = o.schema + "." + key
	}
	l, err := migrate.NewLedger(o.d, o.prefix+ledgerStem, migrate.EngineLock(o.d, key))
	if err != nil {
		return migrate.Ledger{}, fmt.Errorf("outbox: migration ledger: %w", err)
	}
	return l, nil
}

// Migrate brings the outbox table up to this code's version, under the
// engine's migration lock, so concurrent calls are safe. It refuses a
// database a newer release migrated (ErrSchemaTooNew) and never drops
// anything. Write and relays never migrate: running it is the deploy's
// decision.
func (o *Outbox) Migrate(ctx context.Context) error {
	if err := o.checkVersion(ctx); err != nil {
		return err
	}
	l, err := o.ledger()
	if err != nil {
		return err
	}
	if _, err := l.RefuseUnknown().ApplyDB(ctx, o.db, o.migrations()...); err != nil {
		if errors.Is(err, migrate.ErrUnknownMigration) {
			if verr := o.checkVersion(ctx); verr != nil {
				return verr
			}
		}
		return fmt.Errorf("outbox: migrate: %w", err)
	}
	return nil
}

// checkVersion fails with an error matching ErrSchemaTooNew, naming the
// migrations, when the ledger records an outbox migration this code lacks.
func (o *Outbox) checkVersion(ctx context.Context) error {
	l, err := o.ledger()
	if err != nil {
		return err
	}
	rows, err := l.AppliedDB(ctx, o.db)
	if err != nil {
		return fmt.Errorf("outbox: read the migration ledger %s%s: %w", o.prefix, ledgerStem, err)
	}
	known := map[string]bool{}
	for _, m := range o.migrations() {
		known[m.Name] = true
	}
	var unknown []string
	for _, r := range rows {
		if r.Owner == o.owner() && !known[r.Name] {
			unknown = append(unknown, r.Name)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return fmt.Errorf("%w: unknown migration(s) %s; upgrade this client", ErrSchemaTooNew, strings.Join(unknown, ", "))
	}
	return nil
}

// MigrationScript is one outbox migration as SQL text: the exact statements
// Migrate runs, in order, with the table name resolved for the options'
// prefix and schema. Owner and Name are what Migrate records in its ledger.
type MigrationScript struct {
	Owner, Name string
	Statements  []string
}

// MigrationScripts returns every migration Outbox.Migrate applies, in
// order, as SQL text for dialect d and the table options among opts
// (WithTablePrefix, WithSchema), for teams whose DDL goes through goose,
// Atlas, Flyway, Liquibase or a DBA review. The text is frozen per release
// as Migrate's is, and each statement is safe to run twice. Write and
// relays never read the ledger, so a table applied this way works as is,
// and a later Migrate re-runs each script (a no-op) and records it.
func MigrationScripts(d sqlb.Dialect, opts ...Option) ([]MigrationScript, error) {
	o, err := build(nil, d, opts)
	if err != nil {
		return nil, err
	}
	var out []MigrationScript
	for _, m := range o.migrations() {
		ms := MigrationScript{Owner: m.Owner, Name: m.Name}
		for _, st := range m.Script {
			ms.Statements = append(ms.Statements, st.SQL)
		}
		out = append(out, ms)
	}
	return out, nil
}
