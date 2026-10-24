package sqlstore

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/sqlb/contrib/migrate"
	"github.com/ubgo/sqlb/ddl"
)

// MigrationOwner is the ledger owner of every sqlstore migration, so an app
// that keeps its own migrations in a ledger table of the same name never
// collides with these. Under WithSchema the recorded owner is
// "<MigrationOwner>@<schema>".
const MigrationOwner = "ubgo/workflow/sqlstore"

// Ledger naming. Both derive from the table prefix (WithTablePrefix), so two
// prefixes in one database keep separate ledgers and separate locks.
const (
	ledgerStem = "migrations" // the ledger table: <prefix>migrations
	lockStem   = "migrate"    // the lock key; SQLite's lock table is <prefix>migrate_lock
)

// ErrSchemaTooNew means the database records a sqlstore migration this code
// does not know: a newer release migrated it. Running older code against it
// could write rows the new schema does not expect, so Migrate and Start
// refuse it. The error is a *SchemaTooNewError naming the migrations.
var ErrSchemaTooNew = errors.New("sqlstore: the database schema is newer than this code")

// SchemaTooNewError names the migrations the database has that this code
// does not. It matches ErrSchemaTooNew under errors.Is.
type SchemaTooNewError struct {
	Unknown []string
}

func (e *SchemaTooNewError) Error() string {
	return ErrSchemaTooNew.Error() + ": unknown migration(s) " + strings.Join(e.Unknown, ", ") + "; upgrade this client"
}

// Is makes errors.Is(err, ErrSchemaTooNew) true.
func (e *SchemaTooNewError) Is(target error) bool { return target == ErrSchemaTooNew }

// migrations is every schema change, in the order it applies. It is the one
// place the schema's history lives.
//
// Never edit a migration once it has been released: the ledger stores each
// one's checksum, and an edited script fails Migrate with
// migrate.ErrChecksumMismatch on every database that already applied it.
// Change the schema by appending a new, numbered migration. Each script must
// also be safe to run twice (IF NOT EXISTS forms), because MySQL commits DDL
// implicitly: a failure part-way leaves the change without its ledger row and
// the next Migrate runs it again.
func (s *store) migrations() []migrate.Migration {
	return []migrate.Migration{
		{Owner: s.owner(MigrationOwner), Name: "001_initial", Script: s.script(s.ddl())},
		{Owner: s.owner(MigrationOwner), Name: "002_schedule_misfire", Script: s.script(s.render(misfireDefs(typesFor(s.engine))))},
		{Owner: s.owner(MigrationOwner), Name: "003_queues_admin", Script: s.script(append(s.render(adminDefs(typesFor(s.engine))), backfillRunExt))},
		{Owner: s.owner(MigrationOwner), Name: "004_run_states_chain", Script: s.script(append(s.render(statesChainDefs(typesFor(s.engine))), discardedStatus))},
		// 005's backfill decodes each run's JSON labels, which no statement
		// does portably, so it runs in Go: Client.IndexLabels, called by
		// Migrate after this script.
		{Owner: s.owner(MigrationOwner), Name: "005_run_labels", Script: s.script(s.render(labelDefs(typesFor(s.engine))))},
		{Owner: s.owner(MigrationOwner), Name: "006_schedule_history", Script: s.script(append(s.render(historyDefs(typesFor(s.engine))), backfillScheduleLast))},
		{Owner: s.owner(MigrationOwner), Name: "007_leader_archive", Script: s.script(s.render(append(leaderDefs(typesFor(s.engine)), archiveDefs(typesFor(s.engine))...)))},
		{Owner: s.owner(MigrationOwner), Name: "008_run_info_nodes", Script: s.script(append(s.render(runInfoDefs(typesFor(s.engine))), backfillRunInfo))},
		{Owner: s.owner(MigrationOwner), Name: "009_schedule_tenant", Script: s.script(s.render(scheduleTenantDefs(typesFor(s.engine))))},
	}
}

// UniqueMigrationOwner is the ledger owner of UniqueStore's migrations: a
// list of its own, so NewUniqueStore(...).Migrate on a database with no
// sqlstore queue creates only the lock table, while Client.Migrate applies
// both.
const UniqueMigrationOwner = "ubgo/workflow/sqlstore/unique"

// uniqueMigrations is every UniqueStore schema change, in order, under the
// same rules as migrations: never edit a released one, append a new one.
func (s *store) uniqueMigrations() []migrate.Migration {
	return []migrate.Migration{
		{Owner: s.owner(UniqueMigrationOwner), Name: "001_unique", Script: s.script(s.render(uniqueDefs(typesFor(s.engine))))},
		{Owner: s.owner(UniqueMigrationOwner), Name: "002_unique_holder", Script: s.script(s.render(uniqueHolderDefs(typesFor(s.engine))))},
	}
}

// allMigrations is every list Client.Migrate applies: the queue's, then
// UniqueStore's, DebounceStore's and OutputStore's.
func (s *store) allMigrations() []migrate.Migration {
	all := append(append(s.migrations(), s.uniqueMigrations()...), s.debounceMigrations()...)
	all = append(all, s.outputMigrations()...)
	all = append(all, s.journalMigrations()...)
	return append(all, s.bucketMigrations()...)
}

// script turns statement texts into a transactional ddl.Script, table names
// resolved for this prefix and dialect. Every statement here is
// transactional on Postgres and SQLite, so a migration and its ledger row
// commit together there.
func (s *store) script(stmts []string) ddl.Script {
	out := make(ddl.Script, len(stmts))
	for i, q := range stmts {
		out[i] = ddl.Statement{SQL: s.sqlText(q), Transactional: true}
	}
	return out
}

// ledger is this store's migration ledger, serialised by the engine's own
// lock (an advisory lock on Postgres, a named lock on MySQL, a lock table on
// SQLite). The ledger table is never schema-qualified (sqlb's ledger has no
// schema); under WithSchema the lock key carries the schema, so migrating
// one schema does not wait on another.
func (s *store) ledger() (migrate.Ledger, error) {
	key := s.prefix + lockStem
	if s.schema != "" {
		key = s.schema + "." + key
	}
	l, err := migrate.NewLedger(s.d, s.prefix+ledgerStem, migrate.EngineLock(s.d, key))
	if err != nil {
		return migrate.Ledger{}, fmt.Errorf("sqlstore: migration ledger: %w", err)
	}
	return l, nil
}

// Migrate brings the schema up to this code's version: it applies, once
// each and in order, every migration the ledger has not recorded, under the
// engine's migration lock, so concurrent Migrate calls from several
// processes are safe (on SQLite the ledger begins its transactions
// IMMEDIATE, so a second caller waits rather than failing). It refuses with
// ErrSchemaTooNew a database that a newer release already migrated, and with
// migrate.ErrChecksumMismatch one whose recorded migration differs from this
// code's. It never drops anything, and Start never migrates: running it is
// the deploy's decision. It applies UniqueStore's and DebounceStore's lists
// too, so Client.UniqueStore and Client.DebounceStore need no Migrate of
// their own. Last it runs IndexLabels, the label index backfill of
// migration 005, which finds nothing to do once the index is current.
func (c *Client) Migrate(ctx context.Context) error {
	if err := c.st.migrate(ctx, c.st.allMigrations()); err != nil {
		return err
	}
	_, err := c.IndexLabels(ctx)
	return err
}

// migrate applies ms (each owner's full list) under the ledger's lock,
// refusing a database a newer release migrated.
func (s *store) migrate(ctx context.Context, ms []migrate.Migration) error {
	// The early check names the unknown migrations; the ledger's own
	// RefuseUnknown repeats it under the lock, for a newer release that
	// migrates between the two.
	if err := s.checkVersion(ctx, ms); err != nil {
		return err
	}
	l, err := s.ledger()
	if err != nil {
		return err
	}
	if _, err := l.RefuseUnknown().ApplyDB(ctx, s.db, ms...); err != nil {
		if errors.Is(err, migrate.ErrUnknownMigration) {
			if verr := s.checkVersion(ctx, ms); verr != nil {
				return verr
			}
		}
		return fmt.Errorf("sqlstore: migrate: %w", s.mark(err))
	}
	return nil
}

// checkVersion fails with a *SchemaTooNewError when the ledger records a
// sqlstore migration this code does not have. A database without a ledger
// has applied none and passes; checkSchema reports its missing tables.
func (c *Client) checkVersion(ctx context.Context) error {
	return c.st.checkVersion(ctx, c.st.allMigrations())
}

// checkVersion is Client.checkVersion for the owners of ms: a recorded
// migration of one of them that ms lacks is unknown. sqlstore's own
// migrations are named bare; another owner's carry its last path element
// (unique/002_x), so the error says whose they are.
func (s *store) checkVersion(ctx context.Context, ms []migrate.Migration) error {
	l, err := s.ledger()
	if err != nil {
		return err
	}
	rows, err := l.AppliedDB(ctx, s.db)
	if err != nil {
		return fmt.Errorf("sqlstore: read the migration ledger %s%s: %w", s.prefix, ledgerStem, s.mark(err))
	}
	known := map[[2]string]bool{}
	owners := map[string]bool{}
	for _, m := range ms {
		known[[2]string{m.Owner, m.Name}] = true
		owners[m.Owner] = true
	}
	var unknown []string
	for _, r := range rows {
		if owners[r.Owner] && !known[[2]string{r.Owner, r.Name}] {
			name := r.Name
			if r.Owner != s.owner(MigrationOwner) {
				name = strings.TrimSuffix(r.Owner[strings.LastIndex(r.Owner, "/")+1:], "@"+s.schema) + "/" + r.Name
			}
			unknown = append(unknown, name)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return &SchemaTooNewError{Unknown: unknown}
	}
	return nil
}

// MigrationScript is one migration as SQL text: the exact statements
// Migrate runs for it, in order, with table names resolved for the
// options' prefix and schema. Owner and Name are what Migrate records in
// its ledger.
type MigrationScript struct {
	Owner, Name string
	Statements  []string
}

// MigrationScripts returns every migration Client.Migrate applies, in
// order, as SQL text for dialect d and the table options among opts
// (WithTablePrefix, WithSchema; the others are ignored): sqlstore's list
// (owner MigrationOwner), then UniqueStore's (UniqueMigrationOwner) and
// DebounceStore's (DebounceMigrationOwner); take only a store's own list
// for a database that holds just that store. It is for teams whose DDL goes through goose,
// Atlas, Flyway, Liquibase or a DBA review, or whose app user has no DDL
// rights: apply the statements in your own pipeline, recording each
// (Owner, Name) there.
//
// The text is frozen per release exactly as Migrate's is, and every
// statement is safe to run twice (IF NOT EXISTS forms). Start does not
// need Migrate's ledger: it checks that every table is readable, so a
// schema applied this way starts as is. Migration 005's backfill is Go, not
// SQL: run Client.IndexLabels once after applying it. Migrate is still safe to run later
// on such a database: it re-runs each script (a no-op) and records it. A
// release that adds a migration adds it here too, and Start fails on its
// missing table until it is applied.
func MigrationScripts(d sqlb.Dialect, opts ...Option) ([]MigrationScript, error) {
	c, err := build(nil, d, opts)
	if err != nil {
		return nil, err
	}
	var out []MigrationScript
	for _, m := range c.st.allMigrations() {
		ms := MigrationScript{Owner: m.Owner, Name: m.Name}
		for _, st := range m.Script {
			ms.Statements = append(ms.Statements, st.SQL)
		}
		out = append(out, ms)
	}
	return out, nil
}
