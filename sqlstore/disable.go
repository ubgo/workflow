package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/sqlb/contrib/migrate"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/middleware"
)

// DisableStore is a durable middleware.DisableStore (WH-02): each key's
// count of consecutive failures and its disabled switch are a row of
// <prefix>disabled_endpoints, so middleware.AutoDisable counts the failures
// of every worker sharing the database and an endpoint switched off by one
// is off for all of them, until Enable. Like AggregateStore it needs no
// running Client: build it with NewDisableStore over any *sql.DB (an app
// on Hatchet or Redis with a database), or take Client.DisableStore.
//
//	store := c.DisableStore() // after store.Migrate(ctx)
//	mw := middleware.AutoDisable(store, byEndpoint, 5, alertOps)
//
// Fail is atomic across processes on SQLite, Postgres and MySQL alike: one
// upsert counts the failure, flips the switch when the count reaches after,
// and notes in the row whether this very statement flipped it, all
// computed from the row as it was before the statement (an INSERT ... ON
// CONFLICT DO UPDATE on Postgres and SQLite, INSERT ... ON DUPLICATE KEY
// UPDATE on MySQL, whose assignments are ordered so each reads the old
// values). The upsert holds the row's lock (the single writer on SQLite)
// until its transaction commits, and the flag is read back in that same
// transaction, so of any number of concurrent Fails exactly one reports
// newly true. Succeed and Enable are single statements; a key that never
// failed has no row, and Enable deletes the key's row.
//
// updated_at is the store's clock at the last write, for an operator
// reading the table; nothing decides by it.
//
//ds:def id=sql-disablestore-d9gwsxgk owner=@khanakia stability=stable
type DisableStore struct {
	st    *store
	clk   clock.Clock
	names *strings.Replacer // {disabled_endpoints} -> quoted name
}

var _ middleware.DisableStore = (*DisableStore)(nil)

// DisableMigrationOwner is the ledger owner of DisableStore's migrations:
// a list of its own, applied by DisableStore.Migrate, so a database with
// no sqlstore queue gets only the one table. Client.Migrate does not
// apply it: call Migrate on the store.
//
//ds:def id=sql-disablemigrationowner-am6g4z2t owner=@khanakia stability=stable
const DisableMigrationOwner = "ubgo/workflow/sqlstore/disable"

// tblDisabled is DisableStore's table stem, resolved by the store itself
// because the table belongs to its own migration list.
//
//ds:def id=sql-tbldisabled-r4qv45z7 owner=@khanakia stability=stable
const tblDisabled = "disabled_endpoints"

// disableMigrations is every DisableStore schema change, in order, under
// the same rules as migrations: never edit a released one, append a new
// one.
//
//ds:def id=sql-disablestore-disablemigrations-qejfpcxy owner=@khanakia stability=stable
func (s *DisableStore) disableMigrations() []migrate.Migration {
	stmts := s.st.render(disableDefs(typesFor(s.st.engine)))
	for i, q := range stmts {
		stmts[i] = s.names.Replace(q)
	}
	return []migrate.Migration{
		{Owner: s.st.owner(DisableMigrationOwner), Name: "001_disable", Script: s.st.script(stmts)},
	}
}

// disableDefs is DisableStore's table. Its rendered text is frozen once
// released, like ddl.
func disableDefs(t sqlTypes) []tableDef {
	return []tableDef{{stem: tblDisabled, lines: []string{
		// key_hash is the SHA-256 of the key in hex, as in unique_locks: a
		// fixed-width primary key whatever the key's length (an endpoint
		// URL can be long). The primary key is what makes two first Fails
		// of one key race to one row (ADR-0003, a structural constraint).
		"key_hash " + t.id + " NOT NULL PRIMARY KEY",
		// dkey is the key as given, for an operator reading the table.
		"dkey " + t.text + " NOT NULL",
		// fails is the count of consecutive failures; Succeed sets it to 0.
		"fails " + t.int + " NOT NULL",
		// disabled is 1 while the key is switched off.
		"disabled " + t.int + " NOT NULL",
		// tripped is 1 when the row's last Fail is the one that switched
		// the key off: the upsert computes it from the old row, which is
		// how Fail learns newly without RETURNING (MySQL has none).
		"tripped " + t.int + " NOT NULL",
		// updated_at is the store's clock at the last write, Unix
		// nanoseconds; informational.
		"updated_at " + t.int + " NOT NULL",
	}}}
}

// NewDisableStore returns the disable store over db for dialect d. Of the
// Options, WithTablePrefix, WithSchema, WithClock (updated_at),
// WithTxAttempts and WithErrorClassifier apply; the others configure a
// Client and are ignored. Call Migrate before the first Fail.
//
//ds:def id=sql-newdisablestore-32zvw798 owner=@khanakia stability=stable
func NewDisableStore(db *sql.DB, d sqlb.Dialect, opts ...Option) (*DisableStore, error) {
	c, err := New(db, d, opts...)
	if err != nil {
		return nil, err
	}
	return c.DisableStore(), nil
}

// DisableStore returns the disable store in this client's database,
// prefix and scheduling clock. Its table is created by the store's
// Migrate, not by Client.Migrate.
//
//ds:def id=sql-client-disablestore-u46jz3ve owner=@khanakia stability=stable
func (c *Client) DisableStore() *DisableStore {
	return &DisableStore{st: c.st, clk: c.clk, names: strings.NewReplacer("{"+tblDisabled+"}", c.st.table(tblDisabled))}
}

// Migrate creates the disable table (its own migration list, under the
// same ledger and lock as Client.Migrate), and nothing else.
func (s *DisableStore) Migrate(ctx context.Context) error {
	return s.st.migrate(ctx, s.disableMigrations())
}

// MigrationScripts returns the store's migrations as SQL text, with table
// names resolved for its prefix and schema, for teams whose DDL goes
// through their own pipeline (see the package's MigrationScripts).
func (s *DisableStore) MigrationScripts() []MigrationScript {
	return scriptsOf(s.disableMigrations())
}

// scriptsOf turns migrations into MigrationScripts.
func scriptsOf(ms []migrate.Migration) []MigrationScript {
	var out []MigrationScript
	for _, m := range ms {
		ms := MigrationScript{Owner: m.Owner, Name: m.Name}
		for _, st := range m.Script {
			ms.Statements = append(ms.Statements, st.SQL)
		}
		out = append(out, ms)
	}
	return out
}

// Disabled implements middleware.DisableStore.
func (s *DisableStore) Disabled(ctx context.Context, key string) (bool, error) {
	var off int64
	err := s.st.one(ctx, s.st.db, s.q("SELECT disabled FROM {disabled_endpoints} WHERE key_hash = ?"), []any{keyHash(key)}, &off)
	if errors.Is(err, sqlb.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("sqlstore: read disabled key: %w", err)
	}
	return off == 1, nil
}

// failSet is the upsert's update half, shared by every engine. Each right
// side reads the row as it was before the statement: Postgres and SQLite
// evaluate every assignment against the old row, and MySQL, which
// evaluates them left to right with the values already assigned, gets
// them in an order where each reads only columns not yet assigned
// (tripped before disabled and fails, disabled before fails). The two
// placeholders are after, twice.
const failSet = `tripped = CASE WHEN {disabled_endpoints}.disabled = 0 AND {disabled_endpoints}.fails + 1 >= ? THEN 1 ELSE 0 END,
	disabled = CASE WHEN {disabled_endpoints}.disabled = 1 OR {disabled_endpoints}.fails + 1 >= ? THEN 1 ELSE 0 END,
	fails = {disabled_endpoints}.fails + 1,
	updated_at = ?`

// Fail implements middleware.DisableStore, atomically across processes
// (see DisableStore). A transaction that loses a lock race (SQLite busy,
// a deadlock) is tried again, up to WithTxAttempts.
//
//ds:def id=sql-disablestore-fail-za8eg5mc owner=@khanakia stability=stable
func (s *DisableStore) Fail(ctx context.Context, key string, after int) (bool, error) {
	h, now := keyHash(key), s.clk.Now().UnixNano()
	first := int64(0)
	if after <= 1 {
		first = 1 // the first failure already reaches after
	}
	upsert := "INSERT INTO {disabled_endpoints} (key_hash, dkey, fails, disabled, tripped, updated_at) VALUES (?, ?, 1, ?, ?, ?)\n"
	if s.st.engine == engineMySQL {
		upsert += "ON DUPLICATE KEY UPDATE " + failSet
	} else {
		upsert += "ON CONFLICT (key_hash) DO UPDATE SET " + failSet
	}
	var tripped int64
	err := s.st.tx(ctx, func(tx *sql.Tx) error {
		if _, err := s.st.exec(ctx, tx, s.q(upsert), h, key, first, first, now, after, after, now); err != nil {
			return err
		}
		return s.st.one(ctx, tx, s.q("SELECT tripped FROM {disabled_endpoints} WHERE key_hash = ?"), []any{h}, &tripped)
	})
	if err != nil {
		return false, fmt.Errorf("sqlstore: count failure: %w", err)
	}
	return tripped == 1, nil
}

// Succeed implements middleware.DisableStore: it resets the count of a key
// that has a row and leaves its switch as it is. A key with no row stays
// without one.
func (s *DisableStore) Succeed(ctx context.Context, key string) error {
	err := s.st.retry(ctx, func() error {
		_, err := s.st.exec(ctx, s.st.db, s.q("UPDATE {disabled_endpoints} SET fails = 0, updated_at = ? WHERE key_hash = ? AND fails <> 0"),
			s.clk.Now().UnixNano(), keyHash(key))
		return err
	})
	if err != nil {
		return fmt.Errorf("sqlstore: reset failures: %w", err)
	}
	return nil
}

// Enable implements middleware.DisableStore: it deletes the key's row, so
// the key is enabled with no failures counted.
func (s *DisableStore) Enable(ctx context.Context, key string) error {
	err := s.st.retry(ctx, func() error {
		_, err := s.st.exec(ctx, s.st.db, s.q("DELETE FROM {disabled_endpoints} WHERE key_hash = ?"), keyHash(key))
		return err
	})
	if err != nil {
		return fmt.Errorf("sqlstore: enable key: %w", err)
	}
	return nil
}

// q resolves the store's own table stem; the client's sqlText then
// resolves placeholders.
func (s *DisableStore) q(stmt string) string { return s.names.Replace(stmt) }
