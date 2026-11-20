package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/sqlb/contrib/migrate"
	"github.com/ubgo/workflow/middleware"
)

// AggregateStore is a durable middleware.AggregateStore: each pending item
// is a row of <prefix>aggregate_items and each group with items a row of
// <prefix>aggregate_groups, so middleware.Aggregate collects a group's
// items from every process sharing the database (a web tier with several
// replicas) and the flush runs of every worker take from the same rows.
// Like DebounceStore it needs no running Client: build it with
// NewAggregateStore over any *sql.DB, or take Client.AggregateStore.
//
//	store := c.AggregateStore()
//	agg, err := middleware.AggregatePlugin(c, store, middleware.AggregateConfig{...})
//
// Append and Take are atomic per group on SQLite, Postgres and MySQL alike:
// each first bumps the group's row, which locks it (a row lock on Postgres
// and MySQL, the single writer on SQLite), then reads and writes the
// group's items in the same transaction. So the count Append returns is
// exact, and of two Takes at once the second waits and then finds only
// what the first left: no item is returned twice, and none is skipped. No
// statement needs RETURNING or SKIP LOCKED, which is why the same text runs
// on MySQL. When a group has no row yet, the insert's primary key decides:
// of two processes inserting at once, the loser's transaction is tried
// again and finds the winner's row. A group's row outlives its items, so
// a busy group is locked by an update rather than re-inserted each time
// it empties; PruneEmpty removes the rows of empty groups.
//
// Items are kept as given (a BLOB column); the time Append stores is the
// caller's now, never the database's clock.
type AggregateStore struct {
	st    *store
	names *strings.Replacer // {aggregate_items}, {aggregate_groups} -> quoted names
}

var _ middleware.AggregateStore = (*AggregateStore)(nil)

// AggregateMigrationOwner is the ledger owner of AggregateStore's
// migrations: a list of its own, applied by AggregateStore.Migrate, so a
// database with no sqlstore queue gets only the two aggregate tables.
// Client.Migrate does not apply it: call Migrate on the store.
const AggregateMigrationOwner = "ubgo/workflow/sqlstore/aggregate"

// The aggregate tables' stems. They are resolved by the store itself, not
// by the client's table list, because the tables belong to this store's
// own migration list.
const (
	tblAggItems  = "aggregate_items"
	tblAggGroups = "aggregate_groups"
)

// aggregateMigrations is every AggregateStore schema change, in order,
// under the same rules as migrations: never edit a released one, append a
// new one.
func (s *AggregateStore) aggregateMigrations() []migrate.Migration {
	stmts := s.st.render(aggregateDefs(typesFor(s.st.engine)))
	for i, q := range stmts {
		stmts[i] = s.names.Replace(q)
	}
	return []migrate.Migration{
		{Owner: s.st.owner(AggregateMigrationOwner), Name: "001_aggregate", Script: s.st.script(stmts)},
	}
}

// aggregateDefs is AggregateStore's tables. Their rendered text is frozen
// once released, like ddl.
func aggregateDefs(t sqlTypes) []tableDef {
	return []tableDef{
		{stem: tblAggGroups, lines: []string{
			// group_hash is the SHA-256 of the group in hex, as in
			// unique_locks: a fixed-width primary key whatever the group's
			// length. The primary key is what makes two first Appends of
			// one group race to one row (ADR-0003, a structural
			// constraint).
			"group_hash " + t.id + " NOT NULL PRIMARY KEY",
			// gkey is the group as given, for an operator reading the
			// table.
			"gkey " + t.text + " NOT NULL",
			// ver is bumped first by every Append and Take, which is how
			// they lock the group before reading its items.
			"ver " + t.int + " NOT NULL",
		}},
		{stem: tblAggItems, lines: []string{
			// id orders a group's items: rows are inserted under the
			// group's lock, so a later item always has a larger id.
			"id " + t.serial,
			"group_hash " + t.id + " NOT NULL",
			// item is the payload as Append got it.
			"item " + t.blob + " NOT NULL",
			// at_ns is Append's now in Unix nanoseconds; the oldest
			// pending one is the group's first.
			"at_ns " + t.int + " NOT NULL",
		}, indexes: []index{{"group_id", "group_hash, id"}}},
	}
}

// NewAggregateStore returns the aggregate store over db for dialect d. Of
// the Options, WithTablePrefix, WithSchema, WithTxAttempts and
// WithErrorClassifier apply; the others configure a Client and are
// ignored. Call Migrate before the first Append.
func NewAggregateStore(db *sql.DB, d sqlb.Dialect, opts ...Option) (*AggregateStore, error) {
	c, err := New(db, d, opts...)
	if err != nil {
		return nil, err
	}
	return c.AggregateStore(), nil
}

// AggregateStore returns the aggregate store in this client's database
// and prefix. Its tables are created by the store's Migrate, not by
// Client.Migrate.
func (c *Client) AggregateStore() *AggregateStore {
	st := c.st
	return &AggregateStore{st: st, names: strings.NewReplacer(
		"{"+tblAggItems+"}", st.table(tblAggItems),
		"{"+tblAggGroups+"}", st.table(tblAggGroups),
	)}
}

// Migrate creates the aggregate tables (their own migration list, under
// the same ledger and lock as Client.Migrate), and nothing else.
func (s *AggregateStore) Migrate(ctx context.Context) error {
	return s.st.migrate(ctx, s.aggregateMigrations())
}

// MigrationScripts returns the store's migrations as SQL text, with table
// names resolved for its prefix and schema, for teams whose DDL goes
// through their own pipeline (see the package's MigrationScripts).
func (s *AggregateStore) MigrationScripts() []MigrationScript {
	var out []MigrationScript
	for _, m := range s.aggregateMigrations() {
		ms := MigrationScript{Owner: m.Owner, Name: m.Name}
		for _, st := range m.Script {
			ms.Statements = append(ms.Statements, st.SQL)
		}
		out = append(out, ms)
	}
	return out
}

// Append implements middleware.AggregateStore, atomically per group (see
// AggregateStore). A transaction that loses the first-insert race or a
// lock conflict is tried again, up to WithTxAttempts.
func (s *AggregateStore) Append(ctx context.Context, group string, item []byte, now time.Time) (int, time.Time, error) {
	h := keyHash(group)
	if item == nil {
		item = []byte{} // NOT NULL: an empty item is stored as empty
	}
	var count, first int64
	err := s.locked(ctx, h, group, func(tx *sql.Tx) error {
		if _, err := s.st.exec(ctx, tx, s.q("INSERT INTO {aggregate_items} (group_hash, item, at_ns) VALUES (?, ?, ?)"), h, item, now.UnixNano()); err != nil {
			return err
		}
		if err := s.st.one(ctx, tx, s.q("SELECT COUNT(*), MIN(at_ns) FROM {aggregate_items} WHERE group_hash = ?"), []any{h}, &count, &first); err != nil {
			return fmt.Errorf("sqlstore: count aggregate items: %w", err)
		}
		return nil
	})
	if err != nil {
		return 0, time.Time{}, err
	}
	return int(count), time.Unix(0, first).UTC(), nil
}

// Take implements middleware.AggregateStore, atomically per group (see
// AggregateStore): it reads up to limit of the group's oldest items and
// deletes them in the same locked transaction. limit of zero or less takes
// them all. The group's row stays (PruneEmpty removes rows of empty
// groups), so a busy group is locked by an update, never re-inserted.
func (s *AggregateStore) Take(ctx context.Context, group string, limit int) ([][]byte, error) {
	h := keyHash(group)
	var items [][]byte
	err := s.locked(ctx, h, group, func(tx *sql.Tx) error {
		items = nil
		q, args := "SELECT id, item FROM {aggregate_items} WHERE group_hash = ? ORDER BY id", []any{h}
		if limit > 0 {
			q, args = q+" LIMIT ?", append(args, limit)
		}
		var id, last int64
		var item []byte
		if err := s.st.each(ctx, tx, s.q(q), args, func() error {
			items = append(items, append([]byte(nil), item...))
			last = id
			return nil
		}, &id, &item); err != nil {
			return fmt.Errorf("sqlstore: read aggregate items: %w", err)
		}
		if len(items) == 0 {
			return nil
		}
		// Every item of the group up to last is one of those read: they
		// were read in id order under the group's lock.
		_, err := s.st.exec(ctx, tx, s.q("DELETE FROM {aggregate_items} WHERE group_hash = ? AND id <= ?"), h, last)
		return err
	})
	if err != nil {
		return nil, err
	}
	return items, nil
}

// PruneEmpty deletes the rows of groups that hold no item and returns how
// many. Such rows change nothing but the table's size (one per group ever
// used), so run it now and then; it is safe beside Appends and Takes,
// because either one inserts a group's row again when it is missing.
func (s *AggregateStore) PruneEmpty(ctx context.Context) (int64, error) {
	return s.st.execN(ctx, s.st.db, s.q("DELETE FROM {aggregate_groups} WHERE group_hash NOT IN (SELECT group_hash FROM {aggregate_items})"))
}

// locked runs fn in a transaction that holds group h's row: the row is
// bumped first, which locks it, or inserted when the group has none (its
// first use, or after PruneEmpty). A transaction that loses the
// first-insert race or a lock conflict is tried again, up to
// WithTxAttempts.
func (s *AggregateStore) locked(ctx context.Context, h, group string, fn func(tx *sql.Tx) error) error {
	var err error
	for range s.st.txAttempts {
		err = s.st.tx(ctx, func(tx *sql.Tx) error {
			n, err := s.st.execN(ctx, tx, s.q("UPDATE {aggregate_groups} SET ver = ver + 1 WHERE group_hash = ?"), h)
			if err != nil {
				return err
			}
			if n == 0 {
				if _, err := s.st.exec(ctx, tx, s.q("INSERT INTO {aggregate_groups} (group_hash, gkey, ver) VALUES (?, ?, 0)"), h, group); err != nil {
					return err
				}
			}
			return fn(tx)
		})
		if !s.st.isUnique(err) || ctx.Err() != nil {
			break
		}
	}
	return err
}

// q resolves the store's own table stems; the client's sqlText then
// resolves placeholders.
func (s *AggregateStore) q(stmt string) string { return s.names.Replace(stmt) }
