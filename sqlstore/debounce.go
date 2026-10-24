package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/sqlb/contrib/migrate"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/middleware"
)

// DebounceStore is a durable middleware.DebounceStore: each debounce key's
// pending burst is a row of <prefix>debounce, so middleware.Debounce
// debounces a key enqueued from every process sharing the database (a web
// tier with several replicas), not one burst per process. Like UniqueStore
// it needs no running Client: build it with NewDebounceStore over any
// *sql.DB (an app on Hatchet with a database), or take
// Client.DebounceStore.
//
//	mw, err := middleware.Debounce(c, nil, byProduct, 10*time.Second, time.Minute,
//		middleware.WithDebounceStore(store))
//
// Swap is atomic across processes: it first bumps the key's row, which
// locks it (a row lock on Postgres and MySQL, the single writer on
// SQLite), then reads the burst it replaces and writes the new one in the
// same transaction. When the key has no row, the insert's primary key
// decides: of two processes inserting at once, the loser's transaction is
// tried again and finds the winner's row, so every replaced run is
// returned to exactly one caller and is cancelled once.
//
// Rows whose run has started (RunAt passed) are over; Get and Swap still
// return them (Debounce treats a past RunAt as no burst), and a running
// Client's dispatcher deletes them, or PruneExpired for an app with no
// Client.
//
// This package imports middleware only for the DebounceBurst type the
// interface names; middleware imports nothing of sqlstore.
type DebounceStore struct {
	st  *store
	clk clock.Clock
}

var _ middleware.DebounceStore = (*DebounceStore)(nil)

// DebounceMigrationOwner is the ledger owner of DebounceStore's migrations:
// a list of its own, so NewDebounceStore(...).Migrate on a database with no
// sqlstore queue creates only the debounce table, while Client.Migrate
// applies it too.
const DebounceMigrationOwner = "ubgo/workflow/sqlstore/debounce"

// debounceMigrations is every DebounceStore schema change, in order, under
// the same rules as migrations: never edit a released one, append a new one.
func (s *store) debounceMigrations() []migrate.Migration {
	return []migrate.Migration{
		{Owner: s.owner(DebounceMigrationOwner), Name: "001_debounce", Script: s.script(s.render(debounceDefs(typesFor(s.engine))))},
	}
}

// debounceDefs is DebounceStore's table. Its rendered text is frozen once
// released, like ddl.
func debounceDefs(t sqlTypes) []tableDef {
	return []tableDef{{stem: tblDebounce, lines: []string{
		// key_hash is the SHA-256 of the key in hex, as in unique_locks: a
		// fixed-width primary key whatever the key's length. The primary key
		// is what makes two first Swaps of one key race to one row
		// (ADR-0003, a structural constraint).
		"key_hash " + t.id + " NOT NULL PRIMARY KEY",
		// dkey is the key as given, for an operator reading the table.
		"dkey " + t.text + " NOT NULL",
		// first_at and run_at are DebounceBurst.First and RunAt in Unix
		// nanoseconds.
		"first_at " + t.int + " NOT NULL",
		"run_at " + t.int + " NOT NULL",
		// run_id and scheduled_id are DebounceBurst.Ref; empty when unset.
		"run_id " + t.name + " NOT NULL",
		"scheduled_id " + t.name + " NOT NULL",
		// ver is bumped first by every Swap, which is how Swap locks the
		// row before reading it.
		"ver " + t.int + " NOT NULL",
	}, indexes: []index{{"run_at", "run_at"}}}}
}

// NewDebounceStore returns the debounce store over db for dialect d. Of the
// Options, WithTablePrefix, WithSchema, WithClock (pruning), WithTxAttempts
// and WithErrorClassifier apply; the others configure a Client and are
// ignored. Call Migrate before the first Swap.
func NewDebounceStore(db *sql.DB, d sqlb.Dialect, opts ...Option) (*DebounceStore, error) {
	c, err := New(db, d, opts...)
	if err != nil {
		return nil, err
	}
	return c.DebounceStore(), nil
}

// DebounceStore returns the debounce store in this client's database,
// prefix and scheduling clock. Client.Migrate creates its table.
func (c *Client) DebounceStore() *DebounceStore { return &DebounceStore{st: c.st, clk: c.clk} }

// Migrate creates the debounce table (its own migration list, under the
// same ledger and lock as Client.Migrate), and nothing else.
func (s *DebounceStore) Migrate(ctx context.Context) error {
	return s.st.migrate(ctx, s.st.debounceMigrations())
}

// debounceCols is the column list a burst is read with, in scan order.
const debounceCols = "first_at, run_at, run_id, scheduled_id"

// burstRow scans one burst.
type burstRow struct {
	first, runAt     int64
	runID, scheduled string
}

func (r *burstRow) targets() []any { return []any{&r.first, &r.runAt, &r.runID, &r.scheduled} }

func (r burstRow) burst() middleware.DebounceBurst {
	return middleware.DebounceBurst{
		First: time.Unix(0, r.first).UTC(),
		RunAt: time.Unix(0, r.runAt).UTC(),
		Ref:   workflow.RunRef{RunID: workflow.RunID(r.runID), ScheduledID: workflow.ScheduledID(r.scheduled)},
	}
}

// Get implements middleware.DebounceStore.
func (s *DebounceStore) Get(ctx context.Context, key string) (middleware.DebounceBurst, bool, error) {
	var r burstRow
	err := s.st.one(ctx, s.st.db, "SELECT "+debounceCols+" FROM {debounce} WHERE key_hash = ?", []any{keyHash(key)}, r.targets()...)
	if errors.Is(err, sqlb.ErrNoRows) {
		return middleware.DebounceBurst{}, false, nil
	}
	if err != nil {
		return middleware.DebounceBurst{}, false, fmt.Errorf("sqlstore: read debounce key: %w", err)
	}
	return r.burst(), true, nil
}

// Swap implements middleware.DebounceStore, atomically across processes
// (see DebounceStore). A transaction that loses the first-insert race or a
// lock conflict is tried again, up to WithTxAttempts.
func (s *DebounceStore) Swap(ctx context.Context, key string, b middleware.DebounceBurst) (middleware.DebounceBurst, bool, error) {
	var prev burstRow
	var found bool
	var err error
	for range s.st.txAttempts {
		found = false
		err = s.st.tx(ctx, func(tx *sql.Tx) error {
			h := keyHash(key)
			n, err := s.st.execN(ctx, tx, "UPDATE {debounce} SET ver = ver + 1 WHERE key_hash = ?", h)
			if err != nil {
				return err
			}
			args := []any{b.First.UnixNano(), b.RunAt.UnixNano(), string(b.Ref.RunID), string(b.Ref.ScheduledID)}
			if n == 0 {
				_, err := s.st.exec(ctx, tx, "INSERT INTO {debounce} (key_hash, dkey, first_at, run_at, run_id, scheduled_id, ver) VALUES (?, ?, ?, ?, ?, ?, 0)",
					append([]any{h, key}, args...)...)
				return err
			}
			if err := s.st.one(ctx, tx, "SELECT "+debounceCols+" FROM {debounce} WHERE key_hash = ?", []any{h}, prev.targets()...); err != nil {
				return fmt.Errorf("sqlstore: read debounce key: %w", err)
			}
			found = true
			_, err = s.st.exec(ctx, tx, "UPDATE {debounce} SET first_at = ?, run_at = ?, run_id = ?, scheduled_id = ? WHERE key_hash = ?",
				append(args, h)...)
			return err
		})
		if !s.st.isUnique(err) || ctx.Err() != nil {
			break
		}
	}
	if err != nil {
		return middleware.DebounceBurst{}, false, err
	}
	if !found {
		return middleware.DebounceBurst{}, false, nil
	}
	return prev.burst(), true, nil
}

// PruneExpired deletes every burst whose run has started and returns how
// many. Such rows never change what Debounce does, so this only bounds the
// table's size; a running Client does it on its dispatcher's housekeeping.
func (s *DebounceStore) PruneExpired(ctx context.Context) (int64, error) {
	return s.st.execN(ctx, s.st.db, "DELETE FROM {debounce} WHERE run_at <= ?", s.clk.Now().UnixNano())
}
