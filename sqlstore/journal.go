package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/sqlb/contrib/migrate"
	"github.com/ubgo/workflow"
)

// JournalStore is a durable workflow.JournalStore: a run's memoised results
// (workflow.Memo) are rows of <prefix>journal, so a step retried after the
// process died, on any node sharing the database, gets its stored results
// back. Like UniqueStore it needs no running Client: build it with
// NewJournalStore over any *sql.DB (an app on Hatchet with a database), or
// take Client.JournalStore.
//
//	plugin := workflow.Journal(store)
//
// Save of an existing key replaces it, in one transaction (an update, then
// an insert when no row was there; a concurrent first Save of one key is
// decided by the primary key and the loser's transaction is tried again,
// up to WithTxAttempts).
type JournalStore struct {
	st *store
}

var _ workflow.JournalStore = (*JournalStore)(nil)

// JournalMigrationOwner is the ledger owner of JournalStore's migrations:
// a list of its own, so NewJournalStore(...).Migrate creates only the
// journal table, while Client.Migrate applies it too.
const JournalMigrationOwner = "ubgo/workflow/sqlstore/journal"

// journalMigrations is every JournalStore schema change, in order, under
// the same rules as migrations: never edit a released one, append a new one.
func (s *store) journalMigrations() []migrate.Migration {
	return []migrate.Migration{
		{Owner: s.owner(JournalMigrationOwner), Name: "001_journal", Script: s.script(s.render(journalDefs(typesFor(s.engine))))},
	}
}

// journalDefs is JournalStore's table. Its rendered text is frozen once
// released, like ddl.
func journalDefs(t sqlTypes) []tableDef {
	return []tableDef{{stem: tblJournal, lines: []string{
		"run_id " + t.id + " NOT NULL",
		// jkey is the key Memo built (step name length, step, memo name).
		// It is hashed like unique_locks' keys: the key is as long as the
		// step name and the memo name together, past any indexable width.
		"key_hash " + t.id + " NOT NULL",
		"jkey " + t.text + " NOT NULL",
		"val " + t.blob + " NOT NULL",
		// One entry per (run, key): Save of a key replaces its entry, and
		// two first Saves of one key race to one row (ADR-0003, a
		// structural constraint).
		"PRIMARY KEY (run_id, key_hash)",
	}}}
}

// NewJournalStore returns the journal store over db for dialect d. Of the
// Options, WithTablePrefix, WithSchema, WithTxAttempts and
// WithErrorClassifier apply; the others configure a Client and are
// ignored. Call Migrate before the first Save.
func NewJournalStore(db *sql.DB, d sqlb.Dialect, opts ...Option) (*JournalStore, error) {
	c, err := New(db, d, opts...)
	if err != nil {
		return nil, err
	}
	return c.JournalStore(), nil
}

// JournalStore returns the journal store in this client's database and
// prefix. Client.Migrate creates its table.
func (c *Client) JournalStore() *JournalStore { return &JournalStore{st: c.st} }

// Migrate creates the journal table (its own migration list, under the same
// ledger and lock as Client.Migrate), and nothing else.
func (s *JournalStore) Migrate(ctx context.Context) error {
	return s.st.migrate(ctx, s.st.journalMigrations())
}

// Load implements workflow.JournalStore.
func (s *JournalStore) Load(ctx context.Context, run workflow.RunID, key string) ([]byte, bool, error) {
	var val []byte
	err := s.st.one(ctx, s.st.db, "SELECT val FROM {journal} WHERE run_id = ? AND key_hash = ?", []any{string(run), keyHash(key)}, &val)
	if errors.Is(err, sqlb.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("sqlstore: journal load: %w", err)
	}
	return append([]byte{}, val...), true, nil
}

// Save implements workflow.JournalStore.
func (s *JournalStore) Save(ctx context.Context, run workflow.RunID, key string, val []byte) error {
	if val == nil {
		val = []byte{} // the column is NOT NULL; an empty value is a value
	}
	h := keyHash(key)
	var err error
	for range s.st.txAttempts {
		err = s.st.tx(ctx, func(tx *sql.Tx) error {
			n, err := s.st.execN(ctx, tx, "UPDATE {journal} SET val = ? WHERE run_id = ? AND key_hash = ?", val, string(run), h)
			if err != nil || n > 0 {
				return err
			}
			_, err = s.st.exec(ctx, tx, "INSERT INTO {journal} (run_id, key_hash, jkey, val) VALUES (?, ?, ?, ?)", string(run), h, key, val)
			return err
		})
		if !s.st.isUnique(err) || ctx.Err() != nil {
			break
		}
	}
	if err != nil {
		return fmt.Errorf("sqlstore: journal save: %w", err)
	}
	return nil
}

// Forget implements workflow.JournalStore.
func (s *JournalStore) Forget(ctx context.Context, run workflow.RunID) error {
	if _, err := s.st.exec(ctx, s.st.db, "DELETE FROM {journal} WHERE run_id = ?", string(run)); err != nil {
		return fmt.Errorf("sqlstore: journal forget: %w", err)
	}
	return nil
}
