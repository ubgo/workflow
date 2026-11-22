package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/workflow"
)

// PromiseStore is a durable workflow.PromiseStore: each settled promise
// is a row of the journal table (<prefix>journal) under a run id no run
// ever has, so it needs no table or migration of its own beyond
// JournalStore's. Settle is an insert decided by the table's primary key:
// of two processes settling one promise at once, one insert succeeds and
// the other reads the winner's value, so a promise settles once across
// every process sharing the database.
//
//	store := c.PromiseStore()
//	err := workflow.ResolvePromise(ctx, c, store, approvalID, []byte(`"approved"`))
//
// Rows stay until Delete (Update deletes its reply once read); an app that
// settles promises nobody reads deletes them itself.
type PromiseStore struct {
	st *store
}

var _ workflow.PromiseStore = (*PromiseStore)(nil)

// promiseRun is the journal run id promises are kept under. It starts with
// the reserved internal prefix, which no driver's run id does, so
// JournalStore's Forget of a real run never deletes a promise.
//
//ds:def id=sql-promiserun-z394qrzs owner=@khanakia stability=stable
const promiseRun = "ubgo.workflow/promises"

// NewPromiseStore returns the promise store over db for dialect d. Of the
// Options, WithTablePrefix, WithSchema and WithErrorClassifier apply; the
// others configure a Client and are ignored. Call Migrate before the first
// Settle.
//
//ds:def id=sql-newpromisestore-cpmkmayx owner=@khanakia stability=stable
func NewPromiseStore(db *sql.DB, d sqlb.Dialect, opts ...Option) (*PromiseStore, error) {
	c, err := New(db, d, opts...)
	if err != nil {
		return nil, err
	}
	return c.PromiseStore(), nil
}

// PromiseStore returns the promise store in this client's database and
// prefix. Client.Migrate creates its table (the journal's).
//
//ds:def id=sql-client-promisestore-fw6wxfdm owner=@khanakia stability=stable
func (c *Client) PromiseStore() *PromiseStore { return &PromiseStore{st: c.st} }

// Migrate creates the journal table the promises live in (JournalStore's
// migration list), and nothing else.
//
//ds:def id=sql-promisestore-migrate-vbtntpgy owner=@khanakia stability=stable
func (s *PromiseStore) Migrate(ctx context.Context) error {
	return s.st.migrate(ctx, s.st.journalMigrations())
}

// Settle implements workflow.PromiseStore.
func (s *PromiseStore) Settle(ctx context.Context, id string, val []byte) ([]byte, bool, error) {
	if val == nil {
		val = []byte{}
	}
	_, err := s.st.exec(ctx, s.st.db, "INSERT INTO {journal} (run_id, key_hash, jkey, val) VALUES (?, ?, ?, ?)", promiseRun, keyHash(id), id, val)
	if err == nil {
		return append([]byte{}, val...), true, nil
	}
	if !s.st.isUnique(err) {
		return nil, false, fmt.Errorf("sqlstore: promise settle: %w", err)
	}
	stored, ok, gerr := s.Get(ctx, id)
	if gerr != nil {
		return nil, false, gerr
	}
	if !ok {
		// Deleted between the insert and the read: the insert lost to a
		// value that is gone, so try once more.
		return s.Settle(ctx, id, val)
	}
	return stored, false, nil
}

// Get implements workflow.PromiseStore.
func (s *PromiseStore) Get(ctx context.Context, id string) ([]byte, bool, error) {
	var val []byte
	err := s.st.one(ctx, s.st.db, "SELECT val FROM {journal} WHERE run_id = ? AND key_hash = ?", []any{promiseRun, keyHash(id)}, &val)
	if errors.Is(err, sqlb.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("sqlstore: promise get: %w", err)
	}
	return append([]byte{}, val...), true, nil
}

// Delete implements workflow.PromiseStore.
func (s *PromiseStore) Delete(ctx context.Context, id string) error {
	if _, err := s.st.exec(ctx, s.st.db, "DELETE FROM {journal} WHERE run_id = ? AND key_hash = ?", promiseRun, keyHash(id)); err != nil {
		return fmt.Errorf("sqlstore: promise delete: %w", err)
	}
	return nil
}
