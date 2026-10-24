package sqlstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"time"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/workflow/clock"
)

// UniqueStore is a durable middleware.UniqueStore: uniqueness locks as rows
// of <prefix>unique_locks, so middleware.Unique holds across every process
// sharing the database (ADR-0005). It does not need a running Client: build
// it with NewUniqueStore over any *sql.DB, which is how an app that runs on
// Hatchet gets durable uniqueness, or take Client.UniqueStore.
//
// The semantics are MemoryUnique's exactly: a key is held while now is
// before its expiry (now from the store's clock); Acquire on a held key
// reports false whoever the owner, including the holder itself; Acquire on
// a free or expired key takes it for owner until now+ttl (a ttl of zero or
// less takes it already expired, so the next Acquire wins); Release frees
// the key only for its owner, expired or not, and is a no-op otherwise.
//
// Acquire is atomic across processes: on Postgres and SQLite it is one
// upsert that takes the key only when absent or expired, decided by rows
// affected; on MySQL (whose rows-affected count is changed by the
// clientFoundRows setting sqlb's driver turns on) it deletes the key's
// expired row and inserts, and the primary key makes exactly one insert win
// (a deadlock between the two statements of two acquirers is retried).
// Expired rows are dropped by a running Client's dispatcher, or by
// PruneExpired for an app with no Client.
//
// The struct type is not named in middleware: the module's core cannot
// import sqlstore, so the interface is satisfied structurally.
type UniqueStore struct {
	st  *store
	clk clock.Clock
}

// NewUniqueStore returns the lock store over db for dialect d. Of the
// Options, WithTablePrefix (default "wf_", same rules as New) and WithClock
// (expiry; default clock.Real) apply; the others configure a Client and
// are ignored. Call Migrate before the first Acquire.
func NewUniqueStore(db *sql.DB, d sqlb.Dialect, opts ...Option) (*UniqueStore, error) {
	c, err := New(db, d, opts...)
	if err != nil {
		return nil, err
	}
	return c.UniqueStore(), nil
}

// UniqueStore returns the lock store in this client's database, prefix and
// scheduling clock. Client.Migrate creates its table.
func (c *Client) UniqueStore() *UniqueStore { return &UniqueStore{st: c.st, clk: c.clk} }

// Migrate creates the lock table (its own migration list, under the same
// ledger and lock as Client.Migrate), and nothing else, so a database with
// no sqlstore queue gets only this table.
func (u *UniqueStore) Migrate(ctx context.Context) error {
	return u.st.migrate(ctx, u.st.uniqueMigrations())
}

// keyHash is the primary key of key's row.
func keyHash(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// Acquire implements middleware.UniqueStore. An attempt that loses a lock
// race (a MySQL deadlock between two acquirers taking over one expired row,
// SQLite busy) is tried again, up to WithTxAttempts: each attempt decides
// afresh from the rows, so a retry can only win a key that is free.
func (u *UniqueStore) Acquire(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	var ok bool
	var err error
	for range u.st.txAttempts {
		ok, err = u.acquireOnce(ctx, key, owner, ttl)
		if err == nil || !u.st.isRetryable(err) || ctx.Err() != nil {
			return ok, err
		}
	}
	return ok, err
}

func (u *UniqueStore) acquireOnce(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	now := u.clk.Now()
	h, expires := keyHash(key), now.Add(ttl).UnixNano()
	if u.st.engine == engineMySQL {
		if _, err := u.st.exec(ctx, u.st.db, "DELETE FROM {unique_locks} WHERE key_hash = ? AND expires <= ?", h, now.UnixNano()); err != nil {
			return false, err
		}
		_, err := u.st.exec(ctx, u.st.db, "INSERT INTO {unique_locks} (key_hash, ukey, owner, expires) VALUES (?, ?, ?, ?)", h, key, owner, expires)
		if u.st.isUnique(err) {
			return false, nil
		}
		return err == nil, err
	}
	n, err := u.st.execN(ctx, u.st.db, `INSERT INTO {unique_locks} (key_hash, ukey, owner, expires) VALUES (?, ?, ?, ?)
		ON CONFLICT (key_hash) DO UPDATE SET ukey = excluded.ukey, owner = excluded.owner, expires = excluded.expires
		WHERE {unique_locks}.expires <= ?`, h, key, owner, expires, now.UnixNano())
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// Release implements middleware.UniqueStore.
func (u *UniqueStore) Release(ctx context.Context, key, owner string) error {
	if _, err := u.st.exec(ctx, u.st.db, "DELETE FROM {unique_locks} WHERE key_hash = ? AND owner = ?", keyHash(key), owner); err != nil {
		return err
	}
	_, err := u.st.exec(ctx, u.st.db, "DELETE FROM {unique_holders} WHERE key_hash = ? AND owner = ?", keyHash(key), owner)
	return err
}

// PruneExpired deletes every expired lock and returns how many. Expired
// rows never block an Acquire, so this only bounds the table's size; a
// running Client does it on its dispatcher's prune.
func (u *UniqueStore) PruneExpired(ctx context.Context) (int64, error) {
	n, err := u.st.execN(ctx, u.st.db, "DELETE FROM {unique_locks} WHERE expires <= ?", u.clk.Now().UnixNano())
	if err != nil {
		return n, err
	}
	return n, u.pruneHolders(ctx)
}
