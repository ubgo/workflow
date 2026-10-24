package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/sqlb/contrib/migrate"
	"github.com/ubgo/workflow"
)

// BucketStore is a durable workflow.BucketStore: each bucket key's level is
// a row of <prefix>buckets, so every process calling one upstream draws from
// one bucket. Like UniqueStore it needs no running Client: build it with
// NewBucketStore over any *sql.DB, or take Client.BucketStore.
//
//	limit, err := workflow.NewUpstreamLimit(store, spec, nil)
//
// Take is atomic across processes: it first bumps the key's row, which
// locks it (a row lock on Postgres and MySQL, the single writer on SQLite),
// then reads the level, drains it by the leak since the stored time and
// admits or refuses the cost, in the same transaction. When the key has no
// row yet, the insert's primary key decides: of two processes inserting at
// once, the loser's transaction is tried again and finds the winner's row,
// so the cost of both is counted. The time is the now the caller passes
// (never the database's clock), so an injected clock drives it.
type BucketStore struct {
	st *store
}

var _ workflow.BucketStore = (*BucketStore)(nil)

// BucketMigrationOwner is the ledger owner of BucketStore's migrations: a
// list of its own, so NewBucketStore(...).Migrate creates only the buckets
// table, while Client.Migrate applies it too.
const BucketMigrationOwner = "ubgo/workflow/sqlstore/bucket"

// bucketMigrations is every BucketStore schema change, in order, under the
// same rules as migrations: never edit a released one, append a new one.
func (s *store) bucketMigrations() []migrate.Migration {
	return []migrate.Migration{
		{Owner: s.owner(BucketMigrationOwner), Name: "001_buckets", Script: s.script(s.render(bucketDefs(typesFor(s.engine))))},
	}
}

// bucketDefs is BucketStore's table. Its rendered text is frozen once
// released, like ddl.
func bucketDefs(t sqlTypes) []tableDef {
	return []tableDef{{stem: tblBuckets, lines: []string{
		// key_hash is the SHA-256 of the key in hex, as in unique_locks. The
		// primary key is what makes two first Takes of one key race to one
		// row (ADR-0003, a structural constraint).
		"key_hash " + t.id + " NOT NULL PRIMARY KEY",
		// bkey is the key as given, for an operator reading the table.
		"bkey " + t.text + " NOT NULL",
		// level is the bucket's level at at_ns, in shortest-round-trip
		// decimal text: no engine's float type reads back the exact value
		// MemoryBuckets holds, and the level is only ever read inside the
		// locked transaction.
		"level " + t.name + " NOT NULL",
		// at_ns is when level was set, in Unix nanoseconds of the caller's
		// now.
		"at_ns " + t.int + " NOT NULL",
		// ver is bumped first by every Take and Observe, which is how they
		// lock the row before reading it.
		"ver " + t.int + " NOT NULL",
	}}}
}

// NewBucketStore returns the bucket store over db for dialect d. Of the
// Options, WithTablePrefix, WithSchema, WithTxAttempts and
// WithErrorClassifier apply; the others configure a Client and are
// ignored. Call Migrate before the first Take.
func NewBucketStore(db *sql.DB, d sqlb.Dialect, opts ...Option) (*BucketStore, error) {
	c, err := New(db, d, opts...)
	if err != nil {
		return nil, err
	}
	return c.BucketStore(), nil
}

// BucketStore returns the bucket store in this client's database and
// prefix. Client.Migrate creates its table.
func (c *Client) BucketStore() *BucketStore { return &BucketStore{st: c.st} }

// Migrate creates the buckets table (its own migration list, under the same
// ledger and lock as Client.Migrate), and nothing else.
func (s *BucketStore) Migrate(ctx context.Context) error {
	return s.st.migrate(ctx, s.st.bucketMigrations())
}

// formatLevel is the stored text of a level: the shortest decimal that reads
// back to the same float64.
func formatLevel(v float64) string { return strconv.FormatFloat(v, 'g', -1, 64) }

// locked runs fn in a transaction that holds key's row: the row is bumped
// (or inserted) first, then fn reads the stored level and time and returns
// the level to store. A transaction that loses the first-insert race or a
// lock conflict is tried again, up to WithTxAttempts.
func (s *BucketStore) locked(ctx context.Context, key string, spec workflow.BucketSpec, now time.Time,
	fn func(level float64, at time.Time, known bool) (next float64, wait time.Duration)) (time.Duration, error) {
	if !(spec.Capacity > 0) || !(spec.LeakPerSecond > 0) || math.IsInf(spec.Capacity, 0) || math.IsInf(spec.LeakPerSecond, 0) {
		return 0, fmt.Errorf("%w: sqlstore: bucket needs a positive, finite Capacity and LeakPerSecond", workflow.ErrInvalid)
	}
	h := keyHash(key)
	var wait time.Duration
	var err error
	for range s.st.txAttempts {
		wait = 0
		err = s.st.tx(ctx, func(tx *sql.Tx) error {
			n, err := s.st.execN(ctx, tx, "UPDATE {buckets} SET ver = ver + 1 WHERE key_hash = ?", h)
			if err != nil {
				return err
			}
			var level float64
			var at time.Time
			if n > 0 {
				var text string
				var atNs int64
				if err := s.st.one(ctx, tx, "SELECT level, at_ns FROM {buckets} WHERE key_hash = ?", []any{h}, &text, &atNs); err != nil {
					return fmt.Errorf("sqlstore: read bucket: %w", err)
				}
				if level, err = strconv.ParseFloat(text, 64); err != nil {
					return fmt.Errorf("sqlstore: stored bucket level %q does not decode: %w", text, err)
				}
				at = time.Unix(0, atNs)
			}
			next, w := fn(level, at, n > 0)
			wait = w
			if n == 0 {
				_, err := s.st.exec(ctx, tx, "INSERT INTO {buckets} (key_hash, bkey, level, at_ns, ver) VALUES (?, ?, ?, ?, 0)",
					h, key, formatLevel(next), now.UnixNano())
				return err
			}
			_, err = s.st.exec(ctx, tx, "UPDATE {buckets} SET level = ?, at_ns = ? WHERE key_hash = ?", formatLevel(next), now.UnixNano(), h)
			return err
		})
		if !s.st.isUnique(err) || ctx.Err() != nil {
			break
		}
	}
	if err != nil {
		return 0, fmt.Errorf("sqlstore: bucket: %w", err)
	}
	return wait, nil
}

// Take implements workflow.BucketStore, atomically across processes (see
// BucketStore), with MemoryBuckets' arithmetic: the level leaks since the
// stored time, then cost is added when it fits; otherwise nothing is added
// (the drained level and the time are still stored) and the wait until it
// would fit is returned.
func (s *BucketStore) Take(ctx context.Context, key string, spec workflow.BucketSpec, cost float64, now time.Time) (time.Duration, error) {
	return s.locked(ctx, key, spec, now, func(level float64, at time.Time, known bool) (float64, time.Duration) {
		if known {
			if elapsed := now.Sub(at).Seconds(); elapsed > 0 {
				level = max(level-elapsed*spec.LeakPerSecond, 0)
			}
		}
		if over := level + cost - spec.Capacity; over > 0 {
			return level, time.Duration(math.Ceil(over / spec.LeakPerSecond * float64(time.Second)))
		}
		return level + cost, 0
	})
}

// Observe implements workflow.BucketStore: the level is set to what the
// upstream reported, clamped to [0, Capacity].
func (s *BucketStore) Observe(ctx context.Context, key string, spec workflow.BucketSpec, level float64, now time.Time) error {
	_, err := s.locked(ctx, key, spec, now, func(float64, time.Time, bool) (float64, time.Duration) {
		return min(max(level, 0), spec.Capacity), 0
	})
	return err
}
