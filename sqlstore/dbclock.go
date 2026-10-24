package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// defaultClockResync is how often a client with WithDatabaseClock reads the
// database's clock again. Between two reads the clock advances on this
// process's monotonic clock, which drifts by parts per million, so a minute
// keeps the error far below any lease or poll interval; the resync mostly
// follows the database server's own clock adjustments.
const defaultClockResync = time.Minute

// DatabaseClock is a clock.Clock whose Now is the database server's clock:
// one time source for every client of the tables, so due times, leases and
// cron fires compare against the same "now" however far the nodes' own
// clocks have drifted apart.
//
// It never queries on Now. Sync reads the database's time once and records
// it against the local clock; Now is that reading plus the local time
// elapsed since, which on clock.Real is the monotonic clock, so a jump of
// this machine's wall clock (an NTP step, a manual change) moves nothing.
// The reading is taken as the midpoint of the round trip, so it is off by
// at most half the round trip (Uncertainty). Before the first Sync, Now is
// the local clock.
//
// Timers run on the local clock: a duration is a duration on any clock.
// Postgres reads clock_timestamp(), MySQL UTC_TIMESTAMP(6) and SQLite
// julianday('now'); a SQLite file lives on one machine, so there it only
// makes every process on that machine agree, which they already do.
type DatabaseClock struct {
	db     *sql.DB
	engine string
	local  clock.Clock

	mu     sync.Mutex
	synced bool
	base   time.Time     // the database's time at the midpoint of the last read
	at     time.Time     // the local time at that midpoint
	half   time.Duration // half the last read's round trip
}

var _ clock.Clock = (*DatabaseClock)(nil)

// NewDatabaseClock returns a clock over db, whose SQL dialect is d
// (Postgres, SQLite or MySQL). It reads nothing until Sync.
func NewDatabaseClock(db *sql.DB, d sqlb.Dialect) (*DatabaseClock, error) {
	if db == nil || d == nil {
		return nil, fmt.Errorf("%w: NewDatabaseClock needs a *sql.DB and a dialect", workflow.ErrInvalid)
	}
	engine := sqlb.DialectName(d)
	if dbNowQuery(engine) == "" {
		return nil, workflow.Unsupported("dialect "+engine, "the database clock reads Postgres, SQLite and MySQL")
	}
	return &DatabaseClock{db: db, engine: engine, local: clock.Real{}}, nil
}

// dbNowQuery is the statement that returns the server's time as Unix
// microseconds, or "" for an engine without one. MySQL's is plain datetime
// arithmetic on UTC values, so the session time zone cannot shift it.
func dbNowQuery(engine string) string {
	switch engine {
	case enginePostgres:
		return "SELECT CAST(EXTRACT(EPOCH FROM clock_timestamp()) * 1000000 AS BIGINT)"
	case engineMySQL:
		return "SELECT TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP(6))"
	case engineSQLite:
		return "SELECT CAST((julianday('now') - 2440587.5) * 86400000000 AS INTEGER)"
	}
	return ""
}

// Sync reads the database's time and makes it the clock's reference. It
// takes a connection from the pool, so never call it while holding one of
// the pool's transactions on a pool that may have a single connection.
func (k *DatabaseClock) Sync(ctx context.Context) error {
	t0 := k.local.Now()
	var micros int64
	if err := k.db.QueryRowContext(ctx, dbNowQuery(k.engine)).Scan(&micros); err != nil {
		return fmt.Errorf("sqlstore: read the database clock: %w", err)
	}
	t1 := k.local.Now()
	half := t1.Sub(t0) / 2
	k.mu.Lock()
	defer k.mu.Unlock()
	k.base = time.UnixMicro(micros)
	k.at = t0.Add(half)
	k.half = half
	k.synced = true
	return nil
}

// Now is the database's time: the last reading plus the local time elapsed
// since. Before the first Sync it is the local clock's.
func (k *DatabaseClock) Now() time.Time {
	now := k.local.Now()
	k.mu.Lock()
	defer k.mu.Unlock()
	if !k.synced {
		return now
	}
	return k.base.Add(now.Sub(k.at))
}

// NewTimer runs on the local clock.
func (k *DatabaseClock) NewTimer(d time.Duration) clock.Timer { return k.local.NewTimer(d) }

// Offset is how far the database's clock is ahead of the local wall clock
// as of the last Sync (negative: behind); zero before the first Sync.
func (k *DatabaseClock) Offset() time.Duration {
	k.mu.Lock()
	defer k.mu.Unlock()
	if !k.synced {
		return 0
	}
	return k.base.Sub(k.at.Round(0))
}

// Uncertainty bounds the error of the last reading: half its round trip.
func (k *DatabaseClock) Uncertainty() time.Duration {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.half
}

// WithDatabaseClock makes the database server's clock the time source of
// every due time and lease of this client (see DatabaseClock), in place of
// WithClock and WithLeaseClock, which it overrides whatever the order of
// options: the nodes of a cluster then agree on "now" even when their own
// clocks do not. Start reads the database's clock before anything else
// (and fails when it cannot), then again every resync on a running client;
// zero or less means one minute. An enqueue-only client reads it at Start.
func WithDatabaseClock(resync time.Duration) Option {
	return func(cl *Client) {
		if resync <= 0 {
			resync = defaultClockResync
		}
		cl.dbClockEvery = resync
	}
}

// DatabaseClock returns the clock WithDatabaseClock installed, or nil, so
// an app can read its Offset or give the same clock to a UniqueStore or a
// DebounceStore over the same database.
func (c *Client) DatabaseClock() *DatabaseClock { return c.dbClock }

// syncClock reads the database clock when WithDatabaseClock is on.
func (c *Client) syncClock(ctx context.Context) error {
	if c.dbClock == nil {
		return nil
	}
	return c.dbClock.Sync(ctx)
}

// clockLoop reads the database clock again every dbClockEvery until the
// client stops. A failed read keeps the previous reference, which only
// drifts by the local clock's rate error.
func (c *Client) clockLoop() {
	defer c.wg.Done()
	for {
		t := c.dbClock.NewTimer(c.dbClockEvery)
		select {
		case <-c.ctx.Done():
			t.Stop()
			return
		case <-t.C():
		}
		if err := c.dbClock.Sync(c.ctx); err != nil && c.ctx.Err() == nil {
			c.log.Error("sqlstore: resync the database clock (the last reading stays in use)", "error", err)
		}
	}
}
