package e2e_test

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/ubgo/sqlb"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/hatchet"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/recorder"
	"github.com/ubgo/workflow/recorder/recordertest"
	wfredis "github.com/ubgo/workflow/redis"
	"github.com/ubgo/workflow/sqlstore"
	"github.com/ubgo/workflow/workflowtest"
)

// Driver timings, the same the drivers' own conformance tests use: a short
// poll so a run is picked up quickly, a lease a loaded machine never
// overruns.
const (
	testPoll  = 10 * time.Millisecond
	testLease = 2 * time.Second
)

// TestRecorderOnMemoryWithSQLStore records memory-driver runs into
// sqlrecorder on every engine: the SQL store proven under a real driver's
// write pattern.
func TestRecorderOnMemoryWithSQLStore(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			recordertest.RunDriver(t, workflowtest.Harness{New: func(t *testing.T) workflowtest.Instance {
				clk := clock.NewFake(start)
				return workflowtest.Instance{Client: memory.New(memory.WithClock(clk)), Clock: clk}
			}}, recordertest.WithStore(storeFactory(e)))
		})
	}
}

// sqlstoreDialect maps an engine name to the SQL driver's dialect.
func sqlstoreDialect(name string) sqlb.Dialect {
	switch name {
	case "postgres":
		return sqlb.PostgresDialect{}
	case "mysql":
		return sqlb.MySQLDialect{}
	}
	return sqlb.SQLiteDialect{}
}

// TestRecorderOnSQLStore runs the recorder over the SQL driver on every
// engine, recording into sqlrecorder in the same database.
func TestRecorderOnSQLStore(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			var db *sqlDB
			recordertest.RunDriver(t, workflowtest.Harness{New: func(t *testing.T) workflowtest.Instance {
				d, prefix := e.open(t)
				db = &sqlDB{db: d, prefix: prefix}
				clk := clock.NewFake(start)
				c, err := sqlstore.New(d, sqlstoreDialect(e.name), sqlstore.WithTablePrefix(prefix), sqlstore.WithPollInterval(testPoll),
					sqlstore.WithLease(testLease), sqlstore.WithClock(clk))
				if err != nil {
					t.Fatal(err)
				}
				if err := c.Migrate(context.Background()); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(c.Stop)
				return workflowtest.Instance{Client: c, Clock: clk}
			}}, recordertest.WithStore(func(t *testing.T, cfg recordertest.StoreConfig) recorder.Store {
				// The same database and prefix the driver uses.
				return newSQLStore(t, e, db.db, db.prefix)
			}))
		})
	}
}

// sqlDB carries one case's database from Harness.New to the store factory,
// which RunDriver calls right after it.
type sqlDB struct {
	db     *sql.DB
	prefix string
}

// TestRecorderOnRedis runs the recorder over the Redis driver.
func TestRecorderOnRedis(t *testing.T) {
	url := os.Getenv(envRedisURL)
	if url == "" {
		t.Skipf("%s unset: Redis cases skipped (task recorder:e2e:redis)", envRedisURL)
	}
	opt, err := goredis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	rdb := goredis.NewClient(opt)
	t.Cleanup(func() { _ = rdb.Close() })
	recordertest.RunDriver(t, workflowtest.Harness{New: func(t *testing.T) workflowtest.Instance {
		prefix := "rectest-" + randHex(6)
		t.Cleanup(func() { dropRedisPrefix(rdb, prefix) })
		clk := clock.NewFake(start)
		c, err := wfredis.New(rdb, wfredis.WithPrefix(prefix), wfredis.WithPollInterval(testPoll), wfredis.WithLease(testLease), wfredis.WithClock(clk))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(c.Stop)
		return workflowtest.Instance{Client: c, Clock: clk}
	}})
}

// dropRedisPrefix deletes every key of a test's prefix.
func dropRedisPrefix(rdb goredis.UniversalClient, p string) {
	ctx := context.Background()
	iter := rdb.Scan(ctx, 0, "{"+p+"}*", 500).Iterator()
	var keys []string
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if len(keys) > 0 {
		_ = rdb.Del(ctx, keys...).Err()
	}
}

// TestRecorderOnHatchet runs the recorder over a live Hatchet engine, on
// real time (the engine keeps its own clock). It skips without a token.
func TestRecorderOnHatchet(t *testing.T) {
	if os.Getenv(envHatchet) == "" {
		t.Skipf("%s unset: the Hatchet cases need a live engine (task recorder:e2e:hatchet)", envHatchet)
	}
	recordertest.RunDriver(t, workflowtest.Harness{
		New: func(t *testing.T) workflowtest.Instance {
			return workflowtest.Instance{Client: hatchet.New(hatchet.Config{WorkerName: "rectest-hatchet"})}
		},
		Prefix: "rectest_hatchet",
	})
}
