package redis_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	wfredis "github.com/ubgo/workflow/redis"
	"github.com/ubgo/workflow/workflowtest"
)

// envRedisURL names the variable holding the test Redis URL (task
// redis:test sets it to the shared stack's Redis); without it the tests
// skip, so `go test` works on a machine with no Redis.
const envRedisURL = "WFTEST_REDIS_URL"

// start is the fake clock's first instant in every test.
var start = time.Date(2026, 1, 5, 10, 0, 0, 0, time.UTC)

// Test timings: a short poll so a client sees another's work quickly, and
// a lease short enough that the lease-expiry cases finish well inside the
// suite's 5s wait, yet long enough that a loaded machine never delays a
// live worker's renewal past it.
const (
	testPoll  = 10 * time.Millisecond
	testLease = 2 * time.Second
)

// redisClient connects to the test Redis or skips t.
func redisClient(t *testing.T) goredis.UniversalClient {
	t.Helper()
	url := os.Getenv(envRedisURL)
	if url == "" {
		t.Skipf("%s unset: Redis tests skipped (run task redis:test)", envRedisURL)
	}
	opt, err := goredis.ParseURL(url)
	if err != nil {
		t.Fatalf("parse %s: %v", envRedisURL, err)
	}
	rdb := goredis.NewClient(opt)
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("ping Redis at %s: %v", url, err)
	}
	return rdb
}

// testPrefix gives t its own key prefix and deletes every key under it
// when t ends.
func testPrefix(t *testing.T, rdb goredis.UniversalClient) string {
	t.Helper()
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	p := "wftest-" + hex.EncodeToString(b[:])
	t.Cleanup(func() { dropPrefix(rdb, p) })
	return p
}

// dropPrefix deletes every key of prefix p.
func dropPrefix(rdb goredis.UniversalClient, p string) {
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

// newClient builds a client on prefix; t's cleanup stops it.
func newClient(t *testing.T, rdb goredis.UniversalClient, prefix string, opts ...wfredis.Option) *wfredis.Client {
	t.Helper()
	all := append([]wfredis.Option{
		wfredis.WithPrefix(prefix),
		wfredis.WithPollInterval(testPoll),
		wfredis.WithLease(testLease),
	}, opts...)
	c, err := wfredis.New(rdb, all...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(c.Stop)
	return c
}

// TestConformance runs the shared driver suite against Redis on a fake
// scheduling clock, with a Restart hook over the same prefix (a waiting
// run, a scheduled run and a step cut off by a stop survive into a new
// client) and a Crash hook whose old client writes nothing more, so the
// crash case proves lease expiry.
func TestConformance(t *testing.T) {
	rdb := redisClient(t)
	workflowtest.Run(t, workflowtest.Harness{New: func(t *testing.T) workflowtest.Instance {
		prefix := testPrefix(t, rdb)
		clk := clock.NewFake(start)
		current := newClient(t, rdb, prefix, wfredis.WithClock(clk))
		return workflowtest.Instance{
			Client: current,
			Clock:  clk,
			Restart: func(t *testing.T) workflow.Client {
				current = newClient(t, rdb, prefix, wfredis.WithClock(clk))
				return current
			},
			Crash: func(t *testing.T) workflow.Client {
				wfredis.Crash(current)
				current = newClient(t, rdb, prefix, wfredis.WithClock(clk))
				return current
			},
			EnqueueOnly: func(t *testing.T) workflow.Client {
				return newClient(t, rdb, prefix, wfredis.WithClock(clk), wfredis.WithEnqueueOnly())
			},
		}
	}})
}
