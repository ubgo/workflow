package enginetest

import (
	"slices"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/sqlstore"
	"github.com/ubgo/workflow/workflowtest"
)

// The shared conformance suite runs on SQLite in shares, one test binary
// per share (the packages under shards/), because go test runs packages as
// parallel processes. Parallel subtests inside one binary were tried and
// dropped: modernc's SQLite takes process-wide locks on every allocation
// (libc's allocator) and in its own static mutexes, so cases in one
// process wait on each other, and at load 150 the waits fenced lease
// renewals and steps ran twice. A share runs its cases one at a time, the
// way the whole suite always ran, so every case sees its process exactly
// as before; the machine just runs a few such processes at once. Postgres
// and MySQL keep the suite in TestConformance of this package, unchanged:
// their tests share one server and its connection limit.

// conformanceShares names the groups of every SQLite TestConformance
// share but the last; the last share runs every group none of these name,
// so a group added to the suite later always runs. The split balances
// measured run time (queuelimit alone is a third of it).
var conformanceShares = [][]string{
	{"queuelimit", "misfire", "discardexhaust", "durable", "drainreport", "wait", "concurrency"},
}

// oneConnectionShares is conformanceShares for
// TestConformanceOnOneConnection.
var oneConnectionShares = [][]string{
	{"discardexhaust", "misfire", "schedule", "overlap", "drainreport", "runquery", "schedkeep", "cron", "chain", "schedactive", "retryprecedence"},
}

// ConformanceShares is how many shares the SQLite TestConformance is split
// into, and OneConnectionShares the same for
// TestConformanceOnOneConnection; a shard package runs one share each.
var (
	ConformanceShares   = len(conformanceShares) + 1
	OneConnectionShares = len(oneConnectionShares) + 1
)

// shareGroups is the Harness.Groups filter of share i of a suite split by
// named: named[i] for i < len(named), every other group for the last.
func shareGroups(t *testing.T, named [][]string, i int) func(string) bool {
	t.Helper()
	if i < 0 || i > len(named) {
		t.Fatalf("share %d of %d does not exist", i, len(named)+1)
	}
	if i < len(named) {
		return func(g string) bool { return slices.Contains(named[i], g) }
	}
	return func(g string) bool {
		for _, n := range named {
			if slices.Contains(n, g) {
				return false
			}
		}
		return true
	}
}

// ConformanceSQLite runs share i (0-based, below ConformanceShares) of the
// shared driver suite on SQLite as the subtest "sqlite", so its names are
// those TestConformance always had. A shard package's TestConformance
// calls it.
func ConformanceSQLite(t *testing.T, i int) {
	t.Helper()
	e := engines(t)[0]
	groups := shareGroups(t, conformanceShares, i)
	t.Run(e.name, func(t *testing.T) {
		h := conformanceHarness(e)
		h.Groups = groups
		workflowtest.Run(t, h)
	})
}

// ConformanceOnOneConnection runs share i (below OneConnectionShares) of
// the shared driver suite on SQLite with a pool of a single connection:
// the guard against the next nested pool use. Any query the driver makes
// on the pool while the same call path holds a transaction (or an open
// result set) waits for a connection that only it holds, so the suite
// hangs at that call and go test's -timeout dump names it, instead of the
// deadlock reaching a production pool under load. A shard package's
// TestConformanceOnOneConnection calls it.
func ConformanceOnOneConnection(t *testing.T, i int) {
	t.Helper()
	e := engines(t)[0]
	workflowtest.Run(t, workflowtest.Harness{Wait: conformanceWait, Groups: shareGroups(t, oneConnectionShares, i), New: func(t *testing.T) workflowtest.Instance {
		db, prefix := e.open(t)
		db.SetMaxOpenConns(1)
		clk := clock.NewFake(start)
		// One connection queues every statement, renewals included, behind
		// the poll loop's; the queue-case lease leaves a loaded machine room.
		mk := func(extra ...sqlstore.Option) *sqlstore.Client {
			return newClient(t, e, db, prefix, append([]sqlstore.Option{sqlstore.WithClock(clk), sqlstore.WithLease(queueCaseLease)}, extra...)...)
		}
		current := mk()
		return workflowtest.Instance{
			Client: current,
			Clock:  clk,
			Restart: func(t *testing.T) workflow.Client {
				current = mk()
				return current
			},
			EnqueueOnly: func(t *testing.T) workflow.Client {
				return mk(sqlstore.WithEnqueueOnly())
			},
		}
	}})
}

// conformanceHarness is the shared suite's harness on engine e, on a fake
// scheduling clock, with a Restart hook over the same database: the
// durable cases (a waiting run, a scheduled run and a step cut off by a
// stop all survive into a new client) run here, and a Crash hook whose old
// client writes nothing more, so the crash case proves lease expiry.
func conformanceHarness(e engine) workflowtest.Harness {
	return workflowtest.Harness{Wait: conformanceWait, New: func(t *testing.T) workflowtest.Instance {
		db, prefix := e.open(t)
		clk := clock.NewFake(start)
		// The queue-case lease for the same reason as below: a renewal
		// a loaded machine stalls for 2/3 of testLease fences a live
		// attempt and the step runs once more than the case counts.
		mk := func(extra ...sqlstore.Option) *sqlstore.Client {
			return newClient(t, e, db, prefix, append([]sqlstore.Option{sqlstore.WithClock(clk), sqlstore.WithLease(queueCaseLease)}, extra...)...)
		}
		current := mk()
		return workflowtest.Instance{
			Client: current,
			Clock:  clk,
			Restart: func(t *testing.T) workflow.Client {
				current = mk()
				return current
			},
			Crash: func(t *testing.T) workflow.Client {
				crash(current)
				current = mk()
				return current
			},
			EnqueueOnly: func(t *testing.T) workflow.Client {
				return mk(sqlstore.WithEnqueueOnly())
			},
		}
	}, NewWithQueues: func(t *testing.T, qs []workflow.QueueConfig) workflowtest.Instance {
		db, prefix := e.open(t)
		clk := clock.NewFake(start)
		// A lease is wall time (WithLeaseClock), so a client slowed by -race
		// can miss its heartbeat inside testLease and lose an attempt the
		// queue-limit cases hold on purpose; a longer lease leaves headroom,
		// and the crash case waits for it to expire with a matching limit.
		mk := func(extra ...sqlstore.Option) *sqlstore.Client {
			return newClient(t, e, db, prefix, append([]sqlstore.Option{sqlstore.WithClock(clk), sqlstore.WithQueues(qs...), sqlstore.WithLease(queueLimitLease)}, extra...)...)
		}
		current := mk()
		return workflowtest.Instance{
			Client: current,
			Clock:  clk,
			Restart: func(t *testing.T) workflow.Client {
				current = mk()
				return current
			},
			Crash: func(t *testing.T) workflow.Client {
				crash(current)
				current = mk()
				return current
			},
			Peer: func(t *testing.T) workflow.Client { return mk() },
		}
	}}
}
