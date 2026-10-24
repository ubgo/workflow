package sqlstore_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
	"github.com/ubgo/workflow/sqlstore"
)

var _ middleware.UniqueStore = (*sqlstore.UniqueStore)(nil)

// newUnique builds and migrates a standalone lock store (no Client).
func newUnique(t *testing.T, e engine, clk clock.Clock) *sqlstore.UniqueStore {
	t.Helper()
	db, prefix := e.open(t)
	u, err := sqlstore.NewUniqueStore(db, e.dialect, sqlstore.WithTablePrefix(prefix), sqlstore.WithClock(clk))
	if err != nil {
		t.Fatalf("NewUniqueStore: %v", err)
	}
	if err := u.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return u
}

// uniqueOp is one step of the shared script: advance the clock, then
// acquire (ttl set) or release, expecting want from an acquire.
type uniqueOp struct {
	advance    time.Duration
	release    bool
	key, owner string
	ttl        time.Duration
	want       bool
}

// uniqueScript pins MemoryUnique's semantics; both stores must answer it
// identically.
func uniqueScript() []uniqueOp {
	long := strings.Repeat("k", 1000) // beyond any engine's index limit for a raw key
	return []uniqueOp{
		{key: "a", owner: "o1", ttl: 10 * time.Second, want: true},
		{key: "a", owner: "o2", ttl: time.Second, want: false}, // held by another owner
		{key: "a", owner: "o1", ttl: time.Second, want: false}, // held, even by itself
		{key: "b", owner: "o2", ttl: time.Second, want: true},  // keys are independent
		{advance: 9*time.Second + 999*time.Millisecond, key: "a", owner: "o2", ttl: time.Second, want: false},
		{advance: time.Millisecond, key: "a", owner: "o2", ttl: 10 * time.Second, want: true}, // free exactly at expiry
		{release: true, key: "a", owner: "o1"},                                                // not the owner: no-op
		{key: "a", owner: "o3", ttl: time.Second, want: false},
		{release: true, key: "a", owner: "o2"},
		{key: "a", owner: "o3", ttl: time.Second, want: true},
		{release: true, key: "nobody", owner: "o1"}, // a free key: no-op
		{key: "z", owner: "o1", ttl: 0, want: true}, // taken already expired
		{key: "z", owner: "o2", ttl: -time.Second, want: true},
		{key: "z", owner: "o3", ttl: time.Hour, want: true},
		{key: long, owner: "o1", ttl: time.Hour, want: true},
		{key: long, owner: "o2", ttl: time.Hour, want: false},
		{key: long + "x", owner: "o2", ttl: time.Hour, want: true},
		{advance: 2 * time.Hour, release: true, key: long, owner: "o1"}, // its owner frees it, expired or not
		{key: long, owner: "o2", ttl: time.Hour, want: true},
	}
}

func runUniqueScript(t *testing.T, s middleware.UniqueStore, clk *clock.Fake) {
	t.Helper()
	ctx := context.Background()
	for i, op := range uniqueScript() {
		clk.Advance(op.advance)
		if op.release {
			if err := s.Release(ctx, op.key, op.owner); err != nil {
				t.Fatalf("op %d: Release: %v", i, err)
			}
			continue
		}
		got, err := s.Acquire(ctx, op.key, op.owner, op.ttl)
		if err != nil {
			t.Fatalf("op %d: Acquire: %v", i, err)
		}
		if got != op.want {
			t.Fatalf("op %d: Acquire(%.10q, %s, %s) = %v, want %v", i, op.key, op.owner, op.ttl, got, op.want)
		}
	}
}

// TestUniqueMatchesMemoryUnique runs one script against MemoryUnique (the
// reference) and the SQL store on every engine.
func TestUniqueMatchesMemoryUnique(t *testing.T) {
	t.Run("memory-reference", func(t *testing.T) {
		clk := clock.NewFake(start)
		runUniqueScript(t, middleware.NewMemoryUnique(clk), clk)
	})
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			clk := clock.NewFake(start)
			runUniqueScript(t, newUnique(t, e, clk), clk)
		})
	}
}

// TestUniqueOneWinner races acquirers of one key: exactly one wins, every
// round, on every engine.
func TestUniqueOneWinner(t *testing.T) {
	const rounds, racers = 20, 8
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			clk := clock.NewFake(start)
			u := newUnique(t, e, clk)
			for r := range rounds {
				// Every other round the key holds an expired lock, so the race
				// is over a takeover as well as a fresh insert.
				key := "race-" + string(rune('a'+r/2))
				if r%2 == 1 {
					clk.Advance(2 * time.Minute)
				}
				var wins atomic.Int64
				var wg sync.WaitGroup
				gate := make(chan struct{})
				for i := range racers {
					wg.Add(1)
					go func() {
						defer wg.Done()
						<-gate
						ok, err := u.Acquire(context.Background(), key, "owner-"+string(rune('0'+i)), time.Minute)
						if err != nil {
							t.Errorf("round %d: Acquire: %v", r, err)
						}
						if ok {
							wins.Add(1)
						}
					}()
				}
				close(gate)
				wg.Wait()
				if n := wins.Load(); n != 1 {
					t.Fatalf("round %d: %d acquirers won key %s, want exactly 1", r, n, key)
				}
			}
		})
	}
}

// TestUniqueThroughMiddleware uses the SQL store as middleware.Unique's
// store in front of a real client: a second enqueue with the held key is
// refused across two wrapped clients sharing the table.
func TestUniqueThroughMiddleware(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			stores := make([]*sqlstore.UniqueStore, 2)
			for i := range stores {
				u, err := sqlstore.NewUniqueStore(db, e.dialect, sqlstore.WithTablePrefix(prefix))
				if err != nil {
					t.Fatal(err)
				}
				if err := u.Migrate(context.Background()); err != nil {
					t.Fatal(err)
				}
				stores[i] = u
			}
			m := memory.New()
			if err := m.Register(workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "a", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}}); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(func() { cancel(); m.Stop() })
			if err := m.Start(ctx); err != nil {
				t.Fatal(err)
			}
			by := func(j workflow.Job) string { return string(j.Payload) }
			job := workflow.Job{Workflow: "w", Payload: []byte("order-1")}
			first := middleware.WrapClient(m, middleware.Unique(by, stores[0], time.Hour))
			second := middleware.WrapClient(m, middleware.Unique(by, stores[1], time.Hour))
			if _, err := first.Enqueue(ctx, job); err != nil {
				t.Fatal(err)
			}
			if _, err := second.Enqueue(ctx, job); !errors.Is(err, workflow.ErrDuplicate) {
				t.Fatalf("second process enqueued a held key: %v", err)
			}
		})
	}
}

// TestUniqueStandaloneSchema: a lock store's Migrate creates only its table,
// and a Client migrating the same database later adds the queue's tables
// beside it; the client's own lock store then works without another Migrate.
func TestUniqueStandaloneSchema(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			u, err := sqlstore.NewUniqueStore(db, e.dialect, sqlstore.WithTablePrefix(prefix))
			if err != nil {
				t.Fatal(err)
			}
			if err := u.Migrate(context.Background()); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec("SELECT COUNT(*) FROM " + e.dialect.QuoteIdent(prefix+"runs")); err == nil {
				t.Fatal("the lock store's Migrate created the queue's tables")
			}
			c := newClient(t, e, db, prefix)
			ok, err := c.UniqueStore().Acquire(context.Background(), "k", "o", time.Hour)
			if err != nil || !ok {
				t.Fatalf("client lock store Acquire = %v, %v", ok, err)
			}
			if ok, _ := u.Acquire(context.Background(), "k", "o2", time.Hour); ok {
				t.Fatal("the standalone store and the client's store do not share locks")
			}
		})
	}
}

func TestUniquePruneExpired(t *testing.T) {
	e := engines(t)[0]
	clk := clock.NewFake(start)
	u := newUnique(t, e, clk)
	ctx := context.Background()
	for _, k := range []string{"a", "b"} {
		if _, err := u.Acquire(ctx, k, "o", time.Minute); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := u.Acquire(ctx, "c", "o", time.Hour); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Minute)
	n, err := u.PruneExpired(ctx)
	if err != nil || n != 2 {
		t.Fatalf("PruneExpired = %d, %v; want 2", n, err)
	}
	if ok, _ := u.Acquire(ctx, "c", "o2", time.Hour); ok {
		t.Fatal("PruneExpired removed a live lock")
	}
}

// TestUniqueHolders: the store remembers the run of a held key for its
// owner only, and forgets it when the lock is released or expires.
func TestUniqueHolders(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()
			clk := clock.NewFake(start)
			u := newUnique(t, e, clk)
			var hs middleware.UniqueHolders = u
			ref := workflow.RunRef{RunID: "run_1", ScheduledID: "sch_1"}
			if err := hs.SetHolder(ctx, "k", "o", ref); err != nil {
				t.Fatal(err)
			}
			if _, _, ok, _ := hs.Holder(ctx, "k"); ok {
				t.Fatal("a free key has a holder")
			}
			if ok, err := u.Acquire(ctx, "k", "o", time.Minute); err != nil || !ok {
				t.Fatalf("Acquire = %v, %v", ok, err)
			}
			if err := hs.SetHolder(ctx, "k", "intruder", workflow.RunRef{RunID: "run_x"}); err != nil {
				t.Fatal(err)
			}
			if _, _, ok, _ := hs.Holder(ctx, "k"); ok {
				t.Fatal("another owner's SetHolder recorded a holder")
			}
			if err := hs.SetHolder(ctx, "k", "o", ref); err != nil {
				t.Fatal(err)
			}
			owner, got, ok, err := hs.Holder(ctx, "k")
			if err != nil || !ok || owner != "o" || got != ref {
				t.Fatalf("Holder = %q %+v %v %v; want o and %+v", owner, got, ok, err, ref)
			}
			clk.Advance(time.Minute)
			if _, _, ok, _ := hs.Holder(ctx, "k"); ok {
				t.Fatal("an expired lock still has a holder")
			}
			if ok, _ := u.Acquire(ctx, "k", "o2", time.Minute); !ok {
				t.Fatal("the expired key was not taken over")
			}
			if _, _, ok, _ := hs.Holder(ctx, "k"); ok {
				t.Fatal("the new owner inherited the old owner's holder")
			}
			if err := hs.SetHolder(ctx, "k", "o2", ref); err != nil {
				t.Fatal(err)
			}
			if err := u.Release(ctx, "k", "o2"); err != nil {
				t.Fatal(err)
			}
			if _, _, ok, _ := hs.Holder(ctx, "k"); ok {
				t.Fatal("a released key has a holder")
			}
		})
	}
}
