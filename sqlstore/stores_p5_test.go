package sqlstore_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/sqlstore"
)

// TestJournalStore is the JournalStore contract on every engine: Load of a
// missing key is not found, Save replaces, an empty value is a value, runs
// are separate, and Forget removes one run only and is idempotent. A
// second store over the same table (another process) sees the entries.
func TestJournalStore(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()
			db, prefix := e.open(t)
			a, err := sqlstore.NewJournalStore(db, e.dialect, sqlstore.WithTablePrefix(prefix))
			if err != nil {
				t.Fatal(err)
			}
			if err := a.Migrate(ctx); err != nil {
				t.Fatal(err)
			}
			b, err := sqlstore.NewJournalStore(db, e.dialect, sqlstore.WithTablePrefix(prefix))
			if err != nil {
				t.Fatal(err)
			}
			if _, ok, err := a.Load(ctx, "r1", "k"); ok || err != nil {
				t.Fatalf("Load of a missing key = %v, %v; want not found", ok, err)
			}
			if err := a.Save(ctx, "r1", "k", []byte("one")); err != nil {
				t.Fatal(err)
			}
			if err := a.Save(ctx, "r1", "k", []byte("two")); err != nil {
				t.Fatal(err)
			}
			if v, ok, err := b.Load(ctx, "r1", "k"); !ok || err != nil || string(v) != "two" {
				t.Errorf("Load = %q, %v, %v; want two (Save replaces, seen by another store)", v, ok, err)
			}
			if err := a.Save(ctx, "r1", "empty", nil); err != nil {
				t.Fatal(err)
			}
			if v, ok, err := a.Load(ctx, "r1", "empty"); !ok || err != nil || len(v) != 0 {
				t.Errorf("Load of an empty value = %q, %v, %v; want found, empty", v, ok, err)
			}
			if err := a.Save(ctx, "r2", "k", []byte("other")); err != nil {
				t.Fatal(err)
			}
			if err := a.Forget(ctx, "r1"); err != nil {
				t.Fatal(err)
			}
			if err := a.Forget(ctx, "r1"); err != nil {
				t.Errorf("second Forget: %v", err)
			}
			if _, ok, _ := a.Load(ctx, "r1", "k"); ok {
				t.Error("r1 still has an entry after Forget")
			}
			if v, ok, _ := a.Load(ctx, "r2", "k"); !ok || string(v) != "other" {
				t.Errorf("Forget(r1) touched r2: %q, %v", v, ok)
			}
		})
	}
}

// TestJournalSaveRace: many stores saving one new key at once all succeed
// (the primary key decides the insert, the losers update) and one value
// wins.
func TestJournalSaveRace(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()
			db, prefix := e.open(t)
			s, err := sqlstore.NewJournalStore(db, e.dialect, sqlstore.WithTablePrefix(prefix))
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Migrate(ctx); err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			start := make(chan struct{})
			errs := make([]error, 8)
			for i := range errs {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					errs[i] = s.Save(ctx, "r", "k", []byte(fmt.Sprint(i)))
				}()
			}
			close(start)
			wg.Wait()
			for i, err := range errs {
				if err != nil {
					t.Errorf("saver %d: %v", i, err)
				}
			}
			if _, ok, err := s.Load(ctx, "r", "k"); !ok || err != nil {
				t.Errorf("Load after the race = %v, %v", ok, err)
			}
		})
	}
}

// TestBucketStoreTake pins MemoryBuckets' arithmetic on every engine: a
// call that fits is added, one that does not adds nothing and reports the
// wait, the level leaks with the caller's clock, and Observe sets the
// level (clamped).
func TestBucketStoreTake(t *testing.T) {
	spec := workflow.BucketSpec{Capacity: 10, LeakPerSecond: 2}
	t0 := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()
			db, prefix := e.open(t)
			s, err := sqlstore.NewBucketStore(db, e.dialect, sqlstore.WithTablePrefix(prefix))
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Migrate(ctx); err != nil {
				t.Fatal(err)
			}
			take := func(key string, cost float64, at time.Time) time.Duration {
				t.Helper()
				w, err := s.Take(ctx, key, spec, cost, at)
				if err != nil {
					t.Fatal(err)
				}
				return w
			}
			if w := take("k", 8, t0); w != 0 {
				t.Errorf("first take of 8 waits %s", w)
			}
			// Level 8, cost 4: 2 over, leaks at 2/s, so 1s.
			if w := take("k", 4, t0); w != time.Second {
				t.Errorf("take over capacity waits %s, want 1s", w)
			}
			// The refused take added nothing: after 1s the level is 6, so 4 fits.
			if w := take("k", 4, t0.Add(time.Second)); w != 0 {
				t.Errorf("take after the leak waits %s, want 0", w)
			}
			if w := take("other", 10, t0); w != 0 {
				t.Errorf("a different key shares the bucket: waits %s", w)
			}
			if err := s.Observe(ctx, "k", spec, 99, t0.Add(time.Hour)); err != nil {
				t.Fatal(err)
			}
			// Clamped to capacity 10: a cost of 2 needs 1s to drain.
			if w := take("k", 2, t0.Add(time.Hour)); w != time.Second {
				t.Errorf("take after Observe(99) waits %s, want 1s (level clamped to capacity)", w)
			}
			if _, err := s.Take(ctx, "k", workflow.BucketSpec{}, 1, t0); err == nil {
				t.Error("an empty BucketSpec was accepted")
			}
		})
	}
}

// TestBucketTakeAtomic is the store's contract across processes: many
// stores (each its own handle over the same table) taking cost 1 from a
// bucket of capacity N at one instant admit exactly N takes, however they
// interleave, and refuse the rest. A non-atomic read-then-write admits more.
func TestBucketTakeAtomic(t *testing.T) {
	const capacity, takers = 5, 16
	spec := workflow.BucketSpec{Capacity: capacity, LeakPerSecond: 1}
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()
			db, prefix := e.open(t)
			stores := make([]*sqlstore.BucketStore, 4)
			for i := range stores {
				s, err := sqlstore.NewBucketStore(db, e.dialect, sqlstore.WithTablePrefix(prefix))
				if err != nil {
					t.Fatal(err)
				}
				stores[i] = s
			}
			if err := stores[0].Migrate(ctx); err != nil {
				t.Fatal(err)
			}
			for k := range 4 {
				key := fmt.Sprintf("shop-%d", k)
				var wg sync.WaitGroup
				var mu sync.Mutex
				start := make(chan struct{})
				admitted := 0
				for i := range takers {
					wg.Add(1)
					go func() {
						defer wg.Done()
						<-start
						w, err := stores[i%len(stores)].Take(ctx, key, spec, 1, now)
						mu.Lock()
						defer mu.Unlock()
						if err != nil {
							t.Errorf("take: %v", err)
							return
						}
						if w == 0 {
							admitted++
						}
					}()
				}
				close(start)
				wg.Wait()
				if admitted != capacity {
					t.Errorf("%s: %d takes admitted at one instant, want exactly %d", key, admitted, capacity)
				}
			}
		})
	}
}
