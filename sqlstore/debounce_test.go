package sqlstore_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
	"github.com/ubgo/workflow/sqlstore"
)

// TestDebounceSwapRace is the store's contract: many processes swapping one
// key at once (each its own store over the same table, all released by one
// barrier) see every replaced burst exactly once, exactly one of them finds
// no previous burst, and the burst left is the one nobody got back. It runs
// over several fresh keys so the first-insert race happens many times.
func TestDebounceSwapRace(t *testing.T) {
	const swappers, keys = 12, 8
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()
			db, prefix := e.open(t)
			stores := make([]*sqlstore.DebounceStore, 4)
			for i := range stores {
				s, err := sqlstore.NewDebounceStore(db, e.dialect, sqlstore.WithTablePrefix(prefix))
				if err != nil {
					t.Fatal(err)
				}
				stores[i] = s
			}
			if err := stores[0].Migrate(ctx); err != nil {
				t.Fatal(err)
			}
			at := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
			for k := range keys {
				key := fmt.Sprintf("product-%d", k)
				var wg sync.WaitGroup
				var mu sync.Mutex
				go1 := make(chan struct{})
				returned := map[workflow.RunID]int{}
				notFound := 0
				errs := make([]error, swappers)
				for i := range swappers {
					wg.Add(1)
					go func() {
						defer wg.Done()
						<-go1
						b := middleware.DebounceBurst{First: at, RunAt: at.Add(time.Duration(i) * time.Second),
							Ref: workflow.RunRef{RunID: workflow.RunID(fmt.Sprintf("run-%d", i))}}
						prev, found, err := stores[i%len(stores)].Swap(ctx, key, b)
						mu.Lock()
						defer mu.Unlock()
						errs[i] = err
						if err != nil {
							return
						}
						if !found {
							notFound++
							return
						}
						returned[prev.Ref.RunID]++
					}()
				}
				close(go1)
				wg.Wait()
				for i, err := range errs {
					if err != nil {
						t.Fatalf("%s: Swap %d: %v", key, i, err)
					}
				}
				if notFound != 1 {
					t.Fatalf("%s: %d Swaps found no previous burst, want exactly 1", key, notFound)
				}
				last, found, err := stores[0].Get(ctx, key)
				if err != nil || !found {
					t.Fatalf("%s: Get after the race: %v, %v", key, found, err)
				}
				if len(returned) != swappers-1 {
					t.Fatalf("%s: %d distinct bursts returned as replaced, want %d", key, len(returned), swappers-1)
				}
				for id, n := range returned {
					if n != 1 {
						t.Fatalf("%s: burst %s returned %d times, want once", key, id, n)
					}
					if id == last.Ref.RunID {
						t.Fatalf("%s: the stored burst %s was also returned as replaced", key, id)
					}
				}
			}
		})
	}
}

// TestDebounceDispatcherPrunes: a running client's housekeeping deletes
// bursts whose run has started.
func TestDebounceDispatcherPrunes(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			clk := clock.NewFake(start)
			c := newClient(t, e, db, prefix, sqlstore.WithClock(clk), sqlstore.WithHousekeepingEvery(1))
			s := c.DebounceStore()
			ctx := context.Background()
			if _, _, err := s.Swap(ctx, "k", middleware.DebounceBurst{First: start, RunAt: start.Add(time.Minute)}); err != nil {
				t.Fatal(err)
			}
			if err := c.Register(workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "a", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}}); err != nil {
				t.Fatal(err)
			}
			sctx, cancel := context.WithCancel(ctx)
			t.Cleanup(cancel)
			if err := c.Start(sctx); err != nil {
				t.Fatal(err)
			}
			clk.Advance(2 * time.Minute)
			waitFor(t, 5*time.Second, "the finished burst to be pruned", func() bool {
				_, found, err := s.Get(ctx, "k")
				return err == nil && !found
			})
		})
	}
}

// TestDebounceStoreRoundTrip: Get of an unknown key, Swap stores every
// field, PruneExpired drops only bursts whose run has started, and the
// store's table comes from its own migration list.
func TestDebounceStoreRoundTrip(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()
			db, prefix := e.open(t)
			clk := clock.NewFake(start)
			s, err := sqlstore.NewDebounceStore(db, e.dialect, sqlstore.WithTablePrefix(prefix), sqlstore.WithClock(clk))
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Migrate(ctx); err != nil {
				t.Fatal(err)
			}
			if _, found, err := s.Get(ctx, "k"); err != nil || found {
				t.Fatalf("Get of an unknown key: %v, %v", found, err)
			}
			b := middleware.DebounceBurst{First: start, RunAt: start.Add(time.Minute), Ref: workflow.RunRef{RunID: "r1", ScheduledID: "s1"}}
			if _, found, err := s.Swap(ctx, "k", b); err != nil || found {
				t.Fatalf("first Swap: %v, %v", found, err)
			}
			if _, _, err := s.Swap(ctx, "other", middleware.DebounceBurst{First: start, RunAt: start.Add(time.Hour)}); err != nil {
				t.Fatal(err)
			}
			got, found, err := s.Get(ctx, "k")
			if err != nil || !found || !got.First.Equal(b.First) || !got.RunAt.Equal(b.RunAt) || got.Ref != b.Ref {
				t.Fatalf("Get = %+v, %v, %v; want %+v", got, found, err, b)
			}
			clk.Advance(2 * time.Minute)
			if n, err := s.PruneExpired(ctx); err != nil || n != 1 {
				t.Fatalf("PruneExpired = %d, %v; want 1", n, err)
			}
			if _, found, _ := s.Get(ctx, "other"); !found {
				t.Fatal("a burst whose run has not started was pruned")
			}
			var runs int
			if err := db.QueryRow("SELECT COUNT(*) FROM " + e.dialect.QuoteIdent(prefix+"runs")).Scan(&runs); err == nil {
				t.Fatal("NewDebounceStore.Migrate created the queue's tables")
			}
		})
	}
}

// TestDebounceAcrossProcesses runs middleware.Debounce in two "processes"
// (two debouncers, each with its own SQL store over one table) in front of
// one memory client: a burst enqueued from both runs once, the last job.
func TestDebounceAcrossProcesses(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()
			db, prefix := e.open(t)
			var mu sync.Mutex
			var ran []string
			m := memory.New()
			if err := m.Register(workflow.Workflow{Name: "reindex", Steps: []workflow.Step{{Name: "a", Run: func(_ context.Context, in []byte) ([]byte, error) {
				mu.Lock()
				defer mu.Unlock()
				ran = append(ran, string(in))
				return in, nil
			}}}}); err != nil {
				t.Fatal(err)
			}
			mctx, cancel := context.WithCancel(ctx)
			t.Cleanup(func() { cancel(); m.Stop() })
			if err := m.Start(mctx); err != nil {
				t.Fatal(err)
			}
			byPayload := func(workflow.Job) string { return "product-7" }
			var clients []workflow.Client
			for i := range 2 {
				s, err := sqlstore.NewDebounceStore(db, e.dialect, sqlstore.WithTablePrefix(prefix))
				if err != nil {
					t.Fatal(err)
				}
				if i == 0 {
					if err := s.Migrate(ctx); err != nil {
						t.Fatal(err)
					}
				}
				mw, err := middleware.Debounce(m, nil, byPayload, 300*time.Millisecond, 0, middleware.WithDebounceStore(s))
				if err != nil {
					t.Fatal(err)
				}
				clients = append(clients, middleware.WrapClient(m, mw))
			}
			for i := range 6 {
				if _, err := clients[i%2].Enqueue(ctx, workflow.Job{Workflow: "reindex", Payload: []byte(fmt.Sprintf(`"%d"`, i))}); err != nil {
					t.Fatal(err)
				}
			}
			waitFor(t, 5*time.Second, "the debounced run", func() bool {
				mu.Lock()
				defer mu.Unlock()
				return len(ran) > 0
			})
			time.Sleep(500 * time.Millisecond)
			mu.Lock()
			defer mu.Unlock()
			if len(ran) != 1 || ran[0] != `"5"` {
				t.Fatalf("ran %v across two processes, want only the last job once", ran)
			}
		})
	}
}
