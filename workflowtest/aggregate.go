package workflowtest

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow/middleware"
)

// AggregateStoreContract checks the middleware.AggregateStore contract
// every store must keep, on a store with no items in groups "g" and
// "race": Append's count and first, Take's order and limit, a drained
// group starting over, and no item lost or returned twice while several
// goroutines Append and Take one group at once (run it under -race). A
// durable store's test calls it once per engine.
func AggregateStoreContract(t testing.TB, s middleware.AggregateStore) {
	t.Helper()
	ctx := context.Background()
	t0 := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range 3 {
		n, first, err := s.Append(ctx, "g", fmt.Appendf(nil, "%d", i), t0.Add(time.Duration(i)*time.Second))
		if err != nil || n != i+1 || !first.Equal(t0) {
			t.Fatalf("Append %d = %d, %v, %v", i, n, first, err)
		}
	}
	got, err := s.Take(ctx, "g", 2)
	if err != nil || len(got) != 2 || string(got[0]) != "0" || string(got[1]) != "1" {
		t.Fatalf("Take 2 = %q, %v", got, err)
	}
	if n, first, _ := s.Append(ctx, "g", []byte("3"), t0.Add(time.Hour)); n != 2 || !first.Equal(t0.Add(2*time.Second)) {
		t.Fatalf("after a partial take: count %d, first %v", n, first)
	}
	if got, err := s.Take(ctx, "g", 0); err != nil || len(got) != 2 || string(got[0]) != "2" || string(got[1]) != "3" {
		t.Fatalf("Take all = %q, %v", got, err)
	}
	if got, err := s.Take(ctx, "g", 5); err != nil || len(got) != 0 {
		t.Fatalf("Take of an empty group = %q, %v", got, err)
	}
	if n, first, _ := s.Append(ctx, "g", []byte("x"), t0.Add(2*time.Hour)); n != 1 || !first.Equal(t0.Add(2*time.Hour)) {
		t.Fatalf("a drained group did not start over: %d, %v", n, first)
	}
	if _, err := s.Take(ctx, "g", 0); err != nil {
		t.Fatal(err)
	}

	const appenders, per, takers = 6, 40, 3
	var seen sync.Map
	var dups, taken atomic.Int64
	record := func(items [][]byte) {
		for _, it := range items {
			if _, dup := seen.LoadOrStore(string(it), true); dup {
				dups.Add(1)
			}
			taken.Add(1)
		}
	}
	var wg sync.WaitGroup
	done := make(chan struct{})
	errc := make(chan error, appenders+takers)
	for a := range appenders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range per {
				if _, _, err := s.Append(ctx, "race", fmt.Appendf(nil, "%d-%d", a, i), t0); err != nil {
					errc <- err
					return
				}
			}
		}()
	}
	var tg sync.WaitGroup
	for range takers {
		tg.Add(1)
		go func() {
			defer tg.Done()
			for {
				items, err := s.Take(ctx, "race", 7)
				if err != nil {
					errc <- err
					return
				}
				record(items)
				select {
				case <-done:
					return
				default:
				}
			}
		}()
	}
	wg.Wait()
	close(done)
	tg.Wait()
	rest, err := s.Take(ctx, "race", 0)
	if err != nil {
		t.Fatal(err)
	}
	record(rest)
	close(errc)
	for err := range errc {
		t.Fatal(err)
	}
	if dups.Load() != 0 || taken.Load() != appenders*per {
		t.Fatalf("taken %d items with %d duplicates; want %d and none", taken.Load(), dups.Load(), appenders*per)
	}
}
