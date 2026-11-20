package enginetest

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow/middleware"
	"github.com/ubgo/workflow/sqlstore"
	"github.com/ubgo/workflow/workflowtest"
)

// roundRobin spreads calls over several stores over one database, so the
// contract's concurrent Appends and Takes come from what are, to the
// database, separate processes.
type roundRobin struct {
	stores []*sqlstore.AggregateStore
	n      atomic.Uint64
}

func (r *roundRobin) next() *sqlstore.AggregateStore {
	return r.stores[r.n.Add(1)%uint64(len(r.stores))]
}

func (r *roundRobin) Append(ctx context.Context, group string, item []byte, now time.Time) (int, time.Time, error) {
	return r.next().Append(ctx, group, item, now)
}

func (r *roundRobin) Take(ctx context.Context, group string, limit int) ([][]byte, error) {
	return r.next().Take(ctx, group, limit)
}

// TestAggregateStore runs the AggregateStore contract on every engine, with
// four stores over one table set: Append's count and first, Take's order
// and limit, and every item taken exactly once while appenders and takers
// race on one group. It also checks that PruneEmpty removes only the rows
// of drained groups and that the store's migration is its own list.
func TestAggregateStore(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()
			db, prefix := e.open(t)
			t.Cleanup(func() {
				for _, tbl := range []string{"aggregate_items", "aggregate_groups"} {
					_, _ = db.Exec("DROP TABLE IF EXISTS " + e.dialect.QuoteIdent(prefix+tbl))
				}
			})
			rr := &roundRobin{}
			for range 4 {
				s, err := sqlstore.NewAggregateStore(db, e.dialect, sqlstore.WithTablePrefix(prefix))
				if err != nil {
					t.Fatal(err)
				}
				rr.stores = append(rr.stores, s)
			}
			if err := rr.stores[0].Migrate(ctx); err != nil {
				t.Fatal(err)
			}
			if err := rr.stores[1].Migrate(ctx); err != nil {
				t.Fatalf("a second Migrate: %v", err)
			}
			scripts := rr.stores[0].MigrationScripts()
			if len(scripts) != 1 || scripts[0].Owner != sqlstore.AggregateMigrationOwner || scripts[0].Name != "001_aggregate" {
				t.Fatalf("migration scripts = %+v", scripts)
			}
			workflowtest.AggregateStoreContract(t, rr)

			if _, _, err := rr.Append(ctx, "kept", []byte("x"), time.Unix(1, 0)); err != nil {
				t.Fatal(err)
			}
			// The contract drained "g" and "race"; their rows go, "kept"'s stays.
			if n, err := rr.stores[0].PruneEmpty(ctx); err != nil || n != 2 {
				t.Fatalf("PruneEmpty = %d, %v; want the two drained groups", n, err)
			}
			var groups int
			if err := db.QueryRow("SELECT COUNT(*) FROM " + e.dialect.QuoteIdent(prefix+"aggregate_groups")).Scan(&groups); err != nil {
				t.Fatal(err)
			}
			if groups != 1 {
				t.Fatalf("%d group rows after PruneEmpty, want 1", groups)
			}
			if got, err := rr.Take(ctx, "kept", 0); err != nil || len(got) != 1 {
				t.Fatalf("Take after PruneEmpty = %q, %v", got, err)
			}
			// A large item and an empty one round-trip unchanged.
			big := make([]byte, 1<<16)
			for i := range big {
				big[i] = byte(i)
			}
			for _, it := range [][]byte{big, {}} {
				if _, _, err := rr.Append(ctx, "bytes", it, time.Unix(1, 0)); err != nil {
					t.Fatal(err)
				}
			}
			got, err := rr.Take(ctx, "bytes", 0)
			if err != nil || len(got) != 2 || string(got[0]) != string(big) || len(got[1]) != 0 {
				t.Fatalf("bytes round trip: %d items, %v", len(got), err)
			}
		})
	}
}

var _ middleware.AggregateStore = (*roundRobin)(nil)
