package sqlstore_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
)

// TestBatchStorePendingKeepsBatchOpen: a finished batch with a callback
// still pending is open (State.Open), so Unfinished returns it, and drops it
// once the callback has landed and Pending is empty.
func TestBatchStorePendingKeepsBatchOpen(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			bs := newClient(t, e, db, prefix).BatchStore()
			ctx := context.Background()
			if err := bs.Create(ctx, batch.State{ID: "b1", Finished: true, Pending: []batch.PendingCallback{{Kind: batch.CallbackThen, Workflow: "then"}}}); err != nil {
				t.Fatalf("Create: %v", err)
			}
			if err := bs.Create(ctx, batch.State{ID: "b2", Finished: true}); err != nil {
				t.Fatalf("Create: %v", err)
			}
			open, err := bs.Unfinished(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(open) != 1 || open[0].ID != "b1" || len(open[0].Pending) != 1 {
				t.Fatalf("Unfinished = %+v, want only b1 with its pending callback", open)
			}
			if _, err := bs.Update(ctx, "b1", func(st *batch.State) error { st.Pending = nil; return nil }); err != nil {
				t.Fatalf("Update: %v", err)
			}
			if open, err = bs.Unfinished(ctx); err != nil || len(open) != 0 {
				t.Fatalf("Unfinished after the callback landed = %+v, %v; want none", open, err)
			}
		})
	}
}

// TestBatchStorePrune: Prune deletes exactly what batch.PruneFilter.Matches
// accepts (closed batches finished before the cut, open ones created
// before theirs, cancelled ones cancelled before theirs), pages through
// the table, and a zero filter deletes nothing.
func TestBatchStorePrune(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			bs := newClient(t, e, db, prefix).BatchStore()
			ctx := context.Background()
			cut := start
			old, recent := cut.Add(-time.Hour), cut.Add(time.Hour)
			states := []batch.State{
				{ID: "closed-old", Finished: true, CreatedAt: old, FinishedAt: old},
				{ID: "closed-new", Finished: true, CreatedAt: old, FinishedAt: recent},
				{ID: "open-old", CreatedAt: old},
				{ID: "open-new", CreatedAt: recent},
				{ID: "cancelled-old", CreatedAt: recent, Cancelled: true, CancelledAt: old},
			}
			for _, st := range states {
				if err := bs.Create(ctx, st); err != nil {
					t.Fatal(err)
				}
			}
			if n, err := bs.Prune(ctx, batch.PruneFilter{}); err != nil || n != 0 {
				t.Fatalf("Prune(zero) = %d, %v; want 0", n, err)
			}
			n, err := bs.Prune(ctx, batch.PruneFilter{FinishedBefore: cut, UnfinishedBefore: cut, CancelledBefore: cut})
			if err != nil || n != 3 {
				t.Fatalf("Prune = %d, %v; want 3", n, err)
			}
			for _, st := range states {
				_, err := bs.Get(ctx, st.ID)
				gone := errors.Is(err, workflow.ErrNotFound)
				want := st.ID == "closed-old" || st.ID == "open-old" || st.ID == "cancelled-old"
				if gone != want {
					t.Errorf("batch %s gone = %v (%v), want %v", st.ID, gone, err, want)
				}
			}
		})
	}
}
