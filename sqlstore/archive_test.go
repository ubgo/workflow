package sqlstore_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/sqlstore"
)

// TestArchiveTableMovesPrunedRuns: with WithArchiveTable a pruned run
// leaves the hot tables and is readable from the cold ones with its
// payload, labels, error and step output; the archive hook still sees the
// chunk; runs retention keeps stay hot and are not archived.
func TestArchiveTableMovesPrunedRuns(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			var hooked int
			r := newRetEnv(t, e, sqlstore.WithArchiveTable(), sqlstore.WithRetention(sqlstore.Retention{Succeeded: time.Hour, Failed: time.Hour, Undelivered: true}),
				sqlstore.WithArchive(func(_ context.Context, runs []sqlstore.FinishedRun) error { hooked += len(runs); return nil }))
			ok := r.finish(workflow.Job{Workflow: "w", Payload: []byte(`"ok"`), Labels: map[string]string{"order": "o1"}}, workflow.StatusSucceeded)
			failed := r.finish(workflow.Job{Workflow: "w", Payload: []byte(`"fail"`)}, workflow.StatusFailed)
			r.clk.Advance(2 * time.Hour)
			fresh := r.finish(workflow.Job{Workflow: "keep"}, workflow.StatusSucceeded)

			if n := r.prune(r.c); n != 2 || r.exists(ok) || r.exists(failed) || !r.exists(fresh) {
				t.Fatalf("prune deleted %d (want 2: the two old runs, not the fresh one)", n)
			}
			if hooked != 2 {
				t.Fatalf("archive hook saw %d runs, want 2", hooked)
			}
			got, err := r.c.ArchivedRuns(r.ctx, sqlstore.ArchiveQuery{})
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 2 {
				t.Fatalf("ArchivedRuns = %d runs, want 2", len(got))
			}
			one, err := r.c.ArchivedRuns(r.ctx, sqlstore.ArchiveQuery{RunID: ok.RunID})
			if err != nil || len(one) != 1 {
				t.Fatalf("ArchivedRuns by RunID = %d, %v; want 1", len(one), err)
			}
			a := one[0]
			if a.Status != workflow.StatusSucceeded || string(a.Payload) != `"ok"` || a.Labels["order"] != "o1" ||
				string(a.Outputs["only"]) != `"out"` || a.Workflow != "w" || a.FinishedAt.IsZero() {
				t.Fatalf("archived run = %+v; want the succeeded run with its payload, label and output", a)
			}
			f, err := r.c.ArchivedRuns(r.ctx, sqlstore.ArchiveQuery{RunID: failed.RunID})
			if err != nil || len(f) != 1 || f[0].Status != workflow.StatusFailed || f[0].Err == "" || len(f[0].Outputs) != 0 {
				t.Fatalf("archived failed run = %+v, %v; want failed with its error and no output", f, err)
			}
			if n := r.prune(r.c); n != 0 {
				t.Fatalf("a second prune deleted %d", n)
			}
			if again, _ := r.c.ArchivedRuns(r.ctx, sqlstore.ArchiveQuery{Workflow: "w", Limit: 10}); len(again) != 2 {
				t.Fatalf("archive holds %d runs of w after a second prune, want 2", len(again))
			}
			if none, _ := r.c.ArchivedRuns(r.ctx, sqlstore.ArchiveQuery{Workflow: "keep"}); len(none) != 0 {
				t.Fatalf("archive holds %d runs of keep, want 0", len(none))
			}
		})
	}
}

// TestPruneWithoutArchiveTableArchivesNothing: the default prune deletes
// and writes nothing to the cold tables.
func TestPruneWithoutArchiveTableArchivesNothing(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			r := newRetEnv(t, e, sqlstore.WithRetention(sqlstore.Retention{Succeeded: time.Hour, Undelivered: true}))
			r.finish(workflow.Job{Workflow: "w", Payload: []byte(`"ok"`)}, workflow.StatusSucceeded)
			r.clk.Advance(2 * time.Hour)
			if n := r.prune(r.c); n != 1 {
				t.Fatalf("prune deleted %d, want 1", n)
			}
			if got, err := r.c.ArchivedRuns(r.ctx, sqlstore.ArchiveQuery{}); err != nil || len(got) != 0 {
				t.Fatalf("ArchivedRuns = %d, %v; want none", len(got), err)
			}
		})
	}
}

// TestFetchBatchOfOneDrains: with one row per dispatcher query, a backlog
// of delayed runs is still admitted and run in full, pass by pass.
func TestFetchBatchOfOneDrains(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			var runs atomic.Int64
			wf := workflow.Workflow{Name: "drain", Steps: []workflow.Step{{Name: "a", Run: func(context.Context, []byte) ([]byte, error) {
				runs.Add(1)
				return nil, nil
			}}}}
			c := newClient(t, e, db, prefix, sqlstore.WithFetchBatch(1))
			ctx := startWith(t, c, wf)
			for range 10 {
				if _, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, RunAt: time.Now().Add(100 * time.Millisecond)}); err != nil {
					t.Fatal(err)
				}
			}
			waitFor(t, 10*time.Second, "all ten runs", func() bool { return runs.Load() == 10 })
		})
	}
}

// TestArchivePruneUnderWriteContention: Prune with WithArchiveTable moves
// runs while other writers keep committing. The move starts with a read of
// the runs it will lock, and on SQLite a transaction that reads first and
// writes second fails at once with SQLITE_BUSY when another connection
// committed in between (the busy timeout does not apply to that upgrade),
// so every chunk could exhaust its retries under load. The chunk must take
// the write lock before it reads.
func TestArchivePruneUnderWriteContention(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			const old = 40
			r := newRetEnv(t, e, sqlstore.WithArchiveTable(), sqlstore.WithPruneChunk(1),
				sqlstore.WithRetention(sqlstore.Retention{Succeeded: time.Hour, Undelivered: true}))
			for i := 0; i < old; i++ {
				r.finish(workflow.Job{Workflow: "w"}, workflow.StatusSucceeded)
			}
			r.clk.Advance(2 * time.Hour)
			stop := make(chan struct{})
			done := make(chan struct{})
			go func() {
				defer close(done)
				for {
					select {
					case <-stop:
						return
					default:
						// Fresh runs commit (and run) while the prune moves old ones.
						_, _ = r.c.Enqueue(r.ctx, workflow.Job{Workflow: "keep"})
					}
				}
			}()
			res, err := r.c.Prune(r.ctx)
			close(stop)
			<-done
			if err != nil {
				t.Fatalf("Prune under write contention: %v", err)
			}
			if res.Deleted != old {
				t.Fatalf("Prune deleted %d, want %d", res.Deleted, old)
			}
		})
	}
}
