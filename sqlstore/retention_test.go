package sqlstore_test

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/sqlstore"
)

// retEnv is one retention test: a started client on a fake clock with two
// workflows, "w" (fails on the payload "fail") and "keep".
type retEnv struct {
	t      *testing.T
	e      engine
	db     *sql.DB
	prefix string
	clk    *clock.Fake
	c      *sqlstore.Client
	ctx    context.Context
}

var retWorkflows = []workflow.Workflow{
	{Name: "w", Steps: []workflow.Step{{Name: "only", Run: func(_ context.Context, in []byte) ([]byte, error) {
		if string(in) == `"fail"` {
			return nil, workflow.Permanent(errors.New("boom"))
		}
		return []byte(`"out"`), nil
	}}}},
	{Name: "keep", Steps: []workflow.Step{{Name: "only", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}},
}

func newRetEnv(t *testing.T, e engine, opts ...sqlstore.Option) *retEnv {
	t.Helper()
	db, prefix := e.open(t)
	r := &retEnv{t: t, e: e, db: db, prefix: prefix, clk: clock.NewFake(start)}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	r.ctx = ctx
	r.c = r.client(opts...)
	return r
}

// client starts another client over the same tables and clock.
func (r *retEnv) client(opts ...sqlstore.Option) *sqlstore.Client {
	r.t.Helper()
	return r.observed(nil, opts...)
}

// observed is client with a run-end observer registered before Start.
func (r *retEnv) observed(fn workflow.RunFinishedFunc, opts ...sqlstore.Option) *sqlstore.Client {
	r.t.Helper()
	c := newClient(r.t, r.e, r.db, r.prefix, append([]sqlstore.Option{sqlstore.WithClock(r.clk), sqlstore.WithoutAutoPrune()}, opts...)...)
	for _, wf := range retWorkflows {
		if err := c.Register(wf); err != nil {
			r.t.Fatal(err)
		}
	}
	if fn != nil {
		if err := c.OnRunFinished(fn); err != nil {
			r.t.Fatal(err)
		}
	}
	if err := c.Start(r.ctx); err != nil {
		r.t.Fatal(err)
	}
	return c
}

// finish enqueues job and waits until its run reaches want.
func (r *retEnv) finish(job workflow.Job, want workflow.Status) workflow.RunRef {
	r.t.Helper()
	// Some cases stand in for package batch, the owner of its labels.
	ref, err := r.c.Enqueue(workflow.AllowReservedPrefix(r.ctx, workflow.LabelPrefixBatch), job)
	if err != nil {
		r.t.Fatal(err)
	}
	if want == workflow.StatusCancelled {
		if err := r.c.Cancel(r.ctx, ref); err != nil {
			r.t.Fatal(err)
		}
	}
	waitFor(r.t, 10*time.Second, "run to end "+string(want), func() bool {
		st, err := r.c.Status(r.ctx, ref)
		return err == nil && st == want
	})
	return ref
}

func (r *retEnv) prune(c *sqlstore.Client) int {
	r.t.Helper()
	res, err := c.Prune(r.ctx)
	if err != nil {
		r.t.Fatalf("Prune: %v", err)
	}
	return res.Deleted
}

// exists reports whether the run is still stored, and fails when its
// steps outlived it.
func (r *retEnv) exists(ref workflow.RunRef) bool {
	r.t.Helper()
	_, err := r.c.Status(r.ctx, ref)
	if err != nil && !errors.Is(err, workflow.ErrNotFound) {
		r.t.Fatal(err)
	}
	var steps int
	if qerr := r.db.QueryRow("SELECT COUNT(*) FROM "+r.e.dialect.QuoteIdent(r.prefix+"steps")+" WHERE run_id = "+r.e.dialect.Placeholder(1),
		string(ref.RunID)).Scan(&steps); qerr != nil {
		r.t.Fatal(qerr)
	}
	if err != nil && steps > 0 {
		r.t.Fatalf("run %s was deleted but %d step rows remain", ref.RunID, steps)
	}
	return err == nil
}

func TestRetentionAgesPerStatusAndOverride(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			r := newRetEnv(t, e, sqlstore.WithRetention(sqlstore.Retention{
				Succeeded: time.Hour, Failed: 2 * time.Hour, // Cancelled: zero, kept forever
				PerWorkflow: map[string]sqlstore.Retention{"keep": {}},
				Undelivered: true,
			}))
			ok := r.finish(workflow.Job{Workflow: "w", Payload: []byte(`"ok"`)}, workflow.StatusSucceeded)
			failed := r.finish(workflow.Job{Workflow: "w", Payload: []byte(`"fail"`)}, workflow.StatusFailed)
			cancelled := r.finish(workflow.Job{Workflow: "w", RunAt: r.clk.Now().Add(time.Minute)}, workflow.StatusCancelled)
			kept := r.finish(workflow.Job{Workflow: "keep"}, workflow.StatusSucceeded)

			if n := r.prune(r.c); n != 0 {
				t.Fatalf("a prune before any retention passed deleted %d runs", n)
			}
			r.clk.Advance(90 * time.Minute)
			if n := r.prune(r.c); n != 1 || r.exists(ok) || !r.exists(failed) {
				t.Fatalf("after 90m: deleted %d; want only the succeeded run (1h) gone", n)
			}
			r.clk.Advance(time.Hour)
			if n := r.prune(r.c); n != 1 || r.exists(failed) {
				t.Fatalf("after 150m: deleted %d; want the failed run (2h) gone", n)
			}
			r.clk.Advance(24 * 365 * time.Hour)
			if n := r.prune(r.c); n != 0 || !r.exists(cancelled) || !r.exists(kept) {
				t.Fatalf("a year on: deleted %d; the cancelled run (zero: forever) and the overridden workflow must stay", n)
			}

			plain := r.client() // no WithRetention
			if n := r.prune(plain); n != 0 {
				t.Fatalf("Prune without WithRetention deleted %d runs", n)
			}
		})
	}
}

func TestRetentionGuards(t *testing.T) {
	keepHour := sqlstore.Retention{Succeeded: time.Hour}
	for _, e := range engines(t) {
		t.Run(e.name+"/undelivered end", func(t *testing.T) {
			r := newRetEnv(t, e, sqlstore.WithRetention(keepHour))
			ref := r.finish(workflow.Job{Workflow: "w"}, workflow.StatusSucceeded)
			r.clk.Advance(2 * time.Hour)
			if n := r.prune(r.c); n != 0 || !r.exists(ref) {
				t.Fatalf("deleted %d runs whose end nobody has observed yet", n)
			}
			delivered := make(chan struct{}, 1)
			obs := r.observed(func(context.Context, workflow.RunEvent) {
				select {
				case delivered <- struct{}{}:
				default:
				}
			}, sqlstore.WithRetention(keepHour))
			<-delivered
			waitFor(t, 5*time.Second, "the run to be pruned once delivered", func() bool {
				res, err := obs.Prune(r.ctx)
				return err == nil && res.Deleted == 1
			})
		})
		t.Run(e.name+"/held idempotency key", func(t *testing.T) {
			r := newRetEnv(t, e, sqlstore.WithRetention(sqlstore.Retention{Succeeded: time.Hour, Undelivered: true}))
			ref := r.finish(workflow.Job{Workflow: "w", IdempotencyKey: "k"}, workflow.StatusSucceeded)
			// A terminal run has released its key; this stands for a row that
			// still holds one, which Prune must never delete.
			if _, err := r.db.Exec("UPDATE "+e.dialect.QuoteIdent(r.prefix+"runs")+" SET idem_active = 'k' WHERE id = "+e.dialect.Placeholder(1),
				string(ref.RunID)); err != nil {
				t.Fatal(err)
			}
			r.clk.Advance(2 * time.Hour)
			if n := r.prune(r.c); n != 0 || !r.exists(ref) {
				t.Fatalf("deleted %d runs holding an idempotency key", n)
			}
		})
		t.Run(e.name+"/unfinished batch", func(t *testing.T) {
			r := newRetEnv(t, e, sqlstore.WithRetention(sqlstore.Retention{Succeeded: time.Hour, Undelivered: true}))
			bs := r.c.BatchStore()
			if err := bs.Create(r.ctx, batch.State{ID: "b1", Jobs: make([]batch.JobState, 2)}); err != nil {
				t.Fatal(err)
			}
			labels := map[string]string{batch.LabelBatch: "b1", batch.LabelIndex: "0"}
			ref := r.finish(workflow.Job{Workflow: "w", Labels: labels}, workflow.StatusSucceeded)
			r.clk.Advance(2 * time.Hour)
			if n := r.prune(r.c); n != 0 || !r.exists(ref) {
				t.Fatalf("deleted %d runs of an unfinished batch", n)
			}
			if un, err := bs.Unfinished(r.ctx); err != nil || len(un) != 1 {
				t.Fatalf("Unfinished = %d batches, %v; want 1", len(un), err)
			}
			if _, err := bs.Update(r.ctx, "b1", func(st *batch.State) error { st.Finished = true; return nil }); err != nil {
				t.Fatal(err)
			}
			if un, err := bs.Unfinished(r.ctx); err != nil || len(un) != 0 {
				t.Fatalf("Unfinished after finishing = %d batches, %v; want 0", len(un), err)
			}
			if n := r.prune(r.c); n != 1 || r.exists(ref) {
				t.Fatalf("deleted %d runs once their batch finished, want 1", n)
			}
		})
	}
}

func TestRetentionArchiveAndChunks(t *testing.T) {
	const runs = 230 // three chunks: 100, 100, 30
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			var mu sync.Mutex
			var chunks []int
			var refuse bool
			var outputs int
			archive := func(_ context.Context, rs []sqlstore.FinishedRun) error {
				mu.Lock()
				defer mu.Unlock()
				if refuse {
					return errors.New("cold storage is down")
				}
				chunks = append(chunks, len(rs))
				for _, fr := range rs {
					if string(fr.Outputs["only"]) == `"out"` && fr.Workflow == "w" && fr.Status == workflow.StatusSucceeded && !fr.FinishedAt.IsZero() {
						outputs++
					}
				}
				return nil
			}
			r := newRetEnv(t, e, sqlstore.WithRetention(sqlstore.Retention{Succeeded: time.Hour, Undelivered: true}), sqlstore.WithArchive(archive))
			refs := make([]workflow.RunRef, 0, runs)
			for range runs {
				ref, err := r.c.Enqueue(r.ctx, workflow.Job{Workflow: "w"})
				if err != nil {
					t.Fatal(err)
				}
				refs = append(refs, ref)
			}
			for _, ref := range refs {
				waitFor(t, 30*time.Second, "every run to succeed", func() bool {
					st, err := r.c.Status(r.ctx, ref)
					return err == nil && st == workflow.StatusSucceeded
				})
			}
			r.clk.Advance(2 * time.Hour)

			mu.Lock()
			refuse = true
			mu.Unlock()
			if res, err := r.c.Prune(r.ctx); err == nil || res.Deleted != 0 || !r.exists(refs[0]) {
				t.Fatalf("Prune with a refusing archive: deleted %d, err %v; want nothing deleted and the error", res.Deleted, err)
			}
			mu.Lock()
			refuse = false
			mu.Unlock()

			if n := r.prune(r.c); n != runs {
				t.Fatalf("deleted %d runs, want %d", n, runs)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(chunks) != 3 || chunks[0] != 100 || chunks[1] != 100 || chunks[2] != 30 {
				t.Fatalf("archive chunks = %v, want [100 100 30]", chunks)
			}
			if outputs != runs {
				t.Fatalf("%d archived runs carried their step output, want %d", outputs, runs)
			}
		})
	}
}

func TestRetentionTwoClientsPruneAtOnce(t *testing.T) {
	const runs = 150
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			ret := sqlstore.WithRetention(sqlstore.Retention{Succeeded: time.Hour, Undelivered: true})
			r := newRetEnv(t, e, ret)
			other := r.client(ret)
			refs := make([]workflow.RunRef, 0, runs)
			for range runs {
				ref, err := r.c.Enqueue(r.ctx, workflow.Job{Workflow: "w"})
				if err != nil {
					t.Fatal(err)
				}
				refs = append(refs, ref)
			}
			for _, ref := range refs {
				waitFor(t, 30*time.Second, "every run to succeed", func() bool {
					st, err := r.c.Status(r.ctx, ref)
					return err == nil && st == workflow.StatusSucceeded
				})
			}
			r.clk.Advance(2 * time.Hour)
			var wg sync.WaitGroup
			counts := make([]int, 2)
			errs := make([]error, 2)
			for i, c := range []*sqlstore.Client{r.c, other} {
				wg.Add(1)
				go func() {
					defer wg.Done()
					res, err := c.Prune(r.ctx)
					counts[i], errs[i] = res.Deleted, err
				}()
			}
			wg.Wait()
			if errs[0] != nil || errs[1] != nil {
				t.Fatalf("concurrent Prune errors: %v, %v", errs[0], errs[1])
			}
			if counts[0]+counts[1] != runs {
				t.Fatalf("the two prunes deleted %d + %d runs, want %d in total", counts[0], counts[1], runs)
			}
		})
	}
}

// TestAutoPruneRuns checks the background loop deletes without a Prune
// call, on the one client that takes the prune lock.
func TestAutoPruneRuns(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			clk := clock.NewFake(start)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c := newClient(t, e, db, prefix, sqlstore.WithClock(clk),
				sqlstore.WithRetention(sqlstore.Retention{Succeeded: time.Hour, Undelivered: true}),
				sqlstore.WithPruneInterval(50*time.Millisecond))
			for _, wf := range retWorkflows {
				if err := c.Register(wf); err != nil {
					t.Fatal(err)
				}
			}
			if err := c.Start(ctx); err != nil {
				t.Fatal(err)
			}
			ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "w"})
			if err != nil {
				t.Fatal(err)
			}
			waitFor(t, 5*time.Second, "the run to succeed", func() bool {
				st, err := c.Status(ctx, ref)
				return err == nil && st == workflow.StatusSucceeded
			})
			clk.Advance(2 * time.Hour)
			waitFor(t, 5*time.Second, "the background prune", func() bool {
				_, err := c.Status(ctx, ref)
				return errors.Is(err, workflow.ErrNotFound)
			})
		})
	}
}

// TestPruneReportsHeldRuns checks every guard's hold is counted, so a
// retention that can never delete anything is visible: the no-observer
// trap shows as HeldUndelivered.
func TestPruneReportsHeldRuns(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			r := newRetEnv(t, e, sqlstore.WithRetention(sqlstore.Retention{Succeeded: time.Hour}))
			if err := r.c.BatchStore().Create(r.ctx, batch.State{ID: "b1", Jobs: make([]batch.JobState, 1)}); err != nil {
				t.Fatal(err)
			}
			r.finish(workflow.Job{Workflow: "w"}, workflow.StatusSucceeded)
			r.finish(workflow.Job{Workflow: "w"}, workflow.StatusSucceeded)
			keyed := r.finish(workflow.Job{Workflow: "w"}, workflow.StatusSucceeded)
			if _, err := r.db.Exec("UPDATE "+e.dialect.QuoteIdent(r.prefix+"runs")+" SET idem_active = 'k', notified = 1 WHERE id = "+e.dialect.Placeholder(1),
				string(keyed.RunID)); err != nil {
				t.Fatal(err)
			}
			inBatch := r.finish(workflow.Job{Workflow: "w", Labels: map[string]string{batch.LabelBatch: "b1", batch.LabelIndex: "0"}}, workflow.StatusSucceeded)
			if _, err := r.db.Exec("UPDATE "+e.dialect.QuoteIdent(r.prefix+"runs")+" SET notified = 1 WHERE id = "+e.dialect.Placeholder(1),
				string(inBatch.RunID)); err != nil {
				t.Fatal(err)
			}
			r.finish(workflow.Job{Workflow: "w"}, workflow.StatusSucceeded) // a third undelivered run
			r.clk.Advance(2 * time.Hour)
			r.finish(workflow.Job{Workflow: "w"}, workflow.StatusSucceeded) // too young to count
			res, err := r.c.Prune(r.ctx)
			if err != nil {
				t.Fatal(err)
			}
			want := sqlstore.PruneResult{Deleted: 0, HeldUndelivered: 3, HeldIdempotency: 1, HeldBatch: 1}
			if res != want {
				t.Fatalf("PruneResult = %+v, want %+v", res, want)
			}
		})
	}
}
