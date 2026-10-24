package sqlstore_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/sqlstore"
)

// The tests in this file cover what the conformance suite cannot see from
// one client: Retrying and Discarded read from another client, the 004
// rewrite of discards stored by earlier releases, a chain that survives a
// crash and is handed on by a client that registers its next workflow,
// worker scaling, and the closure refusal.

// TestRunStatesAcrossClients: a run in its retry backoff reads Retrying,
// with its next attempt, on a client that does not run it, and a discarded
// run reads Discarded there with its reason.
func TestRunStatesAcrossClients(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			var calls atomic.Int64
			retry := workflow.Workflow{Name: "retry", Retry: workflow.RetryPolicy{Max: 3, Backoff: workflow.Backoff{Initial: time.Hour, Factor: 1}},
				Steps: []workflow.Step{{Name: "a", Run: func(context.Context, []byte) ([]byte, error) {
					calls.Add(1)
					return nil, errors.New("boom")
				}}}}
			discard := workflow.Workflow{Name: "discard", Steps: []workflow.Step{{Name: "a", Run: func(context.Context, []byte) ([]byte, error) {
				return nil, workflow.Discard(errors.New("stale"))
			}}}}
			runner := newClient(t, e, db, prefix)
			ctx := startWith(t, runner, retry, discard)
			reader := newClient(t, e, db, prefix)
			startWith(t, reader, workflow.Workflow{Name: "other", Steps: []workflow.Step{{Name: "a",
				Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}})

			before := time.Now()
			ref, err := runner.Enqueue(ctx, workflow.Job{Workflow: retry.Name})
			if err != nil {
				t.Fatal(err)
			}
			waitStatus(t, reader, ref, workflow.StatusRetrying)
			page, err := reader.Runs(ctx, workflow.RunQuery{Ref: ref})
			if err != nil || len(page.Runs) != 1 {
				t.Fatalf("Runs(ref) = %v, %v", page.Runs, err)
			}
			info := page.Runs[0]
			if info.Status != workflow.StatusRetrying || info.NextAttempt != 2 || info.NextAttemptAt.Before(before.Add(time.Hour)) {
				t.Fatalf("RunInfo = %s attempt %d at %s; want retrying, attempt 2, an hour on", info.Status, info.NextAttempt, info.NextAttemptAt)
			}
			if info.Error != "boom" || info.FailedStep != "" {
				t.Fatalf("RunInfo.Error %q, FailedStep %q while retrying; want the attempt's error and no failed step", info.Error, info.FailedStep)
			}

			dref, err := runner.Enqueue(ctx, workflow.Job{Workflow: discard.Name})
			if err != nil {
				t.Fatal(err)
			}
			waitStatus(t, reader, dref, workflow.StatusDiscarded)
			page, err = reader.Runs(ctx, workflow.RunQuery{Statuses: []workflow.Status{workflow.StatusDiscarded}})
			if err != nil || len(page.Runs) != 1 || page.Runs[0].Ref.RunID != dref.RunID || page.Runs[0].Error == "" {
				t.Fatalf("Runs(discarded) = %v, %v; want the discarded run with its reason", page.Runs, err)
			}
			page, err = reader.Runs(ctx, workflow.RunQuery{Statuses: []workflow.Status{workflow.StatusCancelled}})
			if err != nil || len(page.Runs) != 0 {
				t.Fatalf("Runs(cancelled) = %v, %v; a discarded run is not cancelled", page.Runs, err)
			}
			if _, err := reader.Retry(ctx, dref); !errors.Is(err, workflow.ErrWrongState) {
				t.Fatalf("Retry of a discarded run: %v, want ErrWrongState", err)
			}
		})
	}
}

// TestMigration004RewritesDiscards: a run an earlier release discarded
// (stored cancelled, run_ext.discarded = 1) reads Discarded once 004 is
// applied, and a plain cancelled run stays cancelled.
func TestMigration004RewritesDiscards(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()
			db, prefix := e.open(t)
			applyScripts(t, e, db, prefix, func(m sqlstore.MigrationScript) bool { return m.Name != "004_run_states_chain" })
			q := e.dialect.QuoteIdent
			p := e.dialect.Placeholder
			for i, id := range []string{"run_discarded", "run_cancelled"} {
				ins := "INSERT INTO " + q(prefix+"runs") + " (id, workflow, labels, priority, bucket, rate_keys, status, run_at, sig_floor," +
					" created_at, finished_at, err, fail_until, notified, notify_until, ver) VALUES (" + p(1) + ", 'old', '{}', 0, '', '[]', 'cancelled', 0, 0, 1, 2, 'x', 0, 1, 0, 0)"
				if _, err := db.Exec(ins, id); err != nil {
					t.Fatal(err)
				}
				ext := "INSERT INTO " + q(prefix+"run_ext") + " (run_id, queue, started_at, discarded) VALUES (" + p(1) + ", 'default', 0, " + p(2) + ")"
				if _, err := db.Exec(ext, id, 1-i); err != nil {
					t.Fatal(err)
				}
			}
			applyScripts(t, e, db, prefix, func(m sqlstore.MigrationScript) bool { return m.Name == "004_run_states_chain" })
			c := newClient(t, e, db, prefix)
			startWith(t, c, workflow.Workflow{Name: "old", Steps: []workflow.Step{{Name: "a",
				Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}})
			for id, want := range map[string]workflow.Status{"run_discarded": workflow.StatusDiscarded, "run_cancelled": workflow.StatusCancelled} {
				st, err := c.Status(ctx, workflow.RunRef{RunID: workflow.RunID(id)})
				if err != nil || st != want {
					t.Fatalf("%s after 004: %s, %v; want %s", id, st, err, want)
				}
			}
		})
	}
}

// chainRec records the order chained jobs ran in.
type chainRec struct {
	mu   sync.Mutex
	seen []string
}

func (r *chainRec) wf(name string, run func(ctx context.Context) error) workflow.Workflow {
	return workflow.Workflow{Name: name, Steps: []workflow.Step{{Name: "a", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		if run != nil {
			if err := run(ctx); err != nil {
				return nil, err
			}
		}
		r.mu.Lock()
		r.seen = append(r.seen, name)
		r.mu.Unlock()
		return in, nil
	}}}}
}

func (r *chainRec) got() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.seen)
}

// TestChainSurvivesCrashAndHandsOn: the client running a chained job dies
// mid-step; another client takes the step over after its lease, the step
// edits the chain and succeeds, and the chain goes on. Its next job's
// workflow is registered only on a third client, which hands it on from
// the stored row.
func TestChainSurvivesCrashAndHandsOn(t *testing.T) {
	const lease = 400 * time.Millisecond
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			var rec chainRec
			var execs atomic.Int64
			first := rec.wf("first", func(ctx context.Context) error {
				if execs.Add(1) == 1 {
					<-ctx.Done() // the worker "dies" here (Crash); it writes nothing more
					return ctx.Err()
				}
				return workflow.PrependToChain(ctx, workflow.Job{Workflow: "middle"})
			})
			middle, last := rec.wf("middle", nil), rec.wf("last", nil)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			mk := func(wfs ...workflow.Workflow) *sqlstore.Client {
				c := newClient(t, e, db, prefix, sqlstore.WithLease(lease))
				for _, wf := range wfs {
					if err := c.Register(wf); err != nil {
						t.Fatal(err)
					}
				}
				if err := c.Start(ctx); err != nil {
					t.Fatal(err)
				}
				return c
			}
			a := mk(first, middle)
			if _, err := a.Enqueue(ctx, workflow.Job{Workflow: first.Name, Chain: []workflow.Job{{Workflow: last.Name}}}); err != nil {
				t.Fatal(err)
			}
			waitFor(t, 10*time.Second, "the first attempt", func() bool { return execs.Load() >= 1 })
			sqlstore.Crash(a)
			mk(first, middle)
			waitFor(t, 10*time.Second, "first and middle to run", func() bool { return len(rec.got()) >= 2 })
			time.Sleep(10 * testPoll) // no client registers last yet: the chain waits in its row
			if got := rec.got(); !slices.Equal(got, []string{"first", "middle"}) {
				t.Fatalf("before a client registers last: ran %v, want first middle", got)
			}
			mk(last)
			waitFor(t, 10*time.Second, "last to run", func() bool { return len(rec.got()) >= 3 })
			time.Sleep(10 * testPoll)
			if got := rec.got(); !slices.Equal(got, []string{"first", "middle", "last"}) {
				t.Fatalf("chain ran %v, want first middle last, each once", got)
			}
		})
	}
}

// TestChainResumesAfterRetry: a failed run keeps its chain, so retrying it
// (RunRetrier) continues the chain once it succeeds.
func TestChainResumesAfterRetry(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			var rec chainRec
			var fail atomic.Bool
			fail.Store(true)
			first := rec.wf("first", func(context.Context) error {
				if fail.Load() {
					return workflow.Permanent(errors.New("boom"))
				}
				return nil
			})
			next := rec.wf("next", nil)
			c := newClient(t, e, db, prefix)
			ctx := startWith(t, c, first, next)
			ref, err := c.Enqueue(ctx, workflow.Job{Workflow: first.Name, Chain: []workflow.Job{{Workflow: next.Name}}})
			if err != nil {
				t.Fatal(err)
			}
			waitStatus(t, c, ref, workflow.StatusFailed)
			time.Sleep(10 * testPoll)
			if got := rec.got(); len(got) != 0 {
				t.Fatalf("a failed run's chain ran %v", got)
			}
			fail.Store(false)
			if _, err := c.Retry(ctx, ref); err != nil {
				t.Fatal(err)
			}
			waitFor(t, 5*time.Second, "the chain to go on", func() bool { return len(rec.got()) == 2 })
			if got := rec.got(); !slices.Equal(got, []string{"first", "next"}) {
				t.Fatalf("after Retry: ran %v, want first next", got)
			}
		})
	}
}

// TestWorkerScaler: QueueLoads reports the waiting depth, and SetWorkers
// changes how many attempts of a queue run at once on this client, both
// ways, with the contract's refusals.
func TestWorkerScaler(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			var now, peak atomic.Int64
			release := make(chan struct{})
			wf := workflow.Workflow{Name: "scale", Queue: "bulk", Steps: []workflow.Step{{Name: "a", Run: func(ctx context.Context, in []byte) ([]byte, error) {
				n := now.Add(1)
				for {
					p := peak.Load()
					if n <= p || peak.CompareAndSwap(p, n) {
						break
					}
				}
				defer now.Add(-1)
				select {
				case <-release:
				case <-ctx.Done():
				}
				return in, nil
			}}}}
			c := newClient(t, e, db, prefix, sqlstore.WithWorkers(4),
				sqlstore.WithQueues(workflow.QueueConfig{Name: "bulk", Workers: 1}, workflow.QueueConfig{Name: "aaa", Workers: 2}))
			var _ workflow.WorkerScaler = c
			if !c.Capabilities().Has(workflow.CapWorkerScale) {
				t.Fatal("sqlstore does not declare CapWorkerScale")
			}
			if _, err := c.QueueLoads(context.Background()); !errors.Is(err, workflow.ErrNotStarted) {
				t.Fatalf("QueueLoads before Start: %v, want ErrNotStarted", err)
			}
			ctx := startWith(t, c, wf)
			if err := c.SetWorkers(ctx, "bulk", 0); !errors.Is(err, workflow.ErrInvalid) {
				t.Fatalf("SetWorkers(0): %v, want ErrInvalid", err)
			}
			if err := c.SetWorkers(ctx, "nope", 2); !errors.Is(err, workflow.ErrNotFound) {
				t.Fatalf("SetWorkers(unknown queue): %v, want ErrNotFound", err)
			}
			for range 4 {
				if _, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name}); err != nil {
					t.Fatal(err)
				}
			}
			waitFor(t, 5*time.Second, "one attempt running", func() bool { return now.Load() == 1 })
			var loads []workflow.QueueLoad
			waitFor(t, 5*time.Second, "three waiting", func() bool {
				var err error
				loads, err = c.QueueLoads(ctx)
				return err == nil && len(loads) == 2 && loads[1].Depth == 3
			})
			if loads[0].Queue != "aaa" || loads[1].Queue != "bulk" || loads[1].Workers != 1 || loads[1].Running != 1 {
				t.Fatalf("loads = %+v; want aaa then bulk with 1 worker running 1", loads)
			}
			// The balancer's answer is what the app's loop applies.
			want := workflow.ByDepth(workflow.BalanceConfig{Total: 4}).Balance(loads)
			if want["bulk"] <= 1 {
				t.Fatalf("ByDepth gave bulk %d workers with 3 waiting", want["bulk"])
			}
			if err := c.SetWorkers(ctx, "bulk", 3); err != nil {
				t.Fatal(err)
			}
			waitFor(t, 5*time.Second, "three attempts at once", func() bool { return now.Load() == 3 })
			if err := c.SetWorkers(ctx, "bulk", 1); err != nil {
				t.Fatal(err)
			}
			close(release)
			waitFor(t, 5*time.Second, "the queue to drain", func() bool {
				l, err := c.QueueLoads(ctx)
				return err == nil && l[1].Depth == 0 && now.Load() == 0
			})
			if p := peak.Load(); p != 3 {
				t.Fatalf("peak %d attempts at once, want 3 (the raised cap)", p)
			}
		})
	}
}

// TestClosureRefused: a Go function cannot be stored, so the closure
// helper refuses this driver.
func TestClosureRefused(t *testing.T) {
	e := engines(t)[0]
	db, prefix := e.open(t)
	c := newClient(t, e, db, prefix)
	ctx := startWith(t, c, workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "a",
		Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}})
	if c.Capabilities().Has(workflow.CapClosure) {
		t.Fatal("sqlstore declares CapClosure")
	}
	_, err := workflow.EnqueueClosure(ctx, c, workflow.ClosureJob{Name: "x", Fn: func(context.Context) error { return nil }})
	var ue *workflow.UnsupportedError
	if !errors.As(err, &ue) || ue.Missing != workflow.CapClosure {
		t.Fatalf("EnqueueClosure: %v, want an UnsupportedError missing CapClosure", err)
	}
}
