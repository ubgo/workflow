package sqlstore_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/sqlstore"
)

// The tests in this file run several clients over one database, which is
// how a cluster of workers looks to the driver, and prove the guarantees a
// single client cannot show: a claim runs once, a cron tick fires once, a
// live worker keeps its lease, a dead one loses it, and run ends are
// delivered durably.

// nodes is how many clients each cluster test runs.
const nodes = 3

// waitFor polls cond every few milliseconds until it holds or limit
// passes.
func waitFor(t *testing.T, limit time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", limit, what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// startCluster builds n clients over db with wf registered on each and
// starts them; extra options apply to every client.
func startCluster(t *testing.T, e engine, db *sql.DB, prefix string, wf workflow.Workflow, opts ...sqlstore.Option) []*sqlstore.Client {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	out := make([]*sqlstore.Client, nodes)
	for i := range out {
		c := newClient(t, e, db, prefix, opts...)
		if err := c.Register(wf); err != nil {
			t.Fatalf("Register: %v", err)
		}
		if err := c.Start(ctx); err != nil {
			t.Fatalf("Start: %v", err)
		}
		out[i] = c
	}
	return out
}

// perRun counts executions per run ID.
type perRun struct {
	mu sync.Mutex
	n  map[workflow.RunID]int
}

func (p *perRun) hit(ctx context.Context) {
	info, _ := workflow.StepInfoFrom(ctx)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.n == nil {
		p.n = map[workflow.RunID]int{}
	}
	p.n[info.RunID]++
}

func (p *perRun) snapshot() map[workflow.RunID]int {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[workflow.RunID]int, len(p.n))
	for k, v := range p.n {
		out[k] = v
	}
	return out
}

// TestClusterClaimsEachStepOnce enqueues many runs on one client of three
// and checks every step ran exactly once, on whichever client claimed it.
// It is the test that fails if the claim (SKIP LOCKED, or SQLite's
// conditional update) lets two workers take one row.
//
// No worker dies here, so the lease is long: a step whose lease expires
// runs again by design (at least once), and on a loaded machine the
// renewals of a short lease can wait out the lease on a busy database. A
// second execution under a 30-second lease is a double claim, not an
// expiry.
func TestClusterClaimsEachStepOnce(t *testing.T) {
	const lease = 30 * time.Second
	const runs = 60
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			var seen perRun
			wf := workflow.Workflow{Name: "claim", Steps: []workflow.Step{{Name: "only", Run: func(ctx context.Context, in []byte) ([]byte, error) {
				seen.hit(ctx)
				time.Sleep(2 * time.Millisecond) // long enough for claimers to overlap
				return in, nil
			}}}}
			cs := startCluster(t, e, db, prefix, wf, sqlstore.WithLease(lease))
			refs := make([]workflow.RunRef, 0, runs)
			for i := range runs {
				ref, err := cs[i%nodes].Enqueue(context.Background(), workflow.Job{Workflow: wf.Name})
				if err != nil {
					t.Fatalf("Enqueue: %v", err)
				}
				refs = append(refs, ref)
			}
			waitFor(t, 30*time.Second, "every run to succeed", func() bool {
				for _, ref := range refs {
					st, err := cs[0].Status(context.Background(), ref)
					if err != nil || st != workflow.StatusSucceeded {
						return false
					}
				}
				return true
			})
			got := seen.snapshot()
			if len(got) != runs {
				t.Fatalf("%d runs executed, want %d", len(got), runs)
			}
			for id, n := range got {
				if n != 1 {
					t.Errorf("run %s executed %d times, want exactly 1", id, n)
				}
			}
		})
	}
}

// TestClusterCronTickFiresOnce declares one cron on three clients sharing
// a clock and a database, and checks each tick enqueues exactly one run.
// It is the test that fails if the tick row stops being the guard.
func TestClusterCronTickFiresOnce(t *testing.T) {
	const ticks = 4
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			clk := clock.NewFake(start)
			var runs atomic.Int64
			wf := workflow.Workflow{Name: "tick", Cron: []string{"* * * * *"}, Steps: []workflow.Step{{Name: "only",
				Run: func(_ context.Context, in []byte) ([]byte, error) { runs.Add(1); return in, nil }}}}
			startCluster(t, e, db, prefix, wf, sqlstore.WithClock(clk))
			for n := 1; n <= ticks; n++ {
				// Every client arms a timer for the next fire; waiting for all of
				// them means every client sees the tick due at the same moment.
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if err := clk.BlockUntil(ctx, nodes); err != nil {
					cancel()
					t.Fatalf("waiting for every client's cron timer: %v", err)
				}
				cancel()
				clk.Advance(time.Minute)
				want := int64(n)
				waitFor(t, 5*time.Second, fmt.Sprintf("tick %d to run", n), func() bool { return runs.Load() >= want })
				time.Sleep(100 * time.Millisecond) // room for a second fire of the same tick
				if got := runs.Load(); got != want {
					t.Fatalf("after %d ticks on %d clients, %d runs fired, want %d", n, nodes, got, want)
				}
			}
			var rows int64
			if err := db.QueryRow("SELECT COUNT(*) FROM " + e.dialect.QuoteIdent(prefix+"ticks")).Scan(&rows); err != nil {
				t.Fatalf("count ticks: %v", err)
			}
			if rows != ticks {
				t.Fatalf("%d tick rows, want one per tick (%d)", rows, ticks)
			}
		})
	}
}

// TestClusterLiveLeaseIsKept runs a step for several lease lengths on one
// of three clients and checks no other client takes it over: the renewal
// keeps a live worker's claim. It is the test that fails if the heartbeat
// stops renewing.
func TestClusterLiveLeaseIsKept(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			var execs atomic.Int64
			wf := workflow.Workflow{Name: "long", Steps: []workflow.Step{{Name: "only", Run: func(ctx context.Context, in []byte) ([]byte, error) {
				execs.Add(1)
				select {
				case <-time.After(3 * testLease):
					return in, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}}}}
			cs := startCluster(t, e, db, prefix, wf)
			ref, err := cs[0].Enqueue(context.Background(), workflow.Job{Workflow: wf.Name})
			if err != nil {
				t.Fatalf("Enqueue: %v", err)
			}
			waitFor(t, 10*time.Second, "the run to succeed", func() bool {
				st, err := cs[1].Status(context.Background(), ref)
				return err == nil && st == workflow.StatusSucceeded
			})
			if got := execs.Load(); got != 1 {
				t.Fatalf("a step running for 3 leases executed %d times, want 1 (its lease must be renewed)", got)
			}
		})
	}
}

// TestCancelReachesAnotherClient cancels a run from a client that is not
// executing it: the executing client's handler must see its context
// cancelled (at its next lease renewal).
func TestCancelReachesAnotherClient(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			started := make(chan struct{}, 1)
			stopped := make(chan error, 1)
			wf := workflow.Workflow{Name: "cancel", Steps: []workflow.Step{{Name: "only", Run: func(ctx context.Context, in []byte) ([]byte, error) {
				started <- struct{}{}
				<-ctx.Done()
				stopped <- context.Cause(ctx)
				return nil, ctx.Err()
			}}}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			worker := newClient(t, e, db, prefix)
			if err := worker.Register(wf); err != nil {
				t.Fatal(err)
			}
			if err := worker.Start(ctx); err != nil {
				t.Fatal(err)
			}
			ref, err := worker.Enqueue(ctx, workflow.Job{Workflow: wf.Name})
			if err != nil {
				t.Fatal(err)
			}
			<-started
			// The canceller registers no workflow, so it never claims the step.
			other := newClient(t, e, db, prefix)
			if err := other.Start(ctx); err != nil {
				t.Fatal(err)
			}
			if err := other.Cancel(ctx, ref); err != nil {
				t.Fatalf("Cancel: %v", err)
			}
			select {
			case cause := <-stopped:
				if !errors.Is(cause, workflow.ErrCancelled) {
					t.Fatalf("handler context cause = %v, want ErrCancelled", cause)
				}
			case <-time.After(5 * testLease):
				t.Fatal("a run cancelled on another client kept running")
			}
			if st, err := worker.Status(ctx, ref); err != nil || st != workflow.StatusCancelled {
				t.Fatalf("Status = %q, %v; want cancelled", st, err)
			}
		})
	}
}

// TestRunFinishedIsDurable checks the RunNotifier contract on this driver:
// every terminal run (succeeded, failed, cancelled, never started) is
// delivered, once in the normal case, and ends that happened while no
// client had observers are delivered by the next client that has.
func TestRunFinishedIsDurable(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			clk := clock.NewFake(start)
			wf := workflow.Workflow{Name: "notify", Steps: []workflow.Step{{Name: "only", Run: func(_ context.Context, in []byte) ([]byte, error) {
				if string(in) == `"fail"` {
					return nil, workflow.Permanent(errors.New("boom"))
				}
				return in, nil
			}}}}

			// A client without observers finishes three runs and cancels a
			// delayed one before it starts.
			ctx1, stop1 := context.WithCancel(context.Background())
			first := newClient(t, e, db, prefix, sqlstore.WithClock(clk))
			if err := first.Register(wf); err != nil {
				t.Fatal(err)
			}
			if err := first.Start(ctx1); err != nil {
				t.Fatal(err)
			}
			want := map[workflow.RunID]workflow.Status{}
			for _, p := range []string{`"ok"`, `"fail"`} {
				ref, err := first.Enqueue(ctx1, workflow.Job{Workflow: wf.Name, Payload: []byte(p)})
				if err != nil {
					t.Fatal(err)
				}
				st := workflow.StatusSucceeded
				if p == `"fail"` {
					st = workflow.StatusFailed
				}
				want[ref.RunID] = st
				waitFor(t, 5*time.Second, "run "+p+" to end", func() bool {
					got, err := first.Status(ctx1, ref)
					return err == nil && got == st
				})
			}
			delayed, err := first.Enqueue(ctx1, workflow.Job{Workflow: wf.Name, RunAt: clk.Now().Add(time.Hour)})
			if err != nil {
				t.Fatal(err)
			}
			if err := first.Cancel(ctx1, delayed); err != nil {
				t.Fatal(err)
			}
			first.Stop()
			stop1()

			// The next client with an observer delivers all of them.
			var mu sync.Mutex
			got := map[string]workflow.RunEvent{}
			calls := 0
			second := newClient(t, e, db, prefix, sqlstore.WithClock(clk))
			if err := second.Register(wf); err != nil {
				t.Fatal(err)
			}
			if err := second.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) {
				mu.Lock()
				defer mu.Unlock()
				calls++
				key := string(ev.Ref.RunID)
				got[key] = ev
			}); err != nil {
				t.Fatal(err)
			}
			ctx2, stop2 := context.WithCancel(context.Background())
			defer stop2()
			if err := second.Start(ctx2); err != nil {
				t.Fatal(err)
			}
			if err := second.OnRunFinished(func(context.Context, workflow.RunEvent) {}); !errors.Is(err, workflow.ErrAlreadyStarted) {
				t.Fatalf("OnRunFinished after Start: %v, want ErrAlreadyStarted", err)
			}
			waitFor(t, 5*time.Second, "three run ends", func() bool {
				mu.Lock()
				defer mu.Unlock()
				return len(got) == 3
			})
			time.Sleep(100 * time.Millisecond) // room for a duplicate
			mu.Lock()
			defer mu.Unlock()
			if calls != 3 {
				t.Errorf("observer called %d times for 3 run ends, want 3", calls)
			}
			for id, st := range want {
				ev, ok := got[string(id)]
				if !ok || ev.Status != st {
					t.Errorf("run %s: event %+v, want status %s", id, ev, st)
				}
				if st == workflow.StatusFailed && (ev.Err == nil || ev.Err.Error() == "") {
					t.Errorf("failed run event has no error")
				}
			}
			var cancelled *workflow.RunEvent
			for _, ev := range got {
				if ev.Ref.ScheduledID == delayed.ScheduledID {
					ev := ev
					cancelled = &ev
				}
			}
			if cancelled == nil || cancelled.Status != workflow.StatusCancelled || !errors.Is(cancelled.Err, workflow.ErrCancelled) {
				t.Errorf("the cancelled delayed run's event = %+v, want cancelled with ErrCancelled and its ScheduledID", cancelled)
			}
		})
	}
}

// TestBatchStoreUpdateIsAtomic runs many concurrent Updates on one batch
// from several pools' worth of goroutines: every increment must land, and
// the once-only latch must flip for exactly one caller. It is the test that
// fails if Update stops locking the row before reading it.
func TestBatchStoreUpdateIsAtomic(t *testing.T) {
	const writers = 40
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			bs := newClient(t, e, db, prefix).BatchStore()
			ctx := context.Background()
			if err := bs.Create(ctx, batch.State{ID: "b1", Name: "n", Jobs: make([]batch.JobState, writers)}); err != nil {
				t.Fatalf("Create: %v", err)
			}
			if err := bs.Create(ctx, batch.State{ID: "b1"}); err == nil {
				t.Fatal("Create of an existing id succeeded")
			}
			if _, err := bs.Get(ctx, "nope"); !errors.Is(err, workflow.ErrNotFound) {
				t.Fatalf("Get unknown: %v, want ErrNotFound", err)
			}
			if _, err := bs.Update(ctx, "nope", func(*batch.State) error { return nil }); !errors.Is(err, workflow.ErrNotFound) {
				t.Fatalf("Update unknown: %v, want ErrNotFound", err)
			}
			var wins atomic.Int64
			var wg sync.WaitGroup
			for i := range writers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					won := false
					_, err := bs.Update(ctx, "b1", func(st *batch.State) error {
						won = false
						st.Jobs[i].Status = workflow.StatusSucceeded
						done := 0
						for _, j := range st.Jobs {
							if j.Status != "" {
								done++
							}
						}
						if done == len(st.Jobs) && !st.Finished {
							st.Finished, won = true, true
						}
						return nil
					})
					if err != nil {
						t.Errorf("Update: %v", err)
					}
					if won {
						wins.Add(1)
					}
				}()
			}
			wg.Wait()
			st, err := bs.Get(ctx, "b1")
			if err != nil {
				t.Fatal(err)
			}
			for i, j := range st.Jobs {
				if j.Status != workflow.StatusSucceeded {
					t.Errorf("job %d lost its update", i)
				}
			}
			if !st.Finished || wins.Load() != 1 {
				t.Fatalf("Finished=%v with %d winners, want true with exactly 1", st.Finished, wins.Load())
			}
			if _, err := bs.Update(ctx, "b1", func(st *batch.State) error { st.Name = "changed"; return errors.New("no") }); err == nil {
				t.Fatal("Update whose fn failed returned nil")
			}
			if st, _ := bs.Get(ctx, "b1"); st.Name != "n" {
				t.Fatalf("a failed Update saved its change: name %q", st.Name)
			}
		})
	}
}

// TestPoisonStepIsAbandoned runs a step that "kills" every worker it lands
// on (each client is crashed while the step runs) with WithMaxReclaims(1):
// the first takeover runs it again, the second fails the attempt with an
// error naming the abandonment, and with no retries the run fails. It is
// the test that fails if the reclaim cap stops counting.
func TestPoisonStepIsAbandoned(t *testing.T) {
	const lease = 400 * time.Millisecond
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			var execs atomic.Int64
			wf := workflow.Workflow{Name: "poison", Retry: workflow.RetryPolicy{}, Steps: []workflow.Step{{Name: "only",
				Run: func(ctx context.Context, _ []byte) ([]byte, error) {
					execs.Add(1)
					// The worker "dies" here. Crash cancels the client's context so
					// the handler can return, but a crashed client writes nothing,
					// so the step stays leased exactly as after a real crash.
					<-ctx.Done()
					return nil, ctx.Err()
				}}}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var ends sync.Map
			mk := func() *sqlstore.Client {
				c := newClient(t, e, db, prefix, sqlstore.WithLease(lease), sqlstore.WithMaxReclaims(1))
				if err := c.Register(wf); err != nil {
					t.Fatal(err)
				}
				if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { ends.Store(ev.Ref.RunID, ev) }); err != nil {
					t.Fatal(err)
				}
				if err := c.Start(ctx); err != nil {
					t.Fatal(err)
				}
				return c
			}
			c := mk()
			ref, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name})
			if err != nil {
				t.Fatal(err)
			}
			for n := int64(1); n <= 2; n++ {
				waitFor(t, 10*time.Second, fmt.Sprintf("execution %d", n), func() bool { return execs.Load() >= n })
				sqlstore.Crash(c)
				c = mk()
			}
			waitFor(t, 10*time.Second, "the run to fail", func() bool {
				st, err := c.Status(ctx, ref)
				return err == nil && st == workflow.StatusFailed
			})
			if got := execs.Load(); got != 2 {
				t.Fatalf("the poison step executed %d times, want 2 (first claim plus WithMaxReclaims(1) takeover)", got)
			}
			waitFor(t, 5*time.Second, "the run end", func() bool { _, ok := ends.Load(ref.RunID); return ok })
			v, _ := ends.Load(ref.RunID)
			if msg := v.(workflow.RunEvent).Err.Error(); !strings.Contains(msg, "abandoned by 2 workers") {
				t.Fatalf("run error = %q, want it to say the step was abandoned by 2 workers", msg)
			}
		})
	}
}
