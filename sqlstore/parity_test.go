package sqlstore_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/sqlb"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/sqlstore"
)

// The tests in this file prove on SQL what the conformance suite cannot
// see from one client: queues and pauses shared by several clients of one
// database, a pause that survives a restart, the 003 backfill of runs
// written before it, schedule-pause reports made once across nodes, and
// stop-when-idle.

// queueWorkflow is one step that records which client ran it (by the tag
// the client's middleware puts on the context) and returns at once.
func queueWorkflow(name, queue string) workflow.Workflow {
	return workflow.Workflow{Name: name, Queue: queue, Steps: []workflow.Step{{Name: "a", Run: func(_ context.Context, in []byte) ([]byte, error) {
		return in, nil
	}}}}
}

// tagger is a middleware that counts, per client tag, the steps it ran.
type tagger struct {
	mu  sync.Mutex
	ran map[string][]string // tag -> queues of the steps it ran
}

func (tg *tagger) mw(tag string) workflow.Middleware {
	return func(next workflow.Handler) workflow.Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			info, _ := workflow.StepInfoFrom(ctx)
			tg.mu.Lock()
			if tg.ran == nil {
				tg.ran = map[string][]string{}
			}
			tg.ran[tag] = append(tg.ran[tag], info.Queue)
			tg.mu.Unlock()
			return next(ctx, in)
		}
	}
}

func (tg *tagger) get(tag string) []string {
	tg.mu.Lock()
	defer tg.mu.Unlock()
	return append([]string(nil), tg.ran[tag]...)
}

func waitStatus(t *testing.T, c workflow.Client, ref workflow.RunRef, want workflow.Status) {
	t.Helper()
	waitFor(t, 5*time.Second, "status "+string(want), func() bool {
		st, err := c.Status(context.Background(), ref)
		return err == nil && st == want
	})
}

// TestQueuesSplitAcrossClients: a client given WithQueues claims only its
// queues, so a run on "high" runs on the client serving it and one on the
// default queue on the other, as separate worker processes per queue do.
func TestQueuesSplitAcrossClients(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			var tg tagger
			wf := queueWorkflow("q", "")
			high := newClient(t, e, db, prefix, sqlstore.WithQueues(workflow.QueueConfig{Name: "high"}), sqlstore.WithMiddleware(tg.mw("high")))
			def := newClient(t, e, db, prefix, sqlstore.WithQueues(workflow.QueueConfig{Name: workflow.DefaultQueue}), sqlstore.WithMiddleware(tg.mw("default")))
			ctx := startWith(t, high, wf)
			startWith(t, def, wf)
			var refs []workflow.RunRef
			for i := range 6 {
				q := ""
				if i%2 == 0 {
					q = "high"
				}
				ref, err := def.Enqueue(ctx, workflow.Job{Workflow: wf.Name, Queue: q})
				if err != nil {
					t.Fatal(err)
				}
				refs = append(refs, ref)
			}
			for _, ref := range refs {
				waitStatus(t, def, ref, workflow.StatusSucceeded)
			}
			for tag, want := range map[string]string{"high": "high", "default": workflow.DefaultQueue} {
				got := tg.get(tag)
				if len(got) != 3 {
					t.Errorf("client %s ran %d steps, want 3", tag, len(got))
				}
				for _, q := range got {
					if q != want {
						t.Errorf("client %s ran a step of queue %q", tag, q)
					}
				}
			}
		})
	}
}

// TestQueueWorkersCap: a queue with Workers 1 runs one attempt at a time
// on a client with four workers.
func TestQueueWorkersCap(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			var now, peak atomic.Int64
			release := make(chan struct{})
			wf := workflow.Workflow{Name: "cap", Steps: []workflow.Step{{Name: "a", Run: func(ctx context.Context, in []byte) ([]byte, error) {
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
				sqlstore.WithQueues(workflow.QueueConfig{Name: workflow.DefaultQueue, Workers: 1}))
			ctx := startWith(t, c, wf)
			var refs []workflow.RunRef
			for range 3 {
				ref, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name})
				if err != nil {
					t.Fatal(err)
				}
				refs = append(refs, ref)
			}
			waitFor(t, 5*time.Second, "one attempt running", func() bool { return now.Load() == 1 })
			time.Sleep(100 * time.Millisecond) // the other workers had time to claim
			close(release)
			for _, ref := range refs {
				waitStatus(t, c, ref, workflow.StatusSucceeded)
			}
			if p := peak.Load(); p != 1 {
				t.Fatalf("a Workers-1 queue ran %d attempts at once, want 1", p)
			}
		})
	}
}

// TestQueuePriority: with one worker, the ready attempts of a higher
// Priority queue are all taken before any of a lower one, whatever the
// arrival order.
func TestQueuePriority(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			var mu sync.Mutex
			var order []string
			gate := make(chan struct{})
			wf := workflow.Workflow{Name: "prio", Steps: []workflow.Step{{Name: "a", Run: func(ctx context.Context, in []byte) ([]byte, error) {
				if string(in) == `"block"` {
					select {
					case <-gate:
					case <-ctx.Done():
					}
				}
				mu.Lock()
				order = append(order, string(in))
				mu.Unlock()
				return in, nil
			}}}}
			c := newClient(t, e, db, prefix, sqlstore.WithWorkers(1), sqlstore.WithQueues(
				workflow.QueueConfig{Name: "low"},
				workflow.QueueConfig{Name: "high", Priority: 10},
			))
			ctx := startWith(t, c, wf)
			enqueue := func(q, payload string) workflow.RunRef {
				ref, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, Queue: q, Payload: []byte(`"` + payload + `"`)})
				if err != nil {
					t.Fatal(err)
				}
				return ref
			}
			blocker := enqueue("low", "block")
			waitStatus(t, c, blocker, workflow.StatusRunning)
			refs := []workflow.RunRef{enqueue("low", "l1"), enqueue("low", "l2"), enqueue("high", "h1"), enqueue("high", "h2")}
			close(gate)
			for _, ref := range refs {
				waitStatus(t, c, ref, workflow.StatusSucceeded)
			}
			mu.Lock()
			defer mu.Unlock()
			want := []string{`"block"`, `"h1"`, `"h2"`, `"l1"`, `"l2"`}
			for i := range want {
				if i >= len(order) || order[i] != want[i] {
					t.Fatalf("run order %v, want %v", order, want)
				}
			}
		})
	}
}

// TestPauseHoldsEveryClientAndSurvivesRestart: a pause made on one client
// holds runs on another, still holds after that client restarts, and a
// resume made anywhere releases them.
func TestPauseHoldsEveryClientAndSurvivesRestart(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			var ran atomic.Int64
			wf := workflow.Workflow{Name: "p", Steps: []workflow.Step{{Name: "a", Run: func(_ context.Context, in []byte) ([]byte, error) {
				ran.Add(1)
				return in, nil
			}}}}
			admin := newClient(t, e, db, prefix) // never started: Pause works on the tables alone
			if err := admin.Pause(context.Background(), workflow.PauseWorkflow("p")); err != nil {
				t.Fatal(err)
			}
			worker := newClient(t, e, db, prefix)
			ctx := startWith(t, worker, wf)
			ref, err := worker.Enqueue(ctx, workflow.Job{Workflow: "p"})
			if err != nil {
				t.Fatal(err)
			}
			time.Sleep(200 * time.Millisecond)
			if ran.Load() != 0 {
				t.Fatal("a paused workflow ran a step on another client")
			}
			worker.Stop()
			again := newClient(t, e, db, prefix)
			startWith(t, again, wf)
			time.Sleep(200 * time.Millisecond)
			if ran.Load() != 0 {
				t.Fatal("the pause did not survive a restart")
			}
			paused, err := again.Paused(ctx)
			if err != nil || len(paused) != 1 || paused[0] != workflow.PauseWorkflow("p") {
				t.Fatalf("Paused = %v, %v", paused, err)
			}
			if err := admin.Resume(ctx, workflow.PauseAll()); err != nil {
				t.Fatal(err)
			}
			waitStatus(t, again, ref, workflow.StatusSucceeded)
		})
	}
}

// TestRetryRetakesTheKey: Retry takes the run's idempotency key again, and
// refuses with ErrDuplicate while another unfinished run holds it.
func TestRetryRetakesTheKey(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			var fail atomic.Bool
			fail.Store(true)
			hold := make(chan struct{})
			wf := workflow.Workflow{Name: "k", Steps: []workflow.Step{{Name: "a", Run: func(ctx context.Context, in []byte) ([]byte, error) {
				if string(in) == `"hold"` {
					select {
					case <-hold:
					case <-ctx.Done():
					}
					return in, nil
				}
				if fail.Load() {
					return nil, workflow.Permanent(errors.New("boom"))
				}
				return in, nil
			}}}}
			c := newClient(t, e, db, prefix)
			ctx := startWith(t, c, wf)
			failed, err := c.Enqueue(ctx, workflow.Job{Workflow: "k", IdempotencyKey: "K", Payload: []byte(`"x"`)})
			if err != nil {
				t.Fatal(err)
			}
			waitStatus(t, c, failed, workflow.StatusFailed)
			holder, err := c.Enqueue(ctx, workflow.Job{Workflow: "k", IdempotencyKey: "K", Payload: []byte(`"hold"`)})
			if err != nil {
				t.Fatal(err)
			}
			if holder.RunID == failed.RunID {
				t.Fatal("the key was not released when the run failed")
			}
			if _, err := c.Retry(ctx, failed); !errors.Is(err, workflow.ErrDuplicate) {
				t.Fatalf("Retry while another run holds the key: %v, want ErrDuplicate", err)
			}
			if st, _ := c.Status(ctx, failed); st != workflow.StatusFailed {
				t.Fatalf("a refused Retry left the run %s, want failed", st)
			}
			close(hold)
			waitStatus(t, c, holder, workflow.StatusSucceeded)
			fail.Store(false)
			if _, err := c.Retry(ctx, failed); err != nil {
				t.Fatalf("Retry: %v", err)
			}
			waitStatus(t, c, failed, workflow.StatusSucceeded)
			dup, err := c.Enqueue(ctx, workflow.Job{Workflow: "k", IdempotencyKey: "K"})
			if err != nil || dup.RunID == failed.RunID {
				t.Fatalf("after the retried run finished its key is free: got %v, %v", dup, err)
			}
		})
	}
}

// TestMigration003Backfill: a run written before 003 gets its run_ext row
// (the default queue, its key) from the migration, and a client on the new
// schema runs it.
func TestMigration003Backfill(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()
			db, prefix := e.open(t)
			applyScripts(t, e, db, prefix, func(m sqlstore.MigrationScript) bool {
				return m.Name != "003_queues_admin" && m.Name != "004_run_states_chain"
			})
			q := e.dialect.QuoteIdent
			p := e.dialect.Placeholder
			ins := "INSERT INTO " + q(prefix+"runs") + " (id, workflow, labels, idem_active, priority, bucket, rate_keys, status, run_at, sig_floor," +
				" created_at, finished_at, fail_until, notified, notify_until, ver) VALUES (" + p(1) + ", 'old', '{}', " + p(2) +
				", 0, '', '[]', 'pending', 0, 0, 1, 0, 0, 0, 0, 0)"
			if _, err := db.Exec(ins, "run_old", "K1"); err != nil {
				t.Fatal(err)
			}
			applyScripts(t, e, db, prefix, func(m sqlstore.MigrationScript) bool {
				return m.Name == "003_queues_admin" || m.Name == "004_run_states_chain"
			})
			var queue, key string
			if err := db.QueryRow("SELECT queue, idem_key FROM "+q(prefix+"run_ext")+" WHERE run_id = "+p(1), "run_old").Scan(&queue, &key); err != nil {
				t.Fatalf("read the backfilled row: %v", err)
			}
			if queue != workflow.DefaultQueue || key != "K1" {
				t.Fatalf("backfilled queue %q key %q, want %q and K1", queue, key, workflow.DefaultQueue)
			}
			c := newClient(t, e, db, prefix) // Migrate records the hand-applied scripts
			startWith(t, c, queueWorkflow("old", ""))
			waitStatus(t, c, workflow.RunRef{RunID: "run_old"}, workflow.StatusSucceeded)
			page, err := c.Runs(ctx, workflow.RunQuery{Workflow: "old"})
			if err != nil || len(page.Runs) != 1 || page.Runs[0].Queue != workflow.DefaultQueue {
				t.Fatalf("Runs = %+v, %v", page.Runs, err)
			}
		})
	}
}

// TestSchedulePauseReportsOnceAcrossNodes: two clients fire one declared
// cron; while it is paused each due tick is reported skipped exactly once
// across both, nothing runs, not even on resume, and the next tick after
// the resume fires.
func TestSchedulePauseReportsOnceAcrossNodes(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			clk := clock.NewFake(start)
			var ran atomic.Int64
			wf := workflow.Workflow{Name: "cron", Cron: []string{"* * * * *"}, Steps: []workflow.Step{{Name: "a", Run: func(_ context.Context, in []byte) ([]byte, error) {
				ran.Add(1)
				return in, nil
			}}}}
			var mu sync.Mutex
			skipped := map[time.Time]int{}
			observe := func(_ context.Context, tk workflow.Tick) {
				if tk.Outcome == workflow.TickSkipped && tk.Reason == workflow.TickReasonPaused {
					mu.Lock()
					skipped[tk.At.UTC()]++
					mu.Unlock()
				}
			}
			var clients []*sqlstore.Client
			for range 2 {
				c := newClient(t, e, db, prefix, sqlstore.WithClock(clk))
				if err := c.OnTick(observe); err != nil {
					t.Fatal(err)
				}
				clients = append(clients, c)
			}
			ctx := startWith(t, clients[0], wf)
			startWith(t, clients[1], wf)
			list, err := clients[0].Schedules(ctx)
			if err != nil || len(list) != 1 || !list[0].Declared || list[0].Name != "* * * * *" {
				t.Fatalf("Schedules = %+v, %v; want the declared cron", list, err)
			}
			if err := clients[1].PauseSchedule(ctx, workflow.ScheduleTarget{ID: list[0].ID}); err != nil {
				t.Fatal(err)
			}
			for range 3 {
				clk.Advance(time.Minute)
				time.Sleep(50 * time.Millisecond)
			}
			waitFor(t, 5*time.Second, "three paused ticks reported", func() bool {
				mu.Lock()
				defer mu.Unlock()
				return len(skipped) == 3
			})
			time.Sleep(200 * time.Millisecond) // a second report would land now
			mu.Lock()
			for at, n := range skipped {
				if n != 1 {
					t.Errorf("tick %s reported %d times across the nodes, want 1", at, n)
				}
			}
			mu.Unlock()
			if ran.Load() != 0 {
				t.Fatal("a paused schedule ran")
			}
			if list, _ := clients[0].Schedules(ctx); len(list) != 1 || !list[0].Paused {
				t.Fatalf("Schedules while paused = %+v, want Paused", list)
			}
			if err := clients[0].ResumeSchedule(ctx, workflow.ScheduleTarget{All: true}); err != nil {
				t.Fatal(err)
			}
			time.Sleep(200 * time.Millisecond)
			if n := ran.Load(); n != 0 {
				t.Fatalf("resume fired %d ticks of the pause, want none", n)
			}
			clk.Advance(time.Minute)
			waitFor(t, 5*time.Second, "the next tick after the resume", func() bool { return ran.Load() == 1 })
		})
	}
}

// TestStopWhenIdle: the client drains once it has had no unfinished run
// for the grace on its scheduling clock, and not before.
func TestStopWhenIdle(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			clk := clock.NewFake(start)
			c := newClient(t, e, db, prefix, sqlstore.WithClock(clk), sqlstore.WithStopWhenIdle(time.Minute))
			ctx := startWith(t, c, queueWorkflow("idle", ""))
			ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "idle"})
			if err != nil {
				t.Fatal(err)
			}
			waitStatus(t, c, ref, workflow.StatusSucceeded)
			time.Sleep(100 * time.Millisecond)
			clk.Advance(30 * time.Second)
			select {
			case <-c.Done():
				t.Fatal("stopped before the grace ended")
			case <-time.After(200 * time.Millisecond):
			}
			clk.Advance(31 * time.Second)
			// idleSince is stamped by the first dispatcher pass that sees no
			// unfinished run, which under load (race detector, a database
			// engine running beside it) can come after the clock advances
			// above, so the grace then starts later on the fake clock and
			// 61s are not enough. The not-early check above still holds
			// (idleSince is never before the run finished); here a further
			// full grace is allowed, then the stop must come.
			select {
			case <-c.Done():
			case <-time.After(time.Second):
				clk.Advance(time.Minute)
				select {
				case <-c.Done():
				case <-time.After(5 * time.Second):
					t.Fatal("did not stop once idle for the grace")
				}
			}
			if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "idle"}); !errors.Is(err, workflow.ErrNotStarted) {
				t.Fatalf("Enqueue after stopping: %v, want ErrNotStarted", err)
			}
		})
	}
}

// TestCancelledTxReleasesTheWriteLock is the regression test for the
// SQLite hang fixed by moving to modernc.org/sqlite v1.45.0: a query
// cancelled while its transaction held the write lock and its rows were
// still open must not keep that lock once the client stops. It fails on
// the symptom: a fresh writer on the same file must get the write lock
// within a short timeout, where the leak made it wait for the busy
// timeout (the hang the migration lock showed).
func TestCancelledTxReleasesTheWriteLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wf.db")
	db, err := driversqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	c := newClient(t, engine{name: "sqlite", dialect: sqlb.SQLiteDialect{}}, db, "wf_")
	startWith(t, c, queueWorkflow("w", ""))
	// The error is the cancel itself, or the commit finding the
	// transaction already rolled back by it; either way nothing commits.
	if err := sqlstore.CancelMidTx(c); err == nil {
		t.Fatal("the cancelled transaction committed")
	}
	c.Stop()

	fresh, err := driversqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fresh.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	began := time.Now()
	conn, err := fresh.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		t.Fatalf("a fresh writer could not take the write lock after %s: %v (a cancelled transaction still holds it)", time.Since(began), err)
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		t.Fatal(err)
	}
	// The same pool must also migrate at once, the operation that hung.
	again, err := sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithTablePrefix("wf_"))
	if err != nil {
		t.Fatal(err)
	}
	if err := again.Migrate(ctx); err != nil {
		t.Fatalf("Migrate after the cancelled transaction: %v", err)
	}
}

// TestEnqueueManyIsOneTransaction: EnqueueMany writes every run or none
// (an ID generator failing on the third run leaves the table empty), and a
// key repeated inside one call starts one run whose RunRef both jobs get.
func TestEnqueueManyIsOneTransaction(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			var calls atomic.Int64
			failThird := sqlstore.WithIDGenerator(func(kind sqlstore.IDKind) (string, error) {
				if kind == sqlstore.IDRun && calls.Add(1) == 3 {
					return "", errors.New("id source down")
				}
				return sqlstore.DefaultIDGenerator(kind)
			})
			c := newClient(t, e, db, prefix, failThird)
			ctx := startWith(t, c, queueWorkflow("m", ""))
			jobs := []workflow.Job{{Workflow: "m"}, {Workflow: "m"}, {Workflow: "m"}}
			if refs, err := c.EnqueueMany(ctx, jobs); err == nil || refs != nil {
				t.Fatalf("EnqueueMany with a failing third insert = %v, %v; want nil refs and an error", refs, err)
			}
			page, err := c.Runs(ctx, workflow.RunQuery{Workflow: "m"})
			if err != nil || len(page.Runs) != 0 {
				t.Fatalf("after a failed EnqueueMany the table holds %d runs (%v), want 0", len(page.Runs), err)
			}
			refs, err := c.EnqueueMany(ctx, []workflow.Job{{Workflow: "m", IdempotencyKey: "k"}, {Workflow: "m"}, {Workflow: "m", IdempotencyKey: "k"}})
			if err != nil || len(refs) != 3 {
				t.Fatalf("EnqueueMany = %v, %v", refs, err)
			}
			if refs[0] != refs[2] || refs[0] == refs[1] {
				t.Fatalf("refs %v: want the two keyed jobs to share one run", refs)
			}
			for _, ref := range refs {
				waitStatus(t, c, ref, workflow.StatusSucceeded)
			}
		})
	}
}
