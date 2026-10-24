package sqlstore_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/sqlstore"
)

// TestKeyFIFOHoldsPastARateLimit: with Concurrency.FIFO, a run held back
// by a rate limit also holds every later run of its concurrency key, even
// one whose own rate key has room.
func TestKeyFIFOHoldsPastARateLimit(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			var ran atomic.Int64
			label := func(k string) workflow.ConcurrencyKey {
				return workflow.KeyFunc(func(j workflow.Job) string { return j.Labels[k] })
			}
			wf := workflow.Workflow{Name: "fifo",
				Concurrency: &workflow.Concurrency{Key: label("k"), MaxRuns: 5, FIFO: true},
				RateLimits:  []workflow.RateLimit{{Key: label("r"), Limit: 1, Per: time.Hour}},
				Steps:       []workflow.Step{{Name: "a", Run: func(context.Context, []byte) ([]byte, error) { ran.Add(1); return nil, nil }}}}
			c := newClient(t, e, db, prefix)
			ctx := startWith(t, c, wf)
			job := func(r string) workflow.RunRef {
				ref, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, Labels: map[string]string{"k": "a", "r": r}})
				if err != nil {
					t.Fatal(err)
				}
				return ref
			}
			first := job("x")
			waitFor(t, 10*time.Second, "the first run", func() bool { st, _ := c.Status(ctx, first); return st == workflow.StatusSucceeded })
			second, third := job("x"), job("y")
			time.Sleep(30 * testPoll)
			for name, ref := range map[string]workflow.RunRef{"second (rate-limited)": second, "third (its rate key has room)": third} {
				if st, err := c.Status(ctx, ref); err != nil || st != workflow.StatusQueued {
					t.Errorf("%s run = %s, %v; want queued behind the rate-limited second", name, st, err)
				}
			}
		})
	}
}

// TestSchedulerOnlyAndWorker: a scheduler-only client fires a cron tick and
// runs no step; a worker client runs the step and fires nothing.
func TestSchedulerOnlyAndWorker(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			var ran atomic.Int64
			wf := workflow.Workflow{Name: "cron", Cron: []string{"* * * * *"}, Steps: []workflow.Step{{Name: "a",
				Run: func(context.Context, []byte) ([]byte, error) { ran.Add(1); return nil, nil }}}}
			clk := clock.NewFake(start)
			worker := newClient(t, e, db, prefix, sqlstore.WithClock(clk), sqlstore.WithoutScheduler(), sqlstore.WithNodeID("worker"))
			startWith(t, worker, wf)
			clk.Advance(time.Minute)
			time.Sleep(30 * testPoll)
			if n := ran.Load(); n != 0 {
				t.Fatalf("a client WithoutScheduler fired %d ticks", n)
			}
			sched := newClient(t, e, db, prefix, sqlstore.WithClock(clk), sqlstore.WithSchedulerOnly(), sqlstore.WithNodeID("sched"))
			ctx := startWith(t, sched, wf)
			waitFor(t, 10*time.Second, "the tick's run", func() bool { return ran.Load() == 1 })
			page, err := sched.Runs(ctx, workflow.RunQuery{Workflow: wf.Name})
			if err != nil || len(page.Runs) != 1 {
				t.Fatalf("Runs = %d, %v; want the one fired run", len(page.Runs), err)
			}
			waitFor(t, 10*time.Second, "both nodes listed", func() bool {
				nodes, err := sched.Nodes(ctx)
				return err == nil && len(nodes) == 2
			})
			nodes, _ := sched.Nodes(ctx)
			if nodes[0].ID != "sched" || nodes[0].Mode != workflow.NodeScheduler || nodes[0].Workers != 0 ||
				nodes[1].ID != "worker" || nodes[1].Mode != workflow.NodeWorker {
				t.Fatalf("Nodes = %+v; want sched (scheduler, no workers) and worker", nodes)
			}
			if got := page.Runs[0].AttemptedBy; len(got) != 1 || got[0] != "worker" {
				t.Fatalf("AttemptedBy = %v, want [worker]", got)
			}
			sched.Stop()
			waitFor(t, 10*time.Second, "the stopped node to leave the list", func() bool {
				nodes, err := worker.Nodes(ctx)
				return err == nil && len(nodes) == 1
			})
		})
	}
}

// TestHealthAndPayloadLimit: Health is ready once started with a live
// database and not after Stop; WithPayloadLimit refuses a bigger payload.
func TestHealthAndPayloadLimit(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			wf := workflow.Workflow{Name: "h", Steps: []workflow.Step{{Name: "a", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}}
			c := newClient(t, e, db, prefix, sqlstore.WithPayloadLimit(4))
			if r := c.Health(context.Background()); r.Ready {
				t.Fatalf("Health before Start = %+v, want not ready", r)
			}
			ctx := startWith(t, c, wf)
			if r := c.Health(ctx); !r.Live || !r.Ready {
				t.Fatalf("Health after Start = %+v, want live and ready", r)
			}
			_, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, Payload: []byte(`"12345"`)})
			var big *workflow.PayloadTooLargeError
			if !errors.As(err, &big) || big.Limit != 4 || !errors.Is(err, workflow.ErrInvalid) {
				t.Fatalf("Enqueue over the limit = %v, want a PayloadTooLargeError with Limit 4", err)
			}
			if _, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, Payload: []byte(`"1"`)}); err != nil {
				t.Fatalf("Enqueue under the limit: %v", err)
			}
			if c.PayloadLimit() != 4 {
				t.Fatalf("PayloadLimit = %d", c.PayloadLimit())
			}
			c.Stop()
			if r := c.Health(ctx); r.Live || r.Ready {
				t.Fatalf("Health after Stop = %+v, want neither", r)
			}
		})
	}
}

// TestCancelStepOnAnotherClient: CancelStep from a client that is not
// running the step reaches it at its next lease renewal.
func TestCancelStepOnAnotherClient(t *testing.T) {
	const lease = 300 * time.Millisecond
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			started := make(chan struct{}, 1)
			wf := workflow.Workflow{Name: "cs", Steps: []workflow.Step{{Name: "a", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
				started <- struct{}{}
				<-ctx.Done()
				return nil, context.Cause(ctx)
			}}}}
			runner := newClient(t, e, db, prefix, sqlstore.WithLease(lease))
			ctx := startWith(t, runner, wf)
			other := newClient(t, e, db, prefix, sqlstore.WithLease(lease), sqlstore.WithWorkers(1))
			startWith(t, other, workflow.Workflow{Name: "unrelated", Steps: wf.Steps})
			ref, err := runner.Enqueue(ctx, workflow.Job{Workflow: wf.Name})
			if err != nil {
				t.Fatal(err)
			}
			<-started
			if err := other.CancelStep(ctx, ref, "a"); err != nil {
				t.Fatal(err)
			}
			waitFor(t, 10*time.Second, "the run to fail", func() bool { st, _ := runner.Status(ctx, ref); return st == workflow.StatusFailed })
			p, err := runner.Runs(ctx, workflow.RunQuery{Ref: ref})
			if err != nil || !strings.Contains(p.Runs[0].Error, workflow.ErrStepCancelled.Error()) {
				t.Fatalf("run error = %+v, %v; want it to name ErrStepCancelled", p.Runs, err)
			}
		})
	}
}

// TestWeightedQueues: within one priority, a worker draws the queue to
// claim from by weight on the jitter source; with both queues ready, the
// draw decides which runs first.
//
// The single worker is first parked on a blocker run, and the two probe runs
// are enqueued behind it, so both are ready when the worker is free and the
// draw really chooses between two queues. Enqueued while the worker is idle
// it would claim whichever row landed first (the draw has nothing to choose
// between), and which that is depends on timing.
func TestWeightedQueues(t *testing.T) {
	const blockerPayload = "blocker"
	for _, e := range engines(t) {
		for _, tc := range []struct {
			draw  float64
			first string
		}{{0.1, "a"}, {0.9, "b"}} {
			t.Run(e.name+"/"+tc.first, func(t *testing.T) {
				db, prefix := e.open(t)
				order := make(chan string, 2)
				parked, release := make(chan struct{}), make(chan struct{})
				wf := workflow.Workflow{Name: "wq", Steps: []workflow.Step{{Name: "s", Run: func(ctx context.Context, in []byte) ([]byte, error) {
					if string(in) == blockerPayload {
						close(parked)
						<-release
						return nil, nil
					}
					info, _ := workflow.StepInfoFrom(ctx)
					order <- info.Queue
					return nil, nil
				}}}}
				c := newClient(t, e, db, prefix, sqlstore.WithWorkers(1), sqlstore.WithJitterSource(func() float64 { return tc.draw }),
					sqlstore.WithQueues(workflow.QueueConfig{Name: "a", Weight: 3}, workflow.QueueConfig{Name: "b", Weight: 1}))
				ctx := startWith(t, c, wf)
				if _, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, Queue: "a", Payload: []byte(blockerPayload)}); err != nil {
					t.Fatal(err)
				}
				<-parked
				if _, err := c.EnqueueMany(ctx, []workflow.Job{{Workflow: wf.Name, Queue: "a"}, {Workflow: wf.Name, Queue: "b"}}); err != nil {
					t.Fatal(err)
				}
				close(release)
				if got := <-order; got != tc.first {
					t.Fatalf("with draw %v the first run came from queue %q, want %q (weights a=3, b=1)", tc.draw, got, tc.first)
				}
			})
		}
	}
}
