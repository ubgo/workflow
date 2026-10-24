package sqlstore_test

import (
	"context"
	"database/sql"
	"errors"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/sqlstore"
	"github.com/ubgo/workflow/sqlstore/pgnotify"
)

// busNotifier is an in-process sqlstore.Notifier: every Notify reaches
// every Listen on the same channel. failFirst makes the first Listen call
// fail, to prove the client subscribes again.
type busNotifier struct {
	mu        sync.Mutex
	subs      map[string][]func()
	notifies  atomic.Int64
	listens   atomic.Int64
	failFirst bool
}

func newBus() *busNotifier { return &busNotifier{subs: map[string][]func(){}} }

func (b *busNotifier) Notify(_ context.Context, channel string) error {
	b.notifies.Add(1)
	b.mu.Lock()
	subs := slices.Clone(b.subs[channel])
	b.mu.Unlock()
	for _, wake := range subs {
		wake()
	}
	return nil
}

func (b *busNotifier) Listen(ctx context.Context, channel string, wake func()) error {
	if b.listens.Add(1) == 1 && b.failFirst {
		return errors.New("bus down")
	}
	b.mu.Lock()
	b.subs[channel] = append(b.subs[channel], wake)
	b.mu.Unlock()
	<-ctx.Done()
	return nil
}

// pickupPair starts an enqueuing client that serves only an unused queue,
// so it never runs the job itself, and a worker client polling every poll;
// both get extra. ran receives once per execution on the worker.
func pickupPair(t *testing.T, e engine, db *sql.DB, prefix string, poll time.Duration, extra ...sqlstore.Option) (enq *sqlstore.Client, ran chan time.Time) {
	t.Helper()
	ran = make(chan time.Time, 64)
	wf := workflow.Workflow{Name: "pickup", Steps: []workflow.Step{{Name: "only", Run: func(context.Context, []byte) ([]byte, error) {
		ran <- time.Now()
		return nil, nil
	}}}}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	enq = newClient(t, e, db, prefix, append([]sqlstore.Option{sqlstore.WithPollInterval(time.Hour), sqlstore.WithQueues(workflow.QueueConfig{Name: "unused"})}, extra...)...)
	worker := newClient(t, e, db, prefix, append([]sqlstore.Option{sqlstore.WithPollInterval(poll)}, extra...)...)
	for _, c := range []*sqlstore.Client{enq, worker} {
		if err := c.Register(wf); err != nil {
			t.Fatalf("Register: %v", err)
		}
		if err := c.Start(ctx); err != nil {
			t.Fatalf("Start: %v", err)
		}
	}
	return enq, ran
}

// TestNotifierWakesAnotherClient proves a Notifier carries an enqueue on one
// client to an idle worker on another whose poll would not come for an
// hour, and that a failed Listen is retried. Without the notifier the run
// would wait the full hour, so the 10s limit cannot pass by polling.
func TestNotifierWakesAnotherClient(t *testing.T) {
	e := engines(t)[0]
	db, prefix := e.open(t)
	bus := newBus()
	bus.failFirst = true
	enq, ran := pickupPair(t, e, db, prefix, time.Hour, sqlstore.WithNotifier(bus))
	waitFor(t, 5*time.Second, "both listeners subscribed", func() bool { return bus.listens.Load() >= 3 })
	if _, err := enq.Enqueue(context.Background(), workflow.Job{Workflow: "pickup"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	select {
	case <-ran:
	case <-time.After(10 * time.Second):
		t.Fatal("the idle worker never woke: the notification did not reach it")
	}
	if bus.notifies.Load() == 0 {
		t.Error("no Notify was sent")
	}
}

// TestPgnotifyListenNeedsPgx proves Listen refuses a pool on another
// driver with ErrDriver instead of blocking or polling silently.
func TestPgnotifyListenNeedsPgx(t *testing.T) {
	db, _ := openSQLite(t)
	err := pgnotify.New(db).Listen(context.Background(), "ch", func() {})
	if !errors.Is(err, pgnotify.ErrDriver) {
		t.Fatalf("Listen on SQLite: %v, want ErrDriver", err)
	}
}

// pickupSamples is how many enqueue-to-start latencies each mode measures.
const pickupSamples = 15

// TestPgnotifyPickupLatency measures, on Postgres, how long an enqueue on
// one client takes to start on an idle worker client: by polling alone at
// the production default of one second, and with pgnotify while the
// worker polls only every 30 seconds. It logs p50, p95 and max of both,
// for comparison, and asserts only what load cannot break: with pgnotify
// every sample starts well before a 30-second poll could have found it, so
// each start was a notification. Each sample waits a random fraction of a
// second before enqueueing, so polling is measured across its cycle.
func TestPgnotifyPickupLatency(t *testing.T) {
	var pg *engine
	for _, e := range engines(t) {
		if e.name == "postgres" {
			pg = &e
		}
	}
	if pg == nil {
		t.Skipf("%s unset: the LISTEN/NOTIFY latency case needs Postgres", envPostgresDSN)
	}
	const (
		shortPoll = time.Second      // production default: the polling baseline
		longPoll  = 30 * time.Second // with pgnotify: a poll never finds the run
		noticed   = 5 * time.Second  // every pgnotify start must come before this
	)
	measure := func(t *testing.T, poll time.Duration, notify bool) []time.Duration {
		db, prefix := pg.open(t)
		var extra []sqlstore.Option
		if notify {
			extra = append(extra, sqlstore.WithNotifier(pgnotify.New(db)))
		}
		enq, ran := pickupPair(t, *pg, db, prefix, poll, extra...)
		time.Sleep(200 * time.Millisecond) // let the listener subscribe
		out := make([]time.Duration, 0, pickupSamples)
		for range pickupSamples {
			time.Sleep(time.Duration(rand.Int64N(int64(time.Second))))
			at := time.Now()
			if _, err := enq.Enqueue(context.Background(), workflow.Job{Workflow: "pickup"}); err != nil {
				t.Fatalf("Enqueue: %v", err)
			}
			select {
			case got := <-ran:
				out = append(out, got.Sub(at))
			case <-time.After(2 * poll):
				t.Fatal("the run never started")
			}
		}
		slices.Sort(out)
		return out
	}
	pct := func(d []time.Duration, p int) time.Duration { return d[(len(d)-1)*p/100] }
	report := func(mode string, d []time.Duration) {
		t.Logf("%-8s n=%d p50=%v p95=%v max=%v", mode, len(d), pct(d, 50).Round(time.Microsecond), pct(d, 95).Round(time.Microsecond), d[len(d)-1].Round(time.Microsecond))
	}
	polled := measure(t, shortPoll, false)
	notified := measure(t, longPoll, true)
	report("polling", polled)
	report("notify", notified)
	if worst := notified[len(notified)-1]; worst >= noticed {
		t.Errorf("slowest pgnotify start took %v, want every one under %v (the worker polls every %v, so a slower start was not woken)", worst, noticed, longPoll)
	}
}
